//! Mainline DHT (BEP 5), read-only client mode (BEP 43): iterative `get_peers` lookups through the
//! network guard. We never answer queries, so the DHT doesn't put us in routing tables.

use crate::bencode::{self, Value};
use crate::metainfo::InfoHash;
use crate::net::NetGuard;
use crate::tracker::parse_compact_v4;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::time::Instant;

pub const BOOTSTRAP: &[(&str, u16)] =
    &[("router.bittorrent.com", 6881), ("dht.transmissionbt.com", 6881), ("router.utorrent.com", 6881), ("dht.libtorrent.org", 25401)];

const ALPHA: usize = 8;
const K: usize = 16;

fn distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut d = [0u8; 20];
    for i in 0..20 {
        d[i] = a[i] ^ b[i];
    }
    d
}

fn query(node_id: &[u8; 20], info_hash: &InfoHash, tid: u16) -> Vec<u8> {
    bencode::dict([
        ("a", bencode::dict([("id", bencode::bytes(&node_id[..])), ("info_hash", bencode::bytes(&info_hash[..]))])),
        ("q", bencode::bytes(&b"get_peers"[..])),
        ("ro", bencode::int(1)),
        ("t", bencode::bytes(tid.to_be_bytes().to_vec())),
        ("y", bencode::bytes(&b"q"[..])),
    ])
    .encode()
}

pub struct Reply {
    pub id: Option<[u8; 20]>,
    pub nodes: Vec<([u8; 20], SocketAddr)>,
    pub values: Vec<SocketAddr>,
}

pub fn parse_reply(b: &[u8]) -> Option<(u16, Reply)> {
    let v = bencode::decode(b).ok()?;
    if v.get("y")?.as_bytes()? != b"r" {
        return None;
    }
    let t = v.get("t")?.as_bytes()?;
    let tid = u16::from_be_bytes(t.try_into().ok()?);
    let r = v.get("r")?;
    let id = r.get("id").and_then(Value::as_bytes).and_then(|b| b.try_into().ok());
    let mut nodes = Vec::new();
    if let Some(n) = r.get("nodes").and_then(Value::as_bytes) {
        for c in n.as_chunks::<26>().0 {
            let nid: [u8; 20] = c[..20].try_into().unwrap();
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(c[20], c[21], c[22], c[23])), u16::from_be_bytes([c[24], c[25]]));
            if addr.port() != 0 {
                nodes.push((nid, addr));
            }
        }
    }
    let mut values = Vec::new();
    for item in r.get("values").and_then(Value::as_list).unwrap_or(&[]) {
        if let Some(b) = item.as_bytes() {
            values.extend(parse_compact_v4(b));
        }
    }
    Some((tid, Reply { id, nodes, values }))
}

/// One iterative lookup. Returns the peers found (possibly empty).
pub async fn get_peers(guard: &NetGuard, info_hash: InfoHash, budget: Duration) -> io::Result<Vec<SocketAddr>> {
    let sock = guard.udp_bind(0).await?;
    let mut net = guard.subscribe();
    let opened = net.borrow().clone();
    let node_id: [u8; 20] = rand::random();
    let mut seeds = Vec::new();
    for (h, p) in BOOTSTRAP {
        if let Ok(a) = guard.resolve(h, *p).await {
            seeds.extend(a.into_iter().filter(SocketAddr::is_ipv4));
        }
    }
    if seeds.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "no DHT bootstrap node resolved"));
    }
    let mut closest: BTreeMap<[u8; 20], SocketAddr> = BTreeMap::new();
    let mut queried: HashSet<SocketAddr> = HashSet::new();
    let mut inflight: HashMap<u16, (SocketAddr, Instant)> = HashMap::new();
    let mut peers: Vec<SocketAddr> = Vec::new();
    let mut next_tid: u16 = rand::random();
    let deadline = Instant::now() + budget;
    let mut buf = [0u8; 2048];

    let mut send = |addr: SocketAddr, inflight: &mut HashMap<u16, (SocketAddr, Instant)>| {
        next_tid = next_tid.wrapping_add(1);
        inflight.insert(next_tid, (addr, Instant::now()));
        (addr, query(&node_id, &info_hash, next_tid))
    };
    for s in seeds {
        queried.insert(s);
        let (a, q) = send(s, &mut inflight);
        let _ = sock.send_to(&q, a).await;
    }
    loop {
        if Instant::now() >= deadline || peers.len() >= 300 {
            break;
        }
        // Top up: query the closest unqueried nodes.
        inflight.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(3));
        let todo: Vec<SocketAddr> =
            closest.values().take(K).filter(|a| !queried.contains(a)).copied().take(ALPHA.saturating_sub(inflight.len())).collect();
        if todo.is_empty() && inflight.is_empty() {
            break;
        }
        for a in todo {
            queried.insert(a);
            let (a, q) = send(a, &mut inflight);
            let _ = sock.send_to(&q, a).await;
        }
        let r = tokio::select! {
            r = tokio::time::timeout(Duration::from_millis(500), sock.recv_from(&mut buf)) => r,
            _ = net.changed() => {
                if *net.borrow() != opened { return Err(io::Error::new(io::ErrorKind::NotConnected, "network guard went down")); }
                continue;
            }
        };
        let Ok(Ok((n, from))) = r else { continue };
        let Some((tid, reply)) = parse_reply(&buf[..n]) else { continue };
        if inflight.get(&tid).map(|(a, _)| *a) != Some(from) {
            continue;
        }
        inflight.remove(&tid);
        if let Some(id) = reply.id {
            closest.insert(distance(&id, &info_hash), from);
        }
        for (nid, addr) in reply.nodes {
            closest.entry(distance(&nid, &info_hash)).or_insert(addr);
        }
        while closest.len() > 256 {
            let last = *closest.keys().next_back().unwrap();
            closest.remove(&last);
        }
        for p in reply.values {
            if !peers.contains(&p) {
                peers.push(p);
            }
        }
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_get_peers_reply() {
        let mut nodes = vec![7u8; 20];
        nodes.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        let reply = bencode::dict([
            (
                "r",
                bencode::dict([
                    ("id", bencode::bytes(vec![1u8; 20])),
                    ("nodes", bencode::bytes(nodes)),
                    ("values", Value::List(vec![bencode::bytes(vec![1, 2, 3, 4, 0, 80])])),
                ]),
            ),
            ("t", bencode::bytes(vec![0, 9])),
            ("y", bencode::bytes(&b"r"[..])),
        ])
        .encode();
        let (tid, r) = parse_reply(&reply).unwrap();
        assert_eq!(tid, 9);
        assert_eq!(r.nodes[0].1, "10.0.0.1:6881".parse().unwrap());
        assert_eq!(r.values, vec!["1.2.3.4:80".parse().unwrap()]);
        assert!(query(&[0; 20], &[1; 20], 5).windows(5).any(|w| w == b"2:roi"));
    }
}
