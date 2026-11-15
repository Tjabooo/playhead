//! Local swarm parts for tests, the benchmark and `playhead seed`: a seeder with throttling and
//! piece holding (to fake slow peers), plus tiny HTTP and UDP trackers. They record the source
//! address of everything that reaches them, which is what the leak tests check.

use crate::bencode::{self, Value};
use crate::bitfield::Bitfield;
use crate::metainfo::Metainfo;
use crate::wire::{self, Message};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Default)]
pub struct SeedConfig {
    /// Upload cap in bytes/s for this seeder (all connections together). None = unlimited.
    pub rate: Option<u64>,
    /// Extra delay before each block (latency).
    pub latency: Duration,
    /// Pieces this seeder sits on: requests for them are answered only after `hold_for`.
    pub hold: Vec<u32>,
    pub hold_for: Duration,
    /// Only these pieces (None = all).
    pub have: Option<Vec<u32>>,
    /// Answer ut_metadata requests.
    pub serve_metadata: bool,
    /// Tell the downloader about these peers with ut_pex.
    pub pex: Vec<SocketAddr>,
}

#[derive(Default)]
pub struct Stats {
    pub connections: AtomicU64,
    pub active: AtomicI64,
    pub bytes_sent: AtomicU64,
    pub sources: Mutex<Vec<IpAddr>>,
}

impl Stats {
    pub fn sources(&self) -> Vec<IpAddr> {
        self.sources.lock().unwrap().clone()
    }
}

pub struct Seeder {
    pub addr: SocketAddr,
    pub stats: Arc<Stats>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Seeder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Bucket {
    rate: u64,
    tokens: f64,
    last: Instant,
}

async fn take(bucket: &Option<Arc<Mutex<Bucket>>>, n: usize) {
    let Some(b) = bucket else { return };
    loop {
        let wait = {
            let mut b = b.lock().unwrap();
            let now = Instant::now();
            let rate = b.rate as f64;
            b.tokens = (b.tokens + now.duration_since(b.last).as_secs_f64() * rate).min(rate.max(n as f64));
            b.last = now;
            if b.tokens >= n as f64 {
                b.tokens -= n as f64;
                return;
            }
            (n as f64 - b.tokens) / rate
        };
        tokio::time::sleep(Duration::from_secs_f64(wait.min(1.0))).await;
    }
}

impl Seeder {
    pub async fn start(bind: SocketAddr, meta: Arc<Metainfo>, data: Arc<Vec<u8>>, cfg: SeedConfig) -> std::io::Result<Seeder> {
        let listener = TcpListener::bind(bind).await?;
        let addr = listener.local_addr()?;
        let stats = Arc::new(Stats::default());
        let st = stats.clone();
        let bucket = cfg.rate.map(|r| Arc::new(Mutex::new(Bucket { rate: r, tokens: 0.0, last: Instant::now() })));
        let started = Instant::now();
        let task = tokio::spawn(async move {
            loop {
                let Ok((sock, from)) = listener.accept().await else { continue };
                st.connections.fetch_add(1, Ordering::Relaxed);
                st.sources.lock().unwrap().push(from.ip());
                let (meta, data, cfg, st, bucket) = (meta.clone(), data.clone(), cfg.clone(), st.clone(), bucket.clone());
                tokio::spawn(async move {
                    st.active.fetch_add(1, Ordering::Relaxed);
                    let _ = seed_conn(sock, meta, data, cfg, st.clone(), bucket, started).await;
                    st.active.fetch_sub(1, Ordering::Relaxed);
                });
            }
        });
        Ok(Seeder { addr, stats, task })
    }
}

async fn seed_conn(
    mut sock: tokio::net::TcpStream,
    meta: Arc<Metainfo>,
    data: Arc<Vec<u8>>,
    cfg: SeedConfig,
    st: Arc<Stats>,
    bucket: Option<Arc<Mutex<Bucket>>>,
    started: Instant,
) -> std::io::Result<()> {
    let hs = wire::read_handshake(&mut sock).await?;
    if hs.info_hash != meta.info_hash {
        return Ok(());
    }
    sock.write_all(&wire::handshake_bytes(&meta.info_hash, &wire::new_peer_id())).await?;
    let (mut rd, mut wr) = sock.into_split();
    let n = meta.num_pieces();
    let mut bf = Bitfield::new(n);
    match &cfg.have {
        Some(list) => list.iter().for_each(|&i| bf.set(i)),
        None => (0..n).for_each(|i| bf.set(i)),
    }
    let mut hello = Vec::new();
    if hs.extensions {
        let md = if cfg.serve_metadata { Some(meta.raw_info.len() as u64) } else { None };
        hello.extend(crate::peer::ext_handshake(md, None).encode());
    }
    if hs.extensions && !cfg.pex.is_empty() {
        let mut added = Vec::new();
        for p in &cfg.pex {
            if let std::net::IpAddr::V4(ip) = p.ip() {
                added.extend_from_slice(&ip.octets());
                added.extend_from_slice(&p.port().to_be_bytes());
            }
        }
        let payload = bencode::dict([("added", bencode::bytes(added))]).encode();
        hello.extend(Message::Extended { id: crate::peer::OUR_UT_PEX, payload }.encode());
    }
    hello.extend(Message::Bitfield(bf.as_bytes().to_vec()).encode());
    hello.extend(Message::Unchoke.encode());
    wr.write_all(&hello).await?;

    let cancelled: Arc<Mutex<HashSet<(u32, u32)>>> = Arc::default();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let c2 = cancelled.clone();
    let meta_r = meta.clone();
    let serve_md = cfg.serve_metadata;
    let their_md = Arc::new(Mutex::new(None::<u8>));
    let md2 = their_md.clone();
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if let Message::Request { index, begin, length } = m {
                if c2.lock().unwrap().remove(&(index, begin)) {
                    continue;
                }
                if !bf.get(index) || begin as u64 + length as u64 > meta.piece_size(index) || length > 128 * 1024 {
                    continue;
                }
                if cfg.hold.contains(&index) {
                    let until = started + cfg.hold_for;
                    tokio::time::sleep_until(until.into()).await;
                }
                if !cfg.latency.is_zero() {
                    tokio::time::sleep(cfg.latency).await;
                }
                take(&bucket, length as usize).await;
                if c2.lock().unwrap().remove(&(index, begin)) {
                    continue;
                }
                let off = index as usize * meta.piece_length as usize + begin as usize;
                let msg = Message::Piece { index, begin, data: data[off..off + length as usize].to_vec() };
                if wr.write_all(&msg.encode()).await.is_err() {
                    return;
                }
                st.bytes_sent.fetch_add(length as u64, Ordering::Relaxed);
            } else if wr.write_all(&m.encode()).await.is_err() {
                return;
            }
        }
    });
    let result = async {
        loop {
            match wire::read_message(&mut rd).await? {
                m @ Message::Request { .. } => {
                    let _ = tx.send(m);
                }
                Message::Cancel { index, begin, .. } => {
                    cancelled.lock().unwrap().insert((index, begin));
                }
                Message::Extended { id: 0, payload } => {
                    if let Ok(v) = bencode::decode(&payload) {
                        *md2.lock().unwrap() = v.get("m").and_then(|m| m.get("ut_metadata")).and_then(Value::as_int).map(|i| i as u8);
                    }
                }
                Message::Extended { id: crate::peer::OUR_UT_METADATA, payload } if serve_md => {
                    let Ok((v, _)) = bencode::decode_prefix(&payload) else { continue };
                    if v.get("msg_type").and_then(Value::as_int) != Some(0) {
                        continue;
                    }
                    let piece = v.get("piece").and_then(Value::as_int).unwrap_or(0).max(0) as usize;
                    let Some(theirs) = *their_md.lock().unwrap() else { continue };
                    let raw = &meta_r.raw_info;
                    let s = piece * 16384;
                    let msg = if s < raw.len() {
                        crate::peer::metadata_msg(theirs, 1, piece as u32, Some(raw.len() as u64), &raw[s..(s + 16384).min(raw.len())])
                    } else {
                        crate::peer::metadata_msg(theirs, 2, piece as u32, None, &[])
                    };
                    let _ = tx.send(msg);
                }
                _ => {}
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    };
    let r = result.await;
    writer.abort();
    r
}

/// Tiny HTTP tracker: answers every announce with a fixed compact peer list, records sources.
pub struct HttpTracker {
    pub url: String,
    pub stats: Arc<Stats>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for HttpTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn compact(peers: &[SocketAddr]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in peers {
        if let IpAddr::V4(ip) = p.ip() {
            out.extend_from_slice(&ip.octets());
            out.extend_from_slice(&p.port().to_be_bytes());
        }
    }
    out
}

impl HttpTracker {
    pub async fn start(bind: SocketAddr, peers: Vec<SocketAddr>) -> std::io::Result<HttpTracker> {
        let l = TcpListener::bind(bind).await?;
        let addr = l.local_addr()?;
        let stats = Arc::new(Stats::default());
        let st = stats.clone();
        let body = bencode::dict([("interval", bencode::int(60)), ("peers", bencode::bytes(compact(&peers)))]).encode();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut s, from)) = l.accept().await else { continue };
                st.connections.fetch_add(1, Ordering::Relaxed);
                st.sources.lock().unwrap().push(from.ip());
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = s.read(&mut buf).await;
                    let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(&body).await;
                });
            }
        });
        Ok(HttpTracker { url: format!("http://{addr}/announce"), stats, task })
    }
}

/// Tiny UDP tracker (BEP 15).
pub struct UdpTracker {
    pub url: String,
    pub stats: Arc<Stats>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for UdpTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl UdpTracker {
    pub async fn start(bind: SocketAddr, peers: Vec<SocketAddr>) -> std::io::Result<UdpTracker> {
        let sock = UdpSocket::bind(bind).await?;
        let addr = sock.local_addr()?;
        let stats = Arc::new(Stats::default());
        let st = stats.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let Ok((n, from)) = sock.recv_from(&mut buf).await else { continue };
                st.connections.fetch_add(1, Ordering::Relaxed);
                st.sources.lock().unwrap().push(from.ip());
                if n < 16 {
                    continue;
                }
                let action = u32::from_be_bytes(buf[8..12].try_into().unwrap());
                let tid = &buf[12..16];
                let mut out = Vec::new();
                if action == 0 && n >= 16 {
                    out.extend_from_slice(&0u32.to_be_bytes());
                    out.extend_from_slice(tid);
                    out.extend_from_slice(&0x1234_5678_9abc_def0u64.to_be_bytes());
                } else if action == 1 && n >= 98 {
                    out.extend_from_slice(&1u32.to_be_bytes());
                    out.extend_from_slice(tid);
                    out.extend_from_slice(&60u32.to_be_bytes());
                    out.extend_from_slice(&0u32.to_be_bytes());
                    out.extend_from_slice(&(peers.len() as u32).to_be_bytes());
                    out.extend_from_slice(&compact(&peers));
                } else {
                    continue;
                }
                let _ = sock.send_to(&out, from).await;
            }
        });
        Ok(UdpTracker { url: format!("udp://{addr}/announce"), stats, task })
    }
}

/// Deterministic pseudo-random content (xorshift) so tests and benchmarks don't need real media.
pub fn test_data(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}
