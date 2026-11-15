//! Tracker announces: HTTP (BEP 3/23) and UDP (BEP 15), always through the network guard.
//! HTTPS trackers are skipped in this version (no TLS stack yet).

use crate::bencode::{self, Value};
use crate::magnet::percent_encode;
use crate::metainfo::InfoHash;
use crate::net::NetGuard;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq)]
pub struct Url {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub path: String,
}

pub fn parse_url(url: &str) -> Option<Url> {
    let (scheme, rest) = url.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let default = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => 0,
    };
    let (host, port) = if let Some(h) = authority.strip_prefix('[') {
        let (h, p) = h.split_once(']')?;
        (h.to_string(), p.strip_prefix(':').map(|p| p.parse().ok()).unwrap_or(Some(default))?)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().ok()?),
            None => (authority.to_string(), default),
        }
    };
    if host.is_empty() || port == 0 {
        return None;
    }
    Some(Url { scheme, host, port, path })
}

#[derive(Debug, Clone)]
pub struct Announce {
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: Event,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    None,
    Started,
    Stopped,
    Completed,
}

#[derive(Debug, Clone)]
pub struct AnnounceResult {
    pub peers: Vec<SocketAddr>,
    pub interval: Duration,
}

fn bad(s: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s.into())
}

/// Minimal HTTP/1.1 GET through the guard. Returns (status, body).
pub async fn http_get(guard: &NetGuard, url: &str, timeout: Duration) -> io::Result<(u16, Vec<u8>)> {
    let u = parse_url(url).ok_or_else(|| bad(format!("bad url {url}")))?;
    if u.scheme != "http" {
        return Err(io::Error::new(io::ErrorKind::Unsupported, format!("{} not supported", u.scheme)));
    }
    tokio::time::timeout(timeout, async {
        let addrs = guard.resolve(&u.host, u.port).await?;
        let mut last = io::Error::new(io::ErrorKind::NotFound, "no address");
        for addr in addrs {
            match guard.tcp_connect(addr, Duration::from_secs(8)).await {
                Ok(mut s) => {
                    let host = if u.port == 80 { u.host.clone() } else { format!("{}:{}", u.host, u.port) };
                    let req = format!(
                        "GET {} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: playhead/{}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n",
                        u.path,
                        env!("CARGO_PKG_VERSION")
                    );
                    s.write_all(req.as_bytes()).await?;
                    let mut buf = Vec::new();
                    (&mut s).take(4 * 1024 * 1024).read_to_end(&mut buf).await?;
                    return parse_http_response(&buf);
                }
                Err(e) => last = e,
            }
        }
        Err(last)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "http timeout"))?
}

pub fn parse_http_response(buf: &[u8]) -> io::Result<(u16, Vec<u8>)> {
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").ok_or_else(|| bad("no http header end"))?;
    let head = String::from_utf8_lossy(&buf[..end]);
    let mut lines = head.lines();
    let status: u16 =
        lines.next().and_then(|l| l.split_whitespace().nth(1)).and_then(|s| s.parse().ok()).ok_or_else(|| bad("bad status line"))?;
    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let mut body = buf[end + 4..].to_vec();
    if chunked {
        let mut out = Vec::new();
        let mut i = 0;
        loop {
            let nl = body[i..].windows(2).position(|w| w == b"\r\n").ok_or_else(|| bad("bad chunk"))? + i;
            let size_s = String::from_utf8_lossy(&body[i..nl]);
            let size = usize::from_str_radix(size_s.split(';').next().unwrap_or("").trim(), 16).map_err(|_| bad("bad chunk size"))?;
            if size == 0 {
                break;
            }
            let s = nl + 2;
            out.extend_from_slice(body.get(s..s + size).ok_or_else(|| bad("short chunk"))?);
            i = s + size + 2;
        }
        body = out;
    }
    Ok((status, body))
}

pub fn parse_compact_v4(b: &[u8]) -> Vec<SocketAddr> {
    b.as_chunks::<6>()
        .0
        .iter()
        .map(|c| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(c[0], c[1], c[2], c[3])), u16::from_be_bytes([c[4], c[5]])))
        .filter(|a| a.port() != 0)
        .collect()
}

pub fn parse_compact_v6(b: &[u8]) -> Vec<SocketAddr> {
    b.as_chunks::<18>()
        .0
        .iter()
        .map(|c| {
            let ip: [u8; 16] = c[..16].try_into().unwrap();
            SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), u16::from_be_bytes([c[16], c[17]]))
        })
        .filter(|a| a.port() != 0)
        .collect()
}

pub fn parse_http_announce(body: &[u8]) -> io::Result<AnnounceResult> {
    let v = bencode::decode(body).map_err(|e| bad(e.to_string()))?;
    if let Some(f) = v.get("failure reason") {
        return Err(bad(format!("tracker: {}", f.as_str().unwrap_or("failure"))));
    }
    let mut peers = Vec::new();
    match v.get("peers") {
        Some(Value::Bytes(b)) => peers.extend(parse_compact_v4(b)),
        Some(Value::List(l)) => {
            for p in l {
                let ip = p.get("ip").and_then(Value::as_str).and_then(|s| s.parse::<IpAddr>().ok());
                let port = p.get("port").and_then(Value::as_int).filter(|&p| (1..=65535).contains(&p));
                if let (Some(ip), Some(port)) = (ip, port) {
                    peers.push(SocketAddr::new(ip, port as u16));
                }
            }
        }
        _ => {}
    }
    if let Some(Value::Bytes(b)) = v.get("peers6") {
        peers.extend(parse_compact_v6(b));
    }
    let interval = v.get("interval").and_then(Value::as_int).unwrap_or(1800).clamp(60, 3600) as u64;
    Ok(AnnounceResult { peers, interval: Duration::from_secs(interval) })
}

pub async fn announce(guard: &NetGuard, url: &str, a: &Announce) -> io::Result<AnnounceResult> {
    let u = parse_url(url).ok_or_else(|| bad(format!("bad tracker url {url}")))?;
    match u.scheme.as_str() {
        "http" => {
            let sep = if url.contains('?') { '&' } else { '?' };
            let event = match a.event {
                Event::Started => "&event=started",
                Event::Stopped => "&event=stopped",
                Event::Completed => "&event=completed",
                Event::None => "",
            };
            let full = format!(
                "{url}{sep}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant=80{event}",
                percent_encode(&a.info_hash),
                percent_encode(&a.peer_id),
                a.port,
                a.uploaded,
                a.downloaded,
                a.left
            );
            let (status, body) = http_get(guard, &full, Duration::from_secs(20)).await?;
            if status != 200 {
                return Err(bad(format!("tracker http {status}")));
            }
            parse_http_announce(&body)
        }
        "udp" => announce_udp(guard, &u, a).await,
        other => Err(io::Error::new(io::ErrorKind::Unsupported, format!("{other} trackers not supported"))),
    }
}

async fn announce_udp(guard: &NetGuard, u: &Url, a: &Announce) -> io::Result<AnnounceResult> {
    let addrs = guard.resolve(&u.host, u.port).await?;
    let state = guard.state();
    let target = addrs
        .into_iter()
        .find(|ad| match &state {
            crate::net::NetState::Up { addr } => !guard.bound() || addr.is_ipv4() == ad.is_ipv4(),
            _ => false,
        })
        .ok_or_else(|| bad("no usable tracker address"))?;
    let sock = guard.udp_bind(0).await?;
    let mut buf = [0u8; 2048];
    let mut conn_id = None;
    for attempt in 0..3u32 {
        let wait = Duration::from_secs(3 * (attempt as u64 + 1));
        let cid = match conn_id {
            Some(c) => c,
            None => {
                let tid: u32 = rand::random();
                let mut req = Vec::with_capacity(16);
                req.extend_from_slice(&0x41727101980u64.to_be_bytes());
                req.extend_from_slice(&0u32.to_be_bytes());
                req.extend_from_slice(&tid.to_be_bytes());
                sock.send_to(&req, target).await?;
                match tokio::time::timeout(wait, recv_tid(&sock, &mut buf, target, tid)).await {
                    Ok(Ok(n)) if n >= 16 && buf[..4] == 0u32.to_be_bytes() => {
                        let c = u64::from_be_bytes(buf[8..16].try_into().unwrap());
                        conn_id = Some(c);
                        c
                    }
                    Ok(Ok(n)) if n >= 8 && buf[..4] == 3u32.to_be_bytes() => {
                        return Err(bad(format!("tracker: {}", String::from_utf8_lossy(&buf[8..n]))));
                    }
                    _ => continue,
                }
            }
        };
        let tid: u32 = rand::random();
        let mut req = Vec::with_capacity(98);
        req.extend_from_slice(&cid.to_be_bytes());
        req.extend_from_slice(&1u32.to_be_bytes());
        req.extend_from_slice(&tid.to_be_bytes());
        req.extend_from_slice(&a.info_hash);
        req.extend_from_slice(&a.peer_id);
        req.extend_from_slice(&a.downloaded.to_be_bytes());
        req.extend_from_slice(&a.left.to_be_bytes());
        req.extend_from_slice(&a.uploaded.to_be_bytes());
        let ev: u32 = match a.event {
            Event::None => 0,
            Event::Completed => 1,
            Event::Started => 2,
            Event::Stopped => 3,
        };
        req.extend_from_slice(&ev.to_be_bytes());
        req.extend_from_slice(&0u32.to_be_bytes()); // ip
        req.extend_from_slice(&rand::random::<u32>().to_be_bytes()); // key
        req.extend_from_slice(&(-1i32).to_be_bytes()); // num_want
        req.extend_from_slice(&a.port.to_be_bytes());
        sock.send_to(&req, target).await?;
        match tokio::time::timeout(wait, recv_tid(&sock, &mut buf, target, tid)).await {
            Ok(Ok(n)) if n >= 20 && buf[..4] == 1u32.to_be_bytes() => {
                let interval = u32::from_be_bytes(buf[8..12].try_into().unwrap()).clamp(60, 3600);
                let peers = if target.is_ipv4() { parse_compact_v4(&buf[20..n]) } else { parse_compact_v6(&buf[20..n]) };
                return Ok(AnnounceResult { peers, interval: Duration::from_secs(interval as u64) });
            }
            Ok(Ok(n)) if n >= 8 && buf[..4] == 3u32.to_be_bytes() => {
                return Err(bad(format!("tracker: {}", String::from_utf8_lossy(&buf[8..n]))));
            }
            // Timed out: the connection id stays valid for a minute, so retry with it.
            _ => {}
        }
    }
    Err(io::Error::new(io::ErrorKind::TimedOut, "udp tracker didn't answer"))
}

async fn recv_tid(sock: &tokio::net::UdpSocket, buf: &mut [u8], from: SocketAddr, tid: u32) -> io::Result<usize> {
    loop {
        let (n, src) = sock.recv_from(buf).await?;
        if src == from && n >= 8 && buf[4..8] == tid.to_be_bytes() {
            return Ok(n);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let u = parse_url("udp://tracker.example:6969/announce").unwrap();
        assert_eq!((u.scheme.as_str(), u.host.as_str(), u.port, u.path.as_str()), ("udp", "tracker.example", 6969, "/announce"));
        let u = parse_url("http://[::1]:8080/a?b=1").unwrap();
        assert_eq!((u.host.as_str(), u.port, u.path.as_str()), ("::1", 8080, "/a?b=1"));
        assert_eq!(parse_url("http://x/announce").unwrap().port, 80);
        assert!(parse_url("udp://x/announce").is_none());
    }

    #[test]
    fn http_bodies() {
        let r = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        assert_eq!(parse_http_response(r).unwrap(), (200, b"Wikipedia".to_vec()));
        let body = b"d8:intervali900e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
        let a = parse_http_announce(body).unwrap();
        assert_eq!(a.peers, vec!["127.0.0.1:6881".parse().unwrap()]);
        assert_eq!(a.interval, Duration::from_secs(900));
        assert!(parse_http_announce(b"d14:failure reason4:nopee").is_err());
    }
}
