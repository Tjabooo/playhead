//! One task per peer connection: handshake, framing, extension handshake, and the kill switch
//! (the connection closes as soon as the network guard leaves the state it was opened in).

use crate::bencode::{self, Value};
use crate::metainfo::InfoHash;
use crate::net::{NetGuard, NetState};
use crate::wire::{self, Message};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub type PeerKey = u64;

/// Our extension message id for ut_metadata (what peers must use when sending to us).
pub const OUR_UT_METADATA: u8 = 1;

#[derive(Debug)]
pub enum PeerEvent {
    Ready { key: PeerKey, addr: SocketAddr, peer_id: [u8; 20], incoming: bool },
    ExtHandshake { key: PeerKey, ut_metadata: Option<u8>, metadata_size: Option<u64>, client: Option<String> },
    Msg { key: PeerKey, msg: Message },
    MetadataPiece { key: PeerKey, piece: u32, data: Vec<u8> },
    MetadataReject { key: PeerKey, piece: u32 },
    MetadataRequest { key: PeerKey, piece: u32 },
    Closed { key: PeerKey, reason: String },
}

#[derive(Debug)]
pub enum PeerCmd {
    Send(Message),
    Close,
}

pub struct PeerSetup {
    pub key: PeerKey,
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
    pub metadata_size: Option<u64>,
    pub guard: Arc<NetGuard>,
    pub events: mpsc::Sender<PeerEvent>,
    /// Shared download cap (bytes/s) for all of this engine's peers.
    pub limiter: Option<Arc<RateLimiter>>,
}

/// Token bucket shared by an engine's peer connections. Reading slower backs the peers off over TCP.
pub struct RateLimiter {
    rate: f64,
    state: std::sync::Mutex<(f64, std::time::Instant)>,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        RateLimiter { rate: bytes_per_sec.max(1) as f64, state: std::sync::Mutex::new((0.0, std::time::Instant::now())) }
    }

    pub async fn take(&self, n: usize) {
        loop {
            let wait = {
                let mut s = self.state.lock().unwrap();
                let now = std::time::Instant::now();
                s.0 = (s.0 + now.duration_since(s.1).as_secs_f64() * self.rate).min(self.rate.max(n as f64));
                s.1 = now;
                if s.0 >= n as f64 {
                    s.0 -= n as f64;
                    return;
                }
                (n as f64 - s.0) / self.rate
            };
            tokio::time::sleep(Duration::from_secs_f64(wait.min(0.5))).await;
        }
    }
}

pub fn ext_handshake(metadata_size: Option<u64>, listen_port: Option<u16>) -> Message {
    let mut d = vec![
        ("m", bencode::dict([("ut_metadata", bencode::int(OUR_UT_METADATA as i64))])),
        ("v", bencode::bytes(concat!("playhead ", env!("CARGO_PKG_VERSION")).as_bytes())),
        ("reqq", bencode::int(500)),
    ];
    if let Some(s) = metadata_size {
        d.push(("metadata_size", bencode::int(s as i64)));
    }
    if let Some(p) = listen_port {
        d.push(("p", bencode::int(p as i64)));
    }
    let v = Value::Dict(d.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)).collect());
    Message::Extended { id: 0, payload: v.encode() }
}

pub fn metadata_msg(their_id: u8, msg_type: i64, piece: u32, total: Option<u64>, data: &[u8]) -> Message {
    let mut d = vec![("msg_type", bencode::int(msg_type)), ("piece", bencode::int(piece as i64))];
    if let Some(t) = total {
        d.push(("total_size", bencode::int(t as i64)));
    }
    let mut payload = Value::Dict(d.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)).collect()).encode();
    payload.extend_from_slice(data);
    Message::Extended { id: their_id, payload }
}

/// Run a connection. `stream` is connected; for incoming connections `their_hs` is the handshake
/// already read by the listener.
pub async fn run(
    setup: PeerSetup,
    mut stream: TcpStream,
    addr: SocketAddr,
    their_hs: Option<wire::Handshake>,
    mut cmds: mpsc::UnboundedReceiver<PeerCmd>,
) {
    let key = setup.key;
    let events = setup.events.clone();
    let reason = match session(setup, &mut stream, addr, their_hs, &mut cmds).await {
        Ok(r) => r,
        Err(e) => e.to_string(),
    };
    let _ = stream.shutdown().await;
    let _ = events.send(PeerEvent::Closed { key, reason }).await;
}

async fn session(
    setup: PeerSetup,
    stream: &mut TcpStream,
    addr: SocketAddr,
    their_hs: Option<wire::Handshake>,
    cmds: &mut mpsc::UnboundedReceiver<PeerCmd>,
) -> std::io::Result<String> {
    let mut net = setup.guard.subscribe();
    let opened_in = net.borrow().clone();
    if !opened_in.is_up() {
        return Ok("network down".into());
    }
    let incoming = their_hs.is_some();
    let hs_timeout = Duration::from_secs(10);
    let theirs = match their_hs {
        Some(h) => {
            stream.write_all(&wire::handshake_bytes(&setup.info_hash, &setup.peer_id)).await?;
            h
        }
        None => {
            stream.write_all(&wire::handshake_bytes(&setup.info_hash, &setup.peer_id)).await?;
            tokio::time::timeout(hs_timeout, wire::read_handshake(stream))
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake timeout"))??
        }
    };
    if theirs.info_hash != setup.info_hash {
        return Ok("wrong info hash".into());
    }
    if theirs.peer_id == setup.peer_id {
        return Ok("connected to ourselves".into());
    }
    let (mut rd, mut wr) = stream.split();
    if theirs.extensions {
        wr.write_all(&ext_handshake(setup.metadata_size, None).encode()).await?;
    }
    if setup.events.send(PeerEvent::Ready { key: setup.key, addr, peer_id: theirs.peer_id, incoming }).await.is_err() {
        return Ok("engine gone".into());
    }

    let key = setup.key;
    let events = setup.events.clone();
    let limiter = setup.limiter.clone();
    let reader = async move {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(180), wire::read_message(&mut rd))
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "peer silent for 3 min"))??;
            let ev = match msg {
                Message::Extended { id: 0, payload } => {
                    let Ok(v) = bencode::decode(&payload) else { continue };
                    let ut = v.get("m").and_then(|m| m.get("ut_metadata")).and_then(Value::as_int);
                    PeerEvent::ExtHandshake {
                        key,
                        ut_metadata: ut.filter(|&i| (1..=255).contains(&i)).map(|i| i as u8),
                        metadata_size: v.get("metadata_size").and_then(Value::as_int).filter(|&s| s > 0).map(|s| s as u64),
                        client: v.get("v").and_then(Value::as_str).map(str::to_string),
                    }
                }
                Message::Extended { id: OUR_UT_METADATA, payload } => {
                    let Ok((v, used)) = bencode::decode_prefix(&payload) else { continue };
                    let piece = v.get("piece").and_then(Value::as_int).unwrap_or(-1);
                    if !(0..=10_000).contains(&piece) {
                        continue;
                    }
                    let piece = piece as u32;
                    match v.get("msg_type").and_then(Value::as_int) {
                        Some(0) => PeerEvent::MetadataRequest { key, piece },
                        Some(1) => PeerEvent::MetadataPiece { key, piece, data: payload[used..].to_vec() },
                        Some(2) => PeerEvent::MetadataReject { key, piece },
                        _ => continue,
                    }
                }
                Message::Extended { .. } | Message::KeepAlive | Message::Unknown(_) | Message::Port(_) => continue,
                msg => {
                    if let (Message::Piece { data, .. }, Some(l)) = (&msg, &limiter) {
                        l.take(data.len()).await;
                    }
                    PeerEvent::Msg { key, msg }
                }
            };
            if events.send(ev).await.is_err() {
                return Ok::<String, std::io::Error>("engine gone".into());
            }
        }
    };

    let writer = async {
        let mut keepalive = tokio::time::interval(Duration::from_secs(90));
        keepalive.tick().await;
        loop {
            tokio::select! {
                cmd = cmds.recv() => match cmd {
                    Some(PeerCmd::Send(m)) => {
                        let mut buf = m.encode();
                        // Coalesce whatever else is queued into one write.
                        while let Ok(PeerCmd::Send(m)) = cmds.try_recv() {
                            buf.extend_from_slice(&m.encode());
                            if buf.len() > 256 * 1024 { break; }
                        }
                        wr.write_all(&buf).await?;
                    }
                    Some(PeerCmd::Close) | None => return Ok("closed by engine".to_string()),
                },
                _ = keepalive.tick() => wr.write_all(&Message::KeepAlive.encode()).await?,
                changed = net.changed() => {
                    let now = net.borrow().clone();
                    if changed.is_err() || now != opened_in {
                        return Ok(match now { NetState::Down { reason } => format!("kill switch: {reason}"), _ => "network changed".into() });
                    }
                }
            }
        }
    };

    tokio::select! {
        r = reader => r,
        r = writer => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rate_limiter_paces_reads() {
        let l = RateLimiter::new(200_000);
        let t = std::time::Instant::now();
        for _ in 0..10 {
            l.take(40_000).await;
        }
        let s = t.elapsed().as_secs_f64();
        assert!((1.7..2.6).contains(&s), "400 kB at 200 kB/s took {s:.2} s");
    }
}
