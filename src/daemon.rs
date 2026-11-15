//! `playhead serve`: one long-running process that streams many torrents for another program
//! (a media server), behind a small token-protected JSON API on a loopback address.
//!
//! ```text
//! GET    /api/health                      version, network guard, stream count
//! POST   /api/streams                     start (or reuse) a stream, see `NewStream`
//! GET    /api/streams                     all streams' status
//! GET    /api/streams/<id>                one stream's status + its own pieces ("own": "0022…")
//! PUT    /api/streams/<id>/external       {"states": "0122…", "paths": [...]}  (assist mode)
//! PUT    /api/streams/<id>/peers          {"peers": ["ip:port", ...]}
//! DELETE /api/streams/<id>                stop it and delete its cache
//! GET    /s/<id>/file/<n>                 the file, with range requests (reads wait for pieces)
//! ```
//!
//! Every request needs `Authorization: Bearer <token>`.
//!
//! Assist mode (`"assist": true`) is for running next to another client that downloads the same
//! torrent in order: give it that client's piece states and file paths, and playhead reads what the
//! other client already has from its file, leaves pieces it's busy with alone, and only fetches what a
//! reader needs next and the other client doesn't have (seeks, stuck pieces).

use crate::engine::{Engine, EngineConfig, Source};
use crate::http::{self, Request, respond};
use crate::log;
use crate::magnet::{Magnet, parse_info_hash};
use crate::metainfo::{Metainfo, hex};
use crate::net::{NetGuard, NetState};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
pub struct DaemonConfig {
    pub api: SocketAddr,
    pub token: String,
    pub cache_dir: PathBuf,
    /// Template for every stream's engine (out_dir, trackers and peers are set per stream).
    pub engine: EngineConfig,
    /// Close a stream nobody has asked about (API or reads) for this long.
    pub idle: Duration,
}

struct Stream {
    id: String,
    info_hash: [u8; 20],
    engine: Engine,
    cache: PathBuf,
    touched: Mutex<Instant>,
    files: Mutex<Vec<usize>>,
}

impl Stream {
    fn touch(&self) {
        *self.touched.lock().unwrap() = Instant::now();
    }
}

type Registry = Arc<Mutex<HashMap<String, Arc<Stream>>>>;

pub async fn run(cfg: DaemonConfig, guard: Arc<NetGuard>) -> std::io::Result<()> {
    if !cfg.api.ip().is_loopback() {
        return Err(std::io::Error::other("the API must listen on a loopback address"));
    }
    if cfg.token.len() < 16 {
        return Err(std::io::Error::other("the API token must be at least 16 characters"));
    }
    std::fs::create_dir_all(&cfg.cache_dir)?;
    let listener = TcpListener::bind(cfg.api).await?;
    log::info(&format!("API on http://{}", listener.local_addr()?));
    let streams: Registry = Arc::default();
    {
        let (streams, idle) = (streams.clone(), cfg.idle);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(30));
            loop {
                t.tick().await;
                let stale: Vec<Arc<Stream>> =
                    streams.lock().unwrap().values().filter(|s| s.touched.lock().unwrap().elapsed() > idle).cloned().collect();
                for s in stale {
                    let st = s.engine.status().await;
                    if st["readers"].as_array().is_some_and(|r| !r.is_empty()) {
                        s.touch();
                        continue;
                    }
                    log::info(&format!("stream {} idle, closing", s.id));
                    close(&streams, &s.id).await;
                }
            }
        });
    }
    let cfg = Arc::new(cfg);
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let (cfg, guard, streams) = (cfg.clone(), guard.clone(), streams.clone());
        tokio::spawn(async move {
            let _ = sock.set_nodelay(true);
            if let Ok(Some(req)) = http::read_request(&mut sock).await {
                let _ = route(&cfg, &guard, &streams, &mut sock, req).await;
            }
            let _ = sock.shutdown().await;
        });
    }
}

fn authorized(cfg: &DaemonConfig, req: &Request) -> bool {
    let Some(given) = req.authorization.as_deref().and_then(|a| a.strip_prefix("Bearer ")) else { return false };
    // Constant-time compare.
    let (a, b) = (given.as_bytes(), cfg.token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn reply(s: &mut TcpStream, status: &str, v: &Value) -> std::io::Result<()> {
    respond(s, status, "application/json", serde_json::to_string(v).unwrap().as_bytes()).await
}

async fn route(cfg: &DaemonConfig, guard: &Arc<NetGuard>, streams: &Registry, s: &mut TcpStream, mut req: Request) -> std::io::Result<()> {
    if !authorized(cfg, &req) {
        return reply(s, "401 Unauthorized", &json!({"error": "missing or wrong token"})).await;
    }
    let path = req.path.split('?').next().unwrap_or("/").to_string();
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    let find = |id: &str| streams.lock().unwrap().get(id).cloned();
    match (req.method.as_str(), parts.as_slice()) {
        ("GET", ["api", "health"]) => {
            let net = match guard.state() {
                NetState::Up { addr } => json!({"up": true, "address": addr.to_string(), "bound": guard.bound()}),
                NetState::Down { reason } => json!({"up": false, "reason": reason, "bound": guard.bound()}),
            };
            let n = streams.lock().unwrap().len();
            reply(s, "200 OK", &json!({"version": env!("CARGO_PKG_VERSION"), "network": net, "streams": n})).await
        }
        ("POST", ["api", "streams"]) => match new_stream(cfg, guard, streams, &req.body).await {
            Ok(v) => reply(s, "200 OK", &v).await,
            Err((code, msg)) => reply(s, code, &json!({"error": msg})).await,
        },
        ("GET", ["api", "streams"]) => {
            let list: Vec<Arc<Stream>> = streams.lock().unwrap().values().cloned().collect();
            let mut out = Vec::new();
            for st in list {
                out.push(stream_status(&st).await);
            }
            reply(s, "200 OK", &json!(out)).await
        }
        ("GET", ["api", "streams", id]) => match find(id) {
            Some(st) => {
                st.touch();
                reply(s, "200 OK", &stream_status(&st).await).await
            }
            None => reply(s, "404 Not Found", &json!({"error": "no such stream"})).await,
        },
        ("PUT", ["api", "streams", id, "external"]) => match find(id) {
            Some(st) => {
                st.touch();
                let v: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                if let Some(paths) = v["paths"].as_array() {
                    st.engine.set_external_paths(paths.iter().filter_map(|p| p.as_str()).map(PathBuf::from).collect());
                }
                if let Some(states) = v["states"].as_str() {
                    st.engine.set_external_states(parse_states(states));
                }
                reply(s, "200 OK", &json!({"ok": true})).await
            }
            None => reply(s, "404 Not Found", &json!({"error": "no such stream"})).await,
        },
        ("PUT", ["api", "streams", id, "peers"]) => match find(id) {
            Some(st) => {
                st.touch();
                let v: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
                st.engine.add_peers(parse_peers(&v["peers"]));
                reply(s, "200 OK", &json!({"ok": true})).await
            }
            None => reply(s, "404 Not Found", &json!({"error": "no such stream"})).await,
        },
        ("DELETE", ["api", "streams", id]) => {
            if close(streams, id).await {
                reply(s, "200 OK", &json!({"ok": true})).await
            } else {
                reply(s, "404 Not Found", &json!({"error": "no such stream"})).await
            }
        }
        ("GET" | "HEAD", ["s", id, rest @ ..]) => match find(id) {
            Some(st) => {
                st.touch();
                req.path = format!("/{}", rest.join("/"));
                http::handle(&st.engine, s, req).await
            }
            None => respond(s, "404 Not Found", "text/plain", b"no such stream\n").await,
        },
        _ => reply(s, "404 Not Found", &json!({"error": "not found"})).await,
    }
}

async fn stream_status(st: &Stream) -> Value {
    let mut v = st.engine.status().await;
    v["id"] = json!(st.id);
    v["own"] = json!(st.engine.own_states());
    v["selected_files"] = json!(*st.files.lock().unwrap());
    v
}

async fn close(streams: &Registry, id: &str) -> bool {
    let Some(st) = streams.lock().unwrap().remove(id) else { return false };
    st.engine.shutdown().await;
    // Readers notice the engine stopped and close their files; then the cache can go.
    let cache = st.cache.clone();
    tokio::spawn(async move {
        for _ in 0..20 {
            if !cache.exists() || std::fs::remove_dir_all(&cache).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        log::warn(&format!("couldn't delete {}", cache.display()));
    });
    true
}

pub fn parse_states(s: &str) -> Vec<u8> {
    s.bytes().map(|b| b.wrapping_sub(b'0')).map(|b| if b <= 2 { b } else { 0 }).collect()
}

fn parse_peers(v: &Value) -> Vec<SocketAddr> {
    v.as_array().map(|a| a.iter().filter_map(|p| p.as_str()).filter_map(|p| p.parse().ok()).collect()).unwrap_or_default()
}

pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' | b'\n' | b'\r' | b' ' => continue,
            _ => return None,
        } as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// `POST /api/streams` body.
///
/// ```json
/// {"info_hash": "hex", "torrent": "<base64 .torrent>", "magnet": "magnet:?...", "file": 0,
///  "trackers": [...], "peers": ["ip:port"], "assist": true, "external_paths": [...], "states": "0122..."}
/// ```
/// One of `torrent`, `magnet` or `info_hash` is required; `torrent` skips the metadata download.
async fn new_stream(cfg: &DaemonConfig, guard: &Arc<NetGuard>, streams: &Registry, body: &[u8]) -> Result<Value, (&'static str, String)> {
    let bad = |m: &str| ("400 Bad Request", m.to_string());
    let v: Value = serde_json::from_slice(body).map_err(|e| bad(&format!("bad JSON: {e}")))?;
    let file = v["file"].as_u64().ok_or_else(|| bad("file (index) is required"))? as usize;
    let trackers: Vec<String> =
        v["trackers"].as_array().map(|a| a.iter().filter_map(|t| t.as_str().map(str::to_string)).collect()).unwrap_or_default();
    let peers = parse_peers(&v["peers"]);
    let paths: Vec<PathBuf> =
        v["external_paths"].as_array().map(|a| a.iter().filter_map(|p| p.as_str()).map(PathBuf::from).collect()).unwrap_or_default();
    let assist = v["assist"].as_bool().unwrap_or(false);
    let source = if let Some(t) = v["torrent"].as_str() {
        let bytes = base64_decode(t).ok_or_else(|| bad("torrent isn't base64"))?;
        Source::Torrent(Metainfo::from_torrent_bytes(&bytes).map_err(|e| bad(&e.to_string()))?)
    } else if let Some(m) = v["magnet"].as_str() {
        Source::Magnet(Magnet::parse(m).map_err(|e| bad(&e))?)
    } else if let Some(h) = v["info_hash"].as_str() {
        Source::Magnet(Magnet {
            info_hash: parse_info_hash(h).ok_or_else(|| bad("bad info_hash"))?,
            name: None,
            trackers: vec![],
            peers: vec![],
        })
    } else {
        return Err(bad("one of torrent, magnet or info_hash is required"));
    };
    let info_hash = match &source {
        Source::Torrent(m) => m.info_hash,
        Source::Magnet(m) => m.info_hash,
    };
    if let Some(h) = v["info_hash"].as_str()
        && parse_info_hash(h) != Some(info_hash)
    {
        return Err(bad("info_hash doesn't match the torrent"));
    }
    if !guard.state().is_up() {
        return Err(("503 Service Unavailable", "network guard is down (tunnel gone)".into()));
    }
    // Same torrent already streaming: reuse it.
    let existing = streams.lock().unwrap().values().find(|s| s.info_hash == info_hash).cloned();
    let st = match existing {
        Some(st) => {
            st.engine.add_peers(peers);
            st
        }
        None => {
            let id = format!("{:032x}", rand::random::<u128>());
            let cache = cfg.cache_dir.join(hex(&info_hash));
            let _ = std::fs::remove_dir_all(&cache);
            let mut ecfg = cfg.engine.clone();
            ecfg.out_dir = cache.clone();
            ecfg.extra_trackers = trackers;
            ecfg.extra_peers = peers;
            ecfg.resume = false;
            ecfg.picker.background = !assist;
            if let Some(b) = v["start_lead_bytes"].as_u64() {
                ecfg.start_lead_bytes = b.min(1 << 30);
            }
            if let Some(l) = v["download_limit"].as_u64().filter(|&l| l > 0) {
                ecfg.download_limit = Some(l);
            }
            if let Some(w) = v["window_mb"].as_u64() {
                ecfg.picker.window_bytes = w.clamp(4, 512) * 1024 * 1024;
            }
            let engine = Engine::start(source, ecfg, guard.clone());
            let st = Arc::new(Stream {
                id: id.clone(),
                info_hash,
                engine,
                cache,
                touched: Mutex::new(Instant::now()),
                files: Mutex::new(vec![]),
            });
            streams.lock().unwrap().insert(id, st.clone());
            log::info(&format!("stream {} for {} file #{file}{}", st.id, hex(&info_hash), if assist { " (assist)" } else { "" }));
            st
        }
    };
    if !paths.is_empty() {
        st.engine.set_external_paths(paths);
    }
    if let Some(states) = v["states"].as_str() {
        st.engine.set_external_states(parse_states(states));
    }
    // Metadata: immediate for a .torrent, else wait (bounded) for it from peers.
    if tokio::time::timeout(Duration::from_secs(60), st.engine.metadata()).await.is_err() {
        return Ok(json!({"id": st.id, "state": "metadata", "url": format!("/s/{}/file/{file}", st.id)}));
    }
    st.engine.select(file).await.map_err(|e| ("409 Conflict", e))?;
    {
        let mut f = st.files.lock().unwrap();
        if !f.contains(&file) {
            f.push(file);
        }
    }
    st.touch();
    Ok(json!({"id": st.id, "state": "ready", "url": format!("/s/{}/file/{file}", st.id)}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes() {
        assert_eq!(base64_decode("aGVsbG8gd29ybGQ=").unwrap(), b"hello world");
        assert_eq!(base64_decode("+/8=").unwrap(), vec![0xfb, 0xff]);
        assert!(base64_decode("a$").is_none());
        assert_eq!(parse_states("0129x"), vec![0, 1, 2, 0, 0]);
    }
}
