//! Streaming benchmark on a local swarm: a few capped fast seeders, one slow seeder and one stuck
//! seeder (connected, unchoked, but sits on every request for a long time). A simulated player
//! reads the HTTP stream at a fixed bitrate with a bounded read-ahead, then seeks.
//!
//! Defaults mirror the numbers from the qBittorrent-based Play now measurements this engine was
//! built to beat: swarm capped at 1.5 MB/s, video at 0.78 MB/s.

use crate::engine::{Engine, EngineConfig, Source};
use crate::metainfo::{Metainfo, build_torrent};
use crate::net::{BindSpec, NetGuard};
use crate::picker::{PickerConfig, PickerMode};
use crate::testkit::{SeedConfig, Seeder, test_data};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub size: u64,
    pub piece_length: u64,
    pub bitrate: f64,
    pub fast_peers: usize,
    /// Total upload of the fast seeders in bytes/s (0 = unlimited).
    pub swarm_rate: u64,
    pub slow_peers: usize,
    pub slow_rate: u64,
    pub stuck_peers: usize,
    pub stuck_for: Duration,
    /// Share of the pieces each fast seeder has (1.0 = all). Below 1 the swarm is patchy.
    pub fast_have: f64,
    /// After playing, wait for the whole file and report how long the download took.
    pub measure_complete: bool,
    pub play_for: Duration,
    pub seek_to: f64,
    pub after_seek: Duration,
    pub startup: Duration,
    pub readahead: Duration,
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig {
            size: 1024 * 1024 * 1024,
            piece_length: 1024 * 1024,
            bitrate: 780_000.0,
            fast_peers: 4,
            swarm_rate: 1_500_000,
            slow_peers: 1,
            slow_rate: 12_000,
            stuck_peers: 1,
            stuck_for: Duration::from_secs(1000),
            fast_have: 1.0,
            measure_complete: false,
            play_for: Duration::from_secs(60),
            seek_to: 0.6,
            after_seek: Duration::from_secs(25),
            startup: Duration::from_secs(2),
            readahead: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PlayResult {
    pub first_byte: f64,
    pub startup: f64,
    pub stalls: u32,
    pub stall_secs: f64,
    pub longest_stall: f64,
    pub gave_up: bool,
}

/// Plays `[start..]` of the stream for `dur` of media time; returns startup + stall numbers.
async fn play(http: SocketAddr, start: u64, cfg: &BenchConfig, dur: Duration) -> std::io::Result<PlayResult> {
    let t0 = Instant::now();
    let mut s = TcpStream::connect(http).await?;
    s.write_all(format!("GET /file/0 HTTP/1.1\r\nHost: bench\r\nRange: bytes={start}-\r\n\r\n").as_bytes()).await?;
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        s.read_exact(&mut b).await?;
        head.push(b[0]);
    }
    let mut r = PlayResult::default();
    let mut received: f64 = 0.0;
    let mut buf = vec![0u8; 64 * 1024];
    let br = cfg.bitrate;
    let need_start = br * cfg.startup.as_secs_f64();
    let mut first = None;
    let mut play_start: Option<Instant> = None;
    let mut stalled_since: Option<Instant> = None;
    let mut stall_total = 0.0;
    let give_up = t0 + dur + Duration::from_secs(600);
    loop {
        let now = Instant::now();
        if now > give_up {
            r.gave_up = true;
            if let Some(since) = stalled_since {
                let d = now.duration_since(since).as_secs_f64();
                stall_total += d;
                r.longest_stall = r.longest_stall.max(d);
            }
            if play_start.is_none() {
                r.startup = now.duration_since(t0).as_secs_f64();
            }
            break;
        }
        // Media position the player has consumed.
        let consumed = match play_start {
            Some(ps) => {
                let playing = now.duration_since(ps).as_secs_f64()
                    - stall_total
                    - stalled_since.map(|s| now.duration_since(s).as_secs_f64()).unwrap_or(0.0);
                (playing * br).max(0.0)
            }
            None => 0.0,
        };
        if let Some(ps) = play_start {
            let played = now.duration_since(ps).as_secs_f64() - stall_total;
            if stalled_since.is_none() && played >= dur.as_secs_f64() {
                break;
            }
        }
        match (play_start, stalled_since) {
            (None, _) if received >= need_start => {
                play_start = Some(now);
                r.startup = now.duration_since(t0).as_secs_f64();
            }
            (Some(_), None) if received < consumed => {
                stalled_since = Some(now);
                r.stalls += 1;
            }
            (Some(_), Some(since)) if received >= consumed + br * 1.0 => {
                let d = now.duration_since(since).as_secs_f64();
                stall_total += d;
                r.longest_stall = r.longest_stall.max(d);
                stalled_since = None;
            }
            _ => {}
        }
        // Bounded read-ahead like a real player.
        if received - consumed > br * cfg.readahead.as_secs_f64() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        match tokio::time::timeout(Duration::from_millis(50), s.read(&mut buf)).await {
            Ok(Ok(0)) if first.is_none() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("stream ended before any data ({})", String::from_utf8_lossy(&head).lines().next().unwrap_or("")),
                ));
            }
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                if first.is_none() {
                    first = Some(now);
                    r.first_byte = now.duration_since(t0).as_secs_f64();
                }
                received += n as f64;
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {}
        }
    }
    r.stall_secs = stall_total;
    Ok(r)
}

/// Which client streams the file.
#[derive(Debug, Clone)]
pub enum Client {
    Playhead(PickerMode),
    /// A running qBittorrent (WebUI API without auth from localhost, e.g. `bench/qbittorrent-test.sh`).
    /// It's driven the way a media server streams from it: sequential download + first/last piece
    /// priority, and a reader that polls `pieceStates` once a second and serves only verified pieces.
    Qbittorrent {
        api: SocketAddr,
    },
}

impl Client {
    pub fn name(&self) -> String {
        match self {
            Client::Playhead(PickerMode::Deadline) => "playhead".into(),
            Client::Playhead(PickerMode::Sequential) => "naive".into(),
            Client::Qbittorrent { .. } => "qbittorrent".into(),
        }
    }
}

/// The scenarios of `playhead bench --suite`.
pub fn suite(base: &BenchConfig) -> Vec<(&'static str, BenchConfig)> {
    let healthy = BenchConfig { slow_peers: 0, stuck_peers: 0, ..base.clone() };
    vec![
        ("healthy", healthy.clone()),
        ("slow-peer", BenchConfig { slow_peers: 1, ..healthy.clone() }),
        ("stuck-peer", BenchConfig { slow_peers: 1, stuck_peers: 1, ..healthy.clone() }),
        ("patchy", BenchConfig { fast_peers: 6, fast_have: 0.6, slow_peers: 1, ..healthy.clone() }),
        ("fast", BenchConfig { swarm_rate: 0, measure_complete: true, ..healthy }),
    ]
}

struct Swarm {
    seeders: Vec<Seeder>,
}

async fn start_swarm(cfg: &BenchConfig, meta: &Arc<Metainfo>, data: &Arc<Vec<u8>>) -> Swarm {
    let lo: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut seeders = Vec::new();
    // Stuck seeders first: they get the first connections and requests, like an unlucky real swarm.
    let all: Vec<u32> = (0..meta.num_pieces()).collect();
    for _ in 0..cfg.stuck_peers {
        let c = SeedConfig { hold: all.clone(), hold_for: cfg.stuck_for, ..Default::default() };
        seeders.push(Seeder::start(lo, meta.clone(), data.clone(), c).await.unwrap());
    }
    for _ in 0..cfg.slow_peers {
        let c = SeedConfig { rate: Some(cfg.slow_rate), ..Default::default() };
        seeders.push(Seeder::start(lo, meta.clone(), data.clone(), c).await.unwrap());
    }
    let per_fast = if cfg.swarm_rate == 0 { None } else { Some(cfg.swarm_rate / cfg.fast_peers.max(1) as u64) };
    let n = meta.num_pieces();
    for k in 0..cfg.fast_peers {
        let have = if cfg.fast_have >= 1.0 {
            None
        } else {
            // Deterministic patchy coverage; every piece stays available somewhere (piece i is
            // always on seeder i % fast_peers).
            let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ (k as u64 + 1);
            Some(
                (0..n)
                    .filter(|&i| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        i as usize % cfg.fast_peers == k || (x % 1000) as f64 / 1000.0 < cfg.fast_have
                    })
                    .collect(),
            )
        };
        let c = SeedConfig { rate: per_fast, have, ..Default::default() };
        seeders.push(Seeder::start(lo, meta.clone(), data.clone(), c).await.unwrap());
    }
    Swarm { seeders }
}

pub async fn run_one(cfg: &BenchConfig, client: &Client, meta: Arc<Metainfo>, data: Arc<Vec<u8>>) -> serde_json::Value {
    let swarm = start_swarm(cfg, &meta, &data).await;
    let peers: Vec<SocketAddr> = swarm.seeders.iter().map(|s| s.addr).collect();
    let t0 = Instant::now();
    let mut stream = match client {
        Client::Playhead(mode) => ClientRun::playhead(*mode, &meta, &peers).await,
        Client::Qbittorrent { api } => match ClientRun::qbittorrent(*api, &meta, &peers).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("qbittorrent: {e}");
                return json!({"client": client.name(), "error": e.to_string()});
            }
        },
    };
    let a = play(stream.http, 0, cfg, cfg.play_for).await.unwrap_or_else(|e| {
        eprintln!("play failed: {e}");
        PlayResult { gave_up: true, ..PlayResult::default() }
    });
    let seek_pos = ((meta.total_length as f64 * cfg.seek_to) as u64 / 2) * 2;
    let b = play(stream.http, seek_pos, cfg, cfg.after_seek).await.unwrap_or_else(|e| {
        eprintln!("seek play failed: {e}");
        PlayResult { gave_up: true, ..PlayResult::default() }
    });
    let complete = if cfg.measure_complete { stream.wait_complete(t0, Duration::from_secs(900)).await } else { None };
    let extra = stream.finish().await;
    drop(swarm);
    json!({
        "client": client.name(),
        "startup_s": round(a.startup),
        "stalls": a.stalls + b.stalls,
        "stall_s": round(a.stall_secs + b.stall_secs),
        "longest_stall_s": round(a.longest_stall.max(b.longest_stall)),
        "seek_first_byte_s": round(b.first_byte),
        "seek_startup_s": round(b.startup),
        "gave_up": a.gave_up || b.gave_up,
        "complete_s": complete.map(round),
        "wasted_mb": extra["wasted_mb"],
        "duplicate_requests": extra["duplicate_requests"],
        "wall_s": round(t0.elapsed().as_secs_f64()),
    })
}

/// One client's stream for one run: an HTTP address the player reads from, and cleanup.
struct ClientRun {
    http: SocketAddr,
    kind: RunKind,
}

enum RunKind {
    Playhead { engine: Engine, srv: tokio::task::JoinHandle<()>, dir: std::path::PathBuf },
    Qbit { api: SocketAddr, hash: String, srv: tokio::task::JoinHandle<()> },
}

impl ClientRun {
    async fn playhead(mode: PickerMode, meta: &Arc<Metainfo>, peers: &[SocketAddr]) -> ClientRun {
        let dir = std::env::temp_dir().join(format!("playhead-bench-{}-{}", std::process::id(), rand::random::<u32>()));
        let guard = NetGuard::manual(BindSpec::Address("127.0.0.1".parse().unwrap()), "127.0.0.1".parse().unwrap());
        let ecfg = EngineConfig {
            out_dir: dir.clone(),
            listen: false,
            dht: false,
            resume: false,
            upload: false,
            extra_peers: peers.to_vec(),
            picker: PickerConfig { mode, ..PickerConfig::default() },
            ..EngineConfig::default()
        };
        let engine = Engine::start(Source::Torrent((**meta).clone()), ecfg, guard);
        engine.select(0).await.unwrap();
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = l.local_addr().unwrap();
        let srv = tokio::spawn(crate::http::serve(engine.clone(), l));
        ClientRun { http, kind: RunKind::Playhead { engine, srv, dir } }
    }

    async fn qbittorrent(api: SocketAddr, meta: &Arc<Metainfo>, peers: &[SocketAddr]) -> std::io::Result<ClientRun> {
        let hash = crate::metainfo::hex(&meta.info_hash);
        let torrent = build_torrent_bytes(meta);
        let boundary = format!("playhead{}", rand::random::<u64>());
        let mut body = Vec::new();
        let mut field = |name: &str, value: &str| {
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
            );
        };
        // What a media server's "play while downloading" sets: in order, first and last pieces early.
        field("sequentialDownload", "true");
        field("firstLastPiecePrio", "true");
        field("stopped", "false");
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; filename=\"bench.torrent\"\r\nContent-Type: application/x-bittorrent\r\n\r\n")
                .as_bytes(),
        );
        body.extend_from_slice(&torrent);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let (st, resp) = api_call(api, "POST", "/api/v2/torrents/add", &format!("multipart/form-data; boundary={boundary}"), &body).await?;
        if st != 200 {
            return Err(std::io::Error::other(format!("add: http {st} {}", String::from_utf8_lossy(&resp))));
        }
        // Wait for it to show up with its save path.
        let mut save_path = None;
        for _ in 0..100 {
            let (_, b) = api_call(api, "GET", &format!("/api/v2/torrents/info?hashes={hash}"), "", &[]).await?;
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b)
                && let Some(t) = v.as_array().and_then(|a| a.first())
            {
                save_path = t["save_path"].as_str().map(str::to_string);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let save_path = save_path.ok_or_else(|| std::io::Error::other("torrent didn't appear in qBittorrent"))?;
        let list: Vec<String> = peers.iter().map(|p| p.to_string()).collect();
        let form = format!("hashes={hash}&peers={}", crate::magnet::percent_encode(list.join("|").as_bytes()));
        api_call(api, "POST", "/api/v2/torrents/addPeers", "application/x-www-form-urlencoded", form.as_bytes()).await?;
        let file = std::path::Path::new(&save_path).join(meta.files[0].display_path());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let http = l.local_addr()?;
        let srv = tokio::spawn(qbit_reader(l, api, hash.clone(), file, meta.clone()));
        Ok(ClientRun { http, kind: RunKind::Qbit { api, hash, srv } })
    }

    async fn wait_complete(&mut self, t0: Instant, max: Duration) -> Option<f64> {
        loop {
            if t0.elapsed() > max {
                return None;
            }
            let done = match &self.kind {
                RunKind::Playhead { engine, .. } => engine.status().await["state"] == "complete",
                RunKind::Qbit { api, hash, .. } => {
                    qbit_info(*api, hash).await.is_some_and(|t| t["progress"].as_f64().unwrap_or(0.0) >= 1.0)
                }
            };
            if done {
                return Some(t0.elapsed().as_secs_f64());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn finish(self) -> serde_json::Value {
        match self.kind {
            RunKind::Playhead { engine, srv, dir } => {
                let st = engine.status().await;
                engine.shutdown().await;
                srv.abort();
                let _ = std::fs::remove_dir_all(&dir);
                json!({"wasted_mb": round(st["wasted_bytes"].as_f64().unwrap_or(0.0) / 1e6), "duplicate_requests": st["duplicate_requests"]})
            }
            RunKind::Qbit { api, hash, srv } => {
                srv.abort();
                let (_, b) = api_call(api, "GET", &format!("/api/v2/torrents/properties?hash={hash}"), "", &[]).await.unwrap_or_default();
                let wasted = serde_json::from_slice::<serde_json::Value>(&b).ok().and_then(|v| v["total_wasted"].as_f64()).unwrap_or(0.0);
                let form = format!("hashes={hash}&deleteFiles=true");
                let _ = api_call(api, "POST", "/api/v2/torrents/delete", "application/x-www-form-urlencoded", form.as_bytes()).await;
                json!({"wasted_mb": round(wasted / 1e6), "duplicate_requests": null})
            }
        }
    }
}

fn build_torrent_bytes(meta: &Metainfo) -> Vec<u8> {
    let mut out = b"d4:info".to_vec();
    out.extend_from_slice(&meta.raw_info);
    out.push(b'e');
    out
}

async fn qbit_info(api: SocketAddr, hash: &str) -> Option<serde_json::Value> {
    let (_, b) = api_call(api, "GET", &format!("/api/v2/torrents/info?hashes={hash}"), "", &[]).await.ok()?;
    serde_json::from_slice::<serde_json::Value>(&b).ok()?.as_array()?.first().cloned()
}

/// Minimal HTTP/1.1 request to the qBittorrent WebUI API.
async fn api_call(api: SocketAddr, method: &str, path: &str, ctype: &str, body: &[u8]) -> std::io::Result<(u16, Vec<u8>)> {
    let mut s = TcpStream::connect(api).await?;
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: {}\r\n", body.len());
    if !ctype.is_empty() {
        req.push_str(&format!("Content-Type: {ctype}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await?;
    s.write_all(body).await?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await?;
    crate::tracker::parse_http_response(&buf)
}

/// The reader a media server puts in front of qBittorrent: piece states polled at most once a second,
/// reads held until the pieces at the read position are verified, then served straight from the file.
async fn qbit_reader(l: tokio::net::TcpListener, api: SocketAddr, hash: String, file: std::path::PathBuf, meta: Arc<Metainfo>) {
    let states: Arc<tokio::sync::Mutex<(Option<Instant>, Vec<u8>)>> = Arc::default();
    loop {
        let Ok((mut s, _)) = l.accept().await else { continue };
        let (states, hash, file, meta) = (states.clone(), hash.clone(), file.clone(), meta.clone());
        tokio::spawn(async move {
            let mut head = Vec::new();
            let mut b = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if s.read_exact(&mut b).await.is_err() {
                    return;
                }
                head.push(b[0]);
            }
            let text = String::from_utf8_lossy(&head).to_string();
            let start: u64 = text
                .lines()
                .find_map(|l| l.to_ascii_lowercase().strip_prefix("range: bytes=").map(|r| r.trim_end_matches('-').to_string()))
                .and_then(|r| r.split('-').next().and_then(|x| x.parse().ok()))
                .unwrap_or(0);
            let size = meta.files[0].length;
            let resp = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{size}\r\nConnection: close\r\n\r\n",
                size - start,
                size - 1
            );
            if s.write_all(resp.as_bytes()).await.is_err() {
                return;
            }
            let (mut rd, mut wr) = s.into_split();
            let mut sink = [0u8; 16];
            let mut fh: Option<tokio::fs::File> = None;
            let mut at = start;
            let mut buf = vec![0u8; 4 * 1024 * 1024];
            while at < size {
                // Readable bytes in a row from `at`.
                let readable = {
                    let mut st = states.lock().await;
                    if st.0.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
                        if let Ok((_, b)) = api_call(api, "GET", &format!("/api/v2/torrents/pieceStates?hash={hash}"), "", &[]).await
                            && let Ok(v) = serde_json::from_slice::<Vec<u8>>(&b)
                        {
                            st.1 = v;
                        }
                        st.0 = Some(Instant::now());
                    }
                    let pl = meta.piece_length;
                    let mut p = (at / pl) as usize;
                    while p < st.1.len() && st.1[p] == 2 {
                        p += 1;
                    }
                    ((p as u64 * pl).min(size)).saturating_sub(at)
                };
                if readable == 0 {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        r = rd.read(&mut sink) => if matches!(r, Ok(0) | Err(_)) { return; },
                    }
                    continue;
                }
                if fh.is_none() {
                    match tokio::fs::File::open(&file).await {
                        Ok(f) => fh = Some(f),
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                    }
                }
                let n = readable.min(buf.len() as u64) as usize;
                let f = fh.as_mut().unwrap();
                if f.seek(std::io::SeekFrom::Start(at)).await.is_err() {
                    return;
                }
                let got = match f.read(&mut buf[..n]).await {
                    Ok(0) | Err(_) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                    Ok(g) => g,
                };
                if wr.write_all(&buf[..got]).await.is_err() {
                    return;
                }
                at += got as u64;
            }
        });
    }
}

fn round(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

pub fn make_swarm(cfg: &BenchConfig) -> (Arc<Metainfo>, Arc<Vec<u8>>) {
    let data = test_data(cfg.size as usize, 0x9e3779b97f4a7c15);
    let t = build_torrent("bench.mkv", &[("bench.mkv".to_string(), data.clone())], cfg.piece_length, None);
    (Arc::new(Metainfo::from_torrent_bytes(&t).unwrap()), Arc::new(data))
}
