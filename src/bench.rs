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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub size: u64,
    pub piece_length: u64,
    pub bitrate: f64,
    pub fast_peers: usize,
    pub swarm_rate: u64,
    pub slow_rate: u64,
    pub stuck_for: Duration,
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
            slow_rate: 12_000,
            stuck_for: Duration::from_secs(1000),
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
    loop {
        let now = Instant::now();
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

pub async fn run_one(cfg: &BenchConfig, mode: PickerMode, meta: Arc<Metainfo>, data: Arc<Vec<u8>>) -> serde_json::Value {
    let lo: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let per_fast = cfg.swarm_rate / cfg.fast_peers.max(1) as u64;
    let mut seeders = Vec::new();
    for _ in 0..cfg.fast_peers {
        seeders
            .push(Seeder::start(lo, meta.clone(), data.clone(), SeedConfig { rate: Some(per_fast), ..Default::default() }).await.unwrap());
    }
    seeders
        .push(Seeder::start(lo, meta.clone(), data.clone(), SeedConfig { rate: Some(cfg.slow_rate), ..Default::default() }).await.unwrap());
    let all: Vec<u32> = (0..meta.num_pieces()).collect();
    seeders.push(
        Seeder::start(lo, meta.clone(), data.clone(), SeedConfig { hold: all, hold_for: cfg.stuck_for, ..Default::default() })
            .await
            .unwrap(),
    );
    let dir = std::env::temp_dir().join(format!("playhead-bench-{}-{}", std::process::id(), rand::random::<u32>()));
    let guard = NetGuard::manual(BindSpec::Address("127.0.0.1".parse().unwrap()), "127.0.0.1".parse().unwrap());
    let ecfg = EngineConfig {
        out_dir: dir.clone(),
        listen: false,
        dht: false,
        resume: false,
        upload: false,
        // The stuck seeder gets the first connection, like an unlucky real swarm.
        extra_peers: seeders.iter().rev().map(|s| s.addr).collect(),
        picker: PickerConfig { mode, ..PickerConfig::default() },
        ..EngineConfig::default()
    };
    let engine = Engine::start(Source::Torrent((*meta).clone()), ecfg, guard);
    engine.select(0).await.unwrap();
    let http = tokio::net::TcpListener::bind(lo).await.unwrap();
    let haddr = http.local_addr().unwrap();
    let srv = tokio::spawn(crate::http::serve(engine.clone(), http));

    let t0 = Instant::now();
    let a = play(haddr, 0, cfg, cfg.play_for).await.unwrap_or_else(|e| {
        eprintln!("play failed: {e}");
        PlayResult::default()
    });
    let seek_pos = ((meta.total_length as f64 * cfg.seek_to) as u64 / 2) * 2;
    let b = play(haddr, seek_pos, cfg, cfg.after_seek).await.unwrap_or_else(|e| {
        eprintln!("seek play failed: {e}");
        PlayResult::default()
    });
    let st = engine.status().await;
    engine.shutdown().await;
    srv.abort();
    drop(seeders);
    let _ = std::fs::remove_dir_all(&dir);
    json!({
        "picker": format!("{mode:?}").to_lowercase(),
        "startup_s": round(a.startup),
        "stalls": a.stalls + b.stalls,
        "stall_s": round(a.stall_secs + b.stall_secs),
        "longest_stall_s": round(a.longest_stall.max(b.longest_stall)),
        "seek_first_byte_s": round(b.first_byte),
        "seek_startup_s": round(b.startup),
        "duplicate_requests": st["duplicate_requests"],
        "wasted_mb": round(st["wasted_bytes"].as_f64().unwrap_or(0.0) / 1e6),
        "wall_s": round(t0.elapsed().as_secs_f64()),
    })
}

fn round(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

pub fn make_swarm(cfg: &BenchConfig) -> (Arc<Metainfo>, Arc<Vec<u8>>) {
    let data = test_data(cfg.size as usize, 0x9e3779b97f4a7c15);
    let t = build_torrent("bench.mkv", &[("bench.mkv".to_string(), data.clone())], cfg.piece_length, None);
    (Arc::new(Metainfo::from_torrent_bytes(&t).unwrap()), Arc::new(data))
}
