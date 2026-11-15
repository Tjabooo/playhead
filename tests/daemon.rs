//! `playhead serve`: API, auth, assist mode (reading another client's file), cleanup.

use playhead::daemon::{DaemonConfig, run};
use playhead::engine::EngineConfig;
use playhead::metainfo::{Metainfo, build_torrent, hex};
use playhead::net::{BindSpec, NetGuard};
use playhead::testkit::{SeedConfig, Seeder, test_data};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "test-token-0123456789abcdef";
const PIECE: u64 = 64 * 1024;

fn b64(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                out.push(T[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

async fn call(api: SocketAddr, method: &str, path: &str, token: Option<&str>, body: Option<Value>) -> (u16, Vec<u8>) {
    let mut s = tokio::net::TcpStream::connect(api).await.unwrap();
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let auth = token.map(|t| format!("Authorization: Bearer {t}\r\n")).unwrap_or_default();
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: t\r\n{auth}Content-Length: {}\r\n\r\n{body}", body.len()).as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let status = String::from_utf8_lossy(&buf[..end]).split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, buf[end + 4..].to_vec())
}

async fn daemon(cache: &std::path::Path) -> SocketAddr {
    let api = { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap() };
    let cfg = DaemonConfig {
        api,
        token: TOKEN.into(),
        cache_dir: cache.to_path_buf(),
        engine: EngineConfig { dht: false, listen: false, ..EngineConfig::default() },
        idle: Duration::from_secs(600),
    };
    let guard = NetGuard::manual(BindSpec::Unbound, "0.0.0.0".parse().unwrap());
    tokio::spawn(run(cfg, guard));
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(api).await.is_ok() {
            return api;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon didn't start");
}

fn swarm_data(len: usize) -> (Arc<Metainfo>, Arc<Vec<u8>>, Vec<u8>) {
    let data = test_data(len, 99);
    let t = build_torrent("film.mkv", &[("film.mkv".to_string(), data.clone())], PIECE, None);
    (Arc::new(Metainfo::from_torrent_bytes(&t).unwrap()), Arc::new(data), t)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_needs_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let api = daemon(dir.path()).await;
    assert_eq!(call(api, "GET", "/api/health", None, None).await.0, 401);
    assert_eq!(call(api, "GET", "/api/health", Some("wrong-token-0123456789"), None).await.0, 401);
    let (st, body) = call(api, "GET", "/api/health", Some(TOKEN), None).await;
    assert_eq!(st, 200);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["network"]["up"], true);
    assert_eq!(call(api, "GET", "/s/nope/file/0", Some(TOKEN), None).await.0, 404);
}

/// The other client already has the first half: playhead serves that from its file and downloads
/// only the rest. One "verified" piece in the other file is actually corrupt: it's caught and fetched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assist_mode_reads_the_other_clients_file() {
    let (meta, data, torrent) = swarm_data(4_000_000);
    let n = meta.num_pieces() as usize;
    let half = n / 2;
    let other = tempfile::tempdir().unwrap();
    let other_file = other.path().join("film.mkv");
    let mut partial = vec![0u8; data.len()];
    partial[..half * PIECE as usize].copy_from_slice(&data[..half * PIECE as usize]);
    partial[3 * PIECE as usize + 10] ^= 0xff; // piece 3: claimed verified, but corrupt on disk
    std::fs::write(&other_file, &partial).unwrap();
    let states: String = (0..n).map(|i| if i < half { '2' } else { '0' }).collect();

    let seeder = Seeder::start("127.0.0.1:0".parse().unwrap(), meta.clone(), data.clone(), SeedConfig::default()).await.unwrap();
    let cache = tempfile::tempdir().unwrap();
    let api = daemon(cache.path()).await;
    let (st, body) = call(
        api,
        "POST",
        "/api/streams",
        Some(TOKEN),
        Some(json!({"info_hash": hex(&meta.info_hash), "torrent": b64(&torrent), "file": 0, "assist": true,
                    "peers": [seeder.addr.to_string()], "external_paths": [other_file.to_string_lossy()], "states": states})),
    )
    .await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
    let v: Value = serde_json::from_slice(&body).unwrap();
    let url = v["url"].as_str().unwrap().to_string();
    let id = v["id"].as_str().unwrap().to_string();

    let (st, got) = call(api, "GET", &url, Some(TOKEN), None).await;
    assert_eq!(st, 200);
    assert!(got == *data, "streamed bytes differ from the original");
    let sent = seeder.stats.bytes_sent.load(Ordering::Relaxed);
    let missing = (n - half) as u64 * PIECE;
    assert!(sent >= missing - PIECE, "should have downloaded the missing half ({sent})");
    assert!(sent <= missing + 3 * PIECE, "downloaded {sent} bytes: the other client's half was fetched again");

    let (_, body) = call(api, "GET", &format!("/api/streams/{id}"), Some(TOKEN), None).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    let own = v["own"].as_str().unwrap();
    assert_eq!(own.as_bytes()[3], b'2', "the corrupt external piece should have been downloaded");
    assert_eq!(own.as_bytes()[0], b'0', "good external pieces aren't downloaded");

    assert_eq!(call(api, "DELETE", &format!("/api/streams/{id}"), Some(TOKEN), None).await.0, 200);
    assert_eq!(call(api, "GET", &format!("/api/streams/{id}"), Some(TOKEN), None).await.0, 404);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!cache.path().join(hex(&meta.info_hash)).exists(), "cache not cleaned up");
}

/// Stream-only (assist) mode with nothing from the other client: it fetches what's read, not the
/// whole file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assist_mode_fetches_only_what_is_read() {
    let (meta, data, torrent) = swarm_data(16_000_000);
    let seeder = Seeder::start("127.0.0.1:0".parse().unwrap(), meta.clone(), data.clone(), SeedConfig::default()).await.unwrap();
    let cache = tempfile::tempdir().unwrap();
    let api = daemon(cache.path()).await;
    let (st, body) = call(
        api,
        "POST",
        "/api/streams",
        Some(TOKEN),
        Some(json!({"torrent": b64(&torrent), "file": 0, "assist": true, "window_mb": 4, "peers": [seeder.addr.to_string()]})),
    )
    .await;
    assert_eq!(st, 200);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let url = v["url"].as_str().unwrap().to_string();
    // Read 1 MB at 8 MB in (a seek), then idle a moment.
    let mut s = tokio::net::TcpStream::connect(api).await.unwrap();
    s.write_all(format!("GET {url} HTTP/1.1\r\nAuthorization: Bearer {TOKEN}\r\nRange: bytes=8000000-8999999\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    assert_eq!(&buf[end + 4..], &data[8_000_000..9_000_000]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sent = seeder.stats.bytes_sent.load(Ordering::Relaxed);
    // 1 MB read + 4 MB window + 2 MB head/tail prefetch + piece slack, far below the 16 MB file.
    assert!(sent < 9_000_000, "assist mode downloaded {sent} bytes");
}
