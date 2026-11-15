//! End-to-end tests on a local swarm: streaming, trackers, metadata, the VPN binding (leak tests),
//! the kill switch and the file blocker.

use playhead::engine::{Engine, EngineConfig, Source};
use playhead::magnet::Magnet;
use playhead::metainfo::{Metainfo, build_torrent, hex};
use playhead::net::{BindSpec, NetGuard, NetState};
use playhead::testkit::{HttpTracker, SeedConfig, Seeder, test_data};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const LO: &str = "127.0.0.1:0";

fn swarm(files: &[(&str, usize)], piece: u64) -> (Arc<Metainfo>, Arc<Vec<u8>>, Vec<Vec<u8>>) {
    let contents: Vec<(String, Vec<u8>)> =
        files.iter().enumerate().map(|(i, (n, len))| (n.to_string(), test_data(*len, 7 + i as u64))).collect();
    let name = if files.len() == 1 { files[0].0 } else { "pack" };
    let t = build_torrent(name, &contents, piece, None);
    let meta = Arc::new(Metainfo::from_torrent_bytes(&t).unwrap());
    let all: Vec<u8> = contents.iter().flat_map(|c| c.1.clone()).collect();
    (meta, Arc::new(all), contents.into_iter().map(|c| c.1).collect())
}

fn cfg(dir: &std::path::Path, peers: Vec<SocketAddr>) -> EngineConfig {
    EngineConfig { out_dir: dir.to_path_buf(), dht: false, listen: false, extra_peers: peers, ..EngineConfig::default() }
}

async fn http_get(addr: SocketAddr, path: &str, range: Option<&str>) -> (u16, String, Vec<u8>) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let r = range.map(|r| format!("Range: {r}\r\n")).unwrap_or_default();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: t\r\n{r}\r\n").as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&buf[..end]).to_string();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, head, buf[end + 4..].to_vec())
}

async fn serve(engine: &Engine) -> SocketAddr {
    let l = tokio::net::TcpListener::bind(LO).await.unwrap();
    let a = l.local_addr().unwrap();
    tokio::spawn(playhead::http::serve(engine.clone(), l));
    a
}

async fn wait_complete(engine: &Engine, secs: u64) {
    let t = Instant::now();
    loop {
        let s = engine.status().await;
        if s["state"] == "complete" {
            return;
        }
        assert!(t.elapsed() < Duration::from_secs(secs), "not complete in {secs}s: {s}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_metadata_and_range_streaming() {
    let (meta, data, files) = swarm(&[("film.mkv", 3_000_000)], 64 * 1024);
    let s1 = Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { serve_metadata: true, ..Default::default() })
        .await
        .unwrap();
    let s2 = Seeder::start(
        LO.parse().unwrap(),
        meta.clone(),
        data.clone(),
        SeedConfig { serve_metadata: true, rate: Some(400_000), ..Default::default() },
    )
    .await
    .unwrap();
    let magnet = Magnet::parse(&format!("magnet:?xt=urn:btih:{}&x.pe={}&x.pe={}", hex(&meta.info_hash), s1.addr, s2.addr)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let guard = NetGuard::manual(BindSpec::Unbound, "0.0.0.0".parse().unwrap());
    let engine = Engine::start(Source::Magnet(magnet), cfg(dir.path(), vec![]), guard);
    let got = tokio::time::timeout(Duration::from_secs(20), engine.metadata()).await.expect("metadata via ut_metadata");
    assert_eq!(got.info_hash, meta.info_hash);
    let http = serve(&engine).await;

    // Seek first: a range in the middle, before anything else.
    let (st, head, body) = http_get(http, "/file/0/film.mkv", Some("bytes=2000000-2100000")).await;
    assert_eq!(st, 206);
    assert!(head.contains("Content-Range: bytes 2000000-2100000/3000000"), "{head}");
    assert_eq!(body, files[0][2_000_000..=2_100_000]);
    // Suffix range (players probe the tail).
    let (st, _, body) = http_get(http, "/file/0", Some("bytes=-1000")).await;
    assert_eq!(st, 206);
    assert_eq!(body, files[0][3_000_000 - 1000..]);
    // Whole file.
    let (st, _, body) = http_get(http, "/stream", None).await;
    assert_eq!(st, 200);
    assert_eq!(body, files[0]);
    let (st, _, _) = http_get(http, "/file/0", Some("bytes=3000000-")).await;
    assert_eq!(st, 416);
    wait_complete(&engine, 20).await;
    assert_eq!(std::fs::read(dir.path().join("film.mkv")).unwrap(), files[0]);
    engine.shutdown().await;
}

/// Leak test: the engine is bound to 127.0.0.2; every connection that reaches the seeders and both
/// trackers must come from exactly that address.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_socket_uses_the_bound_address() {
    use playhead::testkit::UdpTracker;
    use std::net::IpAddr;
    let (meta, data, files) = swarm(&[("film.mkv", 2_000_000)], 64 * 1024);
    let s1 = Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { serve_metadata: true, ..Default::default() })
        .await
        .unwrap();
    let s2 = Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { serve_metadata: true, ..Default::default() })
        .await
        .unwrap();
    let ht = HttpTracker::start(LO.parse().unwrap(), vec![s1.addr]).await.unwrap();
    let ut = UdpTracker::start(LO.parse().unwrap(), vec![s2.addr]).await.unwrap();
    // No x.pe: peers only come from the trackers.
    let magnet = Magnet::parse(&format!(
        "magnet:?xt=urn:btih:{}&tr={}&tr={}",
        hex(&meta.info_hash),
        playhead::magnet::percent_encode(ht.url.as_bytes()),
        playhead::magnet::percent_encode(ut.url.as_bytes())
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let bound: IpAddr = "127.0.0.2".parse().unwrap();
    let guard = NetGuard::start(BindSpec::Address(bound), vec![]).unwrap();
    let engine = Engine::start(Source::Magnet(magnet), cfg(dir.path(), vec![]), guard);
    tokio::time::timeout(Duration::from_secs(20), engine.metadata()).await.expect("metadata");
    engine.select(0).await.unwrap();
    wait_complete(&engine, 30).await;
    assert_eq!(std::fs::read(dir.path().join("film.mkv")).unwrap(), files[0]);
    for (what, st) in [("seeder 1", &s1.stats), ("seeder 2", &s2.stats), ("http tracker", &ht.stats), ("udp tracker", &ut.stats)] {
        let src = st.sources();
        assert!(!src.is_empty(), "{what} was never contacted");
        assert!(src.iter().all(|ip| *ip == bound), "{what} saw traffic from outside the tunnel: {src:?}");
    }
    engine.shutdown().await;
}

/// Kill switch: when the guard goes down, every peer connection closes, nothing reconnects and no
/// tracker is contacted until it's back; then the download finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_switch_stops_all_traffic() {
    let (meta, data, files) = swarm(&[("film.mkv", 4_000_000)], 64 * 1024);
    let seeders = [
        Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { rate: Some(300_000), ..Default::default() })
            .await
            .unwrap(),
        Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { rate: Some(300_000), ..Default::default() })
            .await
            .unwrap(),
    ];
    let ht = HttpTracker::start(LO.parse().unwrap(), seeders.iter().map(|s| s.addr).collect()).await.unwrap();
    let mut t = (*meta).clone();
    t.announce = vec![ht.url.clone()];
    let dir = tempfile::tempdir().unwrap();
    let guard = NetGuard::manual(BindSpec::Address("127.0.0.1".parse().unwrap()), "127.0.0.1".parse().unwrap());
    let engine = Engine::start(Source::Torrent(t), cfg(dir.path(), vec![]), guard.clone());
    engine.select(0).await.unwrap();
    // Let it get going.
    let t0 = Instant::now();
    while engine.status().await["have"].as_u64().unwrap_or(0) < 5 {
        assert!(t0.elapsed() < Duration::from_secs(15));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    guard.set_state(NetState::Down { reason: "test: tunnel gone".into() });
    let t1 = Instant::now();
    while seeders.iter().any(|s| s.stats.active.load(Ordering::Relaxed) > 0) {
        assert!(t1.elapsed() < Duration::from_millis(1500), "connections still open after the kill switch");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let conns: Vec<u64> = seeders.iter().map(|s| s.stats.connections.load(Ordering::Relaxed)).collect();
    let announces = ht.stats.connections.load(Ordering::Relaxed);
    let have = engine.status().await["have"].as_u64().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let st = engine.status().await;
    assert_eq!(st["state"], "killed");
    assert_eq!(st["have"].as_u64().unwrap(), have, "pieces arrived while killed");
    assert_eq!(seeders.iter().map(|s| s.stats.connections.load(Ordering::Relaxed)).collect::<Vec<_>>(), conns, "reconnected while killed");
    assert_eq!(ht.stats.connections.load(Ordering::Relaxed), announces, "announced while killed");
    assert!(st["wanted_left"].as_u64().unwrap() > 0);

    guard.set_state(NetState::Up { addr: "127.0.0.1".parse().unwrap() });
    wait_complete(&engine, 30).await;
    assert_eq!(std::fs::read(dir.path().join("film.mkv")).unwrap(), files[0]);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executables_are_never_fetched_or_served() {
    let piece = 64 * 1024;
    let (meta, data, files) = swarm(&[("Show.S01E01.mkv", 1_000_000), ("Show.S01E01.mkv.exe", 2_000_000)], piece);
    let s = Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig::default()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let guard = NetGuard::manual(BindSpec::Unbound, "0.0.0.0".parse().unwrap());
    let engine = Engine::start(Source::Torrent((*meta).clone()), cfg(dir.path(), vec![s.addr]), guard);
    assert!(engine.select(1).await.unwrap_err().contains("blocked"));
    let http = serve(&engine).await;
    let (st, _, _) = http_get(http, "/file/1", None).await;
    assert_eq!(st, 403);
    let (st, _, body) = http_get(http, "/file/0", None).await;
    assert_eq!(st, 200);
    assert_eq!(body, files[0]);
    wait_complete(&engine, 20).await;
    assert!(!dir.path().join("pack/Show.S01E01.mkv.exe").exists());
    // Only the mkv's pieces (+ the one it shares with the exe) were requested.
    let sent = s.stats.bytes_sent.load(Ordering::Relaxed);
    assert!(sent <= 1_000_000 + piece, "downloaded {sent} bytes, the exe was fetched");
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_peer_does_not_stall_the_start() {
    let (meta, data, files) = swarm(&[("film.mkv", 16_000_000)], 256 * 1024);
    let all: Vec<u32> = (0..meta.num_pieces()).collect();
    // The stuck peer is listed first so it's connected first and gets the first requests.
    let stuck = Seeder::start(
        LO.parse().unwrap(),
        meta.clone(),
        data.clone(),
        SeedConfig { hold: all, hold_for: Duration::from_secs(120), ..Default::default() },
    )
    .await
    .unwrap();
    let fast = Seeder::start(LO.parse().unwrap(), meta.clone(), data.clone(), SeedConfig { rate: Some(2_000_000), ..Default::default() })
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let guard = NetGuard::manual(BindSpec::Unbound, "0.0.0.0".parse().unwrap());
    let engine = Engine::start(Source::Torrent((*meta).clone()), cfg(dir.path(), vec![stuck.addr, fast.addr]), guard);
    let http = serve(&engine).await;
    let t = Instant::now();
    let (st, _, body) = http_get(http, "/file/0", Some("bytes=0-1999999")).await;
    let took = t.elapsed();
    assert_eq!(st, 206);
    assert_eq!(body, files[0][..2_000_000]);
    assert!(took < Duration::from_secs(12), "first 2 MB took {took:?} with a stuck peer");
    let s = engine.status().await;
    assert!(s["duplicate_requests"].as_u64().unwrap() > 0, "{s}");
    engine.shutdown().await;
}

#[test]
fn missing_interface_refuses_to_start() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        assert!(NetGuard::start(BindSpec::Interface("playhead-test-nope0".into()), vec![]).is_err());
    });
}
