use clap::{Args, Parser, Subcommand};
use playhead::engine::{Engine, EngineConfig, Source};
use playhead::filter::FileFilter;
use playhead::magnet::Magnet;
use playhead::metainfo::{Metainfo, build_torrent, hex};
use playhead::net::{BindSpec, NetGuard, NetState};
use playhead::picker::{PickerConfig, PickerMode};
use playhead::{bench, log};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "playhead", version, about = "A BitTorrent engine built for streaming", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
#[group(required = true, multiple = false)]
struct BindArgs {
    /// Bind every socket to this interface (e.g. wg0, wg0-mullvad, or the WireGuard adapter's name).
    #[arg(long, value_name = "NAME")]
    bind_interface: Option<String>,
    /// Bind every socket to this local address (the tunnel's IP).
    #[arg(long, value_name = "IP")]
    bind_address: Option<IpAddr>,
    /// Use the default route, no binding and no kill switch. Must be chosen on purpose.
    #[arg(long)]
    unbound: bool,
}

#[derive(Args, Clone)]
struct NetArgs {
    #[command(flatten)]
    bind: BindArgs,
    /// DNS resolver used through the tunnel for tracker/DHT host names (repeatable).
    /// Default 1.1.1.1 and 9.9.9.9. Use your VPN's resolver if it blocks others (Mullvad: 10.64.0.1),
    /// or `system` for the OS resolver (only private if the OS resolver already is).
    #[arg(long, value_name = "IP[:PORT]")]
    dns: Vec<String>,
}

impl NetArgs {
    fn spec(&self) -> BindSpec {
        if let Some(i) = &self.bind.bind_interface {
            BindSpec::Interface(i.clone())
        } else if let Some(a) = self.bind.bind_address {
            BindSpec::Address(a)
        } else {
            BindSpec::Unbound
        }
    }
    fn dns(&self) -> Vec<SocketAddr> {
        self.dns
            .iter()
            .map(|d| {
                if d == "system" {
                    return playhead::net::SYSTEM_DNS;
                }
                d.parse::<SocketAddr>()
                    .ok()
                    .or_else(|| d.parse::<IpAddr>().ok().map(|ip| SocketAddr::new(ip, 53)))
                    .unwrap_or_else(|| die(&format!("bad --dns {d:?} (IP, IP:PORT or system)")))
            })
            .collect()
    }
    fn guard(&self) -> Arc<NetGuard> {
        match NetGuard::start(self.spec(), self.dns()) {
            Ok(g) => g,
            Err(e) => die(&format!("refusing to start: {e}")),
        }
    }
}

#[derive(Args, Clone)]
struct EngineArgs {
    /// Download folder.
    #[arg(long, short, default_value = ".")]
    out: PathBuf,
    /// Incoming peer port (set it to your VPN's forwarded port). 0 = random.
    #[arg(long, default_value_t = 0)]
    port: u16,
    #[arg(long)]
    no_dht: bool,
    #[arg(long)]
    no_upload: bool,
    /// Extra tracker URL (repeatable).
    #[arg(long, value_name = "URL")]
    tracker: Vec<String>,
    /// Extra peer address (repeatable).
    #[arg(long, value_name = "IP:PORT")]
    peer: Vec<SocketAddr>,
    #[arg(long, default_value_t = 60)]
    max_peers: usize,
    /// Read-ahead window per playhead, in MiB.
    #[arg(long, default_value_t = 32)]
    window_mb: u64,
    #[arg(long, value_parser = ["deadline", "sequential"], default_value = "deadline")]
    picker: String,
    /// Allow an extension that's blocked by default (repeatable), e.g. --allow-ext zip.
    #[arg(long, value_name = "EXT")]
    allow_ext: Vec<String>,
    /// Block another extension (repeatable).
    #[arg(long, value_name = "EXT")]
    block_ext: Vec<String>,
}

impl EngineArgs {
    fn config(&self) -> EngineConfig {
        EngineConfig {
            out_dir: self.out.clone(),
            listen_port: self.port,
            dht: !self.no_dht,
            upload: !self.no_upload,
            extra_trackers: self.tracker.clone(),
            extra_peers: self.peer.clone(),
            max_peers: self.max_peers,
            filter: FileFilter::new(&self.allow_ext, &self.block_ext),
            picker: PickerConfig {
                mode: if self.picker == "sequential" { PickerMode::Sequential } else { PickerMode::Deadline },
                window_bytes: self.window_mb * 1024 * 1024,
                ..PickerConfig::default()
            },
            ..EngineConfig::default()
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Stream one file of a torrent over HTTP (range requests wait for pieces).
    Stream {
        /// Magnet link or .torrent file.
        source: String,
        /// File index (see `playhead files`). Default: the largest allowed file.
        #[arg(long)]
        file: Option<usize>,
        /// HTTP listen address.
        #[arg(long, default_value = "127.0.0.1:8888")]
        http: SocketAddr,
        /// Print a status line every N seconds (0 = never).
        #[arg(long, default_value_t = 5)]
        status_every: u64,
        #[command(flatten)]
        net: NetArgs,
        #[command(flatten)]
        engine: EngineArgs,
    },
    /// Fetch the metadata and list the files.
    Files {
        source: String,
        #[command(flatten)]
        net: NetArgs,
        #[command(flatten)]
        engine: EngineArgs,
    },
    /// Check that traffic really leaves through the bound interface and that nothing gets out when it's down.
    LeakTest {
        #[command(flatten)]
        net: NetArgs,
        /// Plain-HTTP URL that answers with your public IP.
        #[arg(long, default_value = "http://api.ipify.org/")]
        check_url: String,
    },
    /// Build a .torrent from a file (for local tests).
    MakeTorrent {
        file: PathBuf,
        #[arg(long, short)]
        output: PathBuf,
        #[arg(long, default_value_t = 1024)]
        piece_kb: u64,
        #[arg(long)]
        tracker: Option<String>,
    },
    /// Seed a file to the local network for tests (no binding: it's a test peer).
    Seed {
        torrent: PathBuf,
        /// The file's data.
        data: PathBuf,
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: SocketAddr,
        /// Upload cap in KB/s.
        #[arg(long)]
        rate_kb: Option<u64>,
    },
    /// Streaming benchmark on a local swarm: deadline picker vs a naive sequential picker.
    Bench {
        #[arg(long, default_value_t = 1024)]
        size_mb: u64,
        #[arg(long, default_value_t = 1024)]
        piece_kb: u64,
        /// Video bitrate in kB/s.
        #[arg(long, default_value_t = 780)]
        bitrate_kb: u64,
        /// Total upload of the fast seeders in kB/s.
        #[arg(long, default_value_t = 1500)]
        swarm_kb: u64,
        #[arg(long, default_value_t = 4)]
        fast_peers: usize,
        /// The stuck seeder answers requests only after this many seconds (default: never, within a run).
        #[arg(long, default_value_t = 1000)]
        stuck_s: u64,
        #[arg(long, default_value_t = 60)]
        play_s: u64,
        #[arg(long, default_value_t = 1)]
        runs: usize,
        /// Only run one picker.
        #[arg(long, value_parser = ["deadline", "sequential"])]
        only: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

fn die(msg: &str) -> ! {
    eprintln!("playhead: {msg}");
    std::process::exit(2)
}

fn load_source(s: &str) -> Source {
    if s.starts_with("magnet:") {
        Source::Magnet(Magnet::parse(s).unwrap_or_else(|e| die(&e)))
    } else {
        let bytes = std::fs::read(s).unwrap_or_else(|e| die(&format!("{s}: {e}")));
        Source::Torrent(Metainfo::from_torrent_bytes(&bytes).unwrap_or_else(|e| die(&e.to_string())))
    }
}

fn print_files(meta: &Metainfo, filter: &FileFilter) {
    println!("{} ({})", meta.name, hex(&meta.info_hash));
    for (i, f) in meta.files.iter().enumerate() {
        let tag = if filter.is_blocked(f.name()) { "  [blocked]" } else { "" };
        println!("{i:>4}  {:>10.1} MiB  {}{tag}", f.length as f64 / 1048576.0, f.display_path());
    }
}

fn default_file(meta: &Metainfo, filter: &FileFilter) -> Option<usize> {
    meta.files
        .iter()
        .enumerate()
        .filter(|(_, f)| !filter.is_blocked(f.name()) && f.length > 0)
        .max_by_key(|(_, f)| f.length)
        .map(|(i, _)| i)
}

#[tokio::main]
async fn main() {
    log::init_from_env();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Stream { source, file, http, status_every, net, engine } => {
            let guard = net.guard();
            let cfg = engine.config();
            let filter = cfg.filter.clone();
            let e = Engine::start(load_source(&source), cfg, guard);
            log::info("fetching metadata...");
            let meta = e.metadata().await;
            let idx = match file {
                Some(i) => i,
                None => default_file(&meta, &filter).unwrap_or_else(|| die("no streamable file in this torrent")),
            };
            if let Err(err) = e.select(idx).await {
                die(&err);
            }
            let listener = tokio::net::TcpListener::bind(http).await.unwrap_or_else(|err| die(&format!("http {http}: {err}")));
            let name = playhead::magnet::percent_encode(meta.files[idx].name().as_bytes());
            println!("http://{http}/file/{idx}/{name}");
            tokio::spawn(playhead::http::serve(e.clone(), listener));
            let e2 = e.clone();
            if status_every > 0 {
                tokio::spawn(async move {
                    let mut t = tokio::time::interval(Duration::from_secs(status_every));
                    loop {
                        t.tick().await;
                        let s = e2.status().await;
                        let have = s["have"].as_u64().unwrap_or(0);
                        let left = s["wanted_left"].as_u64().unwrap_or(0);
                        let buffered: Vec<String> = s["readers"]
                            .as_array()
                            .map(|r| r.iter().map(|r| format!("{:.1}", r["buffered_bytes"].as_f64().unwrap_or(0.0) / 1048576.0)).collect())
                            .unwrap_or_default();
                        eprintln!(
                            "[{}] {:.2} MB/s, {} peers, {have} pieces ({left} left), readers ahead MiB: [{}], dup {}",
                            s["state"].as_str().unwrap_or("?"),
                            s["download_rate"].as_f64().unwrap_or(0.0) / 1e6,
                            s["peers_connected"],
                            buffered.join(", "),
                            s["duplicate_requests"]
                        );
                    }
                });
            }
            let _ = tokio::signal::ctrl_c().await;
            e.shutdown().await;
        }
        Cmd::Files { source, net, engine } => {
            let guard = net.guard();
            let cfg = engine.config();
            let filter = cfg.filter.clone();
            let e = Engine::start(load_source(&source), cfg, guard);
            match tokio::time::timeout(Duration::from_secs(180), e.metadata()).await {
                Ok(meta) => print_files(&meta, &filter),
                Err(_) => die("no metadata after 3 minutes (no peers?)"),
            }
            e.shutdown().await;
        }
        Cmd::LeakTest { net, check_url } => std::process::exit(leak_test(net, &check_url).await),
        Cmd::MakeTorrent { file, output, piece_kb, tracker } => {
            let data = std::fs::read(&file).unwrap_or_else(|e| die(&format!("{}: {e}", file.display())));
            let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("file").to_string();
            let t = build_torrent(&name, &[(name.clone(), data)], piece_kb * 1024, tracker.as_deref());
            std::fs::write(&output, &t).unwrap_or_else(|e| die(&e.to_string()));
            let m = Metainfo::from_torrent_bytes(&t).unwrap();
            println!("{} {}", hex(&m.info_hash), output.display());
        }
        Cmd::Seed { torrent, data, listen, rate_kb } => {
            let meta = Arc::new(
                Metainfo::from_torrent_bytes(&std::fs::read(&torrent).unwrap_or_else(|e| die(&e.to_string())))
                    .unwrap_or_else(|e| die(&e.to_string())),
            );
            let data = Arc::new(std::fs::read(&data).unwrap_or_else(|e| die(&e.to_string())));
            if data.len() as u64 != meta.total_length {
                die("data size doesn't match the torrent");
            }
            let cfg = playhead::testkit::SeedConfig { rate: rate_kb.map(|k| k * 1000), serve_metadata: true, ..Default::default() };
            let s = playhead::testkit::Seeder::start(listen, meta.clone(), data, cfg).await.unwrap_or_else(|e| die(&e.to_string()));
            println!("seeding {} on {}", hex(&meta.info_hash), s.addr);
            println!("magnet:?xt=urn:btih:{}&x.pe={}", hex(&meta.info_hash), s.addr);
            let _ = tokio::signal::ctrl_c().await;
        }
        Cmd::Bench { size_mb, piece_kb, bitrate_kb, swarm_kb, fast_peers, stuck_s, play_s, runs, only, json } => {
            log::set_level(1);
            let cfg = bench::BenchConfig {
                size: size_mb * 1024 * 1024,
                piece_length: piece_kb * 1024,
                bitrate: bitrate_kb as f64 * 1000.0,
                swarm_rate: swarm_kb * 1000,
                fast_peers,
                stuck_for: Duration::from_secs(stuck_s),
                play_for: Duration::from_secs(play_s),
                ..bench::BenchConfig::default()
            };
            let (meta, data) = bench::make_swarm(&cfg);
            let modes: Vec<PickerMode> = match only.as_deref() {
                Some("deadline") => vec![PickerMode::Deadline],
                Some("sequential") => vec![PickerMode::Sequential],
                _ => vec![PickerMode::Sequential, PickerMode::Deadline],
            };
            let mut results = Vec::new();
            for run in 0..runs {
                for &m in &modes {
                    eprintln!("run {}/{runs}: {m:?} ...", run + 1);
                    let r = bench::run_one(&cfg, m, meta.clone(), data.clone()).await;
                    if !json {
                        println!(
                            "{:<10} startup {:>6.2}s  stalls {:>2} ({:>6.2}s, longest {:>6.2}s)  seek: first byte {:>6.2}s, playing {:>6.2}s  dup {:>4}  wasted {:.1} MB",
                            r["picker"].as_str().unwrap(),
                            r["startup_s"].as_f64().unwrap(),
                            r["stalls"],
                            r["stall_s"].as_f64().unwrap(),
                            r["longest_stall_s"].as_f64().unwrap(),
                            r["seek_first_byte_s"].as_f64().unwrap(),
                            r["seek_startup_s"].as_f64().unwrap(),
                            r["duplicate_requests"],
                            r["wasted_mb"].as_f64().unwrap(),
                        );
                    }
                    results.push(r);
                }
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&results).unwrap());
            }
        }
    }
}

async fn leak_test(net: NetArgs, check_url: &str) -> i32 {
    let spec = net.spec();
    if spec == BindSpec::Unbound {
        eprintln!("leak-test needs --bind-interface or --bind-address");
        return 2;
    }
    let mut failed = false;
    let mut line = |ok: bool, what: &str| {
        println!("{} {what}", if ok { "PASS" } else { "FAIL" });
        failed |= !ok;
    };
    let guard = match NetGuard::start(spec.clone(), net.dns()) {
        Ok(g) => g,
        Err(e) => {
            println!("FAIL bind target missing: {e} (the engine would refuse to start, which is the safe outcome)");
            return 1;
        }
    };
    let addr = match guard.state() {
        NetState::Up { addr } => addr,
        NetState::Down { reason } => {
            println!("FAIL guard down: {reason}");
            return 1;
        }
    };
    line(true, &format!("bound to {addr} ({spec:?})"));
    let host = playhead::tracker::parse_url(check_url).map(|u| u.host).unwrap_or_default();
    match guard.resolve(&host, 80).await {
        Ok(a) => {
            let via = if net.dns().contains(&playhead::net::SYSTEM_DNS) { "the system resolver" } else { "the tunnel" };
            line(true, &format!("DNS via {via}: {host} -> {}", a[0].ip()))
        }
        Err(e) => line(false, &format!("DNS through the tunnel failed: {e} (try --dns with your VPN's resolver)")),
    }
    let tunnel_ip = playhead::tracker::http_get(&guard, check_url, Duration::from_secs(15)).await;
    let unbound = NetGuard::start(BindSpec::Unbound, Vec::new()).unwrap();
    let direct_ip = playhead::tracker::http_get(&unbound, check_url, Duration::from_secs(15)).await;
    let body = |r: &std::io::Result<(u16, Vec<u8>)>| r.as_ref().ok().map(|(_, b)| String::from_utf8_lossy(b).trim().to_string());
    let (t, d) = (body(&tunnel_ip), body(&direct_ip));
    match (&t, &d) {
        (Some(t), Some(d)) if t != d => line(true, &format!("public IP through the tunnel {t}, without it {d}")),
        (Some(t), Some(_)) => println!(
            "WARN public IP {t} with and without the binding: fine if this whole machine is on the VPN, \
             otherwise the binding isn't taking effect"
        ),
        (Some(t), None) => line(true, &format!("public IP through the tunnel {t} (no route outside it: lockdown?)")),
        (None, _) => line(false, &format!("couldn't reach {check_url} through the tunnel: {:?}", tunnel_ip.err())),
    }
    // Kill switch: with the guard down nothing may connect.
    let fake = NetGuard::manual(spec, addr);
    fake.set_state(NetState::Down { reason: "leak test".into() });
    let target: SocketAddr = "1.1.1.1:80".parse().unwrap();
    let tcp = fake.tcp_connect(target, Duration::from_secs(3)).await.is_err();
    let udp = fake.udp_bind(0).await.is_err();
    let dns = fake.resolve("example.org", 80).await.is_err();
    line(tcp && udp && dns, "kill switch: no TCP, UDP or DNS while the tunnel is down");
    if failed { 1 } else { 0 }
}
