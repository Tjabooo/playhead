//! The engine: one actor task owns the picker, storage and peer table. Peers, trackers, DHT and the
//! listener are separate tasks that talk to it over channels; HTTP readers use the `Engine` handle.

use crate::bitfield::Bitfield;
use crate::filter::FileFilter;
use crate::log;
use crate::magnet::Magnet;
use crate::metainfo::{InfoHash, Metainfo, hex};
use crate::net::{NetGuard, NetState};
use crate::peer::{self, PeerCmd, PeerEvent, PeerKey, PeerSetup};
use crate::picker::{Picker, PickerConfig};
use crate::storage::Storage;
use crate::tracker::{self, Announce, Event};
use crate::wire::{self, Message};
use serde_json::json;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub out_dir: PathBuf,
    /// Incoming port (0 = pick a random one at start; set this to a VPN-forwarded port).
    pub listen_port: u16,
    pub listen: bool,
    pub max_peers: usize,
    pub picker: PickerConfig,
    pub filter: FileFilter,
    pub dht: bool,
    pub extra_trackers: Vec<String>,
    pub extra_peers: Vec<SocketAddr>,
    pub resume: bool,
    pub upload: bool,
    /// Prefetch this many bytes at the file's head and tail when it's selected (players probe both).
    pub prefetch_bytes: u64,
    /// Download cap in bytes/s for this engine (None = unlimited).
    pub download_limit: Option<u64>,
    /// Fetch this much of a selected file's start before anyone reads it, far end first (see
    /// `Picker::set_start_lead`). 0 = off.
    pub start_lead_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            out_dir: PathBuf::from("."),
            listen_port: 0,
            listen: true,
            max_peers: 60,
            picker: PickerConfig::default(),
            filter: FileFilter::default(),
            dht: true,
            extra_trackers: Vec::new(),
            extra_peers: Vec::new(),
            resume: true,
            upload: true,
            prefetch_bytes: 2 * 1024 * 1024,
            download_limit: None,
            start_lead_bytes: 0,
        }
    }
}

pub enum Source {
    Magnet(Magnet),
    Torrent(Metainfo),
}

enum Ctrl {
    Select { file: usize, reply: oneshot::Sender<Result<(), String>> },
    ReaderOpen { id: u64, file: usize, pos: u64 },
    ReaderMove { id: u64, pos: u64 },
    ReaderClose { id: u64 },
    Status { reply: oneshot::Sender<serde_json::Value> },
    Peers { addrs: Vec<SocketAddr>, source: &'static str },
    TrackerDone { idx: usize, result: Result<tracker::AnnounceResult, String> },
    DhtDone { result: Result<Vec<SocketAddr>, String> },
    Incoming { stream: tokio::net::TcpStream, addr: SocketAddr, hs: wire::Handshake },
    External { states: Vec<u8> },
    ExternalBad { index: u32 },
    Shutdown { reply: oneshot::Sender<()> },
}

struct Shared {
    /// Pieces this engine downloaded and verified (in its own files).
    have: Mutex<Bitfield>,
    /// Assist mode: pieces another client already has, read from `external_paths`.
    external: Mutex<Bitfield>,
    external_paths: Mutex<Vec<PathBuf>>,
}

/// Handle used by the HTTP server and the CLI.
#[derive(Clone)]
pub struct Engine {
    ctrl: mpsc::UnboundedSender<Ctrl>,
    meta: watch::Receiver<Option<Arc<Metainfo>>>,
    have_ver: watch::Receiver<u64>,
    shared: Arc<Shared>,
    pub info_hash: InfoHash,
    out_dir: PathBuf,
    filter: FileFilter,
    next_reader: Arc<std::sync::atomic::AtomicU64>,
}

impl Engine {
    pub fn start(source: Source, cfg: EngineConfig, guard: Arc<NetGuard>) -> Engine {
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::channel(4096);
        let (meta_tx, meta_rx) = watch::channel(None);
        let (have_tx, have_rx) = watch::channel(0u64);
        let shared = Arc::new(Shared {
            have: Mutex::new(Bitfield::new(0)),
            external: Mutex::new(Bitfield::new(0)),
            external_paths: Mutex::new(Vec::new()),
        });
        let (info_hash, meta, mut trackers, mut peers) = match source {
            Source::Magnet(m) => (m.info_hash, None, m.trackers, m.peers),
            Source::Torrent(t) => {
                let tr = t.announce.clone();
                (t.info_hash, Some(Arc::new(t)), tr, Vec::new())
            }
        };
        for t in &cfg.extra_trackers {
            if !trackers.contains(t) {
                trackers.push(t.clone());
            }
        }
        peers.extend(cfg.extra_peers.iter().copied());
        let listen_port = if cfg.listen_port == 0 { rand::random_range(20000..60000) } else { cfg.listen_port };
        let handle = Engine {
            ctrl: ctrl_tx.clone(),
            meta: meta_rx,
            have_ver: have_rx,
            shared: shared.clone(),
            info_hash,
            out_dir: cfg.out_dir.clone(),
            filter: cfg.filter.clone(),
            next_reader: Arc::new(1.into()),
        };
        let mut actor = Actor {
            cfg,
            guard,
            info_hash,
            peer_id: wire::new_peer_id(),
            listen_port,
            meta: None,
            meta_tx,
            md: MetadataFetch::default(),
            picker: None,
            storage: None,
            selected: Vec::new(),
            shared,
            have_tx,
            peers: HashMap::new(),
            known: HashMap::new(),
            events_tx,
            ctrl_tx,
            next_key: 1,
            trackers: trackers
                .into_iter()
                .map(|url| TrackerState { url, next: Instant::now(), fails: 0, busy: false, started: false })
                .collect(),
            dht_busy: false,
            dht_next: Instant::now(),
            listener: None,
            killed: false,
            started: Instant::now(),
            downloaded: 0,
            uploaded: 0,
            hash_fails: 0,
            readers: HashMap::new(),
            rate_window: Vec::new(),
            last_choke: Instant::now(),
            optimistic: None,
            completed_at: None,
            pending_external: None,
            external_bad: Default::default(),
            limiter: None,
        };
        actor.limiter = actor.cfg.download_limit.map(|l| Arc::new(peer::RateLimiter::new(l)));
        for p in peers {
            actor.add_known(p);
        }
        if let Some(m) = meta {
            actor.init_metadata(m);
        }
        tokio::spawn(actor.run(ctrl_rx, events_rx));
        handle
    }

    pub async fn metadata(&self) -> Arc<Metainfo> {
        let mut rx = self.meta.clone();
        loop {
            if let Some(m) = rx.borrow().clone() {
                return m;
            }
            if rx.changed().await.is_err() {
                // Engine gone: wait forever (callers time out themselves).
                std::future::pending::<()>().await;
            }
        }
    }

    pub fn metadata_now(&self) -> Option<Arc<Metainfo>> {
        self.meta.borrow().clone()
    }

    pub async fn select(&self, file: usize) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.ctrl.send(Ctrl::Select { file, reply: tx }).map_err(|_| "engine stopped")?;
        rx.await.map_err(|_| "engine stopped".to_string())?
    }

    /// Feed peer addresses from elsewhere (another client, PEX, the CLI).
    pub fn add_peers(&self, addrs: Vec<SocketAddr>) {
        let _ = self.ctrl.send(Ctrl::Peers { addrs, source: "manual" });
    }

    pub fn has_piece(&self, i: u32) -> bool {
        self.has_own(i) || self.has_external(i)
    }

    /// Downloaded and verified by this engine.
    pub fn has_own(&self, i: u32) -> bool {
        self.shared.have.lock().unwrap().get(i)
    }

    /// Available from the other client's file (assist mode).
    pub fn has_external(&self, i: u32) -> bool {
        self.shared.external.lock().unwrap().get(i)
    }

    /// Own pieces as a digit string, `2` = have, `0` = not (same codes as qBittorrent's pieceStates).
    pub fn own_states(&self) -> String {
        let b = self.shared.have.lock().unwrap();
        (0..b.len()).map(|i| if b.get(i) { '2' } else { '0' }).collect()
    }

    /// Assist mode: where the other client keeps the selected file (tried in order).
    pub fn set_external_paths(&self, paths: Vec<PathBuf>) {
        let changed = *self.shared.external_paths.lock().unwrap() != paths;
        *self.shared.external_paths.lock().unwrap() = paths.clone();
        // On Windows the first open and read of a file by a process can take up to a second
        // (on-access scanning). Pay that now rather than on the first seek.
        if changed && let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                use tokio::io::AsyncReadExt;
                for p in paths {
                    if let Ok(mut f) = tokio::fs::File::open(&p).await {
                        let mut b = [0u8; 4096];
                        let _ = f.read(&mut b).await;
                        break;
                    }
                }
            });
        }
    }

    pub fn external_paths(&self) -> Vec<PathBuf> {
        self.shared.external_paths.lock().unwrap().clone()
    }

    /// Assist mode: the other client's piece states (0 = missing, 1 = downloading, 2 = verified).
    /// Verified pieces are read from its file instead of being downloaded; ones it's downloading
    /// are left to it unless a reader needs them next.
    pub fn set_external_states(&self, states: Vec<u8>) {
        let _ = self.ctrl.send(Ctrl::External { states });
    }

    /// A piece read from the other client's file failed its hash check: download it after all.
    pub fn external_bad(&self, i: u32) {
        self.shared.external.lock().unwrap().clear(i);
        let _ = self.ctrl.send(Ctrl::ExternalBad { index: i });
    }

    /// Wait until piece `i` can be read. False if the engine stopped first.
    pub async fn wait_piece(&self, i: u32) -> bool {
        let mut rx = self.have_ver.clone();
        loop {
            if self.has_piece(i) {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    }

    pub fn open_reader(&self, file: usize, pos: u64) -> u64 {
        let id = self.next_reader.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _ = self.ctrl.send(Ctrl::ReaderOpen { id, file, pos });
        id
    }
    pub fn move_reader(&self, id: u64, pos: u64) {
        let _ = self.ctrl.send(Ctrl::ReaderMove { id, pos });
    }
    pub fn close_reader(&self, id: u64) {
        let _ = self.ctrl.send(Ctrl::ReaderClose { id });
    }

    pub async fn status(&self) -> serde_json::Value {
        let (tx, rx) = oneshot::channel();
        if self.ctrl.send(Ctrl::Status { reply: tx }).is_err() {
            return json!({"state": "stopped"});
        }
        rx.await.unwrap_or(json!({"state": "stopped"}))
    }

    pub fn file_path(&self, meta: &Metainfo, file: usize) -> PathBuf {
        meta.file_path(&self.out_dir, file)
    }

    pub fn filter(&self) -> &FileFilter {
        &self.filter
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.ctrl.send(Ctrl::Shutdown { reply: tx }).is_ok() {
            let _ = rx.await;
        }
    }
}

#[derive(Default)]
struct MetadataFetch {
    size: Option<u64>,
    pieces: Vec<Option<Vec<u8>>>,
    asked: HashMap<u32, (PeerKey, Instant)>,
}

struct PeerConn {
    cmd: mpsc::UnboundedSender<PeerCmd>,
    addr: SocketAddr,
    ready: bool,
    incoming: bool,
    ut_metadata: Option<u8>,
    client: Option<String>,
    am_interested: bool,
    am_choking: bool,
    peer_interested: bool,
    /// Whether the peer chokes us (tracked even before we have metadata).
    peer_choking: bool,
    pending_bitfield: Option<Vec<u8>>,
    pending_haves: Vec<u32>,
    uploaded: u64,
    md_rejects: u32,
}

/// Outgoing connections being set up at once, and how long one may take. A peer that answers does so within
/// a second or two even through a VPN; waiting longer only holds a slot that a reachable peer could use.
const MAX_CONNECTING: usize = 40;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);

struct KnownPeer {
    fails: u32,
    next_try: Instant,
    connected: bool,
    banned: bool,
}

struct TrackerState {
    url: String,
    next: Instant,
    fails: u32,
    busy: bool,
    started: bool,
}

struct Actor {
    cfg: EngineConfig,
    guard: Arc<NetGuard>,
    info_hash: InfoHash,
    peer_id: [u8; 20],
    listen_port: u16,
    meta: Option<Arc<Metainfo>>,
    meta_tx: watch::Sender<Option<Arc<Metainfo>>>,
    md: MetadataFetch,
    picker: Option<Picker>,
    storage: Option<Storage>,
    selected: Vec<usize>,
    shared: Arc<Shared>,
    have_tx: watch::Sender<u64>,
    peers: HashMap<PeerKey, PeerConn>,
    known: HashMap<SocketAddr, KnownPeer>,
    events_tx: mpsc::Sender<PeerEvent>,
    ctrl_tx: mpsc::UnboundedSender<Ctrl>,
    next_key: PeerKey,
    trackers: Vec<TrackerState>,
    dht_busy: bool,
    dht_next: Instant,
    listener: Option<tokio::task::JoinHandle<()>>,
    killed: bool,
    started: Instant,
    downloaded: u64,
    uploaded: u64,
    hash_fails: u32,
    readers: HashMap<u64, (usize, u64)>,
    rate_window: Vec<(Instant, u64)>,
    last_choke: Instant,
    optimistic: Option<PeerKey>,
    completed_at: Option<Instant>,
    pending_external: Option<Vec<u8>>,
    external_bad: std::collections::HashSet<u32>,
    limiter: Option<Arc<peer::RateLimiter>>,
}

const METADATA_PIECE: u64 = 16 * 1024;
const MAX_METADATA: u64 = 32 * 1024 * 1024;

impl Actor {
    async fn run(mut self, mut ctrl: mpsc::UnboundedReceiver<Ctrl>, mut events: mpsc::Receiver<PeerEvent>) {
        let mut net = self.guard.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        if net.borrow().is_up() {
            self.start_listener();
        } else {
            self.killed = true;
        }
        log::info(&format!("engine started for {} (listen port {})", hex(&self.info_hash), self.listen_port));
        loop {
            tokio::select! {
                Some(ev) = events.recv() => {
                    self.on_event(ev);
                    // Drain a batch before the next pick so blocks don't wait on the tick.
                    for _ in 0..256 {
                        match events.try_recv() { Ok(ev) => self.on_event(ev), Err(_) => break }
                    }
                    self.request_blocks();
                }
                c = ctrl.recv() => match c {
                    Some(Ctrl::Shutdown { reply }) => { self.shutdown(); let _ = reply.send(()); return; }
                    Some(c) => self.on_ctrl(c),
                    None => { self.shutdown(); return; }
                },
                _ = tick.tick() => self.on_tick(),
                r = net.changed() => {
                    if r.is_err() { self.shutdown(); return; }
                    let s = net.borrow().clone();
                    self.on_net(s);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        for p in self.peers.values() {
            let _ = p.cmd.send(PeerCmd::Close);
        }
        self.peers.clear();
        if let Some(l) = self.listener.take() {
            l.abort();
        }
        if let Some(s) = &mut self.storage {
            s.flush();
        }
    }

    // ---- network guard ----

    fn on_net(&mut self, s: NetState) {
        match s {
            NetState::Down { reason } => {
                if self.killed {
                    return;
                }
                self.killed = true;
                log::warn(&format!("kill switch: closing {} peer connections ({reason})", self.peers.len()));
                let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
                for k in keys {
                    self.drop_peer(k, false);
                }
                if let Some(l) = self.listener.take() {
                    l.abort();
                }
            }
            NetState::Up { .. } => {
                if !self.killed {
                    return;
                }
                self.killed = false;
                log::info("network guard up: resuming");
                self.start_listener();
                for t in &mut self.trackers {
                    t.next = Instant::now();
                    t.fails = 0;
                }
                for k in self.known.values_mut() {
                    k.next_try = Instant::now();
                    k.fails = 0;
                }
                self.dht_next = Instant::now();
            }
        }
    }

    fn start_listener(&mut self) {
        if !self.cfg.listen {
            return;
        }
        let guard = self.guard.clone();
        let port = self.listen_port;
        let info_hash = self.info_hash;
        let ctrl = self.ctrl_tx.clone();
        self.listener = Some(tokio::spawn(async move {
            let listener = match guard.tcp_listen(port).await {
                Ok(l) => l,
                Err(e) => {
                    log::warn(&format!("can't listen on port {port}: {e} (incoming connections off)"));
                    return;
                }
            };
            let mut net = guard.subscribe();
            let opened = net.borrow().clone();
            loop {
                tokio::select! {
                    r = listener.accept() => {
                        let Ok((mut stream, addr)) = r else { continue };
                        let ok = stream.local_addr().map(|l| guard.accept_ok(l)).unwrap_or(false);
                        if !ok { continue; }
                        let ctrl = ctrl.clone();
                        tokio::spawn(async move {
                            let hs = tokio::time::timeout(Duration::from_secs(10), wire::read_handshake(&mut stream)).await;
                            if let Ok(Ok(hs)) = hs && hs.info_hash == info_hash {
                                let _ = ctrl.send(Ctrl::Incoming { stream, addr, hs });
                            }
                        });
                    }
                    _ = net.changed() => {
                        if *net.borrow() != opened { return; }
                    }
                }
            }
        }));
    }

    // ---- peers ----

    fn add_known(&mut self, addr: SocketAddr) {
        if addr.port() == 0 {
            return;
        }
        self.known.entry(addr).or_insert(KnownPeer { fails: 0, next_try: Instant::now(), connected: false, banned: false });
    }

    fn connect_more(&mut self) {
        if self.killed {
            return;
        }
        let active = self.peers.len();
        if active >= self.cfg.max_peers {
            return;
        }
        // Most addresses trackers and DHT hand out don't answer (NAT, gone): try many at once so the reachable
        // ones are found in seconds, not minutes (12 at a time found 2 of ~1000 in a minute through a VPN).
        let connecting = self.peers.values().filter(|p| !p.ready).count();
        let budget = (self.cfg.max_peers - active).min(MAX_CONNECTING.saturating_sub(connecting));
        let now = Instant::now();
        let mut cands: Vec<(SocketAddr, u32)> =
            self.known.iter().filter(|(_, k)| !k.connected && !k.banned && k.next_try <= now).map(|(a, k)| (*a, k.fails)).collect();
        cands.sort_by_key(|(_, f)| *f);
        for (addr, _) in cands.into_iter().take(budget) {
            let k = self.known.get_mut(&addr).unwrap();
            k.connected = true;
            self.spawn_peer(addr, None);
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr, incoming: Option<(tokio::net::TcpStream, wire::Handshake)>) {
        let key = self.next_key;
        self.next_key += 1;
        let (tx, rx) = mpsc::unbounded_channel();
        let is_incoming = incoming.is_some();
        self.peers.insert(
            key,
            PeerConn {
                cmd: tx,
                addr,
                ready: false,
                incoming: is_incoming,
                ut_metadata: None,
                client: None,
                am_interested: false,
                am_choking: true,
                peer_interested: false,
                peer_choking: true,
                pending_bitfield: None,
                pending_haves: Vec::new(),
                uploaded: 0,
                md_rejects: 0,
            },
        );
        let setup = PeerSetup {
            key,
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            metadata_size: self.meta.as_ref().map(|m| m.raw_info.len() as u64),
            guard: self.guard.clone(),
            events: self.events_tx.clone(),
            limiter: self.limiter.clone(),
        };
        let guard = self.guard.clone();
        tokio::spawn(async move {
            match incoming {
                Some((stream, hs)) => peer::run(setup, stream, addr, Some(hs), rx).await,
                None => match guard.tcp_connect(addr, CONNECT_TIMEOUT).await {
                    Ok(stream) => peer::run(setup, stream, addr, None, rx).await,
                    Err(e) => {
                        let _ = setup.events.send(PeerEvent::Closed { key, reason: format!("connect: {e}") }).await;
                    }
                },
            }
        });
    }

    fn send(&self, key: PeerKey, m: Message) {
        if let Some(p) = self.peers.get(&key) {
            let _ = p.cmd.send(PeerCmd::Send(m));
        }
    }

    fn drop_peer(&mut self, key: PeerKey, ban: bool) {
        if let Some(p) = self.peers.remove(&key) {
            let _ = p.cmd.send(PeerCmd::Close);
            if let Some(k) = self.known.get_mut(&p.addr) {
                k.connected = false;
                k.banned |= ban;
            }
        }
        if let Some(pk) = &mut self.picker {
            pk.remove_peer(key);
        }
        for (k, _) in self.md.asked.values_mut() {
            if *k == key {
                *k = 0;
            }
        }
    }

    fn update_interest(&mut self, key: PeerKey) {
        let Some(pk) = &self.picker else { return };
        let want = pk.interesting(key);
        if let Some(p) = self.peers.get_mut(&key)
            && p.ready
            && want != p.am_interested
        {
            p.am_interested = want;
            let _ = p.cmd.send(PeerCmd::Send(if want { Message::Interested } else { Message::NotInterested }));
        }
    }

    fn on_event(&mut self, ev: PeerEvent) {
        let now = Instant::now();
        match ev {
            PeerEvent::Ready { key, addr, peer_id: _, incoming } => {
                let Some(p) = self.peers.get_mut(&key) else { return };
                p.ready = true;
                p.addr = addr;
                if let Some(k) = self.known.get_mut(&addr) {
                    k.fails = 0;
                }
                log::debug(&format!("peer {addr} connected{}", if incoming { " (incoming)" } else { "" }));
                if let Some(pk) = &mut self.picker {
                    pk.add_peer(key, now);
                    if pk.have.count() > 0 {
                        let bf = pk.have.as_bytes().to_vec();
                        self.send(key, Message::Bitfield(bf));
                    }
                }
            }
            PeerEvent::ExtHandshake { key, ut_metadata, metadata_size, client } => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.ut_metadata = ut_metadata;
                    p.client = client;
                }
                if self.meta.is_none()
                    && self.md.size.is_none()
                    && let Some(s) = metadata_size.filter(|&s| s <= MAX_METADATA)
                {
                    self.md.size = Some(s);
                    self.md.pieces = vec![None; s.div_ceil(METADATA_PIECE) as usize];
                }
            }
            PeerEvent::Msg { key, msg } => self.on_message(key, msg, now),
            PeerEvent::MetadataPiece { key, piece, data } => self.on_metadata_piece(key, piece, data),
            PeerEvent::MetadataReject { key, piece } => {
                self.md.asked.remove(&piece);
                if let Some(p) = self.peers.get_mut(&key) {
                    p.md_rejects += 1;
                }
            }
            PeerEvent::Pex { key: _, peers } => {
                let before = self.known.len();
                for a in peers.into_iter().take(200) {
                    if self.guard.bound() && a.is_ipv4() != matches!(self.guard.state(), NetState::Up { addr } if addr.is_ipv4()) {
                        continue;
                    }
                    self.add_known(a);
                }
                if self.known.len() > before {
                    log::debug(&format!("pex: {} new peers", self.known.len() - before));
                    self.connect_more();
                }
            }
            PeerEvent::MetadataRequest { key, piece } => {
                let Some(their) = self.peers.get(&key).and_then(|p| p.ut_metadata) else { return };
                let msg = match &self.meta {
                    Some(m) if (piece as u64) * METADATA_PIECE < m.raw_info.len() as u64 => {
                        let s = piece as usize * METADATA_PIECE as usize;
                        let e = (s + METADATA_PIECE as usize).min(m.raw_info.len());
                        peer::metadata_msg(their, 1, piece, Some(m.raw_info.len() as u64), &m.raw_info[s..e])
                    }
                    _ => peer::metadata_msg(their, 2, piece, None, &[]),
                };
                self.send(key, msg);
            }
            PeerEvent::Closed { key, reason } => {
                if let Some(p) = self.peers.get(&key) {
                    let addr = p.addr;
                    let was_ready = p.ready;
                    log::debug(&format!("peer {addr} closed: {reason}"));
                    if let Some(k) = self.known.get_mut(&addr) {
                        k.connected = false;
                        if !was_ready {
                            k.fails += 1;
                        }
                        let backoff = 15u64.saturating_mul(1 << k.fails.min(6));
                        k.next_try = Instant::now() + Duration::from_secs(backoff);
                    }
                }
                self.peers.remove(&key);
                if let Some(pk) = &mut self.picker {
                    pk.remove_peer(key);
                }
            }
        }
    }

    fn on_message(&mut self, key: PeerKey, msg: Message, now: Instant) {
        match msg {
            Message::Bitfield(b) => {
                if let Some(pk) = &mut self.picker {
                    match Bitfield::from_bytes(&b, pk.num_pieces()) {
                        Some(bf) => pk.peer_bitfield(key, bf),
                        None => return self.drop_peer(key, false),
                    }
                    self.update_interest(key);
                } else if let Some(p) = self.peers.get_mut(&key) {
                    p.pending_bitfield = Some(b);
                }
            }
            Message::Have(i) => {
                if let Some(pk) = &mut self.picker {
                    pk.peer_have(key, i);
                    self.update_interest(key);
                } else if let Some(p) = self.peers.get_mut(&key)
                    && p.pending_haves.len() < 100_000
                {
                    p.pending_haves.push(i);
                }
            }
            Message::Choke => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.peer_choking = true;
                }
                if let Some(pk) = &mut self.picker {
                    pk.peer_choking(key, true, now);
                }
            }
            Message::Unchoke => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.peer_choking = false;
                }
                if let Some(pk) = &mut self.picker {
                    pk.peer_choking(key, false, now);
                }
            }
            Message::Interested => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.peer_interested = true;
                }
            }
            Message::NotInterested => {
                if let Some(p) = self.peers.get_mut(&key) {
                    p.peer_interested = false;
                }
            }
            Message::Piece { index, begin, data } => self.on_block(key, index, begin, data, now),
            Message::Request { index, begin, length } => self.on_request(key, index, begin, length),
            _ => {}
        }
    }

    fn on_block(&mut self, key: PeerKey, index: u32, begin: u32, data: Vec<u8>, now: Instant) {
        let Some(pk) = &mut self.picker else { return };
        let out = pk.block_received(key, index, begin, &data, now);
        if out.useful {
            self.downloaded += data.len() as u64;
            self.rate_window.push((now, data.len() as u64));
        }
        for c in out.cancels {
            self.send(c.peer, Message::Cancel { index: c.index, begin: c.begin, length: c.length });
        }
        let Some(done) = out.completed else { return };
        let meta = self.meta.clone().unwrap();
        let ok = Sha1::digest(&done.data).as_slice() == meta.pieces[done.index as usize];
        if !ok {
            self.hash_fails += 1;
            let pk = self.picker.as_mut().unwrap();
            pk.piece_failed(done.index);
            log::warn(&format!("piece {} failed its hash check ({} peers sent parts)", done.index, done.contributors.len()));
            if done.contributors.len() == 1 {
                self.drop_peer(done.contributors[0], true);
            }
            return;
        }
        if let Some(st) = &mut self.storage
            && let Err(e) = st.write_piece(done.index, &done.data)
        {
            log::error(&format!("disk write failed for piece {}: {e}", done.index));
            self.picker.as_mut().unwrap().piece_failed(done.index);
            return;
        }
        self.piece_done(done.index);
    }

    fn piece_done(&mut self, index: u32) {
        let pk = self.picker.as_mut().unwrap();
        pk.mark_have(index);
        self.shared.have.lock().unwrap().set(index);
        self.have_tx.send_modify(|v| *v += 1);
        let keys: Vec<PeerKey> = self.peers.iter().filter(|(_, p)| p.ready).map(|(k, _)| *k).collect();
        for k in keys {
            self.send(k, Message::Have(index));
            self.update_interest(k);
        }
        self.check_complete();
    }

    fn check_complete(&mut self) {
        if self.completed_at.is_none() && self.picker.as_ref().is_some_and(|p| p.wanted_remaining() == 0) && !self.selected.is_empty() {
            self.completed_at = Some(Instant::now());
            log::info(&format!("selected file(s) complete in {:.1} s", self.started.elapsed().as_secs_f64()));
            if let Some(s) = &mut self.storage {
                s.flush();
            }
            for t in &mut self.trackers {
                t.next = Instant::now();
            }
        }
    }

    fn on_request(&mut self, key: PeerKey, index: u32, begin: u32, length: u32) {
        if !self.cfg.upload || length == 0 || length > 128 * 1024 {
            return;
        }
        let Some(p) = self.peers.get(&key) else { return };
        if p.am_choking {
            return;
        }
        let Some(pk) = &self.picker else { return };
        if !pk.have.get(index) || begin as u64 + length as u64 > pk.piece_size(index) {
            return;
        }
        let Some(st) = &mut self.storage else { return };
        // Pieces are small enough that re-reading them per request is fine for phase 1.
        if let Ok(Some(piece)) = st.read_piece(index) {
            let data = piece[begin as usize..(begin + length) as usize].to_vec();
            self.uploaded += length as u64;
            if let Some(p) = self.peers.get_mut(&key) {
                p.uploaded += length as u64;
            }
            self.send(key, Message::Piece { index, begin, data });
        }
    }

    fn on_metadata_piece(&mut self, key: PeerKey, piece: u32, data: Vec<u8>) {
        if self.meta.is_some() {
            return;
        }
        let Some(size) = self.md.size else { return };
        let idx = piece as usize;
        let expect = (size - (piece as u64 * METADATA_PIECE).min(size)).min(METADATA_PIECE) as usize;
        if idx >= self.md.pieces.len() || data.len() != expect {
            return;
        }
        self.md.asked.remove(&piece);
        self.md.pieces[idx] = Some(data);
        if self.md.pieces.iter().all(Option::is_some) {
            let raw: Vec<u8> = self.md.pieces.iter().flat_map(|p| p.clone().unwrap()).collect();
            let hash: [u8; 20] = Sha1::digest(&raw).into();
            if hash != self.info_hash {
                log::warn("metadata hash mismatch, starting over");
                self.md = MetadataFetch::default();
                self.drop_peer(key, false);
                return;
            }
            match Metainfo::from_info_bytes(&raw) {
                Ok(m) => {
                    log::info(&format!("got metadata: {} ({} files, {} pieces)", m.name, m.files.len(), m.num_pieces()));
                    self.init_metadata(Arc::new(m));
                }
                Err(e) => log::error(&format!("bad metadata: {e}")),
            }
        }
    }

    fn init_metadata(&mut self, meta: Arc<Metainfo>) {
        let mut pk = Picker::new(self.cfg.picker.clone(), meta.piece_length, meta.total_length);
        pk.set_wanted_range(1, 0, (0, meta.total_length)); // nothing until a file is selected
        *self.shared.have.lock().unwrap() = Bitfield::new(meta.num_pieces());
        *self.shared.external.lock().unwrap() = Bitfield::new(meta.num_pieces());
        self.storage = Some(Storage::new(self.cfg.out_dir.clone(), meta.clone()));
        let now = Instant::now();
        let keys: Vec<PeerKey> = self.peers.iter().filter(|(_, p)| p.ready).map(|(k, _)| *k).collect();
        for k in &keys {
            pk.add_peer(*k, now);
            let p = self.peers.get_mut(k).unwrap();
            if let Some(b) = p.pending_bitfield.take()
                && let Some(bf) = Bitfield::from_bytes(&b, meta.num_pieces())
            {
                pk.peer_bitfield(*k, bf);
            }
            for i in std::mem::take(&mut p.pending_haves) {
                pk.peer_have(*k, i);
            }
            pk.peer_choking(*k, p.peer_choking, now);
        }
        self.picker = Some(pk);
        self.meta = Some(meta.clone());
        self.meta_tx.send_replace(Some(meta));
        if let Some(states) = self.pending_external.take() {
            self.on_external(states);
        }
    }

    // ---- control ----

    fn on_ctrl(&mut self, c: Ctrl) {
        match c {
            Ctrl::Select { file, reply } => {
                let _ = reply.send(self.select(file));
            }
            Ctrl::ReaderOpen { id, file, pos } => {
                self.readers.insert(id, (file, pos));
                self.reader_update(id);
            }
            Ctrl::ReaderMove { id, pos } => {
                if let Some(r) = self.readers.get_mut(&id) {
                    r.1 = pos;
                    self.reader_update(id);
                }
            }
            Ctrl::ReaderClose { id } => {
                self.readers.remove(&id);
                if let Some(pk) = &mut self.picker {
                    pk.remove_reader(id);
                }
            }
            Ctrl::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Ctrl::Peers { addrs, source } => {
                let before = self.known.len();
                for a in addrs {
                    self.add_known(a);
                }
                log::debug(&format!("{source}: {} new peers", self.known.len() - before));
                self.connect_more();
            }
            Ctrl::TrackerDone { idx, result } => {
                let t = &mut self.trackers[idx];
                t.busy = false;
                match result {
                    Ok(r) => {
                        t.fails = 0;
                        t.started = true;
                        t.next = Instant::now() + r.interval;
                        log::debug(&format!("tracker {}: {} peers", t.url, r.peers.len()));
                        for a in r.peers {
                            self.add_known(a);
                        }
                        self.connect_more();
                    }
                    Err(e) => {
                        t.fails += 1;
                        t.next = Instant::now() + Duration::from_secs((30u64 << t.fails.min(6)).min(1800));
                        log::debug(&format!("tracker {}: {e}", t.url));
                    }
                }
            }
            Ctrl::DhtDone { result } => {
                self.dht_busy = false;
                let few = self.peers.len() < 15;
                self.dht_next = Instant::now() + Duration::from_secs(if few { 60 } else { 300 });
                match result {
                    Ok(peers) => {
                        log::debug(&format!("dht: {} peers", peers.len()));
                        for a in peers {
                            self.add_known(a);
                        }
                        self.connect_more();
                    }
                    Err(e) => log::debug(&format!("dht: {e}")),
                }
            }
            Ctrl::Incoming { stream, addr, hs } => {
                if self.killed || self.peers.len() >= self.cfg.max_peers + 10 {
                    return;
                }
                if self.known.get(&addr).is_some_and(|k| k.banned)
                    || self.peers.values().any(|p| p.addr.ip() == addr.ip() && p.addr.port() == addr.port())
                {
                    return;
                }
                self.spawn_peer(addr, Some((stream, hs)));
            }
            Ctrl::External { states } => self.on_external(states),
            Ctrl::ExternalBad { index } => {
                self.external_bad.insert(index);
                if let Some(pk) = &mut self.picker
                    && !self.shared.have.lock().unwrap().get(index)
                {
                    log::warn(&format!("piece {index} from the other client's file failed its hash check: downloading it"));
                    pk.unmark_have(index);
                    self.completed_at = None;
                }
            }
            Ctrl::Shutdown { .. } => unreachable!(),
        }
    }

    fn on_external(&mut self, states: Vec<u8>) {
        let Some(pk) = &mut self.picker else {
            self.pending_external = Some(states);
            return;
        };
        let n = pk.num_pieces() as usize;
        let mut cancels = Vec::new();
        let mut newly = 0;
        {
            let own = self.shared.have.lock().unwrap();
            let mut ext = self.shared.external.lock().unwrap();
            for (i, &s) in states.iter().enumerate().take(n) {
                let i = i as u32;
                if s == 2 && !own.get(i) && !ext.get(i) && !self.external_bad.contains(&i) {
                    ext.set(i);
                    cancels.extend(pk.mark_have_elsewhere(i));
                    newly += 1;
                }
                pk.set_avoid(i, s == 1);
            }
        }
        for c in cancels {
            self.send(c.peer, Message::Cancel { index: c.index, begin: c.begin, length: c.length });
        }
        if newly > 0 {
            self.have_tx.send_modify(|v| *v += 1);
            self.check_complete();
        }
    }

    fn select(&mut self, file: usize) -> Result<(), String> {
        let meta = self.meta.clone().ok_or("no metadata yet")?;
        let f = meta.files.get(file).ok_or_else(|| format!("no file #{file} (torrent has {})", meta.files.len()))?;
        if self.cfg.filter.is_blocked(f.name()) {
            return Err(format!("{} is a blocked file type (executable/archive)", f.display_path()));
        }
        let Some((first, last)) = meta.file_pieces(file) else { return Err("file is empty".into()) };
        if self.selected.contains(&file) {
            return Ok(());
        }
        self.selected.push(file);
        let st = self.storage.as_mut().unwrap();
        st.select(file);
        let pk = self.picker.as_mut().unwrap();
        // Union of selected files' pieces.
        let (mut lo, mut hi) = (first, last);
        for &s in &self.selected {
            if let Some((a, b)) = meta.file_pieces(s) {
                lo = lo.min(a);
                hi = hi.max(b);
            }
        }
        // Pieces between two selected files that belong only to unselected files aren't wanted;
        // phase 1 streams one file at a time, so a single range is fine.
        pk.set_wanted_range(lo, hi, (f.offset, f.offset + f.length));
        let pf = self.cfg.prefetch_bytes.min(f.length);
        pk.set_prefetch(vec![(f.offset, f.offset + pf), (f.offset + f.length - pf, f.offset + f.length)]);
        if self.cfg.start_lead_bytes > 0 {
            pk.set_start_lead(Some((f.offset, f.offset + self.cfg.start_lead_bytes.min(f.length))));
        }
        if self.cfg.resume {
            let path = meta.file_path(&self.cfg.out_dir, file);
            if path.exists() {
                let good = st.verify_existing(first..=last);
                if !good.is_empty() {
                    log::info(&format!("resume: {} of {} pieces already on disk", good.len(), last - first + 1));
                }
                for i in good {
                    self.piece_done(i);
                }
            }
        }
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for k in keys {
            self.update_interest(k);
        }
        self.completed_at = None;
        if self.picker.as_ref().unwrap().wanted_remaining() == 0 {
            self.completed_at = Some(Instant::now());
        }
        log::info(&format!("streaming file #{file}: {} ({} MiB)", f.display_path(), f.length / (1024 * 1024)));
        Ok(())
    }

    fn reader_update(&mut self, id: u64) {
        let (Some(meta), Some(pk)) = (&self.meta, &mut self.picker) else { return };
        let (file, pos) = self.readers[&id];
        let Some(f) = meta.files.get(file) else { return };
        let cancels = pk.set_reader(id, f.offset + pos.min(f.length), f.offset + f.length);
        for c in cancels {
            self.send(c.peer, Message::Cancel { index: c.index, begin: c.begin, length: c.length });
        }
        self.request_blocks();
    }

    // ---- periodic ----

    fn request_blocks(&mut self) {
        if self.killed {
            return;
        }
        let Some(pk) = &mut self.picker else { return };
        let reqs = pk.pick(Instant::now());
        for r in reqs {
            self.send(r.peer, Message::Request { index: r.index, begin: r.begin, length: r.length });
        }
    }

    fn on_tick(&mut self) {
        if self.killed {
            return;
        }
        let now = Instant::now();
        self.rate_window.retain(|(t, _)| now.duration_since(*t) < Duration::from_secs(5));
        self.fetch_metadata(now);
        self.request_blocks();
        self.connect_more();
        self.announce(now);
        if self.cfg.dht && !self.dht_busy && now >= self.dht_next && (self.peers.len() < self.cfg.max_peers / 2) {
            self.dht_busy = true;
            let guard = self.guard.clone();
            let ih = self.info_hash;
            let ctrl = self.ctrl_tx.clone();
            tokio::spawn(async move {
                let r = crate::dht::get_peers(&guard, ih, Duration::from_secs(25)).await.map_err(|e| e.to_string());
                let _ = ctrl.send(Ctrl::DhtDone { result: r });
            });
        }
        if now.duration_since(self.last_choke) >= Duration::from_secs(10) {
            self.last_choke = now;
            self.rechoke();
        }
    }

    fn fetch_metadata(&mut self, now: Instant) {
        if self.meta.is_some() || self.md.size.is_none() {
            return;
        }
        self.md.asked.retain(|_, (k, at)| *k != 0 && now.duration_since(*at) < Duration::from_secs(10));
        let mut holders: Vec<(PeerKey, u8)> =
            self.peers.iter().filter(|(_, p)| p.ready && p.md_rejects < 3).filter_map(|(k, p)| p.ut_metadata.map(|id| (*k, id))).collect();
        if holders.is_empty() {
            return;
        }
        holders.sort_by_key(|(k, _)| self.md.asked.values().filter(|(a, _)| a == k).count());
        let missing: Vec<u32> =
            (0..self.md.pieces.len() as u32).filter(|i| self.md.pieces[*i as usize].is_none() && !self.md.asked.contains_key(i)).collect();
        for (n, piece) in missing.into_iter().enumerate() {
            let (key, id) = holders[n % holders.len()];
            self.md.asked.insert(piece, (key, now));
            self.send(key, peer::metadata_msg(id, 0, piece, None, &[]));
        }
    }

    fn left(&self) -> u64 {
        match (&self.meta, &self.picker) {
            (Some(m), Some(pk)) => pk.wanted_remaining() as u64 * m.piece_length,
            _ => 1,
        }
    }

    fn announce(&mut self, now: Instant) {
        let left = self.left();
        for (idx, t) in self.trackers.iter_mut().enumerate() {
            if t.busy || now < t.next {
                continue;
            }
            t.busy = true;
            let event = if !t.started {
                Event::Started
            } else if left == 0 && self.completed_at.is_some() {
                Event::Completed
            } else {
                Event::None
            };
            let a = Announce {
                info_hash: self.info_hash,
                peer_id: self.peer_id,
                port: self.listen_port,
                uploaded: self.uploaded,
                downloaded: self.downloaded,
                left,
                event,
            };
            let url = t.url.clone();
            let guard = self.guard.clone();
            let ctrl = self.ctrl_tx.clone();
            tokio::spawn(async move {
                let r = tracker::announce(&guard, &url, &a).await.map_err(|e| e.to_string());
                let _ = ctrl.send(Ctrl::TrackerDone { idx, result: r });
            });
        }
    }

    /// Upload slots: the 4 interested peers that give us the most, plus one rotating optimistic slot.
    fn rechoke(&mut self) {
        if !self.cfg.upload {
            return;
        }
        let rates: HashMap<PeerKey, f64> =
            self.picker.as_ref().map(|pk| pk.peers.iter().map(|(k, p)| (*k, p.rate)).collect()).unwrap_or_default();
        let mut interested: Vec<PeerKey> = self.peers.iter().filter(|(_, p)| p.ready && p.peer_interested).map(|(k, _)| *k).collect();
        interested.sort_by(|a, b| rates.get(b).unwrap_or(&0.0).total_cmp(rates.get(a).unwrap_or(&0.0)));
        let mut unchoke: Vec<PeerKey> = interested.iter().take(4).copied().collect();
        let rest: Vec<PeerKey> = interested.iter().skip(4).copied().collect();
        if !rest.is_empty() {
            let keep = self.optimistic.filter(|o| rest.contains(o) && rand::random_range(0..3) != 0);
            let pick = keep.unwrap_or_else(|| rest[rand::random_range(0..rest.len())]);
            self.optimistic = Some(pick);
            unchoke.push(pick);
        }
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for k in keys {
            let should = unchoke.contains(&k);
            let p = self.peers.get_mut(&k).unwrap();
            if !p.ready {
                continue;
            }
            if should && p.am_choking {
                p.am_choking = false;
                let _ = p.cmd.send(PeerCmd::Send(Message::Unchoke));
            } else if !should && !p.am_choking {
                p.am_choking = true;
                let _ = p.cmd.send(PeerCmd::Send(Message::Choke));
            }
        }
    }

    fn status(&self) -> serde_json::Value {
        let now = Instant::now();
        let rate: u64 = self.rate_window.iter().map(|(_, b)| *b).sum::<u64>() / 5;
        let net = match self.guard.state() {
            NetState::Up { addr } => json!({"up": true, "address": addr.to_string(), "bound": self.guard.bound()}),
            NetState::Down { reason } => json!({"up": false, "reason": reason, "bound": self.guard.bound()}),
        };
        let mut peers: Vec<serde_json::Value> = Vec::new();
        if let Some(pk) = &self.picker {
            let mut list: Vec<(&PeerKey, &crate::picker::PeerState)> = pk.peers.iter().collect();
            list.sort_by(|a, b| b.1.rate.total_cmp(&a.1.rate));
            for (k, ps) in list.into_iter().take(12) {
                let Some(pc) = self.peers.get(k) else { continue };
                peers.push(json!({
                    "addr": pc.addr.to_string(),
                    "client": pc.client,
                    "rate": ps.rate as u64,
                    "downloaded": ps.downloaded,
                    "outstanding": ps.outstanding(),
                    "choking_us": ps.choking_us,
                    "snubbed": ps.snubbed,
                    "incoming": pc.incoming,
                }));
            }
        }
        let state = if self.killed {
            "killed"
        } else if self.meta.is_none() {
            "metadata"
        } else if self.completed_at.is_some() {
            "complete"
        } else {
            "downloading"
        };
        let files: Vec<serde_json::Value> = self
            .meta
            .as_ref()
            .map(|m| {
                m.files
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        json!({"index": i, "path": f.display_path(), "length": f.length,
                               "blocked": self.cfg.filter.is_blocked(f.name()), "selected": self.selected.contains(&i)})
                    })
                    .collect()
            })
            .unwrap_or_default();
        let readers: Vec<serde_json::Value> = self
            .readers
            .iter()
            .map(|(id, (file, pos))| json!({"id": id, "file": file, "pos": pos, "buffered_bytes": self.buffered_ahead(*file, *pos)}))
            .collect();
        let pk = self.picker.as_ref();
        json!({
            "info_hash": hex(&self.info_hash),
            "name": self.meta.as_ref().map(|m| m.name.clone()),
            "state": state,
            "network": net,
            "listen_port": self.listen_port,
            "uptime_s": now.duration_since(self.started).as_secs_f64(),
            "complete_after_s": self.completed_at.map(|c| c.duration_since(self.started).as_secs_f64()),
            "download_rate": rate,
            "downloaded": self.downloaded,
            "uploaded": self.uploaded,
            "pieces": pk.map(|p| p.num_pieces()),
            "have": pk.map(|p| p.have.count()),
            "wanted_left": pk.map(|p| p.wanted_remaining()),
            "in_progress": pk.map(|p| p.in_progress()),
            "duplicate_requests": pk.map(|p| p.duplicate_requests),
            "wasted_bytes": pk.map(|p| p.wasted_bytes),
            "hash_fails": self.hash_fails,
            "peers_connected": self.peers.values().filter(|p| p.ready).count(),
            "peers_known": self.known.len(),
            "trackers": self.trackers.iter().map(|t| json!({"url": t.url, "ok": t.started && t.fails == 0, "fails": t.fails})).collect::<Vec<_>>(),
            "files": files,
            "readers": readers,
            "top_peers": peers,
        })
    }

    /// Bytes verified in a row from `pos` in `file`.
    fn buffered_ahead(&self, file: usize, pos: u64) -> u64 {
        let (Some(m), Some(pk)) = (&self.meta, &self.picker) else { return 0 };
        let f = &m.files[file];
        let abs = f.offset + pos;
        let end = f.offset + f.length;
        let mut i = m.piece_of(abs);
        let mut upto = abs;
        while upto < end && pk.have.get(i) {
            upto = ((i as u64 + 1) * m.piece_length).min(end);
            i += 1;
        }
        upto - abs
    }
}
