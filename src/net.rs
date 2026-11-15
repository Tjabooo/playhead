//! Network guard: every socket the engine opens goes through here.
//!
//! * With `BindSpec::Interface` / `BindSpec::Address`, every TCP/UDP socket is bound to that
//!   interface's address (and on Linux also to the device with `SO_BINDTODEVICE`), so traffic can
//!   only leave through the tunnel. DNS for trackers/DHT bootstrap goes through the tunnel too.
//! * The interface is re-checked twice a second. When it disappears or its address changes, the
//!   state flips to `Down`: no new sockets can be made and every peer/tracker/DHT task watching the
//!   state closes its connection. Traffic resumes only when the same interface is back.
//! * `BindSpec::Unbound` must be chosen explicitly; there's no silent fallback.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindSpec {
    Interface(String),
    Address(IpAddr),
    Unbound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetState {
    Up { addr: IpAddr },
    Down { reason: String },
}

impl NetState {
    pub fn is_up(&self) -> bool {
        matches!(self, NetState::Up { .. })
    }
}

pub struct NetGuard {
    spec: BindSpec,
    tx: watch::Sender<NetState>,
    dns: Vec<SocketAddr>,
    dns_cache: Mutex<Vec<(String, Vec<IpAddr>, Instant)>>,
}

pub const DEFAULT_DNS: &[&str] = &["1.1.1.1:53", "9.9.9.9:53"];

/// Put this in the resolver list to use the system resolver even when bound (`--dns system`). Tracker
/// and DHT host names then go to whatever the OS uses, which is fine when that's private already
/// (a machine fully on the VPN, or a local resolver) and a leak of names otherwise.
pub const SYSTEM_DNS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

fn down(reason: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, format!("network guard: {}", reason.into()))
}

/// Look up the address the spec points at right now.
pub fn resolve_spec(spec: &BindSpec) -> Result<IpAddr, String> {
    match spec {
        BindSpec::Unbound => Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        BindSpec::Interface(name) => {
            let ifs = if_addrs::get_if_addrs().map_err(|e| format!("can't list interfaces: {e}"))?;
            let mine: Vec<_> = ifs.iter().filter(|i| &i.name == name).collect();
            if mine.is_empty() {
                let names: Vec<_> = ifs.iter().map(|i| i.name.clone()).collect();
                return Err(format!("interface {name:?} not found (have: {})", names.join(", ")));
            }
            mine.iter()
                .map(|i| i.ip())
                .find(IpAddr::is_ipv4)
                .or_else(|| mine.first().map(|i| i.ip()))
                .ok_or_else(|| format!("interface {name:?} has no address"))
        }
        BindSpec::Address(ip) => {
            let ifs = if_addrs::get_if_addrs().map_err(|e| format!("can't list interfaces: {e}"))?;
            // 127.0.0.0/8 is all on the loopback interface on Linux, but only 127.0.0.1 is listed.
            if ifs.iter().any(|i| i.ip() == *ip) || (ip.is_loopback() && cfg!(target_os = "linux")) {
                Ok(*ip)
            } else {
                Err(format!("no interface has address {ip}"))
            }
        }
    }
}

impl NetGuard {
    /// Resolve the spec now (fail closed if it's missing) and start the interface monitor.
    pub fn start(spec: BindSpec, dns: Vec<SocketAddr>) -> Result<Arc<Self>, String> {
        let addr = resolve_spec(&spec)?;
        let g = Arc::new(Self::make(spec.clone(), dns, NetState::Up { addr }));
        if spec != BindSpec::Unbound {
            let weak = Arc::downgrade(&g);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(500));
                loop {
                    tick.tick().await;
                    let Some(g) = weak.upgrade() else { break };
                    let spec = g.spec.clone();
                    let now = tokio::task::spawn_blocking(move || resolve_spec(&spec)).await.unwrap_or(Err("probe failed".into()));
                    let cur = g.state();
                    let next = match (now, &cur) {
                        (Ok(a), NetState::Up { addr: old }) if a == *old => continue,
                        // Address changed: go down first so every socket bound to the old one closes.
                        (Ok(_), NetState::Up { .. }) => NetState::Down { reason: "tunnel address changed".into() },
                        (Ok(a), NetState::Down { .. }) => NetState::Up { addr: a },
                        (Err(e), NetState::Up { .. }) => NetState::Down { reason: e },
                        (Err(_), NetState::Down { .. }) => continue,
                    };
                    crate::log::warn(&format!(
                        "network guard: {}",
                        match &next {
                            NetState::Up { addr } => format!("tunnel up again ({addr}), resuming"),
                            NetState::Down { reason } => format!("TUNNEL DOWN ({reason}), all traffic stopped"),
                        }
                    ));
                    g.tx.send_replace(next);
                }
            });
        }
        Ok(g)
    }

    /// A guard whose state is set by hand (tests and the benchmark).
    pub fn manual(spec: BindSpec, addr: IpAddr) -> Arc<Self> {
        Arc::new(Self::make(spec, Vec::new(), NetState::Up { addr }))
    }

    fn make(spec: BindSpec, dns: Vec<SocketAddr>, state: NetState) -> Self {
        let dns = if dns.is_empty() { DEFAULT_DNS.iter().map(|s| s.parse().unwrap()).collect() } else { dns };
        NetGuard { spec, tx: watch::channel(state).0, dns, dns_cache: Mutex::new(Vec::new()) }
    }

    pub fn set_state(&self, s: NetState) {
        self.tx.send_replace(s);
    }

    pub fn spec(&self) -> &BindSpec {
        &self.spec
    }

    pub fn state(&self) -> NetState {
        self.tx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<NetState> {
        self.tx.subscribe()
    }

    pub fn bound(&self) -> bool {
        self.spec != BindSpec::Unbound
    }

    fn local(&self) -> io::Result<IpAddr> {
        match self.state() {
            NetState::Up { addr } => Ok(addr),
            NetState::Down { reason } => Err(down(reason)),
        }
    }

    fn check_family(&self, local: IpAddr, dest: &SocketAddr) -> io::Result<()> {
        if self.bound() && local.is_ipv4() != dest.is_ipv4() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "address family differs from the bound address"));
        }
        Ok(())
    }

    #[allow(unused_variables)]
    fn tcp_socket(&self, local: IpAddr, v4: bool) -> io::Result<TcpSocket> {
        let s = if v4 { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let BindSpec::Interface(name) = &self.spec {
            s.bind_device(Some(name.as_bytes()))?;
        }
        if self.bound() {
            s.bind(SocketAddr::new(local, 0))?;
        }
        Ok(s)
    }

    /// Outgoing TCP connection, bound to the tunnel.
    pub async fn tcp_connect(&self, dest: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
        let local = self.local()?;
        self.check_family(local, &dest)?;
        let sock = self.tcp_socket(local, dest.is_ipv4())?;
        let stream = tokio::time::timeout(timeout, sock.connect(dest))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
        self.verify_local(stream.local_addr()?.ip())?;
        Ok(stream)
    }

    /// Paranoia: refuse a socket that ended up on another address, or one made while going down.
    fn verify_local(&self, got: IpAddr) -> io::Result<()> {
        let now = self.local()?;
        if self.bound() && got != now {
            return Err(down(format!("socket landed on {got}, expected {now}")));
        }
        Ok(())
    }

    pub async fn tcp_listen(&self, port: u16) -> io::Result<TcpListener> {
        let local = self.local()?;
        let sock = self.tcp_socket_unbound(local)?;
        sock.bind(SocketAddr::new(local, port))?;
        sock.listen(64)
    }

    #[allow(unused_variables)]
    fn tcp_socket_unbound(&self, local: IpAddr) -> io::Result<TcpSocket> {
        let s = if local.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let BindSpec::Interface(name) = &self.spec {
            s.bind_device(Some(name.as_bytes()))?;
        }
        s.set_reuseaddr(true)?;
        Ok(s)
    }

    /// Check an accepted connection came in on the tunnel address.
    pub fn accept_ok(&self, local: SocketAddr) -> bool {
        match self.state() {
            NetState::Up { addr } => !self.bound() || local.ip() == addr,
            NetState::Down { .. } => false,
        }
    }

    pub async fn udp_bind(&self, port: u16) -> io::Result<UdpSocket> {
        let local = self.local()?;
        let sock = UdpSocket::bind(SocketAddr::new(local, port)).await?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let BindSpec::Interface(name) = &self.spec {
            sock.bind_device(Some(name.as_bytes()))?;
        }
        self.verify_local(sock.local_addr()?.ip())?;
        Ok(sock)
    }

    /// Resolve a host name. When bound, queries go to the configured resolvers through the tunnel
    /// (never the system resolver, which would leak tracker names outside it).
    pub async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        if !self.bound() || self.dns.contains(&SYSTEM_DNS) {
            return Ok(tokio::net::lookup_host((host, port)).await?.collect());
        }
        {
            let cache = self.dns_cache.lock().unwrap();
            if let Some((_, ips, at)) = cache.iter().find(|(h, _, _)| h == host)
                && at.elapsed() < Duration::from_secs(600)
            {
                return Ok(ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect());
            }
        }
        let mut last = io::Error::new(io::ErrorKind::NotFound, "no resolver answered");
        for server in &self.dns {
            let sock = self.udp_bind(0).await?;
            match crate::dns::query_a(&sock, *server, host).await {
                Ok(ips) if !ips.is_empty() => {
                    let mut cache = self.dns_cache.lock().unwrap();
                    cache.retain(|(h, _, _)| h != host);
                    cache.push((host.to_string(), ips.clone(), Instant::now()));
                    return Ok(ips.into_iter().map(|ip| SocketAddr::new(ip, port)).collect());
                }
                Ok(_) => last = io::Error::new(io::ErrorKind::NotFound, format!("{host}: no A records")),
                Err(e) => last = e,
            }
        }
        Err(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_interface_fails_closed() {
        let e = resolve_spec(&BindSpec::Interface("playhead-nope0".into())).unwrap_err();
        assert!(e.contains("not found"));
        assert!(resolve_spec(&BindSpec::Address("203.0.113.77".parse().unwrap())).is_err());
    }

    #[tokio::test]
    async fn down_guard_refuses_sockets() {
        let g = NetGuard::manual(BindSpec::Address("127.0.0.1".parse().unwrap()), "127.0.0.1".parse().unwrap());
        g.set_state(NetState::Down { reason: "test".into() });
        assert!(g.tcp_connect("127.0.0.1:9".parse().unwrap(), Duration::from_secs(1)).await.is_err());
        assert!(g.udp_bind(0).await.is_err());
        assert!(g.tcp_listen(0).await.is_err());
    }

    #[tokio::test]
    async fn rejects_other_family() {
        let g = NetGuard::manual(BindSpec::Address("127.0.0.1".parse().unwrap()), "127.0.0.1".parse().unwrap());
        let e = g.tcp_connect("[::1]:9".parse().unwrap(), Duration::from_secs(1)).await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);
    }
}
