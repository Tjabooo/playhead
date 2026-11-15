//! Minimal DNS A-record client over a (tunnel-bound) UDP socket.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;

fn bad(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("dns: {s}"))
}

pub fn build_query(id: u16, host: &str) -> io::Result<Vec<u8>> {
    let mut q = Vec::with_capacity(32 + host.len());
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]); // RD, 1 question
    for label in host.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(bad("bad host name"));
        }
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&[0, 1, 0, 1]); // A, IN
    Ok(q)
}

fn skip_name(b: &[u8], mut i: usize) -> io::Result<usize> {
    loop {
        let len = *b.get(i).ok_or_else(|| bad("truncated name"))? as usize;
        if len == 0 {
            return Ok(i + 1);
        }
        if len & 0xc0 == 0xc0 {
            return Ok(i + 2);
        }
        i += 1 + len;
    }
}

pub fn parse_answer(b: &[u8], id: u16) -> io::Result<Vec<IpAddr>> {
    if b.len() < 12 || u16::from_be_bytes([b[0], b[1]]) != id {
        return Err(bad("unexpected reply"));
    }
    if b[3] & 0x0f != 0 {
        return Err(bad(&format!("server error rcode {}", b[3] & 0x0f)));
    }
    let qd = u16::from_be_bytes([b[4], b[5]]) as usize;
    let an = u16::from_be_bytes([b[6], b[7]]) as usize;
    let mut i = 12;
    for _ in 0..qd {
        i = skip_name(b, i)? + 4;
    }
    let mut out = Vec::new();
    for _ in 0..an {
        i = skip_name(b, i)?;
        let h = b.get(i..i + 10).ok_or_else(|| bad("truncated answer"))?;
        let typ = u16::from_be_bytes([h[0], h[1]]);
        let len = u16::from_be_bytes([h[8], h[9]]) as usize;
        i += 10;
        let data = b.get(i..i + len).ok_or_else(|| bad("truncated rdata"))?;
        if typ == 1 && len == 4 {
            out.push(IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])));
        }
        i += len;
    }
    Ok(out)
}

pub async fn query_a(sock: &UdpSocket, server: SocketAddr, host: &str) -> io::Result<Vec<IpAddr>> {
    let id: u16 = rand::random();
    let q = build_query(id, host)?;
    let mut buf = [0u8; 1500];
    for attempt in 0..2 {
        sock.send_to(&q, server).await?;
        let wait = Duration::from_millis(1500 * (attempt + 1));
        let deadline = tokio::time::Instant::now() + wait;
        while let Ok(r) = tokio::time::timeout_at(deadline, sock.recv_from(&mut buf)).await {
            let (n, from) = r?;
            if from == server
                && let Ok(ips) = parse_answer(&buf[..n], id)
            {
                return Ok(ips);
            }
        }
    }
    Err(io::Error::new(io::ErrorKind::TimedOut, format!("dns: {server} didn't answer for {host}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_reply_with_pointer() {
        let mut r = build_query(7, "example.org").unwrap();
        r[2] = 0x81;
        r[3] = 0x80;
        r[7] = 1; // one answer
        r.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 93, 184, 216, 34]);
        assert_eq!(parse_answer(&r, 7).unwrap(), vec!["93.184.216.34".parse::<IpAddr>().unwrap()]);
        assert!(parse_answer(&r, 8).is_err());
    }
}
