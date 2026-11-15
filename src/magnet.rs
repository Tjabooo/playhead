//! Magnet URI parsing (BEP 9): `xt=urn:btih:` (hex or base32), `dn`, `tr`, `x.pe`.

use crate::metainfo::InfoHash;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq)]
pub struct Magnet {
    pub info_hash: InfoHash,
    pub name: Option<String>,
    pub trackers: Vec<String>,
    pub peers: Vec<SocketAddr>,
}

pub fn percent_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let h = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
                match h {
                    Some(v) => {
                        out.push(v);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

pub fn percent_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            s.push(b as char);
        } else {
            s.push_str(&format!("%{b:02X}"));
        }
    }
    s
}

fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut bits: u64 = 0;
    let mut nbits = 0;
    let mut out = Vec::new();
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            b'=' => continue,
            _ => return None,
        } as u64;
        bits = (bits << 5) | v;
        nbits += 5;
        if nbits >= 8 {
            nbits -= 8;
            out.push((bits >> nbits) as u8);
            bits &= (1 << nbits) - 1;
        }
    }
    Some(out)
}

pub fn parse_info_hash(s: &str) -> Option<InfoHash> {
    let bytes = if s.len() == 40 {
        (0..20).map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()).collect::<Option<Vec<u8>>>()?
    } else if s.len() == 32 {
        base32_decode(s)?
    } else {
        return None;
    };
    bytes.try_into().ok()
}

impl Magnet {
    pub fn parse(uri: &str) -> Result<Magnet, String> {
        let rest = uri.strip_prefix("magnet:?").ok_or("not a magnet link")?;
        let mut info_hash = None;
        let mut name = None;
        let mut trackers = Vec::new();
        let mut peers = Vec::new();
        for pair in rest.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let value = String::from_utf8_lossy(&percent_decode(v)).into_owned();
            match k {
                "xt" => {
                    if let Some(h) = value.strip_prefix("urn:btih:") {
                        info_hash = Some(parse_info_hash(h).ok_or("bad btih")?);
                    }
                }
                "dn" => name = Some(value),
                "tr" if !value.is_empty() => {
                    if !trackers.contains(&value) {
                        trackers.push(value)
                    }
                }
                "x.pe" => {
                    if let Ok(a) = value.parse() {
                        peers.push(a)
                    }
                }
                _ => {}
            }
        }
        Ok(Magnet { info_hash: info_hash.ok_or("magnet without urn:btih (v2-only magnets aren't supported)")?, name, trackers, peers })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_magnet() {
        let m = Magnet::parse(
            "magnet:?xt=urn:btih:08ada5a7a6183aae1e09d831df6748d566095a10&dn=Sintel&tr=udp%3A%2F%2Fexplodie.org%3A6969&x.pe=127.0.0.1:6881",
        )
        .unwrap();
        assert_eq!(m.info_hash[0], 0x08);
        assert_eq!(m.name.as_deref(), Some("Sintel"));
        assert_eq!(m.trackers, vec!["udp://explodie.org:6969"]);
        assert_eq!(m.peers.len(), 1);
    }

    #[test]
    fn base32_magnet() {
        // base32 of 0x08ada5a7a6183aae1e09d831df6748d566095a10
        let m = Magnet::parse("magnet:?xt=urn:btih:BCW2LJ5GDA5K4HQJ3AY56Z2I2VTASWQQ").unwrap();
        assert_eq!(crate::metainfo::hex(&m.info_hash), "08ada5a7a6183aae1e09d831df6748d566095a10");
    }

    #[test]
    fn percent() {
        assert_eq!(percent_decode("a%20b%zz%4"), b"a b%zz%4");
        assert_eq!(percent_encode(&[0x12, b'a', 0xff]), "%12a%FF");
    }
}
