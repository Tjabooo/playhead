//! Peer wire protocol (BEP 3) + extension protocol (BEP 10) framing.

use crate::metainfo::InfoHash;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const BLOCK: u32 = 16 * 1024;
const PROTOCOL: &[u8] = b"BitTorrent protocol";
/// Largest message we accept: a 16 KiB block (+ header) or a bitfield for 64 Mi pieces.
const MAX_MSG: usize = 8 * 1024 * 1024 + 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request { index: u32, begin: u32, length: u32 },
    Piece { index: u32, begin: u32, data: Vec<u8> },
    Cancel { index: u32, begin: u32, length: u32 },
    Port(u16),
    Extended { id: u8, payload: Vec<u8> },
    Unknown(u8),
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        let id: Option<u8> = match self {
            Message::KeepAlive => None,
            Message::Choke => Some(0),
            Message::Unchoke => Some(1),
            Message::Interested => Some(2),
            Message::NotInterested => Some(3),
            Message::Have(i) => {
                body.extend_from_slice(&i.to_be_bytes());
                Some(4)
            }
            Message::Bitfield(b) => {
                body.extend_from_slice(b);
                Some(5)
            }
            Message::Request { index, begin, length } | Message::Cancel { index, begin, length } => {
                body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes());
                body.extend_from_slice(&length.to_be_bytes());
                Some(if matches!(self, Message::Request { .. }) { 6 } else { 8 })
            }
            Message::Piece { index, begin, data } => {
                body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes());
                body.extend_from_slice(data);
                Some(7)
            }
            Message::Port(p) => {
                body.extend_from_slice(&p.to_be_bytes());
                Some(9)
            }
            Message::Extended { id, payload } => {
                body.push(*id);
                body.extend_from_slice(payload);
                Some(20)
            }
            Message::Unknown(id) => Some(*id),
        };
        let mut out = Vec::with_capacity(5 + body.len());
        let len = body.len() as u32 + id.is_some() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        if let Some(id) = id {
            out.push(id);
        }
        out.extend_from_slice(&body);
        out
    }

    pub fn decode(frame: &[u8]) -> io::Result<Message> {
        if frame.is_empty() {
            return Ok(Message::KeepAlive);
        }
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed peer message");
        let u32_at =
            |i: usize| -> io::Result<u32> { frame.get(i..i + 4).map(|b| u32::from_be_bytes(b.try_into().unwrap())).ok_or_else(bad) };
        let body = &frame[1..];
        Ok(match frame[0] {
            0 => Message::Choke,
            1 => Message::Unchoke,
            2 => Message::Interested,
            3 => Message::NotInterested,
            4 => Message::Have(u32_at(1)?),
            5 => Message::Bitfield(body.to_vec()),
            6 | 8 => {
                if body.len() != 12 {
                    return Err(bad());
                }
                let (index, begin, length) = (u32_at(1)?, u32_at(5)?, u32_at(9)?);
                if frame[0] == 6 { Message::Request { index, begin, length } } else { Message::Cancel { index, begin, length } }
            }
            7 => {
                if body.len() < 8 {
                    return Err(bad());
                }
                Message::Piece { index: u32_at(1)?, begin: u32_at(5)?, data: body[8..].to_vec() }
            }
            9 => Message::Port(u16::from_be_bytes(body.get(0..2).ok_or_else(bad)?.try_into().unwrap())),
            20 => Message::Extended { id: *body.first().ok_or_else(bad)?, payload: body.get(1..).unwrap_or(&[]).to_vec() },
            other => Message::Unknown(other),
        })
    }
}

pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Message> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_MSG {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "peer message too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Message::decode(&buf)
}

#[derive(Debug, Clone)]
pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
    pub extensions: bool,
}

pub fn handshake_bytes(info_hash: &InfoHash, peer_id: &[u8; 20]) -> Vec<u8> {
    let mut h = Vec::with_capacity(68);
    h.push(PROTOCOL.len() as u8);
    h.extend_from_slice(PROTOCOL);
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // BEP 10 extension protocol
    h.extend_from_slice(&reserved);
    h.extend_from_slice(info_hash);
    h.extend_from_slice(peer_id);
    h
}

pub async fn read_handshake<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Handshake> {
    let mut buf = [0u8; 68];
    r.read_exact(&mut buf).await?;
    if buf[0] as usize != PROTOCOL.len() || &buf[1..20] != PROTOCOL {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "not a BitTorrent handshake"));
    }
    Ok(Handshake { extensions: buf[25] & 0x10 != 0, info_hash: buf[28..48].try_into().unwrap(), peer_id: buf[48..68].try_into().unwrap() })
}

pub async fn write_all<W: AsyncWrite + Unpin>(w: &mut W, data: &[u8]) -> io::Result<()> {
    w.write_all(data).await
}

pub fn new_peer_id() -> [u8; 20] {
    let mut id = *b"-PH0220-000000000000";
    for b in &mut id[8..] {
        *b = b"0123456789abcdefghijklmnopqrstuvwxyz"[rand::random_range(0..36)];
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_messages() {
        let msgs = [
            Message::Choke,
            Message::Have(7),
            Message::Bitfield(vec![0xf0]),
            Message::Request { index: 1, begin: 16384, length: 16384 },
            Message::Piece { index: 2, begin: 0, data: vec![1, 2, 3] },
            Message::Cancel { index: 1, begin: 0, length: 5 },
            Message::Extended { id: 0, payload: b"d1:md11:ut_metadatai1eee".to_vec() },
        ];
        for m in msgs {
            let enc = m.encode();
            let len = u32::from_be_bytes(enc[..4].try_into().unwrap()) as usize;
            assert_eq!(len, enc.len() - 4);
            assert_eq!(Message::decode(&enc[4..]).unwrap(), m);
        }
        assert_eq!(Message::KeepAlive.encode(), vec![0, 0, 0, 0]);
        assert!(Message::decode(&[6, 0, 0]).is_err());
    }

    #[tokio::test]
    async fn handshake_roundtrip() {
        let hs = handshake_bytes(&[3; 20], &new_peer_id());
        let h = read_handshake(&mut &hs[..]).await.unwrap();
        assert!(h.extensions);
        assert_eq!(h.info_hash, [3; 20]);
        assert!(h.peer_id.starts_with(b"-PH0220-"));
    }
}
