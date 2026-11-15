//! Torrent metainfo (BEP 3, v1 only): info dict parsing, file layout, info hash.

use crate::bencode::{self, Value};
use sha1::{Digest, Sha1};
use std::path::PathBuf;

pub type InfoHash = [u8; 20];

#[derive(Debug, Clone)]
pub struct FileEntry {
    /// Sanitized path components (relative to the torrent's root folder).
    pub path: Vec<String>,
    pub length: u64,
    /// Offset of the file's first byte within the torrent's byte stream.
    pub offset: u64,
}

impl FileEntry {
    pub fn display_path(&self) -> String {
        self.path.join("/")
    }
    pub fn name(&self) -> &str {
        self.path.last().map(String::as_str).unwrap_or("")
    }
}

#[derive(Debug, Clone)]
pub struct Metainfo {
    pub info_hash: InfoHash,
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileEntry>,
    pub total_length: u64,
    /// True for a single-file torrent (file lives directly at `<out>/<name>`).
    pub single_file: bool,
    pub raw_info: Vec<u8>,
    pub announce: Vec<String>,
}

#[derive(Debug)]
pub struct Error(pub String);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metainfo: {}", self.0)
    }
}
impl std::error::Error for Error {}
impl From<bencode::Error> for Error {
    fn from(e: bencode::Error) -> Self {
        Error(e.to_string())
    }
}

fn err<T>(s: &str) -> Result<T, Error> {
    Err(Error(s.into()))
}

/// Make one path component safe: no separators, no `..`, no Windows-reserved names or characters.
pub fn sanitize_component(raw: &str) -> String {
    let mut s: String = raw
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    s = s.trim_end_matches([' ', '.']).to_string();
    if s.is_empty() || s == "." || s == ".." {
        return "_".into();
    }
    let stem = s.split('.').next().unwrap_or("").to_ascii_uppercase();
    const RESERVED: &[&str] = &["CON", "PRN", "AUX", "NUL"];
    let reserved = RESERVED.contains(&stem.as_str())
        || ((stem.starts_with("COM") || stem.starts_with("LPT")) && stem.len() == 4 && stem.as_bytes()[3].is_ascii_digit());
    if reserved {
        s.insert(0, '_');
    }
    if s.len() > 200 {
        let mut cut = 200;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    s
}

impl Metainfo {
    /// Parse a `.torrent` file.
    pub fn from_torrent_bytes(data: &[u8]) -> Result<Self, Error> {
        let (root, span) = bencode::decode_with_info_span(data)?;
        let (s, e) = span.ok_or_else(|| Error("no info dict".into()))?;
        let mut m = Self::from_info_bytes(&data[s..e])?;
        if let Some(a) = root.get("announce").and_then(Value::as_str) {
            m.announce.push(a.to_string());
        }
        if let Some(tiers) = root.get("announce-list").and_then(Value::as_list) {
            for tier in tiers {
                for t in tier.as_list().unwrap_or(&[]) {
                    if let Some(u) = t.as_str()
                        && !m.announce.iter().any(|x| x == u)
                    {
                        m.announce.push(u.to_string());
                    }
                }
            }
        }
        Ok(m)
    }

    /// Parse a raw bencoded info dict (as fetched with ut_metadata).
    pub fn from_info_bytes(raw: &[u8]) -> Result<Self, Error> {
        let info = bencode::decode(raw)?;
        let info_hash: InfoHash = Sha1::digest(raw).into();
        let name_raw =
            info.get("name.utf-8").or_else(|| info.get("name")).and_then(Value::as_bytes).ok_or_else(|| Error("no name".into()))?;
        let name = sanitize_component(&String::from_utf8_lossy(name_raw));
        let piece_length = info.get("piece length").and_then(Value::as_int).unwrap_or(0);
        if !(16 * 1024..=64 * 1024 * 1024).contains(&piece_length) {
            return err("piece length out of range");
        }
        let piece_length = piece_length as u64;
        let pieces_raw = info.get("pieces").and_then(Value::as_bytes).ok_or_else(|| Error("no pieces".into()))?;
        if pieces_raw.is_empty() || pieces_raw.len() % 20 != 0 {
            return err("bad pieces field");
        }
        let pieces: Vec<[u8; 20]> = pieces_raw.chunks(20).map(|c| c.try_into().unwrap()).collect();

        let mut files = Vec::new();
        let mut offset = 0u64;
        let single_file;
        if let Some(len) = info.get("length").and_then(Value::as_int) {
            if len < 0 {
                return err("negative length");
            }
            single_file = true;
            files.push(FileEntry { path: vec![name.clone()], length: len as u64, offset: 0 });
            offset = len as u64;
        } else if let Some(list) = info.get("files").and_then(Value::as_list) {
            single_file = false;
            for f in list {
                let len = f.get("length").and_then(Value::as_int).ok_or_else(|| Error("file without length".into()))?;
                if len < 0 {
                    return err("negative length");
                }
                let parts = f
                    .get("path.utf-8")
                    .or_else(|| f.get("path"))
                    .and_then(Value::as_list)
                    .ok_or_else(|| Error("file without path".into()))?;
                let path: Vec<String> =
                    parts.iter().filter_map(Value::as_bytes).map(|b| sanitize_component(&String::from_utf8_lossy(b))).collect();
                if path.is_empty() {
                    return err("empty path");
                }
                // BEP 47 padding files carry no data worth keeping, but they occupy byte ranges.
                files.push(FileEntry { path, length: len as u64, offset });
                offset = offset.checked_add(len as u64).ok_or_else(|| Error("size overflow".into()))?;
            }
            if files.is_empty() {
                return err("no files");
            }
        } else {
            return err("neither length nor files");
        }
        let total_length = offset;
        let expected = total_length.div_ceil(piece_length);
        if expected != pieces.len() as u64 {
            return err("piece count doesn't match length");
        }
        Ok(Metainfo {
            info_hash,
            name,
            piece_length,
            pieces,
            files,
            total_length,
            single_file,
            raw_info: raw.to_vec(),
            announce: Vec::new(),
        })
    }

    pub fn num_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    pub fn piece_size(&self, index: u32) -> u64 {
        let start = index as u64 * self.piece_length;
        (self.total_length - start).min(self.piece_length)
    }

    /// Piece indices `[first, last]` that cover the file's bytes (None for empty files).
    pub fn file_pieces(&self, file: usize) -> Option<(u32, u32)> {
        let f = &self.files[file];
        if f.length == 0 {
            return None;
        }
        let first = (f.offset / self.piece_length) as u32;
        let last = ((f.offset + f.length - 1) / self.piece_length) as u32;
        Some((first, last))
    }

    pub fn piece_of(&self, abs_offset: u64) -> u32 {
        (abs_offset / self.piece_length) as u32
    }

    /// Where the file lives on disk under `root`.
    pub fn file_path(&self, root: &std::path::Path, file: usize) -> PathBuf {
        let mut p = root.to_path_buf();
        if !self.single_file {
            p.push(&self.name);
        }
        for c in &self.files[file].path {
            p.push(c);
        }
        p
    }
}

/// Build a torrent from in-memory files (used by tests, `playhead make-torrent` and the benchmark).
/// Returns (`.torrent` bytes, raw info dict).
pub fn build_torrent(name: &str, files: &[(String, Vec<u8>)], piece_length: u64, announce: Option<&str>) -> Vec<u8> {
    let mut all = Vec::new();
    for (_, d) in files {
        all.extend_from_slice(d);
    }
    let mut pieces = Vec::new();
    for chunk in all.chunks(piece_length as usize) {
        pieces.extend_from_slice(&Sha1::digest(chunk));
    }
    let mut info = vec![
        ("name", bencode::bytes(name.as_bytes())),
        ("piece length", bencode::int(piece_length as i64)),
        ("pieces", bencode::bytes(pieces)),
    ];
    if files.len() == 1 && files[0].0 == name {
        info.push(("length", bencode::int(files[0].1.len() as i64)));
    } else {
        let list = files
            .iter()
            .map(|(path, d)| {
                let parts = path.split('/').map(|p| bencode::bytes(p.as_bytes())).collect();
                bencode::dict([("length", bencode::int(d.len() as i64)), ("path", Value::List(parts))])
            })
            .collect();
        info.push(("files", Value::List(list)));
    }
    let info = Value::Dict(info.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)).collect());
    let mut root = vec![("info", info)];
    if let Some(a) = announce {
        root.push(("announce", bencode::bytes(a.as_bytes())));
    }
    Value::Dict(root.into_iter().map(|(k, v)| (k.as_bytes().to_vec(), v)).collect()).encode()
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes() {
        assert_eq!(sanitize_component(".."), "_");
        assert_eq!(sanitize_component("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_component("CON.txt"), "_CON.txt");
        assert_eq!(sanitize_component("com1"), "_com1");
        assert_eq!(sanitize_component("film. "), "film");
        assert_eq!(sanitize_component("ok name.mkv"), "ok name.mkv");
    }

    #[test]
    fn multi_file_layout() {
        let files = vec![("a/one.bin".to_string(), vec![1u8; 40_000]), ("two.bin".to_string(), vec![2u8; 30_000])];
        let t = build_torrent("pack", &files, 16384, Some("http://t/announce"));
        let m = Metainfo::from_torrent_bytes(&t).unwrap();
        assert_eq!(m.total_length, 70_000);
        assert_eq!(m.num_pieces(), 5);
        assert_eq!(m.files[1].offset, 40_000);
        assert_eq!(m.file_pieces(0), Some((0, 2)));
        assert_eq!(m.file_pieces(1), Some((2, 4)));
        assert_eq!(m.piece_size(4), 70_000 - 4 * 16384);
        assert_eq!(m.announce, vec!["http://t/announce"]);
        let p = m.file_path(std::path::Path::new("/x"), 0);
        assert_eq!(p, std::path::Path::new("/x/pack/a/one.bin"));
    }

    #[test]
    fn traversal_is_neutralised() {
        let files = vec![("../../etc/passwd".to_string(), vec![0u8; 100])];
        let t = build_torrent("..", &files, 16384, None);
        let m = Metainfo::from_torrent_bytes(&t).unwrap();
        let p = m.file_path(std::path::Path::new("/out"), 0);
        assert!(p.starts_with("/out"));
        assert!(!p.components().any(|c| matches!(c, std::path::Component::ParentDir)));
    }
}
