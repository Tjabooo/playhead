//! HTTP range server. Every request is a playhead: the picker downloads from where it reads, and a
//! read of a missing piece simply waits until that piece is verified (holes are never served).
//!
//! Routes: `GET /` (file list), `GET /status` (JSON), `GET|HEAD /file/<n>[/<name>]`, `GET /stream`
//! (the first selected file).

use crate::engine::Engine;
use crate::magnet::percent_encode;
use serde_json::json;
use std::io::SeekFrom;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CHUNK: u64 = 256 * 1024;

pub fn content_type(name: &str) -> &'static str {
    let ext = name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).unwrap_or_default();
    match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "avi" => "video/x-msvideo",
        "mov" => "video/quicktime",
        "ts" | "m2ts" => "video/mp2t",
        "mpg" | "mpeg" => "video/mpeg",
        "wmv" => "video/x-ms-wmv",
        "flv" => "video/x-flv",
        "mp3" => "audio/mpeg",
        "flac" => "audio/flac",
        "m4a" => "audio/mp4",
        "ogg" | "opus" => "audio/ogg",
        "srt" => "application/x-subrip",
        "vtt" => "text/vtt",
        "ass" | "ssa" => "text/x-ssa",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "txt" | "nfo" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Parse a single `Range: bytes=` spec against a length. Ok(None) = no/ignored range.
#[allow(clippy::result_unit_err)]
pub fn parse_range(h: Option<&str>, len: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(h) = h else { return Ok(None) };
    let Some(spec) = h.trim().strip_prefix("bytes=") else { return Ok(None) };
    if spec.contains(',') {
        return Ok(None); // multi-range: serve the whole thing
    }
    let (a, b) = spec.split_once('-').ok_or(())?;
    let (a, b) = (a.trim(), b.trim());
    if len == 0 {
        return Err(());
    }
    let (s, e) = if a.is_empty() {
        let n: u64 = b.parse().map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
        (len.saturating_sub(n), len - 1)
    } else {
        let s: u64 = a.parse().map_err(|_| ())?;
        let e: u64 = if b.is_empty() { len - 1 } else { b.parse::<u64>().map_err(|_| ())?.min(len - 1) };
        (s, e)
    };
    if s >= len || s > e {
        return Err(());
    }
    Ok(Some((s, e)))
}

struct Request {
    method: String,
    path: String,
    range: Option<String>,
}

async fn read_request(s: &mut TcpStream) -> std::io::Result<Option<Request>> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(15), s.read(&mut tmp))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout"))??;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Ok(None);
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.split("\r\n");
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("").to_string();
    let path = first.next().unwrap_or("/").to_string();
    let mut range = None;
    for l in lines {
        if let Some((k, v)) = l.split_once(':')
            && k.trim().eq_ignore_ascii_case("range")
        {
            range = Some(v.trim().to_string());
        }
    }
    Ok(Some(Request { method, path, range }))
}

async fn respond(s: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await?;
    s.write_all(body).await
}

pub async fn serve(engine: Engine, listener: TcpListener) {
    loop {
        let Ok((mut s, _)) = listener.accept().await else { continue };
        let engine = engine.clone();
        tokio::spawn(async move {
            let _ = s.set_nodelay(true);
            if let Ok(Some(req)) = read_request(&mut s).await {
                let _ = handle(&engine, &mut s, req).await;
            }
            let _ = s.shutdown().await;
        });
    }
}

async fn handle(engine: &Engine, s: &mut TcpStream, req: Request) -> std::io::Result<()> {
    if req.method != "GET" && req.method != "HEAD" {
        return respond(s, "405 Method Not Allowed", "text/plain", b"GET or HEAD only\n").await;
    }
    let path = req.path.split('?').next().unwrap_or("/").to_string();
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match parts.as_slice() {
        ["status"] => {
            let st = engine.status().await;
            respond(s, "200 OK", "application/json", serde_json::to_string_pretty(&st).unwrap().as_bytes()).await
        }
        [""] => {
            let Some(meta) = engine.metadata_now() else {
                return respond(s, "503 Service Unavailable", "application/json", b"{\"state\":\"metadata\"}\n").await;
            };
            let files: Vec<_> = meta
                .files
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    json!({"index": i, "path": f.display_path(), "length": f.length, "blocked": engine.filter().is_blocked(f.name()),
                           "url": format!("/file/{i}/{}", percent_encode(f.name().as_bytes()))})
                })
                .collect();
            let body = json!({"name": meta.name, "info_hash": crate::metainfo::hex(&meta.info_hash), "files": files});
            respond(s, "200 OK", "application/json", serde_json::to_string_pretty(&body).unwrap().as_bytes()).await
        }
        ["stream"] => {
            let st = engine.status().await;
            let idx = st["files"].as_array().and_then(|f| f.iter().find(|f| f["selected"] == true)).and_then(|f| f["index"].as_u64());
            match idx {
                Some(i) => stream_file(engine, s, &req, i as usize).await,
                None => respond(s, "404 Not Found", "text/plain", b"no file selected\n").await,
            }
        }
        ["file", n, ..] => match n.parse::<usize>() {
            Ok(i) => stream_file(engine, s, &req, i).await,
            Err(_) => respond(s, "404 Not Found", "text/plain", b"bad file index\n").await,
        },
        _ => respond(s, "404 Not Found", "text/plain", b"not found\n").await,
    }
}

struct ReaderGuard<'a> {
    engine: &'a Engine,
    id: u64,
}
impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        self.engine.close_reader(self.id);
    }
}

async fn stream_file(engine: &Engine, s: &mut TcpStream, req: &Request, index: usize) -> std::io::Result<()> {
    let meta = match tokio::time::timeout(Duration::from_secs(120), engine.metadata()).await {
        Ok(m) => m,
        Err(_) => return respond(s, "504 Gateway Timeout", "text/plain", b"no metadata yet\n").await,
    };
    let Some(f) = meta.files.get(index).cloned() else {
        return respond(s, "404 Not Found", "text/plain", b"no such file\n").await;
    };
    if engine.filter().is_blocked(f.name()) {
        return respond(s, "403 Forbidden", "text/plain", b"blocked file type (executable or archive)\n").await;
    }
    if let Err(e) = engine.select(index).await {
        return respond(s, "409 Conflict", "text/plain", format!("{e}\n").as_bytes()).await;
    }
    let len = f.length;
    let (status, start, end) = match parse_range(req.range.as_deref(), len) {
        Ok(Some((a, b))) => ("206 Partial Content", a, b),
        Ok(None) => ("200 OK", 0, len.saturating_sub(1)),
        Err(()) => {
            let head = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{len}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            return s.write_all(head.as_bytes()).await;
        }
    };
    let body_len = if len == 0 { 0 } else { end - start + 1 };
    let mut head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {}\r\nContent-Length: {body_len}\r\nAccept-Ranges: bytes\r\nCache-Control: no-store\r\nConnection: close\r\n",
        content_type(f.name())
    );
    if status.starts_with("206") {
        head.push_str(&format!("Content-Range: bytes {start}-{end}/{len}\r\n"));
    }
    head.push_str("\r\n");
    if req.method == "HEAD" || body_len == 0 {
        // A HEAD still tells the picker where a player is about to read.
        let id = engine.open_reader(index, start);
        engine.close_reader(id);
        return s.write_all(head.as_bytes()).await;
    }
    let reader = ReaderGuard { engine, id: engine.open_reader(index, start) };
    s.write_all(head.as_bytes()).await?;

    let path = engine.file_path(&meta, index);
    let mut file: Option<tokio::fs::File> = None;
    let (mut rd, mut wr) = s.split();
    let mut pos = start;
    let mut last_piece = u32::MAX;
    let mut buf = vec![0u8; CHUNK as usize];
    let mut sink = [0u8; 64];
    while pos <= end {
        let abs = f.offset + pos;
        let piece = meta.piece_of(abs);
        if piece != last_piece {
            engine.move_reader(reader.id, pos);
            last_piece = piece;
        }
        if !engine.has_piece(piece) {
            // Wait for the piece, but stop as soon as the client goes away.
            tokio::select! {
                _ = engine.wait_piece(piece) => {}
                r = rd.read(&mut sink) => {
                    if matches!(r, Ok(0) | Err(_)) { return Ok(()); }
                }
            }
            continue;
        }
        let piece_end = ((piece as u64 + 1) * meta.piece_length).min(f.offset + f.length) - f.offset; // exclusive, file-relative
        let n = (piece_end - pos).min(end + 1 - pos).min(CHUNK) as usize;
        if file.is_none() {
            file = Some(tokio::fs::File::open(&path).await?);
        }
        let fh = file.as_mut().unwrap();
        fh.seek(SeekFrom::Start(pos)).await?;
        fh.read_exact(&mut buf[..n]).await?;
        wr.write_all(&buf[..n]).await?;
        pos += n as u64;
    }
    drop(reader);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(parse_range(None, 100), Ok(None));
        assert_eq!(parse_range(Some("bytes=0-"), 100), Ok(Some((0, 99))));
        assert_eq!(parse_range(Some("bytes=10-19"), 100), Ok(Some((10, 19))));
        assert_eq!(parse_range(Some("bytes=90-500"), 100), Ok(Some((90, 99))));
        assert_eq!(parse_range(Some("bytes=-10"), 100), Ok(Some((90, 99))));
        assert_eq!(parse_range(Some("bytes=100-"), 100), Err(()));
        assert_eq!(parse_range(Some("bytes=5-1"), 100), Err(()));
        assert_eq!(parse_range(Some("bytes=0-1,5-6"), 100), Ok(None));
        assert_eq!(content_type("A.MKV"), "video/x-matroska");
    }
}
