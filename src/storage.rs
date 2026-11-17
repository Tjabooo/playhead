//! Disk storage: verified pieces are written into the selected files, which are sparse: a file only
//! takes the space of the pieces written into it. Bytes that belong to unselected or blocked files are
//! dropped, so a blocked `.exe` is never created.
//!
//! Sparse matters on Windows: NTFS allocates a normal file's full length up front and, on a write far
//! past what was written before, fills everything in between with zeros first. Next to another client
//! that already has most of a 20 GB file, that meant 20 GB of zeros (minutes of disk time, and a full
//! disk) for a piece at the end.

use crate::metainfo::Metainfo;
use sha1::{Digest, Sha1};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

pub struct Storage {
    root: PathBuf,
    meta: Arc<Metainfo>,
    selected: Vec<bool>,
    files: Vec<Option<File>>,
}

/// Mark a file sparse, so its length costs no disk space until data is written (Unix files already are).
#[cfg(windows)]
pub fn set_sparse(f: &File) -> io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn DeviceIoControl(
            device: *mut c_void,
            code: u32,
            input: *const c_void,
            input_len: u32,
            output: *mut c_void,
            output_len: u32,
            returned: *mut u32,
            overlapped: *mut c_void,
        ) -> i32;
    }
    const FSCTL_SET_SPARSE: u32 = 0x0009_00C4;
    let mut returned = 0u32;
    // SAFETY: a valid handle owned by `f`; no buffers (FSCTL_SET_SPARSE with no input sets the flag).
    let ok = unsafe {
        DeviceIoControl(
            f.as_raw_handle(),
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

#[cfg(not(windows))]
pub fn set_sparse(_f: &File) -> io::Result<()> {
    Ok(())
}

/// One piece's overlap with one file: (file index, offset in file, offset in piece, length).
pub fn piece_spans(meta: &Metainfo, index: u32) -> Vec<(usize, u64, usize, usize)> {
    let start = index as u64 * meta.piece_length;
    let end = start + meta.piece_size(index);
    let mut out = Vec::new();
    for (fi, f) in meta.files.iter().enumerate() {
        let (fs, fe) = (f.offset, f.offset + f.length);
        if fe <= start || fs >= end || f.length == 0 {
            continue;
        }
        let s = start.max(fs);
        let e = end.min(fe);
        out.push((fi, s - fs, (s - start) as usize, (e - s) as usize));
    }
    out
}

impl Storage {
    pub fn new(root: PathBuf, meta: Arc<Metainfo>) -> Self {
        let n = meta.files.len();
        Storage { root, meta, selected: vec![false; n], files: (0..n).map(|_| None).collect() }
    }

    pub fn select(&mut self, file: usize) {
        self.selected[file] = true;
    }

    pub fn is_selected(&self, file: usize) -> bool {
        self.selected[file]
    }

    fn open(&mut self, fi: usize, create: bool) -> io::Result<&mut File> {
        if self.files[fi].is_none() {
            let path = self.meta.file_path(&self.root, fi);
            if create && let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let f = OpenOptions::new().read(true).write(true).create(create).truncate(false).open(&path)?;
            let want = self.meta.files[fi].length;
            if f.metadata()?.len() != want {
                if let Err(e) = set_sparse(&f) {
                    crate::log::debug(&format!("{}: not a sparse file ({e})", path.display()));
                }
                f.set_len(want)?;
            }
            self.files[fi] = Some(f);
        }
        Ok(self.files[fi].as_mut().unwrap())
    }

    /// Write a verified piece. Returns true if every byte of it landed on disk (all its files selected).
    pub fn write_piece(&mut self, index: u32, data: &[u8]) -> io::Result<bool> {
        let mut complete = true;
        for (fi, foff, poff, len) in piece_spans(&self.meta, index) {
            if !self.selected[fi] {
                complete = false;
                continue;
            }
            let f = self.open(fi, true)?;
            f.seek(SeekFrom::Start(foff))?;
            f.write_all(&data[poff..poff + len])?;
        }
        Ok(complete)
    }

    /// Read a whole piece back (None if part of it isn't stored).
    pub fn read_piece(&mut self, index: u32) -> io::Result<Option<Vec<u8>>> {
        let spans = piece_spans(&self.meta, index);
        if spans.iter().any(|(fi, ..)| !self.selected[*fi]) {
            return Ok(None);
        }
        let mut buf = vec![0u8; self.meta.piece_size(index) as usize];
        for (fi, foff, poff, len) in spans {
            let path = self.meta.file_path(&self.root, fi);
            if !path.exists() {
                return Ok(None);
            }
            let f = self.open(fi, false)?;
            f.seek(SeekFrom::Start(foff))?;
            f.read_exact(&mut buf[poff..poff + len])?;
        }
        Ok(Some(buf))
    }

    /// Hash what's already on disk (resume). Returns the pieces that check out.
    pub fn verify_existing(&mut self, pieces: impl Iterator<Item = u32>) -> Vec<u32> {
        let mut good = Vec::new();
        for i in pieces {
            if let Ok(Some(data)) = self.read_piece(i)
                && Sha1::digest(&data).as_slice() == self.meta.pieces[i as usize]
            {
                good.push(i);
            }
        }
        good
    }

    pub fn flush(&mut self) {
        for f in self.files.iter_mut().flatten() {
            let _ = f.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::build_torrent;

    #[test]
    fn writes_only_selected_files() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![("a.mkv".to_string(), vec![7u8; 20_000]), ("b.exe".to_string(), vec![9u8; 20_000])];
        let t = build_torrent("pack", &files, 16384, None);
        let meta = Arc::new(Metainfo::from_torrent_bytes(&t).unwrap());
        let mut s = Storage::new(dir.path().to_path_buf(), meta.clone());
        s.select(0);
        let all: Vec<u8> = files.iter().flat_map(|f| f.1.clone()).collect();
        for i in 0..meta.num_pieces() {
            let st = (i as u64 * 16384) as usize;
            let en = (st + meta.piece_size(i) as usize).min(all.len());
            let full = s.write_piece(i, &all[st..en]).unwrap();
            assert_eq!(full, i == 0, "piece {i}");
        }
        assert_eq!(std::fs::read(dir.path().join("pack/a.mkv")).unwrap(), files[0].1);
        assert!(!dir.path().join("pack/b.exe").exists());
        assert_eq!(s.verify_existing(0..meta.num_pieces()), vec![0]);
    }
}
