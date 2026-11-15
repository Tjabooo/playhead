//! File filter: executables and archives are never downloaded or served.
//!
//! Fake "episode" releases often ship a `.exe` or an archive next to (or instead of) the video.
//! Any file whose extension is on the block list is treated like it doesn't exist: its pieces are
//! not requested (unless shared with a wanted file), it's never written to disk, and the HTTP
//! server refuses it.

/// Extensions that are blocked by default (lowercase, without the dot).
#[rustfmt::skip]
pub const DEFAULT_BLOCKED: &[&str] = &[
    // executables and scripts
    "exe", "scr", "com", "pif", "lnk", "bat", "cmd", "ps1", "psm1", "psd1", "vbs", "vbe", "js", "jse", "wsf", "wsh",
    "hta", "msi", "msp", "mst", "dll", "cpl", "sys", "drv", "ocx", "jar", "apk", "appx", "msix", "reg", "inf", "chm",
    "url", "sh", "desktop", "appimage", "run", "bin", "deb", "rpm", "dmg", "pkg",
    // disk images
    "iso", "img", "vhd", "vhdx", "vmdk",
    // archives (split RAR / 7z / zip parts are handled in `is_blocked`)
    "zip", "rar", "7z", "tar", "gz", "tgz", "bz2", "xz", "zst", "cab", "arj", "ace", "lzh", "lha", "z", "uue",
];

#[derive(Debug, Clone)]
pub struct FileFilter {
    blocked: Vec<String>,
}

impl Default for FileFilter {
    fn default() -> Self {
        FileFilter { blocked: DEFAULT_BLOCKED.iter().map(|s| s.to_string()).collect() }
    }
}

impl FileFilter {
    /// Default list minus `allow`, plus `extra`.
    pub fn new(allow: &[String], extra: &[String]) -> Self {
        let norm = |s: &String| s.trim_start_matches('.').to_ascii_lowercase();
        let allow: Vec<String> = allow.iter().map(norm).collect();
        let mut blocked: Vec<String> = DEFAULT_BLOCKED.iter().map(|s| s.to_string()).filter(|e| !allow.contains(e)).collect();
        blocked.extend(extra.iter().map(norm));
        FileFilter { blocked }
    }

    pub fn is_blocked(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        let Some((_, ext)) = lower.rsplit_once('.') else { return false };
        if self.blocked.iter().any(|b| b == ext) {
            return true;
        }
        // split archives: .r00-.r99, .z01, .7z.001, .zip.001, .part1.rar is caught above
        let b = ext.as_bytes();
        let numbered = |prefix: u8| b.len() == 3 && b[0] == prefix && b[1].is_ascii_digit() && b[2].is_ascii_digit();
        if (numbered(b'r') && self.blocked.iter().any(|x| x == "rar")) || (numbered(b'z') && self.blocked.iter().any(|x| x == "zip")) {
            return true;
        }
        if b.len() == 3 && b.iter().all(u8::is_ascii_digit) {
            // name.7z.001 / name.zip.001
            let stem = &lower[..lower.len() - 4];
            if let Some((_, inner)) = stem.rsplit_once('.') {
                return self.blocked.iter().any(|x| x == inner);
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_exe_and_archives() {
        let f = FileFilter::default();
        for n in ["Show.S01E01.mkv.exe", "setup.EXE", "a.rar", "a.r07", "a.z01", "a.7z.001", "x.iso", "readme.lnk"] {
            assert!(f.is_blocked(n), "{n}");
        }
        for n in ["Show.S01E01.mkv", "film.mp4", "subs.srt", "sample.avi", "noext", "poster.jpg", "track.001.mkv"] {
            assert!(!f.is_blocked(n), "{n}");
        }
    }

    #[test]
    fn allow_and_extra() {
        let f = FileFilter::new(&["ZIP".into()], &[".nfo".into()]);
        assert!(!f.is_blocked("a.zip"));
        assert!(!f.is_blocked("a.z01"));
        assert!(f.is_blocked("info.nfo"));
    }
}
