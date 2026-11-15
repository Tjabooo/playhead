//! Tiny stderr logger. Level from `PLAYHEAD_LOG` (error, warn, info, debug); default info.

use std::sync::atomic::{AtomicU8, Ordering};

static LEVEL: AtomicU8 = AtomicU8::new(2);

pub fn init_from_env() {
    let l = match std::env::var("PLAYHEAD_LOG").unwrap_or_default().as_str() {
        "error" => 0,
        "warn" => 1,
        "debug" => 3,
        "quiet" | "off" => 255,
        _ => 2,
    };
    set_level(l);
}

pub fn set_level(l: u8) {
    LEVEL.store(l, Ordering::Relaxed);
}

fn emit(level: u8, tag: &str, msg: &str) {
    let cur = LEVEL.load(Ordering::Relaxed);
    if cur != 255 && level <= cur {
        eprintln!("[playhead {tag}] {msg}");
    }
}

pub fn error(msg: &str) {
    emit(0, "error", msg)
}
pub fn warn(msg: &str) {
    emit(1, "warn", msg)
}
pub fn info(msg: &str) {
    emit(2, "info", msg)
}
pub fn debug(msg: &str) {
    emit(3, "debug", msg)
}
pub fn debug_on() -> bool {
    let cur = LEVEL.load(Ordering::Relaxed);
    cur != 255 && cur >= 3
}
