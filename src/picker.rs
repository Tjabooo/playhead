//! Deadline piece picker: the part that makes streaming work.
//!
//! Every HTTP reader is a playhead. Pieces are ranked:
//! 1. **critical**: the first `critical_bytes` after a playhead (what the player needs next),
//! 2. **window**: up to `window_bytes` after a playhead, plus prefetch ranges (file head/tail),
//! 3. **background**: the rest of the wanted file, in order from the furthest playhead.
//!
//! Rules (deadline mode):
//! * Critical/window pieces only go to peers that are fast enough; a slow or snubbed peer gets them
//!   only if no fast peer has the piece.
//! * A critical/window block that's been out too long (compared with what its peer's rate predicts)
//!   is requested again from a faster peer; the first copy to arrive wins, the rest are cancelled.
//! * A peer with requests out and nothing delivered for `snub_secs` is snubbed: depth 1, no urgent
//!   pieces, and its outstanding blocks are immediately eligible for duplication.
//! * Seeking (a playhead jump beyond the window) cancels background requests so the pipelines drain
//!   into the new window right away.
//!
//! `Sequential` mode is a naive baseline for the benchmark: in-order, no peer ranking, no
//! duplication, 60 s request timeout (roughly what a sequential-download client does).

use crate::bitfield::Bitfield;
use crate::peer::PeerKey;
use crate::wire::BLOCK;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerMode {
    Deadline,
    Sequential,
}

#[derive(Debug, Clone)]
pub struct PickerConfig {
    pub mode: PickerMode,
    pub window_bytes: u64,
    pub critical_bytes: u64,
    pub snub_secs: f64,
    pub max_dup_critical: usize,
    pub max_dup_window: usize,
    /// Fetch the rest of the file in the background. Off = stream-only (assist) mode: only what
    /// readers need next, for when another client downloads the file in order anyway.
    pub background: bool,
    /// Assist mode: a piece the other client is downloading is left to it this long. After that it's
    /// most likely stuck on one slow peer there, and fetching it here too is what gets it moving.
    pub avoid_secs: f64,
}

impl Default for PickerConfig {
    fn default() -> Self {
        PickerConfig {
            mode: PickerMode::Deadline,
            window_bytes: 32 * 1024 * 1024,
            critical_bytes: 3 * 1024 * 1024,
            snub_secs: 6.0,
            max_dup_critical: 3,
            max_dup_window: 2,
            background: true,
            avoid_secs: 8.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Critical,
    Window,
    Background,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Req {
    pub peer: PeerKey,
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Debug, Clone)]
enum Slot {
    Free,
    Asked(Vec<(PeerKey, Instant)>),
    Done,
}

struct InProgress {
    blocks: Vec<Slot>,
    done: u32,
    data: Vec<u8>,
    contributors: HashSet<PeerKey>,
}

pub struct PeerState {
    pub has: Bitfield,
    pub choking_us: bool,
    outstanding: HashSet<(u32, u32)>,
    pub rate: f64,
    bytes_since: u64,
    last_progress: Instant,
    connected_at: Instant,
    pub snubbed: bool,
    pub downloaded: u64,
}

impl PeerState {
    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }
}

#[derive(Debug, Clone)]
struct Reader {
    pos: u64,
    end: u64,
}

pub struct Completed {
    pub index: u32,
    pub data: Vec<u8>,
    pub contributors: Vec<PeerKey>,
}

#[derive(Default)]
pub struct BlockOutcome {
    pub cancels: Vec<Req>,
    pub completed: Option<Completed>,
    /// Bytes that were new to us (not a duplicate copy).
    pub useful: bool,
}

pub struct Picker {
    pub cfg: PickerConfig,
    piece_length: u64,
    total_length: u64,
    num_pieces: u32,
    pub have: Bitfield,
    wanted: Vec<bool>,
    inprog: HashMap<u32, InProgress>,
    readers: HashMap<u64, Reader>,
    prefetch: Vec<(u64, u64)>,
    start_lead: Option<(u64, u64)>,
    /// Pieces another client is downloading right now: left to it unless they're critical, or it has had
    /// them for longer than `avoid_secs` (`avoid_since`: since when, for as long as it's on them).
    avoid: Bitfield,
    avoid_since: HashMap<u32, Instant>,
    focus: (u64, u64),
    pub peers: HashMap<PeerKey, PeerState>,
    last_rate_update: Instant,
    pub duplicate_requests: u64,
    pub wasted_bytes: u64,
}

impl Picker {
    pub fn new(cfg: PickerConfig, piece_length: u64, total_length: u64) -> Self {
        let num_pieces = total_length.div_ceil(piece_length) as u32;
        Picker {
            cfg,
            piece_length,
            total_length,
            num_pieces,
            have: Bitfield::new(num_pieces),
            wanted: vec![true; num_pieces as usize],
            inprog: HashMap::new(),
            readers: HashMap::new(),
            prefetch: Vec::new(),
            start_lead: None,
            avoid: Bitfield::new(num_pieces),
            avoid_since: HashMap::new(),
            focus: (0, total_length),
            peers: HashMap::new(),
            last_rate_update: Instant::now(),
            duplicate_requests: 0,
            wasted_bytes: 0,
        }
    }

    pub fn num_pieces(&self) -> u32 {
        self.num_pieces
    }

    pub fn piece_size(&self, i: u32) -> u64 {
        (self.total_length - i as u64 * self.piece_length).min(self.piece_length)
    }

    fn blocks_in(&self, i: u32) -> u32 {
        self.piece_size(i).div_ceil(BLOCK as u64) as u32
    }

    fn block_len(&self, i: u32, b: u32) -> u32 {
        let size = self.piece_size(i);
        (size - b as u64 * BLOCK as u64).min(BLOCK as u64) as u32
    }

    /// Only pieces in `[first, last]` are wanted; the file at `focus` drives background order.
    pub fn set_wanted_range(&mut self, first: u32, last: u32, focus: (u64, u64)) {
        for (i, w) in self.wanted.iter_mut().enumerate() {
            *w = (first as usize..=last as usize).contains(&i);
        }
        self.focus = focus;
    }

    pub fn is_wanted(&self, i: u32) -> bool {
        self.wanted.get(i as usize).copied().unwrap_or(false)
    }

    pub fn wanted_remaining(&self) -> u32 {
        (0..self.num_pieces).filter(|&i| self.wanted[i as usize] && !self.have.get(i)).count() as u32
    }

    pub fn set_prefetch(&mut self, ranges: Vec<(u64, u64)>) {
        self.prefetch = ranges;
    }

    /// The start of the file a player needs before it begins, fetched from its far end backwards: next
    /// to a client downloading in order from the front, the two meet in the middle instead of overlapping.
    pub fn set_start_lead(&mut self, range: Option<(u64, u64)>) {
        self.start_lead = range;
    }

    pub fn mark_have(&mut self, i: u32) {
        self.have.set(i);
        self.inprog.remove(&i);
    }

    /// Another source has piece `i` (assist mode): stop fetching it and return the cancels.
    pub fn mark_have_elsewhere(&mut self, i: u32) -> Vec<Req> {
        let mut cancels = Vec::new();
        if self.have.get(i) {
            return cancels;
        }
        for (key, p) in self.peers.iter_mut() {
            let out: Vec<(u32, u32)> = p.outstanding.iter().copied().filter(|(pi, _)| *pi == i).collect();
            for (pi, b) in out {
                p.outstanding.remove(&(pi, b));
                let size = (self.total_length - pi as u64 * self.piece_length).min(self.piece_length);
                let len = (size - b as u64 * BLOCK as u64).min(BLOCK as u64) as u32;
                cancels.push(Req { peer: *key, index: pi, begin: b * BLOCK, length: len });
            }
        }
        self.mark_have(i);
        cancels
    }

    /// The other source turned out not to have it after all: fetch it again.
    pub fn unmark_have(&mut self, i: u32) {
        self.have.clear(i);
    }

    /// The other client is (or isn't) downloading piece `i`. Called with its states every second or so.
    pub fn set_avoid(&mut self, i: u32, avoid: bool, now: Instant) {
        if !avoid {
            self.avoid.clear(i);
            self.avoid_since.remove(&i);
        } else if let std::collections::hash_map::Entry::Vacant(e) = self.avoid_since.entry(i) {
            e.insert(now);
            self.avoid.set(i);
        }
    }

    /// Pieces the other client has been on for too long are no longer left to it.
    fn expire_avoid(&mut self, now: Instant) {
        let patience = Duration::from_secs_f64(self.cfg.avoid_secs);
        for (&i, &since) in &self.avoid_since {
            if now.duration_since(since) >= patience && self.avoid.get(i) {
                self.avoid.clear(i);
            }
        }
    }

    // ---- peers ----

    pub fn add_peer(&mut self, key: PeerKey, now: Instant) {
        self.peers.insert(
            key,
            PeerState {
                has: Bitfield::new(self.num_pieces),
                choking_us: true,
                outstanding: HashSet::new(),
                rate: 0.0,
                bytes_since: 0,
                last_progress: now,
                connected_at: now,
                snubbed: false,
                downloaded: 0,
            },
        );
    }

    pub fn remove_peer(&mut self, key: PeerKey) {
        if let Some(p) = self.peers.remove(&key) {
            for (i, b) in p.outstanding {
                self.unask(i, b, key);
            }
        }
    }

    fn unask(&mut self, i: u32, b: u32, key: PeerKey) {
        if let Some(ip) = self.inprog.get_mut(&i)
            && let Some(slot) = ip.blocks.get_mut(b as usize)
            && let Slot::Asked(list) = slot
        {
            list.retain(|(k, _)| *k != key);
            if list.is_empty() {
                *slot = Slot::Free;
            }
        }
    }

    pub fn peer_bitfield(&mut self, key: PeerKey, bf: Bitfield) {
        if let Some(p) = self.peers.get_mut(&key) {
            p.has = bf;
        }
    }

    pub fn peer_have(&mut self, key: PeerKey, i: u32) {
        if let Some(p) = self.peers.get_mut(&key) {
            p.has.set(i);
        }
    }

    pub fn peer_choking(&mut self, key: PeerKey, choking: bool, now: Instant) {
        let Some(p) = self.peers.get_mut(&key) else { return };
        p.choking_us = choking;
        p.last_progress = now;
        if choking {
            let out: Vec<_> = p.outstanding.drain().collect();
            for (i, b) in out {
                self.unask(i, b, key);
            }
        }
    }

    /// Does the peer have something we still want?
    pub fn interesting(&self, key: PeerKey) -> bool {
        let Some(p) = self.peers.get(&key) else { return false };
        (0..self.num_pieces).any(|i| self.wanted[i as usize] && !self.have.get(i) && p.has.get(i))
    }

    // ---- readers ----

    /// Register or move a playhead (absolute torrent offset; `end` = end of its file).
    /// Returns cancels when the move was a seek beyond the window.
    pub fn set_reader(&mut self, id: u64, pos: u64, end: u64) -> Vec<Req> {
        let prev = self.readers.insert(id, Reader { pos, end });
        let jumped = match prev {
            Some(r) => pos < r.pos || pos > r.pos + self.cfg.window_bytes,
            // A new reader is usually a seek (players reopen the connection) or a probe of the tail.
            None => self.any_outstanding(),
        };
        if jumped && self.cfg.mode == PickerMode::Deadline { self.cancel_background() } else { Vec::new() }
    }

    pub fn remove_reader(&mut self, id: u64) {
        self.readers.remove(&id);
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    fn any_outstanding(&self) -> bool {
        self.peers.values().any(|p| !p.outstanding.is_empty())
    }

    fn cancel_background(&mut self) -> Vec<Req> {
        let classes = self.classify();
        let mut cancels = Vec::new();
        let keys: Vec<PeerKey> = self.peers.keys().copied().collect();
        for key in keys {
            let out: Vec<(u32, u32)> = self.peers[&key].outstanding.iter().copied().collect();
            for (i, b) in out {
                if !classes.contains_key(&i) {
                    self.peers.get_mut(&key).unwrap().outstanding.remove(&(i, b));
                    self.unask(i, b, key);
                    cancels.push(Req { peer: key, index: i, begin: b * BLOCK, length: self.block_len(i, b) });
                }
            }
        }
        cancels
    }

    /// Urgent pieces (critical + window) with their rank: (class, distance from playhead).
    fn classify(&self) -> HashMap<u32, (Class, u64)> {
        let mut out: HashMap<u32, (Class, u64)> = HashMap::new();
        let mut add = |i: u32, c: Class, d: u64, have: &Bitfield, wanted: &[bool]| {
            if i >= self.num_pieces || have.get(i) || !wanted[i as usize] {
                return;
            }
            let e = out.entry(i).or_insert((c, d));
            if (c, d) < *e {
                *e = (c, d);
            }
        };
        for r in self.readers.values() {
            if r.pos >= r.end {
                continue;
            }
            let first = (r.pos / self.piece_length) as u32;
            let crit_end = (r.pos + self.cfg.critical_bytes.max(1)).min(r.end) - 1;
            let win_end = (r.pos + self.cfg.window_bytes.max(1)).min(r.end) - 1;
            let crit_last = (crit_end / self.piece_length) as u32;
            let win_last = (win_end / self.piece_length) as u32;
            for i in first..=win_last {
                let d = (i as u64 * self.piece_length).saturating_sub(r.pos);
                let c = if i <= crit_last { Class::Critical } else { Class::Window };
                if c == Class::Window && self.avoid.get(i) {
                    continue;
                }
                add(i, c, d, &self.have, &self.wanted);
            }
        }
        // The file's head and tail, which a player reads before it starts: fetched here even while the other
        // client is on them too (a few MB twice at worst, against a start held up by one slow peer).
        for &(s, e) in &self.prefetch {
            if e <= s {
                continue;
            }
            for i in (s / self.piece_length) as u32..=((e - 1) / self.piece_length) as u32 {
                add(i, Class::Window, u64::MAX / 2, &self.have, &self.wanted);
            }
        }
        if let Some((s, e)) = self.start_lead.filter(|(s, e)| e > s) {
            let last = ((e - 1) / self.piece_length) as u32;
            for i in (s / self.piece_length) as u32..=last {
                if !self.avoid.get(i) {
                    // Farther pieces rank first (smaller distance), right after the readers' own windows.
                    add(i, Class::Window, u64::MAX / 4 + (last - i) as u64, &self.have, &self.wanted);
                }
            }
        }
        out
    }

    /// Full piece order for this round.
    fn order(&self) -> Vec<(u32, Class)> {
        let urgent = self.classify();
        let mut ranked: Vec<(u32, Class, u64)> = urgent.iter().map(|(&i, &(c, d))| (i, c, d)).collect();
        ranked.sort_by_key(|&(i, c, d)| (c, d, i));
        let mut order: Vec<(u32, Class)> = ranked.into_iter().map(|(i, c, _)| (i, c)).collect();
        // Background: from the furthest playhead to the end of the focus file, then from its start.
        let start = self
            .readers
            .values()
            .map(|r| r.pos.saturating_add(self.cfg.window_bytes))
            .max()
            .unwrap_or(self.focus.0)
            .clamp(self.focus.0, self.focus.1.max(self.focus.0 + 1) - 1);
        let first = (self.focus.0 / self.piece_length) as u32;
        let last = ((self.focus.1.max(1) - 1) / self.piece_length) as u32;
        let pivot = ((start / self.piece_length) as u32).clamp(first, last);
        let background = self.cfg.background;
        let mut push = |i: u32| {
            if background && self.wanted[i as usize] && !self.have.get(i) && !urgent.contains_key(&i) {
                order.push((i, Class::Background));
            }
        };
        for i in pivot..=last {
            push(i);
        }
        for i in first..pivot {
            push(i);
        }
        // Wanted pieces outside the focus file (none in the single-file flow).
        for i in (0..first).chain(last + 1..self.num_pieces) {
            push(i);
        }
        if self.cfg.mode == PickerMode::Sequential {
            // Naive: strictly by position from the furthest-back playhead (or file start).
            let from = self.readers.values().map(|r| (r.pos / self.piece_length) as u32).min().unwrap_or(first);
            order.sort_by_key(|&(i, _)| if i >= from { (0, i) } else { (1, i) });
        }
        order
    }

    // ---- rates and snubbing ----

    fn update_rates(&mut self, now: Instant) {
        let dt = now.duration_since(self.last_rate_update).as_secs_f64();
        if dt < 0.05 {
            return;
        }
        self.last_rate_update = now;
        let alpha = dt / (dt + 2.0);
        let snub = Duration::from_secs_f64(self.cfg.snub_secs);
        for p in self.peers.values_mut() {
            let inst = p.bytes_since as f64 / dt;
            p.bytes_since = 0;
            p.rate = p.rate * (1.0 - alpha) + inst * alpha;
            if !p.outstanding.is_empty() && now.duration_since(p.last_progress) > snub && !p.snubbed {
                p.snubbed = true;
                crate::log::debug(&format!("snubbed a peer ({} requests out, {:.0} B/s)", p.outstanding.len(), p.rate));
            }
        }
    }

    fn best_rate(&self) -> f64 {
        self.peers.values().filter(|p| !p.choking_us).map(|p| p.rate).fold(0.0, f64::max)
    }

    fn is_slow(&self, p: &PeerState, best: f64, now: Instant) -> bool {
        if p.snubbed {
            return true;
        }
        let age = now.duration_since(p.connected_at).as_secs_f64();
        if age < 3.0 && p.downloaded == 0 {
            return false; // unknown yet: give it a chance
        }
        p.rate < 16.0 * 1024.0 || p.rate < best / 8.0
    }

    /// How long we expect `asker` to take for one of its outstanding blocks.
    /// How long a block of `class` may stay with `asker` before a faster peer may fetch it too.
    /// What the asker's rate predicts, but never longer than the class can afford: a slow peer's
    /// honest 8 s estimate is still 8 s of frozen video for a critical block.
    fn patience(&self, asker: PeerKey, class: Class) -> Duration {
        let (floor, cap) = match class {
            Class::Critical => (0.6, 1.5),
            Class::Window => (2.0, 5.0),
            Class::Background => (6.0, 30.0),
        };
        let Some(p) = self.peers.get(&asker) else { return Duration::ZERO };
        if p.snubbed {
            return Duration::ZERO;
        }
        let expect = if p.rate > 1.0 { (p.outstanding.len().max(1) as f64 * BLOCK as f64) / p.rate } else { 1.5 };
        let mult = if class == Class::Critical { 1.5 } else { 2.5 };
        Duration::from_secs_f64((expect * mult).clamp(floor, cap))
    }

    // ---- picking ----

    /// Requests to send now.
    pub fn pick(&mut self, now: Instant) -> Vec<Req> {
        self.update_rates(now);
        self.expire_avoid(now);
        let mut reqs = Vec::new();
        if self.wanted_remaining() == 0 {
            return reqs;
        }
        let order = self.order();
        let best = self.best_rate();
        let deadline = self.cfg.mode == PickerMode::Deadline;

        // Fastest peers first so they claim the critical blocks.
        let mut keys: Vec<PeerKey> = self.peers.iter().filter(|(_, p)| !p.choking_us).map(|(k, _)| *k).collect();
        keys.sort_by(|a, b| self.peers[b].rate.total_cmp(&self.peers[a].rate));

        // Which urgent pieces have at least one fast, unchoked holder?
        let fast_keys: Vec<PeerKey> = keys.iter().copied().filter(|k| !self.is_slow(&self.peers[k], best, now)).collect();

        let all_asked = self.endgame(&order);

        for key in keys {
            let p = &self.peers[&key];
            let slow = deadline && self.is_slow(p, best, now);
            let depth = if deadline && p.snubbed { 1 } else { ((p.rate * 2.0) / BLOCK as f64).clamp(12.0, 250.0) as usize };
            let mut room = depth.saturating_sub(p.outstanding.len());
            if room == 0 {
                continue;
            }
            for &(i, class) in &order {
                if room == 0 {
                    break;
                }
                if !self.peers[&key].has.get(i) {
                    continue;
                }
                let urgent = class != Class::Background;
                if slow && urgent && fast_keys.iter().any(|k| self.peers[k].has.get(i)) {
                    continue;
                }
                let nblocks = self.blocks_in(i);
                let ip = self.inprog.entry(i).or_insert_with(|| InProgress {
                    blocks: vec![Slot::Free; nblocks as usize],
                    done: 0,
                    data: Vec::new(),
                    contributors: HashSet::new(),
                });
                // Free blocks first.
                let mut picked = Vec::new();
                for (b, slot) in ip.blocks.iter_mut().enumerate() {
                    if room == 0 {
                        break;
                    }
                    if matches!(slot, Slot::Free) {
                        *slot = Slot::Asked(vec![(key, now)]);
                        picked.push(b as u32);
                        room -= 1;
                    }
                }
                // Then duplicates of stale urgent blocks (or anything, in endgame).
                if deadline && room > 0 && !self.peers[&key].snubbed && (urgent || all_asked) {
                    let max_dup = match class {
                        Class::Critical => self.cfg.max_dup_critical,
                        _ => self.cfg.max_dup_window,
                    };
                    let my_rate = self.peers[&key].rate;
                    let nb = self.inprog[&i].blocks.len();
                    for b in 0..nb {
                        if room == 0 {
                            break;
                        }
                        let Slot::Asked(list) = &self.inprog[&i].blocks[b] else { continue };
                        if list.len() >= max_dup || list.iter().any(|(k, _)| *k == key) {
                            continue;
                        }
                        let stale = list.iter().all(|(k, at)| {
                            let other = self.peers.get(k);
                            let other_rate = other.map(|o| o.rate).unwrap_or(0.0);
                            let snubbed = other.map(|o| o.snubbed).unwrap_or(true);
                            now.duration_since(*at) > self.patience(*k, class)
                                && (snubbed || my_rate > other_rate * 1.25 || other_rate < 1.0)
                        });
                        if stale {
                            if let Slot::Asked(list) = &mut self.inprog.get_mut(&i).unwrap().blocks[b] {
                                list.push((key, now));
                            }
                            self.duplicate_requests += 1;
                            picked.push(b as u32);
                            room -= 1;
                        }
                    }
                }
                let p = self.peers.get_mut(&key).unwrap();
                for b in picked {
                    if p.outstanding.is_empty() {
                        p.last_progress = now;
                    }
                    p.outstanding.insert((i, b));
                    reqs.push(Req { peer: key, index: i, begin: b * BLOCK, length: 0 });
                }
            }
        }
        for r in &mut reqs {
            r.length = self.block_len(r.index, r.begin / BLOCK);
        }
        if !deadline {
            self.expire_sequential(now, &mut reqs);
        }
        reqs
    }

    /// Endgame: every remaining wanted block is already requested somewhere.
    fn endgame(&self, order: &[(u32, Class)]) -> bool {
        order.len() <= 8
            && order.iter().all(|(i, _)| self.inprog.get(i).is_some_and(|ip| ip.blocks.iter().all(|s| !matches!(s, Slot::Free))))
    }

    /// Sequential baseline: requests time out after 60 s and go back to the pool.
    fn expire_sequential(&mut self, now: Instant, _reqs: &mut [Req]) {
        let timeout = Duration::from_secs(60);
        let mut expired = Vec::new();
        for (&i, ip) in &self.inprog {
            for (b, slot) in ip.blocks.iter().enumerate() {
                if let Slot::Asked(list) = slot {
                    for (k, at) in list {
                        if now.duration_since(*at) > timeout {
                            expired.push((i, b as u32, *k));
                        }
                    }
                }
            }
        }
        for (i, b, k) in expired {
            if let Some(p) = self.peers.get_mut(&k) {
                p.outstanding.remove(&(i, b));
            }
            self.unask(i, b, k);
        }
    }

    /// A block arrived. Returns cancels for duplicate requests and the piece if it's complete.
    pub fn block_received(&mut self, key: PeerKey, index: u32, begin: u32, data: &[u8], now: Instant) -> BlockOutcome {
        let mut out = BlockOutcome::default();
        if let Some(p) = self.peers.get_mut(&key) {
            p.bytes_since += data.len() as u64;
            p.downloaded += data.len() as u64;
            p.last_progress = now;
            p.snubbed = false;
        }
        if index >= self.num_pieces || !begin.is_multiple_of(BLOCK) || self.have.get(index) || !self.wanted[index as usize] {
            self.wasted_bytes += data.len() as u64;
            if let Some(p) = self.peers.get_mut(&key) {
                p.outstanding.remove(&(index, begin / BLOCK));
            }
            return out;
        }
        let b = begin / BLOCK;
        if b >= self.blocks_in(index) || data.len() as u32 != self.block_len(index, b) {
            self.wasted_bytes += data.len() as u64;
            return out;
        }
        if let Some(p) = self.peers.get_mut(&key) {
            p.outstanding.remove(&(index, b));
        }
        let nblocks = self.blocks_in(index) as usize;
        let size = self.piece_size(index) as usize;
        let ip = self.inprog.entry(index).or_insert_with(|| InProgress {
            blocks: vec![Slot::Free; nblocks],
            done: 0,
            data: Vec::new(),
            contributors: HashSet::new(),
        });
        if ip.data.len() != size {
            ip.data.resize(size, 0);
        }
        let slot = std::mem::replace(&mut ip.blocks[b as usize], Slot::Done);
        match slot {
            Slot::Done => {
                self.wasted_bytes += data.len() as u64;
                return out;
            }
            Slot::Asked(list) => {
                for (k, _) in list {
                    if k != key {
                        out.cancels.push(Req { peer: k, index, begin, length: data.len() as u32 });
                    }
                }
            }
            Slot::Free => {}
        }
        let s = begin as usize;
        ip.data[s..s + data.len()].copy_from_slice(data);
        ip.done += 1;
        ip.contributors.insert(key);
        out.useful = true;
        for c in &out.cancels {
            if let Some(p) = self.peers.get_mut(&c.peer) {
                p.outstanding.remove(&(index, b));
            }
        }
        if ip.done as usize == ip.blocks.len() {
            let ip = self.inprog.remove(&index).unwrap();
            out.completed = Some(Completed { index, data: ip.data, contributors: ip.contributors.into_iter().collect() });
        }
        out
    }

    /// The hash check failed: start the piece over.
    pub fn piece_failed(&mut self, index: u32) {
        self.inprog.remove(&index);
        for p in self.peers.values_mut() {
            p.outstanding.retain(|(i, _)| *i != index);
        }
    }

    pub fn in_progress(&self) -> usize {
        self.inprog.len()
    }

    /// Classes of the pieces right after each playhead (for status output).
    pub fn urgent_missing(&self) -> (usize, usize) {
        let c = self.classify();
        (c.values().filter(|(k, _)| *k == Class::Critical).count(), c.values().filter(|(k, _)| *k == Class::Window).count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PL: u64 = 64 * 1024; // 4 blocks per piece

    fn picker(mode: PickerMode) -> Picker {
        let cfg = PickerConfig { mode, window_bytes: 4 * PL, critical_bytes: PL, snub_secs: 5.0, ..PickerConfig::default() };
        Picker::new(cfg, PL, 32 * PL)
    }

    fn seeder(p: &mut Picker, key: PeerKey, now: Instant) {
        p.add_peer(key, now);
        p.peer_bitfield(key, Bitfield::full(32));
        p.peer_choking(key, false, now);
    }

    fn deliver(p: &mut Picker, r: &Req, now: Instant) -> BlockOutcome {
        p.block_received(r.peer, r.index, r.begin, &vec![0u8; r.length as usize], now)
    }

    #[test]
    fn starts_at_the_playhead() {
        let now = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, now);
        p.set_reader(1, 10 * PL + 5, 32 * PL);
        let reqs = p.pick(now);
        assert_eq!(reqs[0].index, 10, "first request should be the playhead piece");
        assert!(reqs.iter().take(4).all(|r| r.index == 10));
    }

    #[test]
    fn slow_peer_never_holds_the_critical_piece() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, t0); // fast
        seeder(&mut p, 2, t0); // slow
        // Build up rates: fast delivers lots, slow a trickle.
        let mut t = t0;
        for _ in 0..40 {
            t += Duration::from_millis(100);
            for r in p.pick(t) {
                if r.peer == 1 {
                    deliver(&mut p, &r, t);
                }
            }
        }
        // Slow peer is now snubbed / slow. Seek to piece 20.
        p.set_reader(7, 20 * PL, 32 * PL);
        t += Duration::from_millis(100);
        let reqs = p.pick(t);
        assert!(reqs.iter().filter(|r| r.index == 20).all(|r| r.peer == 1), "{reqs:?}");
    }

    #[test]
    fn stuck_critical_block_is_duplicated_to_a_faster_peer() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, t0);
        p.set_reader(1, 0, 32 * PL);
        let first = p.pick(t0);
        assert!(first.iter().any(|r| r.index == 0 && r.peer == 1));
        // Peer 1 goes silent; peer 2 arrives and is fast.
        seeder(&mut p, 2, t0);
        let mut t = t0;
        let mut dup = false;
        for _ in 0..30 {
            t += Duration::from_millis(100);
            for r in p.pick(t) {
                if r.peer == 2 {
                    if r.index == 0 {
                        dup = true;
                    }
                    deliver(&mut p, &r, t);
                }
            }
            if dup {
                break;
            }
        }
        assert!(dup, "piece 0 should have been re-requested from peer 2");
        assert!(p.duplicate_requests > 0);
    }

    #[test]
    fn duplicate_arrival_cancels_the_other_copy() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, t0);
        p.set_reader(1, 0, 32 * PL);
        let r1 = p.pick(t0).into_iter().find(|r| r.index == 0 && r.begin == 0).unwrap();
        seeder(&mut p, 2, t0);
        let t = t0 + Duration::from_secs(8); // peer 1 snubbed by now
        let r2 = p.pick(t).into_iter().find(|r| r.peer == 2 && r.index == 0 && r.begin == 0).expect("duplicate");
        let out = deliver(&mut p, &r2, t);
        assert!(out.useful);
        assert_eq!(out.cancels, vec![Req { peer: 1, ..r1.clone() }]);
        let late = deliver(&mut p, &r1, t);
        assert!(!late.useful);
        assert_eq!(p.wasted_bytes, r1.length as u64);
    }

    #[test]
    fn sequential_mode_never_duplicates() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Sequential);
        seeder(&mut p, 1, t0);
        p.set_reader(1, 0, 32 * PL);
        p.pick(t0);
        seeder(&mut p, 2, t0);
        let reqs = p.pick(t0 + Duration::from_secs(20));
        assert!(reqs.iter().all(|r| r.peer == 2));
        assert_eq!(p.duplicate_requests, 0);
        assert!(!reqs.iter().any(|r| r.index == 0 && r.begin == 0));
    }

    #[test]
    fn start_lead_is_fetched_from_its_far_end() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        p.cfg.background = false;
        p.set_start_lead(Some((0, 4 * PL)));
        seeder(&mut p, 1, t0);
        let reqs = p.pick(t0);
        assert_eq!(reqs.first().map(|r| r.index), Some(3), "{reqs:?}");
        assert!(reqs.iter().all(|r| r.index < 4), "nothing past the lead without background");
    }

    #[test]
    fn a_piece_the_other_client_is_slow_with_is_fetched_here_too() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        p.cfg.background = false;
        p.set_start_lead(Some((0, 4 * PL)));
        seeder(&mut p, 1, t0);
        for i in 0..4 {
            p.set_avoid(i, true, t0);
        }
        assert!(p.pick(t0).is_empty(), "left to the other client at first");
        // It reports the same pieces every second: that doesn't restart the clock.
        for i in 0..4 {
            p.set_avoid(i, true, t0 + Duration::from_secs(5));
        }
        assert!(p.pick(t0 + Duration::from_secs(5)).is_empty());
        let reqs = p.pick(t0 + Duration::from_secs(9));
        assert_eq!(reqs.first().map(|r| r.index), Some(3), "stuck there: fetched here, far end first");
        // Once the other client lets go of a piece and picks it up again, it gets its full turn again.
        p.set_avoid(2, false, t0 + Duration::from_secs(10));
        p.set_avoid(2, true, t0 + Duration::from_secs(10));
        p.piece_failed(2);
        assert!(p.pick(t0 + Duration::from_secs(11)).iter().all(|r| r.index != 2));
    }

    #[test]
    fn head_and_tail_are_fetched_even_while_the_other_client_is_on_them() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        p.cfg.background = false;
        p.set_prefetch(vec![(0, PL), (31 * PL, 32 * PL)]);
        seeder(&mut p, 1, t0);
        p.set_avoid(0, true, t0);
        p.set_avoid(31, true, t0);
        let pieces: HashSet<u32> = p.pick(t0).iter().map(|r| r.index).collect();
        assert_eq!(pieces, HashSet::from([0, 31]));
    }

    #[test]
    fn seek_cancels_background_requests() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, t0);
        p.peers.get_mut(&1).unwrap().rate = 40.0 * 16384.0 / 2.0; // 40-block pipeline
        p.set_reader(1, 0, 32 * PL);
        let reqs = p.pick(t0);
        assert!(reqs.iter().any(|r| r.index > 4), "background requested too");
        let cancels = p.set_reader(1, 24 * PL, 32 * PL);
        assert!(!cancels.is_empty());
        assert!(cancels.iter().all(|c| !(24..28).contains(&c.index)));
        assert_eq!(p.peers[&1].outstanding(), 0);
        let next = p.pick(t0 + Duration::from_millis(100));
        assert_eq!(next.first().map(|r| r.index), Some(24));
    }

    #[test]
    fn completes_and_only_wanted() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        p.set_wanted_range(4, 7, (4 * PL, 8 * PL));
        seeder(&mut p, 1, t0);
        let mut done = Vec::new();
        let mut t = t0;
        for _ in 0..50 {
            t += Duration::from_millis(100);
            for r in p.pick(t) {
                assert!((4..=7).contains(&r.index));
                if let Some(c) = deliver(&mut p, &r, t).completed {
                    p.mark_have(c.index);
                    done.push(c.index);
                }
            }
        }
        done.sort();
        assert_eq!(done, vec![4, 5, 6, 7]);
        assert_eq!(p.wanted_remaining(), 0);
    }

    #[test]
    fn choke_returns_requests() {
        let t0 = Instant::now();
        let mut p = picker(PickerMode::Deadline);
        seeder(&mut p, 1, t0);
        assert!(!p.pick(t0).is_empty());
        p.peer_choking(1, true, t0);
        assert_eq!(p.peers[&1].outstanding(), 0);
        seeder(&mut p, 2, t0);
        let r = p.pick(t0);
        assert!(r.iter().any(|r| r.index == 0 && r.begin == 0 && r.peer == 2));
    }
}
