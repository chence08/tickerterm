//! Liquidation flow: USD notional of forced orders per side, in fixed 10-second buckets.
//!
//! One hour of buckets per side lives in two fixed rings (no allocation after start), which give the
//! rolling 1m / 5m / 1h counters and the timeline. The last five minutes of individual prints are kept
//! as well, for the per-symbol totals and the tape.

use std::collections::VecDeque;

pub const BUCKET_MS: u64 = 10_000;
/// Buckets kept per side: one hour.
pub const NB: usize = 360;
/// A single liquidation at or above this notional is "large" and drawn in the deep colour.
pub const BIG: f64 = 1e6;
/// How long individual prints are kept, in buckets (5 minutes).
const KEEP: u64 = 30;
const MAX_EVS: usize = 8192;

/// Side of the *position* that was liquidated. A forced SELL closes a long.
pub const SHORTS: usize = 0;
pub const LONGS: usize = 1;

pub struct Ev {
    pub t: u64,
    pub row: u32,
    pub side: u8,
    pub usd: f64,
    px: [u8; 15],
    pxl: u8,
}
impl Ev {
    pub fn price(&self) -> &[u8] {
        &self.px[..self.pxl as usize]
    }
}

pub struct Flow {
    sum: [[f64; NB]; 2],
    big: [[bool; NB]; 2],
    /// absolute index (wall ms / BUCKET_MS) of the newest bucket
    head: u64,
    /// bucket in which recording started
    first: u64,
    pub evs: VecDeque<Ev>,
    /// bumped whenever anything the footer shows may have changed
    pub generation: u64,
    pub seen: bool,
    acc: Vec<(f64, bool)>,
}

impl Flow {
    pub fn new(wall: u64) -> Self {
        let head = wall / BUCKET_MS;
        Flow {
            sum: [[0.0; NB]; 2],
            big: [[false; NB]; 2],
            head,
            first: head,
            evs: VecDeque::with_capacity(256),
            generation: 0,
            seen: false,
            acc: Vec::new(),
        }
    }

    /// Roll the rings forward to `wall`, zeroing the buckets that open up.
    pub fn advance(&mut self, wall: u64) {
        let now = wall / BUCKET_MS;
        if now <= self.head {
            return;
        }
        let from = if now - self.head >= NB as u64 { now - NB as u64 + 1 } else { self.head + 1 };
        for b in from..=now {
            let i = (b % NB as u64) as usize;
            for s in 0..2 {
                self.sum[s][i] = 0.0;
                self.big[s][i] = false;
            }
        }
        self.head = now;
        while self.evs.front().is_some_and(|e| e.t / BUCKET_MS + KEEP <= now) {
            self.evs.pop_front();
        }
        self.generation += 1;
    }

    pub fn add(&mut self, wall: u64, row: u32, side: usize, usd: f64, price: &str) {
        self.advance(wall);
        let i = (self.head % NB as u64) as usize;
        self.sum[side][i] += usd;
        self.big[side][i] |= usd >= BIG;
        // prices arrive zero-padded ("865.98000"): keep the significant part
        let p = if price.contains('.') { price.trim_end_matches('0').trim_end_matches('.') } else { price };
        let p = &p.as_bytes()[..p.len().min(15)];
        let mut px = [0u8; 15];
        px[..p.len()].copy_from_slice(p);
        if self.evs.len() == MAX_EVS {
            self.evs.pop_front();
        }
        self.evs.push_back(Ev { t: wall, row, side: side as u8, usd, px, pxl: p.len() as u8 });
        self.seen = true;
        self.generation += 1;
    }

    /// Notional and large-print flag of the bucket `age` buckets back (0 = the one filling now).
    pub fn bucket(&self, side: usize, age: usize) -> (f64, bool) {
        if age >= NB {
            return (0.0, false);
        }
        let i = ((self.head + NB as u64 - age as u64) % NB as u64) as usize;
        (self.sum[side][i], self.big[side][i])
    }

    /// Total over the newest `n` buckets.
    pub fn window(&self, side: usize, n: usize) -> f64 {
        (0..n.min(NB)).map(|a| self.bucket(side, a).0).sum()
    }

    /// The last minute against this side's own average minute over the rest of the hour:
    /// 1.0 is a normal minute. None until there are five minutes of history to compare with.
    pub fn surge(&self, side: usize) -> Option<f64> {
        let have = ((self.head - self.first + 1) as usize).min(NB);
        if have < 36 {
            return None;
        }
        let m1 = self.window(side, 6);
        let rest = self.window(side, have) - m1;
        if rest <= 0.0 {
            return None;
        }
        Some(m1 / (rest / (have - 6) as f64 * 6.0))
    }

    /// Per-symbol totals over the kept prints for one side, largest first, into `out` as
    /// (row, notional, had a large print). O(prints), using a dense scratch table indexed by row.
    pub fn tops(&mut self, side: usize, n_rows: usize, out: &mut Vec<(u32, f64, bool)>) {
        out.clear();
        if self.acc.len() < n_rows {
            self.acc.resize(n_rows, (0.0, false));
        }
        for e in &self.evs {
            if e.side as usize != side {
                continue;
            }
            let a = &mut self.acc[e.row as usize];
            if a.0 == 0.0 {
                out.push((e.row, 0.0, false));
            }
            a.0 += e.usd;
            a.1 |= e.usd >= BIG;
        }
        for o in out.iter_mut() {
            let a = &mut self.acc[o.0 as usize];
            (o.1, o.2) = *a;
            *a = (0.0, false);
        }
        out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_roll_and_expire() {
        let t0 = 1_000_000 * BUCKET_MS;
        let mut f = Flow::new(t0);
        f.add(t0, 0, LONGS, 500_000.0, "1.2300");
        f.add(t0 + 1, 1, LONGS, 2_000_000.0, "65000");
        f.add(t0 + 2, 0, SHORTS, 10.0, "1.0");
        assert_eq!(f.window(LONGS, 6), 2_500_000.0);
        assert_eq!(f.bucket(LONGS, 0), (2_500_000.0, true));
        assert_eq!(f.bucket(SHORTS, 0), (10.0, false));
        assert_eq!(f.evs[0].price(), b"1.23");
        assert_eq!(f.evs[1].price(), b"65000");

        // seven buckets later it has left the 1m window but not the 5m one
        f.advance(t0 + 7 * BUCKET_MS);
        assert_eq!(f.window(LONGS, 6), 0.0);
        assert_eq!(f.window(LONGS, 30), 2_500_000.0);
        assert_eq!(f.bucket(LONGS, 7), (2_500_000.0, true));
        assert_eq!(f.evs.len(), 3);

        // prints are dropped after five minutes, buckets after an hour
        f.advance(t0 + 30 * BUCKET_MS);
        assert!(f.evs.is_empty());
        assert_eq!(f.window(LONGS, NB), 2_500_000.0);
        f.advance(t0 + NB as u64 * BUCKET_MS);
        assert_eq!(f.window(LONGS, NB), 0.0);
        // a long gap (sleep, reconnect) clears everything without walking the whole gap
        f.add(t0 + 400 * BUCKET_MS, 0, SHORTS, 5.0, "1");
        f.advance(t0 + 1_000_000 * BUCKET_MS);
        assert_eq!(f.window(SHORTS, NB), 0.0);
    }

    #[test]
    fn tops_and_surge() {
        let t0 = 2_000_000 * BUCKET_MS;
        let mut f = Flow::new(t0);
        assert!(f.surge(LONGS).is_none());
        // ten quiet minutes at 60K a minute, then a 1.2M minute
        for b in 0..60u64 {
            f.add(t0 + b * BUCKET_MS, 3, LONGS, 10_000.0, "1");
        }
        for b in 60..66u64 {
            f.add(t0 + b * BUCKET_MS, 5, LONGS, 200_000.0, "1");
        }
        let s = f.surge(LONGS).unwrap();
        assert!((s - 20.0).abs() < 1e-9, "{s}");
        f.add(t0 + 65 * BUCKET_MS, 7, LONGS, 1_500_000.0, "1");
        f.add(t0 + 65 * BUCKET_MS, 9, SHORTS, 1.0, "1");
        let mut out = Vec::new();
        f.tops(LONGS, 16, &mut out);
        assert_eq!(out[0], (7, 1_500_000.0, true));
        assert_eq!(out[1], (5, 1_200_000.0, false));
        assert_eq!(out[2].0, 3);
        assert_eq!(out.len(), 3);
        f.tops(SHORTS, 16, &mut out);
        assert_eq!(out, vec![(9, 1.0, false)]);
    }
}

// ---- footer rendering

use crate::{App, disp, hms, hum, wide};
use std::io::Write;

const BLK: [&str; 9] = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];
/// 256-colour (pale, deep) per side: pale for ordinary flow, deep where a single print was >= BIG.
const COL: [(u8, u8); 2] = [(151, 34), (217, 160)];
const LABEL: [&str; 2] = [" LIQ SHORTS", " LIQ LONGS "];
/// Columns taken by the label, the three counters and the surge ratio.
const LEFT: usize = 44;
/// Columns the timeline needs besides its cells: scale label and the two end caps.
const TL_EXTRA: usize = 9;

impl App {
    /// Build the two footer lines (shorts liquidated above, longs below) into `self.flines`.
    /// Rebuilt only when a liquidation arrived, a 10s bucket rolled, or the layout changed.
    pub(crate) fn draw_flow(&mut self, w: usize) {
        let key = (self.flow.generation, w, self.tape);
        if key == self.fkey {
            return;
        }
        self.fkey = key;

        let avail = w.saturating_sub(LEFT);
        // the tape's entries are longer than the per-symbol totals, so it takes more of the line
        let side_w = if avail < 45 { 0 } else if self.tape { avail / 2 } else { (avail * 2 / 5).min(72) };
        let tl = if avail - side_w >= TL_EXTRA + 8 { (avail - side_w - TL_EXTRA).min(NB) } else { 0 };
        let right = avail - if tl > 0 { tl + TL_EXTRA } else { 0 };
        // both sides share one scale: a full-height bar is the largest visible bucket, at least BIG
        let mut full = BIG;
        for a in 0..tl {
            full = full.max(self.flow.bucket(SHORTS, a).0).max(self.flow.bucket(LONGS, a).0);
        }

        for side in [SHORTS, LONGS] {
            let (pale, deep) = COL[side];
            let m1 = self.flow.window(side, 6);
            let surge = self.flow.surge(side);
            // a cascade: a heavy minute that is also far above this side's normal
            let hot = m1 >= BIG && surge.is_none_or(|x| x >= 5.0);
            self.flow.tops(side, self.rows.len(), &mut self.ftops);
            let s = &mut self.flines[side];
            s.clear();
            let _ = write!(s, "\x1b[0;{}38;5;{}m{}\x1b[0m", if hot { "1;7;" } else { "" }, if hot { deep } else { pale }, LABEL[side]);
            for (lab, n) in [("1m", 6), ("5m", 30), ("1h", NB)] {
                let _ = write!(s, "\x1b[0;2m {}\x1b[0;38;5;{}m", lab, pale);
                hum(s, if n == 6 { m1 } else { self.flow.window(side, n) }, 6);
            }
            match surge {
                Some(x) if x >= 5.0 => {
                    let _ = write!(s, "\x1b[0;1;38;5;{}m {:>5}", deep, format!("x{:.0}", x.min(9999.0)));
                }
                Some(x) => {
                    let _ = write!(s, "\x1b[0;2m {:>5}", format!("x{:.1}", x));
                }
                None => s.extend_from_slice(b"\x1b[0;2m     -"),
            }

            if tl > 0 {
                s.extend_from_slice(b"\x1b[0;2m");
                if side == SHORTS {
                    let secs = tl * (BUCKET_MS / 1000) as usize;
                    let _ = write!(s, " {:>6}", format!("-{}m", secs.div_ceil(60)));
                } else {
                    s.extend_from_slice(" █".as_bytes());
                    hum(s, full, 5);
                }
                s.extend_from_slice("▕".as_bytes());
                let mut cur = 0u8; // colour currently set, 0 = plain
                for age in (0..tl).rev() {
                    let (v, big) = self.flow.bucket(side, age);
                    let lvl = if v <= 0.0 { 0 } else { (((v / full).sqrt() * 8.0).ceil() as usize).clamp(1, 8) };
                    let want = if lvl == 0 { 0 } else if big { deep } else { pale };
                    if want != cur {
                        cur = want;
                        // longs hang down from the axis: reverse video turns a low block into a high one
                        let _ = match (want, side) {
                            (0, _) => write!(s, "\x1b[0m"),
                            (c, SHORTS) => write!(s, "\x1b[0;38;5;{}m", c),
                            (c, _) => write!(s, "\x1b[0;7;38;5;{}m", c),
                        };
                    }
                    let g = if side == SHORTS || lvl == 0 { BLK[lvl] } else { BLK[8 - lvl] };
                    s.extend_from_slice(g.as_bytes());
                }
                s.extend_from_slice("\x1b[0;2m▏".as_bytes());
            }

            // right of the timeline: who was hit (5m totals per symbol), or the latest prints
            if right >= 24 {
                let _ = write!(s, "\x1b[0;2m {}", if self.tape { "last" } else { "5m" });
                let mut used = 5;
                if self.tape {
                    for e in self.flow.evs.iter().rev().filter(|e| e.side as usize == side) {
                        let name = disp(&self.rows[e.row as usize]);
                        let need = 19 + name.chars().map(wide).sum::<usize>() + e.price().len();
                        if used + need > right {
                            break;
                        }
                        used += need;
                        s.extend_from_slice(b"\x1b[0;2m ");
                        hms(s, e.t, false);
                        let big = e.usd >= BIG;
                        let _ = write!(s, "\x1b[0;{}38;5;{}m {} ", if big { "1;" } else { "" }, if big { deep } else { pale }, name);
                        hum(s, e.usd, 0);
                        s.extend_from_slice(b" @");
                        s.extend_from_slice(e.price());
                        s.push(b' ');
                    }
                } else {
                    for &(row, usd, big) in &self.ftops {
                        let name = disp(&self.rows[row as usize]);
                        let need = 9 + name.chars().map(wide).sum::<usize>();
                        if used + need > right {
                            break;
                        }
                        used += need;
                        let _ = write!(s, "\x1b[0;{}38;5;{}m  {} ", if big { "1;" } else { "" }, if big { deep } else { pale }, name);
                        hum(s, usd, 0);
                    }
                }
            }
            s.extend_from_slice(b"\x1b[0m");
        }
    }
}
