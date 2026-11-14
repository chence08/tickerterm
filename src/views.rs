//! Full-screen visualisations: the 24h% history line plot and the distribution histograms.
//! Each fills `App::vlines` (one byte line per screen row below the two status lines).

use crate::{App, Row, ann, disp, hms, hum, now_ms, wide};
use std::io::Write;

/// How many symbols the history plot draws, and one colour for each.
const TOP: usize = 20;
const PAL: [u8; TOP] = [226, 51, 201, 46, 208, 39, 196, 255, 141, 118, 214, 87, 213, 154, 203, 75, 220, 48, 177, 250];
/// Columns for the label beside each line's end: symbol and current 24h%.
const LABW: usize = 18;
/// Braille dot bit for (x & 1, y & 3) inside a 2x4 cell.
const BITS: [[u8; 4]; 2] = [[0x01, 0x02, 0x04, 0x40], [0x08, 0x10, 0x20, 0x80]];
const BLK: [&str; 9] = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];
const LAB: usize = 9; // y-axis label columns

/// Braille canvas: every character cell is 2x4 addressable dots.
#[derive(Default)]
pub struct Canvas {
    w: usize,
    dots: Vec<u8>,
    /// which line (1-based) last touched the cell; it sets the cell's colour
    hi: Vec<u8>,
}

impl Canvas {
    fn reset(&mut self, w: usize, h: usize) {
        self.w = w;
        for v in [&mut self.dots, &mut self.hi] {
            v.clear();
            v.resize(w * h, 0);
        }
    }

    #[inline]
    fn set(&mut self, x: usize, y: usize, hi: u8) {
        let c = (y >> 2) * self.w + (x >> 1);
        self.dots[c] |= BITS[x & 1][y & 3];
        self.hi[c] = hi;
    }
}

impl App {
    /// Line plot of 24h% from launch to now, for the top symbols by volume in the current filter.
    /// `pick` re-selects that set; otherwise it is held (and re-picked once a minute) so lines
    /// and colours do not swap as volumes jostle around the cut-off.
    pub(crate) fn draw_plot(&mut self, w: usize, nl: usize, pick: bool) {
        let mut lines = std::mem::take(&mut self.vlines);
        let mut cv = std::mem::take(&mut self.canvas);
        let mut fbuf = std::mem::take(&mut self.fbuf);
        let mut bases = std::mem::take(&mut self.bases);
        let mut set = std::mem::take(&mut self.plot_set);
        let mut pairs = std::mem::take(&mut self.pairs);
        lines.resize_with(nl, Vec::new);
        lines.iter_mut().for_each(Vec::clear);
        'body: {
            if nl < 8 || w < 60 {
                lines[0].extend_from_slice(b" terminal too small for the plot");
                break 'body;
            }
            let (cw, ch) = (w - LAB - LABW, nl - 3);
            let (pw, ph) = (cw * 2, ch * 4);
            let (hist, rows, order) = (&self.hist, &self.rows, &self.order);
            let (rebase, full) = (self.plot_rebase, self.plot_full);

            // one label row per line, so never more lines than rows
            let want = TOP.min(ch).min(order.len());
            if pick || set.len() != want || self.plot_set_at.elapsed().as_secs() >= 60 {
                pairs.clear();
                pairs.extend(order.iter().map(|&s| (rows[s as usize].qvol, s)));
                pairs.sort_unstable_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
                set.clear();
                set.extend(pairs.iter().take(want).map(|p| p.1));
                self.plot_set_at = std::time::Instant::now();
            }
            if set.is_empty() {
                lines[0].extend_from_slice(b" 24h% HISTORY | no data for the current filter yet");
                break 'body;
            }

            // samples 0..nh come from history; index nh is the live value
            let nh = hist.len();
            let n = nh + 1;
            let val = |i: usize, sym: usize| if i >= nh { rows[sym].chg as f32 } else { hist.get(i, sym) };
            // value at pixel column x, linearly interpolated between the two nearest samples
            let step = (n - 1) as f32 / (pw - 1) as f32;
            let at = |x: usize, sym: usize| {
                let f = x as f32 * step;
                let i = (f as usize).min(n - 1);
                let (v0, fr) = (val(i, sym), f - i as f32);
                if fr < 1e-4 || i + 1 >= n {
                    return v0;
                }
                let v1 = val(i + 1, sym);
                if v1.is_finite() { v0 + (v1 - v0) * fr } else { v0 }
            };

            bases.clear();
            for &s in &set {
                let s = s as usize;
                bases.push(if rebase { (0..n).map(|i| val(i, s)).find(|v| v.is_finite()).unwrap_or(0.0) } else { 0.0 });
            }

            fbuf.clear();
            for (k, &s) in set.iter().enumerate() {
                for x in 0..pw {
                    let v = at(x, s as usize) - bases[k];
                    if v.is_finite() {
                        fbuf.push(v);
                    }
                }
            }
            if fbuf.is_empty() {
                lines[0].extend_from_slice(b" 24h% HISTORY | collecting data");
                break 'body;
            }
            let (mut lo, mut hi) = if full {
                fbuf.iter().fold((f32::MAX, f32::MIN), |(a, b), &v| (a.min(v), b.max(v)))
            } else {
                // 1st..99th percentile, so one runaway symbol does not flatten everything else
                let (a, b) = ((fbuf.len() - 1) / 100, (fbuf.len() - 1) * 99 / 100);
                let lo = *fbuf.select_nth_unstable_by(a, f32::total_cmp).1;
                let hi = *fbuf.select_nth_unstable_by(b, f32::total_cmp).1;
                (lo, hi)
            };
            let pad = ((hi - lo) * 0.05).max(0.02);
            lo -= pad;
            hi += pad;
            let scale = (ph - 1) as f32 / (hi - lo);
            let ypix = |v: f32| (((hi - v) * scale).round() as i64).clamp(-1, ph as i64) as i32;

            // lowest volume first, so the biggest symbols end up drawn on top
            cv.reset(cw, ch);
            for (k, &s) in set.iter().enumerate().rev() {
                let mut prev: Option<i32> = None;
                for x in 0..pw {
                    let v = at(x, s as usize) - bases[k];
                    if !v.is_finite() {
                        prev = None;
                        continue;
                    }
                    let y = ypix(v);
                    // join to the previous column so the line is continuous; clip to the canvas
                    let (a, b) = match prev {
                        Some(p) if p < y => (p + 1, y),
                        Some(p) if p > y => (y, p - 1),
                        _ => (y, y),
                    };
                    for yy in a.max(0)..=b.min(ph as i32 - 1) {
                        cv.set(x, yy as usize, k as u8 + 1);
                    }
                    prev = Some(y);
                }
            }

            // ---- labels: each at the row where its line ends, nudged apart so none overlap
            let mut lab: Vec<(usize, usize)> = set
                .iter()
                .enumerate()
                .map(|(k, &s)| {
                    let v = rows[s as usize].chg as f32 - bases[k];
                    ((ypix(v).clamp(0, ph as i32 - 1) as usize) / 4, k)
                })
                .collect();
            lab.sort_unstable();
            for i in 1..lab.len() {
                lab[i].0 = lab[i].0.max(lab[i - 1].0 + 1);
            }
            let mut limit = ch;
            for l in lab.iter_mut().rev() {
                l.0 = l.0.min(limit - 1);
                limit = l.0;
            }
            let mut label_at = vec![usize::MAX; ch];
            for &(r, k) in &lab {
                label_at[r] = k;
            }

            // ---- title
            let _ = write!(
                lines[0],
                "\x1b[0;1m 24h% HISTORY\x1b[0m | top {} of {} by volume | {} | range {} [{:+.2}% .. {:+.2}%] | {} samples, one per {}s",
                set.len(),
                order.len(),
                if rebase { "CHANGE SINCE START" } else { "ABSOLUTE" },
                if full { "FULL" } else { "p1-p99" },
                lo,
                hi,
                nh,
                hist.interval_secs()
            );

            // ---- canvas
            let zero_row = (lo < 0.0 && hi > 0.0).then(|| ((hi * scale).round() as usize / 4).min(ch - 1));
            for r in 0..ch {
                let l = &mut lines[1 + r];
                if r == 0 || r == ch - 1 || r == ch / 2 || r == ch / 4 || r == 3 * ch / 4 || Some(r) == zero_row {
                    let v = if Some(r) == zero_row { 0.0 } else { hi - (r * 4 + 2) as f32 / scale };
                    let _ = write!(l, "\x1b[0;2m{:>+8.2} ", v);
                } else {
                    l.extend_from_slice(b"\x1b[0m         ");
                }
                let mut cur = 0u8;
                for c in 0..cw {
                    let i = r * cw + c;
                    let d = cv.dots[i];
                    if d == 0 && Some(r) != zero_row {
                        l.push(b' ');
                        continue;
                    }
                    let col = if d == 0 { 240 } else { PAL[cv.hi[i] as usize - 1] };
                    if col != cur {
                        let _ = write!(l, "\x1b[0;38;5;{}m", col);
                        cur = col;
                    }
                    if d == 0 {
                        l.push(b'-');
                    } else {
                        // U+2800 + dot mask, as UTF-8
                        l.extend_from_slice(&[0xE2, 0xA0 | (d >> 6), 0x80 | (d & 0x3F)]);
                    }
                }
                if label_at[r] != usize::MAX {
                    let k = label_at[r];
                    let row = &rows[set[k] as usize];
                    let _ = write!(l, "\x1b[0;38;5;{}m ", PAL[k]);
                    let mut used = 0;
                    for chr in disp(row).chars() {
                        if used + wide(chr) > 9 {
                            break;
                        }
                        used += wide(chr);
                        let mut u = [0u8; 4];
                        l.extend_from_slice(chr.encode_utf8(&mut u).as_bytes());
                    }
                    l.resize(l.len() + 9 - used, b' ');
                    let _ = write!(l, "{:>+7.2}%", row.chg);
                }
                l.extend_from_slice(b"\x1b[0m");
            }

            // ---- x axis
            let l = &mut lines[ch + 1];
            l.extend_from_slice(b"\x1b[0;2m         ");
            let t0 = if nh > 0 { hist.time(0) } else { now_ms() };
            let t1 = now_ms();
            hms(l, t0, false);
            let mut mid = Vec::new();
            mid.extend_from_slice(b"span ");
            hms(&mut mid, t1.saturating_sub(t0), false);
            mid.extend_from_slice(b" UTC");
            let gap = cw.saturating_sub(16 + mid.len());
            l.resize(l.len() + gap / 2, b' ');
            l.extend_from_slice(&mid);
            l.resize(l.len() + gap - gap / 2, b' ');
            hms(l, t1, false);
            l.extend_from_slice(b"  symbol     24h%\x1b[0m");

            lines[ch + 2].extend_from_slice(
                b"\x1b[0;2m p/esc board | d distributions | x absolute/change since start | z p1-p99/full range | m t / filters apply | q quit\x1b[0m",
            );
        }
        self.vlines = lines;
        self.canvas = cv;
        self.fbuf = fbuf;
        self.bases = bases;
        self.plot_set = set;
        self.pairs = pairs;
    }

    /// Histograms of 24h%, quote volume and annualised funding for the current filter.
    pub(crate) fn draw_dist(&mut self, w: usize, nl: usize) {
        let mut lines = std::mem::take(&mut self.vlines);
        let mut vals = std::mem::take(&mut self.pairs);
        lines.resize_with(nl, Vec::new);
        lines.iter_mut().for_each(Vec::clear);
        if nl < 13 || w < 50 {
            lines[0].extend_from_slice(b" terminal too small for the histograms");
        } else {
            let ph = (nl - 1) / 3;
            let (rows, order) = (&self.rows, &self.order);

            vals.clear();
            vals.extend(order.iter().map(|&s| (rows[s as usize].chg, s)));
            panel(&mut lines[..ph], "24h% CHANGE", &mut vals, rows, w, Kind::Percent(1), "");

            vals.clear();
            vals.extend(order.iter().map(|&s| (rows[s as usize].qvol, s)));
            panel(&mut lines[ph..2 * ph], "24h QUOTE VOLUME (log scale)", &mut vals, rows, w, Kind::Log, "");

            // rate per funding interval -> per year: intervals per year = 8760h / interval
            vals.clear();
            vals.extend(order.iter().map(|&s| &rows[s as usize]).zip(order).filter(|(r, _)| r.has_fund).map(
                |(r, &s)| (ann(r), s),
            ));
            let age = self.fund_at.and_then(|t| t.elapsed().ok()).map(|d| d.as_secs() / 60);
            let note = match age {
                Some(m) => format!("{}, fetched {}m ago", self.fund_status, m),
                None => self.fund_status.clone(),
            };
            panel(&mut lines[2 * ph..3 * ph], "ANNUALISED FUNDING RATE", &mut vals, rows, w, Kind::Percent(2), &note);

            lines[nl - 1].extend_from_slice(
                b"\x1b[0;2m d/esc board | p 24h% history plot | m t / filters apply | q quit\x1b[0m",
            );
        }
        self.vlines = lines;
        self.pairs = vals;
    }
}

enum Kind {
    /// Signed percentages on a linear axis spanning the given percentile and its mirror
    /// (1 = p1..p99); values outside are counted in the axis labels rather than drawn.
    Percent(usize),
    /// Positive magnitudes on a log10 axis.
    Log,
}

/// Write `text` centred on column `pos` of `axis` if that span is still blank.
fn place(axis: &mut [u8], pos: usize, text: &[u8]) {
    let start = pos.saturating_sub(text.len() / 2).min(axis.len().saturating_sub(text.len()));
    if text.len() <= axis.len() && axis[start..start + text.len()].iter().all(|&c| c == b' ') {
        axis[start..start + text.len()].copy_from_slice(text);
    }
}

fn panel(out: &mut [Vec<u8>], title: &str, vals: &mut Vec<(f64, u32)>, rows: &[Row], w: usize, kind: Kind, note: &str) {
    let log = matches!(kind, Kind::Log);
    let nb = w - 2; // one column per bin
    let bh = out.len() - 2;
    vals.retain(|v| v.0.is_finite() && (!log || v.0 > 0.0));
    let _ = write!(out[0], "\x1b[0;1m {}\x1b[0m", title);
    if !note.is_empty() {
        let _ = write!(out[0], " \x1b[2m({})\x1b[0m", note);
    }
    if vals.is_empty() {
        out[0].extend_from_slice(b" | no data yet");
        return;
    }
    vals.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    let n = vals.len();
    let tx = |v: f64| if log { v.log10() } else { v };
    let q = |p: usize| vals[(n - 1) * p / 100].0;
    let (mut lo, mut hi) = match kind {
        Kind::Log => (tx(vals[0].0), tx(vals[n - 1].0)),
        Kind::Percent(p) => (q(p), q(100 - p)),
    };
    if hi - lo < 1e-9 {
        lo -= 0.5;
        hi += 0.5;
    }
    let bin = |v: f64| (((tx(v) - lo) / (hi - lo) * nb as f64) as isize).clamp(0, nb as isize - 1) as usize;
    let mut counts = vec![0u32; nb];
    let (mut below, mut above) = (0u32, 0u32);
    for v in vals.iter() {
        match tx(v.0) {
            x if x < lo => below += 1,
            x if x > hi => above += 1,
            _ => counts[bin(v.0)] += 1,
        }
    }
    let maxc = counts.iter().copied().max().unwrap_or(1).max(1);

    // ---- stats
    let (min, max) = (vals[0], vals[n - 1]);
    let s = &mut out[0];
    let _ = write!(s, " | n {}", n);
    if log {
        s.extend_from_slice(b" | median ");
        hum(s, q(50), 0);
        s.extend_from_slice(b" | total ");
        hum(s, vals.iter().map(|v| v.0).sum(), 0);
        let _ = write!(s, " | min {} ", disp(&rows[min.1 as usize]));
        hum(s, min.0, 0);
        let _ = write!(s, " | max {} ", disp(&rows[max.1 as usize]));
        hum(s, max.0, 0);
    } else {
        let mean = vals.iter().map(|v| v.0).sum::<f64>() / n as f64;
        let sd = (vals.iter().map(|v| (v.0 - mean).powi(2)).sum::<f64>() / n as f64).sqrt();
        let pos = vals.iter().filter(|v| v.0 > 0.0).count();
        let neg = vals.iter().filter(|v| v.0 < 0.0).count();
        let _ = write!(
            s,
            " | mean {:+.2}% | median {:+.2}% | sd {:.2} | \x1b[32m+{}\x1b[0m \x1b[31m-{}\x1b[0m | min {} {:+.2}% | max {} {:+.2}%",
            mean,
            q(50),
            sd,
            pos,
            neg,
            disp(&rows[min.1 as usize]),
            min.0,
            disp(&rows[max.1 as usize]),
            max.0
        );
    }
    let _ = write!(s, " | tallest bin {}", maxc);

    // ---- bars, in eighths of a row
    for r in 0..bh {
        let l = &mut out[1 + r];
        l.push(b' ');
        let mut cur: &[u8] = b"";
        for (b, &c) in counts.iter().enumerate() {
            let h8 = (c as usize * bh * 8).div_ceil(maxc as usize);
            let lvl = h8.saturating_sub((bh - 1 - r) * 8).min(8);
            if lvl > 0 {
                let centre = lo + (b as f64 + 0.5) / nb as f64 * (hi - lo);
                let col: &[u8] = match (log, centre < 0.0) {
                    (true, _) => b"\x1b[0;36m",
                    (_, true) => b"\x1b[0;31m",
                    _ => b"\x1b[0;32m",
                };
                if col != cur {
                    l.extend_from_slice(col);
                    cur = col;
                }
            }
            l.extend_from_slice(BLK[lvl].as_bytes());
        }
        l.extend_from_slice(b"\x1b[0m");
    }

    // ---- axis
    let mut axis = vec![b' '; nb];
    let mut t = Vec::new();
    if log {
        for d in lo.ceil() as i32..=hi.floor() as i32 {
            t.clear();
            hum(&mut t, 10f64.powi(d), 0);
            place(&mut axis, bin(10f64.powi(d)), &t);
        }
    } else {
        let _ = if below > 0 { write!(t, "({} below) {:+.1}%", below, lo) } else { write!(t, "{:+.1}%", lo) };
        place(&mut axis, 0, &t);
        t.clear();
        let _ = if above > 0 { write!(t, "{:+.1}% ({} above)", hi, above) } else { write!(t, "{:+.1}%", hi) };
        place(&mut axis, nb - 1, &t);
        if lo < 0.0 && hi > 0.0 {
            place(&mut axis, bin(0.0), b"0");
        }
        for f in [0.25, 0.5, 0.75] {
            t.clear();
            let _ = write!(t, "{:+.1}%", lo + f * (hi - lo));
            place(&mut axis, (f * nb as f64) as usize, &t);
        }
    }
    let l = &mut out[bh + 1];
    l.extend_from_slice(b"\x1b[0;2m ");
    l.extend_from_slice(&axis);
    l.extend_from_slice(b"\x1b[0m");
}
