//! Bounded history of one value per symbol, sampled from launch.
//!
//! Fixed capacity, whole-run coverage: when the buffer fills, every other sample is dropped and
//! the sampling interval doubles. Appends are O(symbols), memory never grows, and there are always
//! between cap/2 and cap evenly spaced samples spanning the whole run - more than a terminal is wide.

use std::time::{Duration, Instant};

pub struct History {
    cap: usize,
    stride: usize,
    data: Vec<f32>, // row-major: one row of `stride` values per sample
    times: Vec<u64>,
    len: usize,
    interval: Duration,
    next: Instant,
}

impl History {
    pub fn new(cap: usize, now: Instant) -> Self {
        let stride = 1024;
        History {
            cap,
            stride,
            data: vec![f32::NAN; cap * stride],
            times: vec![0; cap],
            len: 0,
            interval: Duration::from_secs(1),
            next: now,
        }
    }

    #[inline]
    pub fn due(&self, now: Instant) -> bool {
        now >= self.next
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval.as_secs()
    }

    /// Wall-clock ms of sample `i`.
    pub fn time(&self, i: usize) -> u64 {
        self.times[i]
    }

    #[inline]
    pub fn get(&self, i: usize, sym: usize) -> f32 {
        if sym < self.stride { self.data[i * self.stride + sym] } else { f32::NAN }
    }

    /// Record one sample: `value(sym)` for each of `n_syms` symbols (NaN = no data yet).
    pub fn push(&mut self, now: Instant, wall_ms: u64, n_syms: usize, value: impl Fn(usize) -> f32) {
        if n_syms > self.stride {
            self.grow(n_syms);
        }
        if self.len == self.cap {
            self.compact();
        }
        let row = &mut self.data[self.len * self.stride..(self.len + 1) * self.stride];
        for (i, slot) in row.iter_mut().enumerate() {
            *slot = if i < n_syms { value(i) } else { f32::NAN };
        }
        self.times[self.len] = wall_ms;
        self.len += 1;
        self.next += self.interval;
        if self.next <= now {
            self.next = now + self.interval;
        }
    }

    /// Keep the later sample of each pair, so the newest sample survives and spacing stays even.
    fn compact(&mut self) {
        let s = self.stride;
        for i in 0..self.cap / 2 {
            let src = 2 * i + 1;
            self.data.copy_within(src * s..(src + 1) * s, i * s);
            self.times[i] = self.times[src];
        }
        self.len = self.cap / 2;
        self.interval *= 2;
    }

    fn grow(&mut self, need: usize) {
        let ns = need.next_power_of_two();
        let mut data = vec![f32::NAN; self.cap * ns];
        for i in 0..self.len {
            data[i * ns..i * ns + self.stride].copy_from_slice(&self.data[i * self.stride..(i + 1) * self.stride]);
        }
        self.data = data;
        self.stride = ns;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_keeps_newest_and_even_spacing() {
        let t = Instant::now();
        let mut h = History::new(8, t);
        for i in 0..8u64 {
            h.push(t, i, 2, |s| (i * 10 + s as u64) as f32);
        }
        assert_eq!((h.len(), h.interval_secs()), (8, 1));
        h.push(t, 8, 2, |s| (80 + s) as f32); // triggers compaction first
        assert_eq!((h.len(), h.interval_secs()), (5, 2));
        let times: Vec<u64> = (0..h.len()).map(|i| h.time(i)).collect();
        assert_eq!(times, [1, 3, 5, 7, 8]);
        assert_eq!(h.get(3, 1), 71.0);
        assert_eq!(h.get(4, 0), 80.0);
    }

    #[test]
    fn grows_for_more_symbols_and_pads_with_nan() {
        let t = Instant::now();
        let mut h = History::new(4, t);
        h.push(t, 0, 3, |s| s as f32);
        h.push(t, 1, 3000, |s| s as f32 * 2.0);
        assert_eq!(h.get(0, 2), 2.0);
        assert!(h.get(0, 2999).is_nan());
        assert_eq!(h.get(1, 2999), 5998.0);
        assert!(h.get(1, 5000).is_nan());
    }
}
