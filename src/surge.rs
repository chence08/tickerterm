//! Per-symbol traded volume in 10-second buckets over the last 15 minutes, to spot volume surges.
//!
//! The ticker stream carries no per-interval volume, only the rolling 24h quote volume. Its change
//! between two pushes is what traded in between minus what dropped out of the far end of the 24h
//! window; the caller adds back the expected drop-out (the 24h average rate) before feeding it here.
//!
//! Storage is one flat f32 ring (90 buckets per symbol, ~370 KB for 1024 symbols) plus a running
//! 15-minute sum per symbol, so reading a symbol's surge level is O(1).

pub const BUCKET_MS: u64 = 10_000;
/// Buckets per symbol: 15 minutes.
pub const NB: usize = 90;
/// Brightness steps of the glow (0 = none).
pub const STEPS: u8 = 12;
/// History needed before anything is flagged, in buckets (2 minutes).
const WARMUP: u64 = 12;
/// The recent rate has to be this many times the 15-minute average to start glowing...
const LO: f64 = 2.0;
/// ...and this many times for full brightness. Brightness is linear in the log of the ratio between.
const HI: f64 = 16.0;
/// Ignore surges worth less than this in the recent window (USD): one fill on a dead market is not a surge.
const MIN_USD: f64 = 5_000.0;

pub struct Vols {
    ring: Vec<f32>,
    sum: Vec<f64>,
    head: u64,
    first: u64,
}

impl Vols {
    pub fn new(wall: u64) -> Self {
        let head = wall / BUCKET_MS;
        Vols { ring: Vec::new(), sum: Vec::new(), head, first: head }
    }

    /// Roll forward to `wall`: each bucket that opens up is taken out of every symbol's running sum.
    pub fn advance(&mut self, wall: u64) {
        let now = wall / BUCKET_MS;
        if now <= self.head {
            return;
        }
        if now - self.head >= NB as u64 {
            // a gap longer than the window (sleep, long reconnect): start over
            self.ring.fill(0.0);
            self.sum.fill(0.0);
            self.first = now;
        } else {
            for b in self.head + 1..=now {
                let slot = (b % NB as u64) as usize;
                for (row, sum) in self.sum.iter_mut().enumerate() {
                    let v = &mut self.ring[row * NB + slot];
                    *sum = (*sum - *v as f64).max(0.0);
                    *v = 0.0;
                }
            }
        }
        self.head = now;
    }

    /// Record `usd` of volume for `row` in the current bucket. Call `advance` first.
    pub fn add(&mut self, row: usize, usd: f64) {
        if row >= self.sum.len() {
            self.sum.resize(row + 1, 0.0);
            self.ring.resize((row + 1) * NB, 0.0);
        }
        self.ring[row * NB + (self.head % NB as u64) as usize] += usd as f32;
        self.sum[row] += usd as f32 as f64;
    }

    /// Volume rate over the last 10-20 seconds divided by the average rate over the rest of the last 15 minutes.
    pub fn ratio(&self, row: usize, wall: u64) -> f64 {
        let have = (self.head - self.first + 1).min(NB as u64);
        if row >= self.sum.len() || have < WARMUP {
            return 0.0;
        }
        let slot = (self.head % NB as u64) as usize;
        let cur = self.ring[row * NB + slot] as f64;
        let prev = self.ring[row * NB + (slot + NB - 1) % NB] as f64;
        // seconds into the bucket that is still filling
        let part = (wall.saturating_sub(self.head * BUCKET_MS) as f64 / 1000.0).min(10.0);
        let recent = cur + prev;
        if recent < MIN_USD {
            return 0.0;
        }
        // the baseline is the window before the recent 10-20s, so a burst does not dilute itself
        let before = self.sum[row] - recent;
        if before <= 0.0 {
            return HI;
        }
        let fast = recent / (10.0 + part);
        let slow = before / ((have - 2) as f64 * 10.0);
        fast / slow
    }

    /// Glow brightness for `row`, 0..=STEPS: 0 below 2x the 15-minute average, full at 16x.
    /// It fades by itself as the burst ends and as the average catches up with a sustained one.
    pub fn level(&self, row: usize, wall: u64) -> u8 {
        let r = self.ratio(row, wall);
        if r <= LO {
            return 0;
        }
        let x = ((r / LO).ln() / (HI / LO).ln()).min(1.0);
        (x * STEPS as f64).ceil() as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surge_rises_and_fades() {
        let t0 = 3_000_000 * BUCKET_MS;
        let mut v = Vols::new(t0);
        // five steady minutes at 10K per bucket
        for b in 0..30u64 {
            v.advance(t0 + b * BUCKET_MS);
            v.add(2, 10_000.0);
            v.add(0, 10_000.0);
        }
        let end = |b: u64| t0 + b * BUCKET_MS + 9_999;
        assert_eq!(v.level(2, end(29)), 0);
        assert!((v.ratio(2, end(29)) - 1.0).abs() < 0.01);
        // a 40x bucket: full brightness on that symbol only
        v.advance(t0 + 30 * BUCKET_MS);
        v.add(2, 400_000.0);
        v.add(0, 10_000.0);
        assert_eq!(v.level(2, end(30)), STEPS);
        assert_eq!(v.level(0, end(30)), 0);
        // back to normal: it dims step by step, and is gone once the burst has left the recent window
        let mut last = STEPS;
        for b in 31..34u64 {
            v.advance(t0 + b * BUCKET_MS);
            v.add(2, 10_000.0);
            let l = v.level(2, end(b));
            assert!(l <= last, "{l} > {last}");
            last = l;
        }
        assert_eq!(last, 0);
        // a sustained 5x rate glows at first, then fades as the 15-minute average catches up
        for b in 34..40u64 {
            v.advance(t0 + b * BUCKET_MS);
            v.add(0, 50_000.0);
        }
        let early = v.level(0, end(39));
        assert!(early > 0 && early < STEPS, "{early}");
        for b in 40..130u64 {
            v.advance(t0 + b * BUCKET_MS);
            v.add(0, 50_000.0);
        }
        assert_eq!(v.level(0, end(129)), 0);
        // the running sum matches the ring after buckets have rolled out
        let want: f64 = v.ring[0..NB].iter().map(|&x| x as f64).sum();
        assert!((v.sum[0] - want).abs() < 1.0);
        // unknown rows and tiny volumes never glow
        assert_eq!(v.level(99, end(129)), 0);
        v.add(5, 100.0);
        assert_eq!(v.level(5, end(129)), 0);
    }
}
