//! tickerterm - low-latency terminal board for Binance USD-M futures market-wide streams.
//!
//! Design: one thread, one socket, no async runtime, no TUI framework.
//! poll(2) on {socket, stdin}; every message is parsed and the screen is redrawn
//! immediately (no frame timer). Output is cell-diffed and sent in one write(2).

use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

const DEFAULT_URL: &str =
    "wss://fstream.binance.com/market/stream?streams=!ticker@arr/!markPrice@arr@1s/!forceOrder@arr";

static QUIT: AtomicBool = AtomicBool::new(false);
static WINCH: AtomicBool = AtomicBool::new(false);
static mut ORIG_TERMIOS: Option<libc::termios> = None;

extern "C" fn on_quit(_: libc::c_int) {
    QUIT.store(true, Ordering::Relaxed);
}
extern "C" fn on_winch(_: libc::c_int) {
    WINCH.store(true, Ordering::Relaxed);
}

// ---------------------------------------------------------------- wire types

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct Ticker<'a> {
    #[serde(rename = "E", default)]
    ev: u64,
    #[serde(borrow)]
    s: &'a str,
    #[serde(rename = "P", borrow, default)]
    pct: &'a str,
    #[serde(borrow, default)]
    w: &'a str,
    #[serde(borrow, default)]
    c: &'a str,
    #[serde(rename = "Q", borrow, default)]
    lq: &'a str,
    #[serde(borrow, default)]
    h: &'a str,
    #[serde(borrow, default)]
    l: &'a str,
    #[serde(borrow, default)]
    q: &'a str,
    #[serde(default)]
    n: u64,
    #[serde(default)]
    st: Option<u8>,
}

#[derive(Deserialize)]
struct Mark<'a> {
    #[serde(rename = "E", default)]
    ev: u64,
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow, default)]
    p: &'a str,
    #[serde(borrow, default)]
    r: &'a str,
}

#[derive(Deserialize)]
struct Force<'a> {
    #[serde(rename = "E", default)]
    ev: u64,
    #[serde(borrow)]
    o: ForceOrder<'a>,
}

#[derive(Deserialize)]
struct ForceOrder<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(rename = "S", borrow, default)]
    side: &'a str,
    #[serde(borrow, default)]
    q: &'a str,
    #[serde(borrow, default)]
    p: &'a str,
    #[serde(borrow, default)]
    ap: &'a str,
    #[serde(rename = "T", default)]
    t: u64,
}

// ---------------------------------------------------------------- state

struct Row {
    sym: String,
    st: u8,
    last: f64,
    last_s: String,
    chg: f64,
    high: f64,
    high_s: String,
    low: f64,
    low_s: String,
    vwap_s: String,
    qvol: f64,
    trades: u64,
    lqty: f64,
    mark_s: String,
    fund: f64,
    has_fund: bool,
    has_tick: bool,
    dir: i8,
    seq: u64,
    at: Instant,
    tradfi: bool,
    usdt_perp: bool,
}

struct Liq {
    t: u64,
    sym: String,
    sell: bool,
    notional: f64,
    price: String,
}

#[derive(Clone, Copy, PartialEq)]
enum Sort {
    Vol,
    ChgDesc,
    ChgAsc,
}
const SORTS: [(Sort, &str); 3] = [(Sort::Vol, "VOL v"), (Sort::ChgDesc, "24h% v"), (Sort::ChgAsc, "24h% ^")];

/// Name as shown: USDT perpetuals drop the quote ("BTCUSDT" -> "BTC"); anything else keeps its full symbol.
fn disp(r: &Row) -> &str {
    if r.usdt_perp { &r.sym[..r.sym.len() - 4] } else { &r.sym }
}

fn passes(r: &Row, market: u8, class: u8, filter: &str) -> bool {
    r.has_tick
        && (market == 1 || (r.usdt_perp && r.st != 2))
        && (class == 0 || (class == 1) == r.tradfi)
        && (filter.is_empty() || r.sym.contains(filter))
}

/// Visible cell width per detail level (without the 2-char gutter).
const CW: [usize; 4] = [33, 40, 62, 113];
const HDR_LINES: usize = 4; // 3 status lines + column header
const FIXED: usize = 5; // + footer

#[derive(Default)]
struct Stat {
    last: f64,
    min: f64,
    max: f64,
    avg: f64,
    n: u64,
}
impl Stat {
    fn add(&mut self, v: f64) {
        self.last = v;
        if self.n == 0 {
            self.min = v;
            self.max = v;
            self.avg = v;
        } else {
            self.min = self.min.min(v);
            self.max = self.max.max(v);
            self.avg += (v - self.avg) * 0.1;
        }
        self.n += 1;
    }
}

struct App {
    rows: Vec<Row>,
    index: HashMap<String, usize>,
    liqs: VecDeque<Liq>,
    tseq: u64,
    // view
    sort: usize,
    level: Option<usize>,
    market: u8, // 0 USDT perps only, 1 everything
    class: u8,  // 0 all, 1 TradFi only, 2 crypto only
    rank_dirty: bool,
    ranked_at: Instant,
    live_since: Instant,
    rerank_secs: u64,
    in_rank: Vec<bool>,
    page: usize,
    filter: String,
    filtering: bool,
    help: bool,
    // metrics
    status: String,
    live: bool,
    started: Instant,
    reconnects: u64,
    errors: u64,
    net: Stat,
    proc_us: Stat,
    t_parse: f64,
    t_draw: f64,
    t_write: f64,
    win_at: Instant,
    win_msgs: u64,
    win_bytes: u64,
    win_upd: u64,
    rate_msgs: f64,
    rate_kb: f64,
    rate_upd: f64,
    // render
    w: usize,
    h: usize,
    layout: (usize, usize, usize, usize),
    prev: Vec<Vec<u8>>,
    out: Vec<u8>,
    scratch: Vec<u8>,
    order: Vec<u32>,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn pf(s: &str) -> f64 {
    s.parse().unwrap_or(0.0)
}

fn set(dst: &mut String, s: &str) {
    dst.clear();
    dst.push_str(s);
}

/// Humanise into at most 5 chars: 999, 1.23K, 45.6M, 789B.
fn hum(buf: &mut Vec<u8>, v: f64, width: usize) {
    let mut x = v.abs();
    let mut u = 0;
    const U: [&str; 6] = ["", "K", "M", "B", "T", "Q"];
    while x >= 999.5 && u < 5 {
        x /= 1000.0;
        u += 1;
    }
    let mut tmp = [0u8; 24];
    let mut c = std::io::Cursor::new(&mut tmp[..]);
    let _ = if u == 0 && x.fract() == 0.0 {
        write!(c, "{:.0}", x)
    } else if u == 0 && x < 0.0995 {
        write!(c, "{:.4}", x)
    } else if x < 9.995 {
        write!(c, "{:.2}{}", x, U[u])
    } else if x < 99.95 {
        write!(c, "{:.1}{}", x, U[u])
    } else {
        write!(c, "{:.0}{}", x, U[u])
    };
    let n = c.position() as usize;
    for _ in n..width {
        buf.push(b' ');
    }
    buf.extend_from_slice(&tmp[..n]);
}

/// Right-align an exchange price string into `w` chars, keeping exchange precision when it fits.
fn price(buf: &mut Vec<u8>, s: &str, v: f64, w: usize) {
    let b = s.as_bytes();
    if b.is_empty() {
        for _ in 0..w - 1 {
            buf.push(b' ');
        }
        buf.push(b'-');
    } else if b.len() <= w {
        for _ in b.len()..w {
            buf.push(b' ');
        }
        buf.extend_from_slice(b);
    } else if b.iter().position(|&c| c == b'.').is_some_and(|p| p + 1 < w) {
        buf.extend_from_slice(&b[..w]);
    } else {
        hum(buf, v, w);
    }
}

fn hms(buf: &mut Vec<u8>, ms: u64, millis: bool) {
    let s = ms / 1000;
    let _ = write!(buf, "{:02}:{:02}:{:02}", (s / 3600) % 24, (s / 60) % 60, s % 60);
    if millis {
        let _ = write!(buf, ".{:03}", ms % 1000);
    }
}

impl App {
    fn new() -> Self {
        let now = Instant::now();
        App {
            rows: Vec::with_capacity(1024),
            index: HashMap::with_capacity(1024),
            liqs: VecDeque::new(),
            tseq: 0,
            sort: 0,
            level: None,
            market: 0,
            class: 0,
            rank_dirty: true,
            ranked_at: now,
            live_since: now,
            rerank_secs: 60,
            in_rank: Vec::new(),
            page: 0,
            filter: String::new(),
            filtering: false,
            help: false,
            status: "CONNECTING".into(),
            live: false,
            started: now,
            reconnects: 0,
            errors: 0,
            net: Stat::default(),
            proc_us: Stat::default(),
            t_parse: 0.0,
            t_draw: 0.0,
            t_write: 0.0,
            win_at: now,
            win_msgs: 0,
            win_bytes: 0,
            win_upd: 0,
            rate_msgs: 0.0,
            rate_kb: 0.0,
            rate_upd: 0.0,
            w: 0,
            h: 0,
            layout: (0, 0, 0, 0),
            prev: Vec::new(),
            out: Vec::with_capacity(1 << 16),
            scratch: Vec::with_capacity(512),
            order: Vec::with_capacity(1024),
        }
    }

    fn row(&mut self, sym: &str, now: Instant) -> &mut Row {
        let i = match self.index.get(sym) {
            Some(&i) => i,
            None => {
                let i = self.rows.len();
                self.rows.push(Row {
                    sym: sym.to_string(),
                    st: 1,
                    last: 0.0,
                    last_s: String::new(),
                    chg: 0.0,
                    high: 0.0,
                    high_s: String::new(),
                    low: 0.0,
                    low_s: String::new(),
                    vwap_s: String::new(),
                    qvol: 0.0,
                    trades: 0,
                    lqty: 0.0,
                    mark_s: String::new(),
                    fund: 0.0,
                    has_fund: false,
                    has_tick: false,
                    dir: 0,
                    seq: 0,
                    at: now,
                    tradfi: false,
                    usdt_perp: sym.ends_with("USDT") && !sym.contains('_'),
                });
                self.index.insert(sym.to_string(), i);
                i
            }
        };
        &mut self.rows[i]
    }

    fn on_text(&mut self, txt: &str) -> Result<(), serde_json::Error> {
        self.win_msgs += 1;
        self.win_bytes += txt.len() as u64;
        let (stream, data) = match serde_json::from_str::<Envelope>(txt) {
            Ok(e) => (e.stream, e.data.get()),
            // raw (non-combined) endpoint: guess by shape
            Err(_) if txt.starts_with('[') => ("!ticker@arr", txt),
            Err(e) => return Err(e),
        };
        let now = Instant::now();
        let wall = now_ms();
        let mut ev = 0u64;
        if stream.starts_with("!ticker") || stream.starts_with("!miniTicker") {
            let v: Vec<Ticker> = serde_json::from_str(data)?;
            self.tseq += 1;
            let seq = self.tseq;
            self.win_upd += v.len() as u64;
            for t in &v {
                ev = ev.max(t.ev);
                let r = self.row(t.s, now);
                let last = pf(t.c);
                if r.has_tick && last != r.last {
                    r.dir = if last > r.last { 1 } else { -1 };
                }
                r.last = last;
                set(&mut r.last_s, t.c);
                r.chg = pf(t.pct);
                r.high = pf(t.h);
                set(&mut r.high_s, t.h);
                r.low = pf(t.l);
                set(&mut r.low_s, t.l);
                set(&mut r.vwap_s, t.w);
                r.qvol = pf(t.q);
                r.trades = t.n;
                r.lqty = pf(t.lq);
                if let Some(st) = t.st {
                    r.st = st;
                }
                r.has_tick = true;
                r.seq = seq;
                r.at = now;
            }
        } else if stream.starts_with("!markPrice") {
            let v: Vec<Mark> = serde_json::from_str(data)?;
            // Binance pushes TradFi contracts as their own batch, separate from the crypto one.
            let tradfi = !v.is_empty() && !v.iter().any(|m| m.s == "BTCUSDT");
            for m in &v {
                ev = ev.max(m.ev);
                let r = self.row(m.s, now);
                r.tradfi = tradfi;
                set(&mut r.mark_s, m.p);
                if !m.r.is_empty() {
                    r.fund = pf(m.r) * 100.0;
                    r.has_fund = true;
                }
            }
        } else if stream.starts_with("!forceOrder") {
            let f: Force = serde_json::from_str(data)?;
            ev = f.ev;
            let px = if pf(f.o.ap) > 0.0 { f.o.ap } else { f.o.p };
            self.liqs.push_front(Liq {
                t: f.o.t,
                sym: f.o.s.to_string(),
                sell: f.o.side == "SELL",
                notional: pf(f.o.q) * pf(px),
                price: px.to_string(),
            });
            self.liqs.truncate(64);
        }
        if ev > 0 {
            self.net.add(wall as f64 - ev as f64);
        }
        Ok(())
    }

    fn roll_rates(&mut self, now: Instant) {
        let dt = now.duration_since(self.win_at).as_secs_f64();
        if dt >= 1.0 {
            self.rate_msgs = self.win_msgs as f64 / dt;
            self.rate_kb = self.win_bytes as f64 / dt / 1024.0;
            self.rate_upd = self.win_upd as f64 / dt;
            self.win_msgs = 0;
            self.win_bytes = 0;
            self.win_upd = 0;
            self.win_at = now;
        }
    }

    /// Returns true to quit.
    fn on_keys(&mut self, b: &[u8]) -> bool {
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            i += 1;
            if c == 3 {
                return true;
            }
            if self.filtering {
                match c {
                    27 => {
                        self.filter.clear();
                        self.filtering = false;
                        i = b.len();
                    }
                    13 | 10 => self.filtering = false,
                    127 | 8 => {
                        self.filter.pop();
                    }
                    0x21..=0x7e if self.filter.len() < 16 => self.filter.push(c.to_ascii_uppercase() as char),
                    _ => {}
                }
                self.page = 0;
                self.rank_dirty = true;
                continue;
            }
            match c {
                27 => {
                    if b.get(i) == Some(&b'[') || b.get(i) == Some(&b'O') {
                        i += 1;
                        let st = i;
                        while i < b.len() && !(0x40..=0x7e).contains(&b[i]) {
                            i += 1;
                        }
                        let seq = &b[st..(i + 1).min(b.len())];
                        i += 1;
                        match seq {
                            b"A" | b"D" | b"5~" => self.page = self.page.saturating_sub(1),
                            b"B" | b"C" | b"6~" => self.page += 1,
                            b"H" | b"1~" => self.page = 0,
                            _ => {}
                        }
                    } else {
                        self.filter.clear();
                        self.page = 0;
                        self.rank_dirty = true;
                    }
                }
                b'q' | b'Q' => return true,
                b's' => {
                    self.sort = (self.sort + 1) % SORTS.len();
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'r' => self.rank_dirty = true,
                b't' => {
                    self.class = (self.class + 1) % 3;
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'1'..=b'4' => self.level = Some((c - b'1') as usize),
                b'a' | b'0' => self.level = None,
                b'm' => {
                    self.market ^= 1;
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'/' => self.filtering = true,
                b'?' | b'h' => self.help = !self.help,
                b'j' | b' ' | b'n' => self.page += 1,
                b'k' | b'p' => self.page = self.page.saturating_sub(1),
                b'g' => self.page = 0,
                _ => {}
            }
        }
        false
    }

    fn render(&mut self, fd: i32) {
        let t0 = Instant::now();
        let (w, h) = term_size();
        let grid_rows = h.saturating_sub(FIXED).max(1);

        // ---- membership + ranking. The order is frozen between re-ranks so rows hold still:
        // values update in place, positions only move on a re-rank.
        let (market, class) = (self.market, self.class);
        self.order.retain(|&i| passes(&self.rows[i as usize], market, class, &self.filter));
        self.in_rank.clear();
        self.in_rank.resize(self.rows.len(), false);
        for &i in &self.order {
            self.in_rank[i as usize] = true;
        }
        let mut grew = false;
        for (i, r) in self.rows.iter().enumerate() {
            if !self.in_rank[i] && passes(r, market, class, &self.filter) {
                self.order.push(i as u32);
                grew = true;
            }
        }
        let warmup = t0.duration_since(self.live_since) < Duration::from_secs(20);
        let due = self.rerank_secs > 0 && t0.duration_since(self.ranked_at).as_secs() >= self.rerank_secs;
        if self.rank_dirty || due || (grew && warmup) {
            let rows = &self.rows;
            let key = SORTS[self.sort].0;
            self.order.sort_unstable_by(|&a, &b| {
                let (x, y) = (&rows[a as usize], &rows[b as usize]);
                match key {
                    Sort::Vol => y.qvol.total_cmp(&x.qvol),
                    Sort::ChgDesc => y.chg.total_cmp(&x.chg),
                    Sort::ChgAsc => x.chg.total_cmp(&y.chg),
                }
                .then_with(|| x.sym.cmp(&y.sym))
            });
            self.rank_dirty = false;
            self.ranked_at = t0;
        }
        let (mut adv, mut dec, mut flat, mut vol, mut ntf) = (0u32, 0u32, 0u32, 0f64, 0u32);
        for &i in &self.order {
            let r = &self.rows[i as usize];
            ntf += r.tradfi as u32;
            if r.st != 2 {
                vol += r.qvol;
            }
            if r.chg > 0.0 {
                adv += 1
            } else if r.chg < 0.0 {
                dec += 1
            } else {
                flat += 1
            }
        }

        // ---- layout: auto picks the richest level that shows every symbol on one page
        // symbol column: 10 chars is enough once the USDT suffix is gone, 14 for full symbols
        let sw = if self.market == 0 { 10 } else { 14 };
        let cw = |l: usize| CW[l] - 14 + sw;
        let ncols_at = |l: usize| ((w + 2) / (cw(l) + 2)).max(1);
        let n = self.order.len();
        let level = self.level.unwrap_or_else(|| {
            // never give up symbols for detail, but spend leftover width on richer cells
            let want = n.min(ncols_at(0) * grid_rows);
            (0..4).rev().find(|&l| ncols_at(l) * grid_rows >= want && cw(l) <= w).unwrap_or(0)
        });
        let ncols = ncols_at(level);
        let slots = ncols * grid_rows;
        let pages = n.div_ceil(slots).max(1);
        self.page = self.page.min(pages - 1);

        self.out.clear();
        self.out.extend_from_slice(b"\x1b[?2026h");
        let layout = (w, h, level * 16 + sw, ncols);
        if layout != self.layout || WINCH.swap(false, Ordering::Relaxed) {
            self.layout = layout;
            self.w = w;
            self.h = h;
            self.prev.clear();
            self.out.extend_from_slice(b"\x1b[0m\x1b[2J");
        }
        self.prev.resize(FIXED + slots, Vec::new());

        // ---- line 1: status
        let s = &mut self.scratch;
        s.clear();
        let _ = write!(
            s,
            "\x1b[0;1m TICKERTERM\x1b[0m binance usd-m | {}{}\x1b[0m | {} sym (\x1b[36m{} tradfi\x1b[0m) \x1b[32m+{}\x1b[0m \x1b[31m-{}\x1b[0m ={} | 24h vol ",
            if self.live { "\x1b[1;32m" } else { "\x1b[1;33m" },
            self.status,
            n,
            ntf,
            adv,
            dec,
            flat
        );
        hum(s, vol, 0);
        let _ = write!(
            s,
            " | {}{} | sort {} ({}s ago) | lvl {}{} | pg {}/{}",
            ["USDT-PERP", "ALL"][self.market as usize],
            ["", " TRADFI", " CRYPTO"][self.class as usize],
            SORTS[self.sort].1,
            t0.duration_since(self.ranked_at).as_secs(),
            level + 1,
            if self.level.is_none() { "(auto)" } else { "" },
            self.page + 1,
            pages
        );
        if self.filtering || !self.filter.is_empty() {
            let _ = write!(s, " | \x1b[1;33m/{}{}\x1b[0m", self.filter, if self.filtering { "_" } else { "" });
        }
        emit(&mut self.out, &mut self.prev[0], s, 1, 1, true);

        // ---- line 2: latency + throughput
        s.clear();
        let _ = write!(
            s,
            "\x1b[0m lat E->rx \x1b[1m{:.0}ms\x1b[0m (min {:.0} avg {:.0} max {:.0}) | rx->screen \x1b[1m{:.0}us\x1b[0m (parse {:.0} draw {:.0} write {:.0}, max {:.0}) | {:.1} msg/s {:.0} KB/s {:.0} upd/s | up ",
            self.net.last,
            self.net.min,
            self.net.avg,
            self.net.max,
            self.proc_us.last,
            self.t_parse,
            self.t_draw,
            self.t_write,
            self.proc_us.max,
            self.rate_msgs,
            self.rate_kb,
            self.rate_upd
        );
        hms(s, self.started.elapsed().as_millis() as u64, false);
        let _ = write!(s, " rc {} err {} | ", self.reconnects, self.errors);
        hms(s, now_ms(), true);
        emit(&mut self.out, &mut self.prev[1], s, 2, 1, true);

        // ---- line 3: movers (within current filter)
        s.clear();
        s.extend_from_slice(b"\x1b[0m");
        {
            let rows = &self.rows;
            let k = (w.saturating_sub(10) / 2 / 24).clamp(1, 8);
            let mut top: Vec<u32> = self.order.clone();
            top.sort_unstable_by(|&a, &b| rows[b as usize].chg.total_cmp(&rows[a as usize].chg));
            s.extend_from_slice(b" \x1b[1;32mUP\x1b[0;32m");
            for &i in top.iter().take(k) {
                let r = &rows[i as usize];
                let _ = write!(s, " {} {:+.1}%", disp(r), r.chg);
            }
            s.extend_from_slice(b"\x1b[0m | \x1b[1;31mDN\x1b[0;31m");
            for &i in top.iter().rev().take(k) {
                let r = &rows[i as usize];
                let _ = write!(s, " {} {:+.1}%", disp(r), r.chg);
            }
            s.extend_from_slice(b"\x1b[0m");
        }
        emit(&mut self.out, &mut self.prev[2], s, 3, 1, true);

        // ---- line 4: column headers
        s.clear();
        s.extend_from_slice(b"\x1b[0;7m");
        for c in 0..ncols {
            let _ = write!(s, "{:<sw$} {:>10} {:>7}", "SYMBOL", "LAST", "24h%");
            if level >= 1 {
                let _ = write!(s, " {:>6}", "QVOL");
            }
            if level >= 2 {
                let _ = write!(s, " {:>6} {:5} {:>8}", "TRADES", "L<->H", "FUND%");
            }
            if level >= 3 {
                let _ = write!(s, " {:>10} {:>10} {:>10} {:>10} {:>6}", "HIGH", "LOW", "VWAP", "MARK", "LQTY");
            }
            if c + 1 < ncols {
                s.extend_from_slice(b"  ");
            }
        }
        s.extend_from_slice(b"\x1b[0m");
        emit(&mut self.out, &mut self.prev[3], s, 4, 1, true);

        // ---- grid (column-major)
        let base = self.page * slots;
        let now = Instant::now();
        for slot in 0..slots {
            let s = &mut self.scratch;
            s.clear();
            match self.order.get(base + slot) {
                Some(&i) => {
                    let r = &self.rows[i as usize];
                    cell(s, r, level, sw, r.seq == self.tseq, now.duration_since(r.at).as_secs() > 60);
                }
                None => {
                    s.extend_from_slice(b"\x1b[0m");
                    s.resize(s.len() + cw(level), b' ');
                }
            }
            let (col, row) = (slot / grid_rows, slot % grid_rows);
            emit(&mut self.out, &mut self.prev[FIXED + slot], s, HDR_LINES + 1 + row, col * (cw(level) + 2) + 1, false);
        }

        // ---- footer: liquidation tape / help
        let s = &mut self.scratch;
        s.clear();
        if self.help || self.liqs.is_empty() {
            s.extend_from_slice(
                b"\x1b[0;2m q quit | s sort | r re-rank now | m usdt-perp/all | t tradfi/crypto/both | 1-4 detail, a auto | / filter (esc clears) | arrows/j/k page | ? liq tape\x1b[0m",
            );
        } else {
            s.extend_from_slice(b"\x1b[0;1m LIQ\x1b[0m");
            let mut used = 4;
            for l in &self.liqs {
                let sym = l.sym.strip_suffix("USDT").unwrap_or(&l.sym);
                let need = 9 + sym.len() + 6 + 6 + l.price.len() + 5;
                if used + need > w {
                    break;
                }
                used += need;
                s.push(b' ');
                hms(s, l.t, false);
                let _ = write!(
                    s,
                    " {}{} {}",
                    if l.sell { "\x1b[31m" } else { "\x1b[32m" },
                    sym,
                    if l.sell { "SELL" } else { "BUY " }
                );
                hum(s, l.notional, 6);
                let _ = write!(s, " @{}\x1b[0m |", l.price);
            }
        }
        emit(&mut self.out, &mut self.prev[4], s, h.max(FIXED), 1, true);

        self.out.extend_from_slice(b"\x1b[?2026l");
        let t1 = Instant::now();
        write_all(fd, &self.out);
        let t2 = Instant::now();
        self.t_draw = (t1 - t0).as_secs_f64() * 1e6;
        self.t_write = (t2 - t1).as_secs_f64() * 1e6;
    }
}

/// Emit `new` at (row, col) only if it differs from what is already on screen.
fn emit(out: &mut Vec<u8>, prev: &mut Vec<u8>, new: &[u8], row: usize, col: usize, eol: bool) {
    if prev.as_slice() == new {
        return;
    }
    let _ = write!(out, "\x1b[{};{}H", row, col);
    out.extend_from_slice(new);
    if eol {
        out.extend_from_slice(b"\x1b[0m\x1b[K");
    }
    prev.clear();
    prev.extend_from_slice(new);
}

fn cell(s: &mut Vec<u8>, r: &Row, level: usize, sw: usize, fresh: bool, stale: bool) {
    let sgr = |s: &mut Vec<u8>, sign: f64, bold: bool| {
        s.extend_from_slice(match (stale, bold, sign > 0.0, sign < 0.0) {
            (true, ..) => b"\x1b[0;2m",
            (_, true, true, _) => b"\x1b[0;1;92m",
            (_, true, _, true) => b"\x1b[0;1;91m",
            (_, false, true, _) => b"\x1b[0;32m",
            (_, false, _, true) => b"\x1b[0;31m",
            (_, true, ..) => b"\x1b[0;1m",
            _ => b"\x1b[0m",
        })
    };
    s.extend_from_slice(match (stale, r.tradfi, fresh) {
        (true, true, _) => b"\x1b[0;2;36m" as &[u8],
        (true, ..) => b"\x1b[0;2m",
        (_, true, true) => b"\x1b[0;1;36m",
        (_, true, false) => b"\x1b[0;36m",
        (_, false, true) => b"\x1b[0;1m",
        _ => b"\x1b[0m",
    });
    let _ = write!(s, "{:<sw$.sw$} ", disp(r));
    sgr(s, r.dir as f64, fresh);
    price(s, &r.last_s, r.last, 10);
    sgr(s, r.chg, false);
    // at most 6 chars, so there is always a gap after the price
    let a = r.chg.abs();
    let _ = if a < 99.995 {
        write!(s, " {:>+7.2}", r.chg)
    } else if a < 999.95 {
        write!(s, " {:>+7.1}", r.chg)
    } else {
        write!(s, " {:>+7.0}", r.chg.clamp(-99999.0, 99999.0))
    };
    if level >= 1 {
        s.extend_from_slice(if stale { b"\x1b[0;2m " } else { b"\x1b[0m " });
        hum(s, r.qvol, 6);
    }
    if level >= 2 {
        s.push(b' ');
        hum(s, r.trades as f64, 6);
        s.push(b' ');
        let pos = if r.high > r.low { ((r.last - r.low) / (r.high - r.low) * 4.999) as usize } else { 2 };
        for i in 0..5 {
            s.push(if i == pos.min(4) { b'#' } else { b'-' });
        }
        if r.has_fund {
            sgr(s, r.fund, false);
            let _ = write!(s, " {:>+8.4}", r.fund);
        } else {
            s.extend_from_slice(b"        -");
        }
    }
    if level >= 3 {
        s.extend_from_slice(if stale { b"\x1b[0;2m " } else { b"\x1b[0m " });
        price(s, &r.high_s, r.high, 10);
        s.push(b' ');
        price(s, &r.low_s, r.low, 10);
        s.push(b' ');
        price(s, &r.vwap_s, pf(&r.vwap_s), 10);
        s.push(b' ');
        price(s, &r.mark_s, pf(&r.mark_s), 10);
        s.push(b' ');
        hum(s, r.lqty, 6);
    }
    s.extend_from_slice(b"\x1b[0m");
}

// ---------------------------------------------------------------- terminal

fn write_all(fd: i32, mut b: &[u8]) {
    while !b.is_empty() {
        let n = unsafe { libc::write(fd, b.as_ptr() as *const _, b.len()) };
        if n > 0 {
            b = &b[n as usize..];
        } else if n < 0 {
            let e = std::io::Error::last_os_error();
            match e.kind() {
                std::io::ErrorKind::Interrupted => {}
                std::io::ErrorKind::WouldBlock => {
                    let mut p = libc::pollfd { fd, events: libc::POLLOUT, revents: 0 };
                    unsafe { libc::poll(&mut p, 1, 100) };
                }
                _ => return,
            }
        }
    }
}

fn term_size() -> (usize, usize) {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } == 0 && ws.ws_col > 0 && ws.ws_row > 0 {
        return (ws.ws_col as usize, ws.ws_row as usize);
    }
    let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    (env("COLUMNS", 160), env("LINES", 45))
}

fn term_enter() {
    unsafe {
        let mut t: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(0, &mut t) == 0 {
            ORIG_TERMIOS = Some(t);
            t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
            t.c_iflag &= !(libc::IXON | libc::ICRNL);
            t.c_cc[libc::VMIN] = 0;
            t.c_cc[libc::VTIME] = 0;
            libc::tcsetattr(0, libc::TCSANOW, &t);
        }
    }
    // alt screen, hide cursor, no autowrap
    write_all(1, b"\x1b[?1049h\x1b[?25l\x1b[?7l\x1b[2J");
}

fn term_leave() {
    write_all(1, b"\x1b[?2026l\x1b[0m\x1b[?7h\x1b[?25h\x1b[?1049l");
    unsafe {
        #[allow(static_mut_refs)]
        if let Some(t) = ORIG_TERMIOS.take() {
            libc::tcsetattr(0, libc::TCSANOW, &t);
        }
    }
}

// ---------------------------------------------------------------- network

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

fn connect(url: &str) -> Result<(Ws, TcpStream), String> {
    let rest = url.strip_prefix("wss://").ok_or("url must start with wss://")?;
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().map_err(|e| e.to_string())?),
        None => (authority, 443),
    };
    let mut err = "dns: no addresses".to_string();
    let mut tcp = None;
    for addr in (host, port).to_socket_addrs().map_err(|e| format!("dns: {e}"))? {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
            Ok(s) => {
                tcp = Some(s);
                break;
            }
            Err(e) => err = format!("tcp {addr}: {e}"),
        }
    }
    let tcp = tcp.ok_or(err)?;
    tcp.set_nodelay(true).map_err(|e| e.to_string())?;
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
    let ctl = tcp.try_clone().map_err(|e| e.to_string())?;
    let (ws, _) = tungstenite::client_tls(url, tcp).map_err(|e| format!("handshake: {e}"))?;
    // Blocking handshake done; from here the socket is driven by poll().
    ctl.set_nonblocking(true).map_err(|e| e.to_string())?;
    Ok((ws, ctl))
}

fn main() {
    let mut url = DEFAULT_URL.to_string();
    let mut dump = 0usize;
    let mut all = false;
    let mut class = 0u8;
    let mut rerank = 60u64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--url" => url = args.next().expect("--url needs a value"),
            "--all" => all = true,
            "--tradfi" => class = 1,
            "--crypto" => class = 2,
            "--rerank" => rerank = args.next().and_then(|v| v.parse().ok()).expect("--rerank needs seconds"),
            "--dump" => dump = args.next().and_then(|v| v.parse().ok()).unwrap_or(6),
            _ => {
                eprintln!(
                    "usage: tickerterm [--all] [--tradfi | --crypto] [--rerank SECS] [--url wss://...] [--dump N]\n  \
                     --all      start with every symbol (default: USDT perpetuals only)\n  \
                     --tradfi   start on the TradFi-only page (--crypto for the opposite; 't' cycles)\n  \
                     --rerank   seconds between re-rankings, 0 = only on 'r' (default 60)\n  \
                     default url: {DEFAULT_URL}"
                );
                return;
            }
        }
    }
    let _ = rustls::crypto::ring::default_provider().install_default();

    if dump > 0 {
        let (mut ws, ctl) = connect(&url).expect("connect");
        ctl.set_nonblocking(false).unwrap();
        while dump > 0 {
            if let Ok(Message::Text(t)) = ws.read() {
                let s = t.as_str();
                println!("{} bytes: {}", s.len(), &s[..s.len().min(700)]);
                dump -= 1;
            }
        }
        return;
    }

    unsafe {
        libc::signal(libc::SIGINT, on_quit as extern "C" fn(libc::c_int) as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_quit as extern "C" fn(libc::c_int) as libc::sighandler_t);
        libc::signal(libc::SIGHUP, on_quit as extern "C" fn(libc::c_int) as libc::sighandler_t);
        libc::signal(libc::SIGWINCH, on_winch as extern "C" fn(libc::c_int) as libc::sighandler_t);
    }
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |i| {
        term_leave();
        hook(i);
    }));
    term_enter();

    let mut app = App::new();
    app.market = all as u8;
    app.class = class;
    app.rerank_secs = rerank;
    let mut conn: Option<(Ws, TcpStream)> = None;
    let mut next_try = Instant::now();
    let mut backoff = Duration::from_millis(250);
    let mut last_rx = Instant::now();
    let mut stdin_ok = true;
    let mut first = true;
    app.render(1);

    while !QUIT.load(Ordering::Relaxed) {
        if conn.is_none() && Instant::now() >= next_try {
            if !first {
                app.reconnects += 1;
            }
            first = false;
            match connect(&url) {
                Ok(c) => {
                    conn = Some(c);
                    app.status = "LIVE".into();
                    app.live = true;
                    app.live_since = Instant::now();
                    backoff = Duration::from_millis(250);
                    last_rx = Instant::now();
                }
                Err(e) => {
                    app.status = format!("DOWN ({e})");
                    app.live = false;
                    next_try = Instant::now() + backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(8));
                }
            }
            app.render(1);
        }

        let mut fds = [
            libc::pollfd { fd: if stdin_ok { 0 } else { -1 }, events: libc::POLLIN, revents: 0 },
            libc::pollfd {
                fd: conn.as_ref().map(|c| c.1.as_raw_fd()).unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), 2, 1000) };
        let t_rx = Instant::now();
        let mut dirty = rc < 0 || WINCH.load(Ordering::Relaxed); // EINTR: resize

        if fds[0].revents != 0 {
            let mut kb = [0u8; 64];
            let n = unsafe { libc::read(0, kb.as_mut_ptr() as *mut _, kb.len()) };
            if n > 0 {
                if app.on_keys(&kb[..n as usize]) {
                    break;
                }
                dirty = true;
            } else if n == 0 {
                stdin_ok = false; // EOF (stdin not a terminal)
            }
        }

        let mut got = false;
        let mut drop_conn: Option<String> = None;
        if fds[1].revents != 0 {
            let ws = &mut conn.as_mut().unwrap().0;
            // Drain everything already decrypted/buffered, then draw once.
            loop {
                match ws.read() {
                    Ok(Message::Text(t)) => {
                        got = true;
                        if app.on_text(t.as_str()).is_err() {
                            app.errors += 1;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        drop_conn = Some("closed by server".into());
                        break;
                    }
                    Ok(_) => {} // ping/pong handled by tungstenite
                    Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        drop_conn = Some(e.to_string());
                        break;
                    }
                }
            }
            let _ = ws.flush(); // push out any queued pong
        }
        if got {
            last_rx = t_rx;
        } else if conn.is_some() && drop_conn.is_none() && t_rx.duration_since(last_rx) > Duration::from_secs(10) {
            drop_conn = Some("stalled 10s".into());
        }
        if let Some(why) = drop_conn {
            conn = None;
            app.status = format!("RECONNECTING ({why})");
            app.live = false;
            next_try = Instant::now();
            dirty = true;
        }

        if got || dirty {
            let t_parsed = Instant::now();
            app.roll_rates(t_parsed);
            app.render(1);
            if got {
                app.t_parse = (t_parsed - t_rx).as_secs_f64() * 1e6;
                app.proc_us.add(t_rx.elapsed().as_secs_f64() * 1e6);
            }
        }
    }
    term_leave();
}
