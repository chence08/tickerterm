//! tickerterm - low-latency terminal board for Binance USD-M futures market-wide streams.
//!
//! Design: one thread, one socket, no async runtime, no TUI framework.
//! poll(2) on {socket, stdin}; every message is parsed and the screen is redrawn
//! immediately (no frame timer). Output is cell-diffed and sent in one write(2).

mod favorites;
mod funding;
mod history;
mod hl;
mod liq;
mod oi;
mod surge;
mod views;

use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

const DEFAULT_URL: &str =
    "wss://fstream.binance.com/market/stream?streams=!ticker@arr/!markPrice@arr@1s/!forceOrder@arr/tradingSession";

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
    #[serde(rename = "T", default)]
    next: u64,
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
}

#[derive(Deserialize)]
struct Session<'a> {
    #[serde(borrow)]
    e: &'a str,
    #[serde(rename = "E", default)]
    ev: u64,
    #[serde(rename = "T", default)]
    end: u64,
    #[serde(rename = "S", borrow, default)]
    state: &'a str,
}

// ---------------------------------------------------------------- state

/// Underlying-market session for TradFi contracts (from the `tradingSession` stream).
struct Sess {
    key: String,
    rank: usize,
    state: String,
    end: u64,
}
const MARKETS: [(&str, &str); 6] = [
    ("EquityUpdate", "US"),
    ("CommodityUpdate", "CMDTY"),
    ("FXUpdate", "FX"),
    ("KR_EquityUpdate", "KR"),
    ("HK_EquityUpdate", "HK"),
    ("CN_EquityUpdate", "CN"),
];


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
    vwap: f64,
    qvol: f64,
    trades: u64,
    lqty: f64,
    mark_s: String,
    fund: f64,
    has_fund: bool,
    next_fund: u64,
    has_tick: bool,
    dir: i8,
    seq: u64,
    at: Instant,
    /// funding interval in hours (from fundingInfo; 8 until known)
    fint: u8,
    pinned: bool,
    tradfi: bool,
    /// underlying is a commodity (from exchangeInfo); every commodity is also TradFi
    commodity: bool,
    /// open interest value in USD and its one-hour average (0 until fetched)
    oi: f64,
    oi_ma: f64,
    /// Hyperliquid: funding per year in percent, mark, open interest and 24h volume in USD;
    /// valid while `hl_gen` equals the app's (the asset was in the latest poll)
    hl_fund: f64,
    hl_mark: f64,
    hl_oi: f64,
    hl_vol: f64,
    hl_gen: u64,
    usdt_perp: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Sort {
    Vol,
    ChgDesc,
    ChgAsc,
    FundDesc,
    FundAsc,
}
const SORTS: [(Sort, &str); 5] = [
    (Sort::Vol, "VOL v"),
    (Sort::ChgDesc, "24h% v"),
    (Sort::ChgAsc, "24h% ^"),
    (Sort::FundDesc, "FUND/yr v"),
    (Sort::FundAsc, "FUND/yr ^"),
];

/// Name as shown: USDT perpetuals drop the quote ("BTCUSDT" -> "BTC"); anything else keeps its full symbol.
fn disp(r: &Row) -> &str {
    if r.usdt_perp { &r.sym[..r.sym.len() - 4] } else { &r.sym }
}

/// Funding rate per year, in percent: the per-interval rate times intervals per year.
fn ann(r: &Row) -> f64 {
    r.fund * 8760.0 / r.fint as f64
}

fn passes(r: &Row, market: u8, class: u8, pin_only: bool, filter: &str) -> bool {
    r.has_tick
        && (!pin_only || r.pinned)
        && (market == 1 || (r.usdt_perp && r.st != 2))
        && match class {
            1 => r.tradfi,
            2 => !r.tradfi,
            3 => r.commodity,
            _ => true,
        }
        && (filter.is_empty() || r.sym.contains(filter))
}

/// Visible cell width per detail level (without the 2-char gutter).
const CW: [usize; 5] = [44, 59, 73, 86, 126];
const HDR_LINES: usize = 5; // 3 status lines + sessions + column header
const FIXED: usize = 7; // + two footer lines (liquidation flow)

#[derive(Clone, Copy, PartialEq)]
enum View {
    Board,
    Plot,
    Dist,
    Arb,
}

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
    flow: liq::Flow,
    vols: surge::Vols,
    commodities: HashSet<String>,
    oi_map: HashMap<String, (f64, f64)>,
    oi_note: String,
    hl_gen: u64,
    hl_at: Option<Instant>,
    hl_err: String,
    arb_sort: usize,
    truecolor: bool,
    /// footer lines, their cache key, per-symbol scratch, and whether the right side shows the tape
    flines: [Vec<u8>; 2],
    fkey: (u64, usize, bool),
    ftops: Vec<(u32, f64, bool)>,
    tape: bool,
    sessions: Vec<Sess>,
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
    // favourites
    pins: HashSet<String>,
    /// 0 = pinned symbols lead the page, 1 = pinned only, 2 = favourites hidden (pins ignored, list kept)
    pin_mode: u8,
    pinning: bool,
    pin_buf: Vec<u8>,
    notice: String,
    // visualisations
    view: View,
    view_dirty: bool,
    view_seq: u64,
    plot_rebase: bool,
    plot_full: bool,
    hist: history::History,
    vlines: Vec<Vec<u8>>,
    canvas: views::Canvas,
    fbuf: Vec<f32>,
    bases: Vec<f32>,
    pairs: Vec<(f64, u32)>,
    plot_set: Vec<u32>,
    plot_set_at: Instant,
    fund_hours: funding::Hours,
    fund_status: String,
    fund_at: Option<SystemTime>,
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
            flow: liq::Flow::new(now_ms()),
            vols: surge::Vols::new(now_ms()),
            commodities: HashSet::new(),
            oi_map: HashMap::with_capacity(1024),
            oi_note: String::new(),
            hl_gen: 1,
            hl_at: None,
            hl_err: String::new(),
            arb_sort: 0,
            truecolor: std::env::var("COLORTERM").is_ok_and(|v| v.contains("truecolor") || v.contains("24bit")),
            flines: [Vec::new(), Vec::new()],
            fkey: (u64::MAX, 0, false),
            ftops: Vec::new(),
            tape: false,
            sessions: Vec::new(),
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
            pins: favorites::load(),
            pin_mode: 0,
            pinning: false,
            pin_buf: Vec::new(),
            notice: String::new(),
            view: View::Board,
            view_dirty: true,
            view_seq: 0,
            plot_rebase: false,
            plot_full: false,
            // 1024 samples x 1024 symbols of f32 = 4 MB, fixed for the life of the process
            hist: history::History::new(1024, now),
            vlines: Vec::new(),
            canvas: views::Canvas::default(),
            fbuf: Vec::new(),
            bases: Vec::new(),
            pairs: Vec::new(),
            plot_set: Vec::new(),
            plot_set_at: now,
            fund_hours: funding::Hours::new(),
            fund_status: "assuming 8h intervals, fetching".into(),
            fund_at: None,
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
                    vwap: 0.0,
                    qvol: 0.0,
                    trades: 0,
                    lqty: 0.0,
                    mark_s: String::new(),
                    fund: 0.0,
                    has_fund: false,
                    next_fund: 0,
                    has_tick: false,
                    dir: 0,
                    seq: 0,
                    at: now,
                    pinned: self.pins.contains(sym),
                    fint: self.fund_hours.get(sym).copied().unwrap_or(funding::DEFAULT_HOURS),
                    tradfi: false,
                    hl_fund: 0.0,
                    hl_mark: 0.0,
                    hl_oi: 0.0,
                    hl_vol: 0.0,
                    hl_gen: 0,
                    commodity: self.commodities.contains(sym),
                    oi: self.oi_map.get(sym).map_or(0.0, |v| v.0),
                    oi_ma: self.oi_map.get(sym).map_or(0.0, |v| v.1),
                    usdt_perp: sym.ends_with("USDT") && !sym.contains('_'),
                });
                self.index.insert(sym.to_string(), i);
                i
            }
        };
        &mut self.rows[i]
    }

    /// Pin or unpin each comma-separated name ("BTC", "btcusdt", "ETH,SOL") and save the list.
    fn toggle_pins(&mut self, input: &str) {
        self.notice.clear();
        for name in input.split(',').map(|n| n.trim().to_uppercase()).filter(|n| !n.is_empty()) {
            // accept the name as shown (USDT perpetuals drop the suffix) or the full symbol
            let full = format!("{name}USDT");
            let Some(&i) = self.index.get(&full).or_else(|| self.index.get(&name)) else {
                let _ = write!(self.notice, "no symbol {name}  ");
                continue;
            };
            let r = &mut self.rows[i];
            r.pinned = !r.pinned;
            if r.pinned {
                self.pins.insert(r.sym.clone());
            } else {
                self.pins.remove(&r.sym);
            }
            let _ = write!(self.notice, "{} {}  ", if r.pinned { "pinned" } else { "unpinned" }, disp(r));
        }
        if let Err(e) = favorites::save(&self.pins) {
            let _ = write!(self.notice, "(not saved: {e})");
        }
        self.rank_dirty = true;
    }

    /// Record the current 24h% of every symbol into the history ring.
    fn sample(&mut self, now: Instant) {
        if self.tseq == 0 {
            return; // nothing received yet
        }
        let rows = &self.rows;
        self.hist.push(now, now_ms(), rows.len(), |i| if rows[i].has_tick { rows[i].chg as f32 } else { f32::NAN });
    }

    fn on_rest(&mut self, msg: oi::Msg) {
        match msg {
            oi::Msg::Commodities(list) => {
                self.commodities = list.into_iter().collect();
                for r in &mut self.rows {
                    r.commodity = self.commodities.contains(&r.sym);
                }
                self.rank_dirty |= self.class == 3;
            }
            oi::Msg::Oi(sym, val, ma) => {
                if let Some(&i) = self.index.get(&sym) {
                    (self.rows[i].oi, self.rows[i].oi_ma) = (val, ma);
                }
                self.oi_map.insert(sym, (val, ma));
            }
            oi::Msg::Note(n) => self.oi_note = n,
        }
    }

    fn set_funding(&mut self, res: Result<funding::Hours, String>, at: SystemTime) {
        match res {
            Ok(hours) => {
                for r in &mut self.rows {
                    r.fint = hours.get(&r.sym).copied().unwrap_or(funding::DEFAULT_HOURS);
                }
                self.fund_status = format!("intervals for {} symbols", hours.len());
                self.fund_hours = hours;
                self.fund_at = Some(at);
            }
            Err(e) if self.fund_hours.is_empty() => {
                self.fund_status = format!("assuming 8h intervals, fetch failed: {e}")
            }
            Err(_) => {} // keep the cached intervals
        }
        self.view_dirty = true;
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
            self.vols.advance(wall);
            for t in &v {
                ev = ev.max(t.ev);
                let r = self.row(t.s, now);
                // Volume traded since this symbol's last push: the change in rolling 24h quote volume,
                // plus what is expected to have dropped out of the far end of the window meanwhile.
                let q = pf(t.q);
                let dt = now.duration_since(r.at).as_secs_f64();
                let traded = if r.has_tick && dt < 30.0 { (q - r.qvol + r.qvol * dt / 86400.0).max(0.0) } else { 0.0 };
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
                r.vwap = pf(t.w);
                r.qvol = pf(t.q);
                r.trades = t.n;
                r.lqty = pf(t.lq);
                if let Some(st) = t.st {
                    r.st = st;
                }
                r.has_tick = true;
                r.seq = seq;
                r.at = now;
                if traded > 0.0 {
                    let i = self.index[t.s];
                    self.vols.add(i, traded);
                }
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
                r.next_fund = m.next;
            }
        } else if stream == "tradingSession" {
            let m: Session = serde_json::from_str(data)?;
            ev = m.ev;
            match self.sessions.iter_mut().find(|x| x.key == m.e) {
                Some(x) => {
                    set(&mut x.state, m.state);
                    x.end = m.end;
                }
                None => {
                    let rank = MARKETS.iter().position(|k| k.0 == m.e).unwrap_or(MARKETS.len());
                    self.sessions.push(Sess { key: m.e.to_string(), rank, state: m.state.to_string(), end: m.end });
                    self.sessions.sort_by_key(|x| x.rank);
                }
            }
        } else if stream.starts_with("!forceOrder") {
            let f: Force = serde_json::from_str(data)?;
            ev = f.ev;
            let px = if pf(f.o.ap) > 0.0 { f.o.ap } else { f.o.p };
            self.row(f.o.s, now);
            let row = self.index[f.o.s] as u32;
            // a forced SELL closes a long, a forced BUY closes a short
            let side = if f.o.side == "SELL" { liq::LONGS } else { liq::SHORTS };
            self.flow.add(wall, row, side, pf(f.o.q) * pf(px), px);
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
        self.view_dirty = true;
        self.notice.clear(); // a notice lasts until the next key
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            i += 1;
            if c == 3 {
                return true;
            }
            if self.pinning {
                match c {
                    27 => {
                        self.pinning = false;
                        i = b.len();
                    }
                    13 | 10 => {
                        self.pinning = false;
                        let input = String::from_utf8_lossy(&self.pin_buf).into_owned();
                        self.toggle_pins(&input);
                    }
                    127 | 8 => {
                        // drop one whole character, which may be several bytes
                        while self.pin_buf.pop().is_some_and(|x| x & 0xC0 == 0x80) {}
                    }
                    0x21..=0x7e | 0x80.. if self.pin_buf.len() < 64 => self.pin_buf.push(c.to_ascii_uppercase()),
                    _ => {}
                }
                continue;
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
                    } else if self.view != View::Board {
                        self.view = View::Board;
                    } else {
                        self.filter.clear();
                        self.page = 0;
                        self.rank_dirty = true;
                    }
                }
                b'q' | b'Q' => return true,
                b's' | b'S' if self.view == View::Arb => self.arb_sort = (self.arb_sort + 1) % hl::ARB_SORTS.len(),
                b's' => {
                    self.sort = (self.sort + 1) % SORTS.len();
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'S' => {
                    self.sort = (self.sort + SORTS.len() - 1) % SORTS.len();
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'r' => self.rank_dirty = true,
                b't' => {
                    self.class = (self.class + 1) % 4;
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'1'..=b'5' => self.level = Some((c - b'1') as usize),
                b'a' | b'0' => self.level = None,
                b'm' => {
                    self.market ^= 1;
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'/' => self.filtering = true,
                b'f' => {
                    self.pinning = true;
                    self.pin_buf.clear();
                }
                b'F' => {
                    self.pin_mode = (self.pin_mode + 1) % 3;
                    self.page = 0;
                    self.rank_dirty = true;
                }
                b'?' | b'h' => self.help = !self.help,
                b'l' => self.tape = !self.tape,
                b'p' => self.view = if self.view == View::Plot { View::Board } else { View::Plot },
                b'd' => self.view = if self.view == View::Dist { View::Board } else { View::Dist },
                b'A' => self.view = if self.view == View::Arb { View::Board } else { View::Arb },
                b'b' => self.view = View::Board,
                b'x' => self.plot_rebase = !self.plot_rebase,
                b'z' => self.plot_full = !self.plot_full,
                b'j' | b' ' => self.page += 1,
                b'k' => self.page = self.page.saturating_sub(1),
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
        let (market, class, pin_only) = (self.market, self.class, self.pin_mode == 1);
        let show_pins = self.pin_mode != 2;
        self.order.retain(|&i| passes(&self.rows[i as usize], market, class, pin_only, &self.filter));
        self.in_rank.clear();
        self.in_rank.resize(self.rows.len(), false);
        for &i in &self.order {
            self.in_rank[i as usize] = true;
        }
        let mut grew = false;
        for (i, r) in self.rows.iter().enumerate() {
            if !self.in_rank[i] && passes(r, market, class, pin_only, &self.filter) {
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
                // pinned symbols lead the page whatever the sort; the sort orders them among themselves
                (show_pins && y.pinned).cmp(&(show_pins && x.pinned)).then(match key {
                    Sort::Vol => y.qvol.total_cmp(&x.qvol),
                    Sort::ChgDesc => y.chg.total_cmp(&x.chg),
                    Sort::ChgAsc => x.chg.total_cmp(&y.chg),
                    // symbols with no funding rate yet go last either way
                    Sort::FundDesc => (y.has_fund, ann(y)).partial_cmp(&(x.has_fund, ann(x))).unwrap(),
                    Sort::FundAsc => (y.has_fund, ann(x)).partial_cmp(&(x.has_fund, ann(y))).unwrap(),
                })
                .then_with(|| x.sym.cmp(&y.sym))
            });
            self.rank_dirty = false;
            self.ranked_at = t0;
        }
        let (mut adv, mut dec, mut flat, mut vol, mut ntf) = (0u32, 0u32, 0u32, 0f64, 0u32);
        let (mut oi_sum, mut oi_n) = (0f64, 0u32);
        for &i in &self.order {
            let r = &self.rows[i as usize];
            ntf += r.tradfi as u32;
            oi_sum += r.oi;
            oi_n += (r.oi > 0.0) as u32;
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
            (0..CW.len()).rev().find(|&l| ncols_at(l) * grid_rows >= want && cw(l) <= w).unwrap_or(0)
        });
        let ncols = ncols_at(level);
        let slots = ncols * grid_rows;
        let pages = n.div_ceil(slots).max(1);
        self.page = self.page.min(pages - 1);

        self.out.clear();
        self.out.extend_from_slice(b"\x1b[?2026h");
        let layout = (w, h, level * 16 + sw + 1000 * self.view as usize, ncols);
        if layout != self.layout || WINCH.swap(false, Ordering::Relaxed) {
            self.layout = layout;
            self.w = w;
            self.h = h;
            self.prev.clear();
            self.view_dirty = true;
            self.out.extend_from_slice(b"\x1b[0m\x1b[2J");
        }
        self.prev.resize((FIXED + slots).max(h + 2), Vec::new());

        // ---- line 1: status
        let s = &mut self.scratch;
        s.clear();
        // Prompts and active filters go first, where a narrow terminal cannot cut them off.
        s.extend_from_slice(b"\x1b[0;1m TICKERTERM\x1b[0m");
        if self.pinning {
            s.extend_from_slice(b" | \x1b[1;33mpin/unpin: ");
            s.extend_from_slice(&self.pin_buf);
            s.extend_from_slice(b"_\x1b[0m");
        } else if !self.notice.is_empty() {
            let _ = write!(s, " | \x1b[1;33m{}\x1b[0m", self.notice.trim_end());
        }
        if self.filtering || !self.filter.is_empty() {
            let _ = write!(s, " | \x1b[1;33m/{}{}\x1b[0m", self.filter, if self.filtering { "_" } else { "" });
        }
        match self.pin_mode {
            1 => s.extend_from_slice(b" | \x1b[1;33m* PINNED ONLY\x1b[0m"),
            2 => s.extend_from_slice(b" | \x1b[2mfavourites hidden\x1b[0m"),
            _ => {}
        }
        let _ = write!(
            s,
            " | {}{}\x1b[0m | {} sym (\x1b[36m{} tradfi\x1b[0m) \x1b[32m+{}\x1b[0m \x1b[31m-{}\x1b[0m ={} | 24h vol ",
            if self.live { "\x1b[1;32m" } else { "\x1b[1;33m" },
            self.status,
            n,
            ntf,
            adv,
            dec,
            flat
        );
        hum(s, vol, 0);
        // open interest summed over the symbols of this tab; flagged while some are still to be fetched
        s.extend_from_slice(b" | OI ");
        if oi_n == 0 {
            s.push(b'-');
        } else {
            hum(s, oi_sum, 0);
            if (oi_n as usize) < n {
                let _ = write!(s, "\x1b[2m ({}/{})\x1b[0m", oi_n, n);
            }
        }
        if !self.oi_note.is_empty() {
            let _ = write!(s, " \x1b[33m{}\x1b[0m", self.oi_note);
        }
        let _ = write!(
            s,
            " | {}{} | sort {} ({}s ago) | lvl {}{} | pg {}/{}",
            ["USDT-PERP", "ALL"][self.market as usize],
            ["", " TRADFI", " CRYPTO", " COMMODITIES"][self.class as usize],
            SORTS[self.sort].1,
            t0.duration_since(self.ranked_at).as_secs(),
            level + 1,
            if self.level.is_none() { "(auto)" } else { "" },
            self.page + 1,
            pages
        );
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

        if self.view != View::Board {
            // Recomputed once per ticker push (or on a key / resize), not on every message.
            if self.view_dirty || self.view_seq != self.tseq {
                let pick = self.view_dirty; // a key, resize or filter change: re-pick the plotted set
                self.view_dirty = false;
                self.view_seq = self.tseq;
                let nl = h.saturating_sub(2).max(1);
                match self.view {
                    View::Plot => self.draw_plot(w, nl, pick),
                    View::Arb => self.draw_arb(w, nl),
                    _ => self.draw_dist(w, nl),
                }
                // cost of recomputing the view, shown at the end of its key line
                if let Some(l) = self.vlines.last_mut() {
                    let _ = write!(l, "\x1b[2m | computed in {}us\x1b[0m", t0.elapsed().as_micros());
                }
            }
            for (i, l) in self.vlines.iter().enumerate() {
                emit(&mut self.out, &mut self.prev[2 + i], l, 3 + i, 1, true);
            }
            self.out.extend_from_slice(b"\x1b[?2026l");
            let t1 = Instant::now();
            write_all(fd, &self.out);
            self.t_draw = (t1 - t0).as_secs_f64() * 1e6;
            self.t_write = t1.elapsed().as_secs_f64() * 1e6;
            return;
        }

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

        // ---- line 4: TradFi market sessions, with time left in the current state
        s.clear();
        s.extend_from_slice(b"\x1b[0;36m TRADFI\x1b[0m");
        if self.sessions.is_empty() {
            s.extend_from_slice(b"\x1b[2m sessions: waiting\x1b[0m");
        }
        let wall = now_ms();
        for x in &self.sessions {
            let label = MARKETS.get(x.rank).map(|k| k.1).unwrap_or_else(|| x.key.trim_end_matches("Update"));
            let (col, st) = match x.state.as_str() {
                "REGULAR" => ("\x1b[1;32m", "OPEN"),
                "PRE_MARKET" => ("\x1b[33m", "PRE"),
                "AFTER_MARKET" => ("\x1b[33m", "POST"),
                "OVERNIGHT" => ("\x1b[35m", "OVERNIGHT"),
                "NO_TRADING" => ("\x1b[31m", "CLOSED"),
                other => ("\x1b[0m", other),
            };
            let _ = write!(s, " {} {}{}\x1b[0m", label, col, st);
            let left = x.end.saturating_sub(wall) / 1000;
            if left >= 3600 {
                let _ = write!(s, " {}h{:02}m", left / 3600, (left / 60) % 60);
            } else if left >= 60 {
                let _ = write!(s, " {}m", left / 60);
            } else if left > 0 {
                let _ = write!(s, " {}s", left);
            }
            s.extend_from_slice(b" |");
        }
        emit(&mut self.out, &mut self.prev[FIXED - 1], s, 4, 1, true);

        // ---- line 5: column headers
        s.clear();
        s.extend_from_slice(b"\x1b[0;7m");
        for c in 0..ncols {
            let _ = write!(s, "{:<sw$} {:>10} {:>7} {:>10}", "SYMBOL", "MARK", "24h%", "VWAP");
            if level >= 1 {
                let _ = write!(s, " {:>8} {:>5}", "FUND/yr%", "NEXT");
            }
            if level >= 2 {
                let _ = write!(s, " {:>6} {:>6}", "QVOL", "OI");
            }
            if level >= 3 {
                let _ = write!(s, " {:>6} {:5}", "TRADES", "L<->H");
            }
            if level >= 4 {
                let _ = write!(s, " {:>10} {:>10} {:>10} {:>6}", "HIGH", "LOW", "LAST", "LQTY");
            }
            if c + 1 < ncols {
                s.extend_from_slice(b"  ");
            }
        }
        s.extend_from_slice(b"\x1b[0m");
        emit(&mut self.out, &mut self.prev[3], s, HDR_LINES, 1, true);

        // ---- grid (column-major)
        let base = self.page * slots;
        let now = Instant::now();
        for slot in 0..slots {
            let s = &mut self.scratch;
            s.clear();
            match self.order.get(base + slot) {
                Some(&i) => {
                    let r = &self.rows[i as usize];
                    cell(s, r, level, sw, wall, now.duration_since(r.at).as_secs() > 60, show_pins && r.pinned, self.vols.level(i as usize, wall), self.truecolor);
                }
                None => {
                    s.extend_from_slice(b"\x1b[0m");
                    s.resize(s.len() + cw(level), b' ');
                }
            }
            let (col, row) = (slot / grid_rows, slot % grid_rows);
            emit(&mut self.out, &mut self.prev[FIXED + slot], s, HDR_LINES + 1 + row, col * (cw(level) + 2) + 1, false);
        }

        // ---- footer: liquidation flow (shorts above, longs below) / help
        if self.help || !self.flow.seen {
            const HELP: [&[u8]; 2] = [
                b"\x1b[0;2m q quit | s/S sort | r re-rank now | m usdt-perp/all | t tradfi/crypto/commodities/both | f pin/unpin | F pins first/only/hidden | 1-5 detail, a auto | / filter (esc clears)\x1b[0m",
                b"\x1b[0;2m p plot | d distributions | A funding arbitrage vs Hyperliquid | arrows/j/k page | l liquidations: 5m totals by symbol / latest prints | ? liquidation flow\x1b[0m",
            ];
            emit(&mut self.out, &mut self.prev[5], HELP[0], h.max(FIXED) - 1, 1, true);
            emit(&mut self.out, &mut self.prev[4], HELP[1], h.max(FIXED), 1, true);
        } else {
            self.flow.advance(wall);
            self.draw_flow(w);
            emit(&mut self.out, &mut self.prev[5], &self.flines[liq::SHORTS], h.max(FIXED) - 1, 1, true);
            emit(&mut self.out, &mut self.prev[4], &self.flines[liq::LONGS], h.max(FIXED), 1, true);
        }

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
    if eol {
        // clear first: clearing after a line that exactly fills the width would eat its last column
        out.extend_from_slice(b"\x1b[0m\x1b[K");
    }
    out.extend_from_slice(new);
    prev.clear();
    prev.extend_from_slice(new);
}

/// Terminal columns taken by a character (2 for CJK / fullwidth / emoji, else 1).
fn wide(c: char) -> usize {
    let c = c as u32;
    let w = matches!(c,
        0x1100..=0x115F | 0x2E80..=0xA4CF | 0xAC00..=0xD7A3 | 0xF900..=0xFAFF | 0xFE30..=0xFE4F
        | 0xFF00..=0xFF60 | 0xFFE0..=0xFFE6 | 0x1F300..=0x1FAFF | 0x20000..=0x3FFFD);
    1 + w as usize
}

/// Mark prices arrive with 8 decimals; show them at the same precision as the traded price.
fn mark_disp<'a>(mark: &'a str, last: &str) -> &'a str {
    let (Some(p), false) = (mark.find('.'), last.is_empty()) else { return mark };
    let d = last.find('.').map(|q| last.len() - q - 1).unwrap_or(0);
    &mark[..if d == 0 { p } else { (p + 1 + d).min(mark.len()) }]
}

/// Background for a volume surge of brightness `g` (1..=surge::STEPS): dark amber up to bright yellow,
/// with black text once it is bright enough to need it.
fn glow(s: &mut Vec<u8>, g: u8, truecolor: bool) {
    let x = g as f32 / surge::STEPS as f32;
    if truecolor {
        let _ = write!(s, "\x1b[48;2;{};{};0m", (60.0 + 195.0 * x) as u8, (45.0 + 175.0 * x) as u8);
    } else {
        // the yellow diagonal of the 256-colour cube
        const RAMP: [u8; 5] = [58, 100, 142, 184, 226];
        let _ = write!(s, "\x1b[48;5;{}m", RAMP[((x * 5.0).ceil() as usize).clamp(1, 5) - 1]);
    }
    if x > 0.45 {
        s.extend_from_slice(b"\x1b[22;30m");
    }
}

/// Red-to-green background for a signed deviation `dev`: green above zero, red below, in ten shades
/// reaching full colour at `full`. Within a twentieth of `full` of zero there is no colour at all.
fn tint(s: &mut Vec<u8>, dev: f64, full: f64, truecolor: bool) {
    const STEPS: f64 = 10.0;
    let k = ((dev.abs() / full).min(1.0) * STEPS).round();
    if k == 0.0 {
        return;
    }
    let x = k / STEPS;
    if truecolor {
        let (hi, lo) = ((40.0 + 150.0 * x) as u8, (10.0 * (1.0 - x)) as u8);
        let _ = if dev > 0.0 { write!(s, "\x1b[48;2;{lo};{hi};{lo}m") } else { write!(s, "\x1b[48;2;{hi};{lo};{lo}m") };
    } else {
        const RAMP: [[u8; 5]; 2] = [[22, 28, 34, 40, 46], [52, 88, 124, 160, 196]];
        let _ = write!(s, "\x1b[48;5;{}m", RAMP[(dev < 0.0) as usize][((x * 5.0).ceil() as usize).clamp(1, 5) - 1]);
    }
    s.extend_from_slice(if x > 0.7 && (!truecolor || dev > 0.0) { b"\x1b[30m" } else { b"\x1b[97m" });
}

#[allow(clippy::too_many_arguments)]
fn cell(s: &mut Vec<u8>, r: &Row, level: usize, sw: usize, wall: u64, stale: bool, pin: bool, surge: u8, truecolor: bool) {
    let sgr = |s: &mut Vec<u8>, sign: f64, _bold: bool| {
        s.extend_from_slice(match (stale, sign > 0.0, sign < 0.0) {
            (true, ..) => b"\x1b[0;2m" as &[u8],
            (_, true, _) => b"\x1b[0;32m",
            (_, _, true) => b"\x1b[0;31m",
            _ => b"\x1b[0m",
        })
    };
    // crypto plain, TradFi cyan, commodities orange
    let kind = if r.commodity { 2 } else { r.tradfi as usize };
    const NAME: [[&[u8]; 3]; 3] = [
        [b"\x1b[0m", b"\x1b[0;36m", b"\x1b[0;38;5;214m"],
        [b"\x1b[0;1m", b"\x1b[0;1;36m", b"\x1b[0;1;38;5;214m"],
        [b"\x1b[0;2m", b"\x1b[0;2;36m", b"\x1b[0;2;38;5;214m"],
    ];
    s.extend_from_slice(NAME[if stale { 2 } else { 0 }][kind]);
    // pad by terminal columns: CJK tickers are two columns per character
    let mut used = 0;
    if pin {
        s.extend_from_slice(if stale { b"\x1b[0;2;33m*" } else { b"\x1b[0;1;33m*" });
        s.extend_from_slice(NAME[if stale { 2 } else { 1 }][kind]);
        used = 1;
    }
    if surge > 0 && !stale {
        glow(s, surge, truecolor);
    }
    for ch in disp(r).chars() {
        let cwid = wide(ch);
        if used + cwid > sw {
            break;
        }
        used += cwid;
        let mut u = [0u8; 4];
        s.extend_from_slice(ch.encode_utf8(&mut u).as_bytes());
    }
    // the glow covers the name only, not its padding, so neighbouring columns stay clean
    if surge > 0 && !stale {
        s.extend_from_slice(b"\x1b[0m");
    }
    s.resize(s.len() + sw - used + 1, b' ');
    // Mark price, cut to the contract's tick precision and drawn plain: it moves every second
    // for every symbol, so colouring or bolding it would make the whole board flicker.
    s.extend_from_slice(if stale { b"\x1b[0;2m" } else { b"\x1b[0m" });
    let mark = mark_disp(&r.mark_s, &r.last_s);
    price(s, mark, pf(mark), 10);
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
    let plain: &[u8] = if stale { b"\x1b[0;2m " } else { b"\x1b[0m " };
    // 24h VWAP, tinted by where the mark price sits against it: green when the mark is above
    // (VWAP below), red when it is below, nothing within 0.25%, full colour at 5%.
    s.extend_from_slice(plain);
    let m = pf(mark);
    if !stale && r.vwap > 0.0 && m > 0.0 {
        tint(s, m / r.vwap - 1.0, 0.05, truecolor);
    }
    price(s, &r.vwap_s, r.vwap, 10);
    s.extend_from_slice(if stale { b"\x1b[0;2m" } else { b"\x1b[0m" });
    if level >= 1 {
        if r.has_fund {
            // annualised, so contracts with 1h, 4h and 8h funding compare directly
            let a = ann(r);
            sgr(s, a, false);
            let _ = if a.abs() < 999.995 {
                write!(s, " {:>+8.2}", a)
            } else {
                write!(s, " {:>+8.0}", a.clamp(-999_999.0, 999_999.0))
            };
        } else {
            s.extend_from_slice(b"        -");
        }
        s.extend_from_slice(plain);
        // time until the next funding settlement
        let left = r.next_fund.saturating_sub(wall) / 1000;
        let mut tmp = [0u8; 16];
        let mut c = std::io::Cursor::new(&mut tmp[..]);
        let _ = if r.next_fund == 0 {
            write!(c, "-")
        } else if left >= 360_000 {
            write!(c, "{}d", left / 86400)
        } else if left >= 3600 {
            write!(c, "{}h{:02}m", left / 3600, (left / 60) % 60)
        } else if left >= 60 {
            // whole minutes only, so the column is not ticking every second
            write!(c, "{}m", left / 60)
        } else {
            write!(c, "{}s", left)
        };
        let n = c.position() as usize;
        s.resize(s.len() + 5usize.saturating_sub(n), b' ');
        s.extend_from_slice(&tmp[..n]);
    }
    if level >= 2 {
        s.extend_from_slice(plain);
        hum(s, r.qvol, 6);
        s.extend_from_slice(plain);
        if r.oi > 0.0 {
            if !stale && r.oi_ma > 0.0 {
                // against its one-hour average: green when positions are being added, red when closed
                tint(s, r.oi / r.oi_ma - 1.0, 0.05, truecolor);
            }
            hum(s, r.oi, 6);
            s.extend_from_slice(if stale { b"\x1b[0;2m" } else { b"\x1b[0m" });
        } else {
            s.extend_from_slice(b"     -");
        }
    }
    if level >= 3 {
        s.push(b' ');
        hum(s, r.trades as f64, 6);
        s.push(b' ');
        let pos = if r.high > r.low { ((r.last - r.low) / (r.high - r.low) * 4.999) as usize } else { 2 };
        for i in 0..5 {
            s.push(if i == pos.min(4) { b'#' } else { b'-' });
        }
    }
    if level >= 4 {
        s.push(b' ');
        price(s, &r.high_s, r.high, 10);
        s.push(b' ');
        price(s, &r.low_s, r.low, 10);
        s.push(b' ');
        price(s, &r.last_s, r.last, 10);
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
    let mut hide_pins = false;
    let mut class = 0u8;
    let mut rerank = 60u64;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--url" => url = args.next().expect("--url needs a value"),
            "--all" => all = true,
            "--hide-pins" => hide_pins = true,
            "--tradfi" => class = 1,
            "--crypto" => class = 2,
            "--commodities" => class = 3,
            "--rerank" => rerank = args.next().and_then(|v| v.parse().ok()).expect("--rerank needs seconds"),
            "--dump" => dump = args.next().and_then(|v| v.parse().ok()).unwrap_or(6),
            _ => {
                eprintln!(
                    "usage: tickerterm [--all] [--tradfi | --crypto | --commodities] [--hide-pins] [--rerank SECS] [--url wss://...] [--dump N]\n  \
                     --all      start with every symbol (default: USDT perpetuals only)\n  \
                     --tradfi   start on the TradFi-only page (--crypto for the opposite; 't' cycles)\n  \
                     --hide-pins  start with favourites hidden ('F' cycles first / only / hidden)\n  \
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
    // Funding intervals: use the disk cache straight away, refresh in the background when it expires.
    let mut first_fetch = Duration::ZERO;
    if let Some((hours, at)) = funding::load_cache() {
        let age = at.elapsed().unwrap_or(funding::TTL);
        first_fetch = funding::TTL.saturating_sub(age);
        app.set_funding(Ok(hours), at);
    }
    let funding_rx = funding::spawn(first_fetch);
    let rest_rx = oi::spawn();
    let hl_rx = hl::spawn();
    app.market = all as u8;
    app.class = class;
    app.pin_mode = if hide_pins { 2 } else { 0 };
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
        if let Ok(res) = funding_rx.try_recv() {
            app.set_funding(res, SystemTime::now());
        }
        if let Ok(res) = hl_rx.try_recv() {
            app.on_hl(res);
            dirty |= app.view == View::Arb;
        }
        while let Ok(msg) = rest_rx.try_recv() {
            app.on_rest(msg);
            dirty = true;
        }

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
        // After the frame is on screen: sampling never delays a redraw.
        if app.hist.due(t_rx) {
            app.sample(t_rx);
        }
    }
    term_leave();
}
