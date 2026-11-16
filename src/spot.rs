//! Rolling window statistics for Binance spot, from the all-market `!ticker_<window>@arr` stream:
//! price change, open, high, low, last, volume-weighted average, volume and trade count over the
//! trailing window. The stream carries every spot symbol whose statistics changed, about once a
//! second; this keeps two pinned pairs and every BTC-quoted market.
//!
//! Spot lives on a different host from the futures streams, so this is a second connection. It runs
//! on its own thread with a blocking socket; each batch is published into a shared slot and the
//! main loop is woken through a pipe it polls alongside the futures socket and the keyboard, so the
//! board never waits on this connection.

use crate::{App, hum};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tungstenite::Message;

const HOST: &str = "stream.binance.com";
const PORT: u16 = 9443;
/// Window the statistics cover; the stream offers 1h, 4h and 1d.
const WINDOW: &str = "1d";
/// (symbol, label, base asset, quote asset), one pinned line each
pub const PAIRS: [(&str, &str, &str, &str); 2] = [("USDCUSDT", "USDC/USDT", "USDC", "USDT"), ("ETHBTC", "ETH/BTC", "ETH", "BTC")];

/// One element of the stream's array:
/// `{"e":"1hTicker","E":1672515782136,"s":"BNBBTC","p":"0.0015","P":"250.00","o":"0.0010",
///   "h":"0.0025","l":"0.0010","c":"0.0025","w":"0.0018","v":"10000","q":"18","O":0,"C":1675216573749,
///   "F":0,"L":18150,"n":18151}`
#[derive(Deserialize)]
struct Wire<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(borrow)]
    p: &'a str,
    #[serde(rename = "P", borrow)]
    pct: &'a str,
    #[serde(borrow)]
    o: &'a str,
    #[serde(borrow)]
    h: &'a str,
    #[serde(borrow)]
    l: &'a str,
    #[serde(borrow)]
    c: &'a str,
    #[serde(borrow)]
    w: &'a str,
    #[serde(borrow)]
    v: &'a str,
    #[serde(borrow)]
    q: &'a str,
    n: u64,
}

#[derive(Clone, Default)]
pub struct Tick {
    pub change: f64,
    pub pct: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub last: f64,
    pub vwap: f64,
    /// volume in the base asset
    pub vol: f64,
    /// volume in the quote asset
    pub qvol: f64,
    pub trades: u64,
    pub live: bool,
    pub updates: u64,
}

/// A BTC-quoted spot market.
#[derive(Clone)]
pub struct Market {
    pub sym: String,
    pub t: Tick,
}

/// What the main loop sees of the feed.
#[derive(Clone, Default)]
pub struct Snapshot {
    /// the pinned pairs, in `PAIRS` order
    pub pins: [Tick; PAIRS.len()],
    /// every BTC-quoted market seen so far
    pub btc: Vec<Market>,
    /// BTCUSDT last price on spot, to show BTC volume in dollars
    pub btc_usd: f64,
    pub live: bool,
}

#[derive(Default)]
pub struct Shared {
    snap: Snapshot,
    /// symbol -> position in `snap.btc`
    index: HashMap<String, usize>,
}

pub struct Feed {
    pub shared: Arc<Mutex<Shared>>,
    /// set by the feed when it has published something the main loop has not picked up yet
    pub pending: Arc<AtomicBool>,
    /// read end of the wake-up pipe, for poll()
    pub wake_fd: i32,
}

impl Feed {
    /// Take the latest statistics if there are new ones, emptying the wake-up pipe.
    pub fn take(&self) -> Option<Snapshot> {
        if !self.pending.swap(false, Ordering::AcqRel) {
            return None;
        }
        let mut buf = [0u8; 64];
        while unsafe { libc::read(self.wake_fd, buf.as_mut_ptr() as *mut _, buf.len()) } > 0 {}
        Some(self.shared.lock().unwrap().snap.clone())
    }
}

/// The same statistics from `GET /api/v3/ticker/24hr`, used to fill the table at start-up: the
/// stream only mentions a symbol when it trades, which for a quiet BTC pair can take hours.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Rest<'a> {
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(borrow)]
    price_change: &'a str,
    #[serde(borrow)]
    price_change_percent: &'a str,
    #[serde(borrow)]
    weighted_avg_price: &'a str,
    #[serde(borrow)]
    open_price: &'a str,
    #[serde(borrow)]
    high_price: &'a str,
    #[serde(borrow)]
    low_price: &'a str,
    #[serde(borrow)]
    last_price: &'a str,
    #[serde(borrow)]
    volume: &'a str,
    #[serde(borrow)]
    quote_volume: &'a str,
    count: u64,
}

/// Fetch the 24h statistics of every spot symbol and apply those that traded in the window, so
/// delisted pairs stay out. Only meaningful while `WINDOW` is a day.
fn seed(shared: &Mutex<Shared>) -> Result<(), String> {
    let agent: ureq::Agent =
        ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(15))).user_agent("tickerterm").build().into();
    let body = agent.get("https://api.binance.com/api/v3/ticker/24hr").call().map_err(|e| e.to_string())?.body_mut().read_to_string().map_err(|e| e.to_string())?;
    let all: Vec<Rest> = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    let batch: Vec<Wire> = all
        .iter()
        .filter(|r| r.count > 0)
        .map(|r| Wire {
            s: r.symbol,
            p: r.price_change,
            pct: r.price_change_percent,
            o: r.open_price,
            h: r.high_price,
            l: r.low_price,
            c: r.last_price,
            w: r.weighted_avg_price,
            v: r.volume,
            q: r.quote_volume,
            n: r.count,
        })
        .collect();
    let mut sh = shared.lock().unwrap();
    sh.snap.live = true;
    apply(&mut sh, &batch);
    Ok(())
}

fn set(t: &mut Tick, w: &Wire) {
    let f = |s: &str| s.parse::<f64>().unwrap_or(0.0);
    (t.change, t.pct, t.open, t.high, t.low, t.last) = (f(w.p), f(w.pct), f(w.o), f(w.h), f(w.l), f(w.c));
    (t.vwap, t.vol, t.qvol, t.trades) = (f(w.w), f(w.v), f(w.q), w.n);
    t.live = true;
    t.updates += 1;
}

/// Apply one batch. Returns whether anything we keep was in it.
fn apply(sh: &mut Shared, batch: &[Wire]) -> bool {
    let mut any = false;
    for w in batch {
        if let Some(i) = PAIRS.iter().position(|p| p.0 == w.s) {
            set(&mut sh.snap.pins[i], w);
            any = true;
        }
        if w.s == "BTCUSDT" {
            sh.snap.btc_usd = w.c.parse().unwrap_or(sh.snap.btc_usd);
            any = true;
        }
        if w.s.len() > 3 && w.s.ends_with("BTC") {
            let i = match sh.index.get(w.s) {
                Some(&i) => i,
                None => {
                    sh.index.insert(w.s.to_string(), sh.snap.btc.len());
                    sh.snap.btc.push(Market { sym: w.s.to_string(), t: Tick::default() });
                    sh.snap.btc.len() - 1
                }
            };
            set(&mut sh.snap.btc[i].t, w);
            any = true;
        }
    }
    any
}

fn session(shared: &Mutex<Shared>, publish: &dyn Fn()) -> Result<(), String> {
    let addr = (HOST, PORT).to_socket_addrs().map_err(|e| format!("dns: {e}"))?.next().ok_or("dns: no addresses")?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).map_err(|e| e.to_string())?;
    tcp.set_nodelay(true).map_err(|e| e.to_string())?;
    // the server pings every 20s, so a minute of silence means the connection is dead
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
    let url = format!("wss://{HOST}:{PORT}/ws/!ticker_{WINDOW}@arr");
    let (mut ws, _) = tungstenite::client_tls(url.as_str(), tcp).map_err(|e| format!("handshake: {e}"))?;
    // subscribed first, so nothing that trades while the snapshot is in flight is lost for long
    if WINDOW == "1d" && seed(shared).is_ok() {
        publish();
    }
    loop {
        match ws.read().map_err(|e| e.to_string())? {
            Message::Text(txt) => {
                let Ok(batch) = serde_json::from_str::<Vec<Wire>>(&txt) else { continue };
                let mut sh = shared.lock().unwrap();
                sh.snap.live = true;
                let any = apply(&mut sh, &batch);
                drop(sh);
                if any {
                    publish();
                }
            }
            // tungstenite queues the pong; reading again flushes it
            Message::Ping(_) => ws.flush().map_err(|e| e.to_string())?,
            Message::Close(_) => return Err("closed by server".into()),
            _ => {}
        }
    }
}

pub fn spawn() -> Feed {
    let mut fds = [0i32; 2];
    unsafe {
        libc::pipe(fds.as_mut_ptr());
        for fd in fds {
            libc::fcntl(fd, libc::F_SETFL, libc::fcntl(fd, libc::F_GETFL) | libc::O_NONBLOCK);
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    let shared = Arc::new(Mutex::new(Shared::default()));
    let pending = Arc::new(AtomicBool::new(false));
    let feed = Feed { shared: shared.clone(), pending: pending.clone(), wake_fd: fds[0] };
    let wfd = fds[1];
    std::thread::spawn(move || {
        // one byte in the pipe per unread publication: a burst of updates wakes the main loop once
        let publish = || {
            if !pending.swap(true, Ordering::AcqRel) {
                unsafe { libc::write(wfd, b"x".as_ptr() as *const _, 1) };
            }
        };
        let mut backoff = 1;
        loop {
            let started = Instant::now();
            let _ = session(&shared, &publish);
            {
                let mut sh = shared.lock().unwrap();
                sh.snap.live = false;
                sh.snap.pins.iter_mut().for_each(|t| t.live = false);
            }
            publish();
            if started.elapsed() > Duration::from_secs(30) {
                backoff = 1;
            }
            std::thread::sleep(Duration::from_secs(backoff));
            backoff = (backoff * 2).min(30);
        }
    });
    feed
}

/// One pinned line per pair: last price and its change over the window, the window's open, high,
/// low and volume-weighted average, volume in both assets and the number of trades.
pub fn line(s: &mut Vec<u8>, i: usize, t: &Tick, hum: fn(&mut Vec<u8>, f64, usize)) {
    let (_, label, base, quote) = PAIRS[i];
    let _ = write!(s, "\x1b[0;1m {label:>9}\x1b[0;2m spot {WINDOW}\x1b[0m");
    if t.updates == 0 {
        s.extend_from_slice(b"\x1b[2m | connecting\x1b[0m");
        return;
    }
    let dim = if t.live { "" } else { "2;" };
    let col = if t.change > 0.0 { "32" } else if t.change < 0.0 { "31" } else { "39" };
    let _ = write!(s, " | \x1b[0;{dim}1m{:.5}\x1b[0;{dim}{col}m {:+.3}% ({:+.5})\x1b[0;{dim}m", t.last, t.pct, t.change);
    let _ = write!(s, " | open {:.5} high {:.5} low {:.5} | vwap {:.6} | vol ", t.open, t.high, t.low, t.vwap);
    hum(s, t.vol, 5);
    let _ = write!(s, " {base} ");
    hum(s, t.qvol, 5);
    let _ = write!(s, " {quote} | ");
    hum(s, t.trades as f64, 5);
    s.extend_from_slice(b" trades");
    if !t.live {
        s.extend_from_slice(b" | \x1b[0;33mreconnecting");
    }
    s.extend_from_slice(b"\x1b[0m");
}

pub const BTC_SORTS: [&str; 4] = ["VOL v", "CHG% v", "CHG% ^", "NAME"];

impl App {
    /// The BTC markets table: every BTC-quoted spot pair, with its rolling window statistics.
    pub(crate) fn draw_btc(&mut self, w: usize, nl: usize) {
        let mut lines = std::mem::take(&mut self.vlines);
        lines.resize_with(nl, Vec::new);
        lines.iter_mut().for_each(Vec::clear);
        let snap = std::mem::take(&mut self.spot);

        let mut vals: Vec<&Market> = snap.btc.iter().filter(|m| self.filter.is_empty() || m.sym.contains(self.filter.as_str())).collect();
        match self.btc_sort {
            0 => vals.sort_unstable_by(|a, b| b.t.qvol.total_cmp(&a.t.qvol)),
            1 => vals.sort_unstable_by(|a, b| b.t.pct.total_cmp(&a.t.pct)),
            2 => vals.sort_unstable_by(|a, b| a.t.pct.total_cmp(&b.t.pct)),
            _ => vals.sort_unstable_by(|a, b| a.sym.cmp(&b.sym)),
        }

        const TW: usize = 98;
        let fits = nl >= 5 && w >= TW;
        let cols = ((w + 2) / (TW + 2)).max(1);
        let per = nl.saturating_sub(3).max(1);
        let page_len = if fits { cols * per } else { 1 };
        let pages = vals.len().div_ceil(page_len).max(1);
        self.btc_page = self.btc_page.min(pages - 1);
        let first = self.btc_page * page_len;

        let s = &mut lines[0];
        let total: f64 = vals.iter().map(|m| m.t.qvol).sum();
        let _ = write!(s, "\x1b[0;1m BTC MARKETS\x1b[0m  Binance spot, rolling {WINDOW} | {} pairs | vol ", vals.len());
        hum(s, total, 0);
        s.extend_from_slice(b" BTC");
        if snap.btc_usd > 0.0 {
            s.extend_from_slice(b" = $");
            hum(s, total * snap.btc_usd, 0);
            let _ = write!(s, " | BTC/USDT {:.2}", snap.btc_usd);
        }
        let _ = write!(s, " | sort {} | pg {}/{}", BTC_SORTS[self.btc_sort], self.btc_page + 1, pages);
        if !snap.live {
            s.extend_from_slice(if snap.btc.is_empty() { b"\x1b[2m | connecting\x1b[0m" } else { b" | \x1b[33mreconnecting\x1b[0m" });
        }
        if fits {
            let s = &mut lines[1];
            s.extend_from_slice(b"\x1b[0;7m");
            for c in 0..cols {
                let _ = write!(
                    s,
                    "{:<10} {:>12} {:>8} {:>12} {:>12} {:>12} {:>8} {:>8} {:>7}{}",
                    "SYMBOL", "LAST", "CHG%", "HIGH", "LOW", "VWAP", "VOL BTC", "VOL $", "TRADES",
                    if c + 1 < cols { "  " } else { "" }
                );
            }
            s.extend_from_slice(b"\x1b[0m");
            for (k, m) in vals.iter().skip(first).take(page_len).enumerate() {
                let t = &m.t;
                let s = &mut lines[2 + k % per];
                let base = &m.sym[..m.sym.len() - 3];
                let col = if t.pct > 0.0 { "\x1b[0;32m" } else if t.pct < 0.0 { "\x1b[0;31m" } else { "\x1b[0m" };
                let _ = write!(s, "\x1b[0m{:<10.10} {:>12.8} {col}{:>+8.2}\x1b[0m {:>12.8} {:>12.8} {:>12.8} ", base, t.last, t.pct.clamp(-9999.0, 9999.0), t.high, t.low, t.vwap);
                hum(s, t.qvol, 8);
                s.push(b' ');
                if snap.btc_usd > 0.0 {
                    hum(s, t.qvol * snap.btc_usd, 8);
                } else {
                    s.extend_from_slice(b"       -");
                }
                s.push(b' ');
                hum(s, t.trades as f64, 7);
                if k / per + 1 < cols {
                    s.extend_from_slice(b"  ");
                }
            }
            let more = vals.len().saturating_sub(first + page_len);
            let s = &mut lines[nl - 1];
            s.extend_from_slice(b"\x1b[0;2m B/esc board | s sort | arrows/j/k page | / filter | prices in BTC | pairs with no trade in the window are left out");
            if more > 0 {
                let _ = write!(s, " | {} more below", more);
            }
            s.extend_from_slice(b"\x1b[0m");
        } else {
            lines[nl - 1].extend_from_slice(b" terminal too small for the BTC markets table (needs 98 columns)");
        }
        drop(vals);
        self.spot = snap;
        self.vlines = lines;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_pins_and_btc_quoted() {
        let txt = r#"[{"e":"1dTicker","E":1,"s":"ETHBTC","p":"0.00002","P":"0.072","o":"0.02793","h":"0.02817","l":"0.02783","c":"0.02795","w":"0.028015","v":"5470","q":"153","O":0,"C":1,"F":0,"L":9,"n":33600},
            {"e":"1dTicker","E":1,"s":"BTCUSDT","p":"1","P":"0.1","o":"64000","h":"65000","l":"63000","c":"64500.5","w":"64100","v":"10","q":"641000","O":0,"C":1,"F":0,"L":9,"n":5},
            {"e":"1dTicker","E":1,"s":"SOLUSDT","p":"1","P":"0.1","o":"1","h":"1","l":"1","c":"1","w":"1","v":"1","q":"1","O":0,"C":1,"F":0,"L":9,"n":5}]"#;
        let batch: Vec<Wire> = serde_json::from_str(txt).unwrap();
        let mut sh = Shared::default();
        assert!(apply(&mut sh, &batch));
        assert!(apply(&mut sh, &batch[..1]));
        assert!(!apply(&mut sh, &batch[2..]));
        assert_eq!(sh.snap.btc.len(), 1);
        assert_eq!(sh.snap.btc[0].sym, "ETHBTC");
        assert_eq!(sh.snap.btc[0].t.updates, 2);
        assert_eq!(sh.snap.btc[0].t.qvol, 153.0);
        assert_eq!(sh.snap.pins[1].trades, 33600);
        assert_eq!(sh.snap.pins[0].updates, 0);
        assert_eq!(sh.snap.btc_usd, 64500.5);
    }
}
