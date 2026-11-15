//! Background REST polling: which symbols are commodities, and open interest per symbol.
//!
//! * `GET /fapi/v1/exchangeInfo` lists every contract with its `underlyingType`; `COMMODITY` marks
//!   gold, silver, oil and so on. The symbol list is cached on disk and refreshed hourly.
//! * `GET /futures/data/openInterestHist` returns open interest statistics for ONE symbol per request,
//!   and is limited to 1000 requests per 5 minutes per IP. With ~800 symbols that rules out polling
//!   everything every minute, so the poller walks the symbols (busiest first) at a fixed pace that stays
//!   under half of the limit, and asks for the last 13 five-minute points each time: the newest is
//!   the value shown, the mean of the 12 before it is its one-hour moving average.
//!
//! Results are cached on disk with their fetch time, so a restart shows values at once and does not
//! spend requests on symbols fetched less than five minutes ago.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const TICKER_URL: &str = "https://fapi.binance.com/fapi/v1/ticker/24hr";
const OI_URL: &str = "https://fapi.binance.com/futures/data/openInterestHist";
const INFO_TTL: Duration = Duration::from_secs(3600);
/// The statistics are published every 5 minutes: no point asking for a symbol more often.
const FRESH_MS: u64 = 300_000;
/// Cached values older than this are not shown on start.
const SHOW_MS: u64 = 1_800_000;
/// Pause between requests: 1.5 a second = 450 per 5 minutes, 45% of the limit.
const GAP: Duration = Duration::from_millis(667);
/// The first pass after start goes faster (2.5 a second) to fill the board; still within the limit
/// together with the steady pace that follows it.
const GAP_FIRST: Duration = Duration::from_millis(400);
/// Five-minute points per request: the newest plus one hour before it.
const POINTS: usize = 13;

pub enum Msg {
    /// every commodity symbol
    Commodities(Vec<String>),
    /// symbol, open interest value in USD, its one-hour average
    Oi(String, f64, f64),
    /// a problem worth showing (rate limited, offline)
    Note(String),
}

#[derive(Deserialize)]
struct Info {
    symbols: Vec<InfoSym>,
}
#[derive(Deserialize)]
struct InfoSym {
    symbol: String,
    #[serde(default)]
    status: String,
    #[serde(rename = "contractType", default)]
    contract: String,
    #[serde(rename = "underlyingType", default)]
    underlying: String,
}

#[derive(Deserialize)]
struct Vol {
    symbol: String,
    #[serde(rename = "quoteVolume")]
    quote: String,
}

/// 24h quote volume per symbol, used only to decide who is fetched first.
fn parse_volumes(body: &str) -> HashMap<String, f64> {
    let v: Vec<Vol> = serde_json::from_str(body).unwrap_or_default();
    v.into_iter().map(|x| (x.symbol, x.quote.parse().unwrap_or(0.0))).collect()
}

#[derive(Deserialize)]
struct Point {
    #[serde(rename = "sumOpenInterestValue")]
    value: String,
}

struct Entry {
    val: f64,
    ma: f64,
    at: u64,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn cache_file(name: &str) -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))?;
    Some(base.join("tickerterm").join(name))
}

fn write_cache(name: &str, body: &str) {
    let Some(path) = cache_file(name) else { return };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    // write-then-rename so a crash never leaves a half-written cache
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, body).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// Tradable perpetuals as (symbol, is commodity), in the exchange's own order (majors first).
fn parse_info(body: &str) -> Result<Vec<(String, bool)>, String> {
    let info: Info = serde_json::from_str(body).map_err(|e| format!("bad exchangeInfo: {e}"))?;
    Ok(info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.contract.contains("PERPETUAL"))
        .map(|s| (s.symbol, s.underlying == "COMMODITY"))
        .collect())
}

/// `symbols.txt`: one "SYMBOL" or "SYMBOL C" per line.
fn load_symbols() -> Option<(Vec<(String, bool)>, SystemTime)> {
    let path = cache_file("symbols.txt")?;
    let at = std::fs::metadata(&path).ok()?.modified().ok()?;
    let text = std::fs::read_to_string(&path).ok()?;
    let syms: Vec<_> = text
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            Some((it.next()?.to_string(), it.next() == Some("C")))
        })
        .collect();
    (!syms.is_empty()).then_some((syms, at))
}

fn save_symbols(syms: &[(String, bool)]) {
    let mut body = String::with_capacity(syms.len() * 12);
    for (s, c) in syms {
        body.push_str(s);
        body.push_str(if *c { " C\n" } else { "\n" });
    }
    write_cache("symbols.txt", &body);
}

/// `open_interest.txt`: "SYMBOL value average fetched_ms" per line.
fn load_oi() -> HashMap<String, Entry> {
    let mut map = HashMap::new();
    let Some(text) = cache_file("open_interest.txt").and_then(|p| std::fs::read_to_string(p).ok()) else { return map };
    for l in text.lines() {
        let mut it = l.split_whitespace();
        let (Some(s), Some(v), Some(m), Some(t)) = (it.next(), it.next(), it.next(), it.next()) else { continue };
        if let (Ok(val), Ok(ma), Ok(at)) = (v.parse(), m.parse(), t.parse()) {
            map.insert(s.to_string(), Entry { val, ma, at });
        }
    }
    map
}

fn save_oi(map: &HashMap<String, Entry>) {
    let mut body = String::with_capacity(map.len() * 48);
    for (s, e) in map {
        use std::fmt::Write;
        let _ = writeln!(body, "{} {:.2} {:.2} {}", s, e.val, e.ma, e.at);
    }
    write_cache("open_interest.txt", &body);
}

enum Fail {
    /// rate limited or banned: wait this many seconds
    Limited(u64),
    Other(String),
}

fn get(agent: &ureq::Agent, url: &str) -> Result<String, Fail> {
    let mut resp = agent.get(url).call().map_err(|e| Fail::Other(e.to_string()))?;
    let code = resp.status().as_u16();
    if code == 429 || code == 418 {
        let wait = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
        // 418 is an IP ban for ignoring 429s: stay well away
        return Err(Fail::Limited(wait.unwrap_or(if code == 418 { 600 } else { 60 }).max(10)));
    }
    if code != 200 {
        return Err(Fail::Other(format!("http {code}")));
    }
    resp.body_mut().with_config().limit(32 << 20).read_to_string().map_err(|e| Fail::Other(e.to_string()))
}

/// Newest value and the mean of the points before it (the newest itself when there are none).
fn parse_oi(body: &str) -> Option<(f64, f64)> {
    let pts: Vec<Point> = serde_json::from_str(body).ok()?;
    let vals: Vec<f64> = pts.iter().filter_map(|p| p.value.parse().ok()).collect();
    let (&val, before) = vals.split_last()?;
    let ma = if before.is_empty() { val } else { before.iter().sum::<f64>() / before.len() as f64 };
    Some((val, ma))
}

pub fn spawn() -> Receiver<Msg> {
    let (tx, rx) = channel();
    std::thread::spawn(move || run(tx));
    rx
}

fn run(tx: Sender<Msg>) {
    macro_rules! send {
        ($m:expr) => {
            if tx.send($m).is_err() {
                return;
            }
        };
    }
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(15)))
        .user_agent("tickerterm")
        .http_status_as_error(false)
        .build()
        .into();

    let mut oi = load_oi();
    let now = now_ms();
    for (s, e) in &oi {
        if now.saturating_sub(e.at) < SHOW_MS && e.val > 0.0 {
            send!(Msg::Oi(s.clone(), e.val, e.ma));
        }
    }
    let (mut syms, mut syms_at) = match load_symbols() {
        Some((s, at)) => (s, Some(at)),
        None => (Vec::new(), None),
    };
    let commodities = |syms: &[(String, bool)]| Msg::Commodities(syms.iter().filter(|s| s.1).map(|s| s.0.clone()).collect());
    if !syms.is_empty() {
        send!(commodities(&syms));
    }

    let mut gap = GAP_FIRST;
    let mut saved = now;
    loop {
        if syms_at.is_none_or(|at| at.elapsed().map_or(true, |d| d >= INFO_TTL)) {
            match get(&agent, INFO_URL).map_err(|f| match f {
                Fail::Limited(s) => format!("rate limited for {s}s"),
                Fail::Other(e) => e,
            }) {
                Ok(body) => match parse_info(&body) {
                    Ok(list) if !list.is_empty() => {
                        save_symbols(&list);
                        send!(commodities(&list));
                        syms = list;
                        syms_at = Some(SystemTime::now());
                    }
                    Ok(_) => {}
                    Err(e) => send!(Msg::Note(e)),
                },
                Err(e) => {
                    send!(Msg::Note(format!("exchangeInfo: {e}")));
                    // keep working from the cached list and try again in a minute
                    syms_at = SystemTime::now().checked_sub(INFO_TTL - Duration::from_secs(60));
                }
            }
            if syms.is_empty() {
                std::thread::sleep(Duration::from_secs(60));
                continue;
            }
        }

        // Walk the symbols busiest first, so the ones that matter are filled and refreshed first.
        // One extra request per pass (weight 40 of the 2400 a minute the main API allows).
        if let Ok(body) = get(&agent, TICKER_URL) {
            let vol = parse_volumes(&body);
            let key = |s: &str| vol.get(s).copied().unwrap_or(0.0);
            syms.sort_by(|a, b| key(&b.0).total_cmp(&key(&a.0)));
        }
        let mut fetched = 0;
        for (sym, _) in &syms {
            let now = now_ms();
            if oi.get(sym).is_some_and(|e| now.saturating_sub(e.at) < FRESH_MS) {
                continue;
            }
            let url = format!("{OI_URL}?symbol={sym}&period=5m&limit={POINTS}");
            match get(&agent, &url) {
                Ok(body) => {
                    // an empty list (a contract too new to have statistics) still counts as asked
                    let (val, ma) = parse_oi(&body).unwrap_or((0.0, 0.0));
                    if val > 0.0 {
                        send!(Msg::Oi(sym.clone(), val, ma));
                    }
                    oi.insert(sym.clone(), Entry { val, ma, at: now });
                    fetched += 1;
                }
                Err(Fail::Limited(secs)) => {
                    send!(Msg::Note(format!("open interest: rate limited, pausing {secs}s")));
                    std::thread::sleep(Duration::from_secs(secs));
                }
                Err(Fail::Other(e)) => {
                    // offline, or a symbol the endpoint rejects: do not retry it before the next pass
                    if let Some(old) = oi.get_mut(sym) {
                        old.at = now;
                    } else {
                        oi.insert(sym.clone(), Entry { val: 0.0, ma: 0.0, at: now });
                    }
                    if fetched == 0 {
                        send!(Msg::Note(format!("open interest: {e}")));
                        std::thread::sleep(Duration::from_secs(5));
                    }
                }
            }
            if now.saturating_sub(saved) > 60_000 {
                save_oi(&oi);
                saved = now;
            }
            std::thread::sleep(gap);
        }
        gap = GAP;
        if fetched > 0 {
            save_oi(&oi);
            saved = now_ms();
        } else {
            // everything is fresh: wait for the oldest value to come due
            std::thread::sleep(Duration::from_secs(5));
        }
        if tx.send(Msg::Note(String::new())).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_statistics_and_symbols() {
        let body = r#"[{"symbol":"X","sumOpenInterest":"1","sumOpenInterestValue":"100.0","timestamp":1},
            {"symbol":"X","sumOpenInterest":"1","sumOpenInterestValue":"200.0","timestamp":2},
            {"symbol":"X","sumOpenInterest":"1","sumOpenInterestValue":"330.0","timestamp":3}]"#;
        assert_eq!(parse_oi(body), Some((330.0, 150.0)));
        assert_eq!(parse_oi(r#"[{"sumOpenInterestValue":"7"}]"#), Some((7.0, 7.0)));
        assert_eq!(parse_oi("[]"), None);
        assert_eq!(parse_oi(r#"{"code":-1121,"msg":"Invalid symbol."}"#), None);

        let info = r#"{"symbols":[
            {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","underlyingType":"COIN"},
            {"symbol":"XAUUSDT","status":"TRADING","contractType":"TRADIFI_PERPETUAL","underlyingType":"COMMODITY"},
            {"symbol":"OLDUSDT","status":"SETTLING","contractType":"PERPETUAL","underlyingType":"COIN"},
            {"symbol":"BTCUSDT_270326","status":"TRADING","contractType":"CURRENT_QUARTER","underlyingType":"COIN"}]}"#;
        assert_eq!(parse_info(info).unwrap(), vec![("BTCUSDT".to_string(), false), ("XAUUSDT".to_string(), true)]);
    }
}
