//! Funding interval per symbol, from Binance's `GET /fapi/v1/fundingInfo`.
//!
//! The WebSocket streams give the funding rate but not how often it is charged, which is needed
//! to annualise it. This is fetched on a background thread so the display never waits on it, and
//! cached on disk so a restart has the intervals immediately.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime};

const URL: &str = "https://fapi.binance.com/fapi/v1/fundingInfo";
/// Intervals change rarely; refresh hourly.
pub const TTL: Duration = Duration::from_secs(3600);
pub const DEFAULT_HOURS: u8 = 8;

pub type Hours = HashMap<String, u8>;

#[derive(Deserialize)]
struct Item {
    symbol: String,
    #[serde(rename = "fundingIntervalHours")]
    hours: u8,
}

fn parse(body: &str) -> Result<Hours, String> {
    let items: Vec<Item> = serde_json::from_str(body).map_err(|e| format!("bad response: {e}"))?;
    Ok(items.into_iter().filter(|i| i.hours > 0).map(|i| (i.symbol, i.hours)).collect())
}

fn cache_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))?;
    Some(base.join("tickerterm").join("funding_info.json"))
}

/// Cached intervals and when they were fetched, if a readable cache exists (however old).
pub fn load_cache() -> Option<(Hours, SystemTime)> {
    let path = cache_path()?;
    let at = std::fs::metadata(&path).ok()?.modified().ok()?;
    let hours = parse(&std::fs::read_to_string(&path).ok()?).ok()?;
    Some((hours, at))
}

fn fetch() -> Result<Hours, String> {
    let agent: ureq::Agent =
        ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(15))).user_agent("tickerterm").build().into();
    let body = agent
        .get(URL)
        .call()
        .map_err(|e| e.to_string())?
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;
    let hours = parse(&body)?;
    if let Some(path) = cache_path() {
        // write-then-rename so a crash never leaves a half-written cache
        let tmp = path.with_extension("json.tmp");
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if std::fs::write(&tmp, &body).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }
    Ok(hours)
}

/// Fetch after `first_delay`, then every `TTL` (retrying each minute after a failure).
pub fn spawn(first_delay: Duration) -> Receiver<Result<Hours, String>> {
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        std::thread::sleep(first_delay);
        loop {
            let res = fetch();
            let wait = if res.is_ok() { TTL } else { Duration::from_secs(60) };
            if tx.send(res).is_err() {
                return;
            }
            std::thread::sleep(wait);
        }
    });
    rx
}
