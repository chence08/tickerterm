//! Hyperliquid funding, for the funding-arbitrage view.
//!
//! One `POST https://api.hyperliquid.xyz/info {"type":"metaAndAssetCtxs"}` returns every perp of one
//! dex with its current funding rate (charged hourly), mark price, open interest and 24h volume.
//! Hyperliquid's own dex lists crypto; stocks, indices and commodities trade on builder-deployed
//! dexes, of which trade.xyz ("xyz") is polled too. A background thread asks for each every 10
//! seconds (request weight 20 each, against 1200 a minute allowed per IP).

use crate::{App, Row, ann, disp, hum, tint, wide};
use serde::Deserialize;
use std::io::Write;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

const URL: &str = "https://api.hyperliquid.xyz/info";
/// Dexes polled: "" is Hyperliquid's own, "xyz" is trade.xyz. Their assets are named "xyz:NVDA".
const DEXES: [&str; 2] = ["", "xyz"];
/// trade.xyz names that differ from Binance's base asset.
const ALIASES: [(&str, &str); 7] = [
    ("GOLD", "XAU"),
    ("SILVER", "XAG"),
    ("PLATINUM", "XPT"),
    ("PALLADIUM", "XPD"),
    ("BRENTOIL", "BZ"),
    ("SKHX", "SKHYNIX"),
    ("SMSN", "SAMSUNG"),
];
const EVERY: Duration = Duration::from_secs(10);
/// Hyperliquid charges funding every hour.
const PER_YEAR: f64 = 8760.0;
/// A Hyperliquid mark this far from Binance's means the ticker is not the same asset (or not the
/// same multiple of it): do not pair them.
const MAX_BASIS: f64 = 0.2;

#[derive(Deserialize)]
struct Meta {
    universe: Vec<Asset>,
}
#[derive(Deserialize)]
struct Asset {
    name: String,
    #[serde(rename = "isDelisted", default)]
    delisted: bool,
}
#[derive(Deserialize)]
struct Ctx {
    funding: String,
    #[serde(rename = "markPx", default)]
    mark: Option<String>,
    #[serde(rename = "openInterest", default)]
    oi: String,
    #[serde(rename = "dayNtlVlm", default)]
    vol: String,
}

pub struct Quote {
    /// the Binance symbol this asset corresponds to
    pub sym: String,
    /// funding per year, in percent
    pub fund: f64,
    pub mark: f64,
    pub oi_usd: f64,
    pub vol_usd: f64,
}

/// "BTC" -> "BTCUSDT"; Hyperliquid's "kPEPE" (1000 PEPE) is Binance's "1000PEPEUSDT";
/// trade.xyz's "xyz:GOLD" is "XAUUSDT".
fn binance_symbol(name: &str) -> String {
    if let Some((_dex, base)) = name.split_once(':') {
        let base = ALIASES.iter().find(|a| a.0 == base).map_or(base, |a| a.1);
        return format!("{base}USDT");
    }
    match name.strip_prefix('k') {
        Some(rest) if rest.chars().next().is_some_and(|c| c.is_ascii_uppercase()) => format!("1000{rest}USDT"),
        _ => format!("{name}USDT"),
    }
}

fn parse(body: &str) -> Result<Vec<Quote>, String> {
    let (meta, ctxs): (Meta, Vec<Ctx>) = serde_json::from_str(body).map_err(|e| format!("bad response: {e}"))?;
    let f = |s: &str| s.parse::<f64>().unwrap_or(0.0);
    Ok(meta
        .universe
        .iter()
        .zip(&ctxs)
        .filter(|(a, _)| !a.delisted)
        .filter_map(|(a, c)| {
            let mark = f(c.mark.as_deref()?);
            (mark > 0.0).then(|| Quote {
                sym: binance_symbol(&a.name),
                fund: f(&c.funding) * PER_YEAR * 100.0,
                mark,
                oi_usd: f(&c.oi) * mark,
                vol_usd: f(&c.vol),
            })
        })
        .collect())
}

pub fn spawn() -> Receiver<Result<Vec<Quote>, String>> {
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        let agent: ureq::Agent =
            ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(10))).user_agent("tickerterm").build().into();
        loop {
            // all dexes or nothing: a poll replaces the previous one as a whole
            let res = DEXES.iter().try_fold(Vec::new(), |mut all, dex| {
                let quotes = agent
                    .post(URL)
                    .header("Content-Type", "application/json")
                    .send(format!(r#"{{"type":"metaAndAssetCtxs","dex":"{dex}"}}"#))
                    .map_err(|e| e.to_string())
                    .and_then(|mut r| r.body_mut().read_to_string().map_err(|e| e.to_string()))
                    .and_then(|b| parse(&b))?;
                all.extend(quotes);
                Ok::<_, String>(all)
            });
            // back off after a failure (offline, rate limited)
            let wait = if res.is_ok() { EVERY } else { EVERY * 6 };
            if tx.send(res).is_err() {
                return;
            }
            std::thread::sleep(wait);
        }
    });
    rx
}

pub const ARB_SORTS: [&str; 3] = ["|SPREAD| v", "SPREAD v", "SPREAD ^"];

/// Binance annualised funding minus Hyperliquid's, in percentage points, when the symbol is quoted
/// on both and the two marks agree that it is the same asset.
fn spread(r: &Row, generation: u64) -> Option<f64> {
    let mark = crate::pf(&r.mark_s);
    let paired = r.hl_gen == generation && r.has_fund && mark > 0.0 && (r.hl_mark / mark - 1.0).abs() < MAX_BASIS;
    paired.then(|| ann(r) - r.hl_fund)
}

impl App {
    pub(crate) fn on_hl(&mut self, res: Result<Vec<Quote>, String>) {
        match res {
            Ok(quotes) => {
                self.hl_gen += 1;
                for q in quotes {
                    if let Some(&i) = self.index.get(&q.sym) {
                        let r = &mut self.rows[i];
                        // The same ticker can be a coin on one dex and a stock on another (BB, QNT):
                        // take the quote whose price says it is the asset Binance lists.
                        let mark = crate::pf(&r.mark_s);
                        if mark > 0.0 && (q.mark / mark - 1.0).abs() >= MAX_BASIS {
                            continue;
                        }
                        (r.hl_fund, r.hl_mark, r.hl_oi, r.hl_vol, r.hl_gen) = (q.fund, q.mark, q.oi_usd, q.vol_usd, self.hl_gen);
                    }
                }
                self.hl_at = Some(Instant::now());
                self.hl_err.clear();
            }
            Err(e) => self.hl_err = e,
        }
        self.view_dirty = true;
    }

    /// The funding-arbitrage table: every symbol quoted on both venues, ranked by funding spread.
    pub(crate) fn draw_arb(&mut self, w: usize, nl: usize) {
        let mut lines = std::mem::take(&mut self.vlines);
        let mut vals = std::mem::take(&mut self.pairs);
        lines.resize_with(nl, Vec::new);
        lines.iter_mut().for_each(Vec::clear);

        vals.clear();
        let generation = self.hl_gen;
        vals.extend(self.order.iter().filter_map(|&i| spread(&self.rows[i as usize], generation).map(|s| (s, i))));
        match self.arb_sort {
            0 => vals.sort_unstable_by(|a, b| b.0.abs().total_cmp(&a.0.abs())),
            1 => vals.sort_unstable_by(|a, b| b.0.total_cmp(&a.0)),
            _ => vals.sort_unstable_by(|a, b| a.0.total_cmp(&b.0)),
        }

        // SYMBOL BIN/yr% HL/yr% SPREAD TRADE BASIS% then liquidity on both sides
        const TW: usize = 99;
        let s = &mut lines[0];
        // rows a page holds, when the table fits at all
        let page_len = if nl >= 5 && w >= TW { ((w + 2) / (TW + 2)).max(1) * (nl - 3) } else { 1 };
        let pages = vals.len().div_ceil(page_len).max(1);
        self.arb_page = self.arb_page.min(pages - 1);
        let first = self.arb_page * page_len;
        let _ = write!(
            s,
            "\x1b[0;1m FUNDING ARBITRAGE\x1b[0m  Binance vs Hyperliquid, annualised | {} pairs | sort {} | pg {}/{}",
            vals.len(),
            ARB_SORTS[self.arb_sort],
            self.arb_page + 1,
            pages
        );
        match (self.hl_at, self.hl_err.is_empty()) {
            (Some(t), true) => {
                let _ = write!(s, "\x1b[2m | Hyperliquid {}s ago\x1b[0m", t.elapsed().as_secs());
            }
            (Some(t), false) => {
                let _ = write!(s, " | \x1b[33mHyperliquid {}s old: {}\x1b[0m", t.elapsed().as_secs(), self.hl_err);
            }
            (None, false) => {
                let _ = write!(s, " | \x1b[33mHyperliquid: {}\x1b[0m", self.hl_err);
            }
            (None, true) => s.extend_from_slice(b"\x1b[2m | waiting for Hyperliquid\x1b[0m"),
        }
        if nl >= 5 && w >= TW {
            let cols = ((w + 2) / (TW + 2)).max(1);
            let per = nl - 3;
            let s = &mut lines[1];
            s.extend_from_slice(b"\x1b[0;7m");
            for c in 0..cols {
                let _ = write!(
                    s,
                    "{:<10} {:>9} {:>9} {:>9}  {:<11} {:>7} {:>7} {:>7} {:>7} {:>7}{}",
                    "SYMBOL", "BIN/yr%", "HL/yr%", "SPREAD", "COLLECT BY", "BASIS%", "BIN OI", "HL OI", "BIN VOL", "HL VOL",
                    if c + 1 < cols { "  " } else { "" }
                );
            }
            s.extend_from_slice(b"\x1b[0m");
            for (k, &(sp, i)) in vals.iter().skip(first).take(cols * per).enumerate() {
                let r = &self.rows[i as usize];
                let s = &mut lines[2 + k % per];
                // rows of a later table column start where the previous one ended
                let name = disp(r);
                let mut used = 0;
                s.extend_from_slice(if r.commodity { b"\x1b[0;38;5;214m" } else if r.tradfi { b"\x1b[0;36m" } else { b"\x1b[0m" });
                for ch in name.chars() {
                    if used + wide(ch) > 10 {
                        break;
                    }
                    used += wide(ch);
                    let mut u = [0u8; 4];
                    s.extend_from_slice(ch.encode_utf8(&mut u).as_bytes());
                }
                s.resize(s.len() + 10 - used, b' ');
                let sign = |v: f64| if v > 0.0 { "\x1b[0;32m" } else if v < 0.0 { "\x1b[0;31m" } else { "\x1b[0m" };
                let _ = write!(s, "{} {:>+9.2}{} {:>+9.2}\x1b[0m ", sign(ann(r)), ann(r).clamp(-99999.0, 99999.0), sign(r.hl_fund), r.hl_fund.clamp(-99999.0, 99999.0));
                // full colour at 50 points a year; under 2.5 there is nothing worth the fees
                tint(s, sp, 50.0, self.truecolor);
                let _ = write!(s, "{:>+9.2}\x1b[0m  ", sp.clamp(-99999.0, 99999.0));
                // the side with the higher rate is the one to be short on: shorts receive positive funding
                s.extend_from_slice(if sp > 0.0 { b"S BIN  L HL" } else if sp < 0.0 { b"L BIN  S HL" } else { b"-          " });
                let _ = write!(s, " {:>+7.2}", (r.hl_mark / crate::pf(&r.mark_s) - 1.0) * 100.0);
                for v in [r.oi, r.hl_oi, r.qvol, r.hl_vol] {
                    s.push(b' ');
                    if v > 0.0 {
                        hum(s, v, 7);
                    } else {
                        s.extend_from_slice(b"      -");
                    }
                }
                if k / per + 1 < cols {
                    s.extend_from_slice(b"  ");
                }
            }
            let more = vals.len().saturating_sub(first + cols * per);
            let s = &mut lines[nl - 1];
            s.extend_from_slice(b"\x1b[0;2m A/esc board | s sort | arrows/j/k page | m t / filters apply | SPREAD = Binance - Hyperliquid, % a year | BASIS = HL mark vs Binance mark");
            if more > 0 {
                let _ = write!(s, " | {} more below", more);
            }
            s.extend_from_slice(b"\x1b[0m");
        } else {
            lines[nl - 1].extend_from_slice(b" terminal too small for the arbitrage table (needs 99 columns)");
        }
        self.vlines = lines;
        self.pairs = vals;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_maps_names() {
        assert_eq!(binance_symbol("BTC"), "BTCUSDT");
        assert_eq!(binance_symbol("kPEPE"), "1000PEPEUSDT");
        assert_eq!(binance_symbol("xyz:NVDA"), "NVDAUSDT");
        assert_eq!(binance_symbol("xyz:GOLD"), "XAUUSDT");
        assert_eq!(binance_symbol("xyz:SKHX"), "SKHYNIXUSDT");
        let body = r#"[{"universe":[{"name":"BTC","szDecimals":5},{"name":"kSHIB"},{"name":"OLD","isDelisted":true},{"name":"NOPX"}]},
            [{"funding":"0.00001","openInterest":"100","markPx":"50000","dayNtlVlm":"1e9"},
             {"funding":"-0.00002","openInterest":"10","markPx":"0.02","dayNtlVlm":"5"},
             {"funding":"0","openInterest":"0","markPx":"1","dayNtlVlm":"0"},
             {"funding":"0","openInterest":"0","markPx":null,"dayNtlVlm":"0"}]]"#;
        let q = parse(body).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].sym, "BTCUSDT");
        assert!((q[0].fund - 8.76).abs() < 1e-9);
        assert_eq!(q[0].oi_usd, 5_000_000.0);
        assert_eq!(q[1].sym, "1000SHIBUSDT");
        assert!((q[1].fund + 17.52).abs() < 1e-9);
        assert!(parse("{}").is_err());
    }
}
