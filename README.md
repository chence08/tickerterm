# tickerterm

A low-latency terminal board for Binance USD-M futures. It shows every contract's 24h ticker in a dense grid, straight from the public market-wide WebSocket streams. No API key or account is needed; it only reads public market data.

```
 TICKERTERM binance usd-m | LIVE | 657 sym (135 tradfi) +83 -572 =2 | 24h vol 47.0B | USDT-PERP | sort VOL v (2s ago) | lvl 2 | pg 1/44
 lat E->rx 46ms (min 32 avg 49 max 190) | rx->screen 77us (parse 12 draw 62 write 2, max 777) | 11.0 msg/s 222 KB/s 195 upd/s | up 00:01
 UP EVAA +68.4% AKE +66.0% | DN KAVA -54.9% BULLA -30.2%
 TRADFI US CLOSED 33h45m | CMDTY CLOSED 31h45m | FX CLOSED 30h45m | KR CLOSED 32h45m | HK CLOSED 34h15m | CN CLOSED 34h15m |
SYMBOL           MARK    24h%    FUND%  NEXT  SYMBOL           MARK    24h%    FUND%  NEXT  SYMBOL           MARK    24h%    FUND%  NEXT
BTC          65614.50   -4.57  +0.0035   45m  AKE         0.0125109  +65.97  +0.0050   45m  HYPE         67.31800   -8.09  +0.0006   45m
ETH           1842.99   -4.96  +0.0012   45m  NVDA        161.10697   +0.16  -0.0433   45m  XAU           3876.01   -0.53  +0.0000   45m
SOXL         55.98000   +1.05  +0.0000   45m  SNDK        861.45000   +0.22  +0.0000   45m  SOXS         52.89000   -0.15  +0.0000   45m
SOL           74.0000   -7.52  -0.0031   45m  SPCX        135.37833   -1.91  -0.0014   45m  BNB           597.842   -5.87  +0.0000   45m
ZEC            883.59   -6.85  +0.0078   45m  XRP            0.9817   -4.54  +0.0025   45m  MU          763.33678   +0.14  +0.0000   45m
 LIQ 15:14:41 CRCL SELL   744 @59.39000 | 15:14:40 MSTR SELL 3.54K @104.23000 | 15:14:33 INIT SELL  29.6 @0.0967217 |
```

## Install

**Prebuilt binary** (Linux x86_64 / arm64, macOS Apple Silicon): download the archive for your platform from the [latest release](https://github.com/chence08/tickerterm/releases/latest), then:

```bash
tar -xzf tickerterm-*.tar.gz
./tickerterm
```

The Linux binaries are statically linked, so they run on any distribution.

**With Cargo** (Rust 1.85 or newer):

```bash
cargo install --git https://github.com/chence08/tickerterm
```

**From source:**

```bash
git clone https://github.com/chence08/tickerterm
cd tickerterm
cargo build --release
./target/release/tickerterm
```

## Usage

```
tickerterm [--all] [--tradfi | --crypto] [--rerank SECS] [--url wss://...]
```

| Flag | Effect |
| --- | --- |
| `--all` | Start with every symbol, including COIN-M, USDC and dated contracts. The default is USDT perpetuals only. |
| `--tradfi` | Start on the TradFi-only page (stocks, commodities, FX). `--crypto` is the opposite. |
| `--rerank SECS` | Seconds between re-rankings. Default 60; `0` re-ranks only when you press `r`. |
| `--url` | Use a different stream URL. |

| Key | Action |
| --- | --- |
| `s` / `S` | Cycle sort forwards / backwards: volume, 24h% descending and ascending, annualised funding descending and ascending |
| `r` | Re-rank now |
| `m` | Toggle USDT perpetuals / all symbols |
| `t` | Cycle TradFi only / crypto only / both |
| `1`–`5`, `a` | Detail level, or auto |
| `p` | 24h% history plot (`x` absolute / change since start, `z` p1–p99 / full range) |
| `d` | Distribution histograms |
| `/` | Filter by symbol (Esc clears) |
| arrows, `j`/`k` | Page |
| `b`, Esc | Back to the board |
| `?` | Toggle help / liquidation tape |
| `q` | Quit |

## What it shows

- **Grid:** as many columns of symbols as the terminal fits. USDT perpetuals are shown without the `USDT` suffix. Auto detail never drops symbols for detail, but spends leftover width on richer cells.
- **Detail levels (`1`–`5`):** mark price and 24h %, then annualised funding rate and time to the next funding, then quote volume, then trade count and position in the 24h low–high range, then high, low, VWAP, last price and last quantity.
- **Colour:** 24h % and funding rate are green or red by sign. Mark price is drawn plain, since it moves every second for every symbol. TradFi contracts are cyan.
- **Header:** connection state, advancers and decliners, total 24h volume, top movers, measured latency and message rates.
- **TradFi sessions:** whether each underlying market (US equities, commodities, FX, Korea, Hong Kong, China) is open, in pre/post-market, overnight or closed, with the time left in that state.
- **Footer:** recent liquidations.

Row order is frozen between re-rankings so values update in place and rows stay readable. During the first 20 seconds after connecting the order still shifts, because the ticker stream only sends symbols that have changed.

## Visualisations

Both views apply the current filters (`m`, `t`, `/`), so you can narrow them to TradFi, crypto, or a handful of symbols.

- **24h% history (`p`):** a line for each of the top 20 symbols by volume in the current filter, showing its 24h % change from the moment the program started. Each line has its own colour and a label at its right-hand end with the symbol and current 24h %. The set of 20 is re-picked once a minute, or when you press a key, so lines do not swap as volumes jostle. `x` switches to change since the start of the window, which puts every line on a common origin and makes co-movement easier to see. History is recorded for every symbol in the background, so the plot is complete for whichever filter you open it on.
- **Distributions (`d`):** histograms of 24h % change, 24h quote volume (log scale) and annualised funding rate, each with summary statistics.

History is kept in a fixed 4 MB buffer: when it fills, every other sample is dropped and the sampling interval doubles, so memory never grows and the plot always covers the whole run.

Annualising the funding rate needs each contract's funding interval (1, 4 or 8 hours), which the streams do not carry. It is fetched from `GET /fapi/v1/fundingInfo` on a background thread, refreshed hourly, and cached in `~/.cache/tickerterm/funding_info.json` (or `$XDG_CACHE_HOME`) so a restart has it immediately. Until the first fetch succeeds, 8 hours is assumed.

## Latency

Binance pushes these streams once per second, which is the floor for any client. tickerterm keeps its own share small:

- one thread and one socket, with no async runtime or TUI framework;
- a redraw the moment a message arrives, with no frame timer;
- only changed cells are redrawn, and each frame goes out in a single `write`.

Background work stays off that path: history is sampled after each frame is on screen, the funding fetch runs on its own thread, and the visualisations are recomputed once per second rather than per message.

The header shows both the exchange-to-receive delay and the receive-to-screen time, which is typically a few hundred microseconds.

## Data sources

One combined connection to `wss://fstream.binance.com/market/stream` carrying `!ticker@arr`, `!markPrice@arr@1s`, `!forceOrder@arr` and `tradingSession`. TradFi contracts are identified from the mark-price stream, where Binance sends them as a separate batch. It reconnects automatically with backoff if the connection drops or stalls.

This project is not affiliated with Binance. Nothing here is financial advice.

## License

MIT
