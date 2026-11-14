# tickerterm

A low-latency terminal board for Binance USD-M futures. It shows every contract's 24h ticker in a dense grid, straight from the public market-wide WebSocket streams. No API key or account is needed; it only reads public market data.

```
 TICKERTERM | LIVE | 662 sym (141 tradfi) +74 -588 =0 | 24h vol 45.6B | USDT-PERP | sort VOL v (3s ago) | lvl 2 | pg 1/37
 lat E->rx 72ms (min 58 avg 75 max 221) | rx->screen 51us (parse 9 draw 40 write 3, max 870) | 6.8 msg/s 234 KB/s 132 upd/s | up 00:00:28 rc 0 err 0 7
 UP EVAA +68.1% AKE +23.1% | DN KAVA -52.5% AGT -32.7%
 TRADFI US CLOSED 32h38m | CMDTY CLOSED 30h38m | FX CLOSED 29h38m | KR CLOSED 31h38m | HK CLOSED 33h08m | CN CLOSED 33h08m |
SYMBOL           MARK    24h% FUND/yr%  NEXT  SYMBOL           MARK    24h% FUND/yr%  NEXT  SYMBOL           MARK    24h% FUND/yr%  NEXT
*BTC         65474.10   -4.38    +4.37 7h38m  AKE         0.0133850  +23.07   +10.95 3h38m  BNB           587.489   -6.73    +0.00 7h38m
*ETH          1830.58   -4.71    -0.69 7h38m  NVDA        159.80000   +0.34   -82.74 7h38m  DOGE         0.060230   -6.94    -1.44 7h38m
*SOL          72.9800   -8.10    -3.45 7h38m  SPCX        135.16000   -1.57    +0.00 7h38m  SHELL       0.0271718  +20.66   -71.47 3h38m
*XAU          3842.09   -1.11  -111.14 3h38m  XRP            0.9614   -4.99    +2.52 7h38m  NEAR           3.1230  -10.46   +10.95 7h38m
SOXL         55.07000   +2.40   -69.90 7h38m  HYPE         66.53000   -8.36    +1.64 3h38m  1000PEPE    0.0039318  -11.49   +10.95 7h38m
ZEC            868.01   -7.80    +7.83 7h38m  SNDK        849.77000   -0.80   -80.71 7h38m  SOXS         53.66000   -0.76   +55.49 7h38m
 LIQ 16:21:28 SKHYNIX SELL  26.0 @865.98000 | 16:21:25 DODOX SELL  7.74 @0.0309740 | 16:21:24 DODOX SELL   700 @0.0309924 |
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
| `f` | Pin or unpin: type one or more names (`btc` or `btc,eth,sol`) and press Enter |
| `F` | Show pinned symbols only |
| `/` | Filter by symbol (Esc clears) |
| arrows, `j`/`k` | Page |
| `b`, Esc | Back to the board |
| `?` | Toggle help / liquidation tape |
| `q` | Quit |

## What it shows

- **Grid:** as many columns of symbols as the terminal fits. USDT perpetuals are shown without the `USDT` suffix. Auto detail never drops symbols for detail, but spends leftover width on richer cells.
- **Detail levels (`1`–`5`):** mark price and 24h %, then annualised funding rate and time to the next funding, then quote volume, then trade count and position in the 24h low–high range, then high, low, VWAP, last price and last quantity.
- **Colour:** 24h % and funding rate are green or red by sign. Mark price is drawn plain, since it moves every second for every symbol. TradFi contracts are cyan.
- **Header:** any active prompt or filter, connection state, advancers and decliners, total 24h volume, top movers, measured latency and message rates.
- **TradFi sessions:** whether each underlying market (US equities, commodities, FX, Korea, Hong Kong, China) is open, in pre/post-market, overnight or closed, with the time left in that state.
- **Footer:** recent liquidations.

Row order is frozen between re-rankings so values update in place and rows stay readable. During the first 20 seconds after connecting the order still shifts, because the ticker stream only sends symbols that have changed.

## Favourites

Press `f`, type a symbol and press Enter to pin it; do the same again to unpin. Names are accepted as shown on screen (`BTC`) or in full (`BTCUSDT`), and several can be given at once, separated by commas.

- Pinned symbols are marked with a yellow `*` and always lead the first page, whatever the sort. The sort still orders them among themselves.
- `F` narrows every view to your pinned symbols, including the plot and the histograms.
- In the history plot, pinned symbols are always drawn, and the remaining lines are filled by volume.
- The list is saved to `~/.config/tickerterm/favorites.txt` (or `$XDG_CONFIG_HOME`), one full symbol per line, and is loaded on the next start. It is a plain text file, so you can edit it by hand.

## Visualisations

Both views apply the current filters (`F`, `m`, `t`, `/`), so you can narrow them to TradFi, crypto, or a handful of symbols.

- **24h% history (`p`):** a line for each of up to 20 symbols in the current filter — your pinned symbols first, then the highest by volume — showing its 24h % change from the moment the program started. Each line has its own colour and a label at its right-hand end with the symbol and current 24h %. The set of 20 is re-picked once a minute, or when you press a key, so lines do not swap as volumes jostle. `x` switches to change since the start of the window, which puts every line on a common origin and makes co-movement easier to see. History is recorded for every symbol in the background, so the plot is complete for whichever filter you open it on.
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
