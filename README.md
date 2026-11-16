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
 LIQ SHORTS 1m   394 5m 15.4K 1h 18.6K  x0.1   -12m▕          ▁      ▁ ▁       ▁▁▁▁▁▁▁▁▁▁▁ ▁▁▁ ▁▁▁▁    ▏ 5m  EVAA 6.96K  IDOL 4.84K  NEAR 1.56K
 LIQ LONGS  1m 8.36K 5m  348K 1h  351K  x0.1 █1.00M▕            ▇▇▇▇▇    ▇▇▇▇                ▇▇▃ ▇ ▇▇▇ ▏ 5m  BTC 240K  ZEC 92.5K  EVAA 7.96K
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
tickerterm [--all] [--tradfi | --crypto | --commodities] [--hide-pins] [--rerank SECS] [--url wss://...]
```

| Flag | Effect |
| --- | --- |
| `--all` | Start with every symbol, including COIN-M, USDC and dated contracts. The default is USDT perpetuals only. |
| `--hide-pins` | Start with favourites hidden. |
| `--tradfi` | Start on the TradFi-only page (stocks, commodities, FX). `--crypto` is the opposite; `--commodities` starts on the commodities page. |
| `--rerank SECS` | Seconds between re-rankings. Default 60; `0` re-ranks only when you press `r`. |
| `--url` | Use a different stream URL. |

| Key | Action |
| --- | --- |
| `s` / `S` | Cycle sort forwards / backwards: volume, 24h% descending and ascending, annualised funding descending and ascending |
| `r` | Re-rank now |
| `m` | Toggle USDT perpetuals / all symbols |
| `t` | Cycle TradFi only / crypto only / commodities only / all |
| `1`–`5`, `a` | Detail level, or auto |
| `p` | 24h% history plot (`x` absolute / change since start, `z` p1–p99 / full range) |
| `d` | Distribution histograms |
| `A` | Funding arbitrage against Hyperliquid (`s` cycles its sort, arrows `j` `k` page through it) |
| `B` | BTC-quoted spot markets (`s` cycles its sort, arrows `j` `k` page through it) |
| `f` | Pin or unpin: type one or more names (`btc` or `btc,eth,sol`) and press Enter |
| `F` | Cycle favourites: pinned first, pinned only, hidden |
| `/` | Filter by symbol (Esc clears) |
| arrows, `j`/`k` | Page |
| `b`, Esc | Back to the board |
| `l` | Liquidation footer: 5-minute totals per symbol / latest prints with price |
| `?` | Toggle help / liquidation flow |
| `q` | Quit |

## What it shows

- **Grid:** as many columns of symbols as the terminal fits. USDT perpetuals are shown without the `USDT` suffix. Auto detail never drops symbols for detail, but spends leftover width on richer cells.
- **Detail levels (`1`–`5`):** mark price, 24h % and 24h VWAP, then annualised funding rate and time to the next funding, then quote volume and open interest, then trade count and position in the 24h low–high range, then high, low, last price and last quantity.
- **Colour:** 24h % and funding rate are green or red by sign. Mark price is drawn plain, since it moves every second for every symbol. TradFi contracts are cyan, commodities (gold, silver, platinum, palladium, copper, crude, Brent, natural gas) orange.
- **Volume surge glow:** the background behind a symbol's name turns yellow when its traded volume surges. Brightness is continuous, not on/off: it compares the volume rate over the last 10–20 seconds with the average rate over the rest of the last 15 minutes, starts as dark amber at 2x and reaches bright yellow at 16x (log scale, 24-bit colour where the terminal supports it, a five-step 256-colour ramp otherwise). It fades as the burst ends, and a sustained higher rate fades too as the 15-minute average catches up. Nothing glows for the first two minutes after start, while the baseline builds. The stream has no per-interval volume, so this is derived from changes in the rolling 24h quote volume, corrected for what is expected to drop out of the 24h window; it is an estimate, and bursts under $5K are ignored.
- **Header:** any active prompt or filter, connection state, advancers and decliners, total 24h volume, top movers, measured latency and message rates.
- **VWAP:** the 24h volume-weighted average price, always shown. Its background is green when VWAP is below the mark price and red when it is above, in ten shades by the percentage gap: no colour within 0.25%, full colour at 5%.
- **Open interest (`OI`, detail level 3 and up):** each symbol's open interest value in USD. Its background runs from red to green by how far it sits from its own one-hour average: green when positions are being added, red when they are being closed, full colour at 5% either way. The header shows the total for the current tab (all, TradFi, crypto, commodities, or whatever a filter leaves), with `(fetched/total)` while some symbols are still to be fetched.
- **Spot pairs:** two lines pinned above the grid, one for USDC/USDT and one for ETH/BTC on Binance spot, each showing the trailing 24-hour window: last price, change in percent and in price, open, high, low, volume-weighted average price, volume in both assets and the number of trades. They come from the spot all-market rolling window ticker stream (`!ticker_1d@arr`) on its own connection.
- **BTC markets (`B`):** a table of every BTC-quoted pair on Binance spot from the same stream: last, change, high, low and VWAP in BTC, volume in BTC and in dollars (at spot BTC/USDT), and trades, all over the trailing 24 hours. `s` cycles the sort (volume, change down, change up, name), the arrows and `j`/`k` page, `/` filters. The stream only mentions a pair when it trades, so the table is filled at start-up from `GET /api/v3/ticker/24hr`; pairs with no trade in the window are left out.
- **TradFi sessions:** whether each underlying market (US equities, commodities, FX, Korea, Hong Kong, China) is open, in pre/post-market, overnight or closed, with the time left in that state.
- **Footer (liquidation flow):** two lines, shorts liquidated above (green) and longs liquidated below (red), in USD notional.
  - Rolling totals over 1 minute, 5 minutes and 1 hour.
  - A surge ratio: the last minute against that side's own average minute over the rest of the hour (`x1.0` is normal). When a side takes $1M or more in a minute at 5x its norm, its label is highlighted.
  - A mirrored timeline, one column per 10 seconds, newest on the right. Shorts grow up and longs hang down from a shared axis on one square-root scale; the label on the lower line is the notional of a full-height bar. A cascade shows as a block of tall bars on one side, and its end as the bars thinning out.
  - Columns holding a single liquidation of $1M or more are drawn in a deeper colour, as are those symbols and prints on the right.
  - On the right, the symbols hit hardest over the last 5 minutes; `l` switches this to the latest prints with their prices.

  Binance pushes at most one liquidation per symbol per second on this stream, so totals are a lower bound during heavy cascades. Recording starts when the program starts.

Row order is frozen between re-rankings so values update in place and rows stay readable. During the first 20 seconds after connecting the order still shifts, because the ticker stream only sends symbols that have changed.

## Favourites

Press `f`, type a symbol and press Enter to pin it; do the same again to unpin. Names are accepted as shown on screen (`BTC`) or in full (`BTCUSDT`), and several can be given at once, separated by commas.

- Pinned symbols are marked with a yellow `*` and always lead the first page, whatever the sort. The sort still orders them among themselves.
- `F` cycles three states: pinned first (the default), pinned only, and hidden. Pinned only narrows every view to your pinned symbols, including the plot and the histograms.
- Hidden turns the feature off without losing the list: pinned symbols go back to their normal place in the sort, with no marker. `--hide-pins` starts in this state.
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

## Funding arbitrage (`A`)

A table of every symbol quoted on both Binance and Hyperliquid, ranked by the gap between their annualised funding rates.

- **BIN/yr%, HL/yr%:** each venue's funding rate per year. Binance's is the rate for its 1, 4 or 8 hour interval; Hyperliquid charges hourly, so its rate is multiplied by 8760.
- **SPREAD:** Binance minus Hyperliquid, in percentage points a year, tinted green or red by size (no colour under 2.5, full colour at 50).
- **COLLECT BY:** the pair of positions that earns the spread: short where funding is higher, long where it is lower (`S BIN  L HL` or the reverse).
- **BASIS%:** Hyperliquid's mark price against Binance's. Entering when the basis is against you costs part of the spread.
- **BIN OI, HL OI, BIN VOL, HL VOL:** open interest and 24h volume on each side, in USD, to judge whether the pair can be traded in size.

`s` cycles the sort: largest gap either way, most positive, most negative. The board's filters (`m`, `t`, `/`, `F`) apply. Hyperliquid is polled every 10 seconds from its public `info` endpoint, both its own crypto markets and the stocks and commodities on trade.xyz. Names are matched to Binance's (`kPEPE` is `1000PEPE`, trade.xyz's `GOLD` is `XAU`), and a pair is dropped when the two marks differ by more than 20%, which means the ticker is not the same asset. Both rates are current estimates and move, and the spread ignores fees, slippage and margin costs.

## Open interest and commodities

Neither is on the WebSocket streams, so a background thread fetches them over REST and caches them in `~/.cache/tickerterm/`:

- **Commodities** come from `GET /fapi/v1/exchangeInfo` (`underlyingType` = `COMMODITY`), cached in `symbols.txt` and refreshed hourly.
- **Open interest** comes from `GET /futures/data/openInterestHist` (`sumOpenInterestValue`), which answers for one symbol per request and is limited to 1000 requests per 5 minutes per IP. With about 800 contracts, the poller walks them busiest first at 1.5 requests a second (45% of the limit; 2.5 a second for the first pass), so each value is refreshed roughly every 9 minutes. Each request asks for the last 13 five-minute points: the newest is the value shown and the 12 before it are its one-hour average. Values are cached in `open_interest.txt` with their fetch time, so a restart shows them at once and does not re-request anything fetched in the last 5 minutes. On a 429 or 418 the poller pauses for the time Binance asks.

## Data sources

One combined connection to `wss://fstream.binance.com/market/stream` carrying `!ticker@arr`, `!markPrice@arr@1s`, `!forceOrder@arr` and `tradingSession`. TradFi contracts are identified from the mark-price stream, where Binance sends them as a separate batch. It reconnects automatically with backoff if the connection drops or stalls. The spot lines and the BTC markets table use a second connection, to `wss://stream.binance.com:9443/ws/!ticker_1d@arr`, which reconnects on its own.

This project is not affiliated with Binance. Nothing here is financial advice.

## License

MIT
