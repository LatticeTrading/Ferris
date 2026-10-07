# Bitfinex — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/bitfinex.rs` (REST, ~5465 lines)
- `ccxt-base-4.5.85/src/exchanges/bitfinex_api.rs` (implicit endpoint wrappers, 128 `public_*` endpoints)
- `ccxt-pro-4.5.85/src/pro/bitfinex.rs` (WebSocket, ~1845 lines)
- `ccxt-4.5.85/src/exchanges/bitfinex_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/bitfinex_typed.rs`
- base helpers in `ccxt-base-4.5.85/src/exchange_generated.rs` (`parse_tickers`,
  `parse_funding_rates`, `parse_open_interests`, `safe_ticker`)

Bitfinex is already a compiled provider in the pinned release: the `bitfinex`
cargo feature exists on both `ccxt` and `ccxt-pro`, and `ccxt::Bitfinex` /
`ccxt_pro::pro::bitfinex::BitfinexCore` are exported. Public id in CCXT is
`bitfinex` (no alias such as Lighter's `lighter`/`lighterxyz`).

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works via one bulk `public_get_conf_config` call with labels `pub:info:pair,pub:info:pair:futures,pub:list:pair:securities,pub:list:pair:margin`. Spot and swap are always returned together. |
| `fetchTrades` (REST) | `fetch_trades` | Works via `public_get_trades_symbol_hist` (`/trades/{symbol}/hist`). Default 120, capped at 10000. `since` → `start` and flips sort ascending; `params.until` → `end`. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `public_get_candles_trade_timeframe_symbol_hist`. Default 100, capped at 10000. `since` → `start` (sort ascending); `params.until` → `end`. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `public_get_book_symbol_precision`. Default precision `R0`. `limit` → `len`. |
| `fetchTickers` (bulk REST) | `fetch_tickers` | Works via one bulk `public_get_tickers` call with `symbols=ALL`. Returns a map keyed by unified symbol (via `parse_tickers` → `index_by(..., "symbol")`). |
| Statistics funding/mark/index | `fetch_funding_rates` | Works via `public_get_status_deriv`. **Requires a non-null symbols argument** (panics on null). `parse_funding_rate` sets `markPrice`, `indexPrice`, `fundingRate`, `nextFundingRate`, `nextFundingTimestamp`. |
| Statistics open interest | `fetch_open_interests` | Works via `public_get_status_deriv` with `keys=ALL` when called with `null`. Singular `fetch_open_interest` exists. |
| `/v1/ws` trades | Pro `watch_trades` | Works. Subscribes `trades` channel. |
| `/v1/ws` order book | Pro `watch_order_book` | Works. Subscribes `book` channel with `prec`/`freq` from options. |
| `/v1/ws` candles | Pro `watch_ohlcv` | Works. Subscribes `candles` channel keyed by native interval. |
| `fetchCurrencies` | `fetch_currencies` | Exists (`has.fetchCurrencies = true`); called during market load. |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock; not currently exposed by any Ferris endpoint. |
| `fetchLiquidations` | `fetch_liquidations` | Exists in stock; not currently exposed by any Ferris endpoint. |

All public market-data calls are unauthenticated and use `urls.api.public`
(`https://api-pub.bitfinex.com`).

### Timeframes

`1m, 5m, 15m, 30m, 1h, 3h, 4h, 6h, 12h, 1d, 1w, 2w, 1M`.

Native aliases: `1d → 1D`, `1w → 7D`, `2w → 14D`, `1M → 1M`. There is no `3m`,
`2h`, `8h`, `3d`, `5d`, `3w`, or `4w`.

### Ticker shape (price only)

`parse_ticker` sets top-level `last`/`close` from `LAST_PRICE`, plus `high`,
`low`, `bid`, `ask`, `change`, `percentage`, and `baseVolume`. Unlike
Hyperliquid, ticker `last` is a real last price. The ticker carries **no**
funding, mark, index, or open-interest fields.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No mark / index / premium-index OHLCV

`has.fetchMarkOHLCV`, `has.fetchIndexOHLCV`, and `has.fetchPremiumIndexOHLCV`
are all `false`. Only trade candles exist. There is no candle `price` /
`candleType` selector on the venue.

### 2.2 No aggregate Pro `watchTickers`

`watchTickers` is `false` in the Pro `describe`. There is no `market_stats/all`
equivalent. The maintained Lighter-style `watch_tickers(null)` statistics feed
has no Bitfinex counterpart. Only the singular Pro `watch_ticker` exists.

### 2.3 No `fetchMarkPrices` / `fetchIndexPrices`

No such methods. Mark and index prices exist only as members of
`fetch_funding_rates` / `fetch_open_interests` rows (i.e. `/status/deriv`), or
via the singular Pro `watch_ticker`.

### 2.4 No `fetchFundingIntervals`

No such method. Funding interval is not returned by `parse_funding_rate`
(it sets `interval: null`), and there is no separate intervals endpoint.

### 2.5 No Pro `un_watch_order_book`

`has.unWatchOrderBook` is declared `true`, but `pro/bitfinex.rs` implements no
`un_watch_order_book` method. `un_watch_trades`, `un_watch_ohlcv`, and
`un_watch_ticker` do exist. There is a lower-level `un_subscribe(channel,
topic, symbol, params)` in the dispatch table that the public method would
normally call.

### 2.6 Product coverage is spot + linear swap only

`has.spot = true`, `has.swap = true`, `has.margin = true`, `has.future = false`,
`has.option = false`. There is no inverse product. Margin pairs are modeled as
spot (`type = "spot"`, `margin = true` on the market). Every swap is linear
(`linear = true`, `inverse = false`, `contractSize = 1`).

### 2.7 No bulk multi-symbol trades

`fetch_trades` is per-symbol only; there is no `fetch_trades_for_symbols`
equivalent.

### 2.8 No `fetchOrderBooks` (plural)

`has.fetchOrderBooks = false`.

---

## 3. Findings that may hinder integration

### 3.1 Precision mode is significant digits, and precision is hardcoded

`describe` sets `precisionMode = SIGNIFICANT_DIGITS`. Ferris's
`convert.rs::tick_size_from_precision` returns `None` for `SIGNIFICANT_DIGITS`,
so every Bitfinex catalog row would report `tickSize: null`.

The market `precision` block is also hardcoded in `fetch_markets`:
`precision.amount = 8` (decimal places) and `precision.price = 5` (significant
digits, not a tick), with `limits.price.min = 1e-8`. `price_to_precision` treats
the `5` as a significant-digit count.

### 3.2 Market `active` is always true

`fetch_markets` inserts `active = true` unconditionally. No delisted/inactive
flag is read from the config response. `includeInactive` cannot distinguish
inactive instruments from active ones through stock metadata.

### 3.3 Currencies load during market load

`has.fetchCurrencies = true`, so `load_markets` issues a second
`public_get_conf_config` request (with the currency label set) in addition to
the markets request. This is a second bulk public request per metadata load.
Bitfinex uses `parse_currencies_custom` / `parse_currency_custom`, not the base
`parse_currency`.

### 3.4 No product-scoped catalog acquisition

One bulk config call returns spot and swap together. There is no
spot-only/swap-only load path, so every `CatalogScope` would resolve to the same
combined acquisition.

### 3.5 Symbol and identity shape

- Native market id is prefixed `t`: spot `tBTCUSD`, swap `tBTCF0:USTF0`.
- Unified CCXT symbol is `BTC/USD` (spot) and `BTC/USD:USD` (swap). Swap symbol
  is built by appending `:settle` where settle = quote.
- `settle` is set from the quote id for swaps; spot `settle` is null.
- `contractSize` is `1` for swaps.
- `limits.amount.min`/`max` come from the config pair row (indices 3/4).
- `margin` is true for ids present in `pub:list:pair:margin`, but `type` remains
  `spot`.
- `created` is null; `expiry`/`strike`/`optionType` are null.

### 3.6 Alias resolution gaps

The native id carries the `t` prefix (`tBTCUSD`, `tBTCF0:USTF0`). A bare
`BTCUSD` or `BTCF0:USTF0` is not a native id. Swap markets do not receive a
base-only alias (`BTC`) from the venue's own metadata; only the display symbol,
CCXT symbol, and native `t...` id are available.

### 3.7 Ticker `info` is a raw array, not a keyed object

Bitfinex `/tickers` returns arrays. `parse_ticker` leaves `info` as that raw
array and places the usable values at the ticker top level (`last`, `high`,
`low`, `bid`, `ask`, `change`, `percentage`, `baseVolume`). Ferris's generic
statistics field readers read `ticker.info.<key>` (`lastPrice`, `markPrice`,
`baseVolume`, …), which do not exist on an array. Any Bitfinex statistics
extraction would need the top-level ticker members or raw array indices.

### 3.8 Ticker has no derivative statistics

Funding, mark, index, and open interest are not in the ticker payload. They
require `/status/deriv` (`fetch_funding_rates` / `fetch_open_interests`), whose
rows also leave `info` as a raw array.

### 3.9 `fetchFundingRates` panics on null symbols

`fetch_funding_rates` calls `arguments_required` and panics when `symbols` is
null. It cannot be invoked as a single all-market call. The underlying
`public_get_status_deriv` endpoint accepts `keys=ALL` (which
`fetch_open_interests(null)` uses), but the stock funding-rates wrapper requires
a symbols list.

### 3.10 Funding interval is null in the source

`parse_funding_rate` sets `interval: null` (there is no intervals method). It
does expose `nextFundingTimestamp` (native index 8, ms) and `timestamp` (index
1, ms), from which an interval is not declared.

### 3.11 Funding rate unit is not established by the parser

`parse_funding_rate` maps `fundingRate` from native index 12 and
`nextFundingRate` from index 9 as raw numbers. Nothing in the parser declares
whether these are decimal fractions or percent. Ferris publishes an explicit
`rateUnit` per venue, so the unit must be qualified against the venue before any
value is published.

### 3.12 Open interest is contract count

`parse_open_interest` sets `openInterestAmount` from the native contract count
and `openInterestValue: null`. There is no USD/base notional in the unified row.

### 3.13 24h volume is base-only

`parse_ticker` sets `baseVolume` from native `VOLUME` and `quoteVolume: null`.
`safe_ticker` does not derive quote volume (its vwap computation needs a
non-null quote volume). Perp volume's unit (base coin vs contracts) is not
declared by the parser.

### 3.14 Bulk tickers include non-market rows

The `symbols=ALL` response includes funding-currency rows (ids such as `fUSD`)
alongside trading pairs. These do not resolve to catalog markets and parse to a
null symbol.

### 3.15 REST book defaults to raw `R0`, WS book defaults to aggregated `P0`

`options.fetchOrderBook.precision` defaults to `R0` (raw individual orders).
For `R0`, rows are `[ORDER_ID, PRICE, AMOUNT]` and multiple rows can share one
price with no aggregation; `parse` drops the order id and yields repeated
`[price, amount]` pairs. The Pro `watch_order_book` path defaults
`options.watchOrderBook.prec` to `P0` (aggregated by price). REST bootstrap and
WS updates therefore have different level shapes unless the REST precision is
changed.

### 3.16 REST book has no nonce and receipt-time timestamp

`fetch_order_book` sets `nonce: null` and `timestamp = milliseconds()` at parse
time (CCXT receipt time, not exchange time).

### 3.17 REST book `len` is passed through uncapped

`fetch_order_book` sets `len = limit` whenever a limit is provided, with no
validation or ceiling. The venue's actual `len` ceiling is not enforced by
stock. Ferris's generic per-venue REST depth cap (1000 for non-special venues)
would be the only bound.

### 3.18 Book precision is not exposed as a parameter

The precision selector (`R0`, `P0`–`P3`) is only reachable through the
`options.fetchOrderBook.precision` option. There is no `fetch_order_book`
parameter for it, and Ferris's param validation does not accept a precision key.

### 3.19 WS `watch_order_book` depth must be `None`, `25`, or `100`

`watch_order_book` panics unless the limit argument is undefined, `25`, or
`100`. The default when undefined is 25 price points. Ferris's generic live book
default depth is 20 for non-special venues, which would panic stock. Allowed
Bitfinex WS depth is effectively `{25, 100}`.

### 3.20 WS book checksum is enabled by default

`options.watchOrderBook.checksum` defaults to `true`; `subscribe` sends a
`conf` frame with flags `131072` to enable checksums. `handle_checksum`
recomputes a crc32 over the top 25 levels and rejects the subscription on
mismatch.

### 3.21 WS message hashes differ from Ferris's generic hashes

Stock hashes are:
- trades: `trades:{marketId}`
- book: `book:{marketId}`
- candles: `candles:{interval}:{marketId}` (native interval, e.g. `1D`)

Ferris's generic `message_hash` produces `trade:{symbol}`,
`orderbook:{symbol}`, and (for non-Hyperliquid/Bybit/Binance/Aster/Extended)
has no Ohlcv arm at all — the existing match is not exhaustive once a new venue
is added.

### 3.22 WS unsubscribe hash format differs

`un_subscribe` builds `unsubscribe:{channel}:{marketId}` (and tracks
`{channel}:{marketId}` as the sub hash). Ferris's generic `unsubscribe_hash`
produces `unsubscribe:{hash}` over its own key format.

### 3.23 One shared public WS URL for all channels

Public WS is a single URL `wss://api-pub.bitfinex.com/ws/2` for trades, book,
and candles. There is no per-channel URL. Combined with §2.5 (no public
`un_watch_order_book`), retiring a book feed has no stock public method to
propagate.

### 3.24 REST and WS hosts differ from the private host

Public REST and WS are on `api-pub.bitfinex.com`; the `v1`/private REST host is
`api.bitfinex.com`. All 128 generated public endpoint wrappers are `public_*`
and resolve through `urls.api.public`.

### 3.25 Rate limit

`rateLimit` is `250` (ms per request) with `enableRateLimit`. Singular
`fetch_ticker` / `fetch_open_interest` are one request per instrument.

### 3.26 Trade `until` is not forwarded by the current Ferris path

Stock `fetch_trades` supports `params.until` → `end`. Ferris's REST trades path
only forwards an upper bound for Binance (`until`) and Aster (`endTime`); for
other venues it applies the bound as a local post-filter over the recent-trade
window. Bitfinex supports `end` upstream, but the current generic path would not
send it.

### 3.27 Since-only candles return the oldest `limit`, not a window ending now

`fetch_ohlcv` sets `sort = 1` when `since` is present and returns the earliest
`limit` candles after `since`. It does not return a window ending near the
current time. Ferris's generic handling truncates to `limit` on the oldest side
when `since` is set.

### 3.28 Candle volume is always present

`parse_ohlcv` maps native `[MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]` to
`[timestamp, open, high, low, close, volume]`. Bitfinex always supplies the
volume cell, so the null-volume path (Bybit mark/index, Extended) is not
expected here.

### 3.29 Market statistics fields not covered by the ticker

Because funding/mark/index/OI live only on `/status/deriv` (§3.8) and the bulk
ticker has no such members, an all-market statistics poll would need at least a
ticker call plus a `/status/deriv` call. `fetch_open_interests(null)` is a
single bulk call, but funding rates require a symbols list (§3.9). Last-settled
funding is not provided by the source.

### 3.30 Spot statistics applicability

Spot rows have no funding, mark, index, or open interest; only last price and
volume apply. There is no separate spot statistics endpoint.
