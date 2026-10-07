# Backpack — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/backpack.rs` (REST, ~3445 lines)
- `ccxt-base-4.5.85/src/exchanges/backpack_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/backpack.rs` (WebSocket, ~2010 lines)
- `ccxt-4.5.85/src/exchanges/backpack_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/backpack_typed.rs`

Backpack is already a compiled provider in the pinned release: the `backpack`
cargo feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Backpack` /
`ccxt_pro::Backpack` are exported. Public id in CCXT is `backpack`.

Live probes below were taken against `https://api.backpack.exchange` on
2026-10-06. Counts are one observed snapshot, not guarantees.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works via `public_get_api_v1_markets` (`GET /api/v1/markets`). One call returns spot **and** perp. `active` is derived from `orderBookState` (see §3.10). `has.fetchCurrencies = true`, so `load_markets` also fetches currencies (`GET /api/v1/assets`). |
| `fetchTrades` (REST) | `fetch_trades` | Works via `GET /api/v1/trades` (recent) or `GET /api/v1/trades/history` when `params.offset` is set. `limit` capped at 1000. `since`/`until` are filtered client-side; the native request carries no time window. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `GET /api/v1/depth`. The `limit` argument is ignored; only `params` is forwarded. No `limit` returns the full book (see §3.2). |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `GET /api/v1/klines`. Native `startTime` is required and always set by stock. No `limit` is sent upstream (see §3.3). |
| OHLCV mark/index | `params.price` → `priceType` | `has.fetchMarkOHLCV` and `has.fetchIndexOHLCV` are `true`; `has.fetchPremiumIndexOHLCV` is `false`. |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works. `messageHash` is `trades:<symbol>`. |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works. Subscribes `depth.<nativeId>` and seeds from REST `fetch_order_book`. |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works. `messageHash` is `candles:<symbol>:<interval>`. |
| `/v1/ws` tickers | Pro `watch_ticker(s)` | Works per explicit symbol list; no aggregate `null` form (see §2.6). |
| `fetchFundingRate` | `fetch_funding_rate` | Singular only, via `GET /api/v1/markPrices`. `has.fetchFundingRate = true`. |
| `fetchOpenInterest` | `fetch_open_interest` | Singular only, via `GET /api/v1/openInterest`. `has.fetchOpenInterest = true`. |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Requires a `symbol`. `has.fetchFundingRateHistory = true`. Not currently exposed by any Ferris endpoint. |
| `fetchCurrencies` | `fetch_currencies` | Exists (`has.fetchCurrencies = true`); called during market load. |

All public market-data calls are unauthenticated.

### Timeframes

Stock map (both REST and Pro) exposes: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h,
8h, 12h, 1d, 3d, 1w, 1M`. The map keys are mis-shaped for two entries (see
§3.4).

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No bulk `fetchFundingRates`

`has.fetchFundingRates = false`. Backpack implements no plural
`fetch_funding_rates`. Funding is only available through the singular
`fetch_funding_rate(symbol)` or `fetch_funding_rate_history(symbol)`.

The native endpoint `GET /api/v1/markPrices` is bulk-capable (returns all perps
when no `symbol` is sent), but stock exposes no unified method that calls it
without a symbol.

### 2.2 No `fetchFundingIntervals`

No such method on Backpack; the generic base method panics `NotSupported`.
Funding interval exists only as market metadata `fundingInterval` (ms).

### 2.3 No `fetchMarkPrices` / `fetchIndexPrice`

No such methods. The generic base `fetch_mark_prices` panics `NotSupported`.
Mark and index prices are only available as `markPrice` / `indexPrice` on the
singular `fetch_funding_rate` payload, or `info.markPrice` / `info.indexPrice`.

### 2.4 No `fetchOpenInterests` (bulk open interest)

No plural open-interest method; the generic `fetch_open_interests` panics
`NotSupported`. Only the singular `fetch_open_interest(symbol)` exists.

The native `GET /api/v1/openInterest` is bulk-capable (returns all perps when no
`symbol` is sent), but stock exposes no unified method that calls it without a
symbol.

### 2.5 No Pro funding / open-interest / mark-price watchers

Backpack's Pro module implements `watchTicker(s)`, `watchBidsAsks`,
`watchOrderBook(s)`, `watchTrades(s)`, `watchOHLCV(s)`, `watchOrders`,
`watchPositions`. There is no `watchFundingRate`, `watchFundingRates`,
`watchOpenInterest`, or mark/index watcher. The WS ticker payload carries only
last/high/low/open, base volume, and quote volume (see §3.1).

### 2.6 No aggregate `watchTickers(null)`

Pro `watch_tickers` calls `market_symbols(symbols, null, false)`. With
`symbols = null` and `allowEmpty = false`, the base `market_symbols` panics
`ArgumentsRequired`. An aggregate all-markets ticker subscription is therefore
not available; an explicit non-empty symbol list is required.

### 2.7 `fetchOpenInterestHistory` is declared but panics

`has.fetchOpenInterestHistory = true`, but Backpack implements no
`fetch_open_interest_history`; the generic base method panics `NotSupported`.
The typed wrapper `Backpack::fetch_open_interest_history` delegates to that
generic, so the declared capability is not reachable.

### 2.8 No premium-index OHLCV

`has.fetchPremiumIndexOHLCV = false`. Only mark and index candle sources exist.

### 2.9 Product coverage is spot and perpetual only

`has.spot`, `has.swap` are `true`; `has.future`, `has.option` are `false`.
`parse_market` maps `marketType` `SPOT` → `spot` and `PERP` → `swap`; any other
`marketType` parses to a null type.

---

## 3. Findings that may hinder integration

### 3.1 Bulk tickers omit funding, mark, index, and open interest

`fetch_tickers` (bulk, `GET /api/v1/tickers`) parses only `firstPrice`, `high`,
`low`, `lastPrice`, `priceChange`, `priceChangePercent`, `volume` (base), and
`quoteVolume`. It carries no funding rate, mark price, index price, or open
interest.

Those three fields come from separate native endpoints (`/api/v1/markPrices`,
`/api/v1/openInterest`) that are bulk-capable but have no unified bulk method
(§2.1, §2.4). A poll therefore needs the tickers call plus two raw calls, or one
singular call per market.

### 3.2 REST order book returns the full book by default

Stock `fetch_order_book` reads `limit` from its arguments but never puts it in
the request; it only extends the request with `params`. With no `limit`, live
`GET /api/v1/depth?symbol=BTC_USDC_PERP` returned ~2,561 bid / ~2,141 ask levels.

When supplied, `limit` must be one of the fixed `DepthLimit` enum
`5, 10, 20, 50, 100, 500, 1000`. Values `1, 2, 3, 4, 6, 25, 200, 2000, 5000`
return HTTP 400 (`failed to parse "DepthLimit"`).

The Pro `watch_order_book_for_symbols` also ignores its `limit` argument and
always subscribes `depth.<nativeId>`, seeding the owned book from a REST
`fetch_order_book` (full depth). The retained WS book therefore carries full
depth regardless of any display truncation.

### 3.3 REST OHLCV has a 2000-candle cap and no pagination

Stock `fetch_ohlcv` always sets `startTime` (seconds): to `since` when present,
otherwise to `now - limit*interval`. It sets `endTime` only when `until` is
present, otherwise the native default end (now) applies. It never sends `limit`
to the API; `limit` is used only to compute the window and to truncate
client-side via `parse_ohlc_vs`.

The native `klines` endpoint rejects a window longer than **2000 intervals**
(`INVALID_CLIENT_REQUEST: Time range between startTime and endTime is too
long`). Verified: 1m over exactly 2000 minutes works, 2001 minutes fails; 5m
over 40h (480 candles) works. There is no pagination in stock; a `since` older
than 2000 intervals for the requested timeframe produces an API error rather
than a truncated or paged result.

The endpoint also requires `startTime`; an `endTime`-only request fails
(`failed to parse parameter startTime`).

### 3.4 Timeframe map keys are mis-shaped for 15m and 30m

The stock `timeframes` map uses keys `"15" -> "15m"` and `"30" -> "30m"` instead
of keys `"15m"` / `"30m"`. Consequences:

- `has_timeframe("15m")` is `false`; the alias resolver maps `"15m" -> "15"`, so
  the request works but the retained/normalized timeframe string is `"15"`.
- The Pro OHLCV `messageHash` is built from the mapped interval value
  (`candles:<symbol>:15m`), while the retained timeframe is `"15"`. Any Ferris
  message-hash reconstruction keyed on the retained timeframe would disagree.
- `parse_timeframe` is the only place that resolves the key back to minutes.

### 3.5 Open interest is base-denominated but labeled as a value

Native `openInterest` is a base-asset amount (verified: `BTC_USDC_PERP`
`openInterest = 551.98`, i.e. ~552 BTC, not ~552 USDC). Stock
`parse_open_interest` sets `openInterestValue = openInterest` and
`openInterestAmount = null`, i.e. it labels a base amount as a quote value.

The raw payload also carries a `timestamp` (ms). Ferris's open-interest value
type distinguishes `openInterestAmount` from `openInterestValue`; the stock
unified field is on the wrong side.

### 3.6 Coverage gaps and bulk-only endpoints

One observed snapshot:

- markets: 190 total (103 `PERP`, 87 `SPOT`); `orderBookState` Open 143,
  Closed 46, PostOnly 1.
- tickers: 143 rows (fewer than 190 markets).
- `markPrices`: 92 rows (fewer than 103 perps).
- `openInterest`: 98 rows (fewer than 103 perps).

`GET /api/v1/tickers` ignores a `symbol` query parameter and always returns the
full list; stock filters client-side. `markPrices` and `openInterest` return an
array and accept an optional `symbol`; without it they return all perps.

Inactive/closed markets or markets with no current data have no row in these
responses, so per-market field resolution must tolerate absent rows.

### 3.7 Trades history is offset-paginated, not time-windowed

`fetch_trades` uses `GET /api/v1/trades` (recent) and switches to
`GET /api/v1/trades/history` only when `params.offset` is present. Neither
endpoint accepts a time window; `since`/`until` are applied client-side by
`parse_trades`/`filter_by_symbol_since_limit`. Requesting an old `since` does not
create historical coverage.

For public trades, `side` is derived from `isBuyerMaker` and `takerOrMaker` is
set to `"taker"`. `timestamp` is already ms.

### 3.8 Funding interval, unit, and kind

- Market metadata `fundingInterval` is `3600000` ms (1h) for all 103 observed
  perps. (A stale comment in `parse_market` shows `28800000`; live data is
  hourly.)
- `parse_funding_rate` hardcodes `interval: "1h"` and sets
  `nextFundingTimestamp` from `nextFundingTimestamp`.
- The stock parser does not declare whether `fundingRate` is a decimal fraction
  or a percent. Sample values are small decimals (`0.0000125`, `0.0000633`).
- `LastSettledFunding` is only reachable per-market via
  `fetch_funding_rate_history(symbol)`; there is no bulk settled-funding source.

### 3.9 Symbol and identity shape

- Native ids use underscores and a product suffix: `BTC_USDC` (spot),
  `BTC_USDC_PERP` (perp). Some bases contain dots: `CRCL.US_USDC_PERP`,
  `QQQ.US_USDC_PERP`.
- Unified CCXT symbols are `BTC/USDC` (spot) and `BTC/USDC:USDC` (perp).
- Perps quote and settle in **USDC**, not USDT.
- `settle` is set from the quote id. `contractSize` is `1`.
  `precision.price` is `tickSize`; `precision.amount` is `stepSize`;
  `limits.amount.min` is `minQuantity`; `limits.amount.max` is `maxQuantity`;
  `limits.price.min`/`max` are `minPrice`/`maxPrice`.
- Native ids are unique across products, so `category` is not needed for
  uniqueness. A base-only alias (e.g. `BTC`) would be ambiguous between the spot
  and perp rows.

### 3.10 Market `active` derives only from `orderBookState`

Stock sets `active = (orderBookState == "Open")`. The native response also
carries `visible` and `marketType`, which stock does not read for activity. A
`PostOnly` market (1 observed) and `Closed` markets (46 observed) are therefore
reported inactive. `includeInactive` cannot recover a `PostOnly` market as
active because stock metadata does not distinguish it.

### 3.11 WS message hashes and unsubscribe paths

Public WS `messageHash` shapes (from the handlers):

- ticker: `ticker:<symbol>`
- bid/ask: `bidask:<symbol>`
- candles: `candles:<symbol>:<interval>` (symbol first, then native interval)
- order book: `orderbook:<symbol>`
- trades: `trades:<symbol>` (plural)

Pro implements `un_watch_*` for tickers, bids/asks, ohlcv, trades, order book,
orders, and positions. `handle_unsubscriptions` sends an `UNSUBSCRIBE` frame and
then clears the local cache. A single shared public URL is used (see §3.12).

### 3.12 One shared public WebSocket URL for all channels

Backpack's public WS is a single URL: `wss://ws.backpack.exchange`. Tickers,
books, candles, and trades all share it. The base `describe` declares only
`urls.api.public`/`private`; Pro adds `urls.api.ws.public`/`private`, both set
to the same host. Keepalive is `streaming.ping = "ping"`, `keepAlive = 119000`.

### 3.13 WS timestamps are microseconds

WS ticker, trade, and book events carry microsecond timestamps (`E`, `T`) that
stock divides by 1000 to ms. `handle_order_book` uses `T` (µs) for the book
timestamp and `u` for the nonce. REST `fetch_order_book` divides its
`timestamp` (µs) by 1000 and takes `lastUpdateId` as the nonce.

### 3.14 Rate limit

`rateLimit: 50` (50 ms per request) with `enableRateLimit`. A per-market
statistics fallback would be one request per instrument; the bulk ticker +
`markPrices` + `openInterest` shape is three requests per poll.

### 3.15 URLs and metadata load shape

- REST public/private: `https://api.backpack.exchange` (single host).
- WS public/private: `wss://ws.backpack.exchange`.
- `load_markets` triggers `fetch_currencies` (`GET /api/v1/assets`) in addition
  to `GET /api/v1/markets`, so metadata load is two public requests.
