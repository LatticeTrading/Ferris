# OKX — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/okx.rs` (REST, ~13,100 lines)
- `ccxt-base-4.5.85/src/exchanges/okx_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/okx.rs` (WebSocket, ~3,730 lines)
- `ccxt-4.5.85/src/exchanges/okx_typed.rs`,
  `ccxt-pro-4.5.85/src/pro_typed/okx_typed.rs`

OKX is already a compiled provider in the pinned release: the `okx` cargo
feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Okx` /
`ccxt_pro::Okx` are exported. Enabling support is a matter of turning on the
feature and wiring the existing provider into Ferris's contract. Public id in
CCXT is `okx`. `has.spot`, `has.margin`, `has.swap`, `has.future`,
`has.option` are all `true`. Rate limit is `rateLimit: 110` (100 × 1.1) with
`enableRateLimit`.

Endpoint maxima, payload keys, and field semantics marked **observed** were
confirmed against the live public API during this review; everything else is
read from the pinned source.

---

## 1. What stock CCXT has a working path for

These map onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. One public `public/instruments` call per requested type (`SPOT`, `FUTURES`, `SWAP`, plus one per option underlying); the default type list includes all four (see §3.17). `active = state == "live"`. |
| `fetchTrades` (REST) | `fetch_trades` | Works via `market/trades`. `method: publicGetMarketHistoryTrades` selects history; `paginate: true` drives cursor pagination. Option markets use `publicGetPublicOptionTrades`. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `market/books`, `market/books-full`, or `market/books-rpi`. `method` and `rpi` params select the endpoint. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `market/candles` / `market/history-candles` / mark / index variants. `since`/`until` supported. |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works on the public WS host. `params.channel` `trades` (default) or `trades-all` (requires auth). |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works on the public WS host. Depth selected by `params.depth` / `limit`. |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works, but on the **business** WS host (see §3.1). |
| `/v1/ws` tickers | Pro `watch_tickers` / `watch_mark_prices` / `watch_funding_rates` | All present on the public WS host. |
| Statistics — bulk tickers | REST `fetch_tickers` | Works, one `instType` per call. Carries last, 24h high/low/open, bid/ask, volumes. |
| Statistics — mark price | REST `fetch_mark_prices` | Bulk per `instType`. |
| Statistics — open interest | REST `fetch_open_interests` | Bulk per `instType`. `fetch_open_interest` singular also exists. |
| Statistics — funding | REST `fetch_funding_rates` | True bulk (`instId=ANY`). `fetch_funding_rate` and `fetch_funding_interval` singular exist. |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock; not currently exposed by any Ferris endpoint. |
| `fetchIndexOHLCV` | `fetch_ohlcv` with `price: "index"` | `has.fetchIndexOHLCV` is `true`; there is no distinct `fetch_index_ohlcv` method in the core. Index candles substitute `instFamily` for `instId`. |
| `fetchMarkOHLCV` | `fetch_ohlcv` with `price: "mark"` | Same pattern. |
| `fetchCurrencies` | `fetch_currencies` | `has.fetchCurrencies` is `true`; auth-gated (see §3.16). |

### Timeframes

`1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M, 3M`.

`8h`, `3d`, `2w`, `3w`, `4w` are **not** present. (The funding-interval parser
separately recognises `16h`/`24h`, unrelated to candle timeframes.)

---

## 2. Upstream gaps (no clean stock method exists)

These are gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No bulk index price anywhere

There is no `fetchIndexPrice`/`fetchIndexPrices` method, and **no unified ticker
carries index price**. `idxPx` was **observed absent** from bulk `market/tickers`
for both `SPOT` and `SWAP`, and from `public/mark-price`. `fetch_tickers` and
`fetch_mark_prices` therefore cannot supply `indexPrice`.

The only index sources are:

- raw `market/index-tickers` (implicit `public_get_market_index_tickers`), which
  returns `instId`, `idxPx`, `high24h`, `low24h`, `open24h`, `sodUtc0`,
  `sodUtc8`, `ts`; and
- `fetch_ohlcv` with `price: "index"` (candles only).

`market/index-tickers` is index-id keyed (`BTC-USDT`), not perp-id keyed
(`BTC-USDT-SWAP`), and is scoped by a single `instId` or a `quoteCcy`
(`?quoteCcy=USDT` returns every USDT-quoted index in one call). The perp's `uly`
field equals the matching index `instId`.

### 2.2 No plural `fetchFundingIntervals`

`has.fetchFundingIntervals` is `false`; only singular `fetch_funding_interval`
exists (an alias of `fetch_funding_rate`). The bulk `public/funding-rate`
response does carry per-row `fundingTime`, `prevFundingTime`, `nextFundingTime`,
and `settFundingRate`, and `parse_funding_rate` derives the interval from
`nextFundingTime - fundingTime`.

### 2.3 Option bulk calls require an underlying selector

`fetch_tickers`, `fetch_mark_prices`, and `fetch_open_interests` panic with
`ArgumentsRequired` when the market type is `option` unless `uly` / `marketId` /
`instFamily` is supplied. There is no all-option bulk call.

### 2.4 `fetchMarkets` option coverage is limited to `defaultUnderlying`

`fetch_markets_by_type("option", …)` iterates `options.defaultUnderlying`, whose
default is `["BTC-USD", "ETH-USD"]` only. There is no enumeration of all option
underlyings.

### 2.5 `fetchOpenInterests` cannot select `OPTION`

In `fetch_open_interests` the type branch is
`if marketType == "future" { instType = "FUTURES" } else if instType == "option"
{ instType = "OPTION" }` — the second condition tests `instType`, which is still
`"SWAP"`, instead of `marketType`. `OPTION` is never selected.

### 2.6 Other absent plural methods

`has.fetchOrderBooks` is `false`; `has.fetchPremiumIndexOHLCV` is `false`.

### 2.7 No single cross-product bulk ticker call

`fetch_tickers` is `instType`-scoped (`SPOT` / `SWAP` / `FUTURES`), not one call
for every product. A margin scope (`linear`) spans both `SWAP` and `FUTURES`, so
it is at least two calls, and inverse is likewise spread across both.

---

## 3. Findings that may hinder integration

### 3.1 Two separate public WebSocket hosts

`get_url` routes any channel whose name contains `candle` (and `orders-algo`) to
the **business** host and everything else to the **public** host:

- `wss://ws.okx.com:8443/ws/v5/public` — books, trades, tickers, mark-price,
  funding-rate
- `wss://ws.okx.com:8443/ws/v5/business` — candles

A candle feed and a book/trade feed for the same market are therefore two
distinct upstream URLs, not one shared connection. `streaming.keepAlive` is
`18000` ms with a `ping` text frame.

### 3.2 WebSocket result-hash formats differ from the other venues

`ws_run` routes by result hash, and OKX mixes native and unified ids:

| Channel | Hash produced by stock | Example |
| --- | --- | --- |
| Trades | `{channel}:{unified}` | `trades:BTC/USDT:USDT` |
| Order book | `{depth}:{unified}` | `books:BTC/USDT:USDT` |
| Candles | `candle{nativeInterval}:{nativeInstId}` | `candle1H:BTC-USDT-SWAP` |
| Tickers | `{channel}::{unified}` | `tickers::BTC/USDT:USDT` |
| Mark price | `{channel}::{unified}` | `mark-price::BTC/USDT:USDT` |

Candles use the **native** instrument id and the **native** interval, unlike the
unified symbol used for trades/books. Unsubscribe hashes differ again from
subscribe hashes: order book drops the depth prefix
(`unsubscribe:orderbook:{unified}`), candles use
`unsubscribe:multi:candle{interval}:{unified}`, and tickers use
`unsubscribe:ticker:{unified}`.

### 3.3 WebSocket book depth 50 requires authentication

`watch_order_book_for_symbols` maps `limit` to a channel:

- `1` → `bbo-tbt` (1 level)
- `2..=5` → `books5` (5 levels)
- `50` → `books50-l2-tbt` (**requires credentials**)
- `400` → `books`
- otherwise → the `options.watchOrderBook.depth` value, default `books`

Requesting depth `50` triggers an `AuthenticationError` unless API credentials
are present. `books-l2-tbt` also requires credentials. The public channels are
`books` (max 400), `books5` (5), and `bbo-tbt` (1). `watch_order_book` returns
`orderbook.limit()`.

### 3.4 WebSocket order book enforces its own checksum and sequence continuity

`handle_order_book_message` verifies OKX's `checksum` and compares `prevSeqId`
against the held book `nonce`; a mismatch raises `InvalidNonce` /
`ChecksumError`. Ferris maps `ChecksumError` to `UpstreamData` and other
failures to reconnect. The book nonce is the message `seqId`.

### 3.5 REST order book has three endpoints with different shapes

- `market/books` — up to 400 levels, each level `[price, size, liquidatedOrders,
  orderCount]`.
- `market/books-full` — up to 5000 levels, each level `[price, size,
  orderCount]`.
- `market/books-rpi` — RPI book, capped at 400 levels, same envelope as `books`.

`fetch_order_book` selects `books-full` when `method` is
`publicGetMarketBooksFull` or the resolved limit is `> 400`, and `books-rpi`
when `rpi: true` (limit is clamped to 400 for RPI). `books-full` was
**observed public** with 5000 levels; `market/books?sz=401` was **observed** to
fail with `51000 Parameter sz error.` The two level shapes both place price at
index 0 and amount at index 1, so the extra 4th cell in `books` is ignored by a
price/amount reader.

### 3.6 REST trade limits, history method, and `since`

`fetch_trades` sends only `instId` and `limit` on the default
`publicGetMarketTrades` endpoint; `since` is applied client-side by
`parse_trades`, so an old `since` does not fetch history. Historical trades
require `params.method = "publicGetMarketHistoryTrades"` (or `paginate: true`,
which drives `fetch_paginated_call_cursor` with a `tradeId` cursor and `after`).
Option markets always use `publicGetPublicOptionTrades`.

Observed limits differ by endpoint: `market/trades` returned 500 rows for
`limit=500`; `market/history-trades` returned 300 rows for `limit=500`. There is
no cap applied inside stock CCXT.

### 3.7 OHLCV limits and REST-vs-WS bar naming

`options.fetchOHLCV` caps are `limit: 300`, `mark: 100`, `index: 100`.
`fetch_ohlcv` switches to `HistoryCandles` when `since` is older than
`(1440 - 1) * duration` and uses `before`/`after`. For a UTC timezone and a
duration `>= 6h`, the REST `bar` gets a lowercase `utc` suffix (`6H` → `6Hutc`,
`1D` → `1Dutc`, etc.). The Pro `watch_ohlcv` uses the raw `timeframes` value
with no suffix (`candle6H`), so REST and WS interval spellings diverge for
`6h` and above.

### 3.8 `instType`-scoped bulk vs Ferris's margin-based scope

Ferris's catalog scopes are `Spot` / `Linear` / `Inverse` / `Option`, but every
OKX bulk endpoint is keyed by `instType` (`SPOT` / `SWAP` / `FUTURES`). A single
"linear" scope contains both `SWAP` and `FUTURES` instruments, and "inverse"
likewise. One logical acquisition profile therefore maps to two `instType`
calls. `convert_to_instrument_type` maps `spot→SPOT`, `swap→SWAP`,
`future/futures→FUTURES`, `option→OPTION`.

### 3.9 Unified ticker volume is contracts, not base, for derivatives

`parse_ticker` sets `baseVolume = vol24h` and
`quoteVolume = volCcy24h` only when `spot`, else `quoteVolume = null`. For
derivatives `vol24h` is **contracts** and `volCcy24h` is the **base-coin**
amount. **Observed** for `BTC-USDT-SWAP`: `vol24h=7144925.09`,
`volCcy24h=71449.2509`, `ctVal=0.01` (so `vol24h × ctVal = volCcy24h`). The
unified `baseVolume` field is therefore contract count for derivatives, and the
true base volume is only in raw `info.volCcy24h`. There is no separate quote
volume for derivatives.

### 3.10 Bulk ticker carries no mark, index, or open interest

`market/tickers` rows contain `last`, `lastSz`, `askPx/askSz`, `bidPx/bidSz`,
`open24h`, `high24h`, `low24h`, `vol24h`, `volCcy24h`, `sodUtc0`, `sodUtc8`,
`ts`, `instType`, `instId` — and nothing else. A full statistics poll needs
`fetch_tickers` plus `fetch_funding_rates` plus `fetch_mark_prices` plus
`fetch_open_interests` (and, for index, the raw endpoint), each split by
`instType`.

### 3.11 Funding is limited to swaps and XPERP futures

`fetch_funding_rates` panics `BadRequest` for any symbol that is not `swap` and
not an XPERP future (`info.ruleType == "xperp"`). Plain dated futures do not pay
funding. The bulk `funding-rate` response includes `settFundingRate`,
`fundingTime`, `prevFundingTime`, `nextFundingTime`, `fundingRate`,
`nextFundingRate`, `method`, `min/maxFundingRate`, `premium`, and `ts`.
`parse_funding_rate` notes that `nextFundingRate` is "actually two funding rates
from now".

### 3.12 Open-interest unified field is contracts; base amount is separate

`parse_open_interest` maps `openInterestAmount = oi` (contracts),
`baseVolume = oiCcy` (base coin), and `openInterestValue = oiUsd`. The unified
`openInterestAmount` is contract count; the base-coin amount sits in a different
member.

### 3.13 Symbol and identity shape

- Native ids: spot `BTC-USDT`; linear swap `BTC-USDT-SWAP`; inverse swap
  `BTC-USD-SWAP`; linear future `BTC-USDT-YYMMDD`; inverse future
  `BTC-USD-YYMMDD`; option `BTC-USD-YYMMDD-STRIKE-C/P`.
- Unified symbols: spot `BTC/USDT`; linear perp `BTC/USDT:USDT`; inverse perp
  `BTC/USD:BTC`; futures append `-YYMMDD`; options append
  `-YYMMDD-STRIKE-C/P`.
- `settle` is `settleCcy`; `linear` is `quote == settle`, `inverse` is
  `base == settle`.
- `contractSize` is `ctVal` in `ctValCcy` (linear `BTC-USDT-SWAP`: `0.01 BTC`;
  inverse `BTC-USD-SWAP`: `100 USD`). `limits.amount.min` is `minSz`
  (contracts for derivatives, base for spot). `precision.price` is `tickSz`;
  `precision.amount` is `lotSz`. `created` is `listTime`.
- Display symbols collide across products (spot `BTC/USDT` vs linear perp
  `BTC/USDT` vs a future's `BTC/USDT`), and native spot/derivative ids differ.
  Ferris's existing ambiguity handling applies; `type`/`category`/`settle` or an
  opaque `marketId` is required to disambiguate.

### 3.14 `category` is not populated for OKX

`convert_market`'s `venue_category` currently only sets a category for Bybit,
Aster, and Extended. CCXT does not set a category field for OKX either.
`market_stats/projection.rs::market_id_type` requires `category.is_none()` for
every venue except Bybit, so any OKX identity that carried a category would fail
the projection's `marketId` validation unless that check is extended.

### 3.15 Preopen/inactive instruments can break strict catalog conversion

`market.active` is `state == "live"`. OKX can return `state: "preopen"`
instruments that carry only `instId` and `instType` (no `baseCcy`, `quoteCcy`, or
`uly`). `parse_market` then yields a market with empty base/quote and a
symbol equal to the raw id. Ferris's `convert_market` rejects empty base/quote as
`UpstreamData` and fails the **entire** catalog, not just the row. No preopen
rows were **observed** live during this review, but the path is reachable on a
new listing.

### 3.16 `fetchCurrencies` is authentication-gated

`fetch_currencies` returns an empty map when credentials are absent or sandbox
mode is on, and only then calls the private `asset/currencies` endpoint. Because
`has.fetchCurrencies` is `true`, the stock `load_markets` path still invokes it,
but without credentials it makes no request. Currency metadata is consequently
empty, and symbol/currency resolution relies on `safe_currency_code` fallbacks.

### 3.17 `fetchMarkets` defaults to all four product types

The default `types` list is `["spot", "future", "swap", "option"]`, so an
unrestricted `load_markets` also loads the `BTC-USD`/`ETH-USD` option chain
(thousands of contracts). `options.fetchMarkets.types` (or the legacy
`options.fetchMarkets`) overrides it. Ferris's `for_scope` already writes
`options.fetchMarkets = {"types": [...]}`, which OKX reads.

### 3.18 `defaultType` is `spot`

OKX's `options.defaultType` is `"spot"`. `handle_market_type_and_params` (used
by `fetch_tickers`, `fetch_mark_prices`, `fetch_open_interests`) reads it, so an
unparameterised bulk call targets `SPOT`. Ferris's provider config sets
`defaultType`/`defaultSubType` per scope.

### 3.19 `watch_tickers(null)` is per-symbol, not an aggregate

`watch_tickers` with `null` symbols resolves `market_symbols` to every loaded
symbol and `subscribe_multiple` builds one subscription argument per symbol. It
is not a single aggregate subscription the way Lighter's `watch_tickers(null)`
is. `subscribe_multiple` also builds one message hash per symbol.

### 3.20 Other notes

- `has.fetchCurrencies`, `has.fetchTicker`, `has.fetchOpenInterest`,
  `has.fetchMarkPrices`, `has.fetchFundingRates`, `has.fetchFundingRateHistory`,
  `has.fetchIndexOHLCV`, `has.fetchMarkOHLCV` are all `true`;
  `has.fetchFundingIntervals`, `has.fetchOrderBooks`,
  `has.fetchPremiumIndexOHLCV` are `false`.
- Option `fetch_trades` uses a distinct endpoint and does not honour the
  `method`/history selection used for other products.
- OKX `market/trades` and `market/history-trades` `since` is not sent
  server-side unless the history method and cursor pagination are used.
- `market/index-tickers` accepts either one `instId` or one `quoteCcy`; there is
  no all-quote single call.
- `parse_ticker` always sets `change` and `percentage` to `null`, and
  `vwap`/`previousClose` to `null`.
- The `xperp` rule type (long-dated futures that still pay funding) is treated as
  funding-eligible by `fetch_funding_rate(s)` alongside swaps.
