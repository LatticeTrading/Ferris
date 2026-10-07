# Apex — new exchange support findings

Original research findings are retained below. Integration is now implemented;
see **§5** for supported behavior, corrections discovered during verification,
and remaining limitations.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`), plus live public reads against `https://omni.apex.exchange`.
Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/apex.rs` (REST, ~2836 lines)
- `ccxt-base-4.5.85/src/exchanges/apex_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/apex.rs` (WebSocket)
- `ccxt-4.5.85/src/exchanges/apex_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/apex_typed.rs`
- `https://api-docs.omni.apex.exchange/` (Apex Omni v3 docs)

Apex is already a compiled provider in the pinned release: the `apex` cargo
feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Apex` /
`ccxt_pro::Apex` are exported. Public id in CCXT is `apex`. The provider is
flagged `pro: true`, `dex: true`, `certified: false`.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works via bulk `GET /v3/symbols`. Reads only `data.contractConfig.perpetualContract`. `active` is read from `enableTrade` (unlike GRVT, where it is not read). |
| `fetchTrades` (REST) | `fetch_trades` | Works via `GET /v3/trades`. No historical window; see §3.3. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `GET /v3/depth`. Depth cap 200; see §3.5. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `GET /v3/klines`. Limit capped at 200; see §3.4. |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works. Topic `recentlyTrade.H.{id2}`; message hash `trade:{symbol}`. |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works. Topic `orderBook{N}.H.{id2}`; message hash `orderbook:{symbol}`. |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works. Topic `candle.{timeframeId}.{id2}`; message hash `ohlcv::{symbol}::{timeframe}`. |
| `/v1/ws` tickers | Pro `watch_ticker` / `watch_tickers` | Works. Topic `instrumentInfo.H.{id2}`; message hash `ticker:{symbol}`. |
| Statistics ticker fields | `fetch_tickers` (bulk REST) | One bulk call returns funding, mark, index, last, 24h volume, and open interest. See §1.1. |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock (`GET /v3/history-funding`, public); not currently exposed by any Ferris endpoint. Requires a symbol argument. |
| `fetchCurrencies` | `fetch_currencies` | Exists in stock (`has.fetchCurrencies = true`); called during `load_markets`. Reuses `/v3/symbols`; see §3.16. |
| `fetchOpenInterest` (singular) | `fetch_open_interest` | Exists via `GET /v3/ticker`; per-market fallback only. |
| `fetchTime` | `fetch_time` | Exists via `GET /v3/time`. |

All public market-data calls are unauthenticated.

### 1.1 Bulk statistics ticker

`GET /v3/data/all-ticker-info` is a single bulk public call. Each row carries
`fundingRate`, `predictedFundingRate`, `markPrice`, `indexPrice`, `lastPrice`,
`volume24h`, `turnover24h`, `openInterest`, and `nextFundingTime`. This is the
only bulk source for funding, mark, index, and open interest; Apex has no
`fetchFundingRates`, `fetchFundingIntervals`, or plural `fetchOpenInterests`.

The live response returned **361 rows**, of which only the 138 perpetuals are in
the loaded catalog (see §3.17). The extra rows are tokenized-stock and
prediction markets.

### 1.2 Timeframes

Apex exposes: `1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M`.

`1M` **is** present. There is no `3m`, `8h`, or `1y`.

### 1.3 `id2` indexing

The base `load_markets` indexes `markets_by_id` by the secondary `id2` field
(the CCXT base code explicitly names apex's `crossSymbolName` as the motivating
case). This means WS handlers that resolve a market from the id2-based stream id
(`safeMarket(data['s'])`) reach the unified symbol. Positive for the live path.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 Product coverage is perpetuals only

`has.spot`, `has.margin`, `has.future`, `has.option` are all `false`.
`has.swap` is `true`. `parse_market` hardcodes `type: 'swap'`, `linear: true`,
`inverse: false`, `contract: true` for every row, and `fetch_markets` only reads
`contractConfig.perpetualContract`.

The venue itself publishes other products in the same `/v3/symbols` response
that stock ignores entirely: `contractConfig.stockContract` (tokenized stocks,
48 entries), `contractConfig.predictionContract` (184 entries),
`contractConfig.prelaunchContract`, and `omniSwapConfig`. None reach the
catalog.

### 2.2 No `fetchFundingRates`

`has.fetchFundingRates = false`; no method. Funding is only a member of the
ticker payload.

### 2.3 No `fetchFundingIntervals`

No such method in `has` or the core. No funding-interval field exists anywhere
in the `/v3/symbols` response.

### 2.4 No `fetchOpenInterests` (plural)

`has.fetchOpenInterests = false`. Only the singular `fetch_open_interest`
exists.

### 2.5 No `fetchMarkPrices` / `fetchIndexPrice`

No such methods. Mark and index are only members of the ticker payload.

### 2.6 No candle `price` / `candleType` parameter

`has.fetchIndexOHLCV`, `has.fetchMarkOHLCV`, and `has.fetchPremiumIndexOHLCV`
are all `false`. `fetch_ohlcv` reads no price-source parameter. There is no
mark/index/premium candle source.

### 2.7 No Pro `un_watch_*` methods

Apex Pro implements 12 `watch_*` methods and **zero** `un_watch_*` methods
(compare Binance/Bybit/Hyperliquid/Lighter/Aster, which all implement several).
The base `Exchange::un_watch_trades` / `un_watch_order_book` / `un_watch_ohlcv`
/ `un_watch_tickers` all `panic!(NotSupported)`. There is no **stock CCXT**
unsubscribe path. Apex itself supports native unsubscribe; Ferris now supplies
a small control adapter (see §3.1 and §5).

### 2.8 No aggregate `watch_tickers` semantics assumed by Ferris

`watch_tickers(null, ...)` resolves to every loaded market symbol, not a single
aggregate subscription. This differs from Lighter's stock
`watch_tickers(null)` aggregate on `market_stats/all`, which Ferris's live
statistics path depends on.

---

## 3. Findings that may hinder integration

### 3.1 One shared public WebSocket URL; stock unsubscribe implementation missing

Apex's public WS is a single URL: `wss://quote.omni.apex.exchange/realtime_public?v=2`
(then `&timestamp=<ms>`). Trades, book, candles, and tickers all share it.

Ferris's live layer keys URL owners by URL and isolates transport per URL.
Calling a missing stock `un_watch_*` would panic and rebuild every channel on
Apex's shared URL. The initial integration explicitly used this reconnect
fallback rather than dispatching a missing method. Extended has a similar
stock gap but uses per-channel URLs, so teardown normally affects only that feed.

Apex documents native `{"op":"unsubscribe","args":["topic"]}`. Ferris now
uses a per-channel policy and an Apex control adapter inside stock's single
receive loop; successful removal preserves sibling feeds. Rejection or missing
acknowledgment falls back to a clean reconnect (details in §5).

Stock `watch_topics` skips already-subscribed hashes. Retirement must therefore
clear stock subscription/cache bookkeeping after the acknowledgment before
re-adding the topic. Sending JSON alone does not implement safe unsubscribe.

### 3.2 WS URL carries a per-core cached timestamp

`get_ws_public_url` appends `&timestamp=<milliseconds>` to the WS URL "for
connection-time signing" and caches the result in `options.wsPublicUrl` on the
core instance. A new core recomputes a new timestamp.

Ferris computes `spec.url` in `prepare_live` using one core, then constructs a
**different** core in `run_session` (and again on every reconnect). The session
core's `get_ws_public_url()` therefore resolves a different URL than
`spec.url`. Stock `watch_trades` / `watch_order_book` / `watch_ohlcv` /
`watch_tickers` each call `get_ws_public_url()` internally to build their
`watch_multiple` URL, independent of the URL Ferris passes to `ws_run`.

The timestamp is not guaranteed to be identical between prepare and session, and
reconnect always regenerates it. The comment states the timestamp is used for
connection-time signing, so a divergent value is not merely a cache-key
mismatch.

### 3.3 `fetchTrades` has no historical window; `since`/`until` are not sent

Stock `fetch_trades` builds only `{ symbol, limit }` and calls `GET /v3/trades`.
It never adds `since` to the request; `since` is applied only as a client-side
filter over the returned window. Live checks confirmed `start` and
`beginTimeInclusive` query parameters are ignored: the endpoint always returns
the newest trades regardless.

Consequently `since` and `until` on Apex are client-side filters over a
newest-N window, not upstream time bounds. An old `since` does not produce
historical coverage; a recent `since` can filter the entire response to empty.

Observed limits: stock default `limit` is 500; the endpoint returns up to
**1000** (requests for 1500/5000 still returned 1000). The docs state a maximum
of 500.

### 3.4 OHLCV limit is capped at 200

Stock `fetch_ohlcv` sets `limit = min(limit, 200)` and defaults to 200. The
endpoint rejects larger limits (a raw request with `limit=500` returned no
rows). A raw request with no `limit` returned 1500 rows, but stock always sends
a limit, so the effective ceiling through stock is 200.

`since` maps to `start` (seconds); `until` maps to `end` (seconds) via the
stock `handle_until_option` multiplier `0.001`. Historical windows work for
candles (unlike trades).

### 3.5 Order book depth

- REST `GET /v3/depth`: native default (no `limit`) is **25**; stock
  `fetch_order_book` hardcodes a default of **100**; the observed ceiling is
  **200** (`limit=201` and `limit=500` both returned 200). The docs state a
  default of 100 and do not state a maximum.
- WS topic `orderBook{N}` embeds the requested depth in the topic string
  (docs example `orderBook200.H.BTCUSDT`; stock default `25` when `limit` is
  null). Depth is therefore an upstream topic identity, not just a display
  slice.
- Book `nonce` is `data.u`. Book `timestamp` is the stock receipt time
  (`milliseconds()`), not exchange time.

### 3.6 REST bulk ticker has no exchange timestamp

The `/v3/data/all-ticker-info` rows have no `time` or `closeTime` field (keys:
`iconUrl, fundingRate, highPrice24h, indexPrice, lastPrice, lowPrice24h,
nextFundingTime, openInterest, oraclePrice, markPrice, predictedFundingRate,
price24hPcnt, symbol, tradeCount, turnover24h, volume24h`). Ferris's
`exchange_time` helper reads `info.time` / `info.closeTime`, so
`exchangeTimestamp` resolves null for every REST-sourced Apex statistics field.
Freshness would rely on `receivedTimestamp` only.

The WS ticker path is different: `handle_ticker` sets the parsed ticker's
timestamp from the frame-level `ts` (microseconds × 0.001).

### 3.7 `nextFundingTime` is ISO-8601

The REST bulk ticker returns `nextFundingTime` as an ISO-8601 string
(`"2026-10-06T01:00:00Z"`). The docs show both an ISO example and a
time-only example (`"10:00:00"`). Ferris's `funding_field` reads the
`next_payment_key` through `positive_integer`, which parses only integer
strings/numbers; an ISO-8601 value resolves to null.

### 3.8 Funding interval is not published

No funding-interval field exists in `/v3/symbols` or the ticker. Funding history
shows exact 1-hour spacing (`fundingTimestamp` deltas of 3,600,000 ms; latest
settled `2026-10-06T00:00:00Z` with `nextFundingTime` `2026-10-06T01:00:00Z`).
The interval is an observation, not a venue-declared value.

### 3.9 Funding rate semantics

The bulk ticker exposes both `fundingRate` (e.g. `0.00000052`) and
`predictedFundingRate` (e.g. `0.0000125`), and `/v3/history-funding` exposes
settled `rate` values (e.g. `-0.00000816`). The relationship between
`fundingRate` and the current accruing period is not stated by the source; it
is not obviously equal to the last settled rate nor to the predicted rate.
Ferris publishes an explicit `rateUnit` and `kind` (`Estimate`, `Settled`,
`CurrentUnclassified`) per funding field, and the Apex value's unit/kind is not
established by the source.

### 3.10 Open interest is base-denominated, with no notional

`/v3/data/all-ticker-info` `openInterest` (e.g. `1576.66` for BTC at ~$85,766)
is base-coin quantity. There is no notional/value field. Stock
`parse_open_interest` sets `openInterestAmount` from `openInterest` and
`openInterestValue` null. This is the same shape as Hyperliquid's
"amount is base" case, not a two-sided or notional value.

### 3.11 `contractSize` is stock-derived from `minOrderSize`

`parse_market` sets `contractSize = safeNumber(market, 'minOrderSize')`, and the
`/v3/symbols` response has no `contractSize` field at all. For BTC-USDT this
yields `contractSize = 0.001` (the minimum order step), which is not a contract
multiplier. This is identical to upstream `apex.ts` and is not a Ferris
artifact. The value flows into `UnifiedMarket.contractSize`.

### 3.12 Identity uses the config symbol, not the API symbol

Apex rows carry two native ids:

- `id` = `symbol` = `"BTC-USDT"` (used by stock as `market.id`)
- `id2` = `crossSymbolName` = `"BTCUSDT"` (used by the trading/depth/trades/
  klines/WS topics)

Stock `market.id` is the config `symbol` (`"BTC-USDT"`), and stock
`market.id2` is `"BTCUSDT"`. Ferris's `build_identity` does not override the
native id for Apex, so `exchange_market_id` becomes `"BTC-USDT"`. The REST
market-data endpoints and WS topics use `id2` (`"BTCUSDT"`), not the id Ferris
records as the exchange market id.

### 3.13 `id2` is not a resolution alias

Ferris's `build_aliases` adds the display symbol (`BTC/USDT`), the CCXT symbol
(`BTC/USDT:USDT`), the native id (`BTC-USDT`), the identity market id, and the
`info.name` (absent for Apex). It does not read `id2`, so `BTCUSDT` is not a
resolvable alias even though it is the id the venue's REST/WS APIs actually use.

### 3.14 Bare base asset is not a resolution alias

`build_aliases` adds the bare `base` (`BTC`) only for Hyperliquid/Lighter/
Extended, or for Binance/Bybit/Aster perpetuals with a USDT quote. Apex is not
in either group, so `BTC` alone would not resolve; callers would need the full
display or CCXT symbol.

### 3.15 `isPrelaunch` is not handled by the active check

Ferris's `convert_market` treats a market as inactive when
`info.isPreListing` is true (Bybit's field name). Apex uses `isPrelaunch`
(all currently `false`). A future prelaunch row with `enableTrade = true` would
be classified active. The `/v3/symbols` row also carries `enableDisplay` and
`enableOpenPosition`, neither of which stock maps to `active` and neither of
which Ferris reads. Live counts: 138 perpetuals, 88 with `enableTrade = true`,
86 with `enableDisplay = true`, 87 with `enableOpenPosition = true`; two
markets trade with `enableDisplay = false`, one trades with
`enableOpenPosition = false`.

### 3.16 `fetchCurrencies` reuses `/v3/symbols`

`has.fetchCurrencies = true`, so stock `load_markets` calls `fetch_currencies`
before `fetch_markets`. Apex's `fetch_currencies` calls the same
`public_get_v3_symbols` endpoint (reading `data.spotConfig.assets` and
`spotConfig.multiChain.chains`). Every metadata load therefore makes **two**
requests to `/v3/symbols`. Ferris disables `fetchCurrencies` only for Binance.

The currencies returned are spot tokens (`USDT`, `USDC`, `BNB`, ...), while the
perpetual markets settle in `settleAssetId` (`USDT`).

### 3.17 Bulk ticker returns non-catalog rows

The 361-row bulk ticker includes tokenized-stock and prediction markets that
are absent from the loaded perpetual catalog (e.g. `AAPLUSDT`,
`1000000MOGUSDT`). Live inspection found no duplicate `info.symbol` values and
no alias collision with the perpetual catalog, so these extra rows are ignored
by catalog lookup. They do still pass through stock `parse_tickers` and any
per-row failure handling.

### 3.18 Inconsistent empty/error signaling

Apex signals errors through the response body, not HTTP status. Observed for
invalid symbols:

| Endpoint | Response |
| --- | --- |
| `GET /v3/ticker?symbol=NOPEUSDT` | `200 {"data":[]}` |
| `GET /v3/trades?symbol=NOPEUSDT` | `200 {"data":[]}` |
| `GET /v3/klines?symbol=NOPEUSDT` | `200 {"data":{}}` |
| `GET /v3/depth?symbol=NOPEUSDT` | `200 {"data":{"a":null,"b":null,"s":"","u":0}}` |
| `GET /v3/history-funding?symbol=NOPE-USDT` | `200 {"code":3,"msg":"invalid symbol: NOPE-USDT"}` |

Stock `handle_errors` reads `response.code` and throws only when `code` is
present and non-zero. So the first four cases produce no stock error (empty or
null payloads), while the funding-history case throws. A depth response with
`a: null` / `b: null` is not an array, so a downstream book converter that
requires arrays would fail as invalid upstream data rather than as a bad symbol.

### 3.19 `category` is a sector tag, not a product category

Each `/v3/symbols` row has a `category` field with sector values (`L1`, `MEME`,
`DEFI`, `AI`, `L2`, `INFRA`, `GAME`, and comma-joined combinations; five rows
have none). Stock `parse_market` ignores it, so Ferris's `info.category` would
be null for Apex. It is not a spot/linear/inverse/option product category.

### 3.20 REST and WS hosts differ

- REST public: `https://omni.apex.exchange/api` (configurable base + `/api`).
- WS public: `wss://quote.omni.apex.exchange/realtime_public?v=2` (separate
  host, query string, dynamic timestamp).
- Testnet: `https://testnet.omni.apex.exchange/api` and
  `wss://qa-quote.omni.apex.exchange/realtime_public?v=2`.

Ferris's existing per-venue URL overrides cover a REST base and (for some
venues) a WS base; Apex needs both, and the WS URL is not derivable from the
REST base.

### 3.21 Rate limits

`rateLimit: 20` (20 ms per request) with `enableRateLimit` on. The docs state an
IP limit of 600 requests per 60 seconds for public endpoints and a private
per-account limit of 300/60s. Apex's all-market statistics poll is one bulk
request per poll, so it is not per-instrument; but the doubled metadata load
(§3.16) and any per-market fallback calls count against the IP quota.

### 3.22 WS order book depth is part of the topic identity

Because the topic is `orderBook{N}.H.{id2}`, the requested depth changes the
subscribed topic. The stock default is 25 when `limit` is null. Ferris's
`message_hash` for books is `orderbook:{symbol}` (depth-independent), so all
depths share one hash even though the upstream topic differs.

### 3.23 WS OHLCV hash shape is unique

Apex's OHLCV message hash is `ohlcv::{symbol}::{timeframe}`. Ferris's
`message_hash` match arms cover Hyperliquid, Binance/Bybit, Aster, Extended, and
a `(Lighter, Ohlcv)` unreachable arm; there is no catch-all OHLCV arm, so Apex's
shape is not covered by any existing arm. Trades (`trade:{symbol}`) and books
(`orderbook:{symbol}`) do match existing generic arms.

### 3.24 Trades parse omits fee, type, and taker/maker

Apex `parse_trade` sets `type`, `takerOrMaker`, `cost`, `order`, and `fee` all
null. The raw trade row has only `i` (uuid), `p`, `S`, `v`, `s`, `T`. WS
`parse_ws_trade` additionally reads `L` (tick direction) and `BT`, but still
emits null `type`/`takerOrMaker`/`fee`.

### 3.25 `fetch_time` uses a separate endpoint

`fetch_time` calls `GET /v3/time`. The bulk ticker has no server time field.

### 3.26 Candle and ticker `1M` alias

`timeframes` maps `1M` → `M`. Ferris's timeframe normalization accepts stock
aliases; `1M` is present, so no aliasing gap. Apex has no `3m`/`8h`/`1y`.

---

## 4. Source-of-truth notes

- The venue docs disagree with observed behavior on at least two limits: trades
  (docs say max 500; endpoint returned 1000) and depth (docs say default 100 and
  state no maximum; native default is 25 and the observed ceiling is 200).
- The `contractSize = minOrderSize` mapping is identical in upstream
  `ccxt/ts/src/apex.ts`, so it is not specific to the Rust transpilation.
- `id2` (`crossSymbolName`) indexing in the base `load_markets` is present
  specifically for Apex-style venues; it is not something Ferris adds during REST loading.
  See §5 for the important difference when seeding a Pro core from owned metadata.

---

## 5. Implemented integration and verification

Apex is registered as `apex`, using the pinned stock REST/Pro providers. There
is no custom HTTP adapter, raw WebSocket parser, upstream fork, or private API.

### Supported surfaces

- `fetchMarkets`: stock Omni perpetuals, excluding inactive/prelaunch rows by
  default. `includeInactive` retains those rows. Tokenized stocks, prediction,
  spot and separate prelaunch product catalogs remain outside stock coverage.
- `fetchTrades`: newest-N window up to 1,000; since/until are local filters.
- `fetchOrderBook`: default 100, up to 200 levels per side; larger requests fail
  explicitly rather than silently clamp.
- `fetchOHLCV`: up to 200 historical candles, real start/end bounds, stock
  timeframes including `1M`/`M`. No mark/index/premium candle parameters.
- `/v1/ws`: trades, orderbook, and OHLCV use stock Pro. Book acquisition is fixed
  at `orderBook200`, allowing shared display slices from 1–200 (default 25).
- `fetchMarketStats` and `/v1/ws` marketstats: shared bulk `fetchTickers` polling
  (30 seconds, 90-second freshness), providing funding, mark/index/last prices,
  base/quote volume, and base-quantity open interest. OI notional remains null.
  No singular per-market OI calls or per-symbol ticker subscriptions are needed.
  Last-settled funding/history remain outside this integration.

### Metadata and field decisions

- `exchangeMarketId` uses API/stream `id2` (`BTCUSDT`); the config ID (`BTC-USDT`)
  stays in `info.exchangeSymbol`/`rawSymbol` and is still an alias. `BTC`, display
  pairs, CCXT symbols, and opaque catalog market IDs all resolve.
- Catalog `contractSize` is omitted: stock's minimum-order-size substitution
  is not a multiplier. `minOrderSize` stays intact. Quantities are not rescaled.
- **Additional defect discovered:** although Apex's own trade parser leaves
  cost null, stock `safe_trade` derives `price × amount × contractSize`. With
  the incorrect multiplier this publishes an incorrect notional (a live BTC
  trade of 0.012 at 85,280 became cost 1.02336 instead of 1,023.36). Ferris
  therefore publishes `cost: null` on Apex REST and live trades, without
  inventing a replacement cost or rewriting the stock market objects.
- `isPrelaunch: true` makes a row inactive. `enableDisplay` and
  `enableOpenPosition` do not disable otherwise tradable rows. Sector `category`
  tags do not become product identity categories.
- Optional spot currency loading is disabled, avoiding two identical symbols
  requests per metadata load.
- **Funding clarification:** while neither symbols nor ticker rows declare an
  interval, the official API docs' *Funding Fee* section explicitly says fees
  are exchanged every one hour and gives the multiplicative funding formula.
  Funding therefore uses hourly `decimalFraction`, `CurrentUnclassified`.
  It is not relabeled as predicted or settled. ISO-8601 next-payment values
  are parsed; time-only/malformed values remain null. Exchange timestamps for
  bulk fields remain null; no exchange timestamp is synthesized from receipt.

### WebSocket lifecycle decisions and corrections

- Owner identity uses the stable configured public URL. Each connection gets a
  fresh timestamp, and its exact URL is bound to `options.wsPublicUrl` before
  stock watches run. The actual timestamped client is dropped on teardown.
- **Additional id2 gap:** stock `load_markets` adds the secondary-ID index, but
  constructor seeding and `set_markets` do not. Ferris seeds Pro from owned
  catalog metadata rather than reloading it. The Apex policy mirrors the id2
  index after construction and after adding markets; stock parsers remain in
  charge. Without this, subscribed hashes receive no correctly routed rows.
- Missing stock unwatch is handled by reusable per-channel policy and native
  Apex control, retaining stock transport and market-data parsers. Successful
  unsubscribe leaves siblings live. Re-add waits for its retirement ACK and a
  fresh book snapshot; retiring frames cannot repopulate stock caches. Sending
  is queued behind watches, and success clears per-feed stock bookkeeping.
- Rejected/unsent unsubscribe or a 10-second missing ACK rebuilds the shared
  connection from current demand, with continuity errors for remaining viewers.
  Pending retirements are bounded. Removing one of several viewers does nothing
  upstream; removing all demand closes immediately. Reconnect-only policy is
  reusable for future unsupported channels; control/network failures retain
  ordinary failure backoff.
- Public ACKs echo the operation/topics but not a supplied `reqId`. Initial
  snapshots precede subscribe ACKs; they must not be discarded while waiting
  for that ACK. The unsubscribe ACK is the ordered generation boundary. See
  [streaming-control-boundaries.md](../streaming-control-boundaries.md) for
  protocol assumptions, duplicate-ACK limitations, and reuse requirements.
- REST nonce is `u`; **stock WS nonce is null**, and its timestamp uses frame
  `ts / 1000`, not REST receipt time. The stock Pro parser does not validate
  sequence gaps; delivery does not claim stronger synchronization guarantees.

### Configuration

- `APEX_REST_BASE_URL=https://omni.apex.exchange/api`
- `APEX_WS_URL=wss://quote.omni.apex.exchange/realtime_public?v=2`

The REST setting includes `/api`; the WS setting excludes a timestamp. Testnet
can use the documented testnet REST and QA quote WS hosts independently.

### Verification performed

- Full `cargo test --locked` regression suite passed after native-control
  integration: **159 passed**, 5 opt-in live tests skipped.
- New deterministic HTTP/WS tests use real stock provider methods and loopback
  transports: limits/bounds, unsupported selectors, aliases, inactive/prelaunch
  rows, bulk statistics without fanout, field provenance, shared book depths,
  two finite update bursts followed by silence, mixed channels, feed removal,
  network reconnect, later-symbol id2 indexing, and last-viewer cleanup.
  Native-control additions cover delayed/rejected/missing ACKs, re-add races,
  duplicate/unmatched ACKs, late retirement data, snapshot-before-subscribe-ACK
  ordering, fresh snapshots, timeout continuity errors/recovery, and shutdown.
  A receiver regression ensures reconnect errors survive subsequent epoch
  changes even for slow readers, while stale market data is still discarded.
- Focused unit tests cover unknown/time-only funding dates, valid offsets,
  zero/null/invalid statistics, metadata identity, and omitted contract size.
- Public Apex smoke passed for all four ordinary snapshot endpoints and the
  five opt-in HTTP tests. Public catalog returned 88 active perpetuals; bulk
  statistics enumerated the same 88 without source failures. A mixed public
  `/v1/ws` session delivered trades, depth, candles, and all six statistics
  fields, including ordered statistics snapshot→delta delivery. Public trades
  were rechecked with the incorrect derived cost suppressed. A subsequent
  public native unsubscribe/re-add check exercised all three data channels and
  immediate book re-add on one upstream connection, with no sibling continuity
  errors. Nine direct topic cycles confirmed pre-subscribe-ACK snapshots and
  observed no post-unsubscribe-ACK data during short quiet windows; ACKs omitted
  the supplied request IDs.
- `cargo fmt -- --check` and `cargo build --locked` passed. Clippy completes
  with existing repository warnings; strict `-D warnings` is not clean in the
  baseline shared code (no warnings in the new Apex modules/tests).
- This is bounded integration verification, not a load test or long-running
  reliability guarantee. No production deployment was changed.
