# Nado — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/nado.rs` (REST, ~4177 lines)
- `ccxt-base-4.5.85/src/exchanges/nado_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/nado.rs` (WebSocket, ~2861 lines)
- `ccxt-4.5.85/src/exchanges/nado_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/nado_typed.rs`

Nado is already a compiled provider in the pinned release: the `nado` cargo
feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Nado` /
`ccxt_pro::Nado` are exported. Enabling support is a matter of turning on the
feature and wiring the existing provider into Ferris's contract. Public id in
CCXT is `nado`; display name `Nado`; `pro = true`, `dex = true`, `rateLimit =
25`, `precisionMode = TICK_SIZE`, `version = "v1"`.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. Three bulk calls joined by `promise_all`: gateway `GET symbols`, gatewayV2 `GET pairs`, gatewayV2 `GET assets`. `active` derived from `trading_status != "not_tradable"`. |
| `fetchCurrencies` | `fetch_currencies` | Exists (`has.fetchCurrencies = true`), via gatewayV2 `GET assets`. The stock `load_markets` calls it before `fetch_markets` (see §3.11). |
| `fetchTrades` (REST) | `fetch_trades` | Works via archiveV2 `GET trades`. `limit` clamped to 500. `since` is a local filter only; `params.max_trade_id` passes through for pagination. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via archive `POST` candlesticks. `limit` clamped to 500. `granularity` from the timeframe map. `until` → `max_time` (seconds). `since` is a local filter only. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via gatewayV2 `GET orderbook`. `depth` defaults to 100 and is passed straight through; no stock maximum. |
| `fetchTickers` (bulk) | `fetch_tickers` | Works via archiveV2 `GET tickers`; returns every product in one call. Payload is last/volume/24h-change only (see §3.2). |
| `fetchFundingRates` (bulk) | `fetch_funding_rates` | Works via archiveV2 `GET contracts`; swap-only, one call for all swaps. |
| `fetchOpenInterests` (bulk) | `fetch_open_interests` | Works via the same archiveV2 `GET contracts`; swap-only. |
| `fetchFundingRate` (singular) | `fetch_funding_rate` | Works via archiveV2 `GET contracts`; panics `BadSymbol` for non-swap. |
| `fetchOpenInterest` (singular) | `fetch_open_interest` | Works via archiveV2 `GET contracts`; swap-only. |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works. |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works, but REST-seeded then delta-driven (see §3.7). |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works via `latest_candlestick`. |
| WS tickers/bbo | Pro `watch_ticker(s)` / `watch_bids_asks` | Works; bid/ask only. |
| WS unsubscribe | Pro `un_watch_trades`, `un_watch_order_book`, `un_watch_ohlcv`, `un_watch_tickers`, `un_watch_bids_asks` | All present (unlike GRVT). |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists but is **private** (`walletAddress` required); not currently exposed by any Ferris endpoint. |
| `fetchTime` / `fetchStatus` | `fetch_time` / `fetch_status` | Exist. |

All public market-data calls are unauthenticated. `requiredCredentials` marks
`walletAddress` and `privateKey` as required, but that only gates the private
methods (`fetch_balance`, orders, `fetch_funding_history`, etc.).

### Timeframes

REST and WS both expose: `1m, 5m, 15m, 1h, 2h, 4h, 1d, 1w, 4w`.

`3m, 30m, 6h, 8h, 12h, 3d, 1M` are **not** present.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No `fetchFundingIntervals`

No such method, and `fetchFundingIntervals` is absent from the `has` map. No
funding-interval field is present on any response used by the public methods
(see §3.5).

### 2.2 No `fetchMarkPrice` / `fetchMarkPrices`

No such methods. Mark price is only available as `mark_price` on the archiveV2
`contracts` row, exposed through `fetch_funding_rate(s)` / `fetch_open_interest(s)`
`info` and unified `markPrice`.

### 2.3 No `fetchOpenInterestHistory`

No such method. Open interest is only the current value on the `contracts` row.

### 2.4 No `fetchOrderBooks` (plural)

No such method. Order books are single-market only.

### 2.5 No `fetchLiquidations` / `watchLiquidations`

Neither the REST nor the Pro `has` map declares liquidations.

### 2.6 No Pro `watchFundingRate(s)`

Both are `false` in the Pro `has` map. `watchBalance` is also `false`.

### 2.7 Product coverage is spot + swap only

`has.spot = true`, `has.swap = true`, `has.future = false`, `has.option = false`.
`parse_market` maps `type == "perp"` to `swap`; there are no futures or options.

---

## 3. Findings that may hinder integration

### 3.1 No bulk REST endpoint for order books, trades, or OHLCV

`gatewayV2 GET orderbook`, archiveV2 `GET trades`, and archive `POST`
candlesticks are each **single-market**. Only markets (3 calls), tickers, and
contracts are bulk. Ferris acquires order books/trades/candles per market
already, so this is a request/connection budget characteristic rather than a
missing feature: Nado `rateLimit` is 25 ms and the `orderbook` / `trades` /
candlestick endpoints each cost 1 per call.

### 3.2 The bulk ticker payload omits funding, mark, index, open interest, timestamp, and bid/ask

`archiveV2 GET tickers` returns only `last_price`, `base_volume`,
`quote_volume`, and `price_change_percent_24h`. Funding, mark, index, and open
interest live in the **separate** `archiveV2 GET contracts` bulk endpoint, which
is swap-only. The two bulk sources overlap on `last_price` / volume. Stock
`parse_ticker` sets `high`, `low`, `open`, `bid`, `ask`, `vwap`, `change`, and
`timestamp` to null; `close`/`last` come from `last_price`; `percentage` from
`price_change_percent_24h`.

### 3.3 `fetchTrades` `since` is not sent upstream

`fetch_trades` builds `{ticker_id, limit}` only (limit `min(limit, 500)`) and
applies `since` as a local filter via `parse_trades`. The archive endpoint is a
recent-trades window; backward pagination requires `params.max_trade_id`, which
Ferris does not currently accept.

### 3.4 `fetchOHLCV` `since` is not sent upstream

`fetch_ohlcv` builds `{product_id, granularity, limit: min(limit, 500),
max_time?}` and applies `since` as a local filter via `parse_ohlc_vs`. There is
no `min_time`/start parameter, so the reachable history is at most the 500
candles ending at `until` (default now).

### 3.5 No funding interval; stock parsing hardcodes `24h`

`fetch_funding_rate` / `fetch_funding_rates` read `archiveV2 GET contracts`,
which carries no funding-interval field. Stock `parse_funding_rate` inserts
`interval: "24h"` unconditionally. Ferris's funding equivalents (hourly / 8h /
daily / annualized) scale from the interval, so a wrong interval scales every
equivalent.

### 3.6 Next funding timestamp is unix seconds, not milliseconds

The contracts row's `next_funding_rate_timestamp` is seconds (example
`1694379600`). Stock `parse_funding_rate` converts it to ms for the unified
`fundingTimestamp` via `safe_timestamp`, but the raw `info` value stays in
seconds. Ferris's `funding_field` reads the raw `info` member directly through
`positive_integer`, so a Nado row read that way yields a seconds value.

### 3.7 Order book is REST-seeded, delta-driven, with reject-on-gap and no depth parameter on the stream

`watch_order_book` seeds `fetch_order_book` then subscribes the `book_depth`
stream. `handle_order_book` compares the maintained book's `maxTimestamp`
against the frame's `last_max_timestamp`; on mismatch it removes the book,
deletes the subscription, and rejects with `InvalidNonce` instead of resyncing.
The `book_depth` subscription carries no depth parameter, so live depth is
fixed by the initial REST seed. The book object carries `timestamp`/`datetime`
and a private `maxTimestamp`, but no `nonce`; the generic `parse_order_book`
also sets `nonce: null`, so Ferris's `convert_book` nonce resolves null for
Nado. The pin documents no maximum depth for either the REST book or the
`book_depth` stream.

### 3.8 One shared public WebSocket URL for all channels

All public streams (trades, book, candles, tickers) use a single URL:
`wss://gateway.prod.nado.xyz/v1/subscribe`. A second WS URL
(`wss://gateway.prod.nado.xyz/ws/v2`) exists only for execute responses.
`streaming.keepAlive = 30000`, `streaming.ping = "ping"`. Because every public
channel shares one URL, Ferris's URL-keyed live owner would host all Nado
channels on one connection, dispatched by message hash.

### 3.9 Quote and settlement asset is `USDT0`, not `USDT`

`fetch_markets` reads the pair `quote` (default `USDT0`) and, for contracts,
sets `settle` equal to `quote`. Unified symbols are therefore
`BTC/USDT0:USDT0` (perp) and `BTC/USDT0` (spot). `base` has a trailing `-PERP`
stripped by `remove_market_suffix`. Native ids are numeric `product_id` values;
`info.ticker_id` is e.g. `BTC-PERP_USDT0`.

### 3.10 Minimum order size is not on `limits.amount.min`

`fetch_markets` sets `limits.amount.min = null` and `limits.amount.max = null`.
The minimum cost is `limits.cost.min`, parsed from `min_size` via `parse_x18`.
`precision.amount` is `parse_x18(size_increment)` and `precision.price` is
`parse_x18(price_increment_x18)`. Ferris maps `minOrderSize` from
`limits.amount.min` only, so Nado rows would carry a null minimum order size.

### 3.11 Catalog load is 3–4 requests and fetches assets twice

`has.fetchCurrencies = true`, so stock `load_markets` calls `fetch_currencies`
(gatewayV2 `GET assets`) before `fetch_markets`. `fetch_markets` itself calls
gateway `GET symbols`, gatewayV2 `GET pairs`, and gatewayV2 `GET assets`. The
`assets` endpoint is therefore requested twice per `load_markets`, and the
catalog depends on three calls that must all succeed (`promise_all`); any one
failure fails the whole catalog.

### 3.12 Bulk rows carry no exchange timestamp

Neither the tickers nor the contracts rows carry a time field. Stock
`parse_ticker` sets `timestamp` null and `parse_open_interest` sets `timestamp`
null. Ferris's `exchange_time` reads `info.time` / `info.closeTime`, which are
absent here, so `exchangeTimestamp` would resolve null and freshness would rely
on receipt time.

### 3.13 No WebSocket-maintained statistics

`all_bbo` / `best_bid_offer` carry only bid/ask, with no last, funding, mark,
index, or open interest. There is no aggregate ticker stream carrying the
statistics fields. (Contrast Lighter, whose `market_stats` ticker is maintained
over WS.)

### 3.14 WebSocket message-hash strings

Nado's Pro hashes are `trade:{symbol}`, `orderbook:{symbol}`, and
`ohlcv:{timeframe}:{symbol}` (timeframe before symbol). The order-book and
trade shapes match Ferris's generic arms; the OHLCV shape does not match any
existing arm.

### 3.15 REST archive trades lack taker/maker; WS trades lack ids

`parse_trade` sets `takerOrMaker` null for archive trades (no `is_taker`) and
derives `side` from `trade_type` or the sign of the raw order amount. WS
`trade` frames have no `id` (unified `id` null). Ferris models both as optional.

### 3.16 No candle `price` / `candleType` parameter

`fetch_ohlcv` and `watch_ohlcv` accept no price-source selector; the archive
candlestick request carries only `product_id`, `granularity`, `limit`, and
`max_time`. There is no mark/index candle option analogous to Bybit/Binance or
Extended.

### 3.17 `fetch_funding_rates` and `fetch_open_interests` hit the same endpoint

Both call archiveV2 `GET contracts` with `market_symbols(..., "swap", true)`.
Calling both for one statistics pass issues the same HTTP request twice (once
per method). Both are swap-only; spot has no funding/mark/index/OI.

### 3.18 `fetch_funding_rate` panics on non-swap symbols

`fetch_funding_rate` (and `fetch_open_interest`, `fetch_funding_history`) panic
`BadSymbol` when the resolved market is not `swap`. A per-market statistics pass
that calls these for spot rows would surface as an error rather than a
not-applicable state.

### 3.19 Timestamps and precision use x18 scaling in parsers

`parse_ohlcv` applies `parse_x18` to `open_x18`/`high_x18`/`low_x18`/`close_x18`
and to `volume`. `parse_trade` applies `parse_x18` to archive `base_filled`,
`quote_filled`, `fee`, and `order.priceX18`; WS `trade`/`fill` frames are also
x18. The WS candle `timestamp` is seconds and is normalized to ms by
`safe_timestamp` inside `parse_ohlcv`; the WS trade/fill/order/position frames
use `parse_ws_timestamp`, which converts nanosecond strings (length > 13) to ms
and leaves shorter values as-is.

### 3.20 REST and WS hosts/paths

- Gateway REST: `https://gateway.prod.nado.xyz/v1` (`GET symbols`)
- GatewayV2 REST: `https://gateway.prod.nado.xyz/v2` (`GET assets`, `GET pairs`,
  `GET orderbook`)
- Archive REST: `https://archive.prod.nado.xyz/v1` (`POST` candlesticks,
  funding history)
- ArchiveV2 REST: `https://archive.prod.nado.xyz/v2` (`GET tickers`,
  `GET contracts`, `GET trades`, `GET symbols`)
- Trigger REST: `https://trigger.prod.nado.xyz/v1` (private trigger orders)
- WS subscriptions: `wss://gateway.prod.nado.xyz/v1/subscribe`
- WS gateway (execute): `wss://gateway.prod.nado.xyz/ws/v2`

A `test` block mirrors these against `*.test.nado.xyz`. The five REST hosts are
distinct API roots (gateway, gatewayV2, archive, archiveV2, trigger), so a
single base-URL override does not cover them.

### 3.21 Funding rate unit is not declared by the source

`parse_funding_rate` reads `funding_rate` as a raw number with no unit marker,
and the documented sample is `-0.003664562348812546`. Nothing in the stock
parser states whether this is a decimal fraction or a percent; Ferris publishes
an explicit `rateUnit` per venue, so the unit must be qualified against the
venue before any value is published.
