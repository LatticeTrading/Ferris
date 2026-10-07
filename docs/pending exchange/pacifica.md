# PACIFICA — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/pacifica.rs` (REST, ~5000 lines)
- `ccxt-base-4.5.85/src/exchanges/pacifica_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/pacifica.rs` (WebSocket, ~2200 lines)
- `ccxt-4.5.85/src/exchanges/pacifica_typed.rs`,
  `ccxt-pro-4.5.85/src/pro_typed/pacifica_typed.rs`

PACIFICA is already a compiled provider in the pinned release: the `pacifica`
cargo feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Pacifica` /
`ccxt_pro::Pacifica` are exported. Enabling support is a matter of turning on the
feature and wiring the existing provider into Ferris's contract. Public id in
CCXT is `pacifica`. It is flagged `dex: true`, `pro: true`, precision mode
`TICK_SIZE`. Products are **spot** and **linear perpetual swap**, both
quoted/settled in **USDC**.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. Bulk single call `public_get_info` (`GET /info`) returns every spot and perp market. `params` is forwarded to the endpoint but no product filter is applied. |
| `fetchTrades` (REST) | `fetch_trades` | Works via `public_get_trades` (`GET /trades`). The request only carries `symbol`; `since` and `limit` are applied client-side by `parse_trades`/`filter_by_since_limit`. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `public_get_book` (`GET /book`). Aggregation is `aggLevel` (default 1; allowed 1/10/100/1000/10000). The `limit` argument is accepted but unused. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `public_get_kline` (`GET /kline`). **Requires `since`** (see §3.1). `until`/`endTime` → `end_time`. Default max limit 3950. |
| `/v1/ws` trades | Pro `watch_trades` | Works. Shared URL; `since`/`limit` filtered client-side. |
| `/v1/ws` order book | Pro `watch_order_book` | Works. Snapshot-based; `aggLevel` only. The `limit` argument is accepted but unused. |
| `/v1/ws` candles | Pro `watch_ohlcv` | Works. Uses the stock timeframe ids. |
| Statistics ticker fields | REST `fetch_tickers` (bulk) | Works. One call `public_get_info_prices` (`GET /info/prices`) returns every symbol's funding, mark, mid, next funding, OI, oracle, volume, timestamp. |
| Statistics funding | REST `fetch_funding_rates` (bulk) | Exists and is bulk, but reads the **same** `GET /info/prices` payload. |
| Statistics open interest | REST `fetch_open_interests` (bulk) | Exists and is bulk, same `GET /info/prices` payload. |
| Statistics live tickers | Pro `watch_tickers` (bulk) | Works. `watch_tickers(null)` subscribes to the `prices` source for all symbols and resolves the `tickers` hash (see §3.12). |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock (per symbol, cursor pagination, default limit 100). Not currently exposed by any Ferris endpoint. |
| `fetchCurrencies` | `fetch_currencies` | `has.fetchCurrencies = false`; not called during market load. |

All public market-data calls are unauthenticated. Public WS subscriptions call
`setup_api_key_headers`, which adds headers only when a key is configured.

### Timeframes

REST and WS expose an identity map: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h,
12h, 1d, 1w, 1M`. `1M` **is** present.

---

## 2. What is absent from the source

These are upstream gaps in the pinned CCXT release, not Ferris limitations.
Unlike GRVT, PACIFICA **does** have a bulk `fetch_tickers`, bulk
`fetch_funding_rates`, and bulk `fetch_open_interests`. The gaps are narrower.

### 2.1 No `fetchFundingIntervals`

No such method and no `has` flag. Stock `parse_funding_rate` hardcodes
`interval = "1h"`. No native funding-interval field is read.

### 2.2 No next-funding timestamp

`next_funding` in the prices payload is a **rate**, not a time. `parse_funding_rate`
sets `nextFundingTimestamp = null`. `parse_funding_rate` sets `fundingTimestamp`
to the next hourly wall-clock boundary derived from `self.milliseconds()`, not
from a source field.

### 2.3 No base volume in the bulk payload

The prices payload exposes `volume_24h`, which stock maps to
`quoteVolume`. There is no base-asset volume member.

### 2.4 No last-trade price in the bulk payload

The payload carries `mid` (midpoint) and `yesterday_price`. `parse_ticker` sets
`close = mid`; it does not set a `last`. Because stock `safe_ticker` assigns
`last = close`, the unified ticker's `last` is the **midpoint**, not a trade
price (see §3.5).

### 2.5 No mark/index candles

`has.fetchMarkOHLCV`, `has.fetchIndexOHLCV`, `has.fetchPremiumIndexOHLCV` are
all `false`. `fetch_ohlcv` has no `price`/`candleType`/`priceType` branch.

### 2.6 No order-book depth parameter

`fetch_order_book` and `watch_order_book` ignore the `limit` argument entirely.
The only source knob is `aggLevel` (price grouping), not depth. The full book is
returned.

### 2.7 No server-side trade `limit`/`since` or history

`fetch_trades` sends only `symbol` to `GET /trades`. The response carries
`last_order_id`, but stock does not paginate it and does not use it as a cursor.
`since`/`limit` are client-side filters over the fixed recent window.

### 2.8 No market minimum order size

`parse_market` leaves `limits.amount.min`/`limits.amount.max` null and puts the
native minimum under `limits.cost.min` (`min_order_size`) and
`limits.cost.max` (`max_order_size`).

### 2.9 No inactive/delisted flag

`parse_market` hardcodes `active = true`; the comment states the endpoint never
returns non-active markets. There is no active/pre-listing field read from the
response.

### 2.10 No `fetchMarkPrices` / `fetchIndexPrice`

No such methods. Mark and index are only members of the ticker/funding payload
(`mark`, `oracle`).

---

## 3. Findings that may hinder integration

### 3.1 `fetchOHLCV` requires `since`

`fetch_ohlcv` panics with `ArgumentsRequired` when `since` is null:

```
fetchOHLCV() requires a "since" argument
```

Ferris's REST OHLCV path allows `since` to be omitted and passes `Value::Null`.
There is no stock branch that supplies a default start when `since` is null.

### 3.2 OHLCV source ceiling is ~3950

`defaultMaxLimit = 3950`. The stock comment says the docs claim 4000 but in fact
`>~3960` returns an error. This differs from every currently registered venue's
candle cap.

### 3.3 Rate-limit cost is non-uniform

`rateLimit = 600` (ms). Endpoint costs: `info = 1`, `info/fees = 1`,
`info/prices = 1`, `book = 1`, `trades = 1`, `funding_rate/history = 1`, but
`kline = 12` and `kline/mark = 12`. With `enableRateLimit`, the kline endpoints
throttle at twelve times the base cost. `calculate_rate_limiter_cost` has an
API-key path that returns `maxCostHugeWithApiKey` (default 3) for cost > 1 when a
key is present.

### 3.4 Timestamp field is not the one Ferris reads

Ticker/funding/price raw `info` uses `timestamp`. Ferris's `exchange_time` helper
only reads `info.time` / `info.closeTime`, so `exchangeTimestamp` would resolve
null for PACIFICA statistics rows without a venue-specific read.

### 3.5 Unified ticker `last` is the midpoint

`safe_ticker` sets `last = close`, and PACIFICA's `parse_ticker` sets
`close = mid`. So the unified ticker's `last` is the midpoint. This is the same
shape as Hyperliquid's documented "stock-ticker-last-is-midpoint" limitation,
not a true last trade price. `parse_ticker` does not set `markPrice` or
`indexPrice` on the unified ticker either; those exist only in `info` as `mark`
and `oracle`.

### 3.6 Funding interval is assumed hourly

There is no source interval field. Stock emits `interval = "1h"` and computes
`fundingTimestamp` as the next hour boundary from local time. Any derived
funding-rate equivalents depend on that assumption.

### 3.7 Single shared public WebSocket URL for all channels

PACIFICA's public WS is one URL: `wss://ws.pacifica.fi/ws`. Trades, book,
candles, and tickers all share it. Unlike GRVT, PACIFICA **does** implement
`un_watch_order_book`, `un_watch_trades`, `un_watch_ohlcv`, and
`un_watch_tickers`, so upstream unsubscribe is available. The single-URL
topology still means all PACIFICA channels contend on one owner/connection.

### 3.8 WS candle message hash differs from the generic pattern

`watch_ohlcv` resolves `candles:{parsedTf}:{symbol}` (stock), and
`un_watch_ohlcv` builds `unsubscribe:candles:{tf}:{symbol}`. Ferris's
`message_hash` has venue-specific candle arms for Hyperliquid/Bybit/Binance/
Aster/Extended; PACIFICA is not one of the existing generic patterns.

### 3.9 Spot and perpetual display symbols collide

Perp ids are the bare symbol (`BTC`), spot ids are `BASE-USDC` (`SOL-USDC`).
Both parse to a display pair `BASE/USDC` and CCXT symbol `BASE/USDC:USDC` for
perps, `BASE/USDC` for spot. Bare `BTC/USDC` therefore names both a perp and a
spot row and is ambiguous without a product selector. `settle` is always the
quote id (USDC).

### 3.10 Open-interest and volume denomination are not established by the source

`parse_open_interest` maps `openInterestAmount = open_interest` and
`openInterestValue = open_interest * mark`. The samples show `open_interest`
values of `"3634796"` and `"0.00524"`; nothing in the parser or the payload
declares whether the raw member is base amount or USD notional. Likewise
`volume_24h` is only declared as quote via its unified mapping; no base member
exists to confirm the quote interpretation.

### 3.11 Spot coverage of the prices payload is not established by the source

The `GET /info/prices` schema (`funding`, `mark`, `oracle`, `open_interest`) is
perpetual-oriented. `fetch_tickers`/`fetch_open_interests` iterate whatever rows
the endpoint returns and resolve them with `safe_market`; the stock code does
not assert that spot symbols are present or absent.

### 3.12 Bulk `watch_tickers(null)` is an aggregate subscription

Pro `watch_tickers` calls `market_symbols([symbols, null, true])` with
`allowEmpty = true`, so a null `symbols` argument passes through and the request
subscribes to the `prices` source for all symbols. It resolves the `tickers`
hash and stores rows in `self.tickers` keyed by unified symbol. This is the same
aggregate shape as Lighter's `watch_tickers(null)`, and unlike GRVT's
`watch_tickers`, which requires a non-null symbols array.

Ferris's live statistics path is currently hardwired to Lighter
(`subscribe_market_stats` → `subscribe_lighter_statistics`, and
`LiveChannel::Statistics` handling). The Lighter frame detector
`lighter_ticker_frame` keys on `info.market_id` plus Lighter-specific member
names (`mark_price`, `index_price`, `open_interest`, `last_trade_price`,
`current_funding_rate`, `daily_base_token_volume`). PACIFICA's WS ticker `info`
uses `symbol`/`mark`/`oracle`/`open_interest`/`funding`/`timestamp`, so it would
not match that detector.

### 3.13 REST and WS hosts are separate

- REST: `https://api.pacifica.fi` (`urls.api.public` / `private`)
- WS: `wss://ws.pacifica.fi/ws`, stored by the Pro describe under
  `urls.api.ws.public` (not under `urls.api` directly)
- Testnet: `https://test-api.pacifica.fi`, `wss://test-ws.pacifica.fi/ws`

### 3.14 Order-book aggregation is the only book parameter

`fetch_order_book`/`watch_order_book` read `aggLevel` from
`params`/`options.fetchOrderBook.aggLevel` (default 1). Values are 1, 10, 100,
1000, 10000. No depth limit exists upstream, so any displayed top-N is a client
projection of a full snapshot.

### 3.15 Market load performs no currency request

`has.fetchCurrencies = false`; `load_markets` does not fetch currencies for
PACIFICA (contrast GRVT, where `fetchCurrencies = true` adds a second public
request per metadata load).

### 3.16 Contract size and precision shape

Perps: `contractSize = 1`, `precision.price = tick_size`,
`precision.amount = lot_size`, `limits.leverage.max = max_leverage`,
`limits.price.min/max = min_tick/max_tick`, `limits.cost.min/max =
min_order_size/max_order_size`. `created = created_at`. Spot markets share the
same parse path with `instrument_type = "spot"` and quote parsed from the
`BASE-QUOTE` id.

### 3.17 No funding-interval or open-interest-history methods

`has.fetchOpenInterestHistory = false`; `fetchFundingIntervals` is absent.
Historical funding is only `fetch_funding_rate_history` (per symbol, cursor
pagination). Open interest history is not available.

### 3.18 Non-spot/perp instrument kinds would be dropped

`parse_market` treats anything whose `instrument_type` is not `"spot"` as a
swap (`isSwap = !isSpot`). It does not branch on future/option/other kinds. The
current catalog is spot + perpetual only.

### 3.19 Ferris-side exhaustiveness touch points

Adding a venue touches exhaustive `match venue`/`match (venue, …)` arms and the
`Venue::ALL` array across `venue.rs`, `rest.rs`, `stream.rs`, `statistics.rs`,
and `statistics_profile.rs`, plus the `Provider`/`LiveProvider` enums. The test
helper `disabled_config` in `tests/market_stats.rs` constructs `Config` by struct
literal and would need the new fields to compile.
