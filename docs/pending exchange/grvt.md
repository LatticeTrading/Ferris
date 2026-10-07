# GRVT — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/grvt.rs` (REST, ~4700 lines)
- `ccxt-base-4.5.85/src/exchanges/grvt_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/grvt.rs` (WebSocket, ~1480 lines)
- `ccxt-4.5.85/src/exchanges/grvt_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/grvt_typed.rs`

GRVT is already a compiled provider in the pinned release: the `grvt` cargo
feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Grvt` /
`ccxt_pro::Grvt` are exported. Enabling support is a matter of turning on the
feature and wiring the existing provider into Ferris's contract. Public id in
CCXT is `grvt`.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. Loads `public_market_post_full_v1_all_instruments`. `active` is not read from the response (see §3.3). |
| `fetchTrades` (REST) | `fetch_trades` | Works via `full/v1/trade_history`. Limit capped at 1000. `since` → `start_time` (ns), `until`/`endTime` → `end_time` (ns). |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works via `full/v1/book`. Depth is rounded up to `[10, 50, 100, 500]`; requests above 500 do not set `depth`. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works via `full/v1/kline`. Limit capped at 1000. `since`/`until` converted to ns. |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works. Default `limit` selector 50 (allowed 50/200/500/1000). |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works. Defaults to the **delta** channel `v1.book.d`; snapshot channel `v1.book.s` is opt-in via `params.channel`. |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works. Uses `CI_*` timeframe ids. |
| Statistics ticker fields | Pro `watch_tickers` (WS) or REST `fetch_ticker` (singular) | Ticker payload carries funding, mark, index, last, OI, and buy/sell volume. There is **no bulk REST ticker** (see §2.1). |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock; not currently exposed by any Ferris endpoint. |
| `fetchCurrencies` | `fetch_currencies` | Exists in stock (`has.fetchCurrencies = true`); called during market load. |

All public market-data calls are unauthenticated. `fetch_markets` only calls
`sign_in` when an API key or private key is present.

### Timeframes

REST and WS both expose: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d,
3d, 5d, 1w, 2w, 3w, 4w`.

`1M` is **not** present.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No bulk `fetchTickers`

`has.fetchTickers` is absent from GRVT's `has` map, GRVT implements no plural
`fetch_tickers`, and the base emulation panics `NotSupported`. Only the singular
`fetch_ticker` exists.

Ferris's statistics acquisition (`statistics.rs::acquire_bulk`) is built on one
bulk `fetch_tickers` call per poll for every current venue. GRVT has no
equivalent bulk REST call.

### 2.2 No `fetchFundingRates`

GRVT implements no `fetch_funding_rates`. Funding is only available as a member
of the singular ticker payload (`info.funding_rate`) or via
`fetch_funding_rate_history`.

### 2.3 No `fetchFundingIntervals`

No such method. Funding interval is only present as market metadata
`funding_interval_hours` on each instrument.

### 2.4 No `fetchOpenInterest`

No such method. Open interest is only available as `info.open_interest` on the
singular ticker payload.

### 2.5 No `fetchMarkPrices` / `fetchIndexPrice`

No such methods. Mark and index prices are only available as `markPrice` /
`indexPrice` on the singular ticker, or `info.mark_price` / `info.index_price`.

### 2.6 No Pro `un_watch_*` methods

GRVT's Pro module implements no `un_watch_trades`, `un_watch_order_book`, or
`un_watch_ohlcv` (compare Bybit/Binance/Aster/Hyperliquid/Lighter, which all
have them). Combined with GRVT's single shared WS URL, there is no upstream
unsubscribe path (see §3.1).

### 2.7 Product coverage is perpetuals only

`has.spot`, `has.margin`, `has.future`, `has.option` are all `false`.
`has.swap` is `true`. `parse_market` only maps `kind == "PERPETUAL"` to
`type = "swap"`; every parsed market is linear, contract-based, `contractSize = 1`.

### 2.8 No candle `price` / `candleType` parameter

GRVT selects candle price source through `params.priceType` with values
`last | mark | index`, not through Ferris's `price` (`mark | index |
premiumIndex`) or Extended's `candleType`. The stock `fetch_ohlcv` reads
`params.priceType` only.

### 2.9 Ticker `lastPrice` is a real last price

`parse_ticker` sets `last` from `last_price` (unlike Hyperliquid, whose stock
ticker `last` is a midpoint). `change` and `percentage` are always null.

---

## 3. Findings that may hinder integration

### 3.1 One shared public WebSocket URL for all channels

GRVT's public WS is a single URL: `wss://market-data.grvt.io/ws/full`. Trades,
book, candles, and tickers all share it. Ferris's live layer keys URL owners by
URL and isolates transport per URL. With no `un_watch_*` method, retiring a feed
cannot be propagated upstream; only local book/cache clearing is possible while
the socket remains open. This differs from Extended, which also lacks unwatch
methods but uses per-channel URLs so its owners can be closed outright.

### 3.2 Order book defaults to the delta channel

`watch_order_book_for_symbols` defaults `channel` to `v1.book.d` (delta). The
`v1.book.s` snapshot channel is opt-in. In the delta handler, if the first
message is a delta, the freshly created book is updated against empty sides
before any snapshot resets it. The stock code comments acknowledge this and
force-assign `symbol` to mask a `symbol: null` first message.

### 3.3 Market `active` is not derived from the response

`parse_market` inserts `active = null`. The typed `Market::from_value` defaults a
missing/null `active` to `true`. The raw `all_instruments` response does not
appear to carry an active/delisted flag that stock reads. Ferris's
`includeInactive` therefore cannot distinguish inactive instruments from active
ones through stock metadata.

### 3.4 Timestamps are nanoseconds and are not uniformly parsed

- Ticker `event_time`, `next_funding_time`, trade `event_time`, candle
  `open_time`/`close_time` are **nanoseconds** (`parse_ticker`, `parse_trade`,
  `parse_ohlcv` divide by 1e6 for unified ms fields).
- `fetch_order_book` builds its timestamp via `parse8601(result.event_time)`,
  i.e. it treats the nanosecond string as an ISO-8601 input rather than dividing
  by 1e6.
- Ferris's `exchange_time` helper only reads `info.time` / `info.closeTime`; the
  GRVT ticker uses `event_time`, so `exchangeTimestamp` would resolve null
  without a GRVT-specific read.
- Ferris's `funding_field` `next_payment_key` reads via `positive_integer`, which
  would treat the nanosecond `next_funding_time` as milliseconds.

### 3.5 24h volume is buy-side only in the unified ticker

`parse_ticker` maps `baseVolume = buy_volume_24h_b` and
`quoteVolume = buy_volume_24h_q`. Sell-side volume exists only in `info`
(`sell_volume_24h_b`, `sell_volume_24h_q`). The unified ticker value is one side
of the market, not total 24h volume. Ferris's `volume_field` reads a single base
member and a single quote member and cannot sum two members.

### 3.6 Funding rate unit is not established by the source

The ticker example values are `funding_rate_8h_curr: "0.0037"`,
`funding_rate: "0.0037"` in one sample and `"0.01"` in another. Nothing in the
stock parser declares whether this is a decimal fraction or a percent. Ferris
publishes an explicit `rateUnit` (`decimalFraction` or `percent`) per venue, so
the unit must be qualified against the venue before any value is published.

### 3.7 Per-market ticker cost under the current statistics design

GRVT's rate limit is `rateLimit: 10` (10 ms per request) with `enableRateLimit`.
The singular `fetch_ticker` is one request per instrument. A GRVT catalog is
perpetuals only and can exceed one hundred instruments, so an all-market poll
built from singular calls is one rate-limited request per instrument per poll.
The bulk WS `watch_tickers` alternative requires a non-null symbols array; it
does not accept `null` for an aggregate subscription the way Lighter's
`watch_tickers(null)` does, so it cannot reuse the Lighter live-statistics path
unchanged.

### 3.8 REST book depth rounding and ceiling

`fetch_order_book` rounds the requested limit up to the nearest of
`[10, 50, 100, 500]` and only sets `depth` when the resolved value is `<= 500`.
Requests above 500 are sent without a depth parameter. Ferris enforces its own
per-venue REST/WS depth maxima and over-depth rejection; the GRVT ceiling is 500.

### 3.9 Non-`PERPETUAL` kinds parse to a null type

`parse_market` leaves `type` null for any `kind` other than `PERPETUAL`. The
typed `Market::from_value` defaults a missing type to `"spot"`. Ferris's
`convert_market` rejects a market whose type is not consistently
`spot`/`swap`/`future`/`option` with a matching boolean flag. Today the GRVT
catalog is all `PERPETUAL`, so this path is not exercised, but any non-perp kind
returned by the venue would fail catalog conversion.

### 3.10 Symbol and identity shape

- Native instrument ids are `BTC_USDT_Perp` (underscores, `_Perp` suffix).
- Unified CCXT symbol is `BTC/USDT:USDT` (base/quote:settle).
- `settle` is set from the quote id, so settlement is the quote asset.
- `contractSize` is `1`; `limits.amount.min` is `min_size`;
  `limits.amount.max` is `max_position_size`; `limits.cost.min` is
  `min_notional`; `precision.price` is `tick_size`; `precision.amount` is
  `min_size`; `precision.base`/`quote` are derived from `base_decimals` /
  `quote_decimals`.
- `created` is `create_time` (ns → ms).

### 3.11 Currencies load during market load

`has.fetchCurrencies` is `true`, so the stock `load_markets` path fetches
currencies (`public_market_post_full_v1_currency`). This is a second public
request per metadata load, not a markets-only call.

### 3.12 REST host and WS host are separate

- REST public market: `https://market-data.grvt.io/`
- WS public market: `wss://market-data.grvt.io/ws/full`
- Private REST (edge): `https://edge.grvt.io/`
- Private trading REST: `https://trades.grvt.io/`

Ferris's existing venues configure a small set of URL overrides per venue;
GRVT's public REST and WS are on the same host but distinct paths, and there are
additional private hosts that are unused by public market data.
