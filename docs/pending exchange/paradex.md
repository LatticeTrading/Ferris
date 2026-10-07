# Paradex — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/paradex.rs` (REST, ~4990 lines)
- `ccxt-base-4.5.85/src/exchanges/paradex_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/paradex.rs` (WebSocket, ~1100 lines)
- `ccxt-4.5.85/src/exchanges/paradex_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/paradex_typed.rs`

Live public API observations were taken against `https://api.prod.paradex.trade`
during this pass; they are marked "live" below and are point-in-time, not
guaranteed.

Paradex is already a compiled provider in the pinned release: the `paradex`
cargo feature exists on `ccxt-base` (`paradex = []`), `ccxt` (`paradex =
["ccxt-base/paradex"]`) and `ccxt-pro` (`paradex = ["ccxt-base/paradex"]`), and
`ccxt::Paradex` / `ccxt_pro::Paradex` are exported. Public id in CCXT is
`paradex`. `rateLimit` is 50 (ms), `precisionMode` is `TICK_SIZE`, `dex = true`,
`pro = true`, `has.sandbox = true`.

---

## 1. Live venue shape (observed)

`GET https://api.prod.paradex.trade/v1/markets` returned **8,247 markets**
(~12.4 MB):

| `asset_kind` | Count |
| --- | --- |
| `OPTION` | 8,182 |
| `PERP` | 63 |
| `SPOT` | 2 |

- **No futures / delivery products.**
- Spot markets observed: `ETH-USD` and `DIME-USD`.
- Perp markets observed: e.g. `BTC-USD-PERP`, `ETH-USD-PERP`, `SOL-USD-PERP`.
- All observed perps carried `funding_period_hours: 8`.

`GET /v1/markets/summary?market=ALL` returned **8,248 rows** (~5.4 MB), one per
market plus a `USDC` row. Perp rows carry `mark_price`, `last_traded_price`,
`bid`/`ask` (+ sizes), `volume_24h`, `total_volume`, `created_at`,
`underlying_price`, `open_interest`, `funding_rate`, `price_change_rate_24h`.

---

## 2. What stock CCXT has a working path for

These map onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. `public_get_markets` → `/v1/markets`, one call, `results` array. |
| `fetchTrades` (REST) | `fetch_trades` | Works. `public_get_trades` → `/v1/trades?market=`. `page_size` clamped to `min(limit, 1000)`; `since` → `start_at`; `params.until` → `end_at`. Cursor pagination (`next`) exists via `params.paginate`. |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works. `public_get_orderbook_market` → `/v1/orderbook/{market}`. `limit` → `depth`. Response has `seq_no` (nonce) and `last_updated_at`. |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works. `public_get_markets_klines` → `/v1/markets/klines`. Sets `resolution`, `symbol`, and always `start_at`/`end_at`. `params.price` → `price_kind` (`last`/`mark`/`index`). |
| Statistics tickers | `fetch_tickers` (bulk) | Works. `public_get_markets_summary` with `market=ALL` in one call; symbols are filtered client-side. |
| Statistics funding | `fetch_funding_rates` (bulk) | Works. Same `markets/summary` call; `market=ALL` unless exactly one symbol. |
| Statistics open interest | `fetch_open_interest` (singular) | Works via `markets/summary`, but only the singular method exists (see §3.2). OI is also present in the bulk ticker `info`. |
| `/v1/ws` trades | Pro `watch_trades` | Works. Hash `trades.{nativeId}` (or `trades.ALL` when no symbol). |
| `/v1/ws` order book | Pro `watch_order_book` | Works. Hash `order_book.{nativeId}.snapshot@15@100ms`. |
| WS tickers / funding | Pro `watch_tickers`, `watch_funding_rates` | Methods exist (see §3.6 for the aggregate-hash caveat). |

### Timeframes

Pin `timeframes`: `1m, 3m, 5m, 15m, 30m, 1h` only.

Live `markets/klines` confirms the server-side allow-list: `resolution must be
one of [1 3 5 15 30 60]`. No `4h`, `1d`, `1w`, `1M`.

### WS hashes

- trades: `trades.{nativeId}` / `trades.ALL`
- order book: `order_book.{nativeId}.snapshot@15@100ms`
- tickers: `markets_summary.{unifiedSymbol}`
- funding: `funding_data.{unifiedSymbol}`

---

## 3. What does not work / is absent (upstream gaps in the pin)

### 3.1 No live OHLCV / candles

Pro `describe` sets `watchOHLCV = false` and the Pro module implements no
`watch_ohlcv`. There is no candle subscription channel. REST `fetchOHLCV` exists;
WS candles do not.

### 3.2 No plural open-interest or mark/index bulk methods

`has` does not include `fetchOpenInterests`, `fetchMarkPrices`,
`fetchIndexPrices`, or `fetchFundingIntervals`. Only the singular
`fetch_open_interest` and `fetch_funding_rate` exist (plus the plural
`fetch_funding_rates`).

### 3.3 No bulk order book, trades, or candle endpoint

Markets and `markets/summary` are bulk. Order book, trades, and klines are
**per-market only**. Live endpoints confirm single-market scoping
(`/v1/orderbook/{market}`, `/v1/trades?market=`, `/v1/markets/klines?symbol=`).

### 3.4 No Pro `un_watch_*` methods

The Pro module implements no `un_watch_trades`, `un_watch_order_book`,
`un_watch_tickers`, or `un_watch_funding_rates`. Combined with a single shared
public WS URL, there is no upstream unsubscribe path.

### 3.5 Single shared public WebSocket URL

Public WS is one URL for every channel: `wss://ws.api.prod.paradex.trade/v1`
(testnet `wss://ws.api.testnet.paradex.trade/v1`). REST is
`https://api.prod.paradex.trade/v1` and `/v2` (testnet
`https://api.testnet.paradex.trade/v1` and `/v2`).

### 3.6 WS aggregate tickers/funding hash unverified

`watch_tickers(null)` and `watch_funding_rates(null)` append the bare channel
hash (`markets_summary` / `funding_data`) rather than a per-symbol hash, so the
stock code path permits an aggregate subscription. Whether the venue actually
accepts a symbol-less `markets_summary` / `funding_data` subscribe was not
verified live.

---

## 4. Findings that may hinder integration

### 4.1 SPOT is misclassified as a perpetual by the pin (live venue has spot)

`parse_market` only special-cases `asset_kind == "PERP_OPTION"` and `"OPTION"`.
Every other kind (including live `"SPOT"`) is assigned `type = "swap"`,
`swap = true`, `spot = false`, `contract = true`, `linear = true`,
`inverse = false`, and the unified symbol is built as `base/quote:settle`
(e.g. `ETH/USD:USDC`), with `settle` from `settlement_currency` (USDC).

Consequences in Ferris terms: live spot rows (`ETH-USD`, `DIME-USD`) would
appear as USDC-settled perpetuals. `has.spot` is also `false` in the pin even
though the live venue lists spot.

The same `parse_market` code is present in `ccxt-base-4.5.84`.

### 4.2 OPTION is parsed even though `has.option = false`

Live `asset_kind == "OPTION"` parses to `type = "option"`, `option = true`, with
an expiry/strike/`C`/`P` symbol suffix. The pin's `has.option` flag is `false`,
and there is no `has` support for options generally, yet `fetch_markets` returns
them. `fetchOHLCV` and other unified methods are not option-aware in the pin.

### 4.3 Options dominate the catalog and the metadata payload

8,182 of 8,247 markets are options, and the `/v1/markets` payload is ~12.4 MB.
The bulk summary is ~5.4 MB for 8,248 rows. Any per-poll or per-refresh fan-out
over the loaded catalog touches this volume unless filtered.

### 4.4 `active` is never populated

Live `/v1/markets` rows have **no** `enableTrading` field, and `parse_market`
reads `active` only from `enableTrading` (`safe_bool(...)` → null). The typed
`Market::from_value` defaults a missing/null `active` to `true`, so every market
is reported active and `includeInactive` cannot distinguish delisted markets.
`/v1/markets/history` carries `CREATED`/`DELISTED` events, but the stock parser
does not read it.

### 4.5 Ticker field coverage is partial in the unified structure

`parse_ticker` sets `last`, `close`, `bid`, `ask`, `quoteVolume` (from
`volume_24h`), `percentage`, and `markPrice`. It does **not** set `indexPrice`,
`openInterest`, `baseVolume`, `high`, `low`, `open`, `change`, or `vwap` (all
null). Index (`underlying_price`) and open interest (`open_interest`) exist only
in the raw `info` or via `parse_funding_rate` / `parse_open_interest`.

### 4.6 Volume denomination is not established by the source

`volume_24h` is mapped to `quoteVolume`, `baseVolume` is null. Live values did
not make the denomination unambiguous across products (e.g. `ETH-USD` spot
`volume_24h = 135.744`, `BTC-USD-PERP` `volume_24h = 1596089.5`), and there is
no separate base-volume field in the row. Nothing in the pin declares base vs
quote.

### 4.7 No next-funding timestamp

`markets/summary` has no next-funding time. `parse_funding_rate` sets
`fundingTimestamp`, `nextFundingTimestamp`, `fundingDatetime`,
`nextFundingDatetime`, and the `previous*` fields all null. The only time-like
field is `created_at` (observation time). The WS `funding_data` frame has
`created_at` + `funding_period_hours` but also no next time.

### 4.8 Funding interval is available

Perp `/v1/markets` rows carry `funding_period_hours` (live: 8 for all 63
perps). `parse_funding_rate` reads it from `market.info.funding_period_hours`
and emits `interval` as `"{hours}h"` when `> 0`. The WS `funding_data` frame
also carries `funding_period_hours`. There is no `fetchFundingIntervals`
method; the value comes from market metadata and the funding frame.

### 4.9 Funding-rate unit is not declared by the source

`parse_funding_rate` passes `funding_rate` through as a number without declaring
a unit. Live perp values were small decimals (e.g. `0.00004860053381`,
`0.00009179469467`). Nothing in the pin states decimal-fraction vs percent.

### 4.10 Open interest is an amount only

`parse_open_interest` sets `openInterestAmount` from `open_interest` and leaves
`openInterestValue` null. Live OI values are plain amounts (e.g. `162.272`,
`1.888`). There is no notional/value field in the row.

### 4.11 `min_order_size` is null

`parse_market` sets `limits.amount.min = null` and puts the venue minimum in
`limits.cost.min` (`min_notional`). `limits.amount.max` is `max_order_size`.
Ferris reads `min_order_size` from `limits.amount.min` only.

### 4.12 Quote and settlement naming

Perps and spot both have `quote_currency = "USD"` and
`settlement_currency = "USDC"`. Perps display as `BTC/USD` with `settle = USDC`;
spot rows would (per §4.1) also build `base/USD:USDC`. Spot and perp display
symbols collide on the same `BASE/USD` pair. `contractSize` is `1`.
`precision.price` is `price_tick_size`; `precision.amount` is
`order_size_increment`.

### 4.13 Mark/index candles are flagged supported but use a distinct param

`has.fetchMarkOHLCV` and `has.fetchIndexOHLCV` are `true`; `fetchPremiumIndexOHLCV`
is `false`. The candle source is selected through `params.price` →
`price_kind`, with documented values `"last"`, `"mark"`, `"index"`. This differs
from GRVT's `params.priceType` and from Extended's `candleType`.

### 4.14 Klines require an end bound

Live `/v1/markets/klines` rejects a request missing `end_at`
(`"end_at":"cannot be blank"`). The stock `fetch_ohlcv` always computes an
`end_at` (from `until`, or from `since + limit*duration`, or `now`), so the
stock path satisfies this, but the raw endpoint requires it.

### 4.15 Trades pagination and window

`fetch_trades` clamps `page_size` to `min(limit, 1000)` and attaches the
response `next` cursor to each parsed trade's `info`. The stock method only
paginates when `params.paginate` is true (cursor `next`, default page 100).
Without pagination, the caller gets one recent window plus a cursor in `info`.
`params.until` is forwarded to `end_at`; `since` to `start_at`.

### 4.16 REST order-book depth is passed through unbounded

`fetch_order_book` sets `depth = limit` when a limit is supplied and declares no
maximum. Live `/v1/orderbook/{market}` returned `depth=5` correctly, but no
source-side ceiling was observed or declared. Ferris enforces its own per-venue
REST/WS depth maxima.

### 4.17 WS order-book handler processes only `inserts`

`handle_order_book` reads `inserts`, builds a snapshot, and calls
`orderbook.reset(snapshot)`. It does not read `updates` or `deletes`. The
channel is named `...snapshot@15@100ms` and the sampled frame had
`update_type: "s"`, so it may be snapshot-only, but a delta frame would not be
applied to a maintained book.

### 4.18 Rate limit and connection budget

`rateLimit` is 50 ms with `enableRateLimit`. Order book, trades, and klines are
one request per market, so a many-symbol frontend fan-out is
one rate-limited request per market per operation. Markets and summary are one
request each.

### 4.19 Symbol / identity shape

- Native ids: `BTC-USD-PERP`, `ETH-USD`, options `BASE-USD-DDMMMYY-STRIKE-C|P`.
- Unified CCXT symbol (non-option): `BASE/USD:USDC`.
- `settle` from `settlement_currency` (USDC); `base`/`quote` from
  `base_currency`/`quote_currency`.
- Options get a `-YYMMDD-STRIKE-C|P` suffix and `expiry`/`strike`/`optionType`.
- Bare-base aliases are not added by the pin.

### 4.20 Taker/maker fees are hardcoded in `parse_market`

`parse_market` sets `taker = 0.0003` and `maker = -0.00005` as literals, not
from the response (live rows carry a `fee_config` object with per-market fees).
Options override `maker` to `0.0003`. `fetch_trading_fee(s)` reads `fee_config`
separately.

### 4.21 `fetchMarkets` is a single call, but not a light one

`fetch_markets` issues exactly one `public_get_markets` call and parses the
`results` array. There is no pagination. The cost is the ~12.4 MB payload and
8,247-row parse, not request count.

### 4.22 Authenticated methods exist but public market data is credential-free

`requiredCredentials` marks wallet/private key, and there are many private
endpoints (orders, positions, fills, account, transfers, vaults, staking, XP,
RFQs). All audited public market-data methods (`fetch_markets`, `fetch_tickers`,
`fetch_funding_rates`, `fetch_ohlcv`, `fetch_order_book`, `fetch_trades`,
`fetch_open_interest`, `fetch_time`, `fetch_status`) are credential-free.
`fetch_markets` only calls `sign_in` when credentials are present.
