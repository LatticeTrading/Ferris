# Derive — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/derive.rs` (REST, ~4000 lines)
- `ccxt-base-4.5.85/src/exchanges/derive_api.rs` (implicit endpoint wrappers)
- `ccxt-pro-4.5.85/src/pro/derive.rs` (WebSocket, ~1260 lines)
- `ccxt-4.5.85/src/exchanges/derive_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/derive_typed.rs`

Derive is already a compiled provider in the pinned release: the `derive` cargo
feature exists on both `ccxt-base` and `ccxt-pro`, and `ccxt::Derive` /
`ccxt_pro::Derive` are exported. Public id in CCXT is `derive`.

Derive is a perpetuals/options DEX built on the Lyra API. `has.spot`,
`has.swap`, and `has.option` are all `true`; `has.future` and `has.margin` are
`false`. Settlement is USDC for contracts; perpetual quote is USD. It is a DEX
(`dex = true`, `pro = true`, `certified = false`).

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | Works. Three separate POSTs to `public_get_all_instruments` (`instrument_type` = `erc20`, `perp`, `option`), each with `expired: false`. `has.fetchCurrencies = true`, so `load_markets` also performs a GET `get_all_currencies` before the three calls. |
| `fetchTrades` (REST) | `fetch_trades` | Works via `public_post_get_trade_history`. `page_size` capped at 1000. `since` → `from_timestamp`, `until` → `to_timestamp`. **Taker-only** (see §3.6). |
| Statistics ticker (singular) | `fetch_ticker` | Works via `public_post_get_ticker`. `last` is null (see §3.4). |
| Statistics funding (current) | `fetch_funding_rate` | Implemented in terms of `fetch_funding_rate_history` with `limit = 1`; it is not read from the instrument's live `perp_details.funding_rate`. |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock via `public_post_get_funding_rate_history`; not currently exposed by any Ferris endpoint. |
| `fetchCurrencies` | `fetch_currencies` | Exists in stock (`has.fetchCurrencies = true`); called during market load. |
| `fetchTime` | `fetch_time` | Exists in stock via `public_post_get_time`. |
| `/v1/ws` trades | Pro `watch_trades` | Works. Topic `trades.{instrument_name}`. |
| `/v1/ws` order book | Pro `watch_order_book` | Works. Topic `orderbook.{instrument_name}.10.{limit}`. Snapshot-only (see §3.3). |
| `/v1/ws` ticker | Pro `watch_ticker` | Works. Topic `ticker_slim.{instrument_name}.100`. |

All public market-data calls are unauthenticated. `fetch_markets` only signs when
credentials are present; the public REST `sign()` path needs no wallet.

### Timeframes

`timeframes` exposes: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d, 1w,
1M`.

`6h`, `5d`, `2w`, `3w`, `4w` are **not** present.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No REST `fetchOrderBook`

`has.fetchOrderBook = false`, and the entire `api.public.post` block contains no
orderbook path. Derive's public REST API exposes **no order book endpoint at
all**. There is no raw endpoint to fall back to.

### 2.2 No REST `fetchOHLCV`

`has.fetchOHLCV = false`; there is no `fetch_ohlcv` method and no `parse_ohlcv`
in the base `DeriveCore`. Raw candle endpoints do exist but are unparsed:
`public_post_get_tradingview_chart_data`, `public_post_get_spot_feed_history_candles`,
`public_post_get_index_chart_data`, `public_post_get_spot_feed_history`.

### 2.3 No bulk `fetchTickers`

`has.fetchTickers = false` and no plural `fetch_tickers` is implemented. A raw
`public_post_get_tickers` endpoint exists (wrapper present) but is never called
or parsed by stock code.

### 2.4 No `watchOHLCV`

The Pro describe sets `watchOHLCV = false`; no `watch_ohlcv` method exists. No
REST-polling fallback is present in stock.

### 2.5 No `watchTickers`

The Pro describe sets `watchTickers = false`. There is no aggregate ticker
subscription; only per-market `watch_ticker`.

### 2.6 No `fetchFundingRates` (plural)

`has.fetchFundingRates = false`; no bulk funding method. Only singular
`fetch_funding_rate` / `fetch_funding_rate_history`.

### 2.7 No `fetchFundingIntervals`

No such method. No interval field is present on `perp_details` (see §3.7).

### 2.8 No `fetchOpenInterest` / `fetchOpenInterests` / `fetchOpenInterestHistory`

All three are `false`. Open interest exists only as a raw field inside the
singular REST ticker `stats.open_interest` (see §3.8).

### 2.9 No `fetchMarkPrices` / `fetchIndexPrices`

No such methods. Mark and index exist on the singular ticker as `mark_price` /
`index_price` (REST) and `M` / `I` (WS slim). A raw
`public_post_get_latest_signed_feeds` endpoint exists but is unparsed.

### 2.10 No `un_watch_ticker`

`un_watch_order_book` and `un_watch_trades` exist, but there is no
`un_watch_ticker`. The ticker feed has no upstream unsubscribe path.

### 2.11 No liquidations

`has.fetchLiquidations` and `has.fetchMyLiquidations` are `false`. A raw
`public_post_get_liquidation_history` endpoint exists but is unparsed.

### 2.12 No dated futures

`has.future = false`. `parse_market` only maps `instrument_type` values `erc20`
(spot), `perp`, and `option`. Any other kind would leave `type` null.

### 2.13 `has.ws` is `false` while watch methods exist

The Pro describe inserts `ws = false` alongside working `watchTrades` /
`watchOrderBook` / `watchTicker`. This is a stock quirk; the watch methods and
`urls.api.ws` are present and used.

---

## 3. Findings that may hinder integration

### 3.1 No REST order book is a hard product gap

There is no order book endpoint on the public REST API, so a REST order book
snapshot cannot be produced from stock CCXT at all. Only the WebSocket book
exists. This differs from every currently supported venue, all of which have a
REST `fetch_order_book`.

### 3.2 REST candles would require an unparsed raw endpoint

The unified `fetchOHLCV` path is absent. The only candle data is behind raw
endpoints (`get_tradingview_chart_data`, `get_spot_feed_history_candles`,
`get_index_chart_data`) with no stock request builder or response parser. The
request parameters (instrument name, interval, from/to) and response shape are
not defined by any stock method.

### 3.3 WS order book is snapshot-only and encodes depth in the topic

`handle_order_book` calls `orderbook.reset(snapshot)` on every message. There are
no diffs, no checksums, and no sequence/nonce. The subscription topic is
`orderbook.{instrument_name}.10.{limit}`; `limit` defaults to 10 when null. The
middle `.10.` segment is fixed in stock code (appears to be a grouping
parameter, not a Ferris-style depth). The requested depth is part of the
subscription identity, so different depths are different topics; there is no
separate deeper backing state.

### 3.4 Ticker `lastPrice` is unavailable

REST `parse_ticker` sets `last = null` (and `open`, `close`, `vwap`,
`previousClose`, `average` null). The WS `ticker_slim` handler maps `b`/`B`
(bid/bidVolume), `a`/`A` (ask/askVolume), `stats.h`/`l` (high/low), `stats.c`/`v`
(volumes), `stats.p` (percentage), `M` (mark), `I` (index) — and no last-trade
price. There is no last price on either transport.

### 3.5 Unified REST ticker volume is null; WS slim volume is populated

REST `parse_ticker` sets `baseVolume = null` and `quoteVolume = null`. The WS
`ticker_slim` handler sets `baseVolume = stats.c` and `quoteVolume = stats.v`.
REST and WS therefore disagree on whether 24h volume exists. The raw REST ticker
`stats` object does carry `contract_volume` and `num_trades`, but the stock parser
does not map them to volume.

### 3.6 REST `fetchTrades` drops maker rows

`parse_trades` computes `isFetchTrades = !rawTrade.contains_key("order_id")` and
`continue`s when `liquidity_role == "maker"`. The REST public trade tape is
therefore taker-side only, and `side` always reflects the taker. Realtime
`watch_trades` calls `parse_trade` directly and does **not** apply this filter, so
REST and WS trade tapes differ in contents.

### 3.7 Funding interval and next-payment metadata are absent

`perp_details` carries `index`, `max_rate_per_hour`, `min_rate_per_hour`,
`static_interest_rate`, `aggregate_funding`, and `funding_rate` — there is no
interval and no next-payment timestamp. `parse_funding_rate` sets `interval =
null` and all `nextFunding*` / `previousFunding*` fields null. The live perp
funding rate exists only as `perp_details.funding_rate` in raw ticker/instrument
`info`; the unified `parse_ticker` does not map it. `fetch_funding_rate` returns
a row from funding history, not the live perp rate.

### 3.8 Open interest exists only in the singular REST ticker raw info

There is no unified OI method. OI appears as `stats.open_interest` (alongside
`stats.contract_volume`) inside the REST `get_ticker` response `info`. The WS
`ticker_slim` payload does **not** carry open interest. The unit (base amount,
contracts, or USD value) is not declared by any stock parser.

### 3.9 Expired instruments can never be returned

`fetch_spot_markets`, `fetch_swap_markets`, and `fetch_option_markets` all
hardcode `{"expired": false}`. Expired/inactive instruments are excluded by the
request itself, so `includeInactive` cannot surface them through stock metadata.

### 3.10 Symbol and identity shape

- Perpetual: `symbol = BASE/USD:USDC`, `quote = USD`, `settle = USDC`,
  `linear = true`, `inverse = false`, `contractSize = 1`, `margin = false`.
- Spot (`erc20`): `symbol = BASE/QUOTE`, `settle = null`, `margin = true`,
  `linear`/`inverse` null.
- Option: `symbol = BASE/QUOTE:USDC-YYMMDD-STRIKE-C|P`, `settle = USDC`,
  `linear = true`, `inverse = false`, expiry/strike/`optionType` populated.
- Native `id` is `instrument_name` (e.g. `BTC-PERP`); `baseId`/`quoteId` are
  `base_currency`/`quote_currency`.
- `precision.price` is `tick_size`; `precision.amount` is `amount_step`;
  `limits.amount.min` is `minimum_amount`; `limits.amount.max` is
  `maximum_amount`.
- Perps quote **USD** but settle **USDC**, the same quote/settle split as
  Extended.

### 3.11 Rate limit and per-market ticker cost

`rateLimit` is `50` (50 ms per request) with `enableRateLimit`. The only
statistics source is the singular `fetch_ticker`, i.e. one request per
instrument. There is no bulk REST ticker and no aggregate WS ticker, so an
all-market poll is one rate-limited request per instrument per poll.

### 3.12 Single shared public WebSocket URL

All public channels (book, trades, ticker) share `wss://api.lyra.finance/ws`
(`urls.api.ws`). `un_watch_order_book` and `un_watch_trades` exist, but there is
no `un_watch_ticker`. Retiring a ticker feed cannot be propagated upstream while
the socket remains open.

### 3.13 `handle_trades_un_subscription` checks the wrong cache

`handle_trades_un_subscription` tests `in_op(&self.orderbooks, &symbol)` and then
removes from `self.trades`. If a symbol has a trades subscription but no book
subscription, the trades cache is not removed on unsubscribe. (The book handler
checks `self.orderbooks` and removes from `self.orderbooks` correctly.)

### 3.14 Currencies load during market load

`has.fetchCurrencies = true`, so stock `load_markets` issues a GET
`get_all_currencies` before the three `get_all_instruments` POSTs. A metadata
load is four public requests, not one.

### 3.15 Trade `cost` is null and fee currency is hardcoded

`parse_trade` sets `cost = null` and a fee object with `currency = "USDC"` and
`cost = trade_fee`. `cost` is not derived from price × amount, and the fee
currency is not resolved from market metadata.

### 3.16 REST trade history has no pagination

`fetch_trades` reads a single `result.trades` page and ignores the response
`pagination` object. `page_size` is capped at 1000. `since` requests return at
most one page; there is no deep history retrieval.

### 3.17 Ticker `change`/`percentage` are derived from `stats.percent_change`

REST `parse_ticker` sets `change = stats.percent_change` and `percentage =
change × 100`. WS slim sets `percentage = stats.p`. The two transports report
the field on different scales (REST `change` is the raw percent-change value,
`percentage` multiplies it by 100).

### 3.18 REST host, WS host, and demo host

- REST public: `https://api.lyra.finance/public`
- REST private: `https://api.lyra.finance/private`
- WS: `wss://api.lyra.finance/ws`
- Demo REST: `https://api-demo.lyra.finance/public` / `/private`
- Demo WS: `wss://api-demo.lyra.finance/ws`

Public REST and WS are on the same host but distinct paths.

---

## 4. Not verified against the live venue

The following are read from stock source only and were not exercised against the
live Derive API in this review: the raw candle endpoints' request/response
shapes, `get_tickers` payload contents, `get_latest_signed_feeds` contents,
open-interest units, funding-rate unit (decimal fraction vs percent), the WS book
maximum allowed depth and the meaning of the `.10.` topic segment, and whether
`get_all_instruments` can be called once without `instrument_type`.
