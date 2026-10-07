# KuCoin — new exchange support findings

Findings only. No implementation recommendations and no code changes are made or
implied here.

Baseline inspected: `ccxt` / `ccxt-pro` / `ccxt-base` **4.5.85** (the exact pins
in `Cargo.toml`). Sources inspected:

- `ccxt-base-4.5.85/src/exchanges/kucoin.rs` (REST, ~15.5k lines)
- `ccxt-base-4.5.85/src/exchanges/kucoin_api.rs` (implicit endpoint wrappers)
- `ccxt-base-4.5.85/src/exchanges/kucoinfutures.rs` (contract-only wrapper)
- `ccxt-pro-4.5.85/src/pro/kucoin.rs` (WebSocket, ~4.6k lines)
- `ccxt-pro-4.5.85/src/pro/kucoinfutures.rs`
- `ccxt-4.5.85/src/exchanges/kucoin_typed.rs`, `ccxt-pro-4.5.85/src/pro_typed/kucoin_typed.rs`

KuCoin is already a compiled provider in the pinned release: the `kucoin` and
`kucoinfutures` cargo features exist on both `ccxt-base` and `ccxt-pro`, and
`ccxt::Kucoin` / `ccxt_pro::Kucoin` are exported. Public id in CCXT is `kucoin`.

Two classes exist: `Kucoin` and `Kucoinfutures`. `KucoinfuturesCore` wraps a
`KucoinCore` and only changes defaults (`defaultType = "swap"`,
`types = [swap, future, contract]`); every unified market-data method is
inherited from the same parent core. KuCoin is therefore a single venue with
spot **and** contracts in one `loadMarkets`.

---

## 1. What should be supported (stock CCXT has a working path)

These map directly onto existing Ferris endpoints/operations.

| Ferris operation | Stock CCXT method | Notes |
| --- | --- | --- |
| `fetchMarkets` | `fetch_markets` | One call loads spot + swap + future + contract (`fetchSpotMarkets` + `fetch_contract_markets`). `has.spot/swap/future = true`, `has.option = false`. |
| `fetchTrades` (REST) | `fetch_trades` | Works per product, but ignores `since`/`limit` (see §3.4). |
| `fetchOrderBook` (REST) | `fetch_order_book` | Works; limit must be 20 or 100 for spot, 20/100/full for contracts (see §3.5). |
| `fetchOHLCV` (REST) | `fetch_ohlcv` | Works; contract max 200, spot/UTA max 1500 (see §3.9). |
| `/v1/ws` trades | Pro `watch_trades` / `watch_trades_for_symbols` | Works. Hash `trades:{symbol}`. |
| `/v1/ws` order book | Pro `watch_order_book` / `watch_order_book_for_symbols` | Works. Hash `orderbook:{symbol}`. Limit selector 5/20/50/100 (see §3.6). |
| `/v1/ws` candles | Pro `watch_ohlcv` / `watch_ohlcv_for_symbols` | Works. Hash `candles:{symbol}:{timeframe}`. |
| Statistics ticker fields | REST `fetch_tickers` (bulk, split by product) | Contract bulk ticker carries funding/mark/index/OI/volume/last in `info`; spot bulk ticker carries last/volume only (see §3.3, §3.11). |
| Bulk funding rates | REST `fetch_funding_rates(null)` | Works, public UTA v2 `market/funding-rate`, all contracts (see §3.13). |
| Bulk open interest | REST `fetch_open_interests(null)` | Works, public UTA `market/open-interest` (see §3.14). |
| Bulk mark prices | REST `fetch_mark_prices(null)` | Works but is the **spot/margin** mark-price endpoint, not contract mark price (see §3.21). |
| `fetchFundingRateHistory` | `fetch_funding_rate_history` | Exists in stock; not currently exposed by any Ferris endpoint. |

`precisionMode = TICK_SIZE` for KuCoin, so precision is already expressed as a
tick.

### Timeframes

Top-level map advertises `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d,
1w, 1M`. The nested contract map additionally nulls `3m` and `6h` (see §3.7).

### Public access

`fetch_markets`, `fetch_tickers`, `fetch_funding_rates`, `fetch_open_interests`,
`fetch_mark_prices`, `fetch_order_book`, `fetch_ohlcv`, and `fetch_trades` do not
require credentials for the non-UTA paths. `fetch_currencies` calls
`is_uta_enabled` only when credentials are present, so the public path is
unauthenticated.

---

## 2. What does not work (no stock method exists)

These are upstream gaps in the pinned CCXT release, not Ferris limitations.

### 2.1 No plural `fetchFundingIntervals`

Only the singular `fetch_funding_interval` is implemented
(`has.fetchFundingInterval = true`). There is no `fetch_funding_intervals` on
KuCoin (contrast Binance/Aster, which have plural bulk interval methods). The
funding interval is only obtainable from the bulk `fetch_funding_rates` payload
(`info.currentGranularity` / `info.newGranularity`, ms) or from the singular
interval method.

### 2.2 No `fetchIndexPrice` / `fetchIndexPrices`

No index-price method exists. Index price appears only as `indexPrice` on the
contract ticker and inside contract market metadata / funding payloads.

### 2.3 Singular `fetchOpenInterest` is emulated

KuCoin implements `fetch_open_interests` (plural) but not `fetch_open_interest`
(singular). `has.fetchOpenInterest = true`, so the base emulation calls the plural
method with a one-symbol list. There is no dedicated singular endpoint call.

### 2.4 No plural `un_watch_tickers`

Pro exposes `un_watch_ticker` (singular) but not `un_watch_tickers`. It also
exposes `un_watch_trades`, `un_watch_trades_for_symbols`, `un_watch_order_book`,
`un_watch_order_book_for_symbols`, `un_watch_ohlcv`, `un_watch_funding_rate`, and
`un_watch_mark_price`.

### 2.5 Options are unsupported

`has.option = false`. `fetch_markets` parses no options; `fetch_contract_markets`
maps only swaps and dated futures.

### 2.6 No single bulk ticker spanning spot and contracts

`fetch_tickers` dispatches on market type: spot/margin goes to
`public_get_market_all_tickers`, everything else goes to
`fetch_contract_tickers` (`futures_public_get_contracts_active`). A single call
never returns both products (see §3.3).

### 2.7 Candle price source is UTA-only

`fetch_ohlcv` reads `params.price` (`mark | index | premiumIndex`) and, when set,
forces the UTA path (`fetch_uta_ohlcv`). Non-UTA spot/contract OHLCV have no
mark/index/premium parameter. There is no `candleType` equivalent to Extended.

### 2.8 No `until` upper bound on spot/contract OHLCV

`fetch_spot_ohlcv`, `fetch_contract_ohlcv`, and `fetch_uta_ohlcv` compute `endAt`
from `now` or `since + limit × duration`; they do not read a user `until`. A
`until` parameter only appears in order/funding/position/OI-history methods, not
in `fetch_ohlcv`.

---

## 3. Findings that may hinder integration

### 3.1 Public WebSocket URL is token-negotiated at runtime

Every non-UTA Pro `watch_*` calls `negotiate()`. That method reads
`options.urls[connectId]` (connectId `public` or `publicFutures`); if absent it
POSTs to `public_post_bullet_public` / `futures_public_post_bullet_public`,
takes `data.instanceServers[0].endpoint`, and builds
`{endpoint}?token=...&privateChannel=false&connectId=public[Futures]`. The
resulting URL is cached in `options.urls[connectId]` and passed to `ws_run`.

Consequences for Ferris's live layer, which keys URL owners by the exact URL
string and drives `ws_run(spec.url, hashes)`:

- The URL is not a static config value; it depends on a runtime bullet response.
- Different tokens/endpoints per negotiation mean different registry keys unless
  the same cached URL is reused.
- `LiveProvider::new` builds a fresh core each session, so `options.urls` starts
  empty every reconnect; the cached token URL does not survive a session restart.
- The bullet token has an expiry that is not represented in Ferris's owner
  lifecycle.

This is the largest difference from every currently supported venue. Extended
and Lighter use static or per-channel URLs; Binance uses real static URLs with an
allocator. KuCoin is the first venue whose public WS URL must be obtained from a
REST negotiation first.

### 3.2 Ferris's `urls.api` override does not affect KuCoin WS

`ProviderConfig::new` writes `value["urls"]["api"]`. KuCoin Pro `negotiate` reads
`self.options["urls"]`, a different path. The stock WS URL comes from the bullet
response endpoint, not `urls.api.ws`. Loopback fixtures that override
`urls.api.ws` (as the Binance/Bybit/Aster/Hyperliquid tests do) would not redirect
KuCoin's WebSocket; a fixture would have to intercept the bullet REST calls.

### 3.3 Bulk tickers are split by product

- Spot/margin: `public_get_market_all_tickers` (`/market/allTickers`, cost 15),
  parsed by `parse_spot_or_uta_ticker`. Fields: `last`, `vol`/`baseVolume`,
  `volValue`/`quoteVolume`, `high`, `low`, `buy`/`sell`, `changeRate`.
- Contracts: `futures_public_get_contracts_active` (`contracts/active`, cost 6)
  by default, or `futures_public_get_all_tickers` via `params.method`, parsed by
  `parse_contract_ticker`. Fields: `lastTradePrice`, `volumeOf24h`,
  `turnoverOf24h`, `markPrice`, `indexPrice`, plus `fundingFeeRate`,
  `predictedFundingFeeRate`, `openInterest`, `nextFundingRateTime`,
  `fundingRateGranularity`, `highPrice`, `lowPrice`, `priceChg`.

Which product is returned depends on `handle_market_type_and_params`, i.e.
`params.type` or `options.defaultType`. With `defaultType = "swap"`,
`fetchTickers(null, {})` returns **only contracts**.

### 3.4 REST trades ignore `since` and `limit`

In `fetch_trades`, the `since`/`limit` request construction is commented out
("pagination is not supported on the exchange side anymore"). The method returns
the venue's recent window and then `parse_trades(tradesList, [market, since,
limit])`. Stock `filter_by_since_limit` is applied in the Pro watcher, not in the
REST method. Historical `since`-bounded REST trade queries are therefore not
supported; the endpoint is a recent-trades window.

### 3.5 REST order-book depth is tiered and rejects other values

`fetch_order_book` behavior by product:

- Contracts: `limit == null` → full L2 snapshot (`level2/snapshot`); `20` →
  `level2/depth20`; `100` → `level2/depth100`; any other non-null value panics
  `BadRequest` ("limit argument must be 20 or 100").
- Spot/margin: `limit == null` → defaults to 100; `20` or `100` accepted; any
  other value panics `ExchangeError`.
- `params.level` other than 2 for contracts panics `BadRequest`.

### 3.6 WS order-book depth and partial-depth channels

`watch_order_book_for_symbols` validates `limit` to `undefined | 5 | 20 | 50 |
100` (other values panic). For spot, `limit` 5 or 50 selects the partial-depth
streams `/spotMarket/level2Depth5` / `/spotMarket/level2Depth50`; 20/100 (or
undefined) use the incremental `/market/level2` channel. For contracts the method
is `/contractMarket/level2` regardless. The snapshot/delta maintenance is stock's.

### 3.7 Contract timeframe map nulls `3m` and `6h`

The top-level `timeframes` map advertises `3m` and `6h`, but the nested
`timeframes.swap` map sets `3m` and `6h` to null. `has_timeframe`-style checks
that read the top-level map would accept a timeframe the contract endpoint may
not support.

### 3.8 `fetch_markets` triggers extra public calls

- `fetchTickersFees` defaults to `true` (from `options.fetchMarkets`), so a
  spot market load also calls `/market/allTickers` to attach fee rates. It is
  skipped only when spot is not in the requested types.
- `has.fetchCurrencies = true`, and the stock `load_markets` path fetches
  currencies (`/currencies`, cost 3) as part of a metadata load. Public path does
  not require credentials.

So one Ferris catalog load is more than a markets-only request.

### 3.9 Contract OHLCV limit is much lower than spot

- `fetch_contract_ohlcv`: `maxLimit = 200`.
- `fetch_spot_ohlcv`: `maxLimit = 1500`.
- `fetch_uta_ohlcv`: `maxLimit = 1500`.

The `options.fetchOHLCV.limit` default is 1500. A single per-venue OHLCV max is
not accurate; it is product-dependent.

### 3.10 Product breadth of the contract path

`fetch_contract_markets` and `fetch_contract_tickers` both use
`futuresPublicGetContractsActive`, which returns **all** contracts (linear perps,
inverse perps, and dated futures) in one response. There is no separate
linear-vs-inverse fetch; a Linear or Inverse acquisition profile would still
receive every contract and would have to be filtered by catalog metadata.

### 3.11 Statistics payload shape

- Contract ticker (`parse_contract_ticker`) maps unified `last`, `baseVolume`,
  `quoteVolume`, `markPrice`, `indexPrice`. It does **not** map funding or open
  interest into unified fields; those are only in the raw `info` object
  (`fundingFeeRate`, `predictedFundingFeeRate`, `openInterest`,
  `fundingRateGranularity`, `nextFundingRateTime`).
- Spot ticker (`parse_spot_or_uta_ticker`) maps `last`, `baseVolume`,
  `quoteVolume`, `high`, `low`, `change`. No funding/mark/index/OI.
- `fetch_ticker` (singular) for spot uses `/market/stats`, while spot bulk
  `fetch_tickers` uses `/market/allTickers`; both parse through
  `parse_spot_or_uta_ticker`.

### 3.12 Funding rate unit is not declared by the source

`fundingFeeRate` / `predictedFundingFeeRate` appear as values like `0.000297` /
`0.000327`, and the UTA funding payload uses `nextFundingRate: -0.000004`. The
stock parser does not label these as a decimal fraction or a percent. Ferris
publishes an explicit `rateUnit` per venue, so the unit must be qualified against
the venue before publishing.

### 3.13 Funding interval is only in the bulk funding payload

Contract market metadata contains `fundingRateGranularity` only inside a code
comment; it is not parsed into the unified market. The bulk `fetch_funding_rates`
payload exposes `info.currentGranularity` / `info.newGranularity` (ms) and
`parse_funding_rate` maps `interval` via `parse_funding_interval`
(`3600000→1h, 14400000→4h, 28800000→8h, 57600000→16h, 86400000→24h`). The
singular `fetch_funding_interval` also exists.

### 3.14 Open-interest unit is unqualified

`parse_open_interest` sets `openInterestAmount = info.openInterest` with no
multiplier/contract-size conversion, and `openInterestValue = null`. The bulk
`fetch_open_interests` uses `uta_get_market_open_interest`. The contract bulk
ticker also exposes `openInterest` as a string. Nothing in the stock parser
declares whether the value is a contract/lot count or a base-asset quantity;
`multiplier` (contract size) is present in contract metadata but is not applied.

### 3.15 `nextFundingRateTime` is a countdown, not a timestamp

The contract ticker exposes `nextFundingRateTime: 20022985`, a duration in ms
until the next funding event, not an epoch timestamp. Any consumer expecting a
millisecond epoch `nextPaymentTimestamp` would misinterpret it. The UTA funding
payload instead uses `fundingTime` (epoch ms) and `newGranularityStartTime`.

### 3.16 Timestamp units are mixed

- Contract ticker `ts`: nanoseconds, scaled by `safe_integer_product_k(..., 0.000001)`.
- Spot ticker `time` / `datetime`: milliseconds.
- Trade `time` / `ts`: nanoseconds, divided by 1,000,000 in `parse_trade`.
- OHLCV native cell 0: seconds; `parse_ohlcv` calls `safe_timestamp` and also
  reorders the native `[time, open, close, high, low, volume]` array into
  `[ts, open, high, low, close, volume]`.
- Funding `fundingTime`, `timePoint`, `newGranularityStartTime`: milliseconds.

### 3.17 Rate limiting is weighted, not per-request

`rateLimit = 7.5` ms with `enableRateLimit`. Endpoint costs are weighted, e.g.
`/market/allTickers` 15, `market/stats` 15, `market/candles` 3,
`contracts/active` 6, UTA `market/ticker` 30, UTA `market/kline` 6, UTA
`market/funding-rate` 4, UTA `market/open-interest` 20. A bulk statistics poll
that issues a spot tickers call and a contract tickers call pays two weighted
requests per poll.

### 3.18 Symbol and identity shape

- Spot native id: `BTC-USDT` (hyphen); unified `BTC/USDT`.
- Linear perp native id: `XBTUSDTM`; unified `BTC/USDT:USDT`.
- Inverse perp native id: `XBTUSDM`; unified `BTC/USD:BTC`.
- Dated future: unified `BASE/QUOTE:SETTLE-YYMMDD`; `type = future`.
- Base asset `XBT` is mapped to `BTC` by CCXT's global common-currency map
  (`XBT → BTC`), so display is `BTC` while native ids keep `XBT`.
- `settle` comes from `settleCurrency`; `linear`/`inverse` come from
  `isInverse`. `contractSize` is `|multiplier|`.
- `active`: spot from `enableTrading`; contracts from `status == "Open"`.
- Precision: `precision.price = tickSize` / `priceIncrement`,
  `precision.amount = lotSize` / `baseIncrement`.

### 3.19 WS message hashes and unwatch convention

Result hashes differ by channel:

- order book: `orderbook:{symbol}`
- trades: `trades:{symbol}`
- candles: `candles:{symbol}:{timeframe}`
- tickers: `tickers` (aggregate) or `ticker:{symbol}` (per symbol)

Unwatch hashes are `unsubscribe:{subMessageHash}` (e.g.
`unsubscribe:trades:{symbol}`, `unsubscribe:orderbook:{symbol}`,
`unsubscribe:candles:{symbol}:{timeframe}`). UTA paths prefix `uta:`.

### 3.20 WS `watch_tickers` cannot subscribe spot+contracts together

`watch_tickers` picks the method from market type (`/market/ticker` for spot,
`/contractMarket/ticker` for contracts) and calls `negotiate()` with the matching
connectId. Passing `symbols = null` is allowed only for spot (`.../ticker:all`);
for contracts or UTA a null symbol list panics `ArgumentsRequired`. There is no
single aggregate subscription across both products.

### 3.21 `fetch_mark_prices` is the spot/margin mark-price endpoint

`fetch_mark_prices` calls `public_get_mark_price_all_symbols`
(`/mark-price/all-symbols`), which is the spot/margin mark price. Contract mark
price is in the contract ticker / `contracts/active` payload, not this method.

### 3.22 Pro trades return cache windows filtered by `since`/`limit`

`watch_trades_for_symbols` ends with
`filter_by_since_limit(trades, [since, limitResolved, "timestamp", true])`, where
`limitResolved` comes from the stock cache limit when `newUpdates` is set. The
returned slice is cache-derived, not a delta stream.

### 3.23 `Kucoinfutures` is a defaults-only wrapper

`KucoinfuturesCore` holds a `KucoinCore` and only overrides `defaultType` and
`types`; all market-data methods deref to the parent. It is not a separate
transport or product implementation.

### 3.24 REST hosts and WS host

- Spot REST public: `https://api.kucoin.com`
- Futures REST public: `https://api-futures.kucoin.com`
- UTA REST public: `https://api.kucoin.com` (path prefix `uta`)
- Pro WS: negotiated `instanceServers[0].endpoint` (bullet response); the
  `urls.api.ws` map defines `spot`, `futures`, and `private` placeholders
  (`wss://x-push-spot.kucoin.com`, `wss://x-push-futures.kucoin.com`,
  `wss://wsapi-push.kucoin.com`), but the non-UTA watch paths go through
  `negotiate()` rather than reading those static values directly.
