# Ferris Market Data Backend

Open-source Rust backend with stock CCXT REST snapshots and shared realtime market-data streams.

CCXT migration status and phase approvals live in `FERRIS_V2_PLAN.md`; broader product planning lives in `ROADMAP.md`.

Frontend integration guidance lives in [INTEGRATION_README.md](INTEGRATION_README.md).
For an existing client moving to the rewritten backend, start with
[FRONTEND_MIGRATION_GUIDE.md](FRONTEND_MIGRATION_GUIDE.md): endpoint examples,
response shapes, partial-statistics rendering, chart bootstrap, and current limits.

- unified endpoint shapes (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
- backend websocket fanout for realtime channels (`GET /v1/ws`)
- stock CCXT / CCXT Pro `4.5.85` snapshots, statistics, and realtime acquisition behind isolated Rust owners
- market-data support for `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, and `lighterxyz`, with product-specific capabilities
- shared upstream websocket topics for trades/books/candles; shared 30-second stock statistics polling plus maintained Lighter Pro `watchTickers`
- Hyperliquid public trades use the stock recent-trade window; no additional history collector/cache

## Why this exists

Frontend apps (including Electron and web frontends) often cannot directly use server-side exchange SDK behavior the same way backend code does. This service acts as a hostable bridge:

- you can run it for public users (for example on Hetzner)
- users can self-host their own backend
- your frontend uses one clean API contract regardless of exchange
- app clients subscribe to backend websocket topics instead of high-frequency HTTP polling for live updates

## Realtime Architecture

- app client connects once to `GET /v1/ws` and sends `subscribe` / `unsubscribe` commands
- trades/books/candles use exchange + channel + symbol (+ params) topics and shared upstream websocket streams
- statistics share one exchange/scope acquisition across all-market and selected projections
- clients receive topic-specific updates; statistics use ordered snapshots and revisioned deltas
- per-client order-book depth is serialized from borrowed slices of the owned shared book; rendering top-N does not clone or truncate the shared backing state
- shared topics retain 512 updates; client outgoing queues retain 256 messages, report broadcast lag, and close a full-queue client without blocking acquisition
- idle topics are closed, and dropped upstream streams reconnect with backoff

## Current scope

- Snapshot endpoints (bootstrap/fallback):
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
  - `POST /v1/fetchMarkets`
- `POST /v1/fetchMarketStats` (all six venues; qualified products and numeric metric units below)
- Capability discovery: `GET /v1/capabilities` (no upstream acquisition)
- Realtime endpoint:
  - `GET /v1/ws` (channels `trades`, `orderbook`, `ohlcv`, `marketstats`)
- Realtime channel support:
  - `trades`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, `lighterxyz`
  - `orderbook`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, `lighterxyz`
  - `ohlcv`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
  - `marketstats`: `hyperliquid`, `binance`, `bybit`, `extended`, `aster` (`sharedPolling`), `lighterxyz` (`sharedPollingAndWebSocket`)
- exchange supported:
  - `hyperliquid` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `binance` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `lighterxyz` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `bybit` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `aster` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `extended` perpetual public market data: all five snapshot endpoints plus realtime trades, order books, OHLCV, and marketstats
- Extended accepts `BASE-USD`, `BASE/USD`, and `BASE/USD:USD`. REST and realtime trade/book symbols now use stock `BASE/USDC:USDC`; the catalog display pair remains `BASE/USD`.
- Extended's standard websocket order book is indicative, not the RFQ real-book stream. Spot, private trading/account, funding history, and account streams are not supported.
- Extended REST and websocket URLs are configurable for testnet deployments.
- market-data only (no private trading endpoints yet)

## CCXT REST snapshot contract

All five snapshot endpoints use stock CCXT for the six registered venues. HTTP envelopes remain Ferris-owned. Trades/books/candles on `/v1/ws` use CCXT Pro; statistics use shared stock polling and Lighter Pro `watchTickers` with Ferris snapshot/revisioned-delta delivery.

Real HTTP snapshots were exercised for all six venues. Extended's missing-User-Agent HTTP 403 is fixed by an Extended-only `Ferris/1.0` header on stock HTTP clients; live catalog, statistics, trades, candles, and book snapshots passed. Its stock WebSocket connector still omits headers and receives 403; see [CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004). There is no native REST fallback.

### Selection and response differences

- `exchange` still defaults to `hyperliquid`. Binance defaults to USD-M linear; Bybit data requests default to linear, while its catalog combines linear, inverse, and spot.
- Use `params.type` (`spot`, `perp`/`swap`, `future`, `option`), `category` (`spot`, `linear`, `inverse`, `option`), `subType`, and `settle` for stock-supported products. Carry the same product selectors from catalog discovery into subsequent snapshots, including requests using opaque `marketId` strings; a nondefault identity does not load another acquisition profile automatically. Conflicting or ambiguous identities fail rather than select an arbitrary market.
- `params.coin` overrides `symbol` except on Lighter, which uses `market_id`/`marketId`. Aliases must resolve against metadata. Case, punctuation, Unicode, quote, settlement, expiry, and DEX distinctions are not inferred from ticker spelling.
- Catalog display symbols remain `BASE/QUOTE`; trade/book responses return the resolved CCXT symbol, not an echo of the input alias. Assets, precision, contract size, optional fields, and trade `info` now follow stock output. Catalog identity exists for all retained products; it does not imply statistics support for that product.
- Extended uses native USD catalog/settlement identity but CCXT USDC trade/book symbols. Lighter REST uses CCXT's `USDC` naming and numeric native IDs, not the former Explorer catalog. Hyperliquid spot identity uses metadata `@index`.
- Trades are newest-first; candles are oldest-first. `since` and end bounds are inclusive milliseconds. Recent-only trade APIs filter their returned window; requesting an old time does not create historical coverage.
- Candles remain `[timestamp, open, high, low, close, volume]`; volume is `null` when stock omits it, including Bybit mark/index/premium-index and Extended mark/index candles. Missing/nonfinite required prices or invalid book levels produce `UPSTREAM_DATA_INVALID`, not zero-filled data.
- Book timestamps/nonces retain stock provenance. Extended REST book time is CCXT receipt time, not exchange time; Lighter REST book time/nonce are absent; Bybit full/RPI books have no nonce from the stock parser. Lighter returns individual orders, so multiple rows can share a price; these are not aggregated price levels.

### Limits and optional sources

Defaults remain 100 trades and 200 candles. Positive trade/candle limits are capped: Binance/Bybit 1,000; Aster trades 1,000 and candles 1,500; Hyperliquid 5,000 (actual trades limited to the upstream window); Lighter trades 100 and candles 500; Extended trades 1,000 and candles 10,000. Bybit spot trades are capped at 60. Zero limits are rejected.

Book display depth uses top-level `limit`, then `params.levels`, `depth`, `limit`. Default 100, except Hyperliquid 20 and Bybit options 25. Maxima: Hyperliquid 20, Lighter 100, Binance/Aster/Extended 1,000 (Binance spot 5,000), Bybit 10,000 (options 25). Over-depth requests return 501 instead of silently clamping. Bybit spot above 200 and contracts above 1,000 use stock's full-book endpoint. Binance contract depth is rounded up to a supported source limit before top-N projection.

- Candle timeframe precedence: top-level `timeframe`, then `params.timeframe`, `interval`, then `1m`. Stock-supported native timeframe aliases are accepted. Unsupported intervals and Bybit option candles return 501.
- `params.until` precedes `endTime`, except Extended prefers `endTime`; Lighter also accepts `endTimestamp`/`end_timestamp` after `until`. For Hyperliquid/Lighter/Extended candles, `since` without an end bound requests a window of `limit` intervals from `since`.
- Binance/Bybit accept candle `price: mark | index | premiumIndex`; Aster/Extended accept `mark | index`. These require contracts; Bybit premium-index requires linear. Extended `candleType: trades | mark-prices | index-prices` takes precedence over `price`.
- Lighter retains `set_timestamp_to_end`/`setTimestampToEnd`. Hyperliquid books retain `nSigFigs` and `mantissa`. Binance linear and Bybit contract books expose `rpi: true`; Bybit RPI is capped at 1,000.
- Unknown/unavailable options return 501 / `UNSUPPORTED_FEATURE`; invalid symbols or conflicting selectors return 400 / `BAD_SYMBOL`. Catalog errors retain the nested `{error:{code,message}}` envelope; ordinary snapshot errors remain flat `{code,message}`.
- The public catalog response cache remains 30 seconds, keyed by exchange, `includeInactive`, and canonical params, including its response timestamp. CCXT metadata is shared separately per acquisition profile and refreshed on demand after 30 seconds; refresh failure returns an error without overwriting the prior owned observation.

## CCXT realtime contract

`GET /v1/ws` uses the same owned catalog identity as REST. Carry product selectors from discovery; Binance defaults to USD-M and Bybit to linear. Acknowledgement and update topics contain the resolved stock symbol. `params.coin` selects metadata aliases except on Lighter, which uses `market_id`/`marketId`. Unknown options, ambiguous symbols, and unsupported depth fail rather than selecting a different market or returning empty success.

### Channels and book depth

Trades/books are enabled for all six venues; candles for all except Lighter. Hyperliquid candles are newly enabled. Stock Binance/Bybit product profiles are selectable through `type`, `category`, `subType`, and `settle`; Bybit option candles are unsupported. The focused live checks covered BTC perpetuals, not every spot, option, expiry, settlement, or optional source. A later integrated pass exercised mixed trades/books/candles over `/v1/ws` for the five public venues (Lighter candles intentionally unsupported) and all three channels plus a finite two-burst-then-silence case against a local Extended fixture.

Book display depth uses `params.levels`, then `depth`, then `limit`. Zero is invalid; requests above the supported maximum are rejected, not clamped. Depth is a view of the owned stock book, not an upstream topic identity:

| Venue | Default / maximum displayed levels per side | Stock backing acquisition |
| --- | --- | --- |
| Binance | 20 / 1,000 contracts; 20 / 5,000 spot | Fixed 1,000-level contract or 5,000-level spot seed/diff profile; no shallow-view truncation of backing state |
| Bybit | 1,000 / 1,000; options 100 / 100 | Fixed stock 1,000-level tier, or 100 for options; the former 10,000-level live full-book path is removed |
| Hyperliquid | 20 / 20 | Stock full snapshots; `nSigFigs` and `mantissa` retain distinct acquisition identity |
| Aster | 20 / 20 | Stock partial-depth 20 stream; the former deep diff-book path is removed |
| Lighter (`lighterxyz`) | 20 / all retained source levels | Stock full book; nonce is `offset`, not the old native `nonce` |
| Extended | 20 / 1,000 | Stock standard indicative book; not RFQ liquidity; depth 1 is a view, not a different endpoint |

Binance linear books accept `rpi: true`; Binance trades accept `name: trade | aggTrade`. Candle timeframe precedence is `params.timeframe`, then `interval`, then `1m`; stock timeframe aliases are accepted. Binance contract candles accept `price: mark | index`; Extended accepts `candleType: trades | mark-prices | index-prices`, which takes precedence over `price: mark | index`. REST-only price sources and limits do not imply WS support. Candle volume remains `null` when absent, including Extended mark/index candles; updates replace the candle at the same timestamp.

### Delivery and lifecycle

- One coherent owner drives all result hashes at each actual stock URL. Viewers share maintained feeds; attaching a viewer does not reload metadata or seed a book. No saved snapshot is replayed on join: the viewer waits for the next source update.
- Trades use stock incremental cache cursors, not repeated rolling windows. This is not historical storage or a cross-reconnect exactly-once guarantee. Reconnect invalidates the prior ownership epoch; clients must treat `UPSTREAM_ERROR` as loss of trusted continuity until fresh data arrives.
- `subscribed` precedes updates. Duplicate requests return `alreadySubscribed`; removing one viewer leaves the others running. Final removal releases the source. Late/missing unsubscribe acknowledgements do not block unrelated maintained feeds. Extended uses stock per-channel URLs and closes their owner runtimes rather than inventing an unwatch method.
- Book updates are complete top-N views, not patches. Forwarders serialize borrowed slices of the shared owned book; they do not copy a full deep book for each viewer. Timestamp/nonce meanings follow stock output and need not match REST; Binance event time is stock `E`, Bybit book nonce is stock `u`, and Extended WS uses event `ts`/`seq`.
- Each client has a 256-message outgoing queue and at most 200 realtime subscriptions (separate from 16 statistics subscriptions). A duplicate at capacity is still acknowledged; exceeding demand returns `SUBSCRIPTION_LIMIT`. Slow/full outgoing queues close the client; broadcast lag emits `CLIENT_LAGGED`. Reconnect/resubscribe after closure.
- Live ownership is capped at 128 actual URLs and 200 feeds per shared URL, with only stock Binance URL slots. Binance workers reserve 8 MiB stacks; other live workers 2 MiB. These are admission limits, not a 400-user or 20,000-topic capacity result. Stock rate limiting stays enabled per core; combined IP quotas across owners/replicas and long-duration stock pool memory are not load-qualified. No throughput/latency SLA is claimed.
- Integrated checks observed a warm Bybit BTC feed continuing to update while another topic was added, removed, and re-added, and an Extended source reconnect resuming all three channels. These are single-run observations, not a capacity or soak qualification.

### Accepted upstream book risks

Known stock order-book defects across exchanges are accepted temporary migration risks, not synchronization passes. In pinned `4.5.85`, Binance can accept a post-bridge delta with the wrong `pu` continuity link; prices/liquidity may therefore be wrong while updates appear live. Ferris excludes the timestamp-less seed publication and never limits backing state to a viewer's depth, but neither measure repairs stock continuity or proves deep-level retention. No native fallback, dependency patch, or copied synchronizer is present. Extended HTTP access is verified with the User-Agent fix; stock WebSocket header handling remains blocked as documented in CCXT-004.

Reproduction evidence, current mitigations, fixed Ferris regressions, and closure checks are tracked in [CCXT_KNOWN_ISSUES.md](CCXT_KNOWN_ISSUES.md). Consult that register before treating a new stock release or a successful live stream as resolution of a known issue.


## API

### Fetch Trades (Snapshot)

`POST /v1/fetchTrades`

Use for initial bootstrap or fallback reads. For live trade updates, use `GET /v1/ws`.

Request body:

```json
{
  "exchange": "hyperliquid",
  "symbol": "BTC/USDC:USDC",
  "since": 1771354910000,
  "limit": 10,
  "params": {
    "dex": ""
  }
}
```

Response body (CCXT-like `Trade[]`):

```json
[
  {
    "info": {
      "coin": "BTC",
      "side": "B",
      "px": "68008.0",
      "sz": "0.0231",
      "time": 1771354910961,
      "hash": "0x...",
      "tid": 909337828325218,
      "users": ["0x...", "0x..."]
    },
    "amount": 0.0231,
    "datetime": "2026-02-17T22:35:10.961Z",
    "id": "909337828325218",
    "order": null,
    "price": 68008.0,
    "timestamp": 1771354910961,
    "type": null,
    "side": "buy",
    "symbol": "BTC/USDC:USDC",
    "takerOrMaker": null,
    "cost": 1570.9848,
    "fee": null
  }
]
```

### Fetch OHLCV (Snapshot)

`POST /v1/fetchOHLCV`

Snapshot endpoint for candles.

Request body:

```json
{
  "exchange": "hyperliquid",
  "symbol": "BTC/USDC:USDC",
  "timeframe": "1m",
  "since": 1771357000000,
  "limit": 3,
  "params": {}
}
```

Response body (CCXT-like `OHLCV[]`):

```json
[
  [1771357020000, 67826.0, 67842.0, 67818.0, 67818.0, 15.6008],
  [1771357080000, 67819.0, 67835.0, 67776.0, 67795.0, 11.88073],
  [1771357140000, 67799.0, 67875.0, 67799.0, 67858.0, 3.07521]
]
```

The sixth cell may be `null` when the selected source has no volume. Price cells and the millisecond timestamp remain required.

### Realtime Trades Stream (WebSocket)

`GET /v1/ws`

The websocket endpoint supports `trades`, `orderbook`, `ohlcv`, and `marketstats`. The first three use shared stock CCXT Pro acquisition; see the [realtime contract](#ccxt-realtime-contract). Statistics use shared stock REST calls plus Lighter Pro `watchTickers`. All statistics use the snapshot/delta contract below.

Subscribe command:

```json
{
  "op": "subscribe",
  "channel": "trades",
  "exchange": "bybit",
  "symbol": "BTC/USDT:USDT",
  "params": {
    "category": "linear"
  }
}
```

Unsubscribe command:

```json
{
  "op": "unsubscribe",
  "channel": "trades",
  "exchange": "bybit",
  "symbol": "BTC/USDT:USDT",
  "params": {
    "category": "linear"
  }
}
```

Server trade update:

```json
{
  "type": "trades",
  "topic": {
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "params": {
      "category": "linear"
    }
  },
  "data": [
    {
      "info": {
        "T": 1700000000000,
        "S": "Buy",
        "p": "100.5",
        "v": "0.25",
        "i": "abc-123"
      },
      "amount": 0.25,
      "datetime": "2023-11-14T22:13:20.000Z",
      "id": "abc-123",
      "order": null,
      "price": 100.5,
      "timestamp": 1700000000000,
      "type": null,
      "side": "buy",
      "symbol": "BTC/USDT:USDT",
      "takerOrMaker": null,
      "cost": 25.125,
      "fee": null
    }
  ]
}
```

Recommended client flow for trades:

1. Call `POST /v1/fetchTrades` for initial data.
2. Open `GET /v1/ws` and subscribe to the same topic.
3. Render live updates from `type="trades"` push messages.
4. Use snapshot endpoints only for bootstrap/fallback, not high-frequency polling.

### Realtime Order Book Stream (WebSocket)

Subscribe command:

```json
{
  "op": "subscribe",
  "channel": "orderbook",
  "exchange": "binance",
  "symbol": "BTC/USDT:USDT",
  "params": {
    "levels": 20
  }
}
```
Bybit live books support stock tiers up to 1,000 levels (options 100), not the former 10,000-level full-book stream. Aster live depth is limited to 20. See the [depth table](#channels-and-book-depth); REST and WS limits differ.


Server order book update:

```json
{
  "type": "orderbook",
  "topic": {
    "exchange": "binance",
    "symbol": "BTC/USDT:USDT",
    "params": {
      "levels": 20
    }
  },
  "data": {
    "asks": [[100.2, 1.5], [100.3, 0.9]],
    "bids": [[100.1, 2.1], [100.0, 0.4]],
    "datetime": "2026-02-22T00:00:00.000Z",
    "timestamp": 1771718400000,
    "nonce": null,
    "symbol": "BTC/USDT:USDT"
  }
}
```

### Realtime OHLCV Stream (WebSocket)

Supported exchanges: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`. Lighter has no stock Pro candle watcher.

Aster supports these futures intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, and `1M`.

Extended stock realtime supports `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w`, and `1M`. `params.candleType` accepts `trades` (default), `mark-prices`, or `index-prices`; absent volume is `null`, not `0.0`.

Hyperliquid candles now use stock Pro through the same websocket fanout. Use `POST /v1/fetchOHLCV` for bootstrap history.

Subscribe command:

```json
{
  "op": "subscribe",
  "channel": "ohlcv",
  "exchange": "bybit",
  "symbol": "BTC/USDT:USDT",
  "params": {
    "category": "linear",
    "timeframe": "1m"
  }
}
```

Server OHLCV update:

```json
{
  "type": "ohlcv",
  "topic": {
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "params": {
      "category": "linear",
      "timeframe": "1m"
    }
  },
  "data": [[1771718400000, 100.0, 101.0, 99.8, 100.4, 23.5]]
}
```

### Fetch Order Book (Snapshot)

`POST /v1/fetchOrderBook`

Snapshot endpoint for order book reads.

Request body:

```json
{
  "exchange": "hyperliquid",
  "symbol": "BTC/USDC:USDC",
  "limit": 2,
  "params": {
    "nSigFigs": 5,
    "mantissa": 1
  }
}
```
Bybit spot requests above 200 levels and linear/inverse requests above 1,000, up to 10,000, use CCXT's shipped `GET /v5/market/full_orderbook` method and stock book parser. The endpoint has no upstream `limit`; Ferris truncates to display depth. Options remain capped at 25. `params.rpi: true` selects the stock RPI endpoint for contracts up to 1,000 levels.


Response body (CCXT-like `OrderBook`):

```json
{
  "asks": [[67858.0, 9.14079], [67859.0, 0.15966]],
  "bids": [[67857.0, 0.44056], [67856.0, 0.19065]],
  "datetime": "2026-02-17T19:39:46.271Z",
  "timestamp": 1771357186271,
  "nonce": null,
  "symbol": "BTC/USDC:USDC"
}
```

### Fetch Markets

`POST /v1/fetchMarkets`

Snapshot endpoint for exchange symbols/market metadata, acquired from stock CCXT. Display symbols use `BASE/QUOTE` without a settlement suffix; base/quote naming follows CCXT, except Extended preserves its native collateral display name. Symbols are not unique market identities and must not be normalized into IDs.

Every retained catalog row includes `marketId`, `exchangeMarketId`, `category`, `dex`, `contractType`, `settle`, and `settlementAssetId`. Opaque IDs preserve native market/product identity; do not derive them from display symbols. Hyperliquid primary perps have `dex: ""`, and spot IDs use metadata `@index`. Binance/Aster IDs preserve native symbols with null category/DEX, for example `["binance","perp",null,null,"BTCUSDT"]`; native `marginAsset` supplies contract settlement. Bybit IDs carry product category and native contract settlement. Extended uses native `BTC-USD` and USD collateral identity; `info.isRfq`/`info.isOffHours` retain execution metadata when present. Lighter IDs use numeric native IDs as strings. Statistics still accept only their existing supported products/scopes; catalog identity alone is not a statistics capability.


Request body:

```json
{
  "exchange": "bybit",
  "params": {
    "category": "linear"
  },
  "includeInactive": false
}
```

Response body:

```json
{
  "exchange": "bybit",
  "markets": [
    {
      "exchange": "bybit",
      "symbol": "ADA/USDT",
      "base": "ADA",
      "quote": "USDT",
      "type": "perp",
      "active": true,
      "marketId": "[\"bybit\",\"perp\",\"linear\",null,\"ADAUSDT\"]",
      "exchangeMarketId": "ADAUSDT",
      "category": "linear",
      "dex": null,
      "contractType": "LinearPerpetual",
      "settle": "USDT",
      "settlementAssetId": "USDT",
      "minOrderSize": 1,
      "tickSize": 0.0001,
      "contractSize": 1,
      "info": {
        "category": "linear",
        "rawSymbol": "ADAUSDT",
        "exchangeSymbol": "ADAUSDT"
      }
    }
  ],
  "timestamp": 1760000000000
}
```

Bybit notes:

- If `params.category` is omitted, backend fetches and combines `linear`, `inverse`, and `spot` categories.
- `info.category` is always included for Bybit rows so frontend can disambiguate contract families while keeping canonical `symbol`.
- All retained rows carry identities; only supported perpetual products participate in Bybit statistics. Specify the same category when selecting those IDs; unlike the combined catalog default, statistics default to `linear`.

Error shape for this endpoint:

```json
{
  "error": {
    "code": "INVALID_EXCHANGE",
    "message": "Exchange 'foo' is not supported"
  }
}
```

### Binance Market Statistics

Default statistics enumerate active USDⓈ-M perpetuals. Explicit `category: "inverse"`, `"spot"`, or `"option"` selects another stock catalog; `type: "future"` selects expiry futures within the linear/inverse category. Obtain opaque IDs from `fetchMarkets` with the matching product selectors. Selected known inactive contracts remain selectable with implemented fields `unavailable` / `inactive-market`.

```json
{"exchange":"binance","marketIds":["[\"binance\",\"perp\",null,null,\"BTCUSDT\"]"],"fields":["funding","lastPrice","volume24h","openInterest"],"params":{}}
```

**Open interest requires selected `marketIds`** (up to 100 inputs). An all-market request containing `openInterest` returns HTTP 400 / `VALIDATION_ERROR`; a WS command returns the corresponding error frame. Funding-only demand never triggers an OI sweep. Overlapping HTTP/WS selections share one per-market stock `fetchOpenInterest` observation and polling deadline. Inactive/spot selections do not issue an OI call.

Stock `fetchTickers`, `fetchFundingRates`, and `fetchFundingIntervals` supply last/volume and perpetual funding/mark/index. Options use `fetchMarkPrices` for mark and stock implicit `eapiPublicGetIndex`, shared once per catalog underlying, for index. Ticker `exercisePrice` becomes a settlement estimate near expiry and is not used as index. Funding retains its raw decimal string, `currentUnclassified` kind, `decimalFraction` unit, and positive native `nextFundingTime`. The funding-interval endpoint reports exceptions; a market without qualified interval metadata keeps null intervals/equivalents, not an assumed eight hours. Last-settled funding and funding history are unsupported.

Bulk acquisition polls every 30 seconds; each observation expires after 90 seconds. Catalog metadata is shared with the other CCXT endpoints. `BINANCE_BASE_URL` defaults to `https://fapi.binance.com`; a custom gateway must serve every requested product's stock path. See the [numeric unit table](#numeric-statistics-units) before interpreting derivative amounts.

### Bybit Market Statistics

Omitted/null/empty params canonicalize to `{"category":"linear"}`. Use `category: "inverse"`, `"spot"`, or `"option"` for those products, and `type: "future"` for expiry-only linear/inverse enumeration. IDs must match the category: for example `["bybit","perp","linear",null,"BTCUSDT"]` encoded as a JSON string. No display-symbol or `baseCoin` shortcut is accepted. Active enumeration excludes prelisting/inactive instruments; known inactive selections report `inactive-market`.

```json
{"exchange":"bybit","fields":["funding","markPrice","indexPrice","lastPrice","volume24h","openInterest"],"params":{"category":"linear"}}
```

Stock bulk `fetchTickers` supplies observations; stock `parseOpenInterest` preserves product-specific OI meanings. Option tickers are collected by catalog base-coin groups rather than per viewer. Catalog loads/pagination remain stock-owned. HTTP and Ferris WS share acquisition per category.

Funding preserves native `fundingRate` as an exact `decimalFraction` `estimate`. Valid ticker `fundingIntervalHour` wins over instrument `fundingInterval` minutes; absence allows metadata fallback, explicit invalidity leaves intervals/equivalents null, and a valid conflict records `funding-interval-mismatch`. No universal eight-hour interval is assumed. Native `nextFundingTime` drives `funding-payment-passed` expiry without relabeling the estimate settled or expiring sibling prices. Last-settled funding and history are unsupported.

Mark/index/last prices use catalog base/quote assets, independently of settlement. The batch's top-level server time is not retained in stock ticker rows, so `exchangeTimestamp` is null rather than a fabricated per-field timestamp. Shared polling is 30 seconds with 90-second receipt freshness. `BYBIT_BASE_URL` defaults to `https://api.bybit.com`.

### Lighter Market Statistics

Use public exchange ID `lighterxyz`; stock CCXT calls the provider `lighter`. Catalog-issued identities retain numeric native IDs, for example `["lighterxyz","perp",null,null,"1"]`. Default enumeration is perpetuals; `type: "spot"` selects spot. Display symbols are not identities.

Stock `fetchTickers` polls `orderBookDetails` for last price and rolling volume. Stock Pro `watchTickers` maintains `market_stats/all` for perpetual funding, last-settled funding, mark/index, open interest, last and volume. Its live subscription shares the per-URL owner with public feeds; REST stays on the separate catalog/statistics owner. Capabilities report `sharedPollingAndWebSocket`. No native Ferris socket/parser remains.

Funding uses exact percentage-valued strings with a one-hour basis: `rateUnit: percent`, `rateIntervalMs: 3600000`, `paymentIntervalMs: 3600000`. `current_funding_rate` is an estimate; `funding_rate` is distinct settled funding. A qualified `funding_timestamp` belongs only to settled `paymentTimestamp`, not to every field's exchange time. Thus `-0.0003` means `-0.0003%`, not `-0.03%`.

Lighter open interest is **two-sided** (outstanding longs + shorts), matching the venue UI. `openInterestValue` is `2 × market_stats.open_interest` in USDC; the WS API field is one-sided notional. `openInterestAmount` stays null: Ferris does not divide by mark or mix in REST `orderBookDetails.open_interest`, which is one-sided base coins. For example, WS `118223213` yields `{"openInterestAmount":null,"openInterestValue":236446426}`. Do not double the returned value again.

REST polling cannot freshen silent live funding/mark/index/OI observations. Each field expires after 90 seconds from its own receipt. Missing OI keys leave its prior observation untouched; null clears it as missing, invalid/nonfinite/negative values clear it as invalid, and zero remains available. Spot OI is `notApplicable`.

### Extended Market Statistics

Statistics cover stock catalog perpetuals, including RFQ/off-hours metadata; off-hours alone is not inactivity. Spot remains excluded. Use null/empty params and exact catalog IDs such as `["extended","perp",null,null,"BTC-USD"]`.

```json
{"exchange":"extended","fields":["funding","markPrice","indexPrice","lastPrice","volume24h","openInterest"],"params":{}}
```

Stock `loadMarkets` supplies catalog identity and `fetchTickers` supplies bulk observations. These are separate stock acquisitions, not one native response cache. Display/price assets preserve native `assetName`/`collateralAssetName`; settlement preserves the collateral name and `l2Config.collateralId`, never a guessed token address. Stock trading symbols may still use `USDC` while the catalog display uses `USD`.

Funding is an exact hourly `decimalFraction` `estimate`. Payment/next-payment/exchange timestamps stay null: native `nextFundingRate` is a funding update, not a qualified payment time. An RFQ last price of zero is invalid, not permission to substitute mark or BBO. Numeric volume and OI report base/collateral units as documented below. Last-settled funding and history are unsupported.

Shared 30-second polling and 90-second freshness apply. Failed catalogs retain known membership as incomplete; failures retain observations stale, and explicitly invalid scalars clear only their field. The missing-User-Agent HTTP 403 is fixed and BTC statistics passed with complete coverage; the separate stock WebSocket header limitation remains [CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004). `EXTENDED_REST_BASE_URL` must end in `/api/v1`.

### Aster Market Statistics

Default enumeration is stock V3 perpetuals; `type: "spot"` selects the stock spot catalog. Known inactive contracts remain selectable with implemented fields `unavailable` / `inactive-market`. Exact native IDs, Unicode, punctuation, and quote/settlement variants remain distinct.

```json
{"exchange":"aster","fields":["funding","markPrice","indexPrice","lastPrice","volume24h"],"params":{}}
```

Stock `fetchFundingRates` and `fetchFundingIntervals` supply decimal-fraction `estimate` funding and qualified per-market intervals (not a hardcoded eight hours). `nextFundingTime` drives payment-boundary stale expiry, never conversion to settled funding. Missing/failed rates retain their original interval basis and receipt; fresh metadata cannot rescale a missing old observation.

Stock `fetchTickers` adds last price and numeric rolling volume. Perpetual/spot views share bulk acquisition. Mark/index/last values retain native strings and catalog base/quote assets, separately from settlement. Open interest is `unsupported` / `stock-method-not-supported`; last-settled funding and history are also unsupported. Shared polling/freshness remains 30/90 seconds. `ASTER_BASE_URL` defaults to `https://fapi.asterdex.com`.

### Market Statistics and Capabilities

`GET /v1/capabilities` enumerates registered adapters without fetching upstream data. Hyperliquid, Binance, Bybit, Extended, Aster, and Lighter advertise statistics support; funding history remains unsupported everywhere. Runtime failures do not change capabilities.

`POST /v1/fetchMarketStats`:

```json
{"exchange":"hyperliquid","fields":["funding","markPrice","indexPrice"],"params":{"dex":""}}
```

All six venues use stock CCXT acquisition; an integrated HTTP pass exercised `fetchMarketStats` on all six and returned the documented field states, including the numeric volume/open-interest value objects. Hyperliquid uses primary-DEX `fetchTickers` contexts: hourly decimal-fraction `currentUnclassified` funding, mark/oracle prices, quote-only volume and base-unit OI. `lastPrice` is unsupported because stock ticker `last` is a midpoint. A synthetic next-hour timestamp is not published as an exchange payment schedule. Price assets now follow the CCXT catalog rather than the retired oracle-quote special case.

Omitted/null `marketIds` selects active perpetuals by default, or active spot/future/option markets when that product is explicitly selected. Selected requests accept 1–100 exact catalog-issued ID strings; IDs are case-sensitive, sorted and deduplicated. Omitted/null `fields` means `["funding"]`; empty lists are invalid. Supported selectors are `type`, `category`, and `subType`, plus Hyperliquid's primary `dex: ""`; conflicting or unqualified products fail validation. Binance/Bybit support spot, linear/inverse contracts, and options; Aster/Hyperliquid/Lighter support perpetuals and spot; Extended supports perpetuals. Unknown IDs need a complete matching catalog: incomplete identity proof yields 502, not a fabricated 400. REST rejects unknown top-level keys, including `symbol`.

Response: `{timestamp, scope, markets, coverage}`. Scope contains canonical exchange/product params. Rows flatten catalog identity and add only requested `fields`. Coverage is `{expectedMarkets, returnedMarkets, enumerationComplete, sourceFailures}`. Cold all-market upstream failure returns HTTP 200 with no rows, null expected count, incomplete enumeration and explicit failures. Known membership survives failed/incomplete catalog loads. An adapter without statistics support returns 501 / `UNSUPPORTED_FEATURE`; ordinary errors use flat `{code,message}`.

Each field is `{state,value,reason,exchangeTimestamp,receivedTimestamp,source}`. Funding/price values remain strings; `volume24h`/`openInterest` use the strict numeric schemas below. Sources identify the actual stock method, for example `bybit:ccxt:fetchTickers`, `binance:ccxt:fetchOpenInterest`, or `lighterxyz:ccxt:watchTickers`; catalog failures use `{exchange}:ccxt:loadMarkets`. Missing exchange timestamps stay null. Funding units and kinds remain venue-specific:

```json
{"rate":"-0.0003","rateUnit":"percent","kind":"estimate","rateIntervalMs":3600000,"paymentIntervalMs":3600000,"paymentTimestamp":null,"nextPaymentTimestamp":null,"equivalents":{"oneHourPercent":"-0.0003","eightHourPercent":"-0.0024","oneDayPercent":"-0.0072","annualizedPercent":"-2.628"}}
```

`equivalents` use decimal-string arithmetic and simple linear scaling. Missing/invalid intervals or arithmetic overflow leave equivalents null without discarding the exact native rate, including zero. Positive means longs pay shorts. Do not treat an equivalent interval as a separate funding event or compound the annualized value.
- Prices are `{amount,baseAsset,quoteAsset}`; quote is not necessarily settlement. Never replace index with midpoint or last price.
- Spot funding/last-settled funding/OI/mark/index are `notApplicable`; supported spot last/volume follow capabilities. Expiry futures/options have no funding. Implemented inactive observations are `unavailable` / `inactive-market`.
- Last-settled funding is supported only for Lighter perpetuals. Aster OI has no stock method; Lighter OI is two-sided USDC notional. Other implemented metric units are listed below. Runtime outages do not change capabilities.
- Bulk sources poll every 30 seconds per exchange/acquisition scope; selected Binance OI has shared per-market demand. Catalog metadata refreshes on demand after 30 seconds. Cache reads never manufacture receipts. Failures immediately stale retained values; independent 90-second expiry also runs during pending requests. Aster/Bybit estimates additionally expire at their advertised payment boundary. Explicit invalid values cannot be resurrected by later failures.
- HTTP requests renew a 90-second demand lease; WS subscriptions hold persistent reference-counted demand. Idle workers stop. Capabilities disclose these limits and receipt-time freshness.

Statistics socket subscription (no `symbol`; `funding` is not a channel alias):

```json
{"op":"subscribe","channel":"marketstats","exchange":"hyperliquid","params":{"dex":""},"fields":["funding"]}
```

Optional `marketIds` uses the same selection contract. Server canonicalizes topic IDs/fields; duplicate topics return `alreadySubscribed`. Maximum 16 distinct statistics subscriptions per socket. Unsubscribe with the same topic and `op: "unsubscribe"`, even after a selected market disappears.

Delivery contract:

1. `subscribed` is queued before data. First data has `type:"marketstats"`, `mode:"snapshot"`, canonical `topic`, new opaque `generation`, `revision:1`, plus flattened snapshot fields.
2. Deltas have `mode:"delta"`, the same generation/topic, `previousRevision`, incremented `revision`, `timestamp`, `scope`, `updates`, `removedMarketIds`, and current `coverage`.
3. Replace the whole view on snapshot. Accept a delta only when generation matches and `previousRevision` equals the applied revision; otherwise discard it and resubscribe.
4. Each update includes catalog identity/display columns. Present field objects replace prior objects atomically (including null value); absent fields are unchanged. New rows and identity/active changes include all requested fields. Remove only IDs in `removedMarketIds`.
5. Complete source states coalesce before delta construction, at most once per second. Coverage and new receipt timestamps are changes even if rates are unchanged. Slow-client queue failure closes the connection; reconnect/resubscribe starts a new generation/full snapshot. There is no durable replay.

An integrated pass exercised `marketstats` over `/v1/ws` for all six venues and observed ordered snapshot→delta delivery, including a zero/null numeric update at revision 2. Phase acceptance and exercised verification are recorded in [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md#phase-tracker). No frontend implementation or funding-history endpoint is included.

### Numeric Statistics Units

`fields.volume24h.value` is `{"baseVolume":12.5,"quoteVolume":812500.0}`. `fields.openInterest.value` is `{"openInterestAmount":123.5,"openInterestValue":null}`. Both member names are required, each member is a nullable finite JSON number, and zero is valid. Missing one side remains null; no usable observation means an outer unavailable/stale field, not invented zeroes. Negative/nonfinite/malformed measurements are invalid. Numeric precision follows stock values; funding/price precision remains decimal-string based. Empty, cross-metric, unknown-member and string-number objects are rejected on decoding.

No Ferris currency/contract conversion or missing-side price multiplication is performed. `openInterestAmount` does **not** universally mean base coins:

| Venue/product | `baseVolume` / `quoteVolume` | `openInterestAmount` / `openInterestValue` |
| --- | --- | --- |
| Binance linear perpetual/future | Native base `volume` / quote `quoteVolume` | Base amount / null; selected IDs only |
| Binance inverse perpetual/future | Native `baseVolume` / null when quote turnover is absent; not contract `volume` | Contract count / null; selected IDs only |
| Binance option | Native contract `volume` only when explicit catalog `unit` is 1 base coin; otherwise null / quote `amount` | Contract count / USD `sumOpenInterestUsd`; selected IDs only, no contract-size conversion |
| Bybit linear perpetual/future | Base `volume24h` / quote `turnover24h` | Base amount / null (stock parser does not preserve ticker `openInterestValue`) |
| Bybit inverse perpetual/future | Base `turnover24h` / USD `volume24h` | null / USD value, not base quantity |
| Bybit option | Base `volume24h` / quote `turnover24h` | Base amount (contract multiplier 1) / null |
| Hyperliquid perpetual | null / quote `dayNtlVlm` | Base amount / null |
| Extended perpetual | Base `dailyVolumeBase` / collateral `dailyVolume` | Base `openInterestBase` / collateral `openInterest` (catalog USD naming) |
| Aster perpetual | Base `volume` / quote `quoteVolume` | Unsupported |
| Lighter perpetual | Base `daily_base_token_volume` / quote `daily_quote_token_volume` | null / two-sided USDC notional (`2 ×` WS `open_interest`) |
| Supported spot products | Base / quote from the venue ticker; Hyperliquid preserves quote-only volume | Not applicable |

Primary field references: [Binance OI](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest), [Binance option contracts and units](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/Exchange-Information), [option volume](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/24hr-Ticker-Price-Change-Statistics), [option OI](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/Open-Interest), [Bybit OI units](https://bybit-exchange.github.io/docs/v5/market/open-interest), [Bybit option multiplier/quantity](https://www.bybit.com/en/learn/options/bybit-options-lesson-options-parameters-introduction), [Hyperliquid contexts](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals), [Extended market statistics](https://api.docs.extended.exchange/#get-markets). These explain venue fields; the exact pinned stock mappings and unavailable members remain part of Ferris's contract.

### Health

`GET /healthz`

```json
{
  "status": "ok"
}
```

### Extended Examples

REST candle request (`POST /v1/fetchOHLCV`):

```json
{"exchange":"extended","symbol":"BTC/USD:USD","timeframe":"1m","limit":100,"params":{"candleType":"mark-prices"}}
```

Websocket subscription (`GET /v1/ws`):

```json
{"op":"subscribe","channel":"orderbook","exchange":"extended","symbol":"BTC/USD:USD","params":{"levels":10}}
```

Extended realtime book depth must be 1–1,000; zero/over-depth requests fail rather than clamp. Depth 1 is a view of the maintained standard indicative book, not a different upstream endpoint. WS time/nonce follow stock `ts`/`seq`; accepted stock continuity/retention risks remain, so updates do not prove synchronization correctness. REST depth must be 1–1,000; its timestamp is CCXT receipt time and nonce is null.

## Running locally

```bash
cargo run
```

Defaults:

- host: `0.0.0.0`
- port: `8787`
- Hyperliquid base URL: `https://api.hyperliquid.xyz`

`SIGINT`/`SIGTERM` start a graceful shutdown: the server stops accepting new upgrades, closes client websockets, stops statistics, cancels and joins all catalog and live owners, and only then drains in-flight HTTP requests. Acquisition is cancelled before the HTTP drain, so a pending upstream request does not hold shutdown open.

## Environment variables

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8787`)
- `HYPERLIQUID_BASE_URL` (default: `https://api.hyperliquid.xyz`)
- `BINANCE_BASE_URL` (default: `https://fapi.binance.com`; default leaves other stock product hosts intact, custom gateways override all public product paths)
- `BYBIT_BASE_URL` (default: `https://api.bybit.com`)
- `ASTER_BASE_URL` (default: `https://fapi.asterdex.com`)
- `EXTENDED_REST_BASE_URL` (default: `https://api.starknet.extended.exchange/api/v1`; must end in `/api/v1` for stock signing)
- `EXTENDED_WS_URL` (default: `wss://api.starknet.extended.exchange/stream.extended.exchange/v1`)
- `REQUEST_TIMEOUT_MS` (default: `10000`)
- `RUST_LOG` (default: `info`)
- `LIGHTER_REST_BASE_URL` (default: `https://mainnet.zklighter.elliot.ai`; CCXT catalog, snapshots, and statistics polling)
- `LIGHTER_WS_URL` (default: `wss://mainnet.zklighter.elliot.ai/stream`; CCXT Pro realtime and maintained statistics)

`TRADE_CACHE_CAPACITY_PER_COIN`, `TRADE_CACHE_RETENTION_MS`, and `TRADE_COLLECTOR_ENABLED` were removed with the Hyperliquid history collector.

`LIGHTER_MARKETS_URL` and `LIGHTER_MARKET_CATALOG_REFRESH_MS` were removed with the Explorer realtime catalog. Live identity now comes from CCXT metadata. `HYPERLIQUID_BASE_URL` also derives stock `/ws`; Binance/Bybit/Aster REST overrides do not override their stock public websocket hosts.

## Retired terminal tools

The direct-exchange terminal viewer and order-book probe were retired during the CCXT cutover. Use the backend HTTP/WS interfaces and smoke scripts below.

## Testing against a running server

These checks assume the backend is already running.

Quick smoke test script:

```bash
python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8787
```

PowerShell-native smoke test (recommended on pure Windows):

```powershell
./scripts/smoke_endpoints.ps1 -BaseUrl http://127.0.0.1:8787
```

To print sample returned data (not just pass/fail checks):

```powershell
./scripts/smoke_endpoints.ps1 -BaseUrl http://127.0.0.1:8787 -ShowData
```

By default it waits up to 90 seconds for `/healthz` so you can run it while the backend is still compiling/starting.

If you want to disable waiting:

```bash
python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8787 --wait-seconds 0
```

Optional overrides:

- `--exchange` (default `hyperliquid`)
- `--symbol` (default `BTC/USDC:USDC`)
- `--markets-exchange` (default `bybit`; used for `fetchMarkets` smoke check)

If you get `HTTP 404` on `/healthz`, you are likely hitting a different process on that port.

PowerShell quick fix (use a different port):

```powershell
$env:PORT = "8788"
cargo run
```

If you prefer `cmd.exe`, use quoted `set` syntax to avoid trailing-space env values:

```cmd
cmd /c "set \"PORT=8788\" && cargo run"
```

Then test:

```powershell
python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8788
```

To find who already owns port 8787 on Windows:

```powershell
netstat -ano | findstr :8787
tasklist /FI "PID eq <PID_FROM_NETSTAT>"
```

Extended REST smoke workflow:

```bash
python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8787 --exchange extended --symbol BTC/USD:USD --markets-exchange extended
```

For Extended live integration tests, set `FERRIS_TEST_EXCHANGE=extended`, `FERRIS_TEST_SYMBOL=BTC/USD:USD`, and `FERRIS_TEST_MARKETS_EXCHANGE=extended` before the command below.

Live integration tests (ignored by default):

```bash
cargo test --test live_endpoints -- --ignored
```

You can override target server and test market with env vars:

```bash
FERRIS_BASE_URL=http://127.0.0.1:8787 FERRIS_TEST_SYMBOL=ETH/USDC:USDC cargo test --test live_endpoints -- --ignored
```

Realtime websocket fanout integration test (self-contained; spins up mock upstream + backend in-process):

```bash
cargo test --test realtime_ws
```

## Release builds and Docker

The release artifact is built from the committed `Cargo.lock`; dependencies are pinned to stock `ccxt`/`ccxt-pro` `4.5.85` with only the six registered venue features enabled and default features disabled.

Local release build:

```bash
cargo build --release --locked --bin ferris-market-data-backend
```

Container image (the Dockerfile pins the tested builder toolchain `rust:1.98.1-slim-bookworm`, copies `Cargo.toml`/`Cargo.lock`, and builds with `--locked`):

```bash
docker build -t ferris-market-data-backend .
docker run --rm -p 8787:8787 ferris-market-data-backend
```

Because the image copies the committed `Cargo.lock` and builds with `--locked`, it cannot resolve different dependency versions than the repository. The exact pinned toolchain version, not an unverified minimum, is what the release image uses. `.dockerignore` excludes root and nested `target` directories so local compiler artifacts and retired binaries cannot enter the build context. The container builds one backend binary with one Cargo job to bound build memory.

Phase 5 verification (2026-10-03): `docker build --tag ferris:phase5-review .` passed; image `a782887bb0d3` ran as `app` (uid/gid 1000). Its actual entrypoint passed health/capabilities, public Hyperliquid and local Extended stock HTTP/WS smoke, and SIGTERM with an active stream exited 0. The broader six-venue HTTP/WS/statistics checks and test results are in the [Phase 5 handoff](FERRIS_V2_PLAN.md#phase-5-handoff--final-integration--2026-10-03). Extended loopback does not establish public availability, and successful book delivery does not resolve the accepted stock synchronization defects.

Publishing and deployment are separate user decisions from the CCXT cutover. Deploy the built release artifact to replace the previous release; rollback restores that previous release. There is no embedded native-provider fallback to roll back to.

### Hosted Lattice deployment — 2026-10-03

The user authorized the production cutover for frontend testing. The existing
`latticeterminal.service` now runs the release executable extracted from the locked
`ferris:extended-user-agent-20261003` image (`06c9853b20c5`), not a container. Caddy remains
unchanged: `api.latticeterminal.com` proxies HTTP and WebSocket upgrades to
`127.0.0.1:8787`. The service loads `/root/Ferris/.env.prod`, is enabled at boot,
and retains its existing root user and restart-on-failure policy.

- HTTP: `https://api.latticeterminal.com`
- WebSocket: `wss://api.latticeterminal.com/v1/ws`
- Service executable: `/usr/local/lib/latticeterminal/current/ferris-market-data-backend`
- Active release: `/usr/local/lib/latticeterminal/releases/20261003-extended-06c9853b20c5/`
- Binary SHA-256: `efcf7fa8a85e4df6f5f8edf40c3f557b888d262b39f99cca080e4d8ca24579c5`
- Service: `systemctl status latticeterminal.service`; logs: `journalctl -u latticeterminal.service -f`

Public HTTPS snapshots/statistics and WSS trades/books/candles/statistics
snapshot+delta passed for Binance, Bybit, Hyperliquid, Aster and Lighter, except
the documented unsupported Lighter candles. CORS preflight and WebSocket origin
`http://localhost:5173` passed through the public edge. No frontend application
itself was run.

Extended header fix deployed 2026-10-03: public HTTPS health and catalog passed
(326 active perpetuals), BTC statistics returned all six fields with complete
coverage/no source failures, and REST books returned 20 bids/20 asks. Public
Hyperliquid WSS delivered successive 20-level books after the restart. The running
process executable and SHA-256 matched the new release. Stock Extended WebSocket
header support remains a separate blocker; see [CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004).

**Frontend caveat:** Binance's default `trade` stream can supply zero-price,
zero-size rows (`info.p="0"`, `info.q="0"`, `info.X="NA"`, `info.st=1`), faithfully
carried by stock CCXT. Do not treat these as executable-price prints. The explicit
`params: {"name":"aggTrade"}` stream delivered positive trade rows in the deployment
smoke, but changes aggregation semantics; it is not a silent server-side fallback.
See [CCXT-012](CCXT_KNOWN_ISSUES.md#ccxt-012).

For future updates, run this on the production host as root. It builds the current
working tree using the pinned Docker toolchain, installs an immutable release,
preserves `previous`, atomically switches `current`, and restarts the service.
Restarting disconnects existing WebSocket clients. Health is only a process check;
exercise the changed exchange endpoints afterward.

```bash
(
  set -eu
  cd /root/Ferris
  base=/usr/local/lib/latticeterminal
  rev=$(date -u +%Y%m%dT%H%M%SZ)
  image="ferris:release-$rev"
  release="$base/releases/$rev"
  docker build -t "$image" .
  container=$(docker create "$image")
  trap 'docker rm "$container" >/dev/null' EXIT
  install -d -m 755 "$release"
  docker cp "$container:/usr/local/bin/ferris-market-data-backend" "$release/"
  chmod 755 "$release/ferris-market-data-backend"
  ln -sfnT "$(readlink -f "$base/current")" "$base/previous"
  ln -s "$release" "$base/current-$rev"
  mv -Tf "$base/current-$rev" "$base/current"
  systemctl restart latticeterminal.service
  curl -fsS --max-time 10 https://api.latticeterminal.com/healthz
)
```

Restart only (does **not** rebuild or deploy source changes):

```bash
sudo systemctl restart latticeterminal.service
```

The `previous` symlink currently preserves `20261003-ccxt-a782887bb0d3`; its original
service unit is saved beside the executable. To roll back one release:

```bash
sudo ln -sfnT /usr/local/lib/latticeterminal/previous /usr/local/lib/latticeterminal/current-rollback
sudo mv -Tf /usr/local/lib/latticeterminal/current-rollback /usr/local/lib/latticeterminal/current
sudo systemctl restart latticeterminal.service
```

The original pre-CCXT rollback remains separately available:

Rollback artifacts (previous executable, original unit, environment snapshot and
a ready-to-install rollback unit) are in
`/usr/local/lib/latticeterminal/releases/20261003-pre-ccxt/`. The original unit's
build-directory path no longer exists, so use the prepared rollback unit rather
than reinstalling the original unit verbatim:

```bash
sudo install -m 644 /usr/local/lib/latticeterminal/releases/20261003-pre-ccxt/latticeterminal-rollback.service /etc/systemd/system/latticeterminal.service
sudo systemctl daemon-reload
sudo systemctl restart latticeterminal.service
```

The rollback unit was validated, not activated. The separately enabled, already
failed `ferris.service` pointed at nonexistent `/target/...` and `/.env.prod` paths;
it was backed up and disabled to leave only the Lattice service as the boot owner.


## Exchange maintenance

The CCXT migration targets only the six registered venues. All REST snapshots, realtime streams, and statistics are acquired in `src/exchanges/ccxt` through stock `ccxt`/`ccxt-pro` `4.5.85`; use stock methods and the shared Ferris DTO/catalog boundary rather than add another handwritten HTTP adapter or raw stream parser. No native exchange module, synchronizer, terminal tool, or fallback path remains in the target backend. Additional venues require separate scope approval.

## Notes on Hyperliquid public trades

Hyperliquid public trades use CCXT's shipped `recentTrades` request and trade parser. The unified method in this pin selects user fills, so it is not used for public prints. There is no extra collector, ring buffer, or retained history configuration. `since` filters the short upstream window; it cannot recover older trades. `GET /v1/ws` remains the shared realtime interface.

### Binance realtime order books

Binance live books are owned by the stock CCXT Pro watcher per actual URL, at a fixed 1,000-level contract profile (5,000 for spot). There is no Ferris partial-depth selection (`5`/`10`/`20`) and no Ferris-built REST seed/diff synchronizer; display depth is a per-viewer top-N slice of the owned book and never limits the backing state. Requests with `levels`, `depth`, or `limit` up to 1,000 contracts / 5,000 spot are served from that owner; over-depth requests fail. The `nonce` field carries the stock update ID (`u`) and the timestamp is stock event time `E`, not the former native `T`. The accepted post-bridge `pu` continuity defect applies; see [CCXT_KNOWN_ISSUES.md](CCXT_KNOWN_ISSUES.md).

### Aster futures realtime streams
Aster live market data uses stock CCXT Pro through the same shared-URL owner and fanout as every other venue; no Aster-native stream, parser, or synchronizer remains. Live book depth is the stock partial-depth stream with default and maximum 20 levels per side; over-depth requests fail rather than clamping. Realtime `params.levels`, `depth`, or `limit` select the displayed top-N view of the owned book; the `nonce` field carries stock provenance and is not proof of synchronization. Candles are served through the stock Pro watcher like the other supported venues; see the [depth table](#channels-and-book-depth) and the [realtime contract](#ccxt-realtime-contract).

REST defaults: trades 100 rows (maximum 1,000) and candles 200 (maximum 1,500), both capped per the venue limits above; `since` filters trades/candles and `params.until` or `endTime` supplies an upper bound. The catalog defaults to perpetuals and supports explicit stock product selection; `includeInactive` includes inactive rows of the selected product.

`ASTER_BASE_URL` (default `https://fapi.asterdex.com`) overrides the stock REST paths. Binance, Bybit, and Aster REST overrides do not override their stock public websocket hosts; frontend clients always connect through this backend's `/v1/ws`. Statistics use shared stock V3 REST polling, not a separate native stream.

