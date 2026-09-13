# Ferris Market Data Backend

Open-source Rust backend that gives your frontend a CCXT-like way to fetch snapshots and subscribe to shared realtime market-data streams.

Planning and delivery tracking lives in `ROADMAP.md`.

Frontend integration guidance lives in `INTEGRATION_README.md`.

- unified endpoint shapes (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
- backend websocket fanout for realtime channels (`GET /v1/ws`)
- pluggable exchange adapter architecture
- market-data support for `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, and `lighterxyz` perpetuals
- shared upstream websocket topics for trades/books/candles; Hyperliquid and Binance statistics use shared 30-second REST polling, while Lighter marketstats uses its native WebSocket through the coordinator
- background websocket trade collector with in-memory ring buffer for deeper snapshot history

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
- idle topics are closed, and dropped upstream streams reconnect with backoff

## Current scope

- Snapshot endpoints (bootstrap/fallback):
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
  - `POST /v1/fetchMarkets`
- `POST /v1/fetchMarketStats` (Hyperliquid primary perps, Binance USDⓈ-M PERPETUAL markets, and Lighter perpetual marketstats)
- Capability discovery: `GET /v1/capabilities` (no upstream acquisition)
- Realtime endpoint:
  - `GET /v1/ws` (channels `trades`, `orderbook`, `ohlcv`, `marketstats`)
- Realtime channel support:
  - `trades`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
  - `orderbook`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
  - `ohlcv`: `binance`, `bybit`, `aster`, `extended`
  - `marketstats`: `hyperliquid`, `binance` (`sharedPolling`), `lighterxyz` (`nativeWebSocket`); Bybit, Aster, and Extended remain unsupported
- exchange supported:
  - `hyperliquid` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats`)
  - `binance` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`, `fetchMarketStats` for USDⓈ-M perpetuals)
  - `lighterxyz` (`fetchMarkets`, `fetchMarketStats` for native perpetual marketstats; no unrelated realtime trade/OHLCV claim)
  - `bybit` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`; marketstats unsupported)
  - `aster` futures/perpetual markets (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`, `fetchMarkets`; marketstats unsupported)
  - `extended` perpetual public market data: all four snapshot endpoints plus realtime trades, order books, and OHLCV; marketstats unsupported
- Extended accepts `BASE-USD`, `BASE/USD`, and `BASE/USD:USD`; trade/book/realtime responses use `BASE/USD:USD`. The market catalog uses `BASE/USD`, consistent with the shared catalog contract.
- Extended's standard websocket order book is indicative, not the RFQ real-book stream. Spot, private trading/account, funding, and account streams are not supported.
- Extended REST and websocket URLs are configurable for testnet deployments.
- market-data only (no private trading endpoints yet)

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

### Realtime Trades Stream (WebSocket)

`GET /v1/ws`

The websocket endpoint supports `trades`, `orderbook`, `ohlcv`, and `marketstats`. The first three share upstream exchange streams; statistics use the shared REST acquisition described below, not a native upstream stream.

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
For Bybit `spot`, `linear`, and `inverse`, `params.levels`, `params.depth`, or `params.limit` above `1000` selects `orderbook.full.{symbol}`. The backend buffers deltas, synchronizes them to a full REST snapshot, keeps the complete synchronized book internally, and truncates each client response to its requested depth. Full-depth subscribers for the same Bybit category and symbol share one upstream stream. Options remain limited to 25 levels.


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

Supported exchanges: `binance`, `bybit`, `aster`, `extended`.

Aster supports these futures intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, and `1M`.

Extended supports `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w`, and `1M`. `params.candleType` accepts `trades` (default), `mark-prices`, or `index-prices`; missing mark/index volume is `0.0`. REST uses the top-level `timeframe` and optionally `params.endTime` or `params.until` (milliseconds).

Hyperliquid realtime OHLCV is not enabled yet on websocket fanout; use `POST /v1/fetchOHLCV` for Hyperliquid candles.

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
Bybit `spot`, `linear`, and `inverse` requests above `1000` levels, up to `10000`, use `GET /v5/market/full_orderbook`. The full endpoint has no upstream `limit` parameter; FERRIS truncates its CCXT-like response to the requested depth. Requests at or below `1000` retain the limited endpoint, and options remain capped at 25 levels.


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

Snapshot endpoint for exchange symbols/market metadata. Symbols are always returned in canonical `BASE/QUOTE` format (uppercase, no settlement suffix like `:USDT`).

Hyperliquid and Binance statistics rows include `marketId`, `exchangeMarketId`, `category`, `dex`, `contractType`, `settle`, and `settlementAssetId`. Use the opaque catalog-issued `marketId` to select statistics; never derive it from display symbols. Hyperliquid primary perps have `dex: ""`; Binance USDⓈ-M perps have `category: null`, `dex: null`, and `contractType: "PERPETUAL"`. Binance IDs are exact JSON tuples such as `["binance","perp",null,null,"BTCUSDT"]`; native punctuation and Unicode are preserved. Binance settlement and `settlementAssetId` come from `marginAsset`, or are null when unresolved. Deferred adapters omit the identity extension.


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

Binance statistics cover active USDⓈ-M futures whose exact native `contractType` is `PERPETUAL`; selected known inactive perpetuals remain selectable and report implemented fields as unavailable with reason `inactive-market`. The catalog is authoritative: obtain IDs from `fetchMarkets` and pass those IDs to `fetchMarketStats` or the `marketstats` WebSocket topic. Do not send display symbols in statistics requests.

Example using a catalog-issued BTC perpetual ID:

```json
{"exchange":"binance","marketIds":["[\"binance\",\"perp\",null,null,\"BTCUSDT\"]"],"fields":["funding","markPrice","indexPrice"],"params":{}}
```

Binance funding preserves the native decimal string as `currentUnclassified` and now reports `rateUnit: decimalFraction`. For symbols with a qualified `fundingIntervalHours`, `rateIntervalMs` and `paymentIntervalMs` identify the native eight-hour or exception interval. `equivalents` provides exact decimal-string, simple-linear percentage equivalents for one hour, eight hours, one day, and one year; these are derived display values, not additional exchange observations. Missing interval configuration leaves equivalents null. `nextPaymentTimestamp` comes only from positive native `nextFundingTime`. Mark/index prices preserve decimal strings qualified by catalog `baseAsset` and `quoteAsset`. Last-settled funding, last price, volume, open interest, and funding history are unsupported.

The Binance source uses shared REST polling every 30 seconds, with a 90-second freshness boundary. It has no native statistics WebSocket integration. HTTP and WebSocket statistics accept only `params: null` or `{}` (no symbol/coin/category shortcuts); capability discovery reports `sharedPolling`, `pollIntervalMs: 30000`, and `staleAfterMs: 90000`. The backend USDⓈ-M REST base defaults to `https://fapi.binance.com` and may be overridden with `BINANCE_BASE_URL`; this is not a frontend or upstream statistics-WS setting.
### Lighter Market Statistics

Lighter market statistics use exchange ID `lighterxyz` and the native `market_stats` WebSocket through Ferris's coordinator (`upstreamMode: nativeWebSocket`). Pass the exact opaque `marketId` string returned by `fetchMarkets`, for example `["lighterxyz","perp",null,null,"1"]`; `exchangeMarketId` contains the numeric native ID as a string. Display symbols are not statistics identities. `orderBookDetails` supplies the catalog and identity join.

Lighter's native funding values are percentage-valued strings with a one-hour rate basis: `rateUnit: percent`, `rateIntervalMs: 3600000`, and `paymentIntervalMs: 3600000` for currently deployed markets. `current_funding_rate` is an estimate; `funding_rate` is the distinct last-settled rate and may carry `paymentTimestamp`. Both include exact-linear `equivalents` for one hour, eight hours, one day, and annualized simple percentage. Lighter's `-0.0003` therefore means `-0.0003%`, not `-0.03%`. Mark, index, and last prices remain exact numeric text. Bybit, Aster, and Extended remain unsupported for marketstats.

Lighter shares the coordinator's 30-second poll and 90-second stale policy for HTTP and WebSocket demand. See [FUNDING_MARKET_STATS_PLAN.md](FUNDING_MARKET_STATS_PLAN.md) for acceptance gates and verification evidence.

### Market Statistics and Capabilities

`GET /v1/capabilities` enumerates registered adapters without fetching upstream data. Hyperliquid, Binance, and Lighter advertise statistics support; Bybit, Aster, and Extended remain unsupported for marketstats; funding history remains unsupported everywhere. Runtime failures do not change capabilities.

`POST /v1/fetchMarketStats`:

```json
{"exchange":"hyperliquid","fields":["funding","markPrice","indexPrice"],"params":{"dex":""}}
```

For Binance, use the same shape with `"exchange":"binance"`, exact catalog-issued Binance `marketId` values, and only `params: null` or `{}`. Binance is USDⓈ-M `PERPETUAL` only; active all-market enumeration and selected known inactive perp IDs follow the catalog and field-state rules above.

Omitted/null `marketIds` selects all active perpetuals for the requested exchange. For selected markets, pass an array of exact catalog-issued ID strings (1–100 inputs, case-sensitive; duplicates removed). Binance selections are perpetual IDs only; Hyperliquid selections may include its supported spot identities. Omitted/null `fields` means `["funding"]`; empty lists are invalid. Only null, `{}`, or Hyperliquid's `{"dex":""}` params are accepted. REST rejects unknown top-level keys, including `symbol`. Unknown IDs require a complete corresponding catalog; incomplete catalog identity yields 502, not a fabricated 400.

Response: `{timestamp, scope, markets, coverage}`. Scope is exchange-specific (`{"exchange":"hyperliquid","params":{"dex":""}}` or Binance with `{}`). Rows flatten catalog columns and add only requested `fields`. Coverage contains `expectedMarkets`, `returnedMarkets`, `enumerationComplete`, and `sourceFailures`. Cold all-market upstream failure returns 200 with no rows, null expected count, incomplete enumeration, and explicit failures. Known membership is retained on failure. Registered deferred adapters return 501 / `UNSUPPORTED_FEATURE`; errors use flat `{code,message}`.

Each field is `{state,value,reason,exchangeTimestamp,receivedTimestamp,source}`. Available funding and its supported capability have `reason: null`; unsupported, inactive, stale, and upstream-error reasons remain meaningful. Hyperliquid uses an hourly `decimalFraction` basis; Binance uses `decimalFraction` and its qualified per-market interval; Lighter uses an hourly `percent` basis. Funding values include the native `rate`, explicit `rateUnit`, qualified `rateIntervalMs`, payment schedule when known, and optional `equivalents`:

```json
{"rate":"-0.0003","rateUnit":"percent","kind":"estimate","rateIntervalMs":3600000,"paymentIntervalMs":3600000,"paymentTimestamp":null,"nextPaymentTimestamp":null,"equivalents":{"oneHourPercent":"-0.0003","eightHourPercent":"-0.0024","oneDayPercent":"-0.0072","annualizedPercent":"-2.628"}}
```

`equivalents` use exact decimal-string arithmetic and simple linear scaling. Funding preserves the exact signed native string, including zero. Positive means longs pay shorts. Do not treat an equivalent interval as a separate funding event or compound the annualized value.
- `markPrice`/`indexPrice` are mark/index observations with `{amount,baseAsset,quoteAsset}`. Binance uses native catalog assets, independent of settlement. Hyperliquid's oracle quote is USDC for exact HYPE/PURR names, otherwise USDT. Never replace oracle/index with mid price.
- Hyperliquid selected spot funding/last-settled funding are `notApplicable`; Binance spot IDs are rejected. Selected inactive perp observations are `unavailable` / `inactive-market`.
- Perp `volume24h`/`openInterest` remain unsupported. Lighter implements `lastPrice` and perp `lastSettledFunding`; Hyperliquid and Binance do not. Spot non-funding fields remain unsupported.
- One shared acquisition every 30 seconds per exchange serves HTTP and WS clients; Hyperliquid spot metadata and Binance funding-info metadata cache for five minutes. Field receipts change only on upstream observation, not reads. Failures immediately stale retained values; independent 90-second expiry also stales observations, including while a request is pending. Explicit invalid scalars clear values and cannot be resurrected by later failures.
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

Delivery gates, commands, live observations, and the one-additional-exchange procedure are in [FUNDING_MARKET_STATS_PLAN.md](FUNDING_MARKET_STATS_PLAN.md). No frontend implementation or other exchange funding support is included.

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

Extended book depths are clamped to `1..=1000`. One-level subscriptions use `?depth=1`; deeper subscriptions maintain the full indicative stream and return the requested top N. Snapshot/delta synchronization uses contiguous `seq` values (returned as `nonce`); gaps reconnect before further books are published. REST books leave timestamps and nonce null when the upstream omits them.

## Running locally

```bash
cargo run
```

Defaults:

- host: `0.0.0.0`
- port: `8787`
- Hyperliquid base URL: `https://api.hyperliquid.xyz`

## Environment variables

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8787`)
- `HYPERLIQUID_BASE_URL` (default: `https://api.hyperliquid.xyz`)
- `BINANCE_BASE_URL` (default: `https://fapi.binance.com`)
- `EXTENDED_REST_BASE_URL` (default: `https://api.starknet.extended.exchange/api/v1`)
- `EXTENDED_WS_URL` (default: `wss://api.starknet.extended.exchange/stream.extended.exchange/v1`)
- `REQUEST_TIMEOUT_MS` (default: `10000`)
- `TRADE_CACHE_CAPACITY_PER_COIN` (default: `5000`)
- `TRADE_CACHE_RETENTION_MS` (default: `86400000`)
- `TRADE_COLLECTOR_ENABLED` (default: `true`)
- `RUST_LOG` (default: `info`)

## Terminal stream viewers

These helpers are separate from the main server runtime and connect directly to exchange websocket streams.

`market_stream` is websocket-only and supports `trades`, `orderbook`, and `ohlcv` modes.

Trades stream:

```bash
cargo run --bin market_stream -- trades --exchange bybit --coin BTC
```

Order book stream:

```bash
cargo run --bin market_stream -- orderbook --exchange bybit --coin BTC
```
Bybit full-depth order book stream:

```bash
cargo run --bin market_stream -- orderbook --exchange bybit --symbol BTC/USDT:USDT --levels 10000
```


Binance order book stream:

```bash
cargo run --bin market_stream -- orderbook --exchange binance --symbol BTC/USDT:USDT
```

Real-time OHLCV stream:

```bash
cargo run --bin market_stream -- ohlcv --exchange bybit --coin BTC --timeframe 1m
```

Binance real-time OHLCV stream:

```bash
cargo run --bin market_stream -- ohlcv --exchange binance --symbol BTC/USDT:USDT --timeframe 1m
```

Extended perpetual streams:

```bash
cargo run --bin market_stream -- trades --exchange extended --symbol BTC/USD:USD --iterations 10
cargo run --bin market_stream -- orderbook --exchange extended --symbol BTC/USD:USD --levels 10 --iterations 10
cargo run --bin market_stream -- ohlcv --exchange extended --symbol BTC/USD:USD --timeframe 1m --iterations 10
```

The Extended viewer reconnects after disconnects or book sequence gaps. Its candle mode uses trade candles. Override its upstream with `--ws-url`; backend environment variables configure the server, not this direct viewer.

Useful optional flags:

- `--exchange` (default `hyperliquid`)
- `--symbol` (default `BTC/USDC:USDC`)
- `--ws-url` (websocket base URL override; default depends on `--exchange`)
- `--coin` (optional websocket coin override)
- `--levels` (display depth; default `10`; Hyperliquid `10..=20`, Binance `10..=1000`, Aster/Extended `1..=1000`, Bybit `10..=10000`)
- `--limit` (trades dedup buffer hint; also OHLCV candle window size, default `25` for trades, `120` for OHLCV)
- `--timeframe` (OHLCV timeframe, for example `1m`, `5m`, `1h`)
- `--chart-height` (OHLCV chart rows, default `16`)
- `--duration-secs` (stop automatically after N seconds)
- `--iterations` (stop automatically after N updates)

OHLCV websocket timeframe support:

- `binance`: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M`
- `bybit`: `1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M`

Supported exchanges for websocket modes:

- `trades`: `hyperliquid`, `binance`, `bybit`
- `orderbook`: `hyperliquid`, `binance`, `bybit`
- `ohlcv`: `binance`, `bybit`

If you run `ohlcv` with an unsupported exchange (for example `hyperliquid`), the CLI exits with a clear websocket support error.

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

## Docker

Build:

```bash
docker build -t ferris-market-data-backend .
```

Run:

```bash
docker run --rm -p 8787:8787 ferris-market-data-backend
```

## Adding new exchanges

Adapters implement the `MarketDataExchange` trait (`src/exchanges/traits.rs`).

To add an exchange:

1. Create a new module under `src/exchanges/<exchange_name>/`
2. Implement `MarketDataExchange`
3. Register it in `src/main.rs` through `ExchangeRegistry`
4. Reuse shared response models from `src/models.rs`

Each adapter should implement the full snapshot contract:

- `fetchTrades`
- `fetchOHLCV`
- `fetchOrderBook`
- `fetchMarkets`

This keeps the frontend contract stable while exchange integrations evolve independently.

## Notes on Hyperliquid public trades

Hyperliquid `recentTrades` returns only a short recent window. This backend still queries `recentTrades`, but also runs a websocket collector (`trades` channel) and stores data in an in-memory per-coin ring buffer. That allows `fetchTrades` to serve deeper recent history than the raw upstream REST endpoint alone, while `GET /v1/ws` provides shared realtime fanout for market-data channels.
### Binance realtime order books
Binance realtime order books use USDⓈ-M futures symbols only. Requests with `levels`, `depth`, or `limit` up to 20 retain the partial depth streams (`5`, `10`, or `20`). Requests from 21 through 1,000 share one futures diff stream and one REST snapshot requested at `limit=1000`; the backend maintains the synchronized top-1,000 book, returns each subscriber's requested top N levels, and carries the latest Binance `u` update ID in `nonce`.

### Aster futures realtime streams
Aster supports perpetual futures market data only. REST order-book `limit` and realtime `params.levels`, `depth`, or `limit` are clamped to `1..=1000`. All display depths share one synchronized diff stream per market, with each subscriber receiving its requested top N and the latest update ID in `nonce`. The optional `params.coin` accepts a base asset (`BTC` -> `BTCUSDT`) or raw pair (`BTCUSD1`). Raw symbol suffixes `USDT`, `USD1`, and `U` are supported; explicit slash symbols are preserved in trade/order-book responses.

REST trades default to 100 rows (maximum 1000); OHLCV defaults to 200 candles (maximum 1500). `since` filters trades/candles, and OHLCV also accepts `params.until` or `params.endTime`. Market catalogs include only `PERPETUAL` contracts; `includeInactive` includes non-trading perpetuals, not pending rows without a contract type.

For implementation purposes, Aster REST uses `https://fapi.asterdex.com` and websocket streams use `wss://fstream.asterdex.com/ws`. These are backend upstream details; frontend clients always connect through this backend's `/v1/ws` endpoint.

The terminal viewer supports the same limits. For example:

```text
cargo run --bin market_stream -- orderbook --exchange aster --symbol BTC/USDT:USDT --levels 5 --iterations 1
```

Terminal order-book maxima are explicit: Hyperliquid 20, Binance USDⓈ-M futures 1,000, Aster futures 1,000, and Bybit 10,000. Binance Spot, Coin-M futures, and unsupported 5,000-level paths are not included.
