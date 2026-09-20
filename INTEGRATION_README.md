# Frontend Integration Readme

This guide is for frontend integration (web/Electron/mobile) against this backend.

Primary goal: consume live market data through backend websocket fanout (`GET /v1/ws`) and use REST endpoints for snapshot bootstrap/fallback.

## What To Build

- Load market/symbol catalog with `fetchMarkets` at app startup (or periodic refresh).
- Use snapshot endpoints for initial render (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`).
- Open one websocket connection to `GET /v1/ws`.
- Send `subscribe` commands for the channels you need (`trades`, `orderbook`, `ohlcv`, `marketstats`).
- Consume push updates by message type; statistics have a distinct snapshot/delta reducer contract below.
- Reconnect and resubscribe automatically on disconnect.
- Avoid high-frequency polling for live updates.

## Base URLs

Use one of these backend base URL options:

- Self-hosted (localhost):
  - HTTP base: `http://localhost:3000`
  - WS URL: `ws://localhost:3000/v1/ws`
- Hosted:
  - HTTP base: `https://api.latticeterminal.com/`
  - WS URL: `wss://api.latticeterminal.com/v1/ws`

If you only have an HTTP base URL string, derive WS URL like this:

- `http://...` -> `ws://...`
- `https://...` -> `wss://...`
- append `/v1/ws`

## Endpoints

- Snapshot endpoints:
  - `POST /v1/fetchMarkets`
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
- `POST /v1/fetchMarketStats` (Hyperliquid, Binance USDⓈ-M PERPETUAL, Bybit linear/inverse perpetuals, Extended and Aster perpetuals, and Lighter perpetual marketstats)
- Discovery: `GET /v1/capabilities`
- Realtime endpoint:
  - `GET /v1/ws`

Supported realtime channels:
- `trades`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
- `orderbook`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
- `ohlcv`: `binance`, `bybit`, `aster`, `extended`
- `marketstats`: `hyperliquid`, `binance`, `bybit`, `extended`, and `aster` use shared 30-second REST polling; `lighterxyz` uses native `market_stats` WebSocket (`nativeWebSocket`) through the coordinator.
- Extended is perpetual public market-data only: five REST snapshot endpoints plus realtime trades, books, OHLCV, and marketstats. Use `BTC/USD:USD` for trade/book streams; the market catalog returns `BTC/USD`. Both forms resolve to upstream `BTC-USD`; statistics instead select opaque catalog-issued IDs.
- Extended's standard websocket order book is indicative, not the RFQ real book. Spot, private trading/account, funding history, and account streams are unsupported.
- Extended upstream REST and websocket URLs are configurable for testnet deployments; frontend clients always use this backend contract.

### Fetch Markets (Symbol Catalog)

Use this endpoint to populate symbol dropdowns/search and unified market metadata.

Request:

```json
{
  "exchange": "bybit",
  "params": {
    "category": "linear"
  },
  "includeInactive": false
}
```

Response:

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

- `symbol` is a `BASE/QUOTE` display pair. Bybit preserves native asset names, including case, punctuation, and Unicode; do not use normalized display text as statistics identity.
- `symbol` never includes settlement suffix (`:USDT`, `:BTC`) on this endpoint.
- `type` is normalized to one of: `spot | future | perp | option`.
- `includeInactive` defaults to `false` (active markets only).
- Aster markets are exact perpetual contracts; use display symbols such as `BTC/USDT` for legacy market data and opaque catalog-issued `marketId` strings for statistics. Native Unicode/punctuation and quote/settlement variants must remain distinct even if display symbols collide.
- For Bybit, if `params.category` is omitted backend infers/combines categories and still includes `info.category` per market row.
- Bybit statistics instead default to `linear`. Use the same explicit `linear` or `inverse` category for catalog loading and statistics selection. Only perpetual catalog rows carry Bybit statistics identities.

Error payload shape for this endpoint:

```json
{
  "error": {
    "code": "INVALID_EXCHANGE",
    "message": "Exchange 'foo' is not supported"
  }
}
```

## Websocket Protocol

Client commands are JSON text frames.

### Subscribe

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

`channel` values:

- `trades`
- `orderbook`
- `ohlcv`
- `marketstats`

### Unsubscribe

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

### Ping Command (Optional)

```json
{
  "op": "ping"
}
```

### Server Messages

- `type: "subscribed"` + canonical `topic`
- `type: "alreadySubscribed"` + canonical `topic`
- `type: "unsubscribed"` + canonical `topic`
- `type: "trades"` + `topic` + `data: CcxtTrade[]`
- `type: "orderbook"` + `topic` + `data: CcxtOrderBook`
- `type: "ohlcv"` + `topic` + `data: CcxtOhlcv[]`
- `type: "marketstats"` + `mode: "snapshot" | "delta"` + canonical topic/generation/revision (see [statistics contract](README.md#market-statistics-and-capabilities))
- `type: "warning"` with `code: "CLIENT_LAGGED"`
- `type: "pong"`
- `type: "error"` with codes like:
  - `INVALID_MESSAGE`
  - `INVALID_COMMAND`
  - `INVALID_TOPIC`
  - `SUBSCRIBE_FAILED`
  - `NOT_SUBSCRIBED`
  - Statistics also use `UNSUPPORTED_EXCHANGE`, `UNSUPPORTED_FEATURE`, and `SUBSCRIPTION_LIMIT`.

## Topic Rules

For legacy `trades`/`orderbook`/`ohlcv` topics, include:

- `channel`: `"trades"`, `"orderbook"`, or `"ohlcv"`
- `exchange`: if omitted, defaults to `"hyperliquid"`
- `symbol`: required
- `params`: object or null

- Hyperliquid:
  - `params.coin` optional (otherwise inferred from `symbol`)
- Binance USDⓈ-M futures only:
  - `params.coin` optional shortcut (example `"BTC"` -> `BTCUSDT`)
  - order-book `levels`, `depth`, and `limit` values are clamped to `1..=1000`
  - values up to 20 use partial streams; values 21..=1000 share one futures diff stream synchronized with a REST `limit=1000` snapshot
  - deep updates are full CCXT-like snapshots limited to the requested top N and carry the latest Binance `u` in non-null `nonce`
- Aster futures/perpetuals:
  - `params.coin` optional shortcut: base asset `BTC` resolves to `BTCUSDT`; raw pairs such as `BTCUSD1` are also accepted
  - canonical markets use `BASE/QUOTE`; request and realtime symbols may use `BASE/QUOTE:QUOTE`
  - raw symbol quote suffixes `USDT`, `USD1`, and `U` are supported
  - order-book `levels`, `depth`, and `limit` values are clamped to `1..=1000`
  - all display depths share one synchronized upstream diff stream per market; updates contain requested top N levels and the latest `u` as `nonce`
  - supported OHLCV intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`
  - implementation upstreams default to REST `https://fapi.asterdex.com` (configurable with `ASTER_BASE_URL`) and websocket `wss://fstream.asterdex.com/ws`; frontend clients use this backend's `/v1/ws`. Statistics use shared V3 REST polling, not those native streams.
- Extended:
  - accepts `BASE-USD`, `BASE/USD`, and `BASE/USD:USD`; trade/book/realtime output uses `BASE/USD:USD`, while `fetchMarkets` uses `BASE/USD`
  - for trades/books/candles, `params.coin` optionally overrides the symbol with a base asset or complete Extended pair; statistics accept no symbol shortcuts
  - `params.timeframe` defaults to `1m`; supported intervals: `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w`, `1M`
  - `params.candleType`: `trades` (default), `mark-prices`, or `index-prices`; missing volume becomes `0.0`
  - REST candles use top-level `timeframe`; optional `params.endTime`/`params.until` sets an upper millisecond timestamp
  - order-book `levels`, `depth`, or `limit` clamps to `1..=1000`; `1` uses the one-level stream, otherwise the full stream is synchronized before top N is published
  - book snapshots replace state; deltas use absolute `c` when present or additive `q`, remove zero quantities, and reconnect on sequence gaps; synchronized `seq` is returned as `nonce`
  - standard order book is indicative; RFQ real-book endpoints are not supported
  - spot, private trading/account, funding history, and account streams are not supported
- Bybit:
  - `params.coin` optional shortcut
  - `params.category` optional, default `linear`
  - valid `category`: `spot`, `linear`, `inverse`, `option`
Channel-specific params:
- Order book:
  - `params.levels`, `depth`, or `limit` optional; backend clamps/maps per exchange
  - Binance supports USDⓈ-M futures maximum 1,000 levels; Spot, Coin-M, and 5,000-level paths are unsupported
- OHLCV:
  - `params.timeframe` optional; default `1m`
  - Hyperliquid realtime OHLCV is currently unsupported

If you subscribe with `channel="ohlcv"` and `exchange="hyperliquid"`, backend responds with:

```json
{
  "type": "error",
  "code": "SUBSCRIBE_FAILED",
  "message": "exchange `hyperliquid` is not supported for realtime ohlcv yet"
}
```

Use `POST /v1/fetchOHLCV` for Hyperliquid candles (bootstrap/refresh path).
### Aster Terminal Example

The terminal viewer supports Aster perpetual futures market data:

```text
cargo run --bin market_stream -- orderbook --exchange aster --symbol BTC/USDT:USDT --levels 5 --iterations 1
```

### Extended Terminal Examples

```text
cargo run --bin market_stream -- trades --exchange extended --symbol BTC/USD:USD --iterations 10
cargo run --bin market_stream -- orderbook --exchange extended --symbol BTC/USD:USD --levels 10 --iterations 10
cargo run --bin market_stream -- ohlcv --exchange extended --symbol BTC/USD:USD --timeframe 1m --iterations 10
```

The viewer connects directly upstream and uses trade candles. Backend URLs are configured by `EXTENDED_REST_BASE_URL` and `EXTENDED_WS_URL`; the viewer uses `--ws-url` instead. Public Extended requests need no API key; the adapter and viewer send a `User-Agent` header.


Important: use the `topic` returned in `subscribed` ack as your canonical local key when possible.

## Funding Statistics Client Flow

This backend slice ships Hyperliquid, Binance USDⓈ-M perpetual, Bybit linear/inverse perpetual, Extended perpetual, Aster perpetual, and Lighter perpetual statistics. Query capabilities rather than inferring support from exchange registration. Funding history, volume, open interest, and premium normalization remain unsupported; Lighter last-settled funding is distinct from its current estimate.

1. Get `/v1/capabilities`, then `/v1/fetchMarkets` for authoritative `marketId` strings. Display pairs can collide; statistics use opaque IDs, not `symbol`.

For Lighter, use `exchange: "lighterxyz"` and exact catalog-issued `marketId` strings such as `["lighterxyz","perp",null,null,"1"]`; the separate `exchangeMarketId` holds the numeric native ID. `orderBookDetails` supplies the catalog and join. Native funding uses `rateUnit: percent` and qualified one-hour rate/payment intervals; `current_funding_rate` is an estimate, while `funding_rate` is separate settled funding with an optional `paymentTimestamp`. Derived `equivalents` expose one-hour, eight-hour, one-day, and annualized-simple percentages. Lighter's raw `-0.0003` means `-0.0003%`, not `-0.03%`.

For Bybit, use `exchange: "bybit"` with `params: {"category":"linear"}` or `{"category":"inverse"}`. Null/empty params canonicalize to linear; one category per request/subscription, with only matching catalog-issued perpetual IDs. Example ID strings are `["bybit","perp","linear",null,"BTCUSDT"]` and `["bybit","perp","inverse",null,"BTCUSD"]`. Active enumeration excludes prelisting contracts and expiry futures; explicitly selected known inactive perpetuals report `inactive-market`.

Bybit funding is an exact decimal-fraction `estimate`, not a settled rate. Supplied per-market intervals may change: ticker `fundingIntervalHour` wins over instrument minutes when valid, absence allows metadata fallback, and explicit invalidity leaves intervals/equivalents null without losing the rate. `nextPaymentTimestamp` comes only from the native schedule. At that boundary an old estimate becomes `stale` / `funding-payment-passed`; never relabel it settled. Bybit supports mark/index/last prices qualified by catalog assets, separately from settlement. It has no native statistics WS; REST and Ferris WS share category-scoped bulk polling.

For Extended, use `exchange: "extended"` with null/empty params and exact catalog-issued IDs such as `["extended","perp",null,null,"BTC-USD"]`. Active enumeration includes order-book/RFQ crypto/RWA perpetuals; off-hours alone is not inactivity. Explicit known inactive perpetuals report `inactive-market`. Native `assetName`/`collateralAssetName` define display pairs and price denominations, while `info.isRfq`/`info.isOffHours` preserve execution metadata. Settlement uses the native collateral name and opaque `l2Config.collateralId`, not a guessed chain token address.

Extended funding is an exact hourly decimal-fraction `estimate` with one-hour rate/payment intervals and simple-linear percentage equivalents. Payment and exchange timestamps stay null: native `nextFundingRate` is an update timestamp, not a qualified payment time. Mark/index/last prices are native observations; an invalid RFQ last price is never replaced with mark or BBO. One unfiltered bulk market request and 30-second receipt-based cache serve catalog, REST, and Ferris WS demand, with 90-second stale expiry. Missing row statistics retain earlier values stale; explicit invalid scalars clear only their fields. Last-settled funding, volume, open interest, and history remain unsupported.

For Aster, use `exchange: "aster"` with null/empty params and exact catalog-issued IDs such as `["aster","perp",null,null,"BTCUSDT"]`. All-market enumeration includes only `TRADING` exact `PERPETUAL` contracts; known inactive perpetuals remain selectable with `inactive-market`. Preserve native Unicode, punctuation, and quote variants. Native base/quote assets qualify prices independently of `marginAsset`, which supplies both settlement fields; unresolved settlement stays null.

Aster funding is an exact decimal-fraction `estimate` with per-market rate/payment intervals from V3 funding configuration; do not assume eight hours. Missing/invalid/failed configuration leaves intervals/equivalents null without losing a valid rate. Native `nextPaymentTimestamp` drives payment-boundary stale expiry, never conversion to settled funding. Missing/failed premium observations retain their original values, funding basis, and receipts stale; explicit invalid scalars clear only their fields. Three independent shared 30-second success/failure caches serve catalog, REST, and Ferris WS, with 90-second expiry during in-flight work. Mark/index prices are supported; last-settled funding, last price, volume, open interest, and history remain unsupported.

2. Optional REST bootstrap: `POST /v1/fetchMarketStats` with `marketIds` omitted for active supported perps, or a nonempty array of up to 100 catalog IDs. Default field is funding; empty IDs/fields are errors. Binance/Extended/Aster/Lighter params are null/`{}` only; Hyperliquid also accepts `{"dex":""}`; Bybit also accepts `{"category":"linear"}` or `{"category":"inverse"}`.
3. Subscribe with `{"op":"subscribe","channel":"marketstats","exchange":"hyperliquid","fields":["funding"],"params":{"dex":""}}`; for Binance, Extended, or Aster use the matching exchange and `params: {}` or `null`; for Bybit use `exchange: "bybit"` and the matching category; for Lighter use `exchange: "lighterxyz"`. Optional selections always use exact catalog-issued IDs. Do not send `symbol`. Maximum 16 distinct statistics topics per connection.
4. Expect `subscribed` before the initial snapshot. Replace the view and remember `generation`/`revision`. On a delta, verify generation and `previousRevision`; mismatch means discard/resubscribe. Apply full field-object replacements, keep absent fields unchanged, remove `removedMarketIds`, and replace coverage. Do not merge a later-arriving REST bootstrap into an established WS generation.
5. Statistics coalesce complete states before sparse deltas; new receipt timestamps and coverage changes are observable even with unchanged numbers. On disconnect, reconnect/resubscribe for a new generation/full snapshot; no replay. Unsubscribe by canonical topic on unmount, including after a selected ID disappears.

Treat field states as data, not truthiness: zero funding is available; selected spot funding is not applicable; unsupported OI/volume are not zero; failed/expired retained observations are stale with original receipts. Preserve every native rate string and use `rateUnit` before displaying or converting it. `equivalents` are simple linear display conversions, not additional exchange-reported rates and not compounded APY.

Available funding and supported funding capabilities have `reason: null` on all six implemented venues. Runtime failures, stale values, inactive markets, and unsupported fields retain their specific reasons; do not suppress those messages.

## Recommended Client Flow

For each legacy trade/book/candle market view:

1. Call `fetchMarkets` for the selected exchange and cache it (refresh around every 30s if needed).
2. Use the returned canonical `symbol` (`BASE/QUOTE`) as your UI key.
3. Map to exchange websocket/snapshot symbols only when needed in your existing channel params.
4. Call the matching snapshot endpoint once (`fetchTrades`, `fetchOrderBook`, or `fetchOHLCV`).
5. Open websocket (or reuse shared app-level websocket).
6. Send `subscribe` for the same logical market.
7. Merge incoming updates by message type (`trades`, `orderbook`, `ohlcv`).
8. On view unmount, send `unsubscribe`.

## Backpressure And Disconnect Behavior

- Server uses bounded per-connection outgoing queues.
- If your client falls behind, backend can close that websocket connection.
- Treat websocket disconnect as expected operational behavior.
- Always reconnect with backoff and resubscribe all active topics.

Suggested reconnect delays: `500ms, 1s, 2s, 4s, 8s` with jitter and max cap.

## Merge Guidance

Trades can arrive as arrays (`CcxtTrade[]`).

- Apply in receive order.
- Prefer dedupe key: `id` when present.
- Fallback dedupe key: `timestamp|side|price|amount`.
- Keep a bounded dedupe set (for example last 2k-10k keys).

Order book arrives as `CcxtOrderBook` snapshots.

- Replace local book state for that topic with each incoming snapshot.

OHLCV arrives as `CcxtOhlcv[]`.

- Upsert candles by timestamp.
- Keep a bounded in-memory candle window per topic.

## Minimal TypeScript Skeleton

This example handles all currently supported realtime channels.

```ts
type RealtimeChannel = "trades" | "orderbook" | "ohlcv"

type Topic = {
  channel: RealtimeChannel
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

type RealtimeCmd = {
  op: "subscribe" | "unsubscribe"
  channel: RealtimeChannel
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

type WsMessage = {
  type: string
  topic?: Topic
  data?: unknown
  code?: string
  message?: string
}

export class FerrisRealtimeClient {
  private ws?: WebSocket
  private readonly wsUrl: string
  private readonly topics = new Map<string, Topic>()
  private reconnectTimer?: number
  private reconnectDelayMs = 500

  constructor(
    baseHttpUrl: string,
    private handlers: {
      onTrades: (topic: Topic, trades: any[]) => void
      onOrderBook: (topic: Topic, book: any) => void
      onOhlcv: (topic: Topic, candles: any[]) => void
      onError?: (msg: WsMessage) => void
    },
  ) {
    const u = new URL(baseHttpUrl)
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:"
    u.pathname = "/v1/ws"
    u.search = ""
    this.wsUrl = u.toString()
  }

  connect() {
    this.ws = new WebSocket(this.wsUrl)
    this.ws.onopen = () => {
      this.reconnectDelayMs = 500
      for (const t of this.topics.values()) this.send({ op: "subscribe", ...t })
    }
    this.ws.onmessage = (evt) => {
      const msg = JSON.parse(String(evt.data)) as WsMessage
      if (msg.type === "trades" && msg.topic) this.handlers.onTrades(msg.topic, (msg.data ?? []) as any[])
      if (msg.type === "orderbook" && msg.topic) this.handlers.onOrderBook(msg.topic, msg.data)
      if (msg.type === "ohlcv" && msg.topic) this.handlers.onOhlcv(msg.topic, (msg.data ?? []) as any[])
      if (msg.type === "error" && this.handlers.onError) this.handlers.onError(msg)
    }
    this.ws.onclose = () => this.scheduleReconnect()
    this.ws.onerror = () => this.ws?.close()
  }

  subscribe(topic: Topic) {
    const key = JSON.stringify(topic)
    this.topics.set(key, topic)
    this.send({ op: "subscribe", ...topic })
  }

  unsubscribe(topic: Topic) {
    const key = JSON.stringify(topic)
    this.topics.delete(key)
    this.send({ op: "unsubscribe", ...topic })
  }

  private send(cmd: RealtimeCmd) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(cmd))
    }
  }

  private scheduleReconnect() {
    if (this.reconnectTimer != null) return
    const delay = Math.min(this.reconnectDelayMs, 8000)
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = undefined
      this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 8000)
      this.connect()
    }, delay)
  }
}
```

## Quick Validation Checklist

- Call `POST /v1/fetchMarkets`, confirm non-empty `markets`.
- Confirm returned display pairs and native assets are preserved without a settlement suffix; do not assume ASCII-only asset names.
- On Bybit, confirm each market row has `info.category`, and use perpetual `marketId` plus matching category for statistics.
- Connect one frontend client, subscribe, confirm `type="trades"` messages.
- Subscribe to `orderbook`, confirm `type="orderbook"` messages.
- Subscribe to `ohlcv`, confirm `type="ohlcv"` messages.
- Subscribe to Hyperliquid `ohlcv`, confirm explicit `type="error"` with `code="SUBSCRIBE_FAILED"`.
- Connect second frontend client to same topic, confirm both receive updates.
- Confirm backend does not require high-frequency polling for live updates.
- Force disconnect (network toggle), confirm reconnect + resubscribe works.

## LLM Handoff Notes

If another LLM session is integrating frontend code, tell it:

- Use `POST /v1/fetchMarkets` for symbol catalogs and market metadata.
- Treat `fetchMarkets.symbol` as display metadata, not a statistics key. Use `(exchange, marketId)` for statistics; preserve native names and IDs exactly.
- Preserve Bybit `info.category` and identity `category` for contract-family disambiguation; statistics default to linear, not the combined catalog scope.
- Live updates use websocket `GET /v1/ws` with channel subscriptions (`trades`, `orderbook`, `ohlcv`, `marketstats`); statistics use the generation/revision reducer above.
- REST endpoints are snapshot bootstrap/fallback, not live polling transport.
- Implement reconnect+resubscribe and channel-specific merge logic.
- Support message types listed in this guide, including `error` and `warning`.
