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
- `POST /v1/fetchMarketStats` (Hyperliquid, Binance USDⓈ-M PERPETUAL, and Lighter perpetual marketstats)
- Discovery: `GET /v1/capabilities`
- Realtime endpoint:
  - `GET /v1/ws`

Supported realtime channels:
- `trades`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
- `orderbook`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
- `ohlcv`: `binance`, `bybit`, `aster`, `extended`
- `marketstats`: `hyperliquid` and `binance` use shared 30-second REST polling; `lighterxyz` uses native `market_stats` WebSocket (`nativeWebSocket`) through the coordinator. Bybit, Aster, and Extended remain unsupported.
- Extended is perpetual public market-data only: four REST snapshot endpoints plus realtime trades, books, and OHLCV. Use `BTC/USD:USD` for trade/book streams; the market catalog returns `BTC/USD`. Both forms resolve to upstream `BTC-USD`.
- Extended's standard websocket order book is indicative, not the RFQ real book. Spot, private trading/account, funding, and account streams are unsupported.
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

- `symbol` is always canonical `BASE/QUOTE` uppercase.
- `symbol` never includes settlement suffix (`:USDT`, `:BTC`) on this endpoint.
- `type` is normalized to one of: `spot | future | perp | option`.
- `includeInactive` defaults to `false` (active markets only).
- Aster markets are perpetual futures only; use canonical symbols such as `BTC/USDT`.
- For Bybit, if `params.category` is omitted backend infers/combines categories and still includes `info.category` per market row.

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
  - implementation upstreams are REST `https://fapi.asterdex.com` and websocket `wss://fstream.asterdex.com/ws`; frontend clients use this backend's `/v1/ws`
- Extended:
  - accepts `BASE-USD`, `BASE/USD`, and `BASE/USD:USD`; trade/book/realtime output uses `BASE/USD:USD`, while `fetchMarkets` uses `BASE/USD`
  - `params.coin` optionally overrides the symbol with a base asset or complete Extended pair
  - `params.timeframe` defaults to `1m`; supported intervals: `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w`, `1M`
  - `params.candleType`: `trades` (default), `mark-prices`, or `index-prices`; missing volume becomes `0.0`
  - REST candles use top-level `timeframe`; optional `params.endTime`/`params.until` sets an upper millisecond timestamp
  - order-book `levels`, `depth`, or `limit` clamps to `1..=1000`; `1` uses the one-level stream, otherwise the full stream is synchronized before top N is published
  - book snapshots replace state; deltas use absolute `c` when present or additive `q`, remove zero quantities, and reconnect on sequence gaps; synchronized `seq` is returned as `nonce`
  - standard order book is indicative; RFQ real-book endpoints are not supported
  - spot, private trading/account, funding, and account streams are not supported
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

This backend slice ships Hyperliquid, Binance USDⓈ-M perpetual, and selected Lighter perpetual statistics. Query capabilities rather than inferring support from exchange registration. Funding history, volume, open interest, and premium normalization are unsupported until units are qualified; Lighter last-settled funding is distinct from its current estimate. The sibling frontend was not changed.

1. Get `/v1/capabilities`, then `/v1/fetchMarkets` for authoritative `marketId` strings. Display pairs can collide; statistics use opaque IDs, not `symbol`.

For Lighter, use `exchange: "lighterxyz"` and catalog-issued numeric opaque native market IDs (as strings), joined from `metadata`/`orderBookDetails`; display symbols are not IDs. Current funding is an exact estimate (`currentUnclassified` / `rate-basis-unverified`), while last settled funding is separate and may carry `paymentTimestamp`. Current estimates have no payment or next-payment timestamp. Configurable funding periods leave `paymentIntervalMs` null. Mark, index, and last prices are supported; spot funding is `notApplicable`. Lighter uses native WebSocket acquisition with the shared 30-second coordinator poll and 90-second stale boundary. See the plan for acceptance gates and evidence status.
2. Optional REST bootstrap: `POST /v1/fetchMarketStats` with `marketIds` omitted for active supported perps, or a nonempty array of up to 100 catalog IDs. Default field is funding; empty IDs/fields are errors. Binance params are null/`{}` only; Hyperliquid also accepts `{"dex":""}`.
3. Subscribe with `{"op":"subscribe","channel":"marketstats","exchange":"hyperliquid","fields":["funding"],"params":{"dex":""}}`; for Binance use `exchange: "binance"`, exact catalog-issued `marketIds`, and `params: {}` or `null`. Do not send `symbol`. Maximum 16 distinct statistics topics per connection.
4. Expect `subscribed` before the initial snapshot. Replace the view and remember `generation`/`revision`. On a delta, verify generation and `previousRevision`; mismatch means discard/resubscribe. Apply full field-object replacements, keep absent fields unchanged, remove `removedMarketIds`, and replace coverage. Do not merge a later-arriving REST bootstrap into an established WS generation.
5. Statistics coalesce complete states before sparse deltas; new receipt timestamps and coverage changes are observable even with unchanged numbers. On disconnect, reconnect/resubscribe for a new generation/full snapshot; no replay. Unsubscribe by canonical topic on unmount, including after a selected ID disappears.

Treat field states as data, not truthiness: zero funding is available; Hyperliquid selected spot funding is not applicable (Binance spot is outside scope); unsupported OI/volume are not zero; failed/expired retained observations are stale with original receipts. Valid current funding is `currentUnclassified` / `rate-basis-unverified`, with null rate interval. Binance payment interval is only emitted from explicit positive `fundingIntervalHours` converted safely to milliseconds, never a default eight hours; funding-info failure clears the interval without discarding a valid current rate. Next payment comes only from positive native `nextFundingTime`. Binance catalog IDs encode `["binance","perp",null,null,nativeSymbol]`; active exact `contractType: "PERPETUAL"` markets are enumerated, while selected inactive perps report `unavailable` / `inactive-market`. Binance defaults to `https://fapi.binance.com`; operators may override with `BINANCE_BASE_URL`.

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
- Confirm returned `symbol` format is always `BASE/QUOTE` uppercase and does not include `:`.
- On Bybit, confirm each market row has `info.category`.
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
- Treat `fetchMarkets.symbol` as canonical UI key (`BASE/QUOTE`, uppercase, no `:SETTLE`).
- Preserve Bybit `info.category` in local models for contract-family disambiguation.
- Live updates must use websocket `GET /v1/ws` with channel subscriptions (`trades`, `orderbook`, `ohlcv`).
- REST endpoints are snapshot bootstrap/fallback, not live polling transport.
- Implement reconnect+resubscribe and channel-specific merge logic.
- Support message types listed in this guide, including `error` and `warning`.
