# Frontend Integration Readme

This guide is for frontend integration (web/Electron/mobile) against this backend.

Primary goal: consume live market data through backend websocket fanout (`GET /v1/ws`) and use REST endpoints for snapshot bootstrap/fallback.

## What To Build

- Use snapshot endpoints for initial render (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`).
- Open one websocket connection to `GET /v1/ws`.
- Send `subscribe` commands for the channels you need (`trades`, `orderbook`, `ohlcv`).
- Consume push updates by message type (`trades`, `orderbook`, `ohlcv`).
- Reconnect and resubscribe automatically on disconnect.
- Avoid high-frequency polling for live updates.

## Base URLs

Assume backend base URL is exposed as:

- HTTP base: `https://your-backend.example.com`
- WS URL: `wss://your-backend.example.com/v1/ws`

If you only have an HTTP base URL string, derive WS URL like this:

- `http://...` -> `ws://...`
- `https://...` -> `wss://...`
- append `/v1/ws`

## Endpoints

- Snapshot endpoints:
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
- Realtime endpoint:
  - `GET /v1/ws`

Supported realtime channels:

- `trades`: `hyperliquid`, `binance`, `bybit`
- `orderbook`: `hyperliquid`, `binance`, `bybit`
- `ohlcv`: `binance`, `bybit`

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
- `type: "warning"` with `code: "CLIENT_LAGGED"`
- `type: "pong"`
- `type: "error"` with codes like:
  - `INVALID_MESSAGE`
  - `INVALID_COMMAND`
  - `INVALID_TOPIC`
  - `SUBSCRIBE_FAILED`
  - `NOT_SUBSCRIBED`

## Topic Rules

Include all of these for `subscribe`/`unsubscribe`:

- `channel`: `"trades"`, `"orderbook"`, or `"ohlcv"`
- `exchange`: if omitted, defaults to `"hyperliquid"`
- `symbol`: required
- `params`: object or null

Exchange-specific useful params:

- Hyperliquid:
  - `params.coin` optional (otherwise inferred from `symbol`)
- Binance:
  - `params.coin` optional shortcut (example `"BTC"` -> `BTCUSDT`)
- Bybit:
  - `params.coin` optional shortcut
  - `params.category` optional, default `linear`
  - valid `category`: `spot`, `linear`, `inverse`, `option`

Channel-specific params:

- Order book:
  - `params.levels` (or `depth`) optional; backend clamps/maps per exchange
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

Important: use the `topic` returned in `subscribed` ack as your canonical local key when possible.

## Recommended Client Flow

For each market view:

1. Call the matching snapshot endpoint once (`fetchTrades`, `fetchOrderBook`, or `fetchOHLCV`).
2. Open websocket (or reuse shared app-level websocket).
3. Send `subscribe` for the same logical market.
4. Merge incoming updates by message type (`trades`, `orderbook`, `ohlcv`).
5. On view unmount, send `unsubscribe`.

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

- Connect one frontend client, subscribe, confirm `type="trades"` messages.
- Subscribe to `orderbook`, confirm `type="orderbook"` messages.
- Subscribe to `ohlcv`, confirm `type="ohlcv"` messages.
- Subscribe to Hyperliquid `ohlcv`, confirm explicit `type="error"` with `code="SUBSCRIBE_FAILED"`.
- Connect second frontend client to same topic, confirm both receive updates.
- Confirm backend does not require high-frequency polling for live updates.
- Force disconnect (network toggle), confirm reconnect + resubscribe works.

## LLM Handoff Notes

If another LLM session is integrating frontend code, tell it:

- Live updates must use websocket `GET /v1/ws` with channel subscriptions (`trades`, `orderbook`, `ohlcv`).
- REST endpoints are snapshot bootstrap/fallback, not live polling transport.
- Implement reconnect+resubscribe and channel-specific merge logic.
- Support message types listed in this guide, including `error` and `warning`.
