# Frontend Integration Readme

This guide is for frontend integration (web/Electron/mobile) against this backend.

Primary goal: consume live trades through backend websocket fanout (`GET /v1/ws`) and use REST endpoints for snapshot bootstrap/fallback.

## What To Build

- Use `POST /v1/fetchTrades` for initial trades snapshot.
- Open one websocket connection to `GET /v1/ws`.
- Send `subscribe` commands for trades topics.
- Consume `type="trades"` push updates.
- Reconnect and resubscribe automatically on disconnect.
- Avoid high-frequency polling for live trades.

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
  - `GET /v1/ws` (currently `trades` channel only)

Supported realtime trades exchanges:

- `hyperliquid`
- `binance`
- `bybit`

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

- `channel`: must be `"trades"` for now
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

Important: use the `topic` returned in `subscribed` ack as your canonical local key when possible.

## Recommended Client Flow

For each market view:

1. Call `fetchTrades` once to render initial list.
2. Open websocket (or reuse shared app-level websocket).
3. Send `subscribe` for the same logical market.
4. Merge incoming trades from `type="trades"` into local state.
5. On view unmount, send `unsubscribe`.

## Backpressure And Disconnect Behavior

- Server uses bounded per-connection outgoing queues.
- If your client falls behind, backend can close that websocket connection.
- Treat websocket disconnect as expected operational behavior.
- Always reconnect with backoff and resubscribe all active topics.

Suggested reconnect delays: `500ms, 1s, 2s, 4s, 8s` with jitter and max cap.

## Trade Merge Guidance

Trades can arrive as arrays (`CcxtTrade[]`).

- Apply in receive order.
- Prefer dedupe key: `id` when present.
- Fallback dedupe key: `timestamp|side|price|amount`.
- Keep a bounded dedupe set (for example last 2k-10k keys).

## Minimal TypeScript Skeleton

```ts
type Topic = {
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

type SubscribeCmd = {
  op: "subscribe" | "unsubscribe"
  channel: "trades"
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

export class FerrisTradesClient {
  private ws?: WebSocket
  private readonly wsUrl: string
  private readonly topics = new Map<string, Topic>()
  private reconnectTimer?: number
  private reconnectDelayMs = 500

  constructor(baseHttpUrl: string, private onTrades: (topic: Topic, trades: any[]) => void) {
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
      for (const t of this.topics.values()) this.send({ op: "subscribe", channel: "trades", ...t })
    }
    this.ws.onmessage = (evt) => {
      const msg = JSON.parse(String(evt.data))
      if (msg.type === "trades") this.onTrades(msg.topic, msg.data ?? [])
    }
    this.ws.onclose = () => this.scheduleReconnect()
    this.ws.onerror = () => this.ws?.close()
  }

  subscribe(topic: Topic) {
    const key = JSON.stringify({ channel: "trades", ...topic })
    this.topics.set(key, topic)
    this.send({ op: "subscribe", channel: "trades", ...topic })
  }

  unsubscribe(topic: Topic) {
    const key = JSON.stringify({ channel: "trades", ...topic })
    this.topics.delete(key)
    this.send({ op: "unsubscribe", channel: "trades", ...topic })
  }

  private send(cmd: SubscribeCmd) {
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
- Connect second frontend client to same topic, confirm both receive updates.
- Confirm backend does not require high-frequency polling for live trades.
- Force disconnect (network toggle), confirm reconnect + resubscribe works.

## LLM Handoff Notes

If another LLM session is integrating frontend code, tell it:

- Live trades must use websocket `GET /v1/ws` and `channel="trades"`.
- REST endpoints are snapshot bootstrap/fallback, not live polling transport.
- Implement reconnect+resubscribe and dedupe.
- Support message types listed in this guide, including `error` and `warning`.
