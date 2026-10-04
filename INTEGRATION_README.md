# Frontend Integration Readme

This guide is for frontend integration (web/Electron/mobile) against this backend.

**Migrating an existing frontend after the backend rewrite? Start with
[FRONTEND_MIGRATION_GUIDE.md](FRONTEND_MIGRATION_GUIDE.md).** It gives endpoint
requests, response shapes, field-by-field screener handling, historical/live
candle rules, identity changes, and current limitations without requiring backend
changes.

Primary goal: consume live market data through backend websocket fanout (`GET /v1/ws`) and use REST endpoints for snapshot bootstrap/fallback.

All five snapshot endpoints and all `/v1/ws` channel acquisitions now use stock CCXT / CCXT Pro `4.5.85`. Read the [REST contract](README.md#ccxt-rest-snapshot-contract), [realtime contract](README.md#ccxt-realtime-contract), and [statistics client flow](#funding-statistics-client-flow): support limits, symbols, optional fields, raw `info`, numeric units, and source/timestamp provenance differ from the former native paths. Known stock book defects remain accepted temporary risks, not proof of synchronization.

### Client-visible differences from the former native backend

- Trade/book `symbol` is the resolved stock CCXT symbol, not an echo of the request alias; catalog `symbol` is a display pair and is not a statistics key.
- Candle volume can be `null` when stock omits it; six positions remain.
- Hyperliquid exposes only the stock recent-trade window (no Ferris history collector/cache); Lighter has no realtime candle watcher, and Bybit option candles are unsupported.
- Lighter WS book nonce is stock `offset`, not the former native `nonce`; Bybit live books cap at 1,000 levels (options 100); Aster live books cap at 20; Extended uses indicative standard books with `ts`/`seq`, not RFQ liquidity.
- Extended REST/live trade and book symbols use `BASE/USDC:USDC` while the catalog display stays `BASE/USD`.
- Statistics use numeric `volume24h`/`openInterest` value objects and venue-specific units; Binance OI requires selected IDs, Aster OI is unsupported, and Lighter OI is two-sided USDC notional with null amount.
- Sources name stock methods (`{exchange}:ccxt:{method}`); missing exchange timestamps stay `null` rather than being fabricated.

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
  - HTTP base: `http://localhost:8787`
  - WS URL: `ws://localhost:8787/v1/ws`
- Hosted:
  - HTTP base: `https://api.latticeterminal.com/`
  - WS URL: `wss://api.latticeterminal.com/v1/ws`

The hosted URL switched to the CCXT backend on 2026-10-03 under the existing
`latticeterminal.service`. Public HTTPS/WSS and CORS/localhost frontend-origin
checks passed; no frontend URL migration is needed. Extended's HTTP User-Agent fix
was deployed later that day: public catalog, complete BTC statistics, and REST
books passed. Stock Extended WebSocket headers remain blocked (CCXT-004).
Binance default trade rows can contain source-supplied
zero price/size (`info.X="NA"`, `info.st=1`); do not use those as executable-price
prints. Explicit `params:{"name":"aggTrade"}` selects the separately supported
aggregated stream, not a server-side default change. See
[deployment and rollback](README.md#hosted-lattice-deployment--2026-10-03) and
[CCXT-012](CCXT_KNOWN_ISSUES.md#ccxt-012).

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
- `POST /v1/fetchMarketStats` (all six venues; qualified products and individual field support are disclosed by `/v1/capabilities`, with selected-ID-only Binance OI)
- Discovery: `GET /v1/capabilities`
- Realtime endpoint:
  - `GET /v1/ws`

Supported realtime channels:
- `trades`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, `lighterxyz`
- `orderbook`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`, `lighterxyz`
- `ohlcv`: `hyperliquid`, `binance`, `bybit`, `aster`, `extended`
- `marketstats`: shared stock 30-second polling on all six venues, plus maintained Lighter Pro `watchTickers` (`sharedPollingAndWebSocket`); no native Ferris statistics transport.
- Extended realtime is perpetual public market data. Public HTTP catalog/statistics/snapshots pass with the User-Agent fix; stock WebSocket connections still receive 403 because their connector omits headers (CCXT-004). REST/live trades and books use `BTC/USDC:USDC`, while the catalog display remains `BTC/USD`. Statistics select opaque IDs.
- Extended's standard websocket order book is indicative, not the RFQ real book. Spot, private trading/account, funding history, and account streams are unsupported.
- Extended upstream REST and websocket URLs are configurable for testnet deployments; frontend clients always use this backend contract.

### CCXT Snapshot Integration

- Pass a catalog-issued `marketId`, native ID, or unambiguous metadata alias in `symbol`. Preserve the catalog product selectors (`type`, `category`, `subType`, `settle`) in subsequent requests; nondefault Binance/Bybit profiles must be selected explicitly even with an opaque ID. Never infer a contract from a display pair.
- Trade/book `symbol` is the resolved stock CCXT symbol, not necessarily the input alias. Realtime uses the same catalog resolution. Do not use string equality with the request as a correctness check.
- Candles still contain six positions; the volume cell can be `null`. Bybit mark/index/premium-index and Extended mark/index candles do not invent volume. Invalid required prices fail with 502 / `UPSTREAM_DATA_INVALID`.
- `since`/end bounds are inclusive milliseconds. Trade ordering is descending, candle ordering ascending. Hyperliquid now exposes only the stock recent-trade window, with no Ferris history collector/cache.
- Use top-level book `limit` before `params.levels`, `depth`, or `limit`. Excess depth returns 501; Lighter is limited to 100 source orders per side, Hyperliquid to 20, and Bybit options to 25. Bybit full books support up to 10,000. Lighter can have multiple order rows at the same price.
- Optional candle sources: Binance/Bybit `price: mark | index | premiumIndex`; Aster/Extended `mark | index`; Extended also accepts `candleType`. Binance linear and Bybit contract books accept `rpi: true`. See the README for product restrictions, maxima, and parameter precedence.
- Catalog identity does not itself imply statistics support. Check capabilities by product: Binance/Bybit qualify spot, perpetuals, expiry futures, and options; Aster/Hyperliquid/Lighter qualify spot and perpetuals; Extended qualifies perpetuals. Individual fields can be unsupported or not applicable.
- Extended REST book time is stock receipt time; WS uses event `ts` and sequence `seq`. Missing stock timestamp/nonce fields remain null. Treat trade `info` as venue/source-specific data, not the old native schema.

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

- `symbol` is a `BASE/QUOTE` display pair built from stock metadata (Extended retains native collateral display naming). Do not normalize its case, punctuation, or Unicode into an identity.
- `symbol` never includes settlement suffix (`:USDT`, `:BTC`) on this endpoint.
- `type` is normalized to one of: `spot | future | perp | option`.
- `includeInactive` defaults to `false` (active markets only).
- Aster catalog defaults to perpetuals; explicit stock-supported product selection is available. Native asset/quote/settlement distinctions remain part of opaque identity even when display symbols collide.
- For Bybit, if `params.category` is omitted backend infers/combines categories and still includes `info.category` per market row.
- Bybit statistics default to `linear`. Use the same explicit category for catalog loading and statistics selection; spot/options and expiry futures require matching selectors. Query field capabilities for the selected product rather than assuming every catalog row supports every metric.

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
  - `UPSTREAM_ERROR` for a source failure/reconnect; suspend trusted continuity until fresh data arrives
  - `SUBSCRIPTION_LIMIT` above 200 realtime subscriptions per connection (16 separately for statistics)
  - Statistics also use `UNSUPPORTED_EXCHANGE` and `UNSUPPORTED_FEATURE`.

## Topic Rules

For `trades`/`orderbook`/`ohlcv` topics, include:

- `channel`: `"trades"`, `"orderbook"`, or `"ohlcv"`
- `exchange`: if omitted, defaults to `"hyperliquid"`
- `symbol`: required
- `params`: object or null

- Resolve catalog-issued `marketId`, native ID, or an unambiguous metadata alias; preserve product selectors. Binance defaults to USD-M; Bybit to linear. `type`, `category`, `subType`, and `settle` select stock-supported profiles, not guessed symbol suffixes.
- `params.coin` overrides the symbol except on Lighter, which uses `market_id`/`marketId`. Extended aliases such as `BTC-USD` or `BTC/USD:USD` resolve to stock `BTC/USDC:USDC`.
- Book display depth precedence: `params.levels`, `depth`, `limit`. Positive values within the [WS depth table](README.md#channels-and-book-depth) are sliced per viewer; zero/over-depth fail rather than clamp. Binance uses a fixed 1,000-level contract/5,000-level spot backing profile. Bybit live maximum is 1,000 (options 100); its former 10,000-level stream is gone. Aster is now stock partial-depth only, maximum 20. Hyperliquid is limited to 20; Extended to 1,000; Lighter exposes retained stock levels.
- All received books are complete views: replace, do not merge as deltas. A shallow view never changes the shared backing depth. Lighter WS nonce is stock `offset`, not the former native `nonce`. Extended WS uses `ts`/`seq` and indicative standard books, not RFQ liquidity. Do not carry old additive-`q` or sequence assumptions into a client-side synchronizer.
- Hyperliquid books accept `nSigFigs`/`mantissa`; distinct aggregation requests are not interchangeable. If stock hashes conflict at one actual URL, Ferris rejects the second acquisition instead of cross-routing data.
- Binance linear books accept `rpi: true`; Binance trades accept `name: trade | aggTrade`.
- Candles use `params.timeframe`, then `interval`, then `1m`; stock-supported native aliases are accepted. Hyperliquid candles are enabled; Lighter and Bybit option candles are unsupported. Binance contract `price: mark | index` and Extended `candleType: trades | mark-prices | index-prices` are supported; Extended `candleType` wins over `price: mark | index`. Do not assume REST-only sources are available on WS.
- Candle rows remain six cells. Missing volume is `null`, including Extended mark/index candles; never invent zero volume. Replace an existing candle by its timestamp.
- Trade batches contain stock incremental updates, not a replay of the whole rolling window. Deduplicate bootstrap/reconnect overlap by the source's identity when needed; delivery is not durable history or an exactly-once guarantee across reconnects.
- `subscribed` acknowledges demand, not trusted book synchronization. Warm viewers share the maintained source and wait for the next update; cached snapshots are not replayed. Cold addition/removal and delayed unsubscribe acknowledgements do not freeze unrelated maintained feeds.
- The outgoing queue is bounded at 256 frames; a full queue closes the connection. Handle `CLIENT_LAGGED`, errors, closure, and resubscription. A duplicate at the 200-subscription limit still returns `alreadySubscribed`; an unsubscribe releases capacity.
- Known stock order-book issues across exchanges remain accepted risks. Binance `4.5.85` can accept a post-bridge `pu` mismatch and publish incorrect liquidity; neither an acknowledgement, increasing nonce, nor continuing updates proves correctness. No Ferris native fallback or synchronizer repairs it. Extended's remaining WebSocket header failure is documented in CCXT-004.
- Reproduction evidence, mitigation limits, and closure criteria live in the [CCXT issue register](CCXT_KNOWN_ISSUES.md); known risks are distinct from already-fixed trade/candle delivery bugs.

Important: use the `topic` returned in `subscribed` ack as your canonical local key when possible.

## Funding Statistics Client Flow

All six registered venues now acquire statistics through stock CCXT/Pro. `volume24h` is supported across their qualified products; OI is supported for Binance selected contracts, Bybit contracts, Hyperliquid perpetuals, Extended perpetuals, and Lighter perpetuals. Aster OI has no stock method. Funding history and premium normalization remain unsupported. Query capabilities by product, not just exchange registration; see [numeric units](README.md#numeric-statistics-units).

1. Get `/v1/capabilities`, then `/v1/fetchMarkets` for authoritative `marketId` strings. Display pairs can collide; statistics use opaque IDs, not `symbol`.

For Binance options, `openInterestAmount` is contracts and `openInterestValue` is USD; do not convert or label them as base/quote automatically. `baseVolume` is null unless explicit catalog contract unit is 1; quote turnover remains available. Option `indexPrice` comes from stock `eapiPublicGetIndex`, not the ticker's settlement-sensitive `exercisePrice`.

For Lighter, use `exchange: "lighterxyz"` and catalog-issued numeric IDs, for example `["lighterxyz","perp",null,null,"1"]`. Stock REST `fetchTickers` and maintained Pro `watchTickers` share the owner service (`sharedPollingAndWebSocket`), with independent field receipts. Funding remains hourly `percent`: `current_funding_rate` is an estimate, `funding_rate` is settled, and `funding_timestamp` is only the latter's payment timestamp. `-0.0003` means `-0.0003%`. OI is venue-style two-sided: `openInterestValue = 2 ×` WS `open_interest` in USDC, with `openInterestAmount: null`; do not double again or use REST's one-sided base OI as notional. REST volume refreshes do not freshen silent live funding/mark/index/OI. `type: "spot"` selects spot, where OI is not applicable.

For Bybit, null/empty params mean `{"category":"linear"}`. `category: "inverse"`, `"spot"`, and `"option"` select other products; `type: "future"` selects expiry futures in a contract category. IDs must match the category, for example `["bybit","perp","linear",null,"BTCUSDT"]`. Default enumeration remains active non-prelisting perpetuals; known inactive selections report `inactive-market`. Stock bulk tickers supply prices, volume and OI. Linear OI is base amount; inverse OI is USD value, **not** base quantity. The absent stock numeric member remains null.

Bybit funding is an exact decimal-fraction `estimate`, not a settled rate. Supplied per-market intervals may change: ticker `fundingIntervalHour` wins over instrument minutes when valid, absence allows metadata fallback, and explicit invalidity leaves intervals/equivalents null without losing the rate. `nextPaymentTimestamp` comes only from the native schedule. At that boundary an old estimate becomes `stale` / `funding-payment-passed`; never relabel it settled. Bybit supports mark/index/last prices qualified by catalog assets, separately from settlement. It has no native statistics WS; REST and Ferris WS share category-scoped bulk polling.

For Extended, use `exchange: "extended"` with null/empty params and exact catalog-issued IDs such as `["extended","perp",null,null,"BTC-USD"]`. Active enumeration includes order-book/RFQ crypto/RWA perpetuals; off-hours alone is not inactivity. Explicit known inactive perpetuals report `inactive-market`. Native `assetName`/`collateralAssetName` define display pairs and price denominations, while `info.isRfq`/`info.isOffHours` preserve execution metadata. Settlement uses the native collateral name and opaque `l2Config.collateralId`, not a guessed chain token address.

Extended funding remains an exact hourly decimal-fraction estimate. Native `nextFundingRate` is an update time, not a payment timestamp. Stock `fetchTickers` supplies prices, volume (base/collateral), and OI (base amount/collateral value). Stock catalog acquisition is separate and shared with other endpoints; do not assume one native HTTP response per poll. An invalid RFQ last price is not replaced with mark/BBO. Last-settled funding/history are unsupported. Public BTC statistics passed with complete coverage after the HTTP User-Agent fix; the remaining stock WebSocket failure is tracked in [CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004).

For Aster, null/empty params default to perpetuals; `type: "spot"` selects spot. Use catalog-issued IDs such as `["aster","perp",null,null,"BTCUSDT"]`, not display symbols. Preserve native Unicode, punctuation, quote and settlement distinctions. Known inactive selections report `inactive-market`.

Aster stock funding/interval methods preserve decimal-fraction estimates and per-market intervals; no assumed eight-hour basis. Native next-payment time drives stale expiry, never settlement conversion. Missing observations retain their original funding basis/receipt. Stock tickers now provide last price and numeric volume. Open interest, last-settled funding, and history remain unsupported.

2. Optional REST bootstrap: `POST /v1/fetchMarketStats`. Omitted `marketIds` enumerates active perpetuals, or the explicitly selected product; selections contain 1–100 opaque catalog IDs. Binance requests containing `openInterest` **must** include selected IDs (otherwise HTTP 400 / `VALIDATION_ERROR`); no all-market OI sweep. Overlapping viewers share per-market observations. Default field is funding; empty IDs/fields are errors. `type`/`category`/`subType` must form a qualified consistent profile, and Hyperliquid accepts only primary `dex: ""`. Never infer product from display text.
3. Subscribe with `{"op":"subscribe","channel":"marketstats","exchange":"hyperliquid","fields":["funding"],"params":{"dex":""}}`; for Binance, Extended, or Aster use the matching exchange and `params: {}` or `null`; for Bybit use `exchange: "bybit"` and the matching category; for Lighter use `exchange: "lighterxyz"`. Optional selections always use exact catalog-issued IDs. Do not send `symbol`. Maximum 16 distinct statistics topics per connection.
4. Expect `subscribed` before the initial snapshot. Replace the view and remember `generation`/`revision`. On a delta, verify generation and `previousRevision`; mismatch means discard/resubscribe. Apply full field-object replacements, keep absent fields unchanged, remove `removedMarketIds`, and replace coverage. Do not merge a later-arriving REST bootstrap into an established WS generation.
5. Statistics coalesce complete states before sparse deltas; new receipt timestamps and coverage changes are observable even with unchanged numbers. On disconnect, reconnect/resubscribe for a new generation/full snapshot; no replay. Unsubscribe by canonical topic on unmount, including after a selected ID disappears.

Treat field states as data, not truthiness. Numeric metrics are `{"baseVolume":number|null,"quoteVolume":number|null}` and `{"openInterestAmount":number|null,"openInterestValue":number|null}`. Both keys are present; zero is available, a missing side is null, and invalid/nonfinite measurements clear the outer value. Do not coerce missing/unsupported observations to zero or assume OI amount is base/value is USD. Funding/price values remain strings. Funding `equivalents` are simple-linear display conversions, not compounded APY or separate exchange observations.

Sources now name stock methods (`{exchange}:ccxt:{method}`); missing exchange time stays null. Hyperliquid price assets follow the CCXT catalog, and `lastPrice` is unsupported rather than publishing midpoint. Available funding/support reasons are null; failures/expiry/inactivity retain specific reasons. Thirty-second bulk polling and independent 90-second field expiry apply, including during pending work; HTTP demand lasts 90 seconds, WS demand is reference-counted. A cache read never freshens a receipt. Apply every WS delta in order even when a bulk receipt update arrives before selected OI.

## Recommended Client Flow

For each legacy trade/book/candle market view:

1. Call `fetchMarkets` for the selected exchange and cache it (refresh around every 30s if needed).
2. Use `(exchange, marketId)` as the UI selection key; `symbol` (`BASE/QUOTE`) is display metadata and can collide.
3. Pass the catalog-issued `marketId` in snapshot/WS `symbol`, with matching product selectors. Use the returned canonical topic for routing; do not guess settlement suffixes or require the resolved symbol to equal the request alias.
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

This example handles the `trades`, `orderbook`, and `ohlcv` realtime channels; `marketstats` uses the generation/revision reducer documented above instead of this shape.

```ts
type RealtimeChannel = "trades" | "orderbook" | "ohlcv"

type Topic = {
  channel: RealtimeChannel
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

// The returned topic has no channel; the message type identifies the data channel.
type WireTopic = Omit<Topic, "channel">

type RealtimeCmd = {
  op: "subscribe" | "unsubscribe"
  channel: RealtimeChannel
  exchange: string
  symbol: string
  params?: Record<string, unknown>
}

type WsMessage = {
  type: string
  topic?: WireTopic
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
      onTrades: (topic: WireTopic, trades: any[]) => void
      onOrderBook: (topic: WireTopic, book: any) => void
      onOhlcv: (topic: WireTopic, candles: any[]) => void
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
- Subscribe to Hyperliquid `ohlcv`, confirm `type="ohlcv"` messages (Hyperliquid candles are enabled through the stock Pro watcher).
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
