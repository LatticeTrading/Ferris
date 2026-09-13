# Frontend market-statistics quickstart

Backend endpoints currently support market statistics for **Hyperliquid**, **Binance USDⓈ-M perpetuals**, and **Lighter perpetual markets**.

Use this flow for a funding-rate widget:

1. Read capabilities.
2. Read each exchange's market catalog and cache the opaque `marketId` values.
3. Fetch active perpetual statistics for each supported exchange.
4. Render rows by `(exchange, marketId)`, not by display symbol alone.
5. Refresh with polling or subscribe to the `marketstats` WebSocket channel.

The frontend should not call exchanges directly. Use the Ferris backend base URL.

## 1. Discover supported exchanges

```http
GET /v1/capabilities
```

Select entries where:

```json
{
  "marketStats": {
    "state": "supported"
  }
}
```

Current supported market-statistics exchanges:

- `hyperliquid`
- `binance`
- `lighterxyz`

Hyperliquid and Binance use `sharedPolling` with a 30-second poll interval and a 90-second stale boundary. Lighter uses native `market_stats` WebSocket acquisition through the coordinator, with the same 30-second freshness policy.

## 2. Load the catalog

```http
POST /v1/fetchMarkets
Content-Type: application/json

{"exchange":"binance","includeInactive":false}
```

For Lighter:

```json
{"exchange":"lighterxyz","includeInactive":false}
```

Lighter perpetual catalog rows use canonical `BASE/USD` symbols and numeric native `market_index` values exposed as opaque string IDs. Preserve each `marketId` exactly as returned; use it for statistics selection and join by `(exchange, marketId)`.

For Hyperliquid:

```json
{"exchange":"hyperliquid","includeInactive":false,"params":{"dex":""}}
```

Use catalog rows for display metadata and authoritative IDs. A Binance catalog row looks like:

```json
{
  "exchange": "binance",
  "symbol": "BTC/USDT",
  "base": "BTC",
  "quote": "USDT",
  "type": "perp",
  "active": true,
  "marketId": "[\"binance\",\"perp\",null,null,\"BTCUSDT\"]",
  "exchangeMarketId": "BTCUSDT",
  "contractType": "PERPETUAL",
  "settle": "USDT",
  "settlementAssetId": "USDT"
}
```

Important:

- Keep `marketId` exactly as returned. Do not construct or normalize it.
- Use `(exchange, marketId)` as the frontend key. `BTC/USDT` can exist on multiple exchanges.
- `symbol` is for display only.
- Omitted `marketIds` means all active supported perpetuals.
- Explicit `marketIds` must be catalog-issued IDs; at most 100 may be sent.

Request funding, mark price, and index price for each exchange. Lighter's native stream can fill rows progressively as each market update arrives; do not require the entire market list before rendering rows.

### Binance

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "binance",
  "fields": ["funding", "markPrice", "indexPrice"],
  "params": {}
}
```

### Hyperliquid

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "hyperliquid",
  "fields": ["funding", "markPrice", "indexPrice"],
  "params": {"dex": ""}
}
```

### Lighter

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "lighterxyz",
  "fields": ["funding", "markPrice", "indexPrice"]
}
```

For a selected market, pass catalog-issued IDs:

```json
{
  "exchange": "lighterxyz",
  "marketIds": ["[\"lighterxyz\",\"perp\",null,null,\"1\"]"],
  "fields": ["funding"]
}
```

Lighter funding `current_funding_rate` is an exact native estimate. `funding_rate` is a separate last-settled observation. Preserve both semantics and do not normalize the rate across exchanges.

The response contains:

```json
{
  "coverage": {
    "expectedMarkets": 571,
    "returnedMarkets": 571,
    "enumerationComplete": true,
    "sourceFailures": []
  },
  "markets": [
    {
      "exchange": "binance",
      "symbol": "BTC/USDT",
      "base": "BTC",
      "quote": "USDT",
      "type": "perp",
      "active": true,
      "marketId": "[\"binance\",\"perp\",null,null,\"BTCUSDT\"]",
      "exchangeMarketId": "BTCUSDT",
      "fields": {
        "funding": {
          "state": "available",
          "value": {
            "rate": "0.00004116",
            "kind": "currentUnclassified",
            "rateIntervalMs": null,
            "paymentIntervalMs": 28800000,
            "paymentTimestamp": null,
            "nextPaymentTimestamp": 1789228800000
          },
          "reason": "rate-basis-unverified",
          "exchangeTimestamp": 1789201320000,
          "receivedTimestamp": 1789201321014,
          "source": "binance:premiumIndex"
        }
      }
    }
  ]
}
```

## 4. Build the widget rows

Conceptual TypeScript:

```ts
type FundingRow = {
  key: string;
  exchange: string;
  marketId: string;
  symbol: string;
  base: string;
  quote: string;
  active: boolean;
  rate: string | null;
  state: string;
  reason: string | null;
  paymentIntervalMs: number | null;
  nextPaymentTimestamp: number | null;
  receivedTimestamp: number | null;
};

function fundingRow(market: any): FundingRow {
  const field = market.fields?.funding;
  const value = field?.value;

  return {
    key: `${market.exchange}:${market.marketId}`,
    exchange: market.exchange,
    marketId: market.marketId,
    symbol: market.symbol,
    base: market.base,
    quote: market.quote,
    active: market.active,
    rate: field?.state === "available" ? value?.rate ?? null : null,
    state: field?.state ?? "unavailable",
    reason: field?.reason ?? null,
    paymentIntervalMs: value?.paymentIntervalMs ?? null,
    nextPaymentTimestamp: value?.nextPaymentTimestamp ?? null,
    receivedTimestamp: field?.receivedTimestamp ?? null,
  };
}
```

Render the rate only when `state === "available"`. Suggested UI treatment:

- `available`: show the rate.
- `stale`: show the last rate with a stale indicator and `receivedTimestamp`.
- `unavailable`: show `—` and the reason if useful.
- `unsupported` / `notApplicable`: do not render as zero.

Funding rates are decimal fractions represented as strings. Preserve the string for display and avoid annualizing, multiplying by 100, or assuming a fixed payment schedule. Positive rates mean longs pay shorts. Show `paymentIntervalMs` only when the backend provides it.

## 5. Refresh strategy

For Hyperliquid and Binance, REST polling every 30 seconds is sufficient. Lighter should use its native WebSocket path when the UI needs rapid progressive updates across many markets.

The backend shares upstream acquisition across clients. Do not make one request per symbol.

For a large Lighter list, render the catalog immediately and fill funding fields as native updates arrive. A field may initially be `unavailable` while its market has not produced a native update; this is not a zero rate or proof that the market is unsupported.

## 6. WebSocket option

Connect to:

```text
GET /v1/ws
```

Subscribe to all Binance funding rows:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "binance",
  "fields": ["funding"],
  "params": {}
}
```

Subscribe to Hyperliquid:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "hyperliquid",
  "fields": ["funding"],
  "params": {"dex": ""}
}
```

Subscribe to Lighter:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "lighterxyz",
  "fields": ["funding"]
}
```

Protocol order:

1. Receive `subscribed`.
2. Receive a complete `marketstats` snapshot with `mode: "snapshot"` and `revision: 1`.
3. Replace the local view with the snapshot.
4. Apply later `mode: "delta"` messages only when `generation` matches and `previousRevision` equals the locally applied revision.
5. Replace complete field objects atomically; absent fields are unchanged.
6. Remove IDs listed in `removedMarketIds`.
7. Reconnect and resubscribe if the revision chain is invalid or the socket closes.

For a first sample widget, REST polling is simpler. Use WebSocket subscriptions when the UI needs continuous updates across many markets.

For a first sample widget, REST polling is simplest for Hyperliquid and Binance. Use the Lighter WebSocket subscription when the UI needs continuous updates or progressive filling across many markets.

## 7. Progressive Lighter rendering

1. Fetch the Lighter catalog once and create all rows immediately.
2. Subscribe to `marketstats` for `lighterxyz`.
3. Replace the complete snapshot when received.
4. Apply each valid delta only when `generation` and `previousRevision` match the local state.
5. Merge field objects by opaque `marketId`; absent fields remain unchanged.
6. Show `—`/loading for `unavailable`; show the exact rate only for `available`.
7. Never interpret `unavailable`, `unsupported`, or `notApplicable` as zero.

This is a read-only market-statistics integration. Trading, order placement, funding history, and authenticated account data are not part of this contract.
