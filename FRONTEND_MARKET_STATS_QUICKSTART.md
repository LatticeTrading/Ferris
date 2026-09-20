# Frontend market-statistics quickstart

Backend endpoints currently support market statistics for **Hyperliquid**, **Binance USDⓈ-M perpetuals**, **Bybit linear/inverse perpetuals**, **Extended perpetuals**, **Aster perpetuals**, and **Lighter perpetual markets**.

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
- `bybit`
- `aster`
- `extended`
- `lighterxyz`

Hyperliquid, Binance, Bybit, Extended, and Aster use `sharedPolling` with a 30-second poll interval and a 90-second stale boundary. Bybit acquisition is shared independently per category. Lighter uses native `market_stats` WebSocket acquisition through the coordinator, with the same poll/stale policy.

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

For Bybit, load each required category separately:

```json
{"exchange":"bybit","includeInactive":false,"params":{"category":"linear"}}
```

Use `"inverse"` for inverse perpetuals. Keep catalog-issued `marketId`, `category`, native base/quote assets, and settlement metadata. Statistics identities are present only on perpetual rows; expiry futures, spot, and options are not statistics selections. The catalog's omitted-category default combines families, but statistics default to linear. Never send an inverse ID with a linear request.

For Extended:

```json
{"exchange":"extended","includeInactive":false,"params":{}}
```

The catalog returns perpetuals with exact opaque IDs such as `["extended","perp",null,null,"BTC-USD"]`. Preserve native base/collateral assets and `info.isRfq`/`info.isOffHours`; off-hours alone does not mean inactive. Settlement keeps the native collateral denomination (currently `USD`) and opaque collateral ID (currently `0x1`), not an inferred token address. Use `includeInactive: true` when inactive selection is needed.

For Aster:

```json
{"exchange":"aster","includeInactive":false,"params":{}}
```

Only exact `PERPETUAL` contracts have catalog identities, for example `["aster","perp",null,null,"BTCUSDT"]`. Preserve native symbols, Unicode, punctuation, base/quote assets, and settlement. `settle` and `settlementAssetId` come from native `marginAsset`; do not infer them from the quote or combine USDT/USD1/U markets. Display symbols can collide, so key rows by `marketId`. Use `includeInactive: true` when inactive selection is needed.

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

## 3. Fetch market statistics

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

### Bybit

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "bybit",
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice"],
  "params": {"category": "linear"}
}
```

Use `{"category":"inverse"}` for inverse contracts; null/empty params canonicalize to linear. For selected markets, add catalog-issued IDs, for example `"marketIds": ["[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]"]` for a matching linear request. All-market enumeration excludes prelisting/inactive contracts; explicitly selected known inactive perpetuals report `inactive-market`.

Funding uses `kind: estimate` and `rateUnit: decimalFraction`. Preserve native strings and per-market intervals; do not assume eight hours. A valid ticker interval takes precedence over instrument metadata, while explicit invalidity leaves intervals/equivalents null without losing the rate. `nextPaymentTimestamp` is the native upcoming payment time. An estimate becomes stale at that boundary, not settled. Last-settled funding, volume, open interest, and funding history remain unsupported for Bybit.

### Extended

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "extended",
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice"],
  "params": {}
}
```

Only null/empty params are supported. For selected markets, add catalog-issued IDs, for example `"marketIds": ["[\"extended\",\"perp\",null,null,\"BTC-USD\"]"]`. No symbol/coin/category/DEX shortcuts or spot IDs are accepted. Explicit known inactive perpetuals report `inactive-market`.

Funding is an exact `decimalFraction` `estimate` with `rateIntervalMs: 3600000`, `paymentIntervalMs: 3600000`, and simple-linear percentage equivalents. Payment and exchange timestamps are null. Native `nextFundingRate` denotes an update, not a qualified payment time; do not invent a countdown. RFQ and order-book markets use the same funding basis. Last price is the native observation, never a substitute mark/BBO; zero or invalid prices are unavailable. Last-settled funding, volume, open interest, and funding history remain unsupported for Extended.

### Aster

```http
POST /v1/fetchMarketStats
Content-Type: application/json

{
  "exchange": "aster",
  "fields": ["funding", "markPrice", "indexPrice"],
  "params": {}
}
```

Only null/empty params are supported. For selected markets, add exact catalog-issued IDs, for example `"marketIds": ["[\"aster\",\"perp\",null,null,\"BTCUSDT\"]"]`. No symbol/coin/category/DEX shortcuts or spot IDs are accepted. Explicit known inactive perpetuals report `inactive-market`.

Funding is an exact `decimalFraction` `estimate`, not the last settled rate. Preserve each market's interval; live schedules include 1h, 4h, and 8h rather than one exchange-wide basis. Missing/invalid configuration leaves intervals/equivalents null without losing a valid rate. `nextPaymentTimestamp` is the native upcoming payment time; crossing it makes an old estimate stale, not settled. A missing/failed premium observation retains its previous funding basis and receipts stale. Mark/index prices use native base/quote assets independently of settlement. Last-settled funding, last price, volume, open interest, and funding history remain unsupported.

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

Lighter funding `current_funding_rate` is an exact native percentage estimate with an hourly basis. `funding_rate` is a separate last-settled observation. Preserve both semantics and the native strings; use `rateUnit` and the supplied `equivalents` for comparable percentage displays.

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
            "rateUnit": "decimalFraction",
            "kind": "currentUnclassified",
            "rateIntervalMs": 28800000,
            "paymentIntervalMs": 28800000,
            "paymentTimestamp": null,
            "nextPaymentTimestamp": 1789228800000,
            "equivalents": {
              "oneHourPercent": "0.0005145",
              "eightHourPercent": "0.004116",
              "oneDayPercent": "0.012348",
              "annualizedPercent": "4.50702"
            }
          },
          "reason": null,
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
  rateUnit: "decimalFraction" | "percent" | null;
  state: string;
  reason: string | null;
  equivalents: {
    oneHourPercent: string;
    eightHourPercent: string;
    oneDayPercent: string;
    annualizedPercent: string;
  } | null;
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
    rateUnit: field?.state === "available" ? value?.rateUnit ?? null : null,
    state: field?.state ?? "unavailable",
    reason: field?.reason ?? null,
    equivalents: field?.state === "available" ? value?.equivalents ?? null : null,
    paymentIntervalMs: value?.paymentIntervalMs ?? null,
    nextPaymentTimestamp: value?.nextPaymentTimestamp ?? null,
    receivedTimestamp: field?.receivedTimestamp ?? null,
  };
}
```

Render the rate only when `state === "available"`. Suggested UI treatment:

- `available`: show the rate using the exchange's explicit `rateUnit` and `equivalents`.
- `stale`: show the last rate with a stale indicator and `receivedTimestamp`.
- `unavailable`: show `—` and the reason if useful.
- `unsupported` / `notApplicable`: do not render as zero.

Funding rates include the exact native `rate`, `rateUnit`, and optional simple-linear percentage equivalents. `decimalFraction` values such as Hyperliquid/Binance/Bybit/Extended/Aster must be multiplied by 100 for percentage display; `percent` values such as Lighter are already percentage-valued and must not be multiplied by 100. Preserve native strings and do not annualize by assuming a fixed schedule; show `paymentIntervalMs` only when supplied. Missing intervals or derived-arithmetic overflow can leave equivalents null without invalidating the native rate.

## 5. Refresh strategy

For Hyperliquid, Binance, Bybit, Extended, and Aster, REST polling every 30 seconds is sufficient. Ferris WebSocket delivery also exposes stale transitions and qualified payment-boundary transitions without client polling. Extended has no qualified next-payment timestamp. Lighter should use Ferris's WebSocket path when the UI needs progressive updates across many markets; clients still never connect directly upstream.

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

Subscribe to one Bybit category:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "bybit",
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice"],
  "params": {"category": "linear"}
}
```

Add a separate `inverse` topic when needed. REST and WebSocket use the same category/ID validation; use the canonical topic returned in the acknowledgement for unsubscribe.

Subscribe to Extended:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "extended",
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice"],
  "params": {}
}
```

Add `marketIds` for selected catalog rows. All-market and selected topics share the same upstream acquisition; use the acknowledgement's canonical topic to unsubscribe.

Subscribe to Aster:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "aster",
  "fields": ["funding", "markPrice", "indexPrice"],
  "params": {}
}
```

Add exact catalog-issued `marketIds` for selected rows. REST and WebSocket share the same bulk observations and validation; unsubscribe using the acknowledgement's canonical topic.

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
