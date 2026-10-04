# Frontend guide to the rewritten Ferris backend

Updated 2026-10-03. For the agent adapting an existing frontend to the deployed
CCXT-backed backend. This is a client contract and migration guide, not a request
to restore the old native backend or work around missing data on the server.

**Render each supported statistic independently.** One unsupported field must not
hide a market or discard its other statistics. For example, Hyperliquid has no
supported `lastPrice`, but its perpetual funding, mark/index prices, quote volume,
and open-interest amount can still populate the screener.

Sections: [markets and identity](#2-list-markets-from-an-exchange),
[screener HTTP](#3-populate-a-market-screener),
[screener WS](#4-live-screener-statistics-separate-reducer-from-candlestrades),
[charts](#5-charts-historical-bootstrap-plus-live-candle-upserts),
[trades](#6-recent-trades-and-the-trade-tape),
[books](#7-order-book-snapshot-and-updates),
[lifecycle/errors](#8-websocket-lifecycle-and-errors), and
[current limitations](#9-current-limitations-and-frontend-decisions).

## 1. Connection and endpoint map

The backend implementation changed; the hosted URLs did not:

- HTTP base: `https://api.latticeterminal.com`
- WebSocket: `wss://api.latticeterminal.com/v1/ws`
- Local development backend: `http://localhost:8787` / `ws://localhost:8787/v1/ws`

Send HTTP POST bodies as JSON with `Content-Type: application/json`. Always send
`exchange` explicitly; otherwise it defaults to `hyperliquid`. Public exchange
IDs are `binance`, `bybit`, `hyperliquid`, `aster`, `lighterxyz`, and `extended`.
Use `lighterxyz`, not CCXT's internal name `lighter`.

| Purpose | Endpoint | Successful response |
| --- | --- | --- |
| List markets / metadata | `POST /v1/fetchMarkets` | `{exchange, markets, timestamp}` |
| Discover statistics support | `GET /v1/capabilities` | `{exchanges: [...]}` |
| Screener statistics snapshot | `POST /v1/fetchMarketStats` | `{timestamp, scope, markets, coverage}` |
| Historical / initial candles | `POST /v1/fetchOHLCV` | Bare array of six-cell candle arrays |
| Recent trades snapshot | `POST /v1/fetchTrades` | Bare array of trade objects |
| Order book snapshot | `POST /v1/fetchOrderBook` | Book object with `bids` and `asks` |
| Live updates | `GET /v1/ws` WebSocket upgrade | Channel-specific messages below |
| Backend health | `GET /healthz` | `{"status":"ok"}`; not an exchange-availability check |

There is no generic ticker endpoint in this contract. The catalog does not carry
live screener numbers: load statistics separately. Response examples below are
representative shapes with sample values, not fixed prices or permanent market
lists. Any shortened response is explicitly marked as an excerpt.

## 2. List markets from an exchange

### Request: Bybit linear perpetuals

`POST /v1/fetchMarkets`:

```json
{
  "exchange": "bybit",
  "params": {"category": "linear", "type": "perp"},
  "includeInactive": false
}
```

Representative response showing one market:

```json
{
  "exchange": "bybit",
  "markets": [
    {
      "exchange": "bybit",
      "symbol": "BTC/USDT",
      "base": "BTC",
      "quote": "USDT",
      "type": "perp",
      "active": true,
      "minOrderSize": 0.001,
      "tickSize": 0.1,
      "contractSize": 1.0,
      "info": {
        "category": "linear",
        "rawSymbol": "BTCUSDT",
        "exchangeSymbol": "BTCUSDT"
      },
      "marketId": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
      "exchangeMarketId": "BTCUSDT",
      "category": "linear",
      "dex": null,
      "contractType": "LinearPerpetual",
      "settle": "USDT",
      "settlementAssetId": "USDT"
    }
  ],
  "timestamp": 1791013744410
}
```

### Identity rules: essential for both charts and screeners

- `symbol` is a **display pair**, not a unique market key. Spot, perpetuals,
  expiry futures, options, or settlement variants can share it.
- Use `(exchange, marketId)` as the row/selection identity. `marketId` is an opaque
  **string**, even though its contents look like a JSON array. Store and send the
  exact string returned by the catalog; do not construct, parse/rebuild, or
  normalize it.
- For trades/books/candles, pass that `marketId` string in the request's `symbol`
  field. For statistics, pass it in `marketIds`, an array of strings.
- Preserve the selected acquisition profile alongside the identity. A nondefault
  ID does not automatically load its spot/inverse/option profile. Send matching
  `params` on later requests and subscriptions.
- `exchangeMarketId`, `info.rawSymbol`, and `info.exchangeSymbol` are metadata,
  not interchangeable universal keys. For example, Hyperliquid BTC perpetual
  metadata can contain native identity `BTC` and `info.exchangeSymbol: "0"`.
- Use `base`, `quote`, `settle`, `type`, category, and the opaque ID to distinguish
  rows. Preserve Unicode, punctuation, and asset names. Do not append `:USDT` to
  every display pair or guess an exchange symbol.
- Metadata fields such as `minOrderSize`, `tickSize`, and `contractSize` can be
  omitted. Identity members such as `dex` and `settlementAssetId` can be null.
- `includeInactive` defaults to false. The catalog is cached for 30 seconds;
  repeated reads may return the same timestamp.

Refresh any persisted native-backend catalog before using saved selections.
Hyperliquid spot IDs now use `@index`; Lighter uses numeric native IDs and CCXT's
USDC naming. Rediscover IDs instead of assuming old cached identities still work.

### Exchange/product selection

These are explicit **catalog** profiles for a perpetual screener:

| `exchange` | `params` for `fetchMarkets` |
| --- | --- |
| `binance` | `{"category":"linear","type":"perp"}` |
| `bybit` | `{"category":"linear","type":"perp"}` |
| `hyperliquid` | `{"dex":"","type":"perp"}` |
| `aster` | `{"type":"perp"}` |
| `lighterxyz` | `{"type":"perp"}` |
| `extended` | `{"type":"perp"}`; HTTP User-Agent fix required in the backend release |

For Binance/Bybit inverse perpetuals, use `category: "inverse", type: "perp"`.
For their expiry futures, use the matching contract category and `type: "future"`.
For spot use `type: "spot"`; Binance/Bybit also support `category: "spot"`.
Options use `category: "option"` on Binance/Bybit. Do not combine conflicting
selectors. Extended has no spot profile.

Without explicit selectors, catalogs have different defaults: Binance USD-M
linear contracts; Bybit linear + inverse + spot; Aster perpetuals;
Hyperliquid/Lighter loaded perpetual and spot markets; Extended perpetuals.
**Statistics default to active perpetuals, not necessarily the entire catalog.**
Prefer explicit profiles when joining catalog and statistics results.

## 3. Populate a market screener

### Discover support

`GET /v1/capabilities` returns entries under `exchanges`. Inspect:

- `entry.marketStats.state`: `supported` or `unsupported`.
- `entry.marketStats.fields[market.type][fieldName]`: a capability with
  `state: supported | unsupported | notApplicable` and nullable `reason`.
- `allMarkets.types`, `selectedMarkets.types`, and `selectedMarkets.limit`.
- `limitations`, including Binance's selected-ID-only open interest.
- `pollIntervalMs`, `staleAfterMs`, and `ws` support/limits.

Response excerpt for Hyperliquid (other fields/products omitted):

```json
{
  "exchanges": [
    {
      "exchange": "hyperliquid",
      "marketStats": {
        "state": "supported",
        "scope": {"exchange": "hyperliquid", "params": {"dex": ""}},
        "fields": {
          "perp": {
            "markPrice": {"state": "supported", "reason": null},
            "lastPrice": {
              "state": "unsupported",
              "reason": "stock-ticker-last-is-midpoint"
            }
          }
        },
        "pollIntervalMs": 30000,
        "staleAfterMs": 90000,
        "ws": {"snapshot": true, "delta": true, "maxSubscriptionsPerConnection": 16}
      },
      "fundingRateHistory": {"state": "unsupported", "reason": "adapter-not-implemented"}
    }
  ]
}
```

Capability support is not a promise that today's upstream request succeeds.
Actual observations have separate field states. Funding history is unsupported
on every venue. Do not treat capabilities as a list of candle intervals or an
order-book correctness/availability check.

### Request all active perpetuals in one statistics scope

`POST /v1/fetchMarketStats`:

```json
{
  "exchange": "bybit",
  "params": {"category": "linear"},
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice", "volume24h", "openInterest"]
}
```

Use the matching exchange/profile for other venues, subject to the Binance OI
rule below. Request rules:

- Omitted/null `marketIds`: enumerate active perpetuals by default, or active
  spot/future/option markets when that product is explicitly selected.
- Selected `marketIds`: 1–100 exact catalog-issued strings. An empty array is an
  error, not “all markets.” Selected known inactive markets retain identity and
  implemented fields report `unavailable` / `inactive-market`.
- **Omitted/null `fields` means only `["funding"]`.** Request every desired column.
  Empty or unknown field lists are errors. There are seven supported field names:
  `funding`, `lastSettledFunding`, `markPrice`, `indexPrice`, `lastPrice`,
  `volume24h`, `openInterest`. There is no `priceChange24h`, high/low, or flat
  generic `price` field in this statistics API.
- Statistics accepts `params.type`, `category`, `subType`, and Hyperliquid's
  primary `dex: ""`. It does **not** accept `settle`, `symbol`, `coin`, or arbitrary
  raw CCXT parameters. Do not blindly copy all catalog params into this endpoint;
  use the supported equivalent profile and select exact IDs when needed.
- Bybit defaults to `{"category":"linear"}`. Hyperliquid defaults to primary
  `{"dex":""}`. Binance/Aster/Lighter/Extended default to `{}`. Nondefault
  products need explicit selectors.

### Binance: split bulk statistics from selected open interest

Bulk screener request, without OI:

```json
{
  "exchange": "binance",
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice", "volume24h"]
}
```

OI for selected/visible markets, using IDs from the matching catalog:

```json
{
  "exchange": "binance",
  "marketIds": ["[\"binance\",\"perp\",null,null,\"BTCUSDT\"]"],
  "fields": ["openInterest"]
}
```

Requesting `openInterest` without selected IDs rejects the **whole request** with
HTTP 400 / `VALIDATION_ERROR`: `Binance openInterest requires selected marketIds`.
The same restriction applies to WS subscriptions. Do not turn every screener row
into an all-market OI sweep. Keep the bulk and selected-OI views separate, then
compose their fields by market ID; removing a selection must not delete the
market from the bulk view.

### Response: a market can have both available and unsupported fields

Example selected Hyperliquid request:

```json
{
  "exchange": "hyperliquid",
  "params": {"dex": ""},
  "marketIds": ["[\"hyperliquid\",\"perp\",null,\"\",\"BTC\"]"],
  "fields": ["funding", "markPrice", "lastPrice", "volume24h", "openInterest"]
}
```

Representative response:

```json
{
  "timestamp": 1791014323394,
  "scope": {"exchange": "hyperliquid", "params": {"dex": ""}},
  "markets": [
    {
      "exchange": "hyperliquid",
      "symbol": "BTC/USDC",
      "base": "BTC",
      "quote": "USDC",
      "type": "perp",
      "active": true,
      "tickSize": 1.0,
      "contractSize": 1.0,
      "info": {"rawSymbol": "BTC", "exchangeSymbol": "0"},
      "marketId": "[\"hyperliquid\",\"perp\",null,\"\",\"BTC\"]",
      "exchangeMarketId": "BTC",
      "category": null,
      "dex": "",
      "contractType": null,
      "settle": "USDC",
      "settlementAssetId": null,
      "fields": {
        "funding": {
          "state": "available",
          "value": {
            "rate": "0.0000079364",
            "rateUnit": "decimalFraction",
            "kind": "currentUnclassified",
            "rateIntervalMs": 3600000,
            "paymentIntervalMs": 3600000,
            "paymentTimestamp": null,
            "nextPaymentTimestamp": null,
            "equivalents": {
              "oneHourPercent": "0.0007936400",
              "eightHourPercent": "0.0063491200",
              "oneDayPercent": "0.0190473600",
              "annualizedPercent": "6.9522864000"
            }
          },
          "reason": null,
          "exchangeTimestamp": null,
          "receivedTimestamp": 1791014323394,
          "source": "hyperliquid:ccxt:fetchTickers"
        },
        "markPrice": {
          "state": "available",
          "value": {"amount": "84546.9", "baseAsset": "BTC", "quoteAsset": "USDC"},
          "reason": null,
          "exchangeTimestamp": null,
          "receivedTimestamp": 1791014323394,
          "source": "hyperliquid:ccxt:fetchTickers"
        },
        "lastPrice": {
          "state": "unsupported",
          "value": null,
          "reason": "stock-ticker-last-is-midpoint",
          "exchangeTimestamp": null,
          "receivedTimestamp": null,
          "source": null
        },
        "volume24h": {
          "state": "available",
          "value": {"baseVolume": null, "quoteVolume": 2763292851.782378},
          "reason": null,
          "exchangeTimestamp": null,
          "receivedTimestamp": 1791014323394,
          "source": "hyperliquid:ccxt:fetchTickers"
        },
        "openInterest": {
          "state": "available",
          "value": {"openInterestAmount": 36735.26204, "openInterestValue": null},
          "reason": null,
          "exchangeTimestamp": null,
          "receivedTimestamp": 1791014323394,
          "source": "hyperliquid:ccxt:fetchTickers"
        }
      }
    }
  ],
  "coverage": {
    "expectedMarkets": 1,
    "returnedMarkets": 1,
    "enumerationComplete": true,
    "sourceFailures": []
  }
}
```

Rows flatten the catalog metadata and add `fields`; there is no nested `market`
object. HTTP statistics has no `data` wrapper. Only requested fields are included.

### Column paths, states, and units

| Column | Read from each row | Value type |
| --- | --- | --- |
| Funding | `fields.funding.value.rate` | Decimal string; interpret `rateUnit` and interval |
| Last settled funding | `fields.lastSettledFunding.value.rate` | Same funding shape; Lighter perpetuals only |
| Mark / index / last price | `fields.markPrice.value.amount`, `fields.indexPrice.value.amount`, `fields.lastPrice.value.amount` | Decimal string; assets in `baseAsset` / `quoteAsset` |
| Base / quote volume | `fields.volume24h.value.baseVolume` / `.quoteVolume` | Finite JSON number or null |
| OI quantity / notional | `fields.openInterest.value.openInterestAmount` / `.openInterestValue` | Finite JSON number or null; units depend on product |

Every field object has `state`, `value`, `reason`, `exchangeTimestamp`,
`receivedTimestamp`, and `source`. Handle each cell independently:

| Observation state | Client behavior |
| --- | --- |
| `available` | Display the usable value, including zero; allow null members in numeric objects |
| `unsupported` | Show an unsupported/blank cell or omit that column for this scope; keep other cells |
| `notApplicable` | Show N/A for that product, not a network error |
| `unavailable` | Show missing-data status; use `reason` to explain it |
| `stale` | If a retained value exists, label it stale; never present it as fresh |
| Field absent | Not requested, or unchanged in a WS delta; do not synthesize zero |

Do not require all fields, a non-null exchange timestamp, or a non-null reason
before displaying a row. Available observations normally have `reason: null`.
Missing `exchangeTimestamp` is legitimate; `receivedTimestamp` is a distinct
receipt time, not an exchange event timestamp. `source` names methods such as
`bybit:ccxt:fetchTickers`; do not gate rendering on old native source names.

Funding/price values remain strings; volume/OI objects use numbers, **not strings**.
Both members of each numeric value object are present, but one may be null. Do not
use truthiness to detect availability. Preserve precision during formatting.

Funding uses decimal fractions except Lighter, which uses percent. For example,
`rate: "0.0001", rateUnit: "decimalFraction"` means `0.01%`, whereas Lighter's
`rate: "0.0008", rateUnit: "percent"` means `0.0008%`. Do not multiply both by 100.
`equivalents` may be null; when present its values are already percentages and
simple-linear conversions, not compounded APY. Do not assume every rate is for
8 hours. Respect `kind`, per-market intervals, and nullable payment times; an
estimate or `currentUnclassified` rate is not a settled payment.

| Product | Volume and OI limitations relevant to display |
| --- | --- |
| Binance linear contracts | Base/quote volume; selected OI base amount, notional null |
| Binance inverse contracts | Base volume; absent quote turnover stays null; selected OI amount is **contracts**, not base coins |
| Binance options | Base volume only when explicit contract unit qualifies; quote turnover; selected OI amount is contracts and value is USD |
| Bybit linear contracts | Base/quote volume; OI base amount, notional null |
| Bybit inverse contracts | Base/USD volume; OI amount null, value is USD |
| Bybit options | Base/quote volume; OI base amount, notional null |
| Hyperliquid perpetuals | Quote-only volume; OI base amount, notional null |
| Aster / Lighter perpetuals | Base/quote volume; OI unsupported |
| Extended perpetuals | Base/collateral volume and OI; public HTTP verified with the User-Agent fix |
| Spot | Funding, OI, mark/index are not applicable; use supported last/volume fields; Hyperliquid last remains unsupported |

Never label every `openInterestAmount` as coins or every `openInterestValue` as
USD. The backend does not fill missing notional by multiplying amount and price.
See the complete [numeric unit contract](README.md#numeric-statistics-units).

Statistics bulk sources poll every 30 seconds; Lighter also receives maintained
Pro ticker updates. A WS statistics subscription is not necessarily tick-by-tick
pricing. Fields expire independently after 90 seconds; source failure can stale
them sooner, and scheduled funding estimates can expire at payment time. Repeated
cache reads do not freshen data. HTTP renews a 90-second demand lease; WS holds
demand while subscribed.

Inspect `coverage`, not just HTTP status. Cold all-market upstream failure can
return HTTP 200 with `markets: []`, `expectedMarkets: null`,
`enumerationComplete: false`, and `sourceFailures` entries containing
`{source, reason, message}`. That means an incomplete observation, not a proven
empty exchange. Keep catalog rows available and show the statistics failure.

## 4. Live screener statistics: separate reducer from candles/trades

Subscribe on `/v1/ws`; **do not send `symbol`**:

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "hyperliquid",
  "params": {"dex": ""},
  "fields": ["funding", "markPrice", "indexPrice", "lastPrice", "volume24h", "openInterest"]
}
```

Omit `marketIds` for this all-perpetual view; selected subscriptions use the same
1–100-ID rule as HTTP. Binance bulk subscriptions must exclude `openInterest`.
Use a separate selected-OI subscription when required.

The returned canonical statistics topic has `{exchange, params, marketIds,
fields}`. IDs/fields are normalized and sorted; omitted IDs become null. `channel`
is a command property, not a member of the returned topic.

Wire shapes (`MarketStatsRow` means the complete flattened row shown in section 3):

```text
Acknowledgement:
{type: "subscribed", op: "subscribe", topic}

Initial snapshot:
{type: "marketstats", mode: "snapshot", topic, generation, revision: 1,
 timestamp, scope, markets: MarketStatsRow[], coverage}

Later delta:
{type: "marketstats", mode: "delta", topic, generation,
 previousRevision, revision, timestamp, scope,
 updates: MarketStatsRow[], removedMarketIds: string[], coverage}
```

These messages have **top-level `markets` or `updates`, not `data`**.
`generation` is an opaque string; revisions are numbers.

Reducer rules:

1. Track state separately per canonical statistics topic. The acknowledgement
   precedes its initial snapshot.
2. A snapshot replaces that topic's view and establishes generation/revision.
3. Accept a delta only if generation matches and `previousRevision` equals the
   last applied revision. On mismatch discard the delta, unsubscribe/resubscribe
   (or reconnect) for a new snapshot; sending a duplicate subscribe alone yields
   `alreadySubscribed`, not a resnapshot.
4. Upsert `updates` by `marketId`. Replace each **present field object in full**,
   including a null value. Fields omitted from an update retain their old values.
   New rows and identity/active changes include all requested fields.
5. Remove IDs in `removedMarketIds` from that topic only. Replace `coverage` on
   every message. Do not delete other rows because a sparse delta omitted them.
6. Apply all revisions, even if only receipt timestamps/coverage changed. Updates
   are coalesced, at most once per second; there is no durable replay.
7. If using an HTTP bootstrap, never merge a late HTTP response over an established
   WS generation. Compose separate bulk and selected-OI topics only at the UI
   layer; their revisions are independent.
8. Unsubscribe on unmount with `op: "unsubscribe", channel: "marketstats"` and
   the canonical topic members. Limit: 16 statistics topics per connection.

## 5. Charts: historical bootstrap plus live candle upserts

### Request initial history

`POST /v1/fetchOHLCV` (note the endpoint's capitalization):

```json
{
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "timeframe": "1m",
  "limit": 200,
  "params": {"category": "linear", "type": "perp"}
}
```

Response shape, with two example candles:

```json
[
  [1791001740000, 84498.7, 84498.7, 84475.0, 84484.0, 30.221],
  [1791001800000, 84484.0, 84500.0, 84480.0, 84495.0, 12.5]
]
```

Each row is `[timestampMs, open, high, low, close, volume]`, ordered oldest-first.
The timestamp/OHLC cells are numbers; volume is a number **or null**. Do not read
`response.data`, expect candle objects, reject a candle only for missing volume,
or coerce missing volume into zero. If the chart library uses Unix seconds,
convert milliseconds once at the chart boundary.

For an explicit historical window, add top-level `since` and `params.until`, both
inclusive Unix milliseconds. Example window for 200 one-minute candles:

```json
{
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "timeframe": "1m",
  "since": 1790927280000,
  "limit": 200,
  "params": {"category": "linear", "type": "perp", "until": 1790939279999}
}
```

Those are sample dates, not a permanent bootstrap window. Calculate current
bounds in the frontend. For a fixed-duration interval, request the preceding page
with `since = oldestTimestamp - count * intervalMs` and
`until = oldestTimestamp - 1`, respecting the venue's cap. Merge by timestamp;
exchange retention/gaps can return fewer rows. Calendar intervals such as `1M`
need calendar-aware bounds, not a fixed one-minute formula. An old `since` does not
guarantee unlimited exchange history.

HTTP timeframe precedence: top-level `timeframe`, then `params.timeframe`, then
`params.interval`, then `1m`. Unsupported stock intervals fail. For
Hyperliquid/Lighter/Extended, `since` without an end bound requests `limit`
intervals from `since`; send both bounds when paging a specific older window.

### Subscribe for live updates

```json
{
  "op": "subscribe",
  "channel": "ohlcv",
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "params": {"category": "linear", "type": "perp", "timeframe": "1m"}
}
```

Timeframe is inside `params` for WS, unlike the usual HTTP request. Do not copy
HTTP-only `since`, `until`, or candle `limit` into a live subscription.

Representative data frame (the exact canonical params come from the server):

```json
{
  "type": "ohlcv",
  "topic": {
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "params": {"category": "linear", "type": "perp", "timeframe": "1m"}
  },
  "data": [[1791013800000, 84570.0, 84575.0, 84560.0, 84569.0, 12.3]]
}
```

Use the returned canonical topic; the resolved symbol need not equal your input
ID/alias. **A live frame is not the entire chart history.** Replace a candle with
the same timestamp, append new timestamps, and preserve the rest of the historical
series. The still-open candle can change repeatedly.

Bootstrap REST history and maintain WS updates. If they overlap, buffer live
updates while REST is pending, install the historical series, then apply buffered
updates so a late REST result cannot erase newer data. Isolate requests by
market/timeframe/source so results from a previous chart selection cannot replace
the current series. A warm subscription does not replay saved historical candles.

### Candle support and current limits

| Venue | REST maximum candles per request | Live candles |
| --- | --- | --- |
| Binance | 1,000 | Supported |
| Bybit | 1,000 | Supported except options; option REST candles also unsupported |
| Hyperliquid | 5,000 | Supported |
| Aster | 1,500 | Supported |
| Lighter (`lighterxyz`) | 500 | **Unsupported**; REST candles do work |
| Extended | 10,000 | Implemented; stock WebSocket connector omits required headers and receives 403 |

Default is 200 candles. Positive excessive limits are capped; zero is invalid.
REST source options include contract `price: mark | index | premiumIndex` on
Binance/Bybit and `mark | index` on Aster/Extended. Bybit premium-index is linear
only. Extended also accepts `candleType: trades | mark-prices | index-prices`,
which wins over `price`. Bybit mark/index/premium-index and Extended mark/index
candles can have null volume. WS alternative sources are narrower: Binance
contract `mark | index`, and Extended's listed `candleType` / `price` options;
do not assume every REST source has a live watcher.

### Why history can disappear while live updates work

The deployed compatibility check observed HTTP 400 `BAD_SYMBOL` for bare
`BTC/USDT` on Binance and `BTC/USDC` on Hyperliquid because those display aliases
were ambiguous in the loaded catalog. Qualified BTC perpetual symbols and
catalog-issued IDs succeeded. This is catalog-dependent, not a promise that every
bare display pair always fails.

Check the actual historical request and error body first. Use a discovered ID
and matching profile rather than relying on old symbol heuristics. Then check
that live upserts do not replace the historical array. The prior observation
returned 200 latest and 200 previous-day BTC candles on each of Binance, Bybit,
Hyperliquid, Aster, and Lighter; history was not globally removed by the rewrite.

## 6. Recent trades and the trade tape

`POST /v1/fetchTrades`:

```json
{
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "limit": 100,
  "params": {"category": "linear", "type": "perp"}
}
```

Representative response (raw source `info` abbreviated to an empty object here):

```json
[
  {
    "info": {},
    "amount": 0.01,
    "datetime": "2026-10-03T07:50:00.000Z",
    "id": "sample-trade-id",
    "order": null,
    "price": 84569.0,
    "timestamp": 1791013800000,
    "type": null,
    "side": "buy",
    "symbol": "BTC/USDT:USDT",
    "takerOrMaker": null,
    "cost": 845.69,
    "fee": null
  }
]
```

All trade members other than `info` can be null. A non-null fee is
`{currency: string|null, cost: number|null, rate: number|null}`. REST trades are
newest-first. Optional top-level `since` and end bounds filter the returned
window; they do not manufacture trade history on recent-only APIs. Hyperliquid's
old Ferris history collector/cache is gone.

For live trades, send the same market/profile with `op: "subscribe"`,
`channel: "trades"`, and omit HTTP `limit`/time bounds. Frames are
`{type: "trades", topic, data: Trade[]}`. Batches contain incremental updates,
not a rolling history to replace wholesale. Deduplicate bootstrap/reconnect
overlap by source trade ID within the market/stream when present; delivery is
not exactly-once and missing IDs cannot guarantee collision-free deduplication.
Treat `info` as source-specific data, not the retired native parser schema.

Default REST limit is 100; maxima are 1,000 on Binance/Bybit/Aster/Extended,
100 on Lighter, and 5,000 on Hyperliquid subject to its much smaller actual
recent-trade window. Bybit spot is capped at 60.

### Binance zero-valued trade events

The default stock `trade` stream has been observed returning source rows with
`price: 0`, `amount: 0`, and raw `info.X: "NA", info.st: 1`. Their exact source
semantics are not qualified. **Do not update a displayed last executable trade
price or a trade-derived candle to zero from these rows.** The backend preserves
them and has not silently filtered them.

An explicit aggregated stream is available:

```json
{
  "op": "subscribe",
  "channel": "trades",
  "exchange": "binance",
  "symbol": "[\"binance\",\"perp\",null,null,\"BTCUSDT\"]",
  "params": {"type": "perp", "name": "aggTrade"}
}
```

Aggregation changes trade semantics; it is not a transparent replacement or a
permanent guarantee of positive values. `name` is a Binance WS trade option, not
a REST parameter. See [CCXT-012](CCXT_KNOWN_ISSUES.md#ccxt-012).

## 7. Order book snapshot and updates

`POST /v1/fetchOrderBook`:

```json
{
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "limit": 20,
  "params": {"category": "linear", "type": "perp"}
}
```

Representative response showing one row per side:

```json
{
  "asks": [[84570.0, 1.25]],
  "bids": [[84569.9, 0.75]],
  "datetime": "2026-10-03T07:50:00.000Z",
  "timestamp": 1791013800000,
  "nonce": null,
  "symbol": "BTC/USDT:USDT"
}
```

Side rows are `[price, amount]`. `timestamp`, `datetime`, `nonce`, and `symbol`
are nullable. Do not infer missing values from another channel or treat all
venues' nonce values as the same sequence contract.

Live request:

```json
{
  "op": "subscribe",
  "channel": "orderbook",
  "exchange": "bybit",
  "symbol": "[\"bybit\",\"perp\",\"linear\",null,\"BTCUSDT\"]",
  "params": {"category": "linear", "type": "perp", "levels": 20}
}
```

Frames are `{type: "orderbook", topic, data: Book}`. **Replace the book** with
each complete view. These are not exchange-level deltas for a frontend
synchronizer. Do not apply old additive-size or native sequence logic. If REST
bootstrap and WS overlap, the late REST response must not replace a newer WS book.

| Venue | REST maximum per side | WS maximum per side |
| --- | --- | --- |
| Binance | 1,000 contracts / 5,000 spot | 1,000 contracts / 5,000 spot |
| Bybit | 10,000; options 25 | 1,000; options 100 |
| Hyperliquid | 20 | 20 |
| Aster | 1,000 | 20 |
| Lighter | 100 source orders | All retained source levels |
| Extended | 1,000 | 1,000 indicative levels |

These are upper bounds, not guaranteed returned counts. REST depth precedence is
top-level `limit`, then `params.levels`, `depth`, `limit`; WS depth uses only the
latter params. Zero/over-depth requests fail instead of silently clamping. Use
an explicit small depth such as 20 unless the view needs more. REST defaults to
100 except Hyperliquid 20 / Bybit options 25; WS defaults to 20 except Bybit
1,000 / Bybit options 100.

Lighter REST returns individual orders, so repeated prices need not be duplicate
rows. Its REST timestamp/nonce are absent; WS nonce is stock `offset`, not the old
native `nonce`. Extended REST time is receipt time; its WS uses `ts`/`seq`.
Extended standard books are indicative, not RFQ execution liquidity. Its
`BASE/USD` catalog display can resolve to `BASE/USDC:USDC` trade/book output;
do not rewrite the catalog's denomination based on that output symbol.

**Known book correctness risks remain.** Stock Binance can accept a broken
post-bridge continuity link and publish incorrect liquidity while updates keep
arriving. Other retention/sequence qualifications remain incomplete. An
acknowledgement, increasing nonce, or fresh timestamp is not proof of a correct
book. No frontend parsing adjustment repairs these upstream issues. See the
[current issue register](CCXT_KNOWN_ISSUES.md), especially CCXT-001 through 003.

## 8. WebSocket lifecycle and errors

Use one application-level connection where practical. The supported channels are
`trades`, `orderbook`, `ohlcv`, and `marketstats`; `funding` is not a channel alias.

- `subscribed` / `alreadySubscribed` / `unsubscribed` acknowledgements carry
  `{type, op, topic}`. They acknowledge demand, not historical data delivery or
  synchronized book correctness.
- Trade/book/candle topics have `{exchange, symbol, params}`; statistics topics
  have `{exchange, params, marketIds, fields}`. Returned topics have **no
  `channel` property**. Keep the channel in local subscription state and key by
  channel plus canonical topic, not only the display symbol.
- Trade/book/candle topics use the resolved stock symbol and retain normalized
  request params. Save the returned topic and the discovery profile for future
  requests; do not require the returned symbol to equal the input ID/alias.
- To unsubscribe, send `op: "unsubscribe"`, the channel, and the canonical topic
  members. Release demand on unmount or exchange/product changes.
- `{"op":"ping"}` receives `{"type":"pong"}`.
- Reconnect with backoff and resubscribe active views. Statistics needs a new
  generation/full snapshot. No durable replay is available.
- `CLIENT_LAGGED` warnings mean data was dropped. `UPSTREAM_ERROR` means lost
  source continuity; suspend trusted continuity until fresh data arrives and
  resnapshot/reconnect as the view requires.
- Limits: 200 trade/book/candle subscriptions and 16 statistics subscriptions per
  connection; the outgoing queue is bounded at 256 messages. Queue exhaustion
  can close the socket. Do not open one WS connection per screener row.

Error frame example:

```json
{"type":"error","code":"SUBSCRIBE_FAILED","message":"source subscription failed"}
```

WS codes also include `INVALID_MESSAGE`, `INVALID_COMMAND`, `INVALID_TOPIC`,
`NOT_SUBSCRIBED`, `UPSTREAM_ERROR`, `SUBSCRIPTION_LIMIT`, and statistics validation/
unsupported errors. Surface the backend's message; do not silently turn an error
into an empty data array.

Ordinary HTTP API errors are flat:

```json
{"code":"BAD_SYMBOL","message":"ambiguous hyperliquid symbol `BTC/USDC`; use a catalog marketId or explicit product/settlement"}
```

The **catalog endpoint is different**. An unregistered exchange returns HTTP 422:

```json
{"error":{"code":"INVALID_EXCHANGE","message":"Exchange 'foo' is not supported"}}
```

Ordinary unsupported exchanges return HTTP 400 / `UNSUPPORTED_EXCHANGE`.
Other common statuses: 400 validation/bad symbol; 501 unsupported feature, source,
interval, or depth; 502 upstream request/data failure. Invalid JSON/schema or a
proxy response may not use the ordinary API JSON envelope; check HTTP status and
content type before decoding success data.

## 9. Current limitations and frontend decisions

| Situation | Expected frontend handling |
| --- | --- |
| Hyperliquid `lastPrice` unsupported | Show funding, mark/index, quote volume, and OI amount independently. Leave Last unavailable; if showing mark in a general Price column, label it Mark rather than pretending it is Last. |
| Binance OI requires selected IDs | Keep bulk funding/prices/volume working; request OI only for selected markets. |
| Aster/Lighter OI unsupported | Show the rest of the row, not a failed screener. |
| A numeric value member is null | Show that component as missing; keep the other member and other metrics. |
| Only funding appears | Check whether `fields` was omitted; funding is the default, not all fields. |
| Catalog loads but numbers do not | Check the separate statistics request, profile, ID join, field states, coverage, and WS reducer. |
| Candles live but history absent | Inspect REST errors/ambiguous aliases and ensure live frames upsert rather than replace history. |
| Lighter live candles absent | Expected unsupported WS channel; REST history exists. Do not treat it as a reconnect problem. |
| Extended upstream failure | HTTP missing-User-Agent 403 is fixed and deployed; stock WebSocket headers remain ineffective ([CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004)). Older releases may expose 502 or incomplete statistics coverage. Show unavailable data, not invented prices or a successful empty catalog. |
| Funding history / 24-hour price change missing | Not supplied by this contract; do not look for renamed equivalents. |
| Binance zero-price trade row | Do not use it as an executable-price print. Preserve the distinction from an ordinary trade. |
| Book stream appears healthy | Known stock correctness risks still apply; do not claim synchronization was proved by UI activity. |

No backend fallback, synthetic missing values, source-name remapping, or global
“all metrics required” rule is needed to consume the supported data. Keep
unsupported data distinct from temporary upstream failure and from a client
request/decoder bug.

## 10. Handoff checks and evidence boundary

The 2026-10-03 deployed compatibility investigation observed:

- Latest and previous-day BTC one-minute histories: HTTP 200 and 200 ordered
  candles each on Binance, Bybit, Hyperliquid, Aster, and Lighter.
- Ambiguous display aliases failing on Binance/Hyperliquid; qualified perpetual
  symbols and catalog-ID requests succeeding.
- Populated bulk funding/prices/volume on those five venues, with the documented
  Hyperliquid last-price and Aster/Lighter OI exclusions; selected Binance OI
  available, and bulk OI requests rejected with the stated validation error.
- Hyperliquid WS one-candle updates plus statistics snapshot revision 1 followed
  by delta revision 2, with the distinct envelopes documented here.

While preparing this guide, the explicit perpetual catalog profiles were exercised
on all five reachable venues. The documented Bybit latest/bounded-history, trade,
and 20-level book requests passed, as did Bybit bulk statistics, Binance bulk and
selected-OI requests, and the partial Hyperliquid statistics row. The four JSON
WS subscription examples delivered their respective channels, including an
ordered statistics snapshot/delta, and all four canonical unsubscribe commands
were acknowledged.

These original guide checks do not diagnose the exact frontend bug without its
requests/code, qualify every product/source/interval, or prove book correctness.
Subsequent Extended User-Agent verification passed public HTTP catalog, statistics,
trades, candles, and books on an isolated patched backend. The fix was then deployed;
public production catalog, complete BTC statistics, REST books, and Hyperliquid WSS
passed. Stock Extended WebSocket connections still fail because the connector
omits headers; see [CCXT-004](CCXT_KNOWN_ISSUES.md#ccxt-004).

Frontend acceptance checklist:

- Discover markets for the chosen exchange/profile; use current opaque IDs.
- Render a Hyperliquid row even when Last is unsupported and some numeric members
  are null. Zero remains a valid available value.
- Verify every requested column's actual response path and state; a valid row
  must not be discarded for a missing exchange timestamp.
- Keep Binance bulk statistics alive alongside selected OI.
- Observe historical candles before/alongside live upserts; changing the chart's
  instrument/timeframe must not admit a late response for the old selection.
- Handle statistics snapshot/delta revisions separately from `data` messages;
  preserve fields absent from sparse deltas and isolate each topic's removals.
- Replace complete book views, deduplicate trade overlap, and unsubscribe/recover
  subscriptions on unmount/disconnect.
- Display useful HTTP/WS error messages and incomplete-coverage status.

Further references: [integration guide](INTEGRATION_README.md),
[REST contract](README.md#ccxt-rest-snapshot-contract),
[realtime contract](README.md#ccxt-realtime-contract),
[statistics contract](README.md#market-statistics-and-capabilities), and
[known upstream issues](CCXT_KNOWN_ISSUES.md).
