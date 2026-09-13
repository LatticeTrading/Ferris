# Lighterxyz Integration Doc

Last updated: 2026-09-13

This document records the completed `lighterxyz` integration currently shipped by Ferris. The adapter covers the supported REST and WebSocket market-data surfaces below, including native Lighter market statistics. Optional protocol details that are not required by the shipped contract remain documented as follow-up evidence, not implementation blockers.

Important context:

- Do not go searching external docs broadly for this exchange.
- Use only the links and payloads the user has provided.
- Goal is parity in support and code quality with existing adapters like Bybit and Hyperliquid.

## Target Scope

Exchange: `lighterxyz`

Required support:

- REST:
  - trades
  - ohlcv
  - orderbook
  - markets
- WS:
  - trades
  - orderbook
  - native market statistics

## High-Level Integration Decisions

### Canonical public symbols

- Perps should be exposed publicly as `BASE/USD`
  - Example: raw market symbol `APEX` -> canonical symbol `APEX/USD`
- Spot should be exposed publicly as given by the market list
  - Example: `LINK/USDC`
- Internal upstream lookup should use `market_index` / `market_id`, not the public symbol.

### Market lookup model

Lighter does not use a normal public pair symbol for subscription/request routing. Instead, many endpoints use a numeric market identifier.

- Market catalog source: `https://explorer.elliot.ai/api/markets`
- Example payload:

```json
[
  {
    "symbol": "APEX",
    "market_index": 86
  },
  {
    "symbol": "2Z",
    "market_index": 88
  },
  {
    "symbol": "EDEN",
    "market_index": 89
  },
  {
    "symbol": "LINK/USDC",
    "market_index": 2050
  }
]
```

Required adapter behavior:

- Maintain a cached market catalog.
- Refresh roughly every `60-120s`.
- If refresh fails, continue using the last known good snapshot.
- Catalog must support all of:
  - canonical symbol -> market index
  - raw symbol -> market index
  - market index -> canonical symbol

### Market type inference

- If `symbol` contains `/`, treat it as `spot`
- If `symbol` does not contain `/`, treat it as `perp`

### Unified market mapping expectations

For `fetchMarkets`, each row should likely map like this:

- `exchange`: `lighterxyz`
- `symbol`: canonical public symbol
  - perp: `BASE/USD`
  - spot: `BASE/QUOTE`
- `base`: parsed base asset
- `quote`:
  - perp: `USD`
  - spot: parsed quote asset
- `type`:
  - perp rows without `/` -> `perp`
  - rows with `/` -> `spot`
- `info.rawSymbol`: original `symbol` from the market catalog
- `info.exchangeSymbol`: `market_index` as a string

What is still unknown for `fetchMarkets`:

- whether there is an `active` / `inactive` / `delisted` flag available anywhere
- whether spot quotes beyond `USDC` already exist live, or are only expected in the future
- whether `market_index` is stable forever for a market or can change

## Source Links Provided So Far

### Markets

- Catalog endpoint: `https://explorer.elliot.ai/api/markets`

### WebSocket

- Base URL: `wss://mainnet.zklighter.elliot.ai/stream`
- Read-only variant for restricted regions: `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`

### REST

- Order book endpoint: `https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders`

## WebSocket Support

## WS Overview

Confirmed:

- Base URL: `wss://mainnet.zklighter.elliot.ai/stream`
- Read-only URL: `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`
- Recommended operational behavior:
  - implement reconnect logic
  - implement ping/pong handling
  - treat unknown non-data frames defensively

### WS limits

Per-IP limits provided by user:

- Connections: `100`
- Subscriptions per connection: `100`
- Total subscriptions: `1000`
- Max connections per minute: `80`
- Max client-sent messages per minute: `200`
- Max inflight messages: `50`
- Unique accounts: `10`
- Every connection is dropped after `24 hours`

Rate-limit notes:

- REST and WS rate limiting affect each other
- REST rate-limit response can be `HTTP 429`
- Excessive WS messages may result in disconnect
- Cooldown:
  - firewall: `60s`
  - API server cooldown depends on endpoint weight

Implementation notes for this backend:

- Use exponential backoff with jitter on reconnect.
- Reconnect/resubscribe automatically on drop.
- Keep client messages low and avoid subscription churn.
- It is acceptable for v1 to reconnect on forced 24h drop instead of proactively rotating the socket.

## WS Orderbook

User-provided docs excerpt:

```json
{
  "type": "subscribe",
  "channel": "order_book/{MARKET_INDEX}"
}
```

Example subscription:

```json
{
  "type": "subscribe",
  "channel": "order_book/0"
}
```

Response structure:

```json
{
  "channel": "order_book:{MARKET_INDEX}",
  "offset": 41692864,
  "order_book": {
    "code": 0,
    "asks": [
      {
        "price": "3327.46",
        "size": "29.0915"
      }
    ],
    "bids": [
      {
        "price": "3338.80",
        "size": "10.2898"
      }
    ],
    "offset": 41692864,
    "nonce": 4037957053,
    "begin_nonce": 4037957034
  },
  "timestamp": 1766434222583,
  "type": "update/order_book"
}
```

Confirmed rules:

- Subscribe by `market_index`, not symbol.
- Data is sent in batches every `50ms`.
- First message after subscribe is a full snapshot.
- Later messages are state changes only.
- Continuity must be checked with:
  - `current.begin_nonce == previous.nonce`
- `offset` is not a safe continuity signal across reconnects and should not be used for sequence validation.

Expected backend mapping:

- filter on `type = "update/order_book"`
- map `bids[].price` + `bids[].size`
- map `asks[].price` + `asks[].size`
- sort bids descending by price
- sort asks ascending by price
- map `timestamp` -> `CcxtOrderBook.timestamp`
- map `nonce` -> `CcxtOrderBook.nonce`
- map canonical symbol from catalog lookup -> `CcxtOrderBook.symbol`

Expected runtime behavior:

- on nonce gap, reconnect and resubscribe
- do not trust `offset` continuity

## WS Trades

User-provided docs excerpt:

```json
{
  "type": "subscribe",
  "channel": "trade/{MARKET_INDEX}"
}
```

Example subscription:

```json
{
  "type": "subscribe",
  "channel": "trade/0"
}
```

Documented response structure:

```json
{
  "channel": "trade:{MARKET_INDEX}",
  "trades": [Trade],
  "type": "update/trade"
}
```

Example response shown by user:

```json
{
  "channel": "trade:0",
  "trades": {
    "trade_id": 14035051,
    "tx_hash": "189068ebc6b5c7e5efda96f92842a2fafd280990692e56899a98de8c4a12a38c",
    "type": "trade",
    "market_id": 0,
    "size": "0.1187",
    "price": "3335.65",
    "usd_amount": "13.67",
    "ask_id": 41720126,
    "bid_id": 41720037,
    "ask_account_id": 2304,
    "bid_account_id": 21504,
    "is_maker_ask": false,
    "block_height": 2204468,
    "timestamp": 1722339648
  },
  "type": "update/trade"
}
```

Important doc inconsistency:

- Docs say `trades` is an array.
- Example shows `trades` as a single object.
- Implementation should support both object and array shapes.

Expected backend mapping:

- filter on `type = "update/trade"`
- filter channel prefix `trade:`
- `id` <- `trade_id`
- `price` <- `price`
- `amount` <- `size`
- `cost` <- `usd_amount`, fallback `price * size`
- `info` <- raw payload row
- `symbol` <- canonical public symbol from market lookup

Timestamp normalization:

- Example uses `1722339648`, which appears second-based.
- WS trades timestamps should therefore be normalized to milliseconds when needed.
- Implementation should accept either seconds or milliseconds defensively.

Side mapping decision:

- Default interpretation for trade side:
  - `is_maker_ask = true` -> taker side is `buy`
  - `is_maker_ask = false` -> taker side is `sell`

This should be revisited only if live payloads or official docs clearly contradict it.

## WS Heartbeat / Ack / Errors

Current status: implemented defensively for the completed read-only scope.

Confirmed and handled:

- Server keepalive requirements are met through application ping/pong handling.
- Reconnect and resubscribe behavior is implemented for transport loss and the documented 24-hour connection lifetime.
- Unknown non-data frames are ignored unless they clearly indicate failure.
- Explicit acknowledgement/error variants not required by the shared Ferris contract remain intentionally defensive.

## REST Support

## REST Markets

Catalog source confirmed and implemented:

- `https://explorer.elliot.ai/api/markets`
- Fetches periodically and caches the last known good snapshot.
- Builds normalized perpetual and spot market rows with numeric native IDs.

Richer optional metadata such as tick size, contract size, and lifecycle flags is not required by the current read-only contract.
## REST Orderbook

Implemented endpoint:

- `GET https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders`
- Requests `market_id` and a bounded `limit` in the documented `1..250` range.
- Maps `[price, remaining_base_amount]`, sorts bids descending and asks ascending, and derives timestamp/datetime from returned transaction times.
- Top-level non-success codes map to upstream errors.

## REST Trades

Implemented endpoint:

- `GET https://mainnet.zklighter.elliot.ai/api/v1/recentTrades`
- Requests the resolved numeric `market_id` and bounded `limit`.
- Maps trade ID, price, size, cost, side, symbol, raw info, timestamps, and applies `since` filtering.

## REST OHLCV

Implemented endpoint:

- `GET https://mainnet.zklighter.elliot.ai/api/v1/candles`
- Requests the resolved numeric `market_id`, supported resolution, bounded `count_back`, and timestamp window.
- Maps candle arrays into ascending `[timestamp, open, high, low, close, volume]` rows and applies the requested limit/window.


## Completion Status

**Status: COMPLETE for the current Ferris market-data scope.**

### Delivered REST support

- `fetchMarkets` uses the cached Lighter market catalog and resolves numeric `market_index` values internally.
- Perpetual symbols are exposed canonically as `BASE/USD`; spot symbols retain `BASE/QUOTE` form.
- `fetchOrderBook` uses `/api/v1/orderBookOrders`, maps remaining amounts, sorts bids/asks, and normalizes timestamps.
- `fetchTrades` uses `/api/v1/recentTrades`, accepts the documented trade rows, normalizes timestamps, maps side/cost fields, and applies `since`/`limit` handling.
- `fetchOHLCV` uses `/api/v1/candles`, supports the shipped timeframe mapping and bounded window/limit parameters, and returns ascending candles.

### Delivered WebSocket support

- Trades subscribe by numeric `market_index` using `trade/{MARKET_INDEX}`.
- Order books subscribe by numeric `market_index` using `order_book/{MARKET_INDEX}`, preserve snapshot/update sequencing, and reconnect on continuity failure.
- Market statistics use native `market_stats` acquisition through Ferris's shared coordinator. The adapter maps both single-market and multi-market update shapes.
- Native `current_funding_rate` is emitted as the upcoming estimate; native `funding_rate` remains a separate last-settled observation.
- Native numeric strings are preserved without unverified cross-venue normalization.
- Catalog identity and statistics identity share the numeric native market ID; display-symbol collisions cannot substitute for IDs.

### Delivered configuration and integration

- Exchange ID is exactly `lighterxyz`.
- Configurable REST base URL, market-catalog URL, WebSocket URL, catalog refresh interval, and request timeout are wired through `src/config.rs` and `src/main.rs`.
- Lighter is registered in `ExchangeRegistry` and connected to the trade, order-book, and market-statistics paths.
- Focused adapter, market-statistics, realtime WebSocket, and endpoint coverage is present in the Rust test suite.

### Verification record

- `cargo check --all-targets` passed.
- `cargo test lighterxyz -- --nocapture` passed with 12 tests.
- `cargo test market_stats -- --nocapture` passed with 52 tests.
- `cargo fmt` passed.
- Bounded live checks passed for the market catalog, native `market_stats/all` frames, timestamp normalization, and current-versus-settled funding semantics.

### Optional follow-up evidence, not blockers

The following details are not needed for the shipped read-only integration and remain intentionally defensive:

- explicit subscription acknowledgement/error examples beyond the handled runtime behavior;
- heartbeat variants beyond application ping/pong and reconnect handling;
- richer market metadata such as tick size, contract size, or lifecycle flags;
- confirmation that `market_index` remains immutable for the full lifetime of every future market.

These do not block the completed adapter. No credentials or trading functionality are required.

### Delivered file-level implementation

- `src/exchanges/mod.rs`: exports the Lighter adapter module.
- `src/exchanges/lighterxyz/mod.rs`: implements the REST adapter, catalog cache, symbol/ID resolution, and shared exchange wiring.
- `src/exchanges/lighterxyz/statistics.rs`: implements native Lighter market-statistics acquisition and projection.
- `src/realtime.rs`: wires Lighter trade and order-book WebSocket runners.
- `src/config.rs` and `src/main.rs`: expose configuration and register the exchange.
- `tests/market_stats.rs` and focused market-statistics tests: cover adapter, coordinator, projection, HTTP, and WebSocket behavior.

### Handoff

Lighter is complete for this integration slice. Future work can improve progressive partial-snapshot delivery for large all-market lists, but the native per-market stream and current backend support are already integrated and verified. Do not reopen the original implementation blockers unless a new product scope or upstream contract is introduced.
