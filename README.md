# Ferris Market Data Backend

Open-source Rust backend that gives your frontend a CCXT-like way to fetch snapshots and subscribe to shared realtime market-data streams.

Planning and delivery tracking lives in `ROADMAP.md`.

Frontend integration guidance lives in `INTEGRATION_README.md`.

This first version focuses on:

- unified endpoint shapes (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
- backend websocket fanout for realtime channels (`GET /v1/ws`)
- pluggable exchange adapter architecture
- market-data support for `hyperliquid`, `binance`, and `bybit`
- one upstream websocket stream per active topic with multi-client broadcast
- background websocket trade collector with in-memory ring buffer for deeper snapshot history

## Why this exists

Frontend apps (including Electron and web frontends) often cannot directly use server-side exchange SDK behavior the same way backend code does. This service acts as a hostable bridge:

- you can run it for public users (for example on Hetzner)
- users can self-host their own backend
- your frontend uses one clean API contract regardless of exchange
- app clients subscribe to backend websocket topics instead of high-frequency HTTP polling for live updates

## Realtime Architecture

- app client connects once to `GET /v1/ws` and sends `subscribe` / `unsubscribe` commands
- backend topic manager keys subscriptions by exchange + channel + symbol (+ params)
- one upstream websocket stream is maintained per active topic
- multiple client connections subscribed to the same topic receive the same broadcast updates
- idle topics are closed, and dropped upstream streams reconnect with backoff

## Current scope

- Snapshot endpoints (bootstrap/fallback):
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
- Realtime endpoint:
  - `GET /v1/ws` (websocket subscribe/unsubscribe for realtime `trades`, `orderbook`, `ohlcv`)
- Realtime channel support:
  - `trades`: `hyperliquid`, `binance`, `bybit`
  - `orderbook`: `hyperliquid`, `binance`, `bybit`
  - `ohlcv`: `binance`, `bybit`
- exchange supported:
  - `hyperliquid` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
  - `binance` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
  - `bybit` (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
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

The websocket endpoint supports realtime `trades`, `orderbook`, and `ohlcv` channels. Backend topic fanout is shared, so multiple app clients subscribing to the same topic use one upstream exchange stream.

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

Supported exchanges: `binance`, `bybit`.

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

### Health

`GET /healthz`

```json
{
  "status": "ok"
}
```

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

Useful optional flags:

- `--exchange` (default `hyperliquid`)
- `--symbol` (default `BTC/USDC:USDC`)
- `--ws-url` (websocket base URL override; default depends on `--exchange`)
- `--coin` (optional websocket coin override)
- `--levels` (order book depth to display; default `10`, min `10`, max `20`)
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

This keeps the frontend contract stable while exchange integrations evolve independently.

## Notes on Hyperliquid public trades

Hyperliquid `recentTrades` returns only a short recent window. This backend still queries `recentTrades`, but also runs a websocket collector (`trades` channel) and stores data in an in-memory per-coin ring buffer. That allows `fetchTrades` to serve deeper recent history than the raw upstream REST endpoint alone, while `GET /v1/ws` provides shared realtime fanout for market-data channels.
