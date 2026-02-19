# Roadmap and Timeline

This document tracks what is done, what is next, and what to watch as this backend grows.

Last updated: 2026-02-19

## How To Use This Doc

- Update this file at least once per week.
- Keep milestone status accurate (`planned`, `in_progress`, `done`, `blocked`).
- Record meaningful scope changes in the Decision Log section.
- Keep timelines realistic; move dates when needed, but note why.

## Vision

Build an open-source, hostable backend that gives web and Electron frontends a CCXT-like way to query exchange market data through one unified API.

## Current Status Snapshot

Status: `in_progress`

Completed:

- Rust backend scaffold (Axum + Tokio)
- Unified endpoints:
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
  - `GET /healthz`
- Hyperliquid adapter integrated
- Binance USDS adapter integrated (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
- Bybit adapter integrated (`fetchTrades`, `fetchOHLCV`, `fetchOrderBook`)
- Websocket trade collector + in-memory trade cache
- `market_stream` websocket parity for trades/orderbook on Hyperliquid, Binance, and Bybit
- Smoke test scripts (Python + PowerShell)
- Live ignored integration tests

In progress:

- Production hardening and public-host readiness
- Exchange expansion follow-up (next adapter candidate after Bybit)

Not started:

- Persistent/shared cache (Redis or similar)
- Public deployment automation

## Milestones

## M0 - Foundation (done)

Target window: 2026-02 (completed)
Status: `done`

Delivered:

- Core API architecture and routing
- Hyperliquid market-data support for trades/OHLCV/order book
- Validation and API error mapping
- Local smoke and live test workflows

Acceptance criteria:

- Frontend can pull market data from one backend contract
- Local smoke checks pass reliably

## M1 - Production Hardening

Target window: 2026-02 to 2026-03
Status: `in_progress`

Scope:

- Add request rate limiting
- Add basic API authentication mode (optional self-host disable)
- Improve readiness checks and startup diagnostics
- Add CI checks (`fmt`, `clippy`, `test`)
- Add structured request IDs for debugging

Acceptance criteria:

- Safe enough for small public deployment
- Reproducible CI quality gates on every PR

## M2 - Data Depth and Reliability

Target window: 2026-03
Status: `planned`

Scope:

- Add persistent/shared cache option (Redis)
- Keep in-memory mode for simple self-host setups
- Add reconnection/collector observability metrics
- Define cache retention defaults by endpoint/use case

Acceptance criteria:

- Multi-instance deployments provide consistent recent trade history
- Restart does not fully reset recent history when persistence is enabled

## M3 - Exchange Expansion

Target window: 2026-03 to 2026-04
Status: `done`

Scope:

- Add exchange #2 using current adapter interface (done: Binance USDS)
- Add exchange #3 (done: Bybit)
- Build adapter checklist/template to speed future integrations (done)

Acceptance criteria:

- New exchange can be added without changing frontend contract
- Endpoint behavior remains consistent across exchanges

## Exchange Adapter Checklist (Template)

Use this checklist every time a new exchange is added.

- Adapter wiring
  - Create `src/exchanges/<exchange>/mod.rs` implementing `MarketDataExchange`
  - Register module in `src/exchanges/mod.rs`
  - Register adapter in `src/main.rs` through `ExchangeRegistry`
  - Add only required SDK features/dependencies in `Cargo.toml`
- Contract and normalization
  - Define symbol input normalization and canonical output symbol mapping
  - Define timeframe mapping (`1m`, `5m`, `1h`, etc.) to exchange-native values
  - Clamp/translate endpoint limits to supported upstream values
  - Preserve raw upstream payload in `info` where relevant
- Endpoint parity
  - `fetchTrades`: map id/timestamp/side/price/amount/cost consistently
  - `fetchOHLCV`: map to `(timestamp, open, high, low, close, volume)` and enforce sorting
  - `fetchOrderBook`: normalize bids descending and asks ascending
  - Honor `since`, `limit`, and relevant `params` consistently
- Websocket parity (tester)
  - Add websocket trades parser + endpoint builder in `market_stream`
  - Add websocket orderbook parser + endpoint builder in `market_stream`
  - Ensure default websocket path works with `--exchange <id>`
  - Fallback to poll mode with clear message if websocket path is not implemented
- Errors and observability
  - Map bad symbol/timeframe/limit cases to `ExchangeError::BadSymbol`
  - Map network/upstream failures to `ExchangeError::UpstreamRequest`
  - Map invalid upstream payloads to `ExchangeError::UpstreamData`
  - Emit adapter-level warnings for skipped malformed rows
- Validation
  - Add unit tests for symbol normalization and payload mapping
  - Add parser tests for websocket trade/orderbook messages
  - Run `cargo test`
  - Run smoke checks (`scripts/smoke_endpoints.py` or `.ps1`) for the exchange
  - Verify `market_stream` trades and orderbook workflows

## M4 - Public Rollout and Operations

Target window: 2026-04
Status: `planned`

Scope:

- Deployment docs for Hetzner/self-host
- Optional Helm/Compose examples
- Monitoring/alerting guidance
- Versioned changelog and release process

Acceptance criteria:

- Team can deploy and operate with a clear runbook
- Users can self-host with minimal setup

## Near-Term Timeline (Next 4 Weeks)

Week 1:

- Finalize CI pipeline
- Add request rate limiting
- Add response/request tracing improvements

Week 2:

- Add optional auth mode
- Add production config examples

Week 3:

- Introduce Redis cache option behind feature/config flag
- Add cache integration tests

Week 4:

- Finalize third exchange adapter (Bybit)
- Expand smoke/live test coverage for multi-exchange routing

## Risk Watchlist

- Upstream API contract drift
  - Impact: parsing/mapping breaks unexpectedly
  - Mitigation: defensive parsing + contract smoke tests
- Memory growth from in-memory cache
  - Impact: instability under high traffic
  - Mitigation: strict per-coin capacity and retention, monitor usage
- Abuse on public endpoint
  - Impact: degraded performance/cost spikes
  - Mitigation: rate limits + optional auth + bot filtering
- Port conflicts on Windows local dev (`8787`)
  - Impact: false 404/confusing local tests
  - Mitigation: standardize local fallback port (`8788`)

## Decision Log

2026-02-17:

- Decided to use CCXT-like raw response shape (`Trade[]`, `OHLCV[]`, `OrderBook`) instead of wrapped metadata envelopes for easier frontend migration.

2026-02-17:

- Decided to ship Hyperliquid first and keep adapter architecture ready for incremental exchange additions.

2026-02-17:

- Decided to include websocket collector + in-memory trade cache now to improve `fetchTrades` depth beyond REST-only windows.

2026-02-18:

- Completed Binance USDS integration for `fetchTrades`, `fetchOHLCV`, and `fetchOrderBook` under the unified contract.

2026-02-18:

- Added a repeatable exchange adapter checklist template to standardize future integrations.

2026-02-19:

- Completed Bybit integration for `fetchTrades`, `fetchOHLCV`, and `fetchOrderBook` under the unified contract.

2026-02-19:

- Added Bybit websocket parity in `market_stream` for both `trades` and `orderbook` modes.

## Weekly Update Template

Copy this block weekly and keep old entries below.

Date:

- Status (`green` | `yellow` | `red`):
- Completed this week:
- In progress:
- Blockers:
- Timeline changes:
- Next week focus:

## Definition of Done (for each milestone item)

- Code merged and formatted
- Tests added/updated and passing
- Smoke workflow validated
- README and AGENTS docs updated if behavior changed
