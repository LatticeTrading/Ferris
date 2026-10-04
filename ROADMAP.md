# Roadmap and Timeline

This document tracks what is done, what is next, and what to watch as this backend grows.

Last updated: 2026-10-03

## How To Use This Doc

- Update this file at least once per week.
- Keep milestone status accurate (`planned`, `in_progress`, `done`, `blocked`).
- Record meaningful scope changes in the Decision Log section.
- Keep timelines realistic; move dates when needed, but note why.

## Vision

Build an open-source, hostable backend that gives web and Electron frontends one unified contract for market data: snapshot fetch endpoints plus shared realtime websocket fanout.

## Current Status Snapshot

Status: `in_progress`

CCXT migration phase tracking is authoritative in [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md). Phases 1–4 are user-approved; Phase 5 implementation is `awaiting_review`. The user separately authorized production deployment for frontend testing, and the CCXT release now runs as the existing `latticeterminal.service` at `api.latticeterminal.com`. Integrated checks and locked release/container verification passed; the production edge serves HTTPS snapshots and WSS mixed data/statistics for the five available public venues. Extended retains its upstream 403 limitation. Final retained suite: 118 tests (119 passed before one incidental test was removed; final 73-library + shutdown pass succeeded). Formal Phase 5 acceptance, actual frontend testing and broader operational qualification remain pending.

Per-URL owners, owned feeds, epoch recovery, incremental trades/candles, bounded queues, borrowed book views, and receipt-based revisioned statistics retain Ferris's delivery responsibilities. Known stock order-book issues—including Binance post-bridge `pu` continuity—remain accepted risks, not correctness passes. No dependency patch/native fallback is used.

Completed:

- Rust backend scaffold (Axum + Tokio)
- Unified endpoints:
  - `POST /v1/fetchTrades`
  - `POST /v1/fetchOHLCV`
  - `POST /v1/fetchOrderBook`
  - `GET /healthz`
- All six venues (`binance`, `bybit`, `hyperliquid`, `lighterxyz`, `aster`, `extended`) acquire REST snapshots, realtime streams, and statistics through stock `ccxt`/`ccxt-pro` `4.5.85`, with default features disabled and only the six venue features enabled
- CCXT core/catalog, REST snapshot, realtime streaming, and statistics phases (1–4) delivered and user-accepted; Lighter statistics use maintained stock Pro `watchTickers` sharing the per-URL owner
- Numeric `volume24h`/`openInterest` value schemas delivered over HTTP and revisioned WS with venue-specific units; Binance OI is selected-market-only and Aster/Lighter OI remain explicitly unsupported
- Removed in the cutover: native exchange modules, native realtime/statistics transports and synchronizers, Hyperliquid's extra trade collector/cache, Lighter's Explorer catalog, `src/bin/market_stream`, `src/bin/orderbook_probe.rs`, `src/binance_orderbook.rs`, `src/bybit_full_orderbook.rs`, `src/ws_shared.rs`, and the `binance-sdk`/`crossterm` dependencies
- Stock Hyperliquid public recent-trade snapshots; the former collector/cache and retention settings were removed in the CCXT cutover
- Extended perpetual public market-data adapter: trades, OHLCV, order-book, and market snapshots plus realtime trades, books, and OHLCV
- Extended stock trade/book symbols (`BASE/USDC:USDC`), native catalog display (`BASE/USD`), and configurable upstream URLs
- Extended indicative standard websocket order book support (RFQ real-book stream excluded)
- Funding market-statistics delivery ledger complete for the six approved venue slices: Hyperliquid, Binance, Lighter, Bybit, Extended, and Aster
- CCXT Pro realtime fanout with shared feeds across viewers and one coherent mutable owner per actual stock URL:
  - `trades`: Hyperliquid, Binance, Bybit, Aster, Extended, Lighter
  - `orderbook`: Hyperliquid, Binance, Bybit, Aster, Extended, Lighter
  - `ohlcv`: Hyperliquid, Binance, Bybit, Aster, Extended
- Smoke test scripts (Python + PowerShell)
- Live ignored integration tests

In progress:

- CCXT release deployed to `latticeterminal.service` at `api.latticeterminal.com` with user authorization for frontend testing; Phase 5 formal acceptance remains pending
- Production hardening and public-host readiness
- Realtime stream hardening (limits, backpressure, visibility)
- Extended scope is perpetual public market data only; spot, private trading/account, funding history, account streams, and RFQ real-book endpoints remain unsupported.

Not started:

- Persistent/shared cache (Redis or similar)
- Public deployment automation
- Additional backend websocket fanout channels beyond current scope (for example liquidations/ticker)

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

The former trade-history persistence scope below is superseded by the CCXT migration: stock trade windows only, no Ferris collector/history cache. This milestone is not authorization to restore it.

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

## Exchange Maintenance

The previous native-adapter and terminal-viewer checklist is superseded by [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md). All six registered venues are fully CCXT-backed; no native exchange module, raw stream parser, synchronizer, or direct terminal tool remains. Use stock CCXT methods, the shared metadata/DTO boundary, and backend HTTP/WS smoke checks. Do not add handwritten exchange HTTP clients or raw stream parsers. Additional exchanges and broader verification require their own authorization; the migration targets six venues only.

## M4 - Public Rollout and Operations

Target window: 2026-04
Status: `planned`

Scope:

- Deployment docs for Hetzner/self-host
- Optional Helm/Compose examples
- Monitoring/alerting guidance
- Versioned changelog and release process
- Single production cutover: build the locked release, deploy it in place of the previous release, and roll back to the previous release artifact if needed (no embedded native-provider fallback)

Acceptance criteria:

- Team can deploy and operate with a clear runbook
- Users can self-host with minimal setup

## Near-Term Timeline (Next 4 Weeks)

Week 1:

- Finalize CI pipeline
- Add request rate limiting
- Add response/request tracing improvements
- Add websocket stream metrics (active clients, active topics, dropped/lag events)

Week 2:

- Add optional auth mode
- Add production config examples
- Add per-connection and per-topic websocket guardrails

Week 3:

- Introduce Redis cache option behind feature/config flag
- Add cache integration tests
- Add orderbook/ohlcv stream reliability tests and reconnect soak checks

Week 4:

- Improve startup diagnostics and request tracing for operator debugging
- Publish production config examples and rollout checklist
- Add topic-level observability dashboards for `trades`, `orderbook`, and `ohlcv`

## Risk Watchlist

- Upstream API contract drift
  - Impact: parsing/mapping breaks unexpectedly
  - Mitigation: defensive parsing + contract smoke tests
- Memory growth from retained live state
  - Impact: instability under high traffic
  - Mitigation: bounded shared topics (512 updates), 256-frame client queues, 200 realtime subscriptions per client (16 statistics), 128 URL owners and 200 feeds per URL, idle-topic teardown; stock trade windows are not retained as Ferris history
- Repeated client polling load for live data
  - Impact: unnecessary upstream and backend request amplification
  - Mitigation: use `GET /v1/ws` realtime fanout for live trades; keep REST for bootstrap/fallback
- Abuse on public endpoint
  - Impact: degraded performance/cost spikes
  - Mitigation: rate limits + optional auth + bot filtering
- Port conflicts on Windows local dev (`8787`)
  - Impact: false 404/confusing local tests
  - Mitigation: standardize local fallback port (`8788`)

## Decision Log

2026-10-03:

- Phase 5 submitted for review after explicit authorization. Native modules, transports, synchronizers, terminal tools and obsolete settings are removed. Fixed SIGTERM waiting behind stalled HTTP: close/join client WS work, stop statistics, cancel/join live/catalog owners, then drain HTTP. The actual six-venue server exited 0 with active mixed/statistics clients; `tests/shutdown.rs` retains the stalled-request regression. Docker now pins Rust 1.98.1, copies Cargo.lock, builds one locked release binary with one Cargo job, and excludes nested target caches. Image `ferris:phase5-review` (`a782887bb0d3`) passed unprivileged runtime HTTP/WS and shutdown smoke. Known stock book risks and Extended public API limitation remain; user acceptance/deployment are pending.
- User separately authorized production cutover for frontend testing. Built/tagged `ferris:lattice-ccxt-20261003`, installed its verified release binary at a stable versioned path, preserved the prior executable and validated rollback unit, and restarted the existing enabled `latticeterminal.service`. Caddy/DNS remained unchanged; origin now binds loopback. Public HTTPS/WSS/CORS and five-venue snapshots, mixed streams and revisioned statistics passed. Extended's known public 403 persists. Binance default trade events with source-supplied zero price/size were recorded as [CCXT-012](CCXT_KNOWN_ISSUES.md#ccxt-012); no silent filtering/default change was deployed. Actual frontend testing and explicit Phase 5 acceptance remain pending.

2026-10-02:

- Phase 2 replaces the four public market-data snapshot paths with stock CCXT, removes the extra Hyperliquid trade collector/history cache and its settings, and retires both direct terminal tools. Realtime/statistics remain at their authorized phase boundaries; phase approval records are in `FERRIS_V2_PLAN.md`.

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

2026-02-20:

- Refactored `market_stream` into modules and removed all poll transport paths so the tester is websocket-only.

2026-02-20:

- Added real-time websocket OHLCV mode to `market_stream` for Binance and Bybit, including candle parsing, timeframe mapping, and terminal chart rendering.

2026-02-22:

- Added backend client websocket realtime trades endpoint (`GET /v1/ws`) with shared upstream topic manager (single upstream stream per active topic + multi-client fanout).

2026-02-22:

- Set backend live-trades delivery model to websocket fanout-first (shared topic streams) and positioned REST endpoints as snapshot bootstrap/fallback instead of high-frequency polling.

2026-02-22:

- Added `INTEGRATION_README.md` with frontend implementation guidance for websocket-first market-data integration (bootstrap snapshots + realtime fanout + reconnect/resubscribe patterns).

2026-02-22:

- Extended backend websocket fanout to additional channels: realtime `orderbook` (Hyperliquid/Binance/Bybit) and realtime `ohlcv` (Binance/Bybit).

2026-09-09:

- Completed Extended perpetual public market-data integration with REST/realtime parity; standard websocket order books are explicitly indicative rather than RFQ real books.

2026-09-20:

- Completed Extended perpetual funding/mark/index/last statistics through the shared 30-second coordinator and existing REST/WS contract. Current funding is an exact hourly decimal-fraction estimate; native `nextFundingRate` is not exposed as a payment timestamp. Catalog and statistics share opaque native identities and collateral/RFQ/off-hours metadata. Deterministic and live REST/WS checks passed; full delivery evidence is in `FUNDING_MARKET_STATS_PLAN.md`.
- Completed Aster V3 perpetual funding/mark/index statistics with shared 30-second receipt-based acquisition, native identities and settlement, dynamic per-market funding intervals, payment-boundary expiry, and existing REST/WS snapshot/delta delivery. Eight Aster scenarios and the full Rust suite passed (203 tests, 5 ignored); live checks covered 581 active perpetuals, 1h/4h/8h intervals, quote variants, Unicode, ordered deltas, and legacy endpoints. All six approved venue slices in `FUNDING_MARKET_STATS_PLAN.md` are now complete; the separate verification server was stopped without restarting staging.

2026-10-02:

- Submitted Phase 3 for review: stock CCXT Pro owns realtime acquisition; removed native trade/book/candle engines and obsolete Lighter Explorer configuration. Hyperliquid candles are enabled. Bybit live full-book depth is now stock-limited to 1,000 (options 100); Aster live books are partial-depth 20 only. Extended live symbols follow CCXT and absent candle volume is null. Live five-venue smoke, Extended local fixture, lifecycle regressions, and bounded slow-client checks passed; known stock book defects and Extended's upstream API failure remain accepted limitations. Phase 4 is not authorized.

2026-10-03:

- Added [CCXT_KNOWN_ISSUES.md](CCXT_KNOWN_ISSUES.md): actionable upstream-risk and regression records, exact Binance continuity evidence, mitigation boundaries, and closure checks. Documentation only; Phase 3 was awaiting review at that point and Phase 4 was not authorized.
- User accepted Phase 3 and requested it be marked `completed`; updated the tracker, handoff, and current status references. Known issues remain documented accepted risks. Phase 4 was locked until the subsequent explicit authorization.
- Submitted Phase 4 for review: all six venues use shared stock statistics, Lighter maintains Pro `watchTickers`, and HTTP/revisioned WS deliver strict numeric volume/OI with independent freshness. Binance OI is selected-only; Aster/Lighter OI remains explicitly unsupported. Removed native statistics transports and obsolete runtime dependencies. Focused tests: 118 passed; locked backend build and live/loopback HTTP/WS smoke passed. Product units and source differences are documented; Phase 5 remains locked pending review and authorization.
- User accepted Phase 4 and requested it be marked `completed`; updated the tracker, handoff, and current status references. Documentation-only approval update; existing focused verification and accepted upstream limitations are unchanged. Phase 5 is the remaining final integration/removal/shutdown and broad HTTP/WS/release/container verification stage, still locked pending explicit authorization to start. Deployment requires a separate decision.

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
