# Ferris V2 — CCXT-first backend rewrite plan

**Implementation tracker. Phases 1–4 are completed with user approval; Phase 5 remains awaiting explicit acceptance. The user separately authorized deployment on 2026-10-03: the CCXT release is now running as `latticeterminal.service` at `api.latticeterminal.com` for frontend testing. Known stock order-book issues remain accepted temporary risks, not correctness passes.**

Build a replacement core around stock CCXT Rust / CCXT Pro, then make one complete
cutover. Do not spend the rewrite making the existing exchange implementations
more generic. Remove the work CCXT already owns; retain Ferris's useful API,
subscription-sharing and statistics-delivery responsibilities.

The goal is simpler ownership and less exchange maintenance, not a line-count
quota. Reusing a sound Ferris contract or projection is preferable to rewriting
it merely to make this look like a rewrite.

## Start here — one authorized phase at a time

When the user hands you this plan with a request such as “read this plan and let
me know if you have questions,” use the tracker in [section 8](#8-five-phase-implementation-workflow).
On the first implementation handoff, **start Phase 1 only** if there are no
material questions. Do not respond with another plan instead of implementing it.
An explicit review-only or document-editing request does not authorize code work.

Finish the authorized phase's deliverable, run its focused checks, record a
handoff here, set its status to `awaiting_review`, and stop. **Only the user can
approve completion and authorize the next phase.** Do not implement all five
phases in one assignment, mark your own work `completed`, or advance because the
next phase is the next unchecked item. Later agents resume from the tracker,
not from assumptions about what a previous agent probably finished.

The priority is a working CCXT-backed backend with the main data flows functioning.
Keep Ferris routes and response structures where straightforward; exact legacy
frontend compatibility is not a phase or release blocker. Document differences
and let frontend components or compatibility support be adapted case by case.
Do not preserve native integration complexity solely to reproduce old quirks.
Correct market identity, truthful units/timestamps, synchronized books and reliable
streaming remain required, except for known stock order-book issues across exchanges
accepted under [Phase 3](#phase-3--realtime-streaming). This priority supersedes
earlier strict parity wording. No expanded discovery API or other new product scope
is implied.

## 1. Decisions already made with the user

These decisions govern implementation. Stock CCXT defines feature coverage;
the working-backend priority above replaces the original exact-frontend-parity
requirement. Compatibility targets below guide cheap adaptation, not exhaustive
emulation. Known differences must be recorded; inaccurate or unsafe data is not
an acceptable compatibility tradeoff unless explicitly covered by the accepted
Phase 3 stock order-book risk exception below.

| Area | Approved direction |
| --- | --- |
| Cutover | Develop through five separately authorized phases on the V2 work branch, with one final backend cutover. Phase completion needs user approval; one production cutover does not mean one implementation session. Old code is a behavior reference, not a permanent fallback. |
| Acquisition | Stock `ccxt` / `ccxt-pro` only. Prefer unified methods; shipped exchange-specific/implicit methods are allowed where necessary. Small ownership, subscription and output-mapping glue is allowed. |
| Forbidden substitutes | No retained native integration paths, maintained CCXT fork/runtime patch, copied book synchronizer, handwritten exchange HTTP client or raw WebSocket frame parser. Wrapping the old integration in a generic CCXT request call is not the intended replacement. |
| Accepted temporary risk | The user accepts known stock order-book issues across exchanges, including the demonstrated Binance USD-M post-bridge continuity defect, as temporary Phase 3 non-blockers. Document failures and proceed without upstream patches or a native fallback. This is not a passing synchronization result; safe ownership, lifecycle and incremental delivery remain required. See the Phase 3 exception. |
| Coverage | Stock CCXT defines method/product/depth support for the six current venues. Unsupported features may be removed with explicit documentation. Investigate a missing whole provider and ask before dropping a current venue. |
| Venue boundary | Target all six current exchange IDs: `binance`, `bybit`, `hyperliquid`, `lighterxyz`, `aster`, `extended`. Enable their corresponding features on both `ccxt` and `ccxt-pro`; the CCXT feature/provider ID for public `lighterxyz` is `lighter`. Do not expose CCXT's entire exchange list. |
| Additions | Expose newly stock-supported features within the existing public market-data endpoint/channel families, not just the old overlap. Do not silently retain old unsupported flags when CCXT now supplies the feature. |
| Existing payloads | Preserve familiar endpoints, envelopes and straightforward output mappings. Exact legacy frontend parity is not a blocker: record changed raw `info`, optional fields, precision, symbols, parameters or provider timestamp/nonce provenance for case-by-case adaptation. Do not invent missing data or mislabel a field's meaning. |
| New statistics | Include `volume24h` and `openInterest` value schemas. Use **CCXT-shaped nullable JSON numbers**, not the existing funding/price decimal-string convention. Preserve CCXT's verified open-interest units rather than normalize them to base/quote. This exception applies to the two new metrics only. |
| Unsupported statistics | Binance open interest is selected-market-only; no all-market sweep. The 2026-10-04 Lighter decision supersedes its Phase 4 unsupported status: publish two-sided USDC OI as `2 ×` one-sided stock WS `open_interest`, with null amount. Missing features are upstream coverage gaps, not a reason for native workarounds or wider framework changes; these six exchanges are the initial scope. |
| Runtime | One backend process. Use safe owner isolation inside the Rust service. If safe stock-CCXT operation needs subprocesses or a runtime patch, stop and ask; neither is authorized. |
| Tools | Retire the terminal viewer and order-book probe. They must not keep native integration code alive. |
| Hyperliquid history | Use the stock CCXT trade window. Remove the additional Ferris trade collector/history cache and its retention/capacity configuration. Reduced history is an explicitly approved change. |
| Deployment configuration | Preserve existing upstream URL overrides where stock CCXT can honor them. Remove settings that cannot be honored without custom transport, and document those removals. Keep normal backend bind/timeout configuration. |
| Working style | Implement only the currently authorized phase. Use focused checks during coding; reserve broad integration/release verification for Phase 5. Ask about material blockers, not every frontend compatibility difference, and wait for the user's phase review before advancing. |

Private trading/account APIs, a new funding-history API, new liquidation channels,
portfolio work and unrelated worktrees are not part of this cutover. Preserve
existing trade-type information when stock results supply it; document gaps.

### 1.1 Distinguish support, compatibility and correctness

Classify each provider/product/operation on the **actual pinned Rust release**:

| Finding | Required action |
| --- | --- |
| Stock implementation exists and provides correct data | Implement and expose it through the Ferris boundary. Include newly supported features within the approved families. |
| Correct stock data differs from a legacy frontend expectation | Keep the straightforward mapping, document the difference and continue. Do not block a phase on exact raw-info, formatting or other frontend parity. |
| A method/product/depth option is confirmed absent from stock CCXT | Remove that feature; list the loss and its client-visible outcome. No native fallback and no fake successful empty result. |
| An entire current provider appears absent | Check the exact crate/features/dispatch first; all six feature names are present in the inspected 4.5.85 crates. Ask with evidence before removing a target venue. |
| Not yet inspected, wrapper returns `NotSupported`, or not covered by the notes | Finish source/dispatch inspection. **Unqualified is not unsupported.** A generated method or a capability flag alone is also not proof of working support. |
| Implementation delivers incorrect/untrusted state or cannot be owned safely | Block the affected replacement except for user-accepted stock order-book risks across exchanges in Phase 3. Do not present accepted risk as proven correctness. Unsafe ownership is not covered by that exception. Escalate genuine blockers rather than relabeling them as frontend compatibility work or silently dropping a venue. |
| Temporary source failure after a supported path is deployed | Use Ferris's error/freshness behavior. Do not mutate the supported feature set based on transient connectivity. |

If the user approves dropping an unavailable provider, omit it from registration
and `/v1/capabilities`. Keep a partially supported provider and reject unavailable
operations at request/subscription validation. Shared `/v1/fetch*` routes remain
for implemented operations. Do not manufacture dummy provider objects to keep
retired IDs discoverable.

## 2. Evidence and reading order

1. This document: user decisions, replacement boundaries and work sequence.
2. [CCXT_MIGRATION_NOTES.md](CCXT_MIGRATION_NOTES.md): recorded ownership,
   cold-addition and correctness findings. Its benchmark paths and earlier docs
   are historical references. **Do not search for or recreate deleted artifacts.**
3. Current source at the anchors below: the implementation of today's contract.
4. [README.md](README.md) and [INTEGRATION_README.md](INTEGRATION_README.md): client
   context, checked against source where they disagree.

The notes record 4.5.84 and 4.5.85 experiments; they are not a qualification of a
future release. [Cargo.toml](Cargo.toml) and [Cargo.lock](Cargo.lock) now pin stock
`ccxt`, `ccxt-pro`, and resolved `ccxt-base` to `4.5.85`, selected during Phase 1.
Do not assume a newer release fixes recorded defects, and do not treat a pin
or an accepted upstream risk as a blanket production-correctness qualification.

The evidence is principally about **order books**, not complete REST, trades,
candles, statistics or all six providers. Method names below identify candidate
CCXT API families, not verified Rust signatures or per-venue support claims.

A concrete documentation discrepancy: `RealTradesUpstreamRunner` and
`RealOrderBookUpstreamRunner` dispatch `lighterxyz`, and `LighterExchange`
implements REST trades/candles/books. The README's scope list is narrower.
Inventory these as existing paths, then apply the approved CCXT coverage rule;
do not mistake their absence from the README for proof that they do not exist.

## 3. Public compatibility targets

### 3.1 Routes and envelopes

Route registration is in [src/main.rs](src/main.rs). Validation, serialization
and WS command handling are in [src/web.rs](src/web.rs).

| Route | Retained contract |
| --- | --- |
| `GET /healthz` | Health object with `status`. |
| `POST /v1/fetchTrades` | `CcxtTrade[]`; existing request fields `exchange`, `symbol`, `since`, `limit`, `params`. |
| `POST /v1/fetchOHLCV` | Arrays of six-element `(timestamp, open, high, low, close, volume)` tuples; preserve request `timeframe`, time bounds and ordering. |
| `POST /v1/fetchOrderBook` | `CcxtOrderBook`, not an unfiltered CCXT value. |
| `POST /v1/fetchMarkets` | `{exchange, markets, timestamp}` containing Ferris `UnifiedMarket` rows; preserve `includeInactive`. |
| `POST /v1/fetchMarketStats` | `{timestamp, scope, markets, coverage}`; the new metrics fit within the existing field envelope. |
| `GET /v1/capabilities` | Existing `CapabilitiesResponse` shape. Registry/field support values change with qualified CCXT support; this endpoint does not perform upstream acquisition. |
| `GET /v1/ws` | Existing command and update protocol for `trades`, `orderbook`, `ohlcv`, `marketstats`. V2 is an internal rewrite, not a forced `/v2` frontend migration. |

`/v1/capabilities` currently describes market statistics and funding-rate-history
availability, **not a general REST/WS method matrix**. Keep that shape; maintain
the broader support matrix internally and in the client docs. Do not add an
unagreed discovery schema or funding-history endpoint as incidental work.

These existing shapes are the default implementation target, not a requirement to
finish every frontend adaptation before release. Record any necessary difference
in the phase handoff; do not rename routes or redesign DTOs without a concrete
reason. Reusing the existing outer API is still the simplest starting point.

### 3.2 DTOs are Ferris-owned, despite their names

Keep [src/models.rs](src/models.rs) as the serialization boundary:

- `CcxtTrade`: keep the outer shape and map available fields. Prefer the existing
  `info` structure when stock output supplies it; otherwise return truthful stock
  `info` and document the change. Do not reconstruct an old native response solely
  for exact frontend parity or invent data CCXT did not return.
- `CcxtOhlcv`: keep six positions and millisecond timestamps. Phase 2 preserves
  missing stock volume as `null` rather than inventing a numeric value; required
  price cells remain numeric. A CCXT candle cache is not automatically the
  increment/update payload the client currently receives.
- `CcxtOrderBook`: keep two-number `asks`/`bids` levels and the familiar optional
  `datetime`, `timestamp`, `nonce`, `symbol` fields. Document changed CCXT timestamp
  or sequence provenance instead of pretending it is the old native value. Never
  publish mutable backing storage or use a returned nonce alone as proof of sync.
- `UnifiedMarket`, `UnifiedMarketInfo`, `MarketIdentity`: preserve the familiar
  shape and existing identity strings where straightforward. If metadata requires
  a change, update resolution consistently and document it; wrong-market routing
  and identity collisions are correctness bugs, not acceptable frontend fallout.
- Keep existing funding/price string fields. Prefer exact strings from stock
  `info` or shipped implicit methods where available. If only a CCXT numeric value
  is supplied, document its source precision when formatting it as a string;
  do not claim the original native decimal precision was recovered. The approved
  new volume/open-interest values remain numeric as specified in section 7.3.

Compare the same source observation when assessing data, not unrelated live
prices. A documented change of source timestamp/nonce is different from returning
incorrect time units, fabricated precision or an unsynchronized book.

### 3.3 Symbols, identity and parameters

Use one CCXT-backed metadata catalog for REST, live feeds and statistics. Resolve
native IDs, market type, linear/inverse, quote, settlement, contract size,
precision and active status from metadata. Do not infer a derivative's identity
from ticker text or a row's array position.

Use these current conventions as cheap compatibility mappings, not a mandate to
rebuild native integrations. Document any necessary deviation in the handoff:

- Binance/Bybit/Aster catalogs use `BASE/QUOTE`; their trade/book symbol helpers
  preserve slash-form input, while a native pair can be expanded to
  `BASE/QUOTE:QUOTE`. Preserve that cheap formatting where possible rather than
  blindly replacing every symbol. See each adapter's `resolve_public_symbol` and
  `src/ws_shared.rs::resolve_public_symbol`.
- Extended trade/book streams use `BASE/USD:USD`; its catalog uses `BASE/USD`.
  Accepted aliases include `BASE-USD`, `BASE/USD`, `BASE/USD:USD`.
- Lighter remains `lighterxyz` externally. Replace the Explorer/native dual
  catalogs with CCXT metadata, but preserve existing native-ID identity mappings
  where those markets are retained. Recorded BTC/ETH numeric IDs are not constants.
- Statistics IDs come from `src/market_stats.rs::make_market_id`: the serialized
  tuple `(exchange, market_type, category, dex, exchange_market_id)`. Never rebuild
  them from a display symbol. Preserve category, settlement and primary-DEX
  distinctions; different products must not collide.

Keep existing defaults where their feature remains supported: omitted exchange
means `hyperliquid`; Bybit data requests default to `linear`, while its catalog
without a category combines `linear`, `inverse`, `spot`; Binance's existing
product default is USD-M, not whichever default a CCXT constructor happens to use.
Preserve `coin` aliases, timeframe aliases, `since` filtering, end-time bounds,
parameter precedence and display-depth behavior where stock CCXT supports the
underlying request. Translate Ferris parameters at the boundary; do not blindly
forward every Ferris key to CCXT.

For newly supported products, use the existing `params` container and verified
CCXT product selectors, preserving old defaults where straightforward. Extend
catalog and statistics validation together. Record necessary identity/parameter
changes for frontend adaptation; ask when the intended market is ambiguous.
Supported source data is not permission to pick an arbitrary market.

### 3.4 Errors and WS behavior

Preserve [src/errors.rs](src/errors.rs) and the distinct
`src/web.rs::FetchMarketsApiError` behavior:

- Ordinary API errors are `{code, message}`. Existing categories include
  validation/bad symbol, unsupported exchange/feature, upstream request/data,
  and internal failures; `UnsupportedFeature` is HTTP 501.
- `fetchMarkets` instead uses `{error: {code, message}}`; an unregistered exchange
  is HTTP 422 with `INVALID_EXCHANGE`, unlike ordinary unsupported-exchange errors.
- Add internal operation support checks/error translation where needed; do not
  let a CCXT exception string choose a new public status or envelope.
- WS command aliases, `subscribed`, `alreadySubscribed`, `unsubscribed`, `pong`,
  `error`, `warning` and update envelopes remain. See `parse_stream_command`,
  `handle_stream_command`, `WsAckMessage`, `WsLagWarning` and the forwarders.
- Preserve per-connection duplicate/unsubscribe semantics while changing the
  upstream key implementation. Acquisition sharing and a client's requested
  topic/display depth are separate concerns.
- Preserve bounded fanout and isolated slow-client handling. Current defaults
  are 512 shared-topic messages and 256 outgoing client messages. Lag is reported
  through `CLIENT_LAGGED`/`droppedMessages`; a full outgoing queue closes the slow
  client rather than blocking upstream acquisition.
- No new silent trade dropping or order-book snapshot coalescing policy is
  approved. Existing statistics coalescing operates on complete states before
  producing revisioned deltas; it is not permission to discard sparse deltas.

## 4. Replacement architecture

Keep the small Ferris-facing contracts; replace the implementation behind them.
Avoid a parallel hierarchy of generic providers, native providers, fallback
providers and compatibility wrappers.

```text
Axum HTTP/WS + Ferris DTOs
          |
          +-- CCXT-backed provider registry + metadata catalog
          |             |
          |             +-- stock REST / implicit methods
          |             +-- stock Pro owners / dispatch
          |                            |
          +-- shared live topics <-----+ owned immutable outputs
          |
          +-- statistics acquisition -> freshness/projection -> HTTP/WS
```

### 4.1 Small target module boundary

The following are **proposed destinations**, not existing files to recover:

| Destination | Responsibility |
| --- | --- |
| `src/exchanges/ccxt/mod.rs` | One Ferris `MarketDataExchange` facade per registered provider, backed by the shared CCXT service/handles. Four REST operation adapters, not six rewritten native clients. |
| `src/exchanges/ccxt/venue.rs` | Explicit dispatch to the pinned Rust exchange types, verified support/product settings, and small venue-specific policies. Do not recreate CCXT's exchange metadata tables. |
| `src/exchanges/ccxt/catalog.rs` | Shared metadata loading/refresh and resolution; Ferris public aliases/IDs/projected catalog. |
| `src/exchanges/ccxt/convert.rs` | Owned DTO conversion, straightforward symbol/error mapping, truthful stock raw info and precision handling. No exhaustive legacy-response emulation. |
| `src/exchanges/ccxt/owner.rs` | Safe mutable ownership, commands, stock watch dispatch, cancellation and lifecycle. No native frame parsing or book state machine. |
| `src/exchanges/ccxt/statistics.rs` | CCXT-backed bulk sources and small field/provenance mappings feeding Ferris statistics state. Split only when the real mappings warrant it. |
| `src/realtime.rs` | Replace native upstream runners with one shared live-topic lifecycle implementation; retain typed trade/book/candle outputs and channel-specific delivery rules. |

Reuse `src/exchanges/traits.rs` and `registry.rs` where useful. Adjust their internal
support contract rather than bolt a second provider API alongside them. A facade
may be `Send + Sync` because it holds safe command handles; that does **not** mean
a mutable CCXT Core or Value should be shared that way. No unsafe `Send`/`Sync`
implementations or shared-to-mutable casts in Ferris.

Keep `models.rs`, the useful web protocol code, and statistics projection/delivery.
A near-rewrite of exchange acquisition does not require discarding those parts.

### 4.2 Catalog and REST

- Load metadata once per meaningful provider/product configuration; share owned
  metadata/projections, not CCXT live books, connection registries or allocator
  state. Serialize refresh work rather than fetching a catalog per subscriber.
- Replace both the Lighter Explorer catalog and separate native catalog with the
  supported stock metadata source. Audit aliases/identities during this cutover,
  not after deleting the resolver.
- Reuse CCXT's metadata cache; keep only Ferris caches with a distinct product
  purpose. `src/web.rs::MarketsCache` caches the public response for 30 seconds,
  including its timestamp and `includeInactive`/canonical-params key. Preserve
  that behavior; it is not the same thing as CCXT `loadMarkets` state.
- Candidate REST families are `fetchTrades`, `fetchOHLCV`, `fetchOrderBook`,
  `fetchMarkets`/`loadMarkets`. Verify actual Rust methods and the selected venue's
  implementation, including stock emulation, rather than assuming trait presence.
- Keep cheap Ferris filtering/sorting/formatting required by the old contract.
  Delete request builders, transport response envelopes and native endpoint
  pagination now performed by CCXT. Do not port a whole old adapter under a new name.
- Do not schedule catalog loads, HTTP statistics sweeps or cold REST snapshots on
  a live owner in a way that pauses already-maintained feeds. REST and streaming
  acquisition share metadata and real exchange quotas, not an arbitrary global
  mutex held across network waits.

### 4.3 Live ownership: use the findings, not the failed design

The notes establish all of these constraints for the inspected runtime:

1. The WS registry is process-global and keyed by URL. Two exchange objects or
   Tokio tasks at the same URL are **not** two independent connections.
2. A shared transport needs one coherent mutable owner that dispatches all its
   active result hashes. Competing owners can strand or overwrite settlements.
3. The deferred `SPAWN_QUEUE` is thread-local. The successful 4.5.85 experiments
   used a dedicated OS thread/current-thread Tokio runtime for each mutable owner.
   Ordinary tasks on the shared application runtime are not that topology.
4. Convert a result to owned Ferris data **inside** its owner before fanout. A
   shallow/lazy CCXT Value backed by a changing book is not a client snapshot.
5. Thread isolation does not fix the global registry or establish runtime
   soundness. The notes also identify `SIDE_STORE` and unsafe coercion concerns.

Start from the demonstrated arrangements where applicable, then establish a safe
single-process production owner model for the chosen pin:

| Path recorded in the notes | Design consequence |
| --- | --- |
| Binance USD-M diff books | Fixed-symbol independent owners with genuinely distinct stock URLs kept the warm book moving. Growing a live aggregate subscription reseeded/stalled it. A different instance alone is not isolation. |
| Bybit linear depth-50 aggregate | One owner/native aggregate watcher worked for cold addition. Do not force one socket per symbol or infer support for every product/depth. |
| Lighter books | A thin single-owner/multi-hash dispatcher worked even without a unified aggregate method. CCXT still parsed and maintained books. |
| Hyperliquid books | Later single-owner/multi-hash ordinary watches worked locally, including a finite burst followed by silence. Native unsubscribe still stalled/stranded other outputs and exposed `[UnsubscribeError]`. |

Use only real stock URL allocation. The two Binance URL slots in the experiment
are **not** a production allocator; no invented query-string aliases or destructive
queue draining. Account for thread/stack/connection costs before adopting a large
owner count. If a safe bounded arrangement cannot be implemented with stock CCXT
inside one process, ask rather than create an IPC service or hide a patch.

The same URL may serve multiple channels. Integrate trades/candles into the
ownership design explicitly; the book experiments did not qualify mixed-channel
watchers. Do not independently instantiate three channel managers and assume their
CCXT transports are isolated.

### 4.4 Live lifecycle and owned publication

Implement the topic lifecycle once, with typed channel policies:

- Normalize an upstream feed key from venue, native market/product, channel and
  meaningful acquisition parameters. Keep display slicing and a client's public
  topic descriptor outside CCXT backing-state identity.
- First demand starts an owner/subscription; subsequent viewers attach to the
  same maintained feed. Adding a viewer must not reload markets or seed the book.
- Track lifecycle/readiness separately from “a watch returned.” A book becomes
  live only after the required synchronization is established. On reconnect or
  reseed, invalidate trusted cached publication until the new state is ready.
- Use an internal ownership epoch/generation to reject late results from a
  cancelled owner. It need not become a new public order-book field.
- An optional immediate publication for a joining viewer must be the current,
  trusted owned snapshot. Arrange attachment/publication so an older cached book
  cannot arrive after a newer update. A saved JSON blob is not a warm feed.
- On final demand removal, stop the subscription and release ownership safely.
  Delayed/missing control acknowledgements must not block unrelated warm feeds.
  Keeping every requested symbol forever or restarting the whole service is not
  an acceptable cleanup mechanism.
- Ensure a final settled result is delivered even when no unrelated source frame
  arrives afterward. Do not use sequential blocking watches, a lock across
  `watch().await`, or short-timeout polling to disguise a dispatch problem.
- For trades, use stock incremental/new-update behavior or an owned cursor over
  stock results so watch-cache windows are not replayed as new trades. This is
  delivery bookkeeping, not reinstating the removed historical trade service.
- Keep CCXT's required book state; slice an owned view for client display. Do not
  call a backing-state `limit()` with a viewer's top-N request. Avoid cloning a
  full deep book per client merely to truncate it, as the current
  `spawn_orderbook_topic_forwarder` does; serialize an equivalent borrowed slice
  of owned data instead.

Warm feeds must continue during cold initialization and recovery. The notes allow
roughly 1–2 seconds of cold-widget loading as a tolerance, not a demonstrated
universal readiness deadline. Their 2–5 ms added-latency budget is likewise not an
already-proven end-to-end result. Keep live rate limiting enabled and account for
shared IP quotas, metadata, snapshots, retries and replicas. Do not promote the
two-book results into a capacity claim for the recorded 400-users-per-replica /
up-to-50-symbols-per-user assumptions.

## 5. Concrete removal and replacement ledger

Remove these implementations once their target support is implemented or their
absence is confirmed under section 1.1. Do not delete the contract reference
before recording the retained behavior and approved loss. Before removing exported
symbols, find every caller, including tests and binaries; migrate or retire those
callers in the same cutover. Leave no legacy aliases/re-exports/shims behind.

| Current source / symbols | Replace or retain | What should disappear |
| --- | --- | --- |
| [src/exchanges/binance/mod.rs](src/exchanges/binance/mod.rs), `BinanceExchange` | CCXT-backed provider with existing USD-M default and Ferris conversion. | `binance-sdk` request builders, native DTO mapping that CCXT replaces, exchange-info acquisition cache, native snapshot-provider implementation. |
| [src/exchanges/bybit/mod.rs](src/exchanges/bybit/mod.rs), `BybitExchange` | CCXT-backed category-aware dispatch; preserve old defaults on retained products. | `BybitEnvelope`, reqwest endpoint methods/pagination, copied native interval/depth machinery. |
| [src/exchanges/aster/mod.rs](src/exchanges/aster/mod.rs), `AsterExchange` | Stock Aster implementation if supplied by the selected Rust pin; record actual method/product coverage. Aster remains its own venue. | Custom V3 REST adapter, Binance-compatible raw WS integration and native seed provider. Do not alias Aster to Binance because their APIs resemble one another. |
| [src/exchanges/extended/mod.rs](src/exchanges/extended/mod.rs), `ExtendedExchange` | Target stock Extended; its feature exists in both inspected crates. Qualify methods/products and ask before any whole-venue removal. | Custom REST envelopes/endpoints, native candle/book/trade acquisition. Do not substitute an RFQ real book for today's indicative standard book. |
| [src/exchanges/hyperliquid/mod.rs](src/exchanges/hyperliquid/mod.rs), `HyperliquidExchange`, `TradeCache`, `start_trade_collector`, `run_trade_collector_loop` | CCXT data/catalog mapping. **Delete the extra historical-trade service**, as approved. | `/info` client, native collector/reconnect loop, per-coin history buffers, collector command plumbing, separate catalog-building transport. |
| [src/exchanges/lighterxyz/mod.rs](src/exchanges/lighterxyz/mod.rs), `LighterExchange`, `LighterMarketCatalogService` | Stock `lighter` metadata/acquisition, public ID `lighterxyz`. | Explorer HTTP catalog/refresh loop, native REST request construction, separate native/Explorer resolution infrastructure. |
| All six `src/exchanges/*/statistics.rs` | Keep necessary pure funding/identity/provenance mapping; replace acquisition with stock calls feeding `MarketStatsSourceSnapshot`. | SDK/reqwest calls, duplicated transport caches, native Lighter socket parsing. Do not delete precise value semantics with the transport. |
| [src/realtime.rs](src/realtime.rs), `RealTradesUpstreamRunner`, `RealOrderBookUpstreamRunner`, `RealOhlcvUpstreamRunner`, `Upstream*Stream` | Shared topic lifecycle plus CCXT owner handles. | Native connection loops, URL/topic builders, `send_*` subscription transport, `parse_*` frame dispatch, `apply_*` book maintenance, three copies of substantially the same subscription lifecycle. |
| [src/binance_orderbook.rs](src/binance_orderbook.rs), `OrderBookSnapshotProvider`, `BinancePrice`, diff synchronizer | Delete when native Binance/Aster/Extended consumers are gone. Book correctness becomes a CCXT qualification gate, not a copied Ferris synchronizer. | REST-seed/delta bridge, price tree and native stream selection. `ExtendedOrderBookState` also uses `BinancePrice`; deleting only Binance callers is insufficient. |
| [src/bybit_full_orderbook.rs](src/bybit_full_orderbook.rs), `fetch_bybit_full_snapshot`, full-book state | Replace with stock full-depth support if it exists; otherwise record loss of unsupported depth. | Custom full-book HTTP fetch, decimal price tree and synchronization code. |
| [src/ws_shared.rs](src/ws_shared.rs), `ExtendedOrderBookState`, `parse_*` | Move only still-needed pure public symbol/parameter formatting into the new boundary. | Native frame parsers, local book state and old shared transport helpers. Do not carry this whole file forward as a renamed compatibility module. |
| [src/market_stats.rs](src/market_stats.rs), `MarketStatsCoordinator`, `MarketStatsSourceSnapshot` | Reuse shared demand, source snapshots and lifecycle; adapt only for actual new metric acquisition needs. | Provider-specific transport knowledge, if introduced during migration; duplicated provider worker loops. |
| [src/market_stats/projection.rs](src/market_stats/projection.rs) | Preserve projection/merge/freshness behavior; update hardcoded support/product policy for the new support map. | Stale field-support whitelists and implicit “other venue means Hyperliquid” assumptions when incompatible with the new map. |
| [src/web/market_stats_stream.rs](src/web/market_stats_stream.rs) | Reuse generation/revision, snapshot/delta diff and bounded delivery. | Nothing merely because the source now uses CCXT. |
| [src/web.rs](src/web.rs), `AppState`, handlers, forwarders | Rewire internals; keep protocol/validation/cache behavior. | Coupling to the three native runners and full-book-per-client copying. |
| [src/main.rs](src/main.rs), [src/lib.rs](src/lib.rs), [src/exchanges/registry.rs](src/exchanges/registry.rs), [src/exchanges/traits.rs](src/exchanges/traits.rs) | Register the supported CCXT facades and wire one ownership service. Preserve small useful contracts. | Six native constructors, injected native snapshot providers/catalog services, obsolete module exports and trait implementations. |
| [src/bin/market_stream](src/bin/market_stream), [src/bin/orderbook_probe.rs](src/bin/orderbook_probe.rs) | Retire both tools and their tool-specific tests/documentation. | Direct-exchange CLI runtime, parsers, view code and dependencies retained solely for these tools. |

### 5.1 Manifest, configuration and container

- Add exact CCXT/Pro pins and apply section 5.2 before compiling: disable defaults
  on every relevant CCXT dependency and enable the six target venues explicitly.
  Confirm feature names against the chosen manifests and record the locked engine.
- Remove `binance-sdk` and tool-only `crossterm`. Remove direct `reqwest` and
  `tokio-tungstenite` dependencies when no remaining Ferris code needs them;
  CCXT may still use transport crates transitively. Keep `rust_decimal` for the
  existing funding contract. Do not remove Serde/Tokio/Axum infrastructure simply
  because upstream acquisition moved.
- In [src/config.rs](src/config.rs), remove `TRADE_CACHE_CAPACITY_PER_COIN`,
  `TRADE_CACHE_RETENTION_MS`, `TRADE_COLLECTOR_ENABLED` with their deleted service.
- Audit `HYPERLIQUID_BASE_URL`, `BINANCE_BASE_URL`, `BYBIT_BASE_URL`,
  `ASTER_BASE_URL`, `EXTENDED_REST_BASE_URL`, `EXTENDED_WS_URL`,
  `LIGHTER_REST_BASE_URL`, `LIGHTER_WS_URL` against stock URL configuration.
  Preserve usable overrides; no handwritten transport to preserve an unusable one.
- `LIGHTER_MARKETS_URL` currently targets the Explorer catalog; its old consumer
  is removed. Do not secretly keep that service. Apply the same stock-config
  mapping rule to this setting and `LIGHTER_MARKET_CATALOG_REFRESH_MS`, and list
  any removed/changed setting in the cutover notes.
- [Dockerfile](Dockerfile) currently copies `Cargo.toml` but not `Cargo.lock`.
  Copy the lockfile and use a locked backend-binary build for the target. Check
  the pinned crates' required Rust toolchain before retaining the current builder
  image. This plan does not claim the existing image can build CCXT.
- Wire shutdown for live owners/catalog work as well as statistics. The current
  `main` explicitly shuts down market statistics; new owner threads/tasks must
  not be orphaned when Axum stops.

### 5.2 CCXT feature selection and build memory

**Do this before the first compilation, not after an out-of-memory failure.**
CCXT's default feature set includes every exchange. The
[official Rust build FAQ](https://github.com/ccxt/ccxt/wiki/FAQ#rust-build-is-too-slow-and-heavy-how-to-improve-it)
reports these measurements on the same upstream machine:

| Feature set | Fresh debug build: time / peak RAM | Release build: time / peak RAM |
| --- | --- | --- |
| Default, all exchanges | 3m23s / 18.6 GB | 7m49s / about 50 GB |
| Three selected exchanges | 29s / 2.5 GB | 3m05s / 4.9 GB |

These are upstream examples, **not Ferris measurements or a six-exchange memory
guarantee**. The default release build exceeds a 16 or 32 GB machine's RAM.
Do not solve that by dropping a current Ferris venue or compiling all exchanges
just to discover which ones work.

All six required feature names are present in the inspected
[`ccxt` 4.5.85 features](https://docs.rs/crate/ccxt/4.5.85/features) and
[`ccxt-pro` 4.5.85 features](https://docs.rs/crate/ccxt-pro/4.5.85/features):

| Ferris public exchange ID | Feature on both crates / base provider ID |
| --- | --- |
| `binance` | `binance` |
| `bybit` | `bybit` |
| `hyperliquid` | `hyperliquid` |
| `lighterxyz` | `lighter` |
| `aster` | `aster` |
| `extended` | `extended` |

Feature availability proves that the provider can be selected for compilation,
not that every REST method, watch channel, product or depth works. Phase 1 must
record actual dispatch/support separately. Public `lighterxyz` must map to the
stock `lighter` provider, not be passed unchanged to a CCXT runtime factory.

Exact-pin example for the **inspected** manifests below; Phase 1 still selects and
qualifies the actual release. Keep the explicit feature list on **both** crates:

```toml
[dependencies]
ccxt = { version = "=4.5.85", default-features = false, features = ["binance", "bybit", "hyperliquid", "lighter", "aster", "extended"] }
ccxt-pro = { version = "=4.5.85", default-features = false, features = ["binance", "bybit", "hyperliquid", "lighter", "aster", "extended"] }
```

- Features are configured per crate. Disabling defaults only on `ccxt` leaves
  `ccxt-pro` free to re-enable `all` through the shared engine. Cargo combines
  enabled dependency features; a top-level `--no-default-features` is not a
  substitute for these dependency declarations. Apply the same rule to any
  required direct `ccxt-base` dependency and workspace/dev/build declarations;
  do not add an engine dependency merely for this example.
- The list selects base Binance, not its market product. Preserve Ferris's USD-M
  default explicitly. If the chosen dispatch instead constructs a derived ID
  such as `binanceusdm`, enable that feature on each crate using it. Derived
  exchange features enable their parent automatically, not the reverse.
- `ccxt::from_id(...)` returns `None` for a provider not compiled in. Check the
  six public-to-provider mappings; do not interpret a missing feature or wrong
  factory ID as proof that stock CCXT lacks the venue.
- Do not add `ccxt-prediction` for this rewrite. If separately authorized later,
  it needs its own feature list and `default-features = false`; overlapping IDs
  refer to separate prediction venues, not the spot/derivatives providers here.
- After resolving the gated dependencies and updating `Cargo.lock`, inspect
  `cargo tree --locked -e features -i ccxt-base` before compiling. This inspects
  features without compiling Rust. Confirm the six required features, any needed
  derived-provider features, and no accidental upstream `all`/all-exchange
  defaults. Record the resolved engine version; inspect each version explicitly
  if Cargo reports more than one. Do not run this as a build campaign for the
  current documentation-only task.

**Optional memory tuning if the gated build is still too heavy:** merge these
settings into Ferris's existing profile tables rather than duplicating tables:

```toml
[profile.dev]
debug = 0 # Or "line-tables-only" when line-level debugging information is needed.

[profile.release]
lto = "off"
```

Less debug information reduces debugging detail; disabling LTO gives up that
optimization pass. The FAQ also suggests `RUSTFLAGS="-C codegen-units=4"`, trading
build time for a lower memory peak. These are optional upstream tuning measures,
not reasons to enable every venue. Keep selected flags stable to reuse Cargo
artifacts, batch focused builds within the authorized phase, and never launch
concurrent Cargo builds. Broad release/container verification belongs to Phase 5.

## 6. Venue-specific traps to resolve before deleting native code

| Venue | Current behavior / recorded finding | Replacement instruction |
| --- | --- | --- |
| Binance | Shallow requests use partial-depth streams; deep requests use a 1,000-level REST seed and diff stream. Notes found seed publication before a valid delta bridge, backing-depth loss under `limit()`, and `E` versus native `T` timestamp differences. Phase 3 also demonstrated acceptance of a post-bridge delta with an invalid `pu` link. | Qualify the retained profile subject to the specific user-accepted Phase 3 continuity exception. Do not block on or claim to have fixed that known defect. Other readiness/retention checks remain required; a later result alone is not proof of synchronization. Do not replace deep books with partial books to pass a check. |
| Bybit | Limited books and a separate native full-depth path above 1,000, up to 10,000; options are limited separately. The recorded success was linear depth 50. | Use stock-supported tiers; explicitly record any lost full-depth/product support. Do not claim the 50-level result covers full books, inverse, options, candles or lifecycle. REST and WS nonce behavior are not necessarily identical. |
| Hyperliquid | Existing catalog includes perps/spot and aliases; REST books are shallow; extra trade history is Ferris-owned. Notes found roughly 5.4-second public input cadence and a separate unsubscribe failure. | Remove extra history. Add stock-supported channels through the existing families where qualified. Distinguish source silence from backend stalls; do not make public-endpoint tuning the rewrite's main task. Safe unsubscribe remains necessary. |
| Lighter | Existing native WS books expose `order_book.nonce`; the recorded CCXT book nonce is `order_book.offset`. REST books expose no nonce. Old resolution uses two catalogs. | Document the stock nonce as offset-based if used; frontend sequence assumptions can be adapted. Do not call it the old native nonce or infer synchronized readiness from it. Correct book maintenance/lifecycle remains required. Unify catalog identity. |
| Aster | Native V3 data and synchronized diff books, distinct quote/settlement assets and native identifiers, including punctuation/Unicode. | Inspect actual stock Aster methods and metadata. Notes did not qualify this venue. Do not normalize away distinct assets or reuse Binance's provider identity. |
| Extended | Standard book is indicative; native WS has its own sequence/additive-versus-absolute update handling. Candle types include trades, mark and index. Catalog includes RFQ/off-hours and settlement metadata. | Let CCXT maintain the book; preserve the retained product and metadata. Absent methods may be retired. An RFQ-only stock path is not a same-product substitute for the standard book. |

## 7. Statistics: keep Ferris's product, replace its acquisition

### 7.1 Existing boundary and profiles

`MarketStatsSource` in [src/exchanges/traits.rs](src/exchanges/traits.rs) feeds
`MarketStatsSourceSnapshot` in `market_stats.rs`. The coordinator shares work by
exchange/scope, and projection selects fields/market IDs. Keep that division for
bulk sources rather than fetching once for every subscriber or selected row.

Current source profiles are contract references, **not claims of CCXT support**:

| Venue | Existing data to recover through stock CCXT where retained |
| --- | --- |
| Binance | Catalog + premium-index + funding-interval information; funding, mark, index; `currentUnclassified`, decimal-fraction rates, native payment/interval metadata. |
| Aster | V3 catalog + premium-index + funding-interval information; funding, mark, index; `estimate`, decimal-fraction rates and per-market intervals. |
| Bybit | Category-specific paged instruments + tickers; funding, mark, index, last; `estimate`; validate instrument/ticker funding-interval agreement. |
| Extended | Market catalog/statistics response; funding, mark, index, last; hourly decimal-fraction `estimate`; preserve collateral identity and RFQ/off-hours metadata. |
| Hyperliquid | Primary metadata/asset contexts plus spot metadata; funding, mark, index; hourly decimal-fraction `currentUnclassified`; preserve primary-DEX scope and settlement identity. |
| Lighter | Native metadata plus `market_stats/all`; current and last-settled funding, mark, index, last; hourly **percent** rates with distinct `estimate`/`settled` meanings. |

Current Lighter statistics acquisition is **fetch-driven**: it opens a socket,
subscribes, gathers a baseline into a persistent in-memory cache and returns.
`nativeWebSocket` in capabilities does not mean a permanently maintained source.
Replace this native acquisition rather than keeping it as an exception.

Candidate stock families include `fetchFundingRate(s)`, `fetchTicker(s)`,
`fetchOpenInterest(s)`, supported watch equivalents and shipped implicit methods.
One unified ticker is not automatically a statistics row. Inspect the chosen
provider's source for exact values, intervals, native metadata and timestamps.

### 7.2 Semantics that survive the acquisition replacement

- Preserve `available`, `notApplicable`, `unsupported`, `unavailable`, `stale` as
  distinct states. Absence of a CCXT implementation is not a transient source
  outage, zero is not missing, and inactive markets are not silently deleted.
- Keep per-field source and receipt timestamps, monotonic age, source failures
  and catalog/enumeration completeness. Cache hits must not manufacture fresh
  observations. A field's timestamp is not automatically the ticker timestamp.
- Preserve the current 30-second polling / 90-second stale policy where exposed,
  and the estimate payment-boundary expiry for retained Aster/Bybit funding.
  Extend new metrics through the same freshness/retention machinery.
- Preserve exact funding units, rate/payment intervals, next/settled payment
  distinctions, `FundingKind` and decimal equivalents. CCXT naming alone does
  not tell whether a value is an estimate or last settlement.
- Keep source provenance truthful. If stock CCXT uses a different observation or
  endpoint, report and document the real source instead of claiming the old one
  for parity. A frontend provenance adjustment is not a cutover blocker.
- Keep selected-ID validation against authoritative product metadata, sorted
  projections, coverage and failure retention. A failed/incomplete catalog must
  not look like authoritative market removal.
- Capabilities remain no-I/O discovery. Update supported fields, products,
  limitations and actual upstream mode from the qualified profile. Remove old
  hardcoded `unsupported` states only when their replacement is implemented.

`projection.rs` is not wholly venue-agnostic today. Audit `normalize_topic`,
`market_id_type`, `catalog_source`, `field_implemented_for_exchange`, `implemented`,
`fail_row` and `expire_snapshot` when adding/removing fields or products. A new
metric must not stay `available` forever because the old whitelist ignores it.
Replace duplicated support policy with the small qualified profile; keep existing
identity encodings and defaults for retained products.

For stock methods that are only per-symbol, share collection across all demand
for the same market; do not spawn a poller per viewer. Prefer stock bulk methods.
Only add demand-union bookkeeping for selected IDs/new metric fields if that
source actually needs it; a default funding request should not trigger an
unbounded open-interest sweep. Keep complete shared source state for projection.
If all-market collection cannot fit the existing freshness and quota contract,
ask about selected-only versus paced all-market support rather than claim a stock
method is absent or silently deliver permanently stale “supported” data.

### 7.3 Approved new value schemas

The field names `Volume24h` and `OpenInterest` already exist in
`MarketStatsFieldName`; their **value variants do not**. Add them to
`MarketStatsValue` without changing the existing funding/price variants or outer
HTTP/WS envelopes.

These are schema illustrations, not captured exchange responses:

**`fields.volume24h.value`**

```json
{
  "baseVolume": 12.5,
  "quoteVolume": 812500.0
}
```

**`fields.openInterest.value`**

```json
{
  "openInterestAmount": 123.5,
  "openInterestValue": null
}
```

Rules for both additions:

1. Both named members are present and nullable JSON numbers. Use finite numeric
   values from stock CCXT; the user accepts their numeric precision. Keep existing
   `FundingValue.rate` and `PriceValue.amount` as strings.
2. Missing one member stays `null`, not zero. With no usable observation, use the
   existing outer `MarketStatsField` unavailable/stale behavior and `value: null`
   as appropriate; an object of invented zeroes is not data. Preserve a valid zero.
3. Exchange/receipt timestamps, source and state belong to the surrounding
   `MarketStatsField`; market identity/assets belong to the row. Do not add a
   second nested ticker/open-interest envelope with its own symbol/info fields.
4. `volume24h` is CCXT's rolling-24-hour volume, not the best-bid/ask amount or a
   sum of Ferris's recent-trade window. `baseVolume` and `quoteVolume` refer to the
   corresponding market currencies where the selected implementation verifies
   that interpretation. Do not invent the missing side with a last-price multiply.
5. Preserve the pinned provider/product's `openInterestAmount` and
   `openInterestValue` meanings. **Do not assume amount means base coins or value
   means USD.** Record whether amount is contracts/base and the value currency in
   the per-product support documentation. No Ferris currency/contract conversion
   was selected. Unknown units remain unqualified, not silently guessed.
6. Open interest is contract data, not a spot metric. Use the existing
   `notApplicable` mechanism for non-applicable products. A lack of a stock
   implementation is instead `unsupported`; a failed supported call is
   `unavailable`/`stale`.
7. Validate finite numbers before publication. Do not let NaN/infinity silently
   serialize as null and appear to be an ordinary missing measurement.
8. Adapt equality/decoding correctly: numeric `f64` values do not implement `Eq`.
   Remove affected transitive `Eq` derives while retaining `PartialEq` for delta
   comparison; leave request/key equality alone. Do not implement unsound `Eq`.
9. `MarketStatsValue` is currently untagged. Two variants containing only optional
   numeric members can deserialize into the wrong variant if unknown fields are
   ignored. Decode by the enclosing field name, or enforce strict member sets and
   required-but-nullable keys. Empty or cross-metric objects must not silently
   deserialize as the other metric. No new discriminator on existing wire values.

The [CCXT ticker documentation](https://github.com/ccxt/ccxt/wiki/Manual#ticker-structure)
defines `baseVolume`/`quoteVolume`; the
[open-interest documentation](https://github.com/ccxt/ccxt/wiki/Manual#open-interest-structure)
uses `openInterestAmount`/`openInterestValue`. These generic docs explain the new
names, not exact-pin Rust support or every derivative venue's unit interpretation.

### 7.4 WS statistics must remain revisioned

Reuse `spawn_market_stats_forwarder` and `projection_changes` in
`src/web/market_stats_stream.rs`:

- Subscription starts with a snapshot, a generation and revision 1.
- Deltas carry `previousRevision`, `revision`, the same generation, scope,
  coverage, **top-level** `updates` and `removedMarketIds`.
- Preserve metadata/field differences, coverage-only changes and removals.
- Complete-state coalescing precedes delta calculation; the existing minimum
  emission interval is one second. The last update must arrive without another
  source event to wake it.
- The new value variants participate in projection, equality, expiry, retention
  and delta delivery exactly as real fields, not just in the capabilities list.

## 8. Five-phase implementation workflow

Each phase is a separate, user-reviewed assignment. The tracker below records
implementation status and explicit user approvals. One final production cutover
remains the target; later phases are not authorized by earlier implementation.

### Authorization and status rules

1. Read the tracker and the latest user message before editing code. A generic
   implementation handoff authorizes Phase 1 on a fresh tracker, or resumes the
   already-authorized `ready`/`in_progress` phase. It does not unlock later phases.
2. Set the authorized phase to `in_progress` when work begins. Stay within it;
   modify shared files only as needed for that phase's deliverable. If using two
   agents, split the same phase and keep one integration owner; do not run ahead
   into another phase or launch concurrent Cargo builds.
3. Complete real code and a focused smoke of that phase's changed path. Do not
   stop at interfaces, fake results or unimplemented method stubs. Do not turn an
   early phase into whole-backend verification or a benchmark recreation project.
4. When its exit criteria are met, update the row to `awaiting_review`, record the
   handoff below, and **stop for user review**. Agent confidence or passing checks
   do not authorize the status `completed`.
5. Only explicit user acceptance permits `completed`; record the user's approval
   in the tracker. Only explicit authorization to proceed unlocks the next phase.
   “Phase 1 looks good; move to Phase 2” grants both. Approval alone records
   completion but leaves the next phase locked. A generic “continue” while awaiting
   review is not permission to assume acceptance and cross the boundary.
6. If the user requests fixes, return that phase to `in_progress`; do not advance.
   If genuinely blocked, record `blocked`, the evidence and exact decision needed,
   then ask. A blocker in one phase does not authorize starting the next.
7. A new agent resumes this state, including unfinished work, rather than resetting
   the tracker. If the tracker and latest user instructions conflict, reconcile
   them before advancing. Each phase may span more than one session; never compress
   all five into a single assignment simply because the whole plan is present.

### Phase tracker

| Phase | Deliverable | Status | User approval / authorization record |
| --- | --- | --- | --- |
| 1 | CCXT core and catalog | `completed` | User accepted Phase 1 and authorized Phase 2 on 2026-10-02; later phases remain locked. |
| 2 | REST snapshots | `completed` | User accepted Phase 2 on 2026-10-02 and confirmed Extended's known upstream API issue is a non-blocker. |
| 3 | Realtime streaming | `completed` | User accepted Phase 3 and requested it be marked completed on 2026-10-03. The accepted temporary order-book-risk exception still applies across exchanges. |
| 4 | Market statistics and new metrics | `completed` | User authorized Phase 4 and accepted it on 2026-10-03, requesting it be marked completed. Focused verification is recorded below; accepted upstream limitations are unchanged. |
| 5 | Final integration, removal and verification | `awaiting_review` | User authorized Phase 5, then separately authorized production deployment on 2026-10-03 for frontend testing. Deployment is recorded below; no separate explicit Phase 5 completion acceptance was given. |

### Phase handoff record

On each review submission, append a concise entry here with:

- Phase number, status, changed files/symbols and implemented behavior.
- Exact checks actually run and observed results; explicitly name unverified paths.
- Support changes and frontend compatibility differences, with affected requests
  or fields. Ordinary frontend adjustments are not correctness blockers.
- Remaining in-phase defects/blockers and concrete prerequisites for the next
  phase. Do not hide incomplete required behavior under a “follow-up” label.
- A request for user review. Do not claim approval or start the next phase yourself.

### Phase 1 handoff — 2026-10-02

- **Status:** `completed`; the user accepted Phase 1 and authorized Phase 2 on 2026-10-02. Implemented `src/exchanges/ccxt/{venue,catalog,convert,owner}.rs`, exports in `mod.rs`, exact `ccxt`/`ccxt-pro` `4.5.85` pins with `default-features = false`, and all six explicit provider features. Public `lighterxyz` maps to stock `lighter`.
- **Behavior:** six typed stock providers are constructed behind dedicated current-thread OS-thread owners. Metadata loads are serialized per venue/profile, concurrent cache hits share owned `Arc<CatalogSnapshot>` values, refreshes are generation-checked, failed refreshes retain the previous owned snapshot, requester cancellation does not cancel a shared load, shutdown cancels and joins owners, and stock provider panics are converted to upstream errors at the owner boundary. Conversion retains owned raw metadata, native IDs, product/settlement/DEX identity, active status, contract size, minimum amount, and truthful precision handling. Resolver aliases are exact and fail closed on ambiguity or conflicting product selectors.
- **Support ledger:**
  - `binance`: stock catalog exposes spot, linear, inverse, future and option metadata; Phase 1 default selects USD-M linear and the explicit profile selects other supported products. Stock Pro source implements per-symbol and aggregate trades, OHLCV and order books with unwatch methods; Binance order-book depth/rate are subscription parameters. OI, mark/index and funding have unified stock sources; OI units differ by product and are not globally normalized.
  - `bybit`: stock catalog exposes spot, linear, inverse, future and option metadata; omitted catalog selection retains the legacy linear+inverse+spot default, while request profiles are explicit. Stock Pro source implements per-symbol and aggregate trades, OHLCV and order books with unwatch methods; depth is a product-specific allowlist. Unified OI and funding plus raw ticker mark/index/OI are source-backed; linear/inverse OI units differ.
  - `hyperliquid`: stock catalog exposes spot and swap metadata, including primary/DEX identity qualification. Stock Pro source implements per-symbol trades, OHLCV and order books and unwatch; aggregate channel methods are absent. Unified OI/funding sources exist; OI amount unit is not qualified by the pinned source.
  - `lighterxyz` (`lighter` in CCXT): stock catalog exposes spot and swap metadata and numeric native market IDs. Stock Pro source is per-symbol for trades and order books; aggregate trades/order books and OHLCV watch are confirmed absent. REST/Pro raw metadata and market-stats payloads retain OI/mark/index/funding, but OI units are unqualified; book nonce provenance is the stock offset, not the old native nonce.
  - `aster`: stock catalog exposes spot and swap metadata; default selection retains perpetuals. Stock Pro source implements per-symbol and aggregate trades, OHLCV and order books with unwatch methods. Funding and mark/index sources are present; no OI unified or implicit source was found in the pinned stock API.
  - `extended`: stock catalog and per-symbol Pro trades, OHLCV and order books are implemented in source; aggregate watchers and all unwatch methods are absent. Raw market statistics retain current mark/index/funding/OI fields and an OI-history implicit endpoint, but current OI/funding unified methods are absent and units require qualification. Standard order books remain indicative, not RFQ real books.
  - URL overrides are mapped only where stock configuration accepts them: Binance, Bybit, Hyperliquid, Lighter REST/WS, Aster REST, and Extended REST/WS. The stock WS URL registry is process-global; future live owners must share one coherent owner per actual URL. Full live lifecycle/mixed-channel correctness remains Phase 3 work.
- **Checks actually run:** `cargo tree --locked -e features -i ccxt-base` showed only `aster`, `binance`, `bybit`, `extended`, `hyperliquid`, `lighter`, plus engine/default feature nodes; no unrelated exchange feature was enabled. `cargo test --locked --test ccxt_catalog -- --nocapture`: 2 passed. `cargo test --locked exchanges::ccxt -- --nocapture`: 16 passed. `cargo run --locked --example ccxt_phase1_smoke`: five real catalogs loaded (Binance, Bybit, Hyperliquid, Lighter, Aster), shared Lighter refresh and owner shutdown passed; Extended’s stock `/api/v1/info/assets` request returned HTTP 403 and was recorded as an external API availability limitation. The user accepted that limitation for Phase 1; no Extended catalog was claimed. Existing unmigrated production routes remain the temporary baseline; REST/live/statistics cutover is intentionally unverified and not implemented in this phase.
- **Compatibility differences and next phase:** catalog responses now expose stock-derived product/settlement/precision/raw-info identity; Hyperliquid spot IDs use metadata `@index`, Lighter remains public `lighterxyz` while stock uses `lighter`, and Extended’s source availability remains unqualified until a later real upstream check. No whole-provider removal was made. Phase 2 is authorized and ready to route REST snapshots through the new core; Phase 3–5 remain locked.

### Phase 2 handoff — 2026-10-02

- **Status:** `completed`; the user accepted Phase 2 on 2026-10-02. Phase 3–5 remain locked pending their respective authorizations. No native fallback is used by the four migrated HTTP market-data snapshot endpoints.
- **Implementation:** `src/exchanges/ccxt/{mod,rest,owner,venue,catalog,convert}.rs` now provide the `CcxtExchange` facade, owned snapshot commands, metadata resolution, request policy, stock dispatch, and strict DTO conversion. `src/main.rs` registers all six facades. `src/models.rs` makes only the candle volume cell nullable; `src/realtime.rs` and `src/ws_shared.rs` adapt their constructors without migrating native streams or changing their existing wire values. `src/errors.rs`/the HTTP boundary preserve flat ordinary errors and nested catalog errors, including explicit unsupported-feature responses.
- **Acquisition:** unified stock methods handle supported snapshots. Hyperliquid public trades use shipped `public_post_info`/`recentTrades` plus the stock trade parser, not the unified user-fills method. Lighter uses shipped `public_get_recent_trades` plus its stock parser. Bybit full/RPI books use shipped implicit endpoints and the stock book parser. No handwritten HTTP client or book synchronizer was added for migrated snapshots.
- **Request/cache behavior:** Binance remains USD-M by default; Bybit data remains linear by default and its catalog combines linear/inverse/spot. Existing straightforward limits, inclusive millisecond time bounds, trade/candle ordering, selector precedence, and top-N book projection are retained. Nondefault acquisition profiles require explicit product params even with an opaque catalog ID. The public response cache retains its 30-second timestamp/key behavior; owner metadata now refreshes on demand after 30 seconds instead of remaining loaded indefinitely. Failed refreshes do not overwrite the prior owned observation.
- **Removal:** deleted native implementations of the four snapshot operations and snapshot-only helpers from all six exchange modules; removed Hyperliquid's additional trade collector/history cache and `TRADE_CACHE_CAPACITY_PER_COIN`, `TRADE_CACHE_RETENTION_MS`, `TRADE_COLLECTOR_ENABLED`. Retired `src/bin/market_stream/**`, `src/bin/orderbook_probe.rs`, and the unused `crossterm` dependency. Migrated affected statistics fixture registrations to `CcxtExchange`, removed obsolete native-catalog timing/projection tests, and updated live smoke assertions that incorrectly required input-symbol echoes.
- **Temporary native dependencies, deliberately retained:** Phase 4 statistics still acquire Hyperliquid `metaAndAssetCtxs`/`spotMeta`, Binance/Aster instrument and funding metadata, Bybit category instruments/tickers, Extended `/info/markets`, and Lighter `orderBookDetails`/native market statistics. Their caches, identity helpers, and source-specific DTOs remain for those consumers only. Phase 3 still owns native realtime runners, Binance/Aster book snapshot seeds, Bybit full-book synchronization, and Lighter's Explorer-backed realtime resolver. REST catalog identity for a product does not expand existing statistics support.

**Focused verification actually run**

- `cargo test --locked --lib exchanges::ccxt -- --nocapture`: 18 passed, including malformed/nonfinite candle and book rejection and missing-volume preservation.
- `cargo test --locked --test ccxt_catalog --test market_stats`: 2 catalog lifecycle tests and 38 affected statistics tests passed.
- Three targeted existing native-candle compatibility checks passed: `realtime::tests::aster_dispatches_binance_compatible_trade_and_candle_updates`, `realtime::tests::extended_realtime_resolves_aliases_and_reconnects_on_gap`, and `ws_shared::extended_tests::extended_symbols_timeframes_and_candles`.
- `cargo build --locked --bin ferris-market-data-backend` succeeded. Formatting was applied with `cargo fmt --all` and targeted `rustfmt` after the owner change. Warnings remain for four unused native realtime constants and one test-only statistics fixture helper; no release/container/full-backend campaign was run.
- Launched the actual backend and exercised all four migrated HTTP families with real data from Binance, Bybit, Hyperliquid, Lighter, and Aster. Bybit's full book returned 10,000 rows per side; Binance/Bybit RPI requests succeeded. Binance and Bybit spot/inverse catalog IDs resolved to the intended books with explicit selectors. Candle time bounds, historical default end windows, coin/native-ID and display-depth precedence, native timeframe aliases, canonical catalog-cache keys, and supported error/rejection envelopes were exercised.
- Live Binance/Bybit mark/index/premium-index and Aster mark/index candles returned HTTP 200. The earlier Bybit failure was missing stock volume, not missing price data: after preserving it as null, all three Bybit price-source paths passed.
- `FERRIS_BASE_URL=http://127.0.0.1:18879 FERRIS_TEST_EXCHANGE=bybit FERRIS_TEST_SYMBOL=BTCUSDT FERRIS_TEST_MARKETS_EXCHANGE=bybit cargo test --locked --test live_endpoints -- --ignored`: 5 passed. `python3 scripts/smoke_endpoints.py --base-url http://127.0.0.1:18879 --exchange lighterxyz --symbol BTC --markets-exchange lighterxyz --wait-seconds 0`: all endpoint checks passed. PowerShell smoke changes were not executed.
- Extended's actual assets request returned upstream HTTP 403, as in Phase 1. A local HTTP fixture, reached through the real backend and stock Extended methods, verified all four endpoint families, native identity, mark/index null volume, candle-type/end-time precedence, and URL override handling. A timed metadata-change scenario verified an unchanged cached response before expiry, then updated tick size/inactive filtering after 30 seconds with exactly one additional metadata load. This proves local dispatch/conversion/cache behavior, **not Extended live data availability**.

**Compatibility and verification limits**

- Full caller guidance is in [README.md](README.md#ccxt-rest-snapshot-contract) and [INTEGRATION_README.md](INTEGRATION_README.md#ccxt-snapshot-integration); `ROADMAP.md` records the removal decisions. Trade/book symbols and raw `info` follow stock output rather than echoing native aliases. Extended REST uses `BASE/USDC:USDC` while its catalog/native realtime retain USD naming. Hyperliquid spot identity is metadata `@index`; Lighter uses stock USDC/numeric-ID metadata.
- Missing stock candle volume is null, not invented zero. Extended's trade `type` may be null while native `tT` remains in `info`. Book timestamp/nonce provenance follows stock: Extended REST time is receipt time; Lighter REST time/nonce and Bybit full/RPI nonce are absent. Lighter REST returns source orders, potentially multiple rows at one price, limited to 100 per side rather than the former 250-depth request cap. Excess depth and unavailable options return explicit errors. Hyperliquid history is limited to the upstream recent window.
- Newly exposed product/price-source paths are stock-backed, but focused live evidence is limited to the requests above; not every listed option market, expiry, settlement, or spot instrument was exercised. Extended live availability remains unverified while its assets request returns HTTP 403. On 2026-10-02 the user confirmed this is a known issue with Extended's API, not Ferris, and instructed agents to ignore it as a migration blocker. Defer further live qualification until the upstream API recovers; do not reopen Phase 2 or add a Ferris workaround for this outage. The venue remains registered with no alternate acquisition path.
- **User acceptance / next agent:** Phase 2 is approved and complete, including the accepted Extended upstream limitation. The next implementation phase is Phase 3 (realtime streaming); start it only when the user explicitly authorizes it. This approval does not authorize Phase 3 work.

### Phase 3 handoff — accepted continuity risk — 2026-10-02

- **Status:** `ready` to resume, not `awaiting_review` or complete. The user superseded the earlier block by accepting the demonstrated stock Binance post-bridge continuity risk for now. Native realtime remains the temporary Phase 2 baseline until replacement, not a newly introduced fallback. No provider or deep-book feature was removed. Phases 4–5 remain locked.
- **Completed independent change:** `src/web.rs::spawn_orderbook_topic_forwarder` now serializes `WsOrderBookView` borrowed slices of the owned shared book instead of cloning both full-depth vectors and truncating each client copy. Wire fields, optional values, topic envelopes, and bounded fanout are unchanged. `README.md` records this behavior. The owner-service replacement and native-parser removal are not implemented or claimed complete.
- **New decisive runtime evidence:** a temporary `ccxt_phase3_continuity_smoke` executable ran the actual stock `ccxt_pro::pro::binance::BinanceCore::watch_order_book` over local HTTP and WebSocket connections on one current-thread runtime. Stock rate limiting and the default `watchOrderBook.checksum = true` remained enabled. Metadata was provided through stock constructor configuration, the watch/seed depth was 1,000, and no runtime patch or Ferris synchronizer was involved. After the fixture's REST seed `lastUpdateId=100`, a valid initial delta `U=95, u=101, pu=94` established the bridge and returned owned nonce `101` with bid `[100, 2]`. The next delta `U=100, u=103, pu=102` breaks continuity because the previous final ID is `101`, not `102`. Stock nevertheless returned nonce `103` and bid `[100, 9]`, without rejection or reseed. Output: `CONTINUITY_RESULT accepted broken pu: previous_u=101 incoming_U=100 incoming_u=103 incoming_pu=102; owned_nonce=Some(103) owned_bids=[[100.0, 9.0]]`. This is a new post-bridge case, not a rerun of the already-recorded premature-seed finding.
- **Cause and contract:** pinned `ccxt-pro-4.5.85/src/pro/binance.rs:1751–1757` accepts `U <= nonce || pu == nonce` for every futures delta, not only the initial bridge. Thus the initial overlap condition bypasses subsequent `pu` continuity even with checksum checking enabled (`:585–588`). [Binance's required sequence contract](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly) requires every post-bridge `pu` to equal the previous `u`, otherwise resynchronize. The stock output contains the final nonce but not the incoming `U`/`pu` or a verified synchronization state; a higher nonce or skipping the first result cannot repair or detect this violation. Source also retains the separately documented unconditional REST-seed publication at `binance.rs:1578–1634`.
- **Stock alternative checked:** the [published Pro sparse index](https://index.crates.io/cc/xt/ccxt-pro) ends at `4.5.85`; no newer published Pro release was available during this check. Exact pins/features are unchanged. A distinct stock URL/current-thread owner can isolate cold initialization but does not repair this sequence condition. Keeping acquisition depth at 1,000 and slicing only the owned output avoids viewer-induced truncation; it does not establish synchronization. No compliant configuration-only fix for the demonstrated continuity violation was found.
- **Additional source findings, not runtime qualification:** stock Aster books subscribe only to partial-depth 5/10/20 streams (`pro/aster.rs:1500–1549`), so the old deep diff-book path is absent from that implementation. Its unwatch waits for an unsubscribe result, but the event-only `handle_message` has no acknowledgement dispatch (`:1567–1615`, `:2872–2893`). Extended has per-symbol book URLs and no unwatch override; its accepted upstream API outage was not retried. `ws_client::drop_client` only removes the global registry entry while connection tasks hold `Arc<ClientState>`; do not claim that call alone disconnects a shared owner. Final dedicated-runtime teardown and per-symbol cleanup on active shared URLs require separate qualification. Proposed union-hash driving/nonblocking control registration is not yet proven for mixed-channel lifecycle; it must not be shipped on source plausibility alone.
- **Focused checks actually run:** `cargo tree --locked -e features -i ccxt-base` resolved `4.5.85` with only the six intended providers plus base/default/engine nodes; no all-exchange feature. `cargo test --locked --lib web::tests:: -- --nocapture`: 4 passed, including independent book projection and full outgoing-queue behavior. `cargo build --locked --bin ferris-market-data-backend`: succeeded, with the four previously recorded unused native realtime constants. `cargo run --locked --example ccxt_phase3_continuity_smoke`: produced the broken-continuity observation above; the temporary program was removed after recording its evidence. No broad integration/release/container run.
- **Actual backend smoke:** launched the updated binary and connected two real `/v1/ws` clients to the existing Bybit linear book source with display depths 3 and 7. Both received the same source timestamp, correct independent depth, ordered noncrossed two-number levels, and `BTC/USDT:USDT`. Duplicate subscribe returned `alreadySubscribed`; removing the depth-3 viewer left newer depth-7 updates flowing; `ping` returned `pong`; final unsubscribe and graceful server shutdown succeeded. This proves the changed borrowed serialization through the real client boundary, **not CCXT realtime acquisition**. Warm/cold mixed-channel CCXT lifecycle, reconnect, final quiet-source delivery, and all-venue streaming remain unqualified.
- **Latest user decision:** after the initial “Wait for CCXT to fix it” choice, the user explicitly instructed: “just document in phase 3 that this occurs but we are just going to ignore it for now.” This supersedes the wait decision. Treat this specific post-bridge Binance continuity failure as an accepted temporary risk, not a Phase 3 blocker. Keep stock-only dependencies; do not wait for an upstream fix, implement a custom patch/synchronizer, or re-escalate the same known failure. Preserve its evidence and report the limitation honestly. Other correctness, safe ownership, lifecycle and verification requirements remain; no phase acceptance or Phase 4 work is authorized. This documentation update changes no realtime acquisition code.

### Phase 3 handoff — stock realtime cutover — 2026-10-02

- **Status:** `completed` with user approval recorded on 2026-10-03, superseding the earlier `awaiting_review` status and ready-to-resume handoff. Phase 4 was still locked at that approval; the later authorization and handoff below supersede that boundary. No known stock book defect or the accepted Extended API outage was treated as a blocker or repaired with a patch/fallback.
- **Implementation:** `src/exchanges/ccxt/{live,stream,owner,convert,venue,mod}.rs`, `src/realtime.rs`, `src/web.rs`, and `src/main.rs` now route trades/books/candles through stock Pro `4.5.85`. REST/catalog work remains on its separate owners. One mutable live core/current-thread OS runtime owns each actual stock URL and drives every result hash via stock `ws_run`; no sequential quiet-watch loop, cross-thread stock values, fake URL aliases, worker process, or hand parser was introduced. Binance allocates one feed per real stock stream slot to isolate deferred REST seeds; other venues share nonconflicting feeds/channels at the actual URL.
- **Lifecycle:** RAII receivers own viewer demand. Display depth is outside upstream identity; joining viewers never reload markets/reseed or replay a saved snapshot. Dropping the final viewer invalidates its publication epoch. Reconnect destroys the old runtime/thread (including its deferred queue and socket tasks), releases the registry entry, then resubscribes under a new epoch. Shared-URL retirements use stock unwatch/control hashes, targeted cleanup, and a 10-second retirement deadline; delayed acknowledgements cannot strand unrelated settlements. Hyperliquid's stock `[UnsubscribeError]` is handled as an unsubscribe lifecycle result. Extended's per-channel URLs close on final demand rather than calling its absent unwatch methods.
- **Delivery:** only owned Ferris DTOs cross runtimes. Stock cache cursors deliver incremental trade/candle batches, including Lighter, without rolling-window replay. Extended price-candle cache keys include candle type. `subscribed` is enqueued before forwarding begins. Book views serialize borrowed top-N slices of shared owned state, not per-viewer full-depth copies. Epoch checks discard retired publications. Timestamp-less Binance REST seeds are withheld, but this gate is not a proof of valid stock bridging/continuity or a fix for accepted retention defects.
- **Bounds:** 512 publications per shared feed, 256 outgoing frames per client, 200 realtime subscriptions per client (statistics retain their separate 16), 128 active URL owners per service, and 200 feeds per shared URL. Capacity errors do not break existing viewers; duplicates at the client limit still acknowledge. Slow/full outgoing queues cancel forwarding and the blocked writer. Binance live workers reserve 8 MiB stacks (the real dev build overflowed 2 MiB); other live workers reserve 2 MiB. Stock rate limiting stays enabled. These are bounded admission/fanout choices, not measured capacity for 400 viewers/20,000 distinct topics or an aggregate-IP quota guarantee.
- **Removal:** native `Real*UpstreamRunner`/`Upstream*Stream`, `src/binance_orderbook.rs`, `src/bybit_full_orderbook.rs`, native book/trade/candle parsers in `src/ws_shared.rs`, snapshot-provider implementations, and Lighter's Explorer realtime catalog are gone. `LIGHTER_MARKETS_URL` / `LIGHTER_MARKET_CATALOG_REFRESH_MS` were removed from configuration. `src/ws_shared.rs` retains only scalar/timestamp helpers and constants still consumed by statistics/configuration. Native statistics modules, `binance-sdk`, `reqwest`, and `tokio-tungstenite` remain for those genuine Phase 4 consumers; no new native realtime fallback remains.
- **Client differences:** all six venues expose trades/books; Hyperliquid now exposes WS candles, Lighter has no stock candle watcher. Binance retains the 1,000-level contract backing profile (spot 5,000), Bybit live max is stock 1,000 (options 100), Aster live max is stock partial 20, Hyperliquid 20, Extended 1,000, Lighter all retained stock levels. Bybit's former 10,000-level live full-book and Aster deep diff-book paths are removed; over-depth fails. Bybit option candles remain unsupported. Binance/Extended mark/index candles and metadata-selected stock products are exposed. Extended live symbols now match stock `BASE/USDC:USDC`; missing candle volume is null. Lighter book nonce is stock `offset`; Bybit `u`, Binance stock event `E`, Extended WS `ts`/`seq` retain their source meanings. README and frontend integration guidance record exact limits, params, new channels, and accepted risks.

**Focused checks actually run**

- Before the final smoke, the earlier focused run reported 83 passing checks. The real backend then exposed an undersized Binance stack and Bybit trade-result hash mismatch; these were fixed rather than counted as upstream book risks.
- `cargo test --locked --lib exchanges::ccxt::venue::tests -- --nocapture`: 2 passed. Actual stock Binance/Bybit watches over loopback deliver consecutive trade IDs without replay through the production live owner. Binance registration exercises its corrected stack; Bybit dispatch exercises the corrected stock result hash.
- `cargo test --locked --test realtime_ws -- --nocapture`: 5 passed. Stock Hyperliquid wire fixtures cover shared views, warm A while cold B is removed/re-added with late acknowledgements, reconnect/epoch invalidation, mixed trades/books/candles at one URL, quiet-source final delivery, protocol errors/acks, final retirement, and shutdown. Stock Extended fixtures cover incremental trades, book snapshots/deltas, separate trade/mark candle caches, null volume, and final socket retirement. Expected caught stock `[NetworkError]` / `[UnsubscribeError]` panic-hook messages appear during deliberate disconnect/control cases; the cases pass without suppressing the source condition.
- `cargo test --locked --lib web::tests -- --nocapture`: 3 passed. `cargo build --locked --bin ferris-market-data-backend`: passed, without the former unused native realtime constants.
- Launched the actual backend and connected real `/v1/ws` clients. Public BTC perpetual trades/books/candles arrived for Binance, Bybit, Hyperliquid, Aster; Lighter delivered trades/books. At an observed checkpoint: Binance 1,248 books/454 trade batches/138 candle batches; Bybit 642/43/39; Hyperliquid 25/82/79; Aster 475/22/24; Lighter 1,692/180/0. No source errors or repeated non-null trade IDs were seen in that observation window. Checked finite two-number levels, per-view depth, and six-cell candle rows. These counts demonstrate flow, not a benchmark or synchronization qualification.
- With BTC warm on Bybit, added ETH depth 11, removed/re-added it, and unsubscribed/re-added BTC candles; ETH and candles resumed, BTC advanced throughout, no errors. All live clients received final unsubscribe acknowledgements and `pong`.
- A temporary local Extended HTTP/WS fixture fed the actual backend (no external API retry). Before correction, only books arrived despite four subscription acknowledgements; stock trade/candle hash and price-cache-key mismatches were fixed. Afterward two trade IDs arrived as separate batches, books advanced, and trade/mark candles updated independently with null mark volume; source silence did not trigger replay.
- Actual-client bounds smoke: 200 distinct Extended depth views shared one source; view 201 returned `SUBSCRIPTION_LIMIT`; a duplicate still acknowledged, and unsubscribe released a slot for the rejected view. With a paused TCP/WS reader and a concurrent fast client, sent 600 full 1,000-level fixture books: the slow client was disconnected, the fast client received all 600 plus a final 601st update without errors. All fixture sockets retired; smoke services/fixtures were stopped.

**Accepted limitations / next gate**

- The earlier deterministic Binance `pu` failure remains failing evidence. Known stock book correctness/retention issues across venues remain accepted temporary risks; no live nonce/count result is called proof of synchronization. Extended real upstream availability remains unverified after its accepted API failure. No exhaustive spot/options/expiry/source matrix, long-duration stock allocator soak, global-IP quota test, latency SLA, release/container build, or full statistics integration run was claimed; Phase 5 retains its broader verification scope.
- **Issue register (2026-10-03):** [CCXT_KNOWN_ISSUES.md](CCXT_KNOWN_ISSUES.md) preserves the exact Binance failure sequence, other accepted upstream limitations, implemented lifecycle mitigations, fixed Ferris regressions, qualification gaps, and per-issue closure checks. Added at the user's request for future remediation; documentation only, with no new runtime verification or change to phase approval.
- **User acceptance / next gate (2026-10-03):** the user requested Phase 3 be marked completed; approval is recorded in the tracker. Phase 4 may replace native statistics acquisition/add the approved metrics only after separate explicit authorization. This completion approval does not authorize Phase 4.


### Phase 4 handoff — stock statistics and numeric metrics — 2026-10-03

- **Status:** `completed` with user approval recorded on 2026-10-03, superseding the earlier `awaiting_review` status. The user accepted Phase 4 and requested it be marked completed. Binance selected-only OI remains approved; the 2026-10-04 decision replaces unsupported Lighter OI with two-sided USDC notional (null amount). All six venues remain registered. Phase 5 stays locked pending explicit authorization to start.
- **Acquisition:** `src/exchanges/ccxt/{statistics,statistics_profile,owner,mod,venue}.rs` replaces native statistics with shared stock calls and catalog identity. Binance uses bulk tickers/funding/intervals plus selected `fetchOpenInterest`; Bybit uses bulk tickers and stock OI parsing; Aster uses tickers/funding/intervals; Hyperliquid uses primary-DEX contexts; Extended uses bulk tickers. Binance options use stock mark prices and implicit index calls shared per underlying, never settlement-sensitive ticker `exercisePrice`.
- **Maintained source:** `src/exchanges/ccxt/{live,stream}.rs`, `src/exchanges/traits.rs`, and `src/market_stats.rs` attach Lighter Pro `watchTickers` to the shared real-URL owner. REST supplies last/volume; live supplies funding, settlement, mark/index, two-sided OI and updates last/volume. Field receipts remain independent; polling or reading cached state cannot freshen silent live values.
- **Delivery/state:** `src/models.rs` adds strict required-but-nullable finite numeric `Volume24hValue`/`OpenInterestValue` variants and retains funding/price strings. Coordinator/projection and `src/web/market_stats_stream.rs` preserve field equality, selected-ID proof, coverage/removal, stale retention, invalid-value clearing, and revisioned snapshot/delta coalescing. Thirty-second polling and independent 90-second expiry remain, including during pending acquisition; Aster/Bybit funding also expires at the native payment boundary.
- **Singular demand:** Binance OI requires 1–100 selected IDs, unions overlapping HTTP/WS demand, shares observations/deadlines per market, and never runs an all-market sweep. HTTP renews a 90-second lease; WS demand is reference-counted. Funding-only, inactive and spot selections do not trigger OI calls. All-market OI returns HTTP 400 / `VALIDATION_ERROR` or the corresponding WS error.
- **Support/units:** capabilities remain no-I/O and product-specific. Aster OI is unsupported because stock lacks the method; Lighter OI is two-sided USDC notional (`2 ×` WS `open_interest`) with null amount, matching the venue UI. Binance inverse OI is contracts, Bybit inverse OI is USD value, and absent members remain null. Option quantities/contract multipliers and all remaining metric denominations are recorded in [README.md](README.md#numeric-statistics-units). Binance option base volume is null unless explicit catalog unit is 1; no Ferris conversion is performed. Hyperliquid last price stays unsupported rather than publishing midpoint; funding history is unsupported everywhere.
- **Removal/client guidance:** `main` registers only `CcxtExchange`. All six native exchange/statistics modules and the remaining `src/ws_shared.rs` transport helpers are removed. `binance-sdk`/`chrono` are removed; direct `reqwest`/`tokio-tungstenite` remain dev-only for tests/examples. README, integration guidance, roadmap and migration notes document actual methods, product selectors, nullable numbers, units and source timestamps. Sources now identify stock methods, not retired native endpoints; missing exchange times remain null.

**Focused checks actually run**

- `cargo test --locked --lib --test market_stats --test ccxt_catalog --test realtime_ws --no-fail-fast`: **118 passed** — 74 library, 37 statistics integration, 2 catalog, 5 realtime. Covers strict numeric decoding, independent receipts/expiry, demand sharing, failures/recovery, authoritative removal, no-I/O capabilities, and retained shared-owner lifecycle behavior. `cargo build --locked --bin ferris-market-data-backend`: passed.
- Actual backend `/v1/fetchMarketStats` and `/v1/ws` delivered statistics for Binance, Bybit, Hyperliquid, Aster and Lighter public perpetuals. Bybit spot/inverse also passed. Final rebuilt-server checks passed Binance spot/inverse/options and Bybit options; an untraded Binance option correctly returned zero volume/OI and invalid last price rather than a fabricated price.
- Extended loopback through stock methods and the actual backend passed numeric deltas, zero/one-null-member values, nonfinite clearing, failed-source retention without resurrection, recovery, coverage and authoritative market removal. Lighter finite stock frames followed by silence expired funding without freshening it from REST volume. Both HTTP/WS paths were exercised again on the final build. Extended's accepted public HTTP 403 issue was not retried; loopback does not qualify public availability.
- Boundary failures were reproduced before fixes: a Binance spot row published fixture futures price `30001.20` instead of `500.00`; option ticker/mark calls used contract defaults; malformed OI became missing; explicit invalid Bybit funding interval incorrectly used instrument metadata. The rebuilt server passes HTTP and WS assertions for each. A non-unit option fixture verifies null base volume, unchanged contract OI and a distinct underlying index. A final WS OI sequence delivered zero/null at revision 2, NaN clearing at revision 3, and recovery to 12.5/null at revision 4, matching HTTP.

**Limits / next gate**

- No known in-phase defect remains from these checks. Existing accepted stock order-book risks are unchanged. No exhaustive per-instrument product matrix, long-duration/400-user quota-capacity claim, release/container verification, or deployment is claimed; the authorized focused checks do not replace Phase 5 integration.
- **User acceptance / next gate (2026-10-03):** the user accepted Phase 4 and requested it be marked completed; approval is recorded in the tracker. Phase 5 is the remaining final integration/removal/shutdown and broad HTTP/WS/release/container verification stage, including any Ferris fixes those checks require. This approval does not authorize starting Phase 5 or deploying. Existing unsupported features and accepted upstream defects remain documented; no native fallback or dependency patch is authorized.

### Phase 5 handoff — final integration — 2026-10-03

- **Status:** `awaiting_review`, explicitly authorized by the user on 2026-10-03.
  Phases 1–4 retain their approvals; Phase 5 acceptance and deployment require the user.
- **Runtime fix:** `src/main.rs`, `src/web.rs`, and
  `src/exchanges/ccxt/{owner,live}.rs` now stop accepting, cancel pending WS
  commands, close/join client forwarders and writers, stop statistics, cancel/join
  live and catalog owners, and then finish Axum's HTTP drain. Binding happens
  before owner startup. A live-owner shutdown error no longer skips remaining
  live joins or catalog shutdown. `tests/shutdown.rs` covers the actual executable.
- **Before/after proof:** a loopback stock `/info` request held indefinitely with
  `REQUEST_TIMEOUT_MS=60000` blocked the old SIGTERM path beyond three seconds.
  The fixed executable exits successfully within that bound. With all six venues'
  mixed realtime and statistics clients active, SIGTERM exited 0 in 0.206 seconds;
  all client collectors ended and no Extended fixture source sockets remained.
  These observations do not establish a shutdown latency SLA.
- **Removal/configuration:** confirmed the section 5 native adapters, statistics
  transports, synchronizers, parser helpers, collector/Explorer services, terminal
  tools, exports, and obsolete production dependencies were already removed by
  earlier phases. Removed the disposable `examples/ccxt_phase1_smoke.rs` and empty
  native directories. Removed the incidental pointer-identity unit test, retaining
  changed-row/receipt behavior and real stock lifecycle coverage. No fallback,
  alias, dependency patch, custom transport, or new setting was added. Existing
  URL overrides and timeout/bind settings remain stock-backed.
- **Feature proof:** `cargo tree --locked -e features -i ccxt-base` resolved exactly
  `4.5.85`, the six venue features, and `engine`; the transitive base `default` is
  empty. Neither `ccxt` nor `ccxt-pro` enables `all`. Public `lighterxyz` maps to
  stock `lighter`; all six remain registered in no-I/O capabilities.
- **Tests actually run:** `cargo test --locked --all-targets -j 1` passed 119 tests
  (74 library, 37 statistics, 2 catalog, 5 realtime, 1 executable shutdown), with
  5 opt-in live tests ignored. After deleting the incidental library test, final
  `cargo fmt --all --check` and
  `cargo test --locked --lib --test shutdown -j 1` passed (73 + 1); the retained
  tested suite is 118 tests. `cargo test --locked --doc -j 1` passed with no doctests.
  The optional historical example target had no tests and was removed.
- **Actual backend HTTP/WS:** exercised health/capabilities, catalogs, trades,
  six-cell candles and two-number/depth-limited books for all six venues; Binance,
  Bybit, Hyperliquid, Aster and Lighter used public perpetual feeds, Extended used
  stock methods over loopback (its accepted public failure was not retried).
  Mixed WS trades/books/candles delivered for every implemented channel; Lighter
  candles rejected explicitly. Five public venues produced no repeated non-null
  trade IDs in the observation window. Ordinary unregistered-exchange HTTP 400,
  nested `fetchMarkets` HTTP 422, Bybit option-candle HTTP 501, and Binance
  all-market OI HTTP 400 were verified, with WS errors leaving ping usable.
- **Lifecycle:** warm Bybit BTC kept advancing during cold ETH add/remove/re-add.
  The Extended fixture delivered two finite trade/book/candle bursts followed by
  silence with no cache replay, then all three channels resumed after deliberate
  source disconnect/reconnect. Replay checks are within a connection epoch; the
  fixture intentionally reused trade ID 1 in the new epoch. The backend suite
  also re-exercised shared-URL mixed channels, final silent-source delivery,
  delayed retirement acknowledgements, epochs and source cleanup.
- **Statistics:** actual HTTP and WS snapshots/deltas passed for all six. Extended
  delivered zero amount/null value OI and one-null-member volume at revision 2,
  source failure retained values/receipts as `stale` at revision 3, and recovery
  published 12.5/null OI plus authoritative ALT removal at revision 4. HTTP agreed;
  coverage became 1 expected/1 returned, complete, with no source failure. The
  permanent suite rechecked strict numeric decoding/invalid clearing and Lighter
  finite live frames expiring independently of continued REST polling.
- **Container work:** `Dockerfile` pins `rust:1.98.1-slim-bookworm`, copies
  `Cargo.lock`, and builds only the locked backend release with one Cargo job.
  An initial image attempt exposed a pre-existing 35-GB `src/target` cache entering
  the context because `.dockerignore` covered only root `target`; it was cancelled.
  `.dockerignore` now excludes `**/target` too, without deleting the user's cache.
  The final build context was 1.298 MB. `docker build --tag ferris:phase5-review .`
  passed; the locked optimized build took 11m54s (entire image build 769s).
  Image ID: `sha256:a782887bb0d3a590498f88849fb39b1e49e1cdaeb85e39fc746a64d8a3fa91ef`.
  Image size reported by Docker: 52,115,452 bytes. The actual image entrypoint ran
  as `app` (uid/gid 1000), with all dynamic libraries resolved. Release-container
  health/six-venue capabilities, public Hyperliquid and loopback Extended catalogs,
  trades/candles/books, HTTP/WS statistics and public Hyperliquid live books passed.
  `docker stop` with an active WS client closed it and exited 0, without OOM/error;
  the smoke container and fixture were removed/stopped. The local review image is
  retained; nothing was pushed or deployed. This verifies the locked release inside
  its actual runtime image, not just an independent host binary.
- **Client docs:** README, integration guidance and roadmap now describe the
  complete CCXT-only acquisition boundary, support/depth losses and additions,
  nullable candle volume, raw-info/symbol/sequence provenance, numeric metric units,
  retired tools/settings/history, and single-release deployment/previous-release
  rollback with no embedded native fallback.
- **Limits:** known stock book continuity/retention issues remain accepted risks,
  not passing synchronization evidence. Extended public availability, exhaustive
  per-instrument products/depth, long-duration allocator/storage soundness,
  400-user capacity, aggregate-IP quotas and a latency SLA remain unqualified.
  No publishing or deployment was performed.

### Production deployment — user-authorized — 2026-10-03

- The user requested building and replacing the existing Lattice Terminal service
  so the frontend can exercise the public API. This supersedes the earlier
  deployment lock, without inventing a separate Phase 5 completion approval.
- `docker build --tag ferris:lattice-ccxt-20261003 .` passed using the unchanged,
  previously verified locked release layers (`a782887bb0d3`). Extracted the exact
  executable into `/usr/local/lib/latticeterminal/releases/20261003-ccxt-a782887bb0d3/`.
  Binary SHA-256: `6b467e319427006d377f4ff40acfe3b6c391975f93d2ff826892a543631624dd`.
- Preserved the running previous executable before replacement (SHA-256
  `943b695bf651ad9bfd3ce54b2c65c7cb292c69199313e0da239c444812427fbd`), original unit,
  environment and a validated rollback unit under
  `/usr/local/lib/latticeterminal/releases/20261003-pre-ccxt/`.
  The original service referenced an absent build-output path; deployment and
  rollback now use stable release paths, not the mutable Cargo target directory.
- Updated `/etc/systemd/system/latticeterminal.service` to run the new release and
  load `/root/Ferris/.env.prod`; kept the existing service user/restart policy and
  enabled-at-boot state. Activated at 07:12:18 UTC. The backend now binds only
  `127.0.0.1:8787`; unchanged Caddy provides public HTTPS/WSS at
  `api.latticeterminal.com`. Backed up and disabled the unrelated already-failed
  duplicate `ferris.service`, whose executable/environment paths were nonexistent.
- Host preflight passed six-venue capabilities and an actual Binance book before
  restart. After cutover, the running executable hash matched the image artifact.
  Public HTTPS health/capabilities, catalog/trades/candles/books/statistics passed
  for Binance, Bybit, Hyperliquid, Aster and Lighter. WSS mixed data plus statistics
  snapshot/delta, unsubscribe and ping passed for those five venues (Lighter
  candles remain unsupported). CORS preflight and `http://localhost:5173` WS origin
  passed through the public edge. Service stayed active/enabled with zero restarts.
- A positive-price smoke assertion exposed source-supplied zero-price/zero-size
  Binance `trade` rows (`p/q="0"`, `X="NA"`, `st=1`) preserved in stock `info`.
  Documented as [CCXT-012](CCXT_KNOWN_ISSUES.md#ccxt-012), not suppressed or called
  a book-risk exception. The explicit existing `name:"aggTrade"` option delivered
  18 positive rows in 15 seconds; it changes aggregation semantics, not the default.
- Existing external production requests exposed Extended's known assets 403 as
  upstream 502 in service logs; the deployment smoke did not retry that known
  failure. No frontend application itself was run. The service is ready for
  frontend integration testing, not certified for capacity or stock book correctness.
- Deployment/rollback commands and frontend caveats are recorded in README and
  INTEGRATION_README. Rollback was prepared/validated, not activated.

### Phase 1 — CCXT core and catalog

**Scope:** combine dependency/support work with actual core coding. This is not
another documentation-only planning phase.

- Apply section 5.2's feature gating to every relevant dependency **before the
  first Cargo compilation**. Select exact stock pins/features, preserve the
  lockfile and record the resolved engine version.
- Include all six current providers in the compiled feature set and construction
  map. Verify actual stock method/product dispatch rather than inferring support
  from a feature or one wrapper failure. Record unsupported operations and any
  genuine provider-level blocker; do not quietly omit a venue to make a build fit.
- Record a compact support ledger for REST, live channels, products/depths,
  statistics and usable URL overrides. Distinguish confirmed absence, frontend
  differences and runtime correctness blockers. Include the approved CLI/history
  removals; do not turn this into exhaustive per-market live qualification.
- Implement stock provider construction, metadata loading/sharing, market
  resolution and owned output conversion. Reuse small Ferris contracts and keep
  public IDs distinct from CCXT IDs. Exercise a real catalog path, not mock echoes.
- Establish the safe owner/command boundary needed by later phases using the
  recorded URL/global-state constraints. Investigate evidence that would make
  the chosen single-process design unusable; full live lifecycle implementation
  and mixed-channel exercises belong to Phase 3.
- Keep unmigrated production paths as the temporary branch baseline. Do not
  delete their dependencies prematurely or add stub implementations of future
  methods to pretend those phases are complete. No permanent fallback is allowed.

**Focused check:** inspect the resolved features without compiling everything,
then run the newly implemented provider/catalog/conversion path with the gated
build. Record real provider availability and any external source limitation.

**Exit:** working core/catalog code and traceable six-venue support/feature mapping;
unambiguous market resolution and no fake source data. Not all REST methods,
watchers or statistics sources need to be migrated in this phase.

**Stop:** set Phase 1 to `awaiting_review`; request approval. Do not start Phase 2.

### Phase 2 — REST snapshots

**Prerequisite:** user acceptance of Phase 1 and authorization for Phase 2.

- Route supported `fetchMarkets`, `fetchTrades`, `fetchOHLCV` and `fetchOrderBook`
  operations through the new core and existing Ferris HTTP boundary.
- Keep straightforward request defaults, ordering/time filters, response caching
  and error envelopes. Record necessary frontend differences; reject unavailable
  operations/options explicitly instead of returning invented successful data.
- Replace native metadata acquisition shared with statistics as its consumers
  migrate; identify any remaining temporary dependency for Phase 4.
- Remove the extra Hyperliquid historical collector/cache and its configuration.
  Delete superseded REST acquisition and callers when no later native path still
  uses them; defer remaining shared-code removal to its owning phase.

**Focused check:** exercise actual migrated HTTP snapshot paths and a supported
error/rejection case. Do not run the final full-backend/release campaign here.

**Exit:** implemented snapshot paths use CCXT and return useful real data through
the Ferris API; coverage and frontend differences are documented. No native REST
fallback is used for an operation claimed to be migrated.

**Stop:** set Phase 2 to `awaiting_review`; request approval. Do not start Phase 3.

### Phase 3 — Realtime streaming

**Prerequisite:** user acceptance of Phase 2 and authorization for Phase 3.

**Accepted temporary risk — stock order-book issues across exchanges:** the latest
user instruction extends this exception beyond Binance. Proceed despite known
stock order-book defects and document them honestly. The demonstrated CCXT Pro
`4.5.85` Binance USD-M failure remains concrete evidence of this risk:
after a valid initial bridge, CCXT can accept an overlapping
delta whose `pu` does not match the previous `u`, without rejecting or reseeding
the book. A user could consequently see incorrect prices or liquidity while
updates still appear live; frequency in real traffic has not been established.

For known stock order-book issues across exchanges, **document and proceed; do
not block Phase 3 or wait for an upstream fix**. This supersedes the earlier
Binance-only scope and correctness-blocking rules for these accepted book risks.
Keep recorded failing evidence; do not call it a pass or claim guaranteed book
synchronization. No dependency patch, custom synchronizer, native fallback, or
reduced Binance book depth is authorized. Other requirements—including safe
ownership, warm-feed isolation, reconnect, unsubscribe, incremental delivery and
focused verification—remain in force. Carry each observed book limitation into
client-facing docs when the CCXT realtime path is enabled. Phase completion still
requires user review.

- Replace `Real*UpstreamRunner`/native `Upstream*Stream` with the qualified owner
  service and shared topic lifecycle from section 4.
- Implement warm attachment, initialization, reconnect, final unsubscribe,
  cancellation and shutdown without freezing unrelated maintained feeds.
- Deliver supported trade/book/candle channels through the existing WS boundary.
  Preserve book synchronization/retention and incremental trade delivery;
  document changed provider provenance/formatting for frontend adaptation.
- Rewire forwarders, avoid full-depth copies solely for top-N rendering, and
  remove native parsers/synchronizers once their remaining consumers are migrated.

**Focused check:** observe warm A while adding/removing/re-adding cold B, a
reconnect, mixed channels sharing a URL, and final delivery after source silence.
Use deterministic input for synchronization/retention cases live data cannot prove.

**Exit:** actual CCXT feeds reach clients with owned state and bounded handling;
correctness is qualified subject to the explicit stock order-book-risk exception
above, including Binance's demonstrated continuity failure. Known failures remain
documented; a compiled watch call or its first returned object is not sufficient.

**Stop:** set Phase 3 to `awaiting_review`; request approval. Do not start Phase 4.

### Phase 4 — Market statistics and new metrics

**Prerequisite:** user acceptance of Phase 3 and authorization for Phase 4.

- Replace retained native statistics acquisition with shared stock calls, keeping
  necessary value/identity/provenance mapping and truthful receipt/freshness state.
- Implement the approved numeric volume/open-interest variants, strict decoding
  and real acquisition. Preserve familiar funding/price shapes where practical;
  document source precision or provenance differences rather than emulate native
  clients to satisfy an old frontend expectation.
- Update normalization, product/field support, expiry/merge logic and no-I/O
  capabilities together. Share demand when singular stock methods require it.
- Deliver the new metrics via both HTTP snapshots and WS snapshot/delta paths.
  Remove obsolete statistics transport and now-unused catalog acquisition.

**Focused check:** exercise statistics HTTP and WS snapshot/delta delivery,
stale/recovery behavior, coverage/removal handling, and numeric zero/null boundaries.

**Exit:** statistics are acquired and delivered, not just listed in capabilities;
new fields have verified units, meaningful state and working freshness/deltas.

**Stop:** set Phase 4 to `awaiting_review`; request approval. Do not start Phase 5.

### Phase 5 — Final integration, removal and verification

**Prerequisite:** user acceptance of Phase 4 and authorization for Phase 5.

- Complete `main`, `AppState`, registry/exports, configuration and full shutdown.
- Finish section 5's removal ledger: native exchange transports, synchronizers,
  parser helpers, both binaries, obsolete tests/imports and unused dependencies.
  No native fallback or placeholder implementation remains in the target backend.
- Perform broad integrated verification from section 9: the relevant backend
  suite, actual server HTTP/WS scenarios and a locked release/container build.
  Verify the six-provider feature set remains intact without enabling all venues.
  Earlier focused checks do not substitute for this integrated pass.
- Update README/integration support tables, frontend differences, metric units,
  reduced history, retired tools and changed configuration. Update
  [ROADMAP.md](ROADMAP.md) to the actual status. Do not recreate deleted documents.
- Prepare the single production cutover. Publishing/deploying is a separate user
  decision; Phase 5 review is not permission to deploy. Rollback restores the
  previous release, not an embedded native-provider fallback.

**Exit:** a runnable, integrated CCXT-backed backend with the intended main data
flows working, known support/frontend differences recorded and correctness blockers
resolved. Exact compatibility with every old frontend component is not required.

**Stop:** set Phase 5 to `awaiting_review`; only user acceptance marks it
`completed` and the five-phase implementation accepted.

## 9. Focused verification, not another research project

For this documentation task: source review and document consistency only; no
backend build, live experiment or old benchmark recreation is required.

Phases 1–4 run only the focused checks for their changed paths, with builds batched
after meaningful implementation rather than after each edit. Phase 5 runs the
broad integrated pass. Do not postpone all runtime evidence until Phase 5, and do
not run the entire suite/release build at every earlier review gate. Apply section
5.2 before any compilation; no all-exchange build is part of this plan.

The integrated evidence should cover:

- **API smoke:** exercise actual HTTP and WS paths for each implemented
  provider/family, including unavailable requests. Check useful, correct output,
  tuple/envelope shapes and errors; record differences in raw `info`, symbol
  formatting or provider provenance instead of demanding exact legacy parity.
- **Owner/lifecycle smoke:** warm book A, add cold B, remove B, re-add it, then
  disconnect/reconnect. Observe A throughout. Include a finite burst followed by
  silence and mixed channels at a shared URL. Source silence is not processing
  delay; seed return is not trusted readiness.
- **Book correctness where retained:** delta bridge/continuity, gap/reseed and
  backing-depth retention; correct public timestamp/nonce meanings. Use focused
  deterministic input where a live feed cannot prove these invariants.
- **Statistics smoke:** HTTP snapshot and WS snapshot/delta, a stale/recovery
  transition, catalog removal/coverage handling, and new numeric metrics with
  zero/one-null-member values. Include decoding the two distinct new value shapes.
- **Phase 5 integrated checks:** run the relevant backend test suite, launch the
  actual target server and perform the locked release/container verification.
  Tests alone do not prove runtime ownership. Keep consumer-visible tests from
  `tests/realtime_ws.rs`, `tests/market_stats.rs` and `tests/market_stats/` where
  applicable; adapt coverage/accepted differences and delete removed-tool or
  incidental implementation tests rather than re-pin old quirks.

Do not port the entire native parser test inventory, pin incidental wording, or
create a new benchmark framework before implementing the replacement. The notes
already preserve the historical experiments. Add a focused regression only for a
real uncertain boundary or reproduced consumer-visible bug.

## 10. Stop-and-ask boundaries and completion checklist

Ask with the affected provider/product/field, evidence and alternatives when a
current venue cannot be supported by stock CCXT, runtime safety needs a forbidden
topology/patch, market identity or units cannot be established, or all-market
statistics cannot meet the intended freshness/quota contract. Document ordinary
frontend differences and continue the authorized phase. Source-answerable unknowns
are implementation work; user phase acceptance is a separate mandatory gate.

Each phase is reviewed only against its own exit criteria. The following is the
**Phase 5 / whole-rewrite checklist**, not a demand to finish the entire backend
during Phase 1:

- [ ] All five phases have user acceptance recorded in section 8; an agent's
      `awaiting_review` status is not approval.
- [x] Exact stock pins/features and all six public venues (`binance`, `bybit`,
      `hyperliquid`, `lighterxyz`, `aster`, `extended`) are mapped; `lighterxyz`
      selects CCXT `lighter`. Defaults are disabled on every relevant dependency,
      with no accidental `all` or missing runtime factory provider.
- [x] Confirmed support losses, additions, CLI retirement and reduced Hyperliquid
      history are explicit; unknown/broken implementations were not disguised as
      unsupported coverage.
- [x] All exposed acquisition uses stock CCXT/Pro; no native fallback, parser,
      synchronizer, fork, unsafe ownership workaround or worker-process service.
- [x] Main API/WS data flows work with truthful identities, units, precision and
      provenance; known frontend differences are listed for case-by-case adaptation.
- [x] New numeric volume/open-interest schemas are acquired and delivered, with
      documented CCXT units, strict decoding and working freshness/deltas.
- [x] Warm-feed isolation, lifecycle cleanup and final delivery have actual evidence.
      Stock synchronization/retention failures remain under the accepted risk
      exception, not a correctness pass; see the issue register and Phase 5 limits.
- [x] Old callers, native modules, tools, dependencies and unusable settings are
      removed; kept deployment overrides work through stock configuration.
- [x] Client docs and container/entrypoint wiring describe the implementation that
      actually ships, and the final backend has been exercised.

**Resume action:** follow the current section 8 tracker and the latest explicit
user authorization. Do not restart completed phases, recreate deleted research
artifacts, mark a review submission accepted, or deploy without user approval.
