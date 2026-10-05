# CCXT known issues and follow-up checks

Updated 2026-10-03. Historical upstream observations retain their original phase
and date. Phase 5 integration and shutdown evidence is identified below; known
stock order-book failures were not rerun or reclassified as passing.

**Baseline:** stock `ccxt`, `ccxt-pro`, and resolved `ccxt-base` `4.5.85`; see
[Cargo.toml](Cargo.toml) and [Cargo.lock](Cargo.lock). This is the pinned version,
not a claim about the newest available release. Upstream source references below
are relative to these crate versions, not to Ferris's `src/` directory.

**Authority:** [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md#phase-tracker) owns phase
approval. Phase 3 is `completed` with user approval recorded on 2026-10-03.
Phase 4 is `completed` with user approval recorded on 2026-10-03; Phase 5 is `awaiting_review` after integrated tests and locked release/container smoke. The user separately authorized production deployment for frontend testing, completed that day; explicit Phase 5 acceptance remains pending. The user
accepted known stock order-book issues across exchanges and Extended's known API
failure as temporary migration non-blockers. Acceptance does not establish book
correctness, mean that every venue has Binance's defect, or excuse unrelated
ownership, lifecycle, or trade/candle delivery failures. No dependency patch,
copied synchronizer, native fallback, or new implementation is authorized here.

## Issue index

| ID | Issue | Current status |
| --- | --- | --- |
| [CCXT-001](#ccxt-001) | Binance accepts a broken post-bridge `pu` link | Confirmed upstream defect; accepted risk |
| [CCXT-002](#ccxt-002) | Binance publishes its REST seed before synchronization | Upstream defect; observable seed publication gated in Ferris |
| [CCXT-003](#ccxt-003) | Deep-book retention and sequence edge cases | Historical retention failure; exact-pin qualification incomplete |
| [CCXT-004](#ccxt-004) | Extended rejects requests without User-Agent | HTTP fixed in Ferris; stock WebSocket headers remain ineffective |
| [CCXT-005](#ccxt-005) | Stock unsubscribe, acknowledgement, and socket-lifetime quirks | Mitigated in Ferris; preserve lifecycle regression coverage |
| [CCXT-006](#ccxt-006) | Stock storage lifetime and runtime soundness | Source-level concerns / unqualified; not a demonstrated leak or soundness pass |
| [CCXT-007](#ccxt-007) | Production capacity, aggregate quotas, and latency | Unqualified beyond focused admission/backpressure checks |
| [CCXT-008](#ccxt-008) | Binance live-worker stack overflow | Fixed in Ferris; upgrade-sensitive |
| [CCXT-009](#ccxt-009) | Bybit trade subscriptions acknowledge but do not deliver | Fixed in Ferris; result-hash regression retained |
| [CCXT-010](#ccxt-010) | Extended trades/candles fail to route or mix price caches | Fixed in Ferris; independent-stream regression retained |
| [CCXT-011](#ccxt-011) | SIGTERM waits for stalled HTTP before cancelling owners | Fixed in Ferris during Phase 5; executable regression retained |
| [CCXT-012](#ccxt-012) | Binance default trade stream includes zero-price/zero-size rows | Observed source payload; frontend price-print caveat, no filtering applied |

“Confirmed” means observed failing output, “historical” identifies older evidence,
“source-level” is not a runtime reproduction, and “unqualified” is not a claim of
failure. “Mitigated” does not mean the upstream behavior was fixed.

## Open risks and mitigated upstream behavior

### CCXT-001

**Binance post-bridge continuity — confirmed, accepted upstream defect.**

- **Impact / scope:** the local stock USD-M futures fixture published incorrect
  liquidity after an invalid sequence link. A rising nonce and continuing updates
  can therefore look healthy while the book is wrong. The stock futures branch
  is implicated; other products/venues were not independently shown to fail this
  exact sequence.
- **Recorded reproduction:** actual `BinanceCore::watch_order_book`, local HTTP
  and WebSocket sources, one current-thread runtime, watch/seed depth 1,000,
  stock rate limiting and `watchOrderBook.checksum = true`. No Ferris book parser
  or runtime patch was involved.

  | Step | Input | Observed stock output |
  | --- | --- | --- |
  | REST seed | `lastUpdateId=100` | Bootstrap state |
  | Valid initial bridge | `U=95, u=101, pu=94`, bid `[100, 2]` | Nonce `101`, bid `[100, 2]` |
  | Broken subsequent link | `U=100, u=103, pu=102`, bid `[100, 9]` | Nonce `103`, bid `[100, 9]`; no rejection or reseed |

  The previous final ID is `101`, so the last row's `pu=102` is invalid under
  [Binance's USD-M sequence contract](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
  Preserved output:

  ```text
  CONTINUITY_RESULT accepted broken pu: previous_u=101 incoming_U=100 incoming_u=103 incoming_pu=102; owned_nonce=Some(103) owned_bids=[[100.0, 9.0]]
  ```

- **Cause / source:** `ccxt-pro/src/pro/binance.rs:1751–1757` uses
  `U <= nonce || pu == nonce` beyond the initial bridge. The overlap condition
  bypasses the required post-bridge continuity link even with checksum checking
  enabled. Stock output exposes the final nonce, not incoming `U`/`pu` or a
  verified synchronization state.
- **Current mitigation:** none for this continuity failure. Independent owners,
  fixed acquisition depth, and the seed gate in
  [`live.rs::run_session`](src/exchanges/ccxt/live.rs) solve different problems.
  Skipping a first result or checking that nonce increases is not a repair.
- **Next action / closure:** when evaluating an upstream fix, reconstruct the
  local fixture and replay this exact valid-then-invalid sequence. The invalid
  `[100, 9]` book must not be delivered as maintained live state; recovery must
  establish a fresh valid snapshot/delta bridge. Also exercise ordinary valid
  continuity and reconnect through Ferris before changing this status.
- **Evidence:** [accepted-risk handoff](FERRIS_V2_PLAN.md#phase-3-handoff--accepted-continuity-risk--2026-10-02).
  `ccxt_phase3_continuity_smoke` was a temporary executable, not a retained test
  target. The sequence and output above are the durable reproduction record.

### CCXT-002

**Binance premature seed publication — upstream issue, limited Ferris gate.**

- **Evidence / cause:** earlier local research returned a structurally valid book
  before any bridging delta. In `4.5.85`,
  `ccxt-pro/src/pro/binance.rs::fetch_order_book_snapshot` unconditionally resolves
  the book at line 1634 after processing whatever buffered messages are present.
  A REST snapshot alone is not synchronized diff-book readiness.
- **Current mitigation:** [`live.rs::run_session`](src/exchanges/ccxt/live.rs)
  withholds Binance books whose timestamp is absent, excluding the observed
  timestamp-less seed. This is an observable gate, not a synchronization oracle;
  a timestamp-bearing result still does not prove continuity or retention.
- **Next action / closure:** hold back the bridging delta after a REST seed and
  verify that no seed is delivered to a viewer as live state. Then test valid,
  nonbridging, and invalid subsequent deltas. Remove or replace the timestamp
  gate only with stock-backed readiness evidence; closing this item must not
  imply that [CCXT-001](#ccxt-001) is fixed.
- **Evidence:** [historical venue findings](CCXT_MIGRATION_NOTES.md#venue-specific-notes)
  and [Phase 3 delivery behavior](FERRIS_V2_PLAN.md#phase-3-handoff--stock-realtime-cutover--2026-10-02).

### CCXT-003

**Deep-book retention and sequence boundaries — historical failure, incomplete
exact-pin qualification.**

- **Evidence boundary:** a `4.5.84` fixture demonstrated that stock `limit()` could
  remove known backing levels; later removals exposed a different book. The
  `4.5.85` ownership/topology experiments did not qualify or resolve retention.
  Do not describe that historical failure as a new `4.5.85` reproduction.
- **Current mitigation:** [`stream.rs::prepare_live`](src/exchanges/ccxt/stream.rs)
  fixes Binance backing acquisition at 1,000 levels for contracts and 5,000 for
  spot. Display depth is outside upstream identity; Ferris serializes borrowed
  top-N views. This prevents a shallow viewer from shrinking the acquisition
  profile, but does not prove stock preserves all learned deep levels.
- **Next action / closure:** seed known levels beyond a shallow display boundary,
  add/update deep levels, remove better prices, and compare the resulting full
  retained book with the expected levels learned from snapshot/deltas. Attach
  and remove shallow/deep viewers without changing acquisition. Do not require
  liquidity that was never received, and do not substitute a shallow partial
  stream for the deep diff-book case.
- **Related unqualified cases:** exact-pin gap/overlap, old or duplicate events,
  reseeding, malformed data, and precision boundaries remain separate from
  “updates arrived.” Earlier research found publication/timestamp differences;
  those are not all active Ferris bugs. Lighter's complete offset-gap recovery
  remains unqualified; its stock book nonce is `order_book.offset`, not the
  separate native field named `nonce`. Qualify against each venue's own rules,
  not Binance's `pu` contract.
- **Pointers:** [`live.rs`](src/exchanges/ccxt/live.rs),
  [`stream.rs`](src/exchanges/ccxt/stream.rs),
  [`convert.rs`](src/exchanges/ccxt/convert.rs),
  [historical findings](CCXT_MIGRATION_NOTES.md#venue-specific-notes).
  The shared-view regression in [realtime_ws.rs](tests/realtime_ws.rs) protects
  Ferris fanout behavior; it is not a Binance retention/sequence certification.

### CCXT-004

**Extended missing User-Agent — HTTP fixed; WebSocket transport still blocked.**

- **Original observation:** stock `/api/v1/info/assets` returned HTTP 403 during
  migration. This was accepted as an availability limitation, but subsequent
  request comparison isolated a header difference rather than a general outage.
- **Header evidence (2026-10-03):** same-host curl without User-Agent returned
  HTTP 403 HTML from `awselb/2.0`; changing only User-Agent to
  `ferris-diagnostic/1.0` returned HTTP 200 JSON with 407 assets.
- **HTTP fix:** Extended REST and Pro cores set `exchange.userAgent` to
  `Ferris/1.0` before the HTTP client is initialized. Rust `4.5.85` reads that
  public field but ignores `userAgent` in constructor JSON. No dependency patch,
  native transport, or fallback was added.
- **Verified through an isolated patched Ferris server:** real Extended catalog
  returned 326 active perpetuals; BTC selected statistics returned all six
  requested fields available with complete coverage and no source failures.
  REST BTC books returned 20 bids/20 asks, trades returned five rows, and
  one-minute candles returned four rows, all HTTP 200. Catalog BTC identity was
  `["extended","perp",null,null,"BTC-USD"]`; stock book/trade symbol was
  `BTC/USDC:USDC`. `cargo test --locked extended` passed five tests and the
  backend build passed. The optimized Docker release was subsequently deployed;
  public production catalog (326 active perpetuals), complete BTC statistics,
  20-level REST books, and Hyperliquid WSS passed. The previous executable was
  retained for rollback; see [deployment commands](README.md#hosted-lattice-deployment--2026-10-03).
- **Remaining WebSocket failure:** Ferris acknowledged a BTC book subscription,
  then reported HTTP 403 connecting to
  `wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/BTC-USD`.
  A direct HTTP upgrade to the same endpoint with `User-Agent: Ferris/1.0`
  returned 101 and an actual book snapshot. Stock Extended declares
  `options.ws.options.headers`, but `ccxt-base/src/pro/ws_client.rs::ensure_client`
  calls `connect_async(url)` without headers; its proxy path also passes only
  the URL to `client_async_tls`. Setting the HTTP field does not fix this.
- **Closure:** an upstream WebSocket header-support fix or separately approved
  dependency change must be followed by real Ferris stream delivery checks.
  Do not label the successful direct handshake as a Ferris WebSocket recovery.
- **Pointers:** [`venue.rs`](src/exchanges/ccxt/venue.rs),
  [`stream.rs`](src/exchanges/ccxt/stream.rs), and the existing Extended fixtures
  in [realtime_ws.rs](tests/realtime_ws.rs) and
  [market_stats/extended.rs](tests/market_stats/extended.rs).

### CCXT-005

**Stock unsubscribe and socket lifetime — mitigated, not a reason to suppress
unrelated errors.**

- **Evidence:** earlier Hyperliquid research showed a 350-ms awaited unsubscribe
  acknowledgement withholding output despite 17 arriving BTC versions; restart
  exposed `[UnsubscribeError]`. Phase 3 deliberate disconnect/control fixtures
  still emit caught stock `[NetworkError]` / `[UnsubscribeError]` panic-hook
  messages. The actual Bybit unsubscribe/re-add smoke also logged a caught
  `[UnsubscribeError]` while feeds recovered.
- **Source-only distinctions:** Aster's
  `un_watch_order_book_for_symbols` waits on unsubscribe hashes, but
  `ccxt-pro/src/pro/aster.rs:2872–2893` dispatches event messages only, with no
  acknowledgement branch. Extended has no stock unwatch override and uses
  per-channel URLs. `ccxt-base/src/pro/ws_client.rs::drop_client` removes the
  registry entry; that call alone does not prove socket closure while connection
  tasks still own the client.
- **Current mitigation:** one coherent owner drives data and control hashes;
  unwatch registration does not serially await a quiet channel. Retiring feeds
  get a 10-second deadline, targeted settlement/cache cleanup, and epoch
  invalidation. Late acknowledgements trigger stock subscription reconciliation.
  Final URL retirement/reconnect tears down and joins its runtime/thread before
  reuse. Extended retires its dedicated URL instead of invoking absent unwatch.
  Only the recognized unsubscribe result is treated as control; other upstream
  failures enter recovery.
- **Next action / closure:** preserve warm A while cold B is added, removed,
  re-added, acknowledged late, and left silent. Observe B's final retirement,
  A's final event after source silence, and no stale-epoch publications. Extend
  venue-specific fixtures when stock control behavior changes. The local expiry
  deadline is not proof of remote acknowledgement at every venue; do not remove
  guards merely because the happy-path watch succeeds.
- **Pointers / current proof:** [`live.rs::run_session`](src/exchanges/ccxt/live.rs),
  [`stream.rs::expire_unsubscribe`](src/exchanges/ccxt/stream.rs),
  [lifecycle regressions](tests/realtime_ws.rs), and the
  [Phase 3 handoff](FERRIS_V2_PLAN.md#phase-3-handoff--stock-realtime-cutover--2026-10-02).
  Historical unresolved-lifecycle statements in the migration notes predate
  these implemented mitigations and passing focused checks.

## Qualification gaps, not additional accepted defects

### CCXT-006

**Stock storage lifetime and runtime soundness — source concerns, no long-run
qualification.**

- **Evidence boundary:** stock cache/book marker values refer to separate mutable
  storage (`ccxt-base/src/pro/cache.rs::new_marker` calls `alloc_cache_id`;
  `pro/order_book.rs::new_side` calls `make_side_marker`). Earlier source review
  identified `SIDE_STORE` and `coerce_value_to_mut` concerns. A process-global
  URL registry and thread-local deferred queue are established ownership
  constraints, not proof that every storage/lifetime path is sound.
- **Current safeguards:** stock cores and values stay inside dedicated owner
  threads; only owned Ferris DTOs cross the boundary. Feeds clear retained stock
  payloads and reconnect replaces the entire live thread/runtime. Live admission
  is bounded. These measures do not prove allocator entries are reclaimed,
  bound cumulative pool growth under churn, or certify dependency soundness.
- **Next action / closure:** inspect the pinned storage/aliasing/lifetime paths
  and run a long subscription/unsubscribe/reconnect soak with fixed and rotating
  identities. Record active owners/sockets, memory and retained storage where
  observable, both during churn and after returning to baseline demand. Separate
  allocator high-water behavior from continuing growth. RSS stability alone is
  not a soundness audit; no leak, UB, or safety pass is asserted here.
- **Pointers:** [`live.rs`](src/exchanges/ccxt/live.rs),
  [`stream.rs::clear_feed` / `clear_retained`](src/exchanges/ccxt/stream.rs),
  `ccxt-base/src/{value,runtime,exchange_stubs}.rs`, and
  [historical ownership findings](CCXT_MIGRATION_NOTES.md#ownership-facts-the-next-design-must-respect).
  A required patch, unsafe workaround, or process-isolation redesign still needs
  the plan's explicit decision gate; the accepted book-risk exception does not
  authorize it.

### CCXT-007

**Capacity, combined exchange quotas, and latency — focused checks are not a
production capacity result.**

- **Current bounds:** 128 live URL owners per service, 200 feeds per shared URL,
  512 publications per feed, 200 realtime subscriptions per client, and 256
  outgoing frames per client. Statistics retain a separate 16-subscription
  client limit. Binance uses real stock URL slots and 8-MiB live-worker stacks;
  other live workers reserve 2 MiB. See
  [delivery/lifecycle contract](README.md#delivery-and-lifecycle).
- **Evidence:** actual-backend checks rejected a 201st realtime subscription,
  preserved duplicate/unsubscribe semantics, and disconnected a stalled client
  while a fast client received 600 fixture books plus a final 601st update.
  These are admission and isolation checks, not sustained-load measurements.
- **Unqualified:** historical sizing assumptions were roughly five replicas,
  400 users per replica, up to 50 markets per user, and changes every 5–6 minutes.
  That is up to 20,000 client-market relationships per replica before channel
  multiplication, not necessarily 20,000 distinct upstream feeds. Deduplication,
  thread/stack cost, CPU, memory, egress, reconnect storms, and combined IP quotas
  have not been qualified at that scale. Per-core stock rate limiting does not
  create an independent venue/IP budget for each owner or replica.
- **Next action / closure:** use representative release-mode high/low-overlap
  workloads and subscription/reconnect churn; measure owner/socket count,
  memory, CPU, queue age, serialization/egress, request rate, and rate-limit
  responses. Include metadata, snapshots, and statistics demand sharing the same
  egress IP. Compare with current venue limits before raising admission caps.
  Record event-paired ingress-to-client latency and maximum silence, separately
  from cold initialization and source cadence; do not subtract unrelated p99s.
- **Cadence caveat:** earlier public Hyperliquid checks observed about 5.4 seconds
  between inputs even on a direct socket. That is historical source evidence,
  not a current universal cadence or proof of CCXT delay/throttling. Qualifying
  freshness requires a suitable source and identifiable input/output events.
- **Pointers:** [`live.rs`](src/exchanges/ccxt/live.rs),
  [`web.rs`](src/web.rs), [historical capacity assumptions](CCXT_MIGRATION_NOTES.md#boundaries-still-unresolved-before-production-replacement),
  and [Phase 3 verification limits](FERRIS_V2_PLAN.md#phase-3-handoff--stock-realtime-cutover--2026-10-02).
  No latency SLA, global-IP quota pass, release/container campaign, or full
  statistics integration result was claimed by Phase 3.

## Fixed Ferris regressions to preserve

These were fixed during the identified implementation phases. They are not
unresolved upstream risks and must not be excused by the order-book exception.

### CCXT-008

**Binance live-worker stack overflow — fixed.** The actual dev backend overflowed
its 2-MiB live-worker stack while stock dynamic watch/registration/seed dispatch
nested. [`live.rs::run_url`](src/exchanges/ccxt/live.rs) now reserves 8 MiB for
Binance. The loopback test
`binance_trade_registration_fits_the_live_worker_stack` in
[venue.rs](src/exchanges/ccxt/venue.rs) drives the production owner and receives
consecutive trade IDs; the rebuilt backend also delivered Binance live streams.
Keep this guard when changing pins/build profiles. Removing the stack override
requires measured stack needs across relevant paths, not a passing constructor.
The trade test alone does not qualify every product's worst-case stack usage.

### CCXT-009

**Bybit acknowledged trades without delivered updates — fixed.** Ferris awaited a
nonmatching stock result hash. [`stream.rs::message_hash`](src/exchanges/ccxt/stream.rs)
now uses Bybit's `trade:{symbol}` hash. The test
`bybit_trade_results_reach_the_owner_without_cache_replay` in
[venue.rs](src/exchanges/ccxt/venue.rs) receives IDs `101` then `102` as distinct
updates and observes no quiet-period replay. The final real `/v1/ws` smoke also
received Bybit trades. Preserve actual delivery and incremental-cache checks
when upstream hash/dispatch behavior changes; acknowledgement alone missed this
bug.

### CCXT-010

**Extended trade/candle dispatch and price-cache identity — fixed.** A local
stock-protocol fixture initially produced books only despite four subscription
acknowledgements. Ferris now follows stock `trades:{symbol}` and
`ohlcv:{symbol}:{timeframe}:{candleType}` result hashes, and selects the candle
cache by both timeframe and candle type. Missing volume remains `null`, not
invented zero. See [`stream.rs::message_hash` / `LiveSpec::candle_key`](src/exchanges/ccxt/stream.rs)
and [`convert.rs`](src/exchanges/ccxt/convert.rs).

`extended_delivers_trades_and_separate_price_candle_caches` in
[realtime_ws.rs](tests/realtime_ws.rs) verifies successive trade IDs, book changes,
independent trade/mark candle updates, null mark volume, silence without replay,
and final socket retirement. The actual backend passed the corresponding local
fixture smoke. Preserve those independent content assertions; they do not close
Extended's remaining public WebSocket header issue [CCXT-004](#ccxt-004).

### CCXT-011

**SIGTERM blocked behind pending acquisition — fixed in Phase 5.** A running
backend with a stock `/info` request held by a local fixture and a 60-second
request timeout did not exit within three seconds after SIGTERM. `main` awaited
Axum's graceful HTTP drain before cancelling the owner supplying that response.
Upgraded WebSockets also lacked an explicit application shutdown/join boundary.

- **Fix:** stop accepting, cancel pending WebSocket commands, join their
  forwarders/writers, stop statistics, cancel/join live and catalog owners, then
  finish the HTTP drain. A live shutdown error no longer skips catalog shutdown
  or the remaining live-supervisor joins. Bind failures happen before owners start.
- **Regression:** `tests/shutdown.rs::sigterm_cancels_pending_acquisition_and_closes_websockets`
  launches the actual backend, holds stock HTTP indefinitely, keeps idle and
  subscribing WebSockets open, sends SIGTERM, and requires client closure, an
  HTTP failure response, and successful process exit within five seconds.
- **Observed:** the before/after throwaway scenario now exits successfully inside
  its three-second bound. With all six venues' mixed realtime/statistics clients
  active, the integrated debug backend exited 0 in 0.206 seconds and the Extended
  fixture had no remaining source sockets. This is a shutdown observation, not a
  latency SLA or a long-duration owner-storage qualification.

## Compatibility limits and remaining coverage

These are documented stock contracts, not bugs to “fix” with silent fallback:

| Area | Current contract / follow-up boundary |
| --- | --- |
| Bybit deep live books | Maximum 1,000 levels, options 100. The former 10,000-level live full-book path is gone; REST depth does not imply WS support. |
| Aster deep live books | Stock partial-depth 20 only in Ferris; the former deep diff path is gone. Revisit only if stock coverage changes. |
| Candles | Lighter realtime candles are unsupported; REST candles are available. Bybit option candles are unsupported on REST and WS. Hyperliquid candles are enabled. Missing source volume is nullable. |
| Extended identity/liquidity | Live symbols follow stock `BASE/USDC:USDC`; standard books are indicative, not RFQ liquidity. |
| Timestamp/nonce provenance | Binance event time is stock `E`; Bybit book nonce is `u`; Lighter is `offset`; Extended WS uses `ts`/`seq`. Do not carry native or REST sequence assumptions into WS consumers. |

The full limits/parameter contract is in [README.md](README.md#ccxt-realtime-contract)
and [frontend guidance](INTEGRATION_README.md#topic-rules). Focused public runtime
coverage was BTC perpetual trades/books/candles on Binance, Bybit, Hyperliquid,
and Aster, plus Lighter trades/books. Extended was fixture-only. Not every spot,
option, expiry, settlement, aggregation, RPI, or candle price-source combination
was exercised. Maintain a source/product matrix when expanding qualification;
registration and one successful default market do not certify that matrix.

### CCXT-012

**Binance zero-price/zero-size trade rows — production source observation.**
Public WSS verification on 2026-10-03 observed the default stock `trade` stream
deliver trade ID `8143292052`, timestamp `1791011752239`, with raw
`{"e":"trade","s":"BTCUSDT","p":"0","q":"0","X":"NA","st":1}`.
Ferris delivered `price:0`, `amount:0`, `cost:0` and preserved that raw `info`.
The deployment smoke's positive-price assertion caught this; it is not a fabricated
Ferris price or an order-book exception. The meaning of `st`/`NA` is not qualified.

- **Impact:** clients must not use these rows as ordinary executable-price prints.
  No parser patch, silent filtering, default change or fallback was added during
  deployment; the stock-shaped response remains source-faithful.
- **Explicit alternative:** the already-supported `params:{"name":"aggTrade"}`
  public subscription delivered 18 positive-price/positive-size rows and no zero
  rows in a 15-second observation. Aggregation semantics differ; that observation
  is not proof of all future stream contents.
- **Closure:** establish the exchange's exact event semantics and stock handling
  before changing output policy. Preserve raw evidence and qualify both stream
  choices rather than silently rewriting a source event or declaring it a heartbeat.

## Recheck and closure protocol

The existing focused commands are:

```sh
cargo test --locked --lib exchanges::ccxt::venue::tests -- --nocapture
cargo test --locked --test realtime_ws -- --nocapture
cargo test --locked --lib web::tests -- --nocapture
```

The Phase 3 handoff records **2 + 5 + 3 passing checks**, plus the actual-backend
smokes. These commands were not rerun for this documentation update, and the
suite does not contain the temporary Binance invalid-`pu` reproduction. Use each
issue's specified scenario as well as the relevant regressions; all-green generic
tests cannot close an untested upstream defect.

For any future issue update, retain:

1. Exact crate pins/features and date, affected venue/product/channel, endpoint
   or fixture identity, and relevant upstream/Ferris symbols.
2. Minimal input sequence, expected result, observed result, and whether evidence
   is source-only, local stock-wire runtime, or public upstream runtime.
3. Current mitigation and its limits; distinguish fixed Ferris behavior from an
   upstream fix and a user-accepted risk.
4. A linked upstream ticket/fixed release when available, plus the command and
   observed closure result. No upstream ticket ID was recorded in the Phase 3
   handoff; do not invent one or assume an untested newer release fixes it.

Keep the failing evidence when a pin changes. Update this register and the plan's
handoff when a closure is actually demonstrated; do not turn documentation or a
known non-blocker into implicit phase acceptance.
