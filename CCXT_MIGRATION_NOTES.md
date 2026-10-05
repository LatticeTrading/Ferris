# CCXT migration — findings and handoff

**For the agent writing the migration plan. This is not that plan.**

**Implementation successor:** [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md). It records the
user's later coverage decisions, replacement/deletion map and implementation
sequence. Its decisions supersede this handoff's suggestions about retaining
native exceptions or preserving unsupported features; the technical findings
below remain evidence, not a completed migration.

**Current issue status:** [CCXT_KNOWN_ISSUES.md](CCXT_KNOWN_ISSUES.md) separates open
upstream risks from implemented Phase 3 mitigations and fixed Ferris regressions,
with reproduction evidence and closure checks. The historical unresolved-lifecycle
statements and native source paths below predate that cutover; they are not a
current implementation inventory.

## Phase 4 statistics qualification — 2026-10-03

The implementation/support contract now lives in [README.md](README.md#market-statistics-and-capabilities),
its [numeric unit table](README.md#numeric-statistics-units), and the Phase 4 handoff
in [FERRIS_V2_PLAN.md](FERRIS_V2_PLAN.md). Historical native paths below are references,
not retained fallback transports.

- Exact stock `4.5.85`: Binance `fetchTickers`, `fetchFundingRates`,
  `fetchFundingIntervals`, selected `fetchOpenInterest`; options additionally use
  `fetchMarkPrices` and implicit `eapiPublicGetIndex` once per catalog underlying.
  Bybit bulk `fetchTickers` plus stock `parseOpenInterest`; Hyperliquid primary-DEX
  `fetchTickers`; Aster funding/interval/ticker methods; Extended `fetchTickers`;
  Lighter REST `fetchTickers` plus maintained Pro `watchTickers`.
- User decisions: no Binance all-market OI sweep; selected-ID demand is shared.
  Aster OI has no stock method. The 2026-10-04 Lighter decision supersedes its prior
  unsupported status: publish twice the one-sided WS `open_interest` as two-sided
  USDC notional, with null amount. REST OI is one-sided base and is not used.
- Provenance is `{exchange}:ccxt:{method}`. Each stock response has its own receipt;
  catalog/cache reads do not freshen values. Lighter REST cannot freshen silent live
  funding/mark/index/OI. Missing exchange time remains null. Funding/price strings
  preserve raw spelling; only the two new metrics use nullable finite JSON numbers.
- Binance `is_linear`/`is_inverse` prioritize subtype over type; a constructor's
  linear default misrouted bulk spot/option tickers. Real backend reproduction
  published fixture futures price `30001.20` for spot expected `500.00`. Product
  owners now clear that subtype outside contracts, and option mark calls specify type.
- Stock safe-number parsing can turn malformed OI into null. Ferris checks the
  retained raw members for invalidity while keeping stock amount/value mapping;
  genuine null stays missing, zero stays available, malformed/nonfinite clears the field.
- [Binance option ticker volume](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/24hr-Ticker-Price-Change-Statistics)
  is contracts; [catalog `unit`](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/Exchange-Information)
  defines underlying quantity per contract. No contract conversion is performed:
  base volume is null unless explicit unit is 1. OI preserves contracts and USD value.
  Option ticker `exercisePrice` changes to an estimated settlement price near expiry;
  the [stock index endpoint](https://developers.binance.com/legacy-docs/derivatives/options-trading/market-data/Symbol-Price-Ticker)
  supplies an unambiguous index instead. Bybit option amount is base quantity with
  [multiplier 1](https://www.bybit.com/en/learn/options/bybit-options-lesson-options-parameters-introduction).
- Actual backend HTTP/WS smoke delivered statistics for Binance, Bybit, Hyperliquid,
  Aster and Lighter. Extended loopback covered zero/null numeric deltas, invalid-value
  clearing, failure retention, recovery and catalog removal; Lighter finite frames
  followed by silence expired funding independently of REST volume. Extended's
  accepted public API failure was not retried and remains unqualified.


## Historical research handoff

The direction is to replace Ferris's exchange-specific public market-data
acquisition with **CCXT Rust / CCXT Pro**, reducing duplicated integration and
maintenance work while retaining Ferris's product contract. Use these findings
to choose the architecture and identify exceptions; do not assume every venue,
channel, or lifecycle operation is already qualified.

- Branch: `ccxt-migration`, starting from local `main` at
  `ca2b6c5fd262a30b1254bf4dd1b824a94f387f2a`.
- `docs/ccxt-orderbook-benchmark` is historical research, **deprecated as the
  ongoing work branch**. Its experiments and uncommitted artifacts are preserved,
  not imported into this branch. Earlier blanket stop recommendations must be
  read alongside the later successful ownership experiments below.
- Evidence checkpoint: the recorded **2026-10-01** extension results. The initial
  Ferris harness used **4.5.84**; the later standalone service used unpatched
  **4.5.85**. Do not mix their measurements or assume a newer release fixes them.
- This branch initially adds documentation only: no provider replacement,
  production CCXT dependency, or CCXT runtime patch.

## The takeaway to carry forward

**Cold initialization need not freeze existing feeds. Ownership and dispatch
matter more than whether an API happens to be called “multi-symbol.”**

- For the tested **Binance diff-depth** path, keep existing books on independent,
  stable upstream owners/connections. Growing a live native batch watcher blocked
  BTC while ETH initialized; genuinely isolated owners did not.
- **Bybit's native aggregate watcher** and **Lighter's thin single-owner/multi-hash
  adapter** kept BTC flowing while ETH initialized on a shared connection.
  Therefore, “one socket per symbol everywhere” is **not** the conclusion.
- One shared upstream owner can be appropriate; **competing mutable owners of
  the same URL/transport are not equivalent to independent feeds**.
- Continue sharing each trusted live feed through Ferris fanout. Upstream
  isolation does **not** mean a CCXT instance/socket for every user or widget.
- The successful topology fixes the tested initialization stall. It does **not**
  establish Binance synchronization correctness, safe unsubscribe/reconnect,
  production capacity, or a universal no-issues CCXT integration.

## Product requirements that drove the investigation

A user opening an unwatched ETH/ADA/etc. book may see loading for roughly
**1–2 seconds**. Users already watching BTC or another warm book must keep
receiving current updates throughout initialization, reseeding and recovery.
Some measured cold metadata/connection starts exceeded two seconds; this is a
user tolerance, not an already-demonstrated deadline for every market.

A **warm book** is actively maintained, trusted upstream state. A saved JSON
snapshot, loaded metadata, or an open socket alone does not make a book warm.
A later viewer should attach to the existing live feed without reinitializing
it. Any initial cached publication must come from that trusted current state.

Preserve Ferris's public models, exchange IDs, endpoint shapes, topic routing,
subscription sharing, freshness/error semantics, client protocol and bounded
backpressure. The backend may eventually coalesce intermediate **UI snapshots**
under an explicit product policy, but final state must be current, internally
consistent and delivered without an unrelated frame to wake it up. This is not
permission to drop raw diff events or trade events silently.

An additional **2–5 ms of steady-state backend delivery latency** can be worth
substantial maintenance savings. Cold readiness is a separate measurement. No
experiment so far establishes that added-latency budget against Ferris's full
native-to-client path.

## What the recent tests actually demonstrated

### Prescribed cold ETH delay, continuing BTC input

The 4.5.85 release loopbacks used the same adapters as the standalone live
service, real local HTTP/WebSocket connections, and a **one-second hold** on
ETH's initial book/REST seed. BTC inputs continued at **10 Hz**, with ten inputs
during the hold and five afterward, followed by silence. Three repetitions per
case; rate limiting was disabled **only locally** to isolate the injected wait.

| Tested path | BTC inputs during hold | BTC still outstanding at ETH release | Matched BTC delay p99/max during hold, across runs |
| --- | ---: | ---: | ---: |
| Bybit native aggregate | 10 | 0 | 0.436–0.476 ms |
| Lighter single-owner/multi-hash | 10 | 0 | 0.417–0.447 ms |
| Binance `batch-default` | 10 | **10** | **974.44–975.92 ms** |
| Binance `batch-shared` | 10 | **10** | **972.77–974.87 ms** |
| Binance `independent` | 10 | **0** | **0.422–0.461 ms** |

These are fixture send-start → owned adapter result delays, including loopback
transport, scheduling, dispatch and conversion, **not isolated parser CPU cost**.
With ten samples per hold, nearest-rank p99 is the maximum. Ranges are separate
runs, not pooled percentiles.

“BTC paused” means **delivery stopped while BTC source frames kept arriving**.
Both Binance batches also fetched warm BTC's REST seed again. The driven URL's
15 sentinel versions eventually returned, but a delayed replay of them does not
meet the warm-feed freshness requirement. Default batch mode additionally left
frames on an abandoned connection; post-stop inspection was diagnostic, not
successful publication.

### Fresh-server live comparison

Seven collection runs completed: two all-venue observed trials with independent
Binance, two trials of each Binance batch variant, and one all-venue direct
control. Regression checks were recorded separately. **“All-independent” in the
artifact names refers to Binance's topology**, not per-symbol isolation for
Bybit or Lighter.

| Binance topology, two observed runs | BTC maximum silence during addition/observation |
| --- | ---: |
| `batch-default` | **2,630–2,644 ms** |
| `batch-shared` | **1,252–1,276 ms** |
| `independent` | **130–153 ms** |

Independent Binance BTC interarrival p99 remained approximately **105–107 ms**
before and during/after ETH addition. Bybit and Lighter also had no detected
addition stall in those trials. The original Bybit positive control was about
**1,350 ms** to first BTC book, **167 ms** to first ETH book, and BTC interarrival
p99 **61.04 ms before / 60.15 ms during and after** ETH addition.

The live comparison describes different market-time windows, not a randomized
causal experiment or SLA. Its stall heuristic was
`max(1000 ms, 3 × baseline p99)`, **not an acceptable production freeze budget**.
A long individual pause can disappear from a whole-window p99: inspect maximum
silence, boundary gaps, identifiable input/output delay and outstanding versions.
Collection/check success and zero reported downstream drops do not turn a
known-stalling candidate into a migration pass.

## Ownership facts the next design must respect

### A CCXT exchange object is not necessarily a private connection

In the inspected Rust runtime, the WebSocket `REGISTRY` is **process-global and
keyed by URL**. Separate exchange instances selecting the same URL can share the
connection, incoming queue, subscription registry and settled results while
maintaining different exchange state. Creating another instance or Tokio task
alone does not provide transport isolation.

Earlier concurrent same-URL owners reproduced stranded results, overwritten
settlements and inconsistent extracted books. Some results needed an unrelated
incoming frame before the waiting watcher returned. These were local fixtures,
not slow public-market data.

The successful shared-connection pattern has **one coherent mutable
owner/dispatcher**, registering subscriptions and awaiting results for **all
active book hashes**. CCXT continues to parse frames and maintain the books.
An unsupported unified aggregate method does not by itself make this impossible:
Lighter's small adapter is the concrete example. Nor is a generated wrapper or
method name proof that the venue actually implements it.

Do not reintroduce competing same-URL owners when adding trades, candles or
another provider later. Mixed-channel dispatch was not qualified by the
order-book benchmark.

### Executor isolation was part of the measured topology

4.5.85's deferred `SPAWN_QUEUE` is **thread-local, not owner/task-local**.
`ws_drive_loop` drains deferred work inline and can await a REST snapshot before
returning settled books or reading more frames.

The standalone service therefore ran **each mutable owner on a dedicated OS
thread/current-thread Tokio runtime**. Independent Binance also had distinct
real stock URLs. “Two ordinary tasks on the shared application runtime” is not
the topology that produced those results. This benchmark arrangement is not a
qualified scheduler for hundreds of owners; thread/stack/resource costs and
other safe isolation choices remain production design questions.

Thread separation does not isolate the process-global URL registry or prove
Rust dependency soundness. Earlier source review also identified global
`SIDE_STORE` state and unsafe shared-to-mutable coercions such as
`coerce_value_to_mut`. Passing timing tests is not an audit of those internals.
Do not compensate with unsafe borrowing in Ferris.

### Keep mutable CCXT state inside the owner

Convert to owned Ferris data **before** handing results to shared fanout. The
initial harness already demonstrated direct typed conversion to `CcxtOrderBook`
without JSON-string serialization and reparsing. A mutable/lazy CCXT `Value`
backed by shared book storage is not an immutable client snapshot.

Do not use sequential BTC → ETH → other-symbol blocking watches, a mutex held
across `watch().await`, short-timeout polling disguised as concurrency, arbitrary
URL/query aliases, or destructive queue draining as a publication mechanism.
Those approaches either recreate head-of-line waiting or hide ownership bugs.

## Venue-specific notes

### Binance: the initialization-isolation result is strong, but narrow

The tested product is **USD-M linear USDT perpetuals**, native `BTCUSDT` and
`ETHUSDT`, unified `BTC/USDT:USDT` and `ETH/USDT:USDT`. The profile is a **REST
seed of 1,000 levels plus `@depth@100ms` diffs**, not partial-depth snapshots.

The 4.5.85 service used `BinanceCore`, `defaultType=swap`,
`defaultSubType=linear`, `fetchMarkets.types=[linear]`, `fetchCurrencies=false`,
`watchOrderBookLimit=1000`, and `watchOrderBookRate="100"`, with explicit limit
and rate on the native watch call. These identify the measured configuration,
not a generic setting for every Binance product.

- **Default growing batch:** changing BTC to BTC+ETH changes the subscription
  hash and allocates `/0` then `/1`; the old `/0` remains open but undriven.
- **Forced shared batch:** `streamLimits.future=1` keeps `/0`, but does not
  prevent warm BTC reseeding or the inline ETH snapshot wait.
- **Independent:** separate fixed-symbol owners used actual stock
  `wss://fstream.binance.com/public/ws/{0,1}` URLs, reserved through the stock
  `streamIndex` allocator. No fabricated aliases. The benchmark reserves only
  two slots; it is **not an arbitrary-market production connection allocator**.
- A validated catalog was loaded once and installed in the ETH owner without
  copying live books, transport state or allocator state. ETH was a cold
  connection/book, not a second metadata-load benchmark.

**Independent ownership does not fix these separate correctness concerns:**

1. **REST seed publication before synchronization.** Both research generations
   reproduced a structurally valid result before *any* bridging delta arrived.
   “First book” in those reports is not trusted diff-book readiness. A later
   result or nonzero nonce alone also does not prove a valid USD-M snapshot/
   delta bridge and subsequent `U`/`u`/`pu` continuity. Do not claim that merely
   skipping the first result solves synchronization. A conversion-only adapter
   cannot invent missing synchronization evidence.
2. **Depth retention is not a display option.** The earlier 4.5.84 fixture proved
   CCXT `limit()` could remove known backing-state levels that Ferris retained;
   later removals then exposed a different book. The 4.5.85 topology tests did
   not qualify or resolve this. A 1,000-level seed with a top-100 published view
   is not equivalent to requesting CCXT `limit=100`.
3. **Other semantics still matter.** Earlier fixtures found duplicate
   publications and timestamp differences (CCXT `E` versus native `T`). Gap,
   overlap, old/duplicate events, resync, decimal/precision boundaries and
   malformed data need exact-pin qualification, not silently repaired output.

Ferris already has a distinct native partial-depth path for shallow requests.
It must not be substituted for the deep diff profile to make a migration test
pass. Keeping a native exception where synchronization/retention cannot yet be
preserved is preferable to labeling an untrusted book live.

An earlier 4.5.84 `Binanceusdm` inherent wrapper returned `NotSupported` while
the generic typed dispatch reached Binance's implementation. The later service
used `BinanceCore`. Verify the actual method/dispatch path for the pinned release
rather than treating one wrapper failure as proof the venue is unsupported.

### Bybit: useful positive control, not proof about every CEX

The successful native aggregate path used **linear USDT BTC/ETH perpetuals,
depth 50**, one owner/connection. It supports the desired cold-add behavior
without a custom book parser. Optional spot received separate regression
coverage; other depths/products/channels are not thereby qualified. Outputs in
these runs omitted native `data.u` as a nonce, so timestamp matching and client
sequence checks do not prove full exchange-level sequence correctness.

### Lighter: thin adapter, metadata identity, different nonce meaning

4.5.85 did not implement the native aggregate order-book watcher. The tested
`lighter_next` adapter sends ordinary per-market subscriptions, preserves CCXT's
subscription record shape, and awaits all book hashes on **one Core/connection**.
CCXT still performs snapshot/delta maintenance; the adapter does not copy the
exchange book parser. It passed the delayed-ETH continuity/content fixtures and
had no detected live addition stall. Full offset-gap recovery and lifecycle
parity remain unqualified.

Captured metadata resolved **BTC market `1`, ETH market `0`** for USDC perpetuals.
Those are observations, **not constants to infer from array ordering**. Resolve
and validate the loaded native records, product, settlement and active status.
The exposed CCXT book nonce is native **`order_book.offset`**, not the separate
field named `order_book.nonce`. Ferris's public exchange ID is `lighterxyz`;
CCXT's provider ID is `lighter`. Do not accidentally change the client contract.

### Hyperliquid: retain the lesson, de-prioritize public live testing

The public endpoint showed roughly **5.4-second input cadence**, including a
direct WebSocket check without CCXT. The user reported recent public throttling
and node/provider recommendations (for example `hydromancer.xyz`). That context
is not a proven attribution of every gap. Do not report source silence as CCXT
processing delay; meaningful live freshness qualification needs an appropriate
source. Public Hyperliquid tuning is not the priority of this handoff.

The later single-owner/multi-hash adapter nevertheless passed local quiet/busy,
finite-burst/silence and eight-symbol schedules without an unrelated wakeup:
**128/128** outputs for two symbols and **584/584** for eight, in each of three
runs. This supersedes the idea that the unsupported aggregate API or earlier
same-URL multi-owner failures rule out all thin-adapter approaches.

**Unsubscribe remained a failure:** awaiting a 350-ms native acknowledgement
withheld BTC outputs despite **17 arriving BTC versions**; 16 versions were not
returned. Restart exposed **`[UnsubscribeError]`**. Bounded diagnostic retries
observed recovery, but the live service did not implement that recovery or safe
pending-unsubscribe handling. Ordinary-watch success is not lifecycle success.

## Replacement scope: CCXT below Ferris, not instead of Ferris

Existing exchange integrations include `binance`, `bybit`, `hyperliquid`,
`lighterxyz`, `aster` and `extended`. The investigated hot path is principally
**public order books**. Do not extrapolate it to complete REST, trade, OHLCV,
statistics or liquidation parity, or assume Aster/Extended support was tested.
No private trading/account migration is implied.

Useful baseline boundaries for the planning agent:

| Ferris responsibility | Existing source anchors |
| --- | --- |
| REST provider contracts and registration | `src/exchanges/traits.rs`, `src/exchanges/registry.rs`, `src/exchanges/*/`, `src/main.rs` |
| Shared live topics and upstream runners | `src/realtime.rs` (`OrderBookTopicManager`, trade/candle managers, connection runners) |
| Native synchronization/normalization reference | `src/binance_orderbook.rs`, `src/ws_shared.rs`, venue parsers in `src/realtime.rs` |
| Public models, routing and downstream fanout | `src/models.rs`, `src/web.rs`, especially `spawn_orderbook_topic_forwarder` |
| Statistics acquisition, freshness and projection | `src/market_stats.rs`, `src/market_stats/`, exchange `statistics.rs` modules |
| Contract/regression evidence | `tests/realtime_ws.rs`, `tests/market_stats.rs`, `tests/market_stats/`, inline unit/lifecycle tests |

`Ccxt*` names in Ferris are **Ferris-owned public shapes**, not an existing CCXT
implementation. CCXT values must fit those contracts, including symbols,
units, timestamps, nullability, supported parameters and errors. For example,
market statistics include coverage, field freshness, funding units and
snapshot/revisioned-delta behavior: a CCXT ticker is not automatically a
replacement. Preserve venue-specific product distinctions too, such as
Extended's indicative standard book versus an RFQ real book.

Resolve market identities from metadata, not ticker text alone: spot versus
perpetual, linear versus inverse, quote versus settlement, native ID, active
status, contract size and precision. Topic sharing must respect venue/product
and meaningful parameters, not just `BTC`. The original 4.5.84 preflight verified
BTC, ETH, XRP, ADA, BNB, SOL, ZEC and DOGE identities for Binance/Hyperliquid;
that is not full live, lifecycle or capacity coverage of all eight. The recent
Binance/Bybit/Lighter live extension principally covered BTC and ETH.

## Boundaries still unresolved before production replacement

- **Lifecycle and freshness:** final-viewer unsubscribe, delayed/missing control
  acknowledgements, cancellation, restart, reconnect, reseeding and stale cached
  settlements must not strand other active feeds. The standalone live manager
  intentionally did not unsubscribe mid-run and relied on fresh server processes
  between trials. Neither forever keeping every requested symbol alive nor
  restarting the whole service is a proven production cleanup policy.
- **Rate and connection budgets:** leave live rate limiting enabled. Independent
  CCXT instances/threads do not create independent exchange quotas. Metadata,
  snapshots, retries, reconnect storms, background polling and replicas sharing
  an egress IP all consume the real budget. Verify current venue limits before
  scaling socket-per-market approaches; arbitrary URL aliases are not a remedy.
- **Real capacity:** target assumptions were about five replicas, **400 users
  per replica**, potentially **50 symbols per user**, subscription changes every
  **5–6 minutes**. That is up to 20,000 subscription relationships per replica,
  not necessarily 20,000 upstream books. Measure overlap/deduplication, owner
  count, queue age, memory, CPU and serialization/egress. As an illustration,
  400 × 50 books × 10 updates/s × 1 KB is already about **1.6 Gbit/s** before
  protocol/other channels. Tiny two-book tests do not validate this load.
- **Honest latency comparison:** release builds, exact dependency pins/lockfiles,
  reproducible inputs and explicit quotas remain essential. Distinguish cold
  readiness, source cadence, ingress-to-owned-output, and ingress-to-client.
  The proposed acceptance is p99 of **paired per-event CCXT-minus-native added
  delivery latency ≤5 ms, preferably ≤2 ms**—not subtraction of unrelated p99s.
  Report absolute tails/maxima, failures, missing/coalesced versions and queue
  age alongside it. Relay instrumentation adds a hop/logging; it is not free.
- **Correct matching and final delivery:** match actual symbol, connection and
  event identity/content. Never match abandoned Binance `/0` input to `/1`
  output just because timestamps are nearby. “Unreturned versions” can include
  coalescing, seed supersession or observation-end censoring; it is not itself
  an internal queue-size or packet-loss measurement. Finite bursts followed by
  silence matter because a continuously busy source can hide stranded results.
- **Scope/maintenance honesty:** prefer supported APIs and small ownership/
  subscription/conversion glue, retaining CCXT parsing and maintenance. The
  measured watch-glue modules were small (about 46 Hyperliquid / 55 Lighter /
  62 Binance physical lines), but shared catalog, owner, dispatch and service
  code is additional work. Those counts are not total integration cost. A core
  runtime patch, copied synchronizer or process-isolation service must be called
  out and costed explicitly rather than hidden as a “thin adapter.”

## Evidence map and preservation notes

These are **host-local references**, not files checked into `ccxt-migration`.
This document preserves the conclusions if the next agent lacks those paths;
retrieve the evidence/source snapshot or requalify before making new claims.
Do not treat historical artifact paths mentioning an older worktree location as
proof the files are absent.

**Later 4.5.85 standalone service: `/root/benchmark/`**

- `README.md`, `RESULTS.md`, `CCXT-NOTES.md`: configuration, findings and exact
  pinned-source behavior. `Cargo.toml`/`Cargo.lock`: exact `ccxt` and `ccxt-pro`
  4.5.85 pins, resolved `ccxt-base`, default exchange features disabled and only
  Bybit/Hyperliquid/Binance/Lighter enabled.
- `src/owner.rs`, `src/venue.rs`, `src/adapters/{binance,lighter,hyperliquid}.rs`:
  the measured orchestration and adapters. `examples/loopback.rs`,
  `tests/fixtures/catalog.json`, `bench/evidence.mjs`: input/content oracles and
  measurement boundaries. This is a reference, not a production manager.
- `results/extension-comparison/aggregate.md` and `aggregate.json`: per-run
  comparison tables used above. `results/extension-loopback-release/` and
  `results/extension-live/`: individual raw evidence, matches and checks.
- `results/extension-final-regression-followup-20261001/`,
  `results/extension-verification/`: later regressions/validation.
  `results/validated/`: original Bybit positive control.

**Earlier 4.5.84 Ferris research: `/root/Ferris/` on the deprecated branch**

- `CCXT_ORDERBOOK_BENCHMARK_PLAN.md`, `CCXT_ORDERBOOK_BENCHMARK_RESULTS.md`,
  `CCXT_ORDERBOOK_FOLLOWUP.md`: original scope, correctness reproductions and
  ownership failures. Interpret their recommendations in chronological context.
- `benchmarks/orderbook/`: exact pins, direct typed-to-Ferris adapter,
  correctness/transport tests, native-control seams and historical artifacts.
  Those changes remain uncommitted research; they are not in this branch's
  ancestry merely because the branch exists in the same repository.

The separate `/root/Ferris-liquidations` worktree on
`feat/hyperliquid-trade-liquidations` contains ongoing, uncommitted work. It was
preserved and is **not part of this migration baseline**. Do not overwrite,
clean, or import it as an incidental migration operation.

**Bottom line:** build the migration plan around the demonstrated isolation and
single-owner patterns, not around the earlier failed watch loops. Keep the
remaining correctness, lifecycle and scale questions explicit. A new widget
must never make every viewer of an already-live feed watch a frozen book.
