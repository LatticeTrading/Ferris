# Funding market statistics: Hyperliquid-first delivery ledger

## 1. Active slice and exchange delivery ledger

**Approved execution scope: Hyperliquid only, primary-DEX active perpetuals, using one shared 30-second REST poll.** Deliver exchange-wide and selected-market statistics, truthful capabilities, and snapshot/delta delivery on Ferris's existing WebSocket. Explicitly selected spot funding is not applicable. This is backend work in Ferris; do not modify the sibling Lattice frontend.

This active specification supersedes the older research below. Retained sections 2–9 and 11 are historical evidence and deferred design research, not executable tasks or a statement of current implementation. Their venue recipes, broad model proposals, history endpoints, native-stream options, and illustrative wire examples do not expand this slice. The exact active contract below wins wherever retained research differs. In particular, the old Hyperliquid native-stream recommendation is superseded by shared polling, and there is no instruction to deliver all six venues together.

| Complete | Exchange | Approved product scope | Session status |
| --- | --- | --- | --- |
| [x] | `hyperliquid` | Primary-DEX active perps; selected spot funding explicitly not applicable | **COMPLETE: deterministic and live checks passed** |
| [ ] | `binance` | Not yet selected/qualified for implementation | Deferred |
| [ ] | `bybit` | Not yet selected/qualified for implementation | Deferred |
| [ ] | `aster` | Not yet selected/qualified for implementation | Deferred |
| [ ] | `extended` | Not yet selected/qualified for implementation | Deferred |
| [ ] | `lighterxyz` | Not yet selected/qualified for implementation | Deferred |

Hyperliquid completion gates:

- [x] Catalog IDs and settlement metadata match statistics identities.
- [x] All-market/selected snapshots share one primary bulk acquisition; field states and freshness are correct.
- [x] Ferris WS snapshots/deltas, ordering, unsubscribe, coalescing, and recovery pass the specified checks.
- [x] Capabilities advertise only delivered Hyperliquid support and its unknown rate-basis limitation.
- [x] Deterministic checks and the live smoke below pass; commands/results and delivered contract recorded below.

**Current ledger status:** Hyperliquid is complete. All five gates passed. The other five exchanges remain deferred; no HIP-3, native upstream statistics streaming, funding history, or frontend implementation was added. Stop here until a later exchange slice is explicitly selected.

For a later exchange session, explicitly select exactly one unchecked row; read that venue's retained acquisition recipe and current implementation; append a decision-complete active slice using the then-shipped API/coordinator. Keep every other unchecked row deferred. Implement and verify the selected venue end-to-end before checking it off. Do not repeat the initial shared foundation or advertise an unchecked venue merely because an endpoint exists. A venue requiring native acquisition extends the source driver only in its later approved slice, not with unused abstractions now.

### Delivery record and verification

Delivered `POST /v1/fetchMarketStats`, `GET /v1/capabilities`, and `marketstats` on the existing `/v1/ws`. Catalog and statistics identities share native opaque IDs and index-resolved settlement. Primary acquisition is shared REST polling every 30 seconds; spot metadata is cached for five minutes. HTTP demand lasts 90 seconds; socket demand is reference-counted. Shutdown and idle cancellation stop workers without late publication. Existing trade/book/OHLCV semantics remain separate.

Funding remains exact-string `currentUnclassified`, with `rateIntervalMs: null`, `paymentIntervalMs: 3600000`, and no inferred payment timestamps. Positive rates mean longs pay shorts. Mark/oracle prices have explicit qualified denominations; volume/OI remain unsupported (`units-unverified`). Selected spot funding is `notApplicable`. Stale observations retain original receipt timestamps; explicit scalar invalidation clears values. Context failures retain authoritative membership without shifting positional assignments. Partial or failed catalogs cannot invent removals or unknown IDs.

Socket acknowledgements precede initial snapshots; generations and contiguous revisions are per subscription. Complete states coalesce before sparse delta construction, at most once per second. Fields replace atomically, including null clears; coverage-only transitions are delivered. Backpressure forces disconnect/resubscription rather than continuing a gapped revision chain. The consumer contract is documented in README and INTEGRATION_README; Lattice was not modified.

Commands executed from the Ferris repository:

| Command | Result |
| --- | --- |
| `cargo check` | Passed; existing unused order-book constants/helper warnings remain. |
| `cargo test market_stats` | 52 passed: 42 unit tests and 10 actual localhost HTTP/WS/source tests. |
| `cargo test --test realtime_ws` | 1 passed; existing same-topic trade fanout unchanged. |
| `cargo test` | 165 passed, 5 pre-existing live tests ignored. |
| `cargo fmt -- --check` | Passed. |
| Approved temporary `bun -e` live client | Exit 0; console evidence below. No committed client script. |
| `python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8788 --exchange hyperliquid --markets-exchange hyperliquid` | Passed health, trades, OHLCV, order book, and catalog. |

The supervised actual binary used `HOST=127.0.0.1`, `PORT=8788`, `HYPERLIQUID_BASE_URL=https://api.hyperliquid.xyz`, and `TRADE_COLLECTOR_ENABLED=false`; readiness required the server-start log and listening port. It was stopped after smoke collection.

```text
PASS Hyperliquid: 178 primary perps; BTC funding 0.0000125; ordered WS snapshot/delta/unsubscribe

ok  healthz
ok  fetchTrades (5 rows)
ok  fetchOHLCV (3 rows)
ok  fetchOrderBook
ok  fetchMarkets (504 rows)
all endpoint checks passed
```

Counts and rates above are live observations, not constants or unit-basis evidence. The live client also checked catalog-issued BTC/spot IDs, USDC settlement/token ID, complete primary-only enumeration, spot applicability, unsupported OI/Binance, rejected empty selection, and freshness receipt advancement on the next WS poll.

Deterministic coverage includes positional ETH/OLD/BTC/HYPE joins and zero/negative rates; punctuation/display collisions; index-based settlement with reordered/ambiguous tokens; explicit invalidation without resurrection; context mismatch and metadata failures; concurrent 20-request/catalog/socket acquisition sharing; 30-second rather than 60-second deadlines; duplicate leases and idle restart; pending-request stale expiry; strict ack/snapshot order; reducer-verified coalescing, null clears and authoritative removals; unsubscribe after removal; 16-topic bounds; full outgoing queues and replacement generations; cold 200/incomplete versus selected 502 and unknown-ID 400; and capability reads with zero upstream requests.

Implementation tooling note: a provider rejected a subagent continuation with HTTP 400 / `previous_response_id` after its file writes. This was not a Ferris/upstream API failure. The saved socket scenarios subsequently compiled and passed the complete deterministic and live verification above.

### Active baseline and evidence

- At planning time `src/models.rs` had catalog display models without IDs/statistics, `MarketDataExchange` had trades/OHLCV/book/catalog methods only, and `src/main.rs` registered six adapters without statistics/capabilities routes. Treat this as a historical baseline, not an assertion about a partially edited tree.
- Hyperliquid's `post_info`, `build_catalog`, and `fetch_markets` already use primary `metaAndAssetCtxs` and `spotMeta`. Reuse the HTTP client/helper and existing display mapping. The indefinite legacy `MarketCatalog` and trade/book/OHLCV resolver semantics remain unchanged.
- `MarketsCache` supplies the double-checked, per-key sharing pattern. Existing WS symbol requirements, acknowledgement races, and broadcast lag warning-and-continue behavior are not safe statistics semantics to copy unchanged.
- A read-only public capture on 2026-09-11 returned 234 metadata entries/contexts: 178 active perps and 56 delisted. `collateralToken: 0`, resolved by token `index`, was USDC with token ID `0x6d1e7cde53ba9467b783cb7c530ce054`. BTC funding was `-0.0000006156`; ETH was `0.0000125`. These are observations, not fixed counts/fixtures or proof of the rate's hourly basis.
- [Perpetual contexts][H1] and [spot metadata][H2] establish pairing/collateral lookup. [Funding rules][H5] establish positive-long-pays-short and hourly payments, but not explicitly whether context `funding` is already hourly. Ship the unknown-basis contract; do not infer it from magnitude.
- [Contract specifications](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications.md) distinguish USDC settlement from generally USDT-denominated prices, with exact HYPE/PURR USDC-price exceptions. [Price-index documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/robust-price-indices.md) distinguishes mark/oracle. Volume and OI units remain unqualified here.
- [Rate limits][H4]: these `/info` requests cost 20 weight against 1,200/minute/IP. A primary poll every 30 seconds costs 40 weight/minute independent of viewers; cache spot metadata for five minutes. No new HTTP client, SDK, or upstream statistics WS is needed.

### Active implementation sequence

Execute active steps 1–5 in order as one integrated Hyperliquid delivery. Steps 2 and 3 depend on step 1; exposure depends on both. Add boundary checks alongside changes, then run integrated validation once the tree builds. Before exported-model/trait changes, use LSP references if available; none was configured at planning time. Otherwise use scoped constructor/reference searches. `ApiError` response mapping is in `src/errors.rs`; WS parser/ack callers are in `src/web.rs` and its local tests. The optional source accessor leaves all six existing adapters' required methods unchanged.

#### Active step 1: smallest reusable contract

Add camelCase transport models to `src/models.rs`, reusing `UnifiedMarketType` and display-market fields. Create/export `src/market_stats.rs` from `src/lib.rs` now for identity and internal source types; add the coordinator in step 3. Define capability transport models now using the exact step 4 shape, not a placeholder.

**Catalog identity:** add flattened `identity: Option<MarketIdentity>` to `UnifiedMarket`, omitted when `None`. `MarketIdentity` has exact fields `marketId: String`, `exchangeMarketId: String`, `category: Option<String>`, `dex: Option<String>`, `contractType: Option<String>`, `settle: Option<String>`, `settlementAssetId: Option<String>`. Nullable identity properties serialize as null. Do not change existing `symbol`, `base`, `quote`, `info.*`, or legacy symbol resolution.

Implement `pub fn make_market_id(exchange: &str, market_type: UnifiedMarketType, category: Option<&str>, dex: Option<&str>, exchange_market_id: &str) -> Result<String, ExchangeError>`. Return compact JSON serialization of the exact five-element tuple `(exchange, market_type, category, dex, exchange_market_id)` as an opaque string; no delimiter encoding/dependency, display symbols, or native-ID sanitization.

- Primary perps use native metadata `name`, category null, dex `""`, contractType null. BTC's ID string is `["hyperliquid","perp",null,"","BTC"]`.
- Spot uses decimal `universe[*].index`, category/dex/contractType null. Index-zero spot ID is `["hyperliquid","spot",null,null,"0"]`. Clients obtain IDs from `/v1/fetchMarkets`, never construct them.
- Resolve perp settlement from current `collateralToken` matched to `spotMeta.tokens[*].index`, then token `name`/`tokenId`, never vector position or display quote. Missing/ambiguous metadata yields null fields. Spot settlement fields are null.
- Search `UnifiedMarket\s*\{` under `src` and `tests` immediately before editing. Initialize `identity: None` in deferred Aster `map_market`, Binance `map_exchange_information_symbol`, Bybit `map_market_row`, Extended `map_market`, and Lighter `LighterMarket::into_unified_market` literals and any newly added callers. This is compilation maintenance, not support. Hyperliquid's two literals get `Some(identity)`; no deferred-adapter network/capability changes.

**Request:** `FetchMarketStatsRequest { exchange: String, market_ids: Option<Vec<String>>, fields: Option<Vec<MarketStatsFieldName>>, params: serde_json::Value }`, camelCase and `deny_unknown_fields`; default exchange to `hyperliquid` using the existing helper. Reject unknown top-level keys, especially `symbol`. Field names are exactly `funding`, `lastSettledFunding`, `markPrice`, `indexPrice`, `lastPrice`, `volume24h`, `openInterest`.

```json
{"exchange":"hyperliquid","fields":["funding"],"params":{"dex":""}}
```

Omitted/null `marketIds` selects all active primary perps, never spot/HIP-3; explicit selection may include known inactive perps/spot. Omitted/null fields means `["funding"]`. Reject empty arrays and more than 100 input IDs before deduplication; deduplicate/sort IDs and fields. IDs are case-sensitive opaque strings, not rewritten. Only exchange is trimmed/lowercased. Accept params omitted/null/`{}`/`{"dex":""}`; reject nonobjects, non-string/nonempty dex, category, and all other keys. Nonempty `symbol` is invalid on a statistics WS command.

Before acquisition, server-side scope validation may decode the exact five-element ID tuple to identify exchange/product/category/DEX and determine which catalog a missing ID needs. Reject malformed tuples, foreign exchanges, nonempty perp DEX, non-null categories, unsupported products, and noncanonical encodings with 400. Resolution requires exact equality with the catalog-issued string; never use decoded names for per-market upstream calls. A genuine unknown perp can then return 400 while an unresolved spot catalog returns 502.

**Response:** `MarketStatsSnapshot { timestamp: u64, scope, markets, coverage }`; timestamp is Ferris capture milliseconds, scope is `{"exchange":"hyperliquid","params":{"dex":""}}`. Selected spot rows retain dex null. `MarketStatsRow` flattens `UnifiedMarket`, adding `fields: BTreeMap<MarketStatsFieldName, MarketStatsField>`; include exactly every requested field and sort rows by opaque marketId.

Coverage is `{expectedMarkets: number|null, returnedMarkets: number, enumerationComplete: boolean, sourceFailures: [{source,reason,message}]}`. All-market expected count is latest validated active-perp count, null before a valid catalog; selected count is deduplicated input size. Enumeration concerns membership, not field availability. Primary failure makes current enumeration incomplete while retaining known rows/count. Spot failure does not invalidate otherwise complete primary enumeration.

`MarketStatsField` has `{state,value,reason,exchangeTimestamp,receivedTimestamp,source}`. Value is null or strongly typed untagged `MarketStatsValue::{Funding(FundingValue), Price(PriceValue)}`; do not add unused OI/volume variants. Reason, timestamps, and source are nullable. States are exactly `available` (valid value including zero funding), `notApplicable` (null/product has no meaning), `unsupported` (null/not implemented or unqualified units), `unavailable` (null/no usable observation or explicit invalid/missing scalar), and `stale` (retained last valid value/original timestamps after failure or expiry).

`FundingValue` is `{rate: String, kind: FundingKind, rateIntervalMs: Option<u64>, paymentIntervalMs: Option<u64>, paymentTimestamp: Option<u64>, nextPaymentTimestamp: Option<u64>}`. Enum wire variants are `estimate`, `settled`, `currentUnclassified`; Hyperliquid emits only `currentUnclassified`. Preserve exact validated decimal funding string, rateIntervalMs null, paymentIntervalMs 3600000, payment timestamps null; valid funding reason is `rate-basis-unverified`. Positive means longs pay shorts. No divide-by-eight, percent conversion, annualization, derived next hour, settled label, or guaranteed estimate.

`PriceValue` is `{amount: String, baseAsset: String, quoteAsset: String}`. Map markPx→markPrice and oraclePx→indexPrice, never midPx. Preserve native base names/scaling. Quote is USDC for exact primary names HYPE/PURR and USDT for other primary crypto perps, independent of display quote/USDC settlement. Implemented observations use source `hyperliquid:primary:metaAndAssetCtxs`, exchangeTimestamp null, and receivedTimestamp from HTTP-body receipt, not cache reads; retain monotonic age separately. Fixed unsupported/not-applicable fields have null source/timestamps.

| Field/product | Exact active result |
| --- | --- |
| Active primary perp funding, markPrice, indexPrice | Available when valid; runtime failures follow step 3. |
| Selected inactive perp, implemented fields | Unavailable / `inactive-market`; never current delisted funding. |
| Spot funding or lastSettledFunding | NotApplicable / `non-perpetual-market`. |
| Perp lastSettledFunding | Unsupported / `adapter-not-implemented`; no per-row history. |
| Perp volume24h or openInterest | Unsupported / `units-unverified`. |
| lastPrice; spot price/volume/OI | Unsupported / `adapter-not-implemented`; no extra acquisition. |

**Errors:** reuse flat `{code,message}` `ApiError`, not the catalog's nested envelope. Add `ApiError::UnsupportedFeature(String)` → HTTP 501 / `UNSUPPORTED_FEATURE`; no ExchangeError variant. Bad request, scope, field, empty lists, unknown ID → 400 / `VALIDATION_ERROR`; catalog-confirmed unknown message is `unknown marketId: {id}`. Unregistered exchange → existing 400 / `UNSUPPORTED_EXCHANGE`. Registered deferred exchange → 501 with `market statistics are not implemented for exchange '{id}'`, without upstream stats work. Cold selected catalog failure → existing mapped 502, not invented unknown. Cold all-market failure → 200, empty rows, expectedMarkets null, enumerationComplete false, source failure; retain explicit unavailable/stale rows once identities are known.

#### Active step 2: Hyperliquid-only acquisition and normalization

Add the narrow optional source boundary, retaining all existing required methods:

```rust
#[async_trait]
pub trait MarketStatsSource: Send + Sync {
    fn capabilities(&self) -> MarketStatsCapabilities;
    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError>;
}
// Default method on MarketDataExchange:
fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> { None }
```

`FetchMarketStatsParams { params: Value }` contains validated acquisition scope only, never client IDs/fields. Hyperliquid implements the source and returns Some(self); deferred adapters inherit None. The internal unfiltered `MarketStatsSourceSnapshot` in `src/market_stats.rs` contains exactly `rows: Vec<MarketStatsRow>`, `perp_catalog_known: bool`, `perp_enumeration_complete: bool`, `spot_enumeration_complete: bool`, `contexts_valid: bool`, `received_at: Option<tokio::time::Instant>`, `next_poll_at: tokio::time::Instant`, `source_failures: Vec<MarketStatsSourceFailure>`. Rows include all known perp/spot identities and their fields/receipt wall timestamps. `MarketStatsSourceFailure` is the coverage `{source,reason,message}` record.

1. Add one adapter-owned two-key info cache: metaAndAssetCtxs for 30 seconds, spotMeta for 300 seconds. Follow MarketsCache double-check/per-key single-flight using immutable Arc JSON, actual wall/monotonic receipt timestamps, and Tokio Instant deadlines. A network await may hold the dedicated acquisition gate, never the cache reader/writer lock. Cache latest transport result including failures until next eligible refresh, preventing retry storms. Retain last structurally valid spot token/catalog metadata separately; the coordinator alone retains previous primary normalized rows.
2. Reuse post_info client/timeout; no dependency/client additions. Success and failure TTLs start when that individual observation completes. Capture wall/monotonic receipt immediately after each HTTP/JSON read before awaiting the other source. Cached reuse preserves timestamps/deadline. Expose the primary cache deadline as next_poll_at. Replace direct calls in both build_catalog and fetch_markets with this cache while keeping display, ordering, inactive filtering, aliases, get_catalog lifetime, legacy resolvers, and trade collector unchanged. fetch_market_stats joins it; cold primary/spot calls run concurrently, never per coin.
3. Extract reusable private perp/spot UnifiedMarket mappers from fetch_markets for catalog/statistics. Add identity at mapping time; size fields are not funding/unit evidence. Never use sanitize_asset_for_market, normalize_lookup_key, aliases, or context index for perp ID. Spot statistics requires native pair index; do not invent identity.
4. Decode primary response as exactly two elements: metadata object with universe array, then context array. Validate every metadata row first: unique nonempty native names, no DEX-qualified names. Pair exact universe[i]/context[i] before filtering, sanitizing, or sorting. Never truncate zip or shift indexes after skipping metadata. Invalid metadata, duplicate names, missing universe, or an empty universe/context response on a populated DEX is UPSTREAM_DATA_INVALID, not deletion. An all-delisted valid catalog may project to an empty active list.
5. Distinguish metadata from metric validity. Primary transport/invalid metadata returns Err; coordinator retains normalized membership, marks enumeration incomplete, and never rehydrates an older raw primary payload that could resurrect removals. Before any catalog, state is empty/unknown. Valid complete metadata but invalid/misaligned context array returns Ok with known/complete perp catalog, contexts_valid false, `context-mismatch`; retain stale values or unavailable if never observed. Do not assign shifted values. Invalid individual scalars are field invalidations, not whole-source failure.
6. In a correctly aligned array, a nonobject row or missing/null/invalid scalar clears each affected implemented field to unavailable / `invalid-upstream-value`; valid sibling fields remain available. Never retain an old number after explicit invalidation. Add an exact ASCII lexical parser following BinancePrice::parse's lexical style, not its positive-only f64 model: accept `-?[0-9]+(\.[0-9]+)?` strings; reject empty, whitespace, NaN, infinity, exponents, numeric JSON; preserve strings unchanged. Funding accepts zero/negative. Prices require no negative sign and at least one nonzero digit. Do not alter the order-book parser.
7. Resolve current collateral through five-minute spot metadata. Missing/duplicate matching token indexes or unresolved collateral produces null settlement fields plus `settlement-unresolved`, not loss of current funding. Spot transport failure may use last-good token metadata to resolve the current collateral index while recording `hyperliquid:spotMeta` / `upstream-failure`; without usable metadata, settlement stays null. Successful invalid/duplicate matching tokens invalidate resolution without old fallback. Missing/duplicate pair IDs or unresolved pair tokens exclude those statistics identities and make spot enumeration incomplete; legacy catalog display mapping may remain permissive. A missing selected spot ID cannot be declared unknown without complete spot identity; primary completeness is independent.

Do not implement allDexsAssetCtxs, predictedFundings, fundingHistory, perpDexs, spot contexts, or other exchanges. Do not expose raw volume/OI as qualified metrics.

#### Active step 3: shared snapshots, demand, and freshness

Implement `MarketStatsCoordinator` in the module introduced in step 1, using Tokio RwLock/Mutex/watch, Arc, and existing shutdown style, not trade-event buffers or another cache dependency:

```rust
impl MarketStatsCoordinator {
    pub fn new(registry: Arc<ExchangeRegistry>) -> Self;
    pub async fn snapshot(&self, request: FetchMarketStatsRequest)
        -> Result<MarketStatsSnapshot, ApiError>;
    pub async fn subscribe(&self, request: FetchMarketStatsRequest)
        -> Result<MarketStatsSubscription, ApiError>;
    pub async fn unsubscribe_by_key(&self, key: &str);
    pub async fn shutdown(&self);
}
```

`MarketStatsSubscription` exposes canonical `MarketStatsTopic`, projection key, and `watch::Receiver<Arc<MarketStatsSourceSnapshot>>`. Topic serializes `{exchange,params,marketIds,fields}` with sorted/deduplicated nonempty fields and marketIds null for all-market. Its compact JSON is projection key. Acquisition identity is `(registry exchange ID, canonical params)`; fixed registered adapter owns deployment/base URL. Fields, selection/order, and client count never enter acquisition identity. Keep projection leases `{projectionKey -> (acquisitionKey, subscriberCount)}`; identical clients each own a lease, all/selected share a worker. Unsubscribe releases one lease, removes zero-count entry, never fetches metadata.

- One demand-driven worker per supported acquisition key, retaining latest immutable normalized source state. Fetch immediately, then sleep to shared primary next_poll_at, not an independent TTL/tick pair that samples every 60 seconds. After wholly cold error retry once after 30 seconds; no overlap/catch-up. Between deadlines HTTP projects latest state. Normalize a catalog-refreshed observation once with unchanged timestamps/deadline. Drive in-flight acquisition, expiry, and shutdown together with tokio::select!, so network waits cannot block stale publication.
- HTTP demand holds/renews a 90-second lease from last request; WS demand is persistent/refcounted. Stop when no WS demand and HTTP lease expired; retain state. Renewed idle demand joins one acquisition before claiming freshness. Shutdown signals/awaits workers and is used in server/test teardown. Cancellation drops in-flight source futures and their Tokio acquisition guards; no late publish into a replacement worker. Start/stop/lease changes share the source-state lock and mark/remove stopped worker before replacement.
- Cold snapshot/subscribe callers join one bootstrap using configured HTTP timeout for each concurrent bulk call. Publish failure to wake all waiters, no same-request retry. Known identities get explicit unavailable/stale fields; insufficient selected identity follows 502. Cold all-market WS accepts incomplete/empty snapshot and polls toward recovery. Selected subscribe lacking identity fails SUBSCRIBE_FAILED. Failed validation/unknown selection releases provisional demand and does not gain an HTTP lease; successful all-market incomplete responses retain normal lease.
- Missing selected ID is 400 only when its product catalog is currently complete; otherwise mapped 502. Known selected spot can report notApplicable funding during primary failure without acquiring spot prices. New successful primary receipt atomically replaces implemented field objects, even unchanged numeric values, including metadata/source/timestamps. Build once per new payload and reuse immutable state across views. Latest outcome controls source failures; recovery clears them and capabilities never switch off.
- Transport/integrity failure immediately makes prior usable values stale with original timestamps/failure reason; never-observed or already-cleared values remain unavailable. Do not resurrect an explicitly invalidated value. Structural unsupported/notApplicable/inactive states remain. Successful observations restore valid available fields and clear runtime failures.
- Stale threshold is 90,000 ms from primary monotonic receipt. Independent one-second worker timer publishes expiry even during pending acquisition or unchanged rates; idle retained projections apply same age rule. Use `stale-threshold` only when age alone caused transition; preserve specific observed failure reason past threshold. Ordinary poll failure is `upstream-failure`, structural context failure `context-mismatch`. Capability retains rate-basis limitation while runtime field reason changes.
- Precedence: applicability/support, then inactive implemented perp fields, then explicit invalid scalar, then source failure/expiry, then available. contexts_valid false means stale retention, not three scalar clears. Map primary ExchangeError::UpstreamData→`invalid-upstream-data`, UpstreamRequest→`upstream-failure`. Keep authoritative spot membership separately when next outcome has incomplete spot enumeration.
- Only valid metadata changes membership. All-market removes newly delisted/authoritatively absent IDs; selected keeps known delisted rows as inactive-market. Later authoritative absence removes an already-subscribed selected row and emits removedMarketIds; new requests then fail validation. Selected expected count remains original deduplicated request count even if returned count shrinks. Failed/sparse/invalid payloads never remove rows.
- watch carries complete latest source states only and may skip intermediate complete observations. Client deltas are later compared against that client's last delivered projection; never coalesce/drop constructed sparse deltas. No per-projection upstream collectors/unbounded snapshot queue.

#### Active step 4: REST exposure and truthful capabilities

Add `market_stats: MarketStatsCoordinator` to `AppState`, constructed with exchange_registry.clone() inside unchanged AppState::new signature (main and tests/realtime_ws callers). Add `pub async fn shutdown_market_stats(&self)`. Main passes state.clone() to router and shuts coordinator down after server finishes, including error path; tests retain clone/use same teardown.

Add `fetch_market_stats(State(state): State<AppState>, payload: Result<Json<FetchMarketStatsRequest>, JsonRejection>) -> Result<Json<MarketStatsSnapshot>, ApiError>` and `capabilities(State(state): State<AppState>) -> Json<CapabilitiesResponse>`. Register `POST /v1/fetchMarketStats` and `GET /v1/capabilities` with existing Axum style. Map JSON rejection to ApiError::Validation; HTTP and WS use the same validation/projection. Add sorted `ExchangeRegistry::ids(&self) -> Vec<&str>`; capabilities reflect actual registration, not a second venue list, and make no upstream requests.

CapabilitiesResponse is `{"exchanges":[...]}` sorted by exchange ID; each entry has `{exchange,marketStats,fundingRateHistory}`. Every adapter with no source accessor reports this unsupported shape (example Bybit):

```json
{"exchange":"bybit","marketStats":{"state":"unsupported","reason":"adapter-not-implemented"},"fundingRateHistory":{"state":"unsupported","reason":"adapter-not-implemented"}}
```

Only when delivered, Hyperliquid marketStats uses this exact supported detail; fundingRateHistory stays the same unsupported record. Runtime failures do not rewrite structural capabilities:

```json
{
  "state":"supported",
  "scope":{"exchange":"hyperliquid","params":{"dex":""}},
  "allMarkets":{"types":["perp"],"activeOnly":true},
  "selectedMarkets":{"types":["perp","spot"],"limit":100},
  "fields":{
    "perp":{
      "funding":{"state":"supported","reason":"rate-basis-unverified"},
      "lastSettledFunding":{"state":"unsupported","reason":"adapter-not-implemented"},
      "markPrice":{"state":"supported","reason":null},
      "indexPrice":{"state":"supported","reason":null},
      "lastPrice":{"state":"unsupported","reason":"adapter-not-implemented"},
      "volume24h":{"state":"unsupported","reason":"units-unverified"},
      "openInterest":{"state":"unsupported","reason":"units-unverified"}
    },
    "spot":{
      "funding":{"state":"notApplicable","reason":"non-perpetual-market"},
      "lastSettledFunding":{"state":"notApplicable","reason":"non-perpetual-market"},
      "markPrice":{"state":"unsupported","reason":"adapter-not-implemented"},
      "indexPrice":{"state":"unsupported","reason":"adapter-not-implemented"},
      "lastPrice":{"state":"unsupported","reason":"adapter-not-implemented"},
      "volume24h":{"state":"unsupported","reason":"adapter-not-implemented"},
      "openInterest":{"state":"unsupported","reason":"adapter-not-implemented"}
    }
  },
  "upstreamMode":"sharedPolling",
  "pollIntervalMs":30000,
  "staleAfterMs":90000,
  "ws":{"snapshot":true,"delta":true,"maxSubscriptionsPerConnection":16},
  "fundingKinds":["currentUnclassified"],
  "rateIntervalMs":null,
  "paymentIntervalMs":3600000,
  "limitations":["primary-dex-only","rate-basis-unverified","receipt-time-freshness"]
}
```

#### Active step 5: safe statistics projections on existing WS

Integrate only in src/web.rs; do not change src/realtime.rs, src/ws_shared.rs, or existing trade/book/OHLCV channel semantics.

- Add defaulted raw JSON market_ids/fields to ClientStreamCommand; decode typed stats request only for normalized channel marketstats, ignore new properties on legacy channels. Add ParsedStreamCommand::{SubscribeMarketStats,UnsubscribeMarketStats}, each carrying FetchMarketStatsRequest, branching before TradesTopic::from_client_request. No required symbol resolver or invented `symbol:"*"`. Unsupported-channel message lists marketstats alongside trades/orderbook/ohlcv; funding remains unsupported, not an alias. Retain legacy behavior from parse_stream_command_supports_all_realtime_channels.
- Keep existing subscription map/record unchanged; add per-connection HashMap<String, MarketStatsClientSubscription> storing canonical topic, key, forwarder. Pass through existing command handler and drain/abort/unsubscribe on every exit. Make private WsAckMessage generic over serializable topic, preserving legacy type/op/topic bytes.
- Exact all-market command is `{"op":"subscribe","channel":"marketstats","exchange":"hyperliquid","params":{"dex":""},"fields":["funding"]}`; unsubscribe is identical with op changed, selected adds catalog-issued marketIds. Normalize/validate before resources. Limit 16 distinct statistics subscriptions per socket; duplicate canonical fields/selection returns alreadySubscribed without extra demand; 17th returns error/SUBSCRIPTION_LIMIT. INVALID_COMMAND is syntax, INVALID_TOPIC bad scope/ID, UNSUPPORTED_FEATURE registered unsupported, SUBSCRIBE_FAILED selected identity lost to upstream failure, UNSUPPORTED_EXCHANGE unknown exchange. Failed subscribe retains no lease. Unsubscribe derives canonical key without rechecking membership/acquiring catalog, so vanished IDs remain unsubscribable; missing key gives NOT_SUBSCRIBED.
- Subscribe attaches watch receiver and increments demand under ordering fence. Successfully enqueue subscribed before spawning forwarder; enqueue failure releases the unstored lease immediately. Store successful records. Forwarder initially borrow_and_update() and queues full snapshot before changed(); intervening updates appear in that snapshot or afterward, never first as delta. Later enqueue failure signals close only; socket-map cleanup releases lease exactly once. Forwarders never separately unsubscribe.

| Mode | Exact top-level contract |
| --- | --- |
| Snapshot | `type:"marketstats"`, `mode:"snapshot"`, topic, generation, revision, flattened timestamp/scope/markets/coverage. |
| Delta | `type:"marketstats"`, `mode:"delta"`, topic, generation, previousRevision, revision, timestamp, scope, updates, removedMarketIds, coverage. |

Generation is opaque new string per client subscription: process-start UNIX nanoseconds plus monotonic subscription counter, no dependency/upstream sequence. Snapshot revision is 1; each emitted delta advances exactly one and points previousRevision at that subscription's last delivered revision. Two clients need not share revision. Reconnect/resubscribe always gets new generation/replacement snapshot; no durable replay.

- Forwarders retain last successfully enqueued complete projection, compare with newest complete watch state, and publish at most once/second after immediate initial snapshot, accumulating newest state. Compare rows, field objects, identity, coverage; exclude only response capture timestamp. New field receipt timestamps are meaningful even with identical rate, so healthy funding-only clients get updates after each successful 30-second observation.
- Existing-row updates contain full identity/display and only changed requested fields; new rows or identity/active changes include all requested fields. Present field replaces whole old object, including null/metadata; absent means unchanged. removedMarketIds only reflects authoritative membership removal. Every delta includes current coverage, including coverage-only changes.
- Never drop constructed/enqueued sparse deltas or continue a gapped revision chain. Complete watch states may be skipped safely; compute final change from what this client received, not prior global poll. Keep send_ws_json's bounded outgoing queue. Enqueue failure signals close/reconnect, never CLIENT_LAGGED warning-and-continue.
- Make stats-requested disconnect bounded with per-socket Arc<AtomicBool>, set by failing forwarder before close_signal. After map cleanup abort/await writer on that flag rather than wait forever on blocked send; preserve graceful legacy-only/normal draining.
- Consumer replaces whole view on snapshot; apply delta only if generation matches and previousRevision equals locally applied revision, otherwise discard/resubscribe. Exercise with smoke/test reducer; no Lattice implementation.

### Active verification and completion record

Run only during integrated implementation, from the Ferris repository. Prerequisites are Rust/Cargo, Bun for temporary live client, outbound public HTTPS. Cargo/Bun were found in planning; no build/tests/server ran in plan mode. Only planning acquisition was two read-only public `/info` calls. Ledger restructuring alone is not verification and leaves every gate open.

Add focused market_stats_ checks in Hyperliquid/coordinator/web modules and tests/market_stats.rs for actual localhost HTTP/WS. Follow tests/realtime_ws.rs ephemeral Axum/tokio-tungstenite patterns, but read messages in strict arrival order; its type-skipping helper cannot prove acknowledgement ordering. Disable the mock Hyperliquid trade collector. Use programmable POST /info, barriers, request counters, not live fixtures/long sleeps. Add Tokio test-util only as a dev-dependency feature for paused-time freshness/lease checks; no production timer config/extra exchange library. Sharing proof is actual responses plus request counts. Coalescing proof is a public-contract consumer reducer whose final values/clears/removals match latest full projection, not internal callback counts or nonempty arrays.

| Required scenario | Observable acceptance |
| --- | --- |
| Positional identity/sign/units | Fixture universe ETH, delisted OLD, BTC, HYPE with funding `"0"`, `"0.03"`, `"-0.0000125"`, `"0.0000125"`: default includes ETH/BTC/HYPE, BTC keeps index-2 negative rate, ETH zero is available, rate intervals null. BTC price quote USDT/settlement USDC; HYPE quote USDC. Reorder spot token records to prove collateral resolves by index. |
| Identity collisions/catalog join | Perp/spot same display pair and punctuation-bearing native names sanitizing similarly retain distinct IDs matching catalog/statistics through reordering/filtering/sorting; no wrong context joins. |
| Field invalidation | Valid BTC funding then aligned missing/empty/NaN funding plus valid mark: funding clears unavailable/null (including WS), mark available. Later transport failure never resurrects cleared funding. Spot funding notApplicable, OI/volume unsupported rather than zero. |
| Structural mismatch/recovery | Fewer contexts than universe after baseline retains stale values/context-mismatch without shifting and valid metadata stays enumerable. Invalid/duplicate/empty metadata retains membership with incomplete enumeration; later valid complete observation recovers IDs. |
| Sharing/leases | Barrier-release 20 simultaneous all/selected HTTP, two WS, catalog calls: exactly one primary and one spotMeta request in interval; values agree at common receipt. Refresh after 30 seconds, not 60. Closing one leaves other alive; after both close and HTTP lease expires polling stops; renewed demand shares acquisition. |
| Freshness without numeric changes | Identical successful rates advance receipt only on new observation, not cache read. Failed poll retains stale value/old receipt; recovery refreshes. Pending acquisition cannot block stale-threshold after 90 seconds. |
| Ordering/coalescing/removal | All and BTC-only clients see subscribed first, snapshot revision 1 second. Coalesced complete states touching different fields/markets reduce to latest projection with contiguous client revisions. Valid delisting/removal affects only appropriate view; failure never removes. Unsubscribe works after selected ID vanishes. |
| Bounds/recovery | Empty IDs/fields, 101 IDs, bad/foreign/unknown ID, nonprimary dex, unknown field, conflicting symbol fail without extra source work. Reject 17th distinct topic. Full queue closes rather than gapped chain; resubscribe yields new generation/full snapshot. |
| Honest support/failure | Capabilities makes no acquisition; only Hyperliquid supports, all registered deferred stats requests return 501. Cold all-market failure explicitly incomplete; selected without proof 502, not invented unknown/notApplicable. Runtime failure leaves capabilities supported. |

Keep existing deterministic same-topic trade fanout, book, OHLCV behavior unchanged; update optional catalog construction only where required, not unrelated wording assertions. Integrated commands:

```text
cargo check
cargo test market_stats
cargo test --test realtime_ws
cargo test
```

#### Live Hyperliquid REST/WS smoke

Start actual Ferris through supervised hub, never a browser/dev-server harness:

```text
hub start
  name: ferris-funding-smoke
  application: cargo
  args: ["run"]
  cwd: C:/Users/Kishan/Documents/GitHub/Ferris
  env: { HOST: "127.0.0.1", PORT: "8788", HYPERLIQUID_BASE_URL: "https://api.hyperliquid.xyz", TRADE_COLLECTOR_ENABLED: "false" }
  ready: { log: "server started", port: 8788, timeout: 120 }
```

If 8788 is occupied use 8789 consistently; do not terminate unrelated services. No API key required. After readiness run the temporary client below as `bun -e <script>`, passing multiline script as one argument via eval (no committed script), with FERRIS_BASE_URL=http://127.0.0.1:8788. Record its console evidence in this ledger.

```javascript
const base = process.env.FERRIS_BASE_URL ?? "http://127.0.0.1:8788";
const check = (ok, message) => { if (!ok) throw new Error(message); };
async function request(path, body, expected = 200) {
  const response = await fetch(base + path, {
    method: body === undefined ? "GET" : "POST",
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(20000),
  });
  const result = await response.json();
  check(response.status === expected, `${path}: ${response.status} ${JSON.stringify(result)}`);
  return result;
}
const caps = await request("/v1/capabilities");
const supported = caps.exchanges.filter(x => x.marketStats.state === "supported");
check(supported.length === 1 && supported[0].exchange === "hyperliquid", "scope leaked");
check(supported[0].marketStats.upstreamMode === "sharedPolling", "wrong acquisition claim");
const catalog = await request("/v1/fetchMarkets", { exchange: "hyperliquid", includeInactive: true });
const btc = catalog.markets.find(x => x.type === "perp" && x.exchangeMarketId === "BTC");
const spot = catalog.markets.find(x => x.type === "spot" && x.marketId);
check(btc?.marketId && spot?.marketId, "missing authoritative catalog IDs");
check(btc.settle === "USDC" && btc.settlementAssetId === "0x6d1e7cde53ba9467b783cb7c530ce054", "unexpected primary settlement mapping");
const all = await request("/v1/fetchMarketStats", { exchange: "hyperliquid" });
check(all.coverage.enumerationComplete && all.coverage.expectedMarkets === all.markets.length, "incomplete live coverage");
check(all.markets.every(x => x.type === "perp" && x.active && x.dex === ""), "all-market scope leaked");
const chosen = await request("/v1/fetchMarketStats", { marketIds: [btc.marketId, spot.marketId], fields: ["funding", "openInterest"] });
const row = chosen.markets.find(x => x.marketId === btc.marketId);
check(row?.fields.funding.state === "available", "BTC current funding unavailable");
check(row.fields.funding.value.kind === "currentUnclassified" && row.fields.funding.value.rateIntervalMs === null, "unqualified funding basis");
check(chosen.markets.find(x => x.marketId === spot.marketId)?.fields.funding.state === "notApplicable", "spot treated as zero funding");
check(row.fields.openInterest.state === "unsupported", "unqualified OI advertised");
await request("/v1/fetchMarketStats", { exchange: "binance" }, 501);
await request("/v1/fetchMarketStats", { marketIds: [] }, 400);
const topic = { channel: "marketstats", exchange: "hyperliquid", marketIds: [btc.marketId], fields: ["funding"] };
await new Promise((resolve, reject) => {
  const ws = new WebSocket(base.replace(/^http/, "ws") + "/v1/ws");
  let stage = 0, generation, revision, firstReceipt, finished = false;
  const timer = setTimeout(() => finish(new Error("no ordered snapshot/delta within 75 seconds")), 75000);
  function finish(error) {
    if (finished) return;
    finished = true; clearTimeout(timer); ws.close(); error ? reject(error) : resolve();
  }
  ws.onopen = () => ws.send(JSON.stringify({ op: "subscribe", ...topic }));
  ws.onerror = () => finish(new Error("websocket error"));
  ws.onclose = () => { if (!finished) finish(new Error("websocket closed before completing smoke")); };
  ws.onmessage = event => {
    try {
      const message = JSON.parse(event.data);
      check(message.type !== "error", JSON.stringify(message));
      if (stage === 0) {
        check(message.type === "subscribed", "data arrived before acknowledgement"); stage = 1;
      } else if (stage === 1) {
        check(message.type === "marketstats" && message.mode === "snapshot" && message.revision === 1, "missing initial snapshot");
        check(message.markets.length === 1 && message.markets[0].marketId === btc.marketId, "selected projection mismatch");
        generation = message.generation; revision = message.revision;
        firstReceipt = message.markets[0].fields.funding.receivedTimestamp; stage = 2;
      } else if (stage === 2) {
        check(message.mode === "delta" && message.generation === generation && message.previousRevision === revision && message.revision === revision + 1, "broken revision chain");
        revision = message.revision;
        const funding = message.updates.find(x => x.marketId === btc.marketId)?.fields.funding;
        if (funding?.state === "available" && funding.receivedTimestamp > firstReceipt) {
          ws.send(JSON.stringify({ op: "unsubscribe", ...topic })); stage = 3;
        }
      } else if (message.type === "unsubscribed") { finish(); }
    } catch (error) { finish(error); }
  };
});
console.log(`PASS Hyperliquid: ${all.markets.length} primary perps; BTC funding ${row.fields.funding.value.rate}; ordered WS snapshot/delta/unsubscribe`);
```

Also run the existing public-route smoke against the same supervised server:

```text
python scripts/smoke_endpoints.py --base-url http://127.0.0.1:8788 --exchange hyperliquid --markets-exchange hyperliquid
```

Stop smoke server after collecting results. Check Hyperliquid only after deterministic scenarios and live REST/WS/public-route smoke pass; otherwise record exact failed gate here and leave it open. If public access is unavailable, finish deterministic mock-backed work and record live failure, not another source/predictions/private data/fabricated rates. No browser, screenshots, frontend, private payment history, multi-exchange live acquisition, or automatic scope expansion.

### Retained research boundary

The numbered research sections below preserve venue evidence for later one-exchange planning. They are not additional active steps. Historical descriptions of repository state and illustrative wire examples are not current capability claims. Exact active identity, transport, source, lifecycle, error, and verification contracts above supersede broader proposals below.

## 2. Deferred research: historical repository baseline

Observed source facts:

- `src/main.rs` registers the six adapters and exposes `/healthz`, `/v1/fetchTrades`, `/v1/fetchOHLCV`, `/v1/fetchOrderBook`, `/v1/fetchMarkets`, and `/v1/ws`.
- `MarketDataExchange` in `src/exchanges/traits.rs` has trades, OHLCV, order book, and markets methods. There is no ticker, funding, statistics, or capability method today.
- `src/models.rs` exposes display symbol, base, quote, market type, and limited `info` identifiers. It does not expose a reliable settlement asset or a cross-product market ID.
- `src/web.rs` has a 30-second market-catalog cache with per-key in-flight coordination. This is a useful sharing pattern, but it is not a continuously updated statistics cache.
- `src/realtime.rs`, `src/ws_shared.rs`, and the WebSocket dispatch in `src/web.rs` already provide topic sharing, reconnect behavior, and bounded client delivery. Existing trade/order-book semantics must remain unchanged.
- Hyperliquid already retrieves `metaAndAssetCtxs` while building its catalog, but discards the statistics half. Extended already retrieves `/info/markets`, but its catalog mapping discards `marketStats`. These are upstream opportunities, not existing public funding support.

| Adapter | Existing transport/product scope | Funding-planning consequence |
| --- | --- | --- |
| Binance | `binance-sdk` 40.1.0; USD-M futures | Use the installed SDK's REST operations. Its enabled product module also contains WS stream operations; a new SDK product feature is not required for funding. Spot and COIN-M are outside current scope. |
| Bybit | Direct `reqwest`; category-aware V5 integration | Use linear/inverse perpetuals for funding; retain category and distinguish dated futures, spot, and options. |
| Hyperliquid | Direct `/info`; primary-DEX perps plus spot catalog | Current catalog does not enumerate HIP-3 DEXs. Do not silently call primary-DEX results all Hyperliquid markets. |
| Aster | Direct `reqwest`; V3 perpetual futures | Use Aster V3 routes, not guessed Binance V1 routes. |
| Extended | Direct `reqwest`; Starknet perpetual markets | Reuse the bulk market response. Preserve execution model and collateral identifiers. |
| Lighter | Direct HTTP/WS; Explorer-backed numeric market catalog | Ferris ID is `lighterxyz`, although native API fields call the venue `lighter`. Enrich identity from native metadata rather than guessing from display symbols. |

Existing README statements that funding is unsupported remain correct until implementation. Do not update them to claim support on the strength of this plan.

## 3. Deferred research: upstream feasibility matrix

This table preserves upstream opportunities for later one-exchange slices, **not current Ferris capabilities or an instruction to implement them now**. Hyperliquid's active polling scope is fixed in section 1; section 10's retained qualification evidence applies to later planning where it exceeds that scope.

| Exchange/product | Low-cost all-market funding source | Selected markets | Recommended continuous source | Cheap accompanying statistics | Important limit |
| --- | --- | --- | --- | --- | --- |
| Binance USD-M perpetuals | One symbol-less `/fapi/v1/premiumIndex` | Filter shared cache; native symbol parameter also exists | One `!markPrice@arr` stream; add `!ticker@arr` for volume/last price | Mark/index; bulk ticker volumes | Current OI is per symbol, not a documented bulk snapshot. |
| Bybit linear/inverse perpetuals | `/v5/market/tickers` once per category | Filter category cache; bounded symbol WS subscriptions if needed | Shared category REST polling for all; `tickers.{symbol}` for bounded watchlists | Mark/index, volume, OI | No documented wildcard ticker WS; do not subscribe every market merely because subscriptions can be multiplexed. |
| Hyperliquid primary perps | One `metaAndAssetCtxs` request | Filter shared primary cache; selected spot funding is not applicable | **Active: one shared 30-second primary REST poll; prior native-stream recommendation superseded** | Active mark/oracle; volume/OI explicitly unsupported pending unit qualification | Primary DEX only; pair complete validated metadata/context arrays before filtering. HIP-3/native streams are deferred. |
| Aster perpetuals | One symbol-less `/fapi/v3/premiumIndex` | Filter cache; native symbol parameter also exists | `!markPrice@arr` plus optional `!ticker@arr` | Mark/index; bulk ticker volumes | No current OI source established in the official V3 documentation inspected. |
| Extended perpetuals | One `/api/v1/info/markets` response | Same route with repeated `market` parameters, or filter cache | Shared bulk REST polling initially | Mark/index/last, base/collateral volume and OI | Official SDK has a bulk funding-stream candidate; its behavior needs qualification. REST `nextFundingRate` is a timestamp, not another rate. |
| Lighter perpetuals | Shared WS `market_stats/all` | Filter shared cache; `market_stats/{id}` exists | One native bulk statistics subscription | Mark/index/last and base/quote volumes; OI unit needs confirmation | `/funding-rates` is cross-venue data, not an interchangeable native live feed. |

Funding on spot, options, and ordinary dated futures is **not applicable**, not a zero-valued funding rate. The funding endpoint should default to active perpetuals. A selected non-perpetual market should return explicit applicability rather than silently disappear.

## 4. Deferred research: exchange-specific acquisition recipes

### 4.1 Binance

**Sources:** installed SDK and published generated source [B1], [B2]; official market-stream documentation [B3].

1. Join statistics to `exchange_information()` by native `symbol`. Retain `contractType`, `baseAsset`, `quoteAsset`, and `marginAsset`; only `PERPETUAL` rows enter the default funding screen.
2. Bootstrap with `GET /fapi/v1/premiumIndex` without `symbol`. Fields: `lastFundingRate`, `markPrice`, `indexPrice`, `nextFundingTime`, and `time`. The SDK response supports both a single object and an array; handle both deliberately.
3. Read `GET /fapi/v1/fundingInfo` for adjusted intervals/caps/floors. This is an exceptions list, not a complete catalog of every market's schedule. A missing row is not proof of an immutable eight-hour interval.
4. Maintain one bulk mark-price stream, normally at its three-second cadence. Fields `r`, `p`, `i`, `T`, and `E` correspond to rate, mark, index, next funding time, and event time. Preserve each market's data; an incoming array is not necessarily a complete universe. The installed SDK explicitly notes separate messages for TradFi symbols.
5. If requested, bootstrap volume/last price from symbol-less `/fapi/v1/ticker/24hr` and maintain `!ticker@arr`. Ticker arrays contain changed markets; absence is not deletion.

**Exact SDK operations present in 40.1.0:**

| Operation | SDK method/input |
| --- | --- |
| Premium index / mark and funding | `mark_price(MarkPriceParams)` |
| Funding configuration | `get_funding_rate_info()` |
| Funding-rate history | `get_funding_rate_history(GetFundingRateHistoryParams)` |
| 24-hour ticker | `ticker24hr_price_change_statistics(Ticker24hrPriceChangeStatisticsParams)` |
| Current OI | `open_interest(OpenInterestParams)` |
| Bulk mark/funding WS | `mark_price_stream_for_all_market(MarkPriceStreamForAllMarketParams)` |
| Selected mark/funding WS | `mark_price_stream(MarkPriceStreamParams)` |
| Bulk full ticker WS | `all_market_tickers_streams(AllMarketTickersStreamsParams)` |

The product feature includes `rest_api`, `websocket_api`, and `websocket_streams`; it is not REST-only. Prefer the existing SDK for REST and the repository's established WS transport/reconnect pattern for streaming. The generated WS client is an available alternative, not a reason to add another dependency or a second connection-management convention. Confirm current stream URL routing before implementation; generated base URLs and a documented stream name alone do not prove the deployed path.

**Meaning and units:** rates are decimal fractions. `lastFundingRate` must not be labeled settled merely because its name starts with `last`; nor should `r` be promised as a guaranteed next payment. Qualify the REST/WS current-rate meaning before setting `kind=estimate`; otherwise expose an honest unclassified-current limitation. `estimatedSettlePrice` is not a funding estimate. Prices are quote-denominated; 24-hour `volume` and `quoteVolume` retain native base and quote units, including scaled contract symbols. History is distinct from current estimates.

**Cost/operations:** installed SDK documentation gives premium-index weight 1 with a symbol and 10 without; ticker weight 1 with a symbol and 40 without. Funding configuration/history share a 500-request/5-minute/IP bucket; configuration has weight 0 in the ordinary weight accounting, not unlimited usage. Use bulk bootstrap/recovery, not frequent all-ticker REST polling. Respect stream lifecycle/connection limits and reconnect without creating one connection per client.

**Do not include by default:** all-market OI. `/fapi/v1/openInterest` requires a symbol, and its quantity needs a contract-unit mapping before cross-venue comparison. A capped, slow selected-market OI option is reasonable; funding must not wait for it.

### 4.2 Bybit

**Sources:** V5 ticker REST [Y1], ticker WS [Y2], instruments [Y3], funding explanation [Y4], units/history/connection documentation [Y5]-[Y9].

- Fetch `/v5/market/tickers?category=linear` and, when the scope includes it, `category=inverse`. Omit `symbol` for the whole category. The ticker response is not paginated.
- Join using `(category, symbol)`. Instrument metadata supplies `contractType`, `settleCoin`, and `fundingInterval` in **minutes**. Instruments are paginated: default 500, maximum 1,000, continue through `nextPageCursor`; do not assume one page covers linear contracts. Spot instrument pagination is a different contract.
- Tickers provide `fundingRate`, `nextFundingTime`, `fundingIntervalHour`, mark/index/last prices, volumes, and OI. `fundingIntervalHour` is in **hours**; do not confuse it with instrument `fundingInterval`. Prefer fresh valid per-market interval information and treat conflicts with cached metadata as a refresh/qualification problem.
- Bybit explicitly describes its current funding rate as changing until the upcoming funding time. Classify this as an estimate, not settled. The API rate is a decimal fraction; `nextFundingTime` is a millisecond timestamp encoded as a string. REST envelope `time` and WS `ts` are server timestamps, not funding payment timestamps.
- Intervals can change dynamically, including switching to hourly settlement after a cap/floor is reached. Updating the interval and next-payment time is a meaningful change even if the numeric rate stays equal. Pre-market phases have distinct rules; use product/status metadata, not a universal eight-hour default.

**Realtime decision:** one shared category poller is the default for exchange-wide screens. For watchlists that genuinely need faster updates, multiplex `tickers.{symbol}` subscriptions over shared category sockets. Derivatives push at 100 ms and use snapshot/delta semantics: an omitted field means unchanged. Spot/options tickers are snapshot-only. Empty funding strings for dated futures are not zero. Rebuild state after reconnect; do not apply a delta without a baseline.

**Units:** linear volume is base quantity and turnover is quote quantity; inverse volume is USD contract/quote quantity and turnover is the settlement/base coin quantity. Do not blindly map the same upstream field to the same normalized volume field across categories. The OI documentation specifies linear size in base units and inverse size in USD. Current docs distinguish both-side `openInterest` from `singleOpenInterest`; retain that counting convention. `openInterestValue` is not uniformly USD, especially for inverse products. Only expose a notional mapping when the asset and counting basis are established.

**Limits:** use public category sockets; a subscription args array has a 21,000-character limit. Spot has a ten-argument limit per subscribe request; do not turn that into an invented ten-market perpetual cap. Use the documented heartbeat and HTTP/IP limit behavior. The proposed ten-second two-category poll costs 12 REST calls/minute, independent of the number of rows or viewers. Do not interpret the upstream ceiling as a target utilization.

### 4.3 Hyperliquid research (active polling specification supersedes native streaming)

**Sources:** perpetual/spot info [H1], [H2], subscriptions [H3], rate limits [H4], funding [H5], general info and identifiers [H6], [H7].

- `POST /info` with `{"type":"metaAndAssetCtxs"}` returns metadata and the context array for the primary perpetual DEX. Join context position to the exact corresponding `universe` position **before** filtering inactive markets or sorting rows.
- Relevant context fields are `funding`, `markPx`, `oraclePx`, `openInterest`, `dayNtlVlm`, `prevDayPx`, and optional fields such as `midPx`/`dayBaseVlm`. `oraclePx` is the index/oracle reference; do not substitute a mid price when it is absent.
- Deferred native-WS evidence: `{"method":"subscribe","subscription":{"type":"allDexsAssetCtxs"}}` has shape `ctxs: Array<[dexName, Array<PerpsAssetCtx>]>`, not independently identifying ticker rows. The prior recommendation to use it now is superseded: **do not implement it in the active slice; use one shared 30-second primary REST poll**.
- `activeAssetCtx` is an alternative for a selected coin and also supports spot contexts. `allMids` is not a funding/statistics substitute.
- Native contexts lack exchange event timestamps and next-payment timestamps. Emit a real Ferris receipt timestamp and `exchangeTimestamp: null`. Do not manufacture exchange time from the HTTP request time.

**Scope boundary:** the historical adapter builds the primary-DEX catalog and does not call `perpDexs`. Full HIP-3 support would require `{"type":"perpDexs"}`, metadata per DEX using `dex`, and preserved DEX-qualified coin identities. The DEX list starts with a null primary entry; builder asset identifiers have their own indexed scheme. Existing symbol sanitization and partial `params.dex` handling are not an adequate general HIP-3 identity layer. Keep this deferred extension separate from unrelated trade/book routes. Do not implement an all-DEX stream even with primary filtering in the active slice.

**Funding semantics evidence:** the trading docs describe an eight-hour formula rate and hourly payments at one-eighth of that formula. That does not prove whether API `funding` is formula or already-scaled hourly value. The approved active decision is `currentUnclassified`, rateIntervalMs null, paymentIntervalMs 3600000, reason `rate-basis-unverified`; this explicit limitation is not a blocker requiring speculative conversion. Positive means longs pay shorts. Oracle-based payment amounts are outside scope. A verified rate basis is a later qualification, not permission to divide now.

`predictedFundings` contains cross-venue predictions for primary-DEX coins. Do not substitute it for native contexts or borrow its timing as if it were a native all-market schedule. Initially leave a missing native next-payment timestamp null rather than deriving it from an unqualified local clock.

**Units and settlement:** resolve `collateralToken` through token metadata; the display quote is not sufficient proof of settlement. `dayNtlVlm`/`openInterest` have insufficient unit prose in the inspected context schema for an unqualified universal mapping. Expected native notional/base interpretations must be confirmed, especially before enabling additional DEX collateral types. A numeric field being present does not justify labeling it USD.

**Cost:** `metaAndAssetCtxs`, `perpDexs`, and `spotMetaAndAssetCtxs` fall under the documented 20-weight general info class, against 1,200 weighted units/minute/IP. **Active acquisition is a primary-only 30-second poll (40 weight/minute), not a fallback**, with spotMeta cached for five minutes. DEX enumeration, per-DEX acquisition, and the ten-connection/IP native-WS limit are retained future evidence only; the active slice does not acquire these sources.

Deferred spot price/statistics sources include `spotMetaAndAssetCtxs` and `activeAssetCtx`. Do not implement them now: explicitly selected spot funding is not applicable and other spot statistics fields are unsupported.

### 4.4 Aster

**Sources:** official recommended V3 futures API [A1] and stream documentation [A2].

- Catalog: `/fapi/v3/exchangeInfo`, retaining native symbol, `contractType`, `quoteAsset`, and `marginAsset`. Current Ferris catalog filters perpetuals.
- Current bulk funding/mark/index: `/fapi/v3/premiumIndex` without `symbol`, weight 1. Fields follow the mark-price shape: `lastFundingRate`, `markPrice`, `indexPrice`, `nextFundingTime`, `time`.
- Bulk funding configuration: `/fapi/v3/fundingInfo`, weight 1, optional symbol. Rows include `fundingIntervalHours`, `fundingFeeCap`, `fundingFeeFloor`, `interestRate`, and `time`. Examples include different intervals; do not copy a single interval across the exchange.
- Streaming: `!markPrice@arr` at three seconds, or its one-second variant if justified. Selected stream names are `<symbol>@markPrice`; use lowercase in stream names and native payload symbols for joins.
- Optional volume/last price: `/fapi/v3/ticker/24hr` without a symbol for bootstrap, weight 40; then `!ticker@arr` at one second, which contains changed markets. The one-symbol REST ticker is weight 1.

Reuse the direct HTTP/JSON and WS transport conventions already used by this adapter. Similar JSON to Binance is not evidence of identical routes, limits, or semantics.

**Meaning:** the V3 comment calls `lastFundingRate` the latest funding rate, and WS `r` the funding rate. Neither alone settles forecast-versus-final classification. Qualify that mapping rather than treating the field name as a guarantee. Keep `fundingIntervalHours` separate from interest-rate components. Funding history contains historical rate/time records, not account payment amounts; confirm final/applied-rate semantics before labeling those records settled.

**Units/identity:** retain native base/quote units for `volume`/`quoteVolume`; the ticker is explicitly a rolling 24-hour window. Preserve USDT, USD1, and U variants and the authoritative settlement asset. Do not assume all quote/settlement assets are USDT.

**Operations:** documented stream constraints include 24-hour lifetime, 200 streams per connection, ten incoming client messages/second, and ping/pong requirements. Read runtime rate-limit metadata/headers and back off on throttling. One mark stream and one ticker stream avoid both connection multiplication and repeated expensive ticker snapshots.

**OI:** no current open-interest endpoint/field was established in the official V3 corpus inspected. This is a documentation/support gap, not proof that the exchange can never provide OI. Mark this metric unsupported in the initial Ferris bulk capability; do not guess a legacy route or return zero.

### 4.5 Extended

**Sources:** official API [E1], funding rules [E2], and the official Starknet SDK's stream client/models [E3]-[E5].

**Lowest-risk path:** reuse `GET /api/v1/info/markets` as the shared all-market statistics source. With no market parameter it returns the catalog plus `marketStats`; selected markets can use repeated `market=BTC-USD&market=ETH-USD`. Do not call `/info/markets/{market}/stats` hundreds of times.

Available fields:

- `fundingRate`: current rate, calculated every minute.
- `nextFundingRate`: documented as the timestamp of the next funding update. Despite its name, it is not a numeric next-period rate.
- `markPrice`, `indexPrice`, `lastPrice`.
- `dailyVolume` in collateral units and `dailyVolumeBase` in base units, for the previous 24 hours.
- `openInterest` in collateral units and `openInterestBase` in base units.

There is no general exchange snapshot timestamp in `marketStats`. Record receipt time without inventing an exchange time. `nextFundingRate` is not a replacement for that timestamp.

**Funding basis:** funding is paid hourly; public history explicitly records the one-hour rates applied to funding fees. The formula's eight-hour realization period is not a reason to divide an already-hourly API value by eight again. Order-book, RFQ, crypto, and RWA funding methodologies differ. Qualify the current API value against the applicable hourly rate for these product classes before declaring the interval mapping universal. Current calculated funding is distinct from a finalized history record.

**Native stream opportunity, with stronger evidence than a guessed route:** the official SDK implements `StreamClient.subscribe_to_funding_rates(market_name=None)` using `/funding/<market?>` under the configured stream base. This establishes SDK support for `/funding` and `/funding/{market}`. `FundingRateModel` carries market/rate/time aliases `m`, `f`, `T`; its wrapper has `ts` and `seq`. The current REST documentation page inspected does not explain this stream's funding semantics. Qualify whether the no-market stream initializes every market, whether `f` is current or hourly-applied, what `T` means, and the ordering/reconnect behavior before relying on it. Use bulk REST meanwhile; even a qualified funding stream would not refresh the other statistics fields by itself.

**Time/authentication gates:** examples mix ten- and thirteen-digit timestamps even where the table says milliseconds. `nextFundingRate` is described as next update, not unequivocally next payment. Confirm its unit/meaning before mapping `nextPaymentTimestamp`. Keep the mandatory `User-Agent`. Existing Ferris public info requests are unauthenticated, while the current documentation introduction broadly describes API-key access for read-only operations; verify deployment requirements rather than introducing wallet signing or assuming the stream needs no header.

**Identity:** use market `type` to exclude spot funding, and preserve `isRfq`/`isOffHours`. `collateralAssetName: USD` is an upstream denomination, not proof of a particular token. Preserve the native collateral identifier and resolve the settlement asset. RFQ bid/ask statistics refer to the indicative book, so do not casually advertise them as executable BBO.

A 15-second shared bulk poll is four calls/minute, well below the documented default 1,000 requests/minute/IP before considering other traffic. The static catalog and dynamic statistics extracted from the same response should not require duplicate pollers.

### 4.6 Lighter (`lighterxyz` in Ferris)

**Sources:** official WS [L1], funding-rate REST warning [L2], funding history [L3], metadata [L4], limits [L5], and funding/cadence rules [L6], [L7].

- Use `wss://mainnet.zklighter.elliot.ai/stream` with `{"type":"subscribe","channel":"market_stats/all"}` for native perpetual statistics. Selected channels use `market_stats/{MARKET_INDEX}`.
- Native WS explicitly distinguishes `current_funding_rate` (upcoming-payment estimate) from `funding_rate` (last payment), with `funding_timestamp` identifying the latter. Preserve both; do not let one overwrite the other.
- The message timestamp is distinct from that settlement timestamp. Native documentation examples show millisecond WS times, while history examples require separate qualification.
- There is no documented next-payment field. Hourly payments are documented, but contract specifications say the period is configurable for future markets. Do not use `funding_timestamp` as the next-payment time or assume every future market must remain hourly.
- A REST request to Ferris can start or join the single shared WS acquisition and await its baseline with a bounded deadline. That is still one upstream source, not one connection per HTTP caller. A cold/failed source must return explicit warming/unavailable coverage, not an invented rate.

**Do not use `/api/v1/funding-rates` as the primary live source.** Its official description says it returns rates across venues and directs realtime consumers to `market_stats`. Rows contain `exchange` values including Binance, Bybit, Hyperliquid, and Lighter. Even filtering to `exchange=lighter` does not establish the same freshness, interval, or estimate/settled semantics as the native WS fields.

**Metadata:** `/api/v1/orderBookDetails?filter=perp` is the native bulk metadata/statistics route; `filter=all|spot|perp` and optional `market_id` are documented. Use native market IDs, type, status, asset IDs, and precision to enrich/reconcile the current Explorer catalog. Do not treat the parameter's example/default ID as an all-market limit. Resolve settlement using authoritative asset metadata; the current display `BASE/USD` is not proof of a settlement token. Spot has a separate `spot_market_stats/all` channel and no funding fields.

**Critical unit gate:** the WS docs label `premium` as percentage and show percent-like clamp/interest examples, but do not explicitly specify the wire scaling of both funding-rate strings. Do not infer decimal-versus-percent from magnitude or copy the REST aggregator's conversion rule. Establish the correct conversion for the native fields before exposing normalized funding. Confirm history `rate` versus `value`, the role of `direction`, and seconds versus milliseconds independently.

`daily_base_token_volume` and `daily_quote_token_volume` have explicit asset units. Preserve their window definition rather than inventing UTC-day or rolling boundaries. `open_interest` is present, but its denomination/counting basis was not established by the inspected schema. Do not label it base or USD without additional evidence. Mark/index/last prices are available; native metadata and bulk REST can support their fallback even when funding WS is unavailable.

**Operations:** standard REST allowance is 60 requests/minute; Explorer has a separate 90-weight/minute limit. WS limits include 500 subscriptions/connection, 200 client messages/minute, and a frame at least every two minutes for keepalive. One bulk subscription is preferable to hundreds of selected channels. Bulk first-message completeness, update sparsity, and reconnect behavior must be captured during qualification; an example of one market is not proof of complete coverage.

## 5. Deferred research: broader normalized-contract proposals

Historical proposals/examples below are not the active wire contract. Section 1 fixes the exact ID encoding, typed values, bounds, nullability, scope, and error policy; history/other-venue examples remain deferred:

- `POST /v1/fetchMarketStats`: exchange-wide or selected-market snapshot.
- `GET /v1/capabilities`: implemented support by exchange/product/metric.
- `GET /v1/ws`, channel `marketstats`: snapshot and subsequent updates.
- `POST /v1/fetchFundingRateHistory`: optional separate selected-market historical-rate query.

Keep existing exchange IDs, camelCase JSON, and the existing `params.category`/`params.dex` conventions where applicable. Do not add both ticker and funding endpoints with independent caches for the same data.

### 5.1 Identity and selection

Add a catalog-produced opaque `marketId` and the necessary identity metadata to `/v1/fetchMarkets`. Reuse that identity in statistics. The internal key must include exchange, product/category, native contract ID, and DEX where relevant. Preserve punctuation and native IDs; a concatenated display symbol is not a safe key.

Identity metadata should include:

- `exchange`, `marketId`, `exchangeMarketId`, and display `symbol`.
- `type`, native `contractType`, `category`, and nullable `dex`.
- `base`, `quote`, `settle`, and native settlement asset identifier where needed.
- Activity/status and contract multiplier information needed to interpret native quantities.

Unknown settlement must remain explicitly unresolved. Do not silently make `settle = quote`. Dated contracts and identical base/quote pairs in different products must remain distinct. Existing display symbols can remain unchanged; frontend statistics joins should migrate to `marketId`.

Proposed all-market request; omitted `marketIds` means all active perpetuals in the supported scope, not every product offered by the upstream exchange:

```json
{
  "exchange": "bybit",
  "params": { "category": "linear" },
  "fields": ["funding", "markPrice", "indexPrice", "volume24h", "openInterest"]
}
```

Selected-market form; a one-element list is also the individual-widget API:

```json
{
  "exchange": "bybit",
  "marketIds": ["bybit:linear:BTCUSDT"],
  "fields": ["funding"]
}
```

The example ID is illustrative and must be obtained from the catalog, not constructed by the frontend. Reject an empty ID list instead of interpreting it as all markets. Deduplicate IDs, validate exchange/scope conflicts, and return a clear error for an unknown ID. Advertise a selected-list size limit. Group requests by upstream category/DEX and project the shared result locally.

A snapshot should include capture `timestamp`, canonical scope, rows, and coverage: expected markets, returned markets, whether enumeration is complete, and source failures. Completeness of market enumeration is separate from availability of every metric. Partial upstream failure must not look like a complete smaller exchange.

### 5.2 Funding and metric values

Use a typed field value with common availability/provenance metadata, rather than exchange-specific `info.*` lookups. Recommended field names are `funding`, optional `lastSettledFunding`, `markPrice`, `indexPrice`, `lastPrice`, `volume24h`, and `openInterest`.

For funding:

| Field | Meaning |
| --- | --- |
| `rate` | Signed decimal fraction, preferably encoded as a decimal string for exact transport; `"0.0001"` means 0.01%, and `"0"` is valid. No NaN/infinity or silent parse-to-zero. |
| `kind` | `estimate`, `settled`, or explicitly `currentUnclassified` while upstream meaning is not established. Never silently substitute one kind for another. |
| `rateIntervalMs` | Period to which this numeric rate applies. Nullable only with an explicit unverified/unknown-basis limitation. Not the feed's update cadence. |
| `paymentIntervalMs` | Payment schedule interval where known; separate from formula realization period and observation cadence. |
| `paymentTimestamp` | Actual funding event time for a settled observation; otherwise null. |
| `nextPaymentTimestamp` | Next scheduled payment where genuinely established; otherwise null. Not the same as next rate recalculation. |

Document the sign convention: positive means longs pay shorts. A settled rate is a market funding event, not the amount paid by a particular account. Do not perform annualization or normalize every venue to eight hours in the backend's base contract. If a later consumer derives a comparable period, it needs verified intervals and must label the result derived.

A proposed single-field row, with invented values solely to illustrate the contract:

```json
{
  "marketId": "bybit:linear:BTCUSDT",
  "fields": {
    "funding": {
      "state": "available",
      "value": {
        "rate": "0.0001",
        "kind": "estimate",
        "rateIntervalMs": 28800000,
        "paymentIntervalMs": 28800000,
        "paymentTimestamp": null,
        "nextPaymentTimestamp": 1789056000000
      },
      "exchangeTimestamp": 1789050000000,
      "receivedTimestamp": 1789050000123,
      "source": "bybit:linear:tickers"
    }
  }
}
```

`currentUnclassified` is an honest capability limitation, not a license to advertise full estimate/settled support. An unverified wire unit is stronger: do not put a raw percent-like number into normalized `rate` at all. Resolve the unit gate before enabling that normalized field; raw payloads may remain diagnostic, not something widgets must interpret.

For other values, define units in the typed payload/catalog contract:

- **Prices:** amount plus quote/denominator assets or equivalent explicit price-unit metadata. Preserve scaled native base units. Oracle/index and mark remain different fields.
- **Volume:** base and quote amounts with asset identifiers and the window definition. Distinguish rolling 24 hours, calendar day, and exchange-defined daily windows; include upstream window timestamps where supplied.
- **OI:** amount, denomination (`base`, `quote`, `settlement`, or `contracts`), asset, and counting convention (`oneSide`, `bothSides`, or `unknown`). Contract quantities require a verified multiplier before conversion. A single universal USD OI scalar is not justified by the available schemas.
- Derived notional/annualized figures are not required. If later added, label the derivation and price/time inputs; do not replace the original measurement.

### 5.3 Availability and timestamps

Every requested field has a state. Unrequested fields may be omitted.

| State | Value | Meaning |
| --- | --- | --- |
| `available` | Present, including zero | Implemented, applicable, and current under its source policy. |
| `notApplicable` | Null | Product has no such metric, e.g. spot funding. |
| `unsupported` | Null | Not implemented/exposed for this scope, no established source, or normalization not yet qualified. Supply a reason. |
| `unavailable` | Null | Implemented and applicable, but no usable observation exists right now. |
| `stale` | Retained last known value | Previously usable but freshness/continuity is no longer established. Keep original observation timestamps. |

Use stable reasons such as adapter-not-implemented, no-bulk-source, semantics-unverified, warming, upstream-failure, and invalid-upstream-value. A temporary upstream failure must not switch capability support off.

- `exchangeTimestamp` is nullable and represents the upstream observation/event time when provided.
- `receivedTimestamp` is when Ferris received that observation, not when the client fetched the cache.
- The response's capture/send timestamp is separate; cache reads must not make old funding look newly received.
- Funding event time and schedule time belong inside the funding value and must not overwrite observation timestamps.
- Retain native/derived provenance for any derived interval or schedule. Initial implementation should prefer null to speculative derivation.

## 6. Deferred research: broader acquisition, caching, and cadence

### Ownership

Introduce a statistics coordinator separate from event/order-book topic state. Reuse existing async primitives and transport conventions; do not force a snapshot of 800 markets through one existing single-symbol trades topic.

The acquisition key is the upstream scope: exchange, deployment/base URL, product/category, and DEX as appropriate. Client filters, field selection, and watchlist order are projections, not new upstream resources.

Required behavior:

1. REST and WS consumers read the same latest-value cache.
2. Concurrent cold requests join one bootstrap; repeat requests do not force refreshes beyond the source policy.
3. All-market and selected views overlap without duplicate collectors. Bybit selected WS demand is unioned and reference-counted by native market/category.
4. Decode and normalize each upstream payload once, retain latest fields, and track changed markets. Share immutable output for identical projections where useful; keep locks out of network awaits and serialization.
5. Demand-driven collectors may stop after a short idle grace period. HTTP-only Lighter consumers need a demand lease so successive requests do not reopen a socket each time.
6. Catalog metadata and dynamic statistics have different lifetimes. When an upstream response contains both, as with Extended, share its acquisition rather than running redundant catalog/statistics pollers.
7. Bound subscription count, selection size, queued snapshots, and per-client output. Account for all Ferris requests/connections using the same upstream IP, not just statistics traffic.

No database, Redis, or multi-instance coordination is necessary for the existing process-local architecture. Do not promise deduplication across independently deployed Ferris processes.

### Starting cadence proposals

These are conservative Ferris policy proposals, not upstream SLAs. Tune during the implementation qualification step, within the existing application's total upstream budget.

| Source | Proposed policy |
| --- | --- |
| Binance/Aster mark/funding | Native three-second bulk stream; one-second mode only for demonstrated need. Bootstrap/recover through bulk REST. |
| Binance/Aster ticker | Native changed-market stream when volume/last is requested; avoid recurring high-frequency weight-40 REST snapshots. |
| Bybit exchange-wide | One poll/category every ten seconds. A funding-only product could poll more slowly. |
| Bybit selected fast updates | Bounded shared symbol ticker subscriptions; coalesce the 100 ms input rather than forwarding every packet. When capped, explicitly fall back to the shared polling mode. |
| Hyperliquid perps | **Superseded by active policy:** one shared primary-DEX REST poll every 30 seconds, spotMeta cache five minutes; no native upstream statistics WS or per-DEX fallback architecture in this slice. |
| Extended all statistics | One bulk REST poll every 15 seconds; qualify the SDK funding stream before optionally replacing the funding component. |
| Lighter perps | Native `market_stats/all`; REST can refresh supported non-funding fields, but must not silently replace authoritative funding with the cross-venue aggregator. |
| Catalog/configuration | Cache static metadata on a minutes-scale schedule. Refresh dynamic funding intervals/caps more promptly and on observed schedule changes; do not use a long catalog TTL as proof of a current interval. |

For later venues these publication rates remain research proposals, not executable policy. The active Hyperliquid contract is at most once per second after its immediate initial snapshot, with observable receipt-time freshness after each successful 30-second observation even when funding is unchanged; do not adopt a separate slower funding-only cadence here.

Freshness cannot depend only on whether the numeric value changed. Keep source continuity/last-success metadata and send compact coalesced freshness updates where necessary. A Bybit omitted field remains valid within a continuous, initialized delta stream; an unrelated mark-price update does not validate an independently polled funding/history source. A healthy connection alone is not proof that every market has been initialized.

Use roughly three missed poll periods as an initial polling stale threshold; validate native-feed expectations before choosing their thresholds. Consider upstream event age as well as receipt age. When no exchange clock exists, disclose receipt-based freshness rather than promising exchange-time precision. If an estimate's known payment time passes without a valid rollover, do not keep presenting the old estimate as upcoming. Historical settled records remain valid historical records; their age is different from the freshness of the claim that they are the latest settlement.

## 7. Deferred research: broader WebSocket proposals

Add a `marketstats` branch to the existing subscription protocol. Keep existing acknowledgement, error, unsubscribe, and connection behavior for other channels.

```json
{
  "op": "subscribe",
  "channel": "marketstats",
  "exchange": "bybit",
  "params": { "category": "linear" },
  "fields": ["funding", "markPrice", "indexPrice"]
}
```

Use the same `marketIds` selection as REST for watchlists. All-market subscriptions have explicit supported product/catalog scope; no magic `symbol="*"` passed into existing symbol resolvers.

Recommended delivery contract:

- `type: marketstats`, `mode: snapshot|delta`, canonical `topic`, opaque `generation`, and monotonic `revision`.
- A snapshot is the complete replacement for that subscription's view, including unavailable fields and coverage. It is a cache snapshot, not a claim that every upstream exchange observed every metric simultaneously.
- A delta contains changed market/field entries plus `removedMarketIds`; a new market includes its identity and initial requested fields.
- Deltas include `previousRevision`. Revisions belong to the delivered canonical topic/projection, not an upstream sequence that skips whenever an unrelated market changes.
- A field absent from a delta is unchanged. A field present with `value: null` and an explicit unavailable/not-applicable/unsupported state clears it. Replace a present field object atomically, including rate interval, kind, source, and timestamps. Do not recursively merge old interval metadata into a new funding value.
- Remove a market only when catalog/scope membership is authoritatively removed, not because it was absent from a sparse packet or a failed request.

Example explicit invalidation, again illustrative rather than observed output:

```json
{
  "type": "marketstats",
  "mode": "delta",
  "topic": { "exchange": "bybit", "params": { "category": "linear" } },
  "generation": "example-generation",
  "previousRevision": 41,
  "revision": 42,
  "updates": [
    {
      "marketId": "bybit:linear:BTCUSDT",
      "fields": {
        "funding": {
          "state": "unavailable",
          "value": null,
          "reason": "invalid-upstream-value",
          "exchangeTimestamp": 1789050001000,
          "receivedTimestamp": 1789050001123,
          "source": "bybit:linear:tickers"
        }
      }
    }
  ],
  "removedMarketIds": []
}
```

### Ordering and recovery requirements

1. Attach the subscriber and capture its initial cache/revision under one ordering fence. Queue its snapshot before any later delta. Do not implement subscribe as an unlocked snapshot read followed by registration.
2. Bootstrap upstream state using its documented snapshot semantics where available. Otherwise fence REST bootstrap and WS observations; compare exchange timestamps only when they are actually comparable. Without comparable ordering, do not let an ambiguous buffered pre-bootstrap frame overwrite a newer baseline.
3. On upstream discontinuity, invalidate continuity, retain stale values if useful, reacquire a trustworthy baseline, and publish a replacement snapshot/new generation as needed. In particular, Bybit deltas and Hyperliquid positional contexts require a known baseline.
4. On frontend reconnect, resubscribe and accept a replacement snapshot. Do not promise durable replay or persistence. A REST snapshot used for initial paint is superseded by the WS snapshot, not merged by arrival time.
5. If a delta's generation/previous revision does not match, discard it and resubscribe/resnapshot. Do not apply it optimistically.
6. Latest-value coalescing occurs before revisioned delivery: accumulate every changed field/market and publish their latest values. Dropping an already-produced sparse delta is unsafe unless its changes are merged into the replacement or a full snapshot follows.
7. For a lagging client, either replace pending statistics with a complete fresh snapshot or close the connection and require resubscription. A lag warning followed by silently skipped patches is insufficient.

None of these coalescing rules applies to trade events or the order-book delta streams required for reconstruction.

## 8. Deferred research: broader capability proposals

Upstream findings are not implemented support. The exact active capability record and its limitations are fixed in section 1; broader per-venue proposals below require later approval and qualification. Completion status is recorded only in the unchecked/checked ledger, never inferred from this research matrix.

Per exchange and product, report:

- Current funding and its supported classifications; optional latest-settled funding in snapshots separately.
- Public funding-rate history, with selected-market-only scope and pagination limits when implemented.
- Snapshot fields and whether all-market and selected-market selection are supported.
- Update mode per scope/field: native stream, shared polling, or none. Ferris WS delivery does not mean upstream native realtime.
- Coverage boundaries: Binance USD-M only, Hyperliquid supported DEX list, Bybit categories, and perpetual versus spot applicability.
- Selection/subscription limits, advertised cadence, and applicable freshness policy.

Use capability state for structural support and field state for runtime availability. An upstream timeout should leave support intact and make data unavailable/stale. A missing adapter, no bulk source, or unresolved normalization must not be disguised as a temporary network problem.

Examples of truthful target behavior:

- Binance funding all-market supported after qualification; OI all-market unsupported, while a separately enabled selected OI mode may be supported.
- Bybit exchange-wide updates delivered over Ferris WS with `sharedPolling`, while a bounded selected scope can report `nativeStream`.
- Hyperliquid primary-DEX support does not advertise HIP-3 completeness.
- Lighter spot funding is not applicable; Lighter perpetual funding is not enabled until native rate scaling is established.
- Extended can advertise polled statistics without asserting that its SDK funding-stream candidate is already qualified.

## 9. Deferred research: public funding-rate history (not in active scope)

Use a separate endpoint with one `marketId`, bounded `since`/`until`, limit, and an explicit continuation token/window. Normalize timestamps, ordering, sign, and rate interval per returned observation. Do not apply today's funding interval retroactively to old records when schedules can change.

| Exchange | Public history source | Pagination/meaning constraints |
| --- | --- | --- |
| Binance | `/fapi/v1/fundingRate`; SDK `get_funding_rate_history` | Max 1,000; time-bounded ascending records. Shares funding-info/history request bucket. Set an explicit limit: generated prose about default counts is not a stable contract to inherit. |
| Bybit | `/v5/market/funding/history` | Required category and symbol, max 200. `startTime` alone is invalid; provide `endTime` too. Move the time window with duplicate-boundary protection. Docs explicitly describe settled rates. |
| Hyperliquid | `POST /info`, type `fundingHistory` | Coin and millisecond start/end range; apply general info time-range pagination limit of 500 elements/distinct blocks. Advance without duplicates or stalled cursors. Additional response-size weight applies. |
| Aster | `/fapi/v3/fundingRate` | Optional upstream symbol, but require a selected market in Ferris. Max 1,000, ascending, inclusive boundaries. Confirm final/applied semantics. |
| Extended | `/api/v1/info/{market}/funding` | Required start/end milliseconds, descending records, cursor pagination, up to 10,000. Explicitly applied one-hour rates; example timestamp-unit conflict must be resolved. |
| Lighter | `/api/v1/fundings` | Required market ID, resolution, start/end timestamps, count_back; max 750. Use `1h` for individual funding periods rather than silently treating `1d` aggregates as settlements. Qualify time units, `rate`/`value`, and `direction`. |

Do not promise indefinite retention: the inspected sources do not establish one common retention contract. Return actual coverage and continuation information, not a fabricated complete history. An empty successful history result is different from unsupported or upstream failure. Do not use account payment history endpoints for public market rates.

The easy delivery is a bounded detail/chart query. Persistent backfills, exchange-wide historical datasets, and N history calls to populate `lastSettledFunding` for all rows are not recommended here. Populate `lastSettledFunding` from a current bulk source where natively available, as on Lighter, or advertise that it is not exposed by the snapshot integration.

## 10. One-exchange delivery procedure and deferred qualification evidence

### Active delivery order and later-session procedure

1. Execute only section 1's Hyperliquid steps 1–5: identity/transport contract; shared primary acquisition/normalization; coordinator/lifecycle; REST/capabilities; safe Ferris WS projections. Shared models are not permission to implement another adapter.
2. Run the active deterministic and live acceptance checks, recording actual commands/results and delivered behavior in this ledger. Leave failed/unrun gates unchecked. Do not claim native streaming, verified hourly rate basis, volume/OI support, history, HIP-3, frontend completion, or any deferred exchange.
3. Mark Hyperliquid complete only after all five ledger gates pass, then stop. Do not automatically begin a second venue.
4. A later session explicitly selects exactly one unchecked exchange, reads that venue's evidence/current implementation, and appends its own decision-complete active slice using the shipped shared API/coordinator. Keep every other unchecked venue deferred; extend source driving only if that selected source actually requires it.
5. Implement and verify that one venue end-to-end before checking it off. Do not rerun the initial shared foundation, enable all six together, or introduce frontend/history as implicit follow-on steps.

### Deferred evidence before enabling additional normalized fields

This table and the touchpoints/scenarios following it are retained research, not another execution order. The approved Hyperliquid unknown-basis behavior does not wait for a verified rate interval; its concrete acceptance is section 1.

| Gate | Required evidence | Safe outcome until resolved |
| --- | --- | --- |
| Binance current rate/schedule | Confirm `lastFundingRate`/WS `r` forecast/final meaning, current stream routing, and interval coverage for symbols absent from `fundingInfo`. Include separate-message market families in coverage checks. | Do not invent settled meaning or a universal eight-hour period; report the exact limitation. |
| Bybit interval/unit/category | Confirm current ticker interval behavior, contract filtering, inverse volume/OI assets, and both-side/single-side OI mapping. | Expose only qualified quantity fields; never fill futures' funding blanks with zero. |
| Hyperliquid future rate-basis/scope extensions | Establish API hourly/formula basis before assigning a verified rate interval; qualify collateral/quantity units and positional coverage before any DEX/native-stream extension. | Active slice already chooses primary-only shared polling, currentUnclassified/null rateIntervalMs, hourly paymentIntervalMs, and null next-payment/exchange timestamps. Do not block it on speculative basis conversion or add native streaming. |
| Aster rate semantics | Establish current versus finalized funding meaning and complete funding-info coverage. Verify V3 production bulk shapes. | Keep unclassified meaning explicit; no guessed OI integration. |
| Extended rate/time/stream | Verify current hourly rate mapping across execution classes, `nextFundingRate` meaning/unit, history `T` units, and current header/access requirements. Qualify SDK stream behavior separately. | Use documented bulk REST; do not label next recalculation as next payment or claim native WS coverage. |
| Lighter native scaling/history/OI | Establish decimal-versus-percent for each funding field, timestamp units per endpoint, settlement sign semantics, bulk initialization, native asset identity, and OI denomination. | No raw-number masquerading as normalized funding/OI; retain explicit unqualified capability until evidence is obtained. |

The implementation planner should use official schema/SDK evidence plus a small bounded set of public payload captures, preferably across a settlement boundary for the rate-basis questions. A matching-looking magnitude is not proof of units. These gates do not require credentials for private trading, an all-market history crawl, or changes to unrelated endpoints.

### Deferred source touchpoints for a later selected venue

| Area | Existing location / proposed responsibility |
| --- | --- |
| Transport models | `src/models.rs`: shared identity, requests, snapshots, typed fields, capabilities; preserve existing endpoint shapes unless an intentional migration is planned. |
| Adapter boundary | `src/exchanges/traits.rs`, `src/exchanges/registry.rs`: explicit statistics/history/capability boundaries following existing exchange-error responsibilities. |
| Venue mapping | Each `src/exchanges/<venue>/mod.rs`: native endpoint mapping, product identity, semantic/unit normalization. |
| Latest-value coordination | A dedicated statistics module, e.g. `src/market_stats.rs`, integrated into `AppState`; not an ever-growing addition to trade/book event logic. |
| REST/WS dispatch | `src/web.rs`, `src/main.rs`: new snapshot/capability handlers and statistics subscription branch. |
| Reusable WS transport | `src/ws_shared.rs` and established `src/realtime.rs` patterns; preserve existing reconnect, shutdown, and bounded delivery behavior. |
| Configuration | `src/config.rs`: only the cadence/resource policies actually needed; retain deployment/base-URL separation. |
| Integration guidance | `README.md`, `INTEGRATION_README.md`: actual endpoint examples, capabilities, interval/unit conventions, and resnapshot behavior once implemented. |

Before modifying exported Rust contracts, the implementation pass should inspect references and migrate every affected caller; the table is a responsibility map, not permission to add disconnected interfaces.

### Deferred cross-venue acceptance research (not additional active gates)

- An exchange-wide current-funding request is bulk/category/DEX bounded, never N per-market rate/history/OI calls. Repeating it for many clients does not multiply upstream collectors.
- A watchlist and an exchange-wide screen share the same underlying observations, with correct product/settlement identity and no collisions between spot, inverse, linear, or DEX-qualified instruments.
- Zero and negative rates survive normalization; empty/invalid fields do not become zero. Estimate and settled observations cannot overwrite each other accidentally.
- A changing interval/next-payment time is delivered even if the rate is unchanged. Any percent conversion and hourly/eight-hour mapping is defended by qualified payloads.
- Bybit delta omission keeps initialized data; explicit unavailability clears it. A replacement snapshot removes obsolete fields, and reconnect never starts from an uninitialized delta.
- Hyperliquid context joins remain correct when catalog filtering/order changes; unknown or mismatched positional data is not assigned to the wrong contract.
- Partial snapshots report incomplete coverage. A sparse update does not remove unrelated markets.
- A cache read does not refresh receipt time. Source loss, stale transition, known funding-boundary expiry, and recovery are visible without waiting for a price change.
- Initial snapshot delivery cannot race with a delta; filtered subscriptions have coherent revision chains. A slow client resynchronizes instead of silently losing field changes.
- Existing trade events, order-book reconstruction, and OHLCV behavior remain unchanged. Run repository formatting/tests once after integrated changes and exercise the new endpoints/socket path with a bounded smoke workflow.

## 11. Deferred research: evidence and historical verification boundary

The original research was grounded in repository source, installed Binance 40.1.0 generated modules, official exchange documentation, and the official Extended SDK. It did not implement or live-test proposed Ferris endpoints, collectors, or funding streams. Documentation/SDK presence proves acquisition opportunity, not production freshness or correct conversion. Section 1 now records the approved active slice and its explicit completion gates; this retained reference corpus does not claim those gates have passed.

No new SDK is required merely to obtain these public statistics. Binance already has the relevant product modules; the other adapters can follow their existing direct HTTP/WS approach. Official Python SDKs are useful protocol evidence, not a recommendation to add a Python sidecar to this Rust service.

### Primary references

Binance:

- [B1] Installed/published `binance-sdk` 40.1.0 USD-M REST operations, models, and weight documentation.
- [B2] Same version's USD-M WS operations and response models.
- [B3] Official USD-M stream documentation; check current routing during implementation.

Bybit:

- [Y1] Bulk tickers; [Y2] ticker snapshots/deltas; [Y3] instruments, interval and settlement metadata.
- [Y4] Current rate calculation and dynamic funding intervals.
- [Y5] Public settled funding history; [Y6] OI denomination/counting; [Y7] volume/turnover units.
- [Y8] WS topics, args limits and keepalive; [Y9] HTTP/WS rate limits.

Hyperliquid:

- [H1] Perpetual metadata, contexts and funding requests; [H2] spot metadata/contexts.
- [H3] `activeAssetCtx`/`allDexsAssetCtxs` and exact context schemas; [H4] request/connection limits.
- [H5] Funding formula versus payment cadence; [H6] general info pagination; [H7] native asset/DEX identifiers.

Aster:

- [A1] Official recommended V3 futures specification: bulk premium index, funding info/history, ticker, identity and weights.
- [A2] Official market streams and connection constraints.

Extended:

- [E1] Bulk markets/statistics, public applied-rate history, units and API requirements.
- [E2] Funding payment cadence and execution-model-specific calculations.
- [E3] Official SDK stream methods; [E4] funding model; [E5] stream envelope.

Lighter:

- [L1] Native bulk/selected statistics, current-versus-last funding semantics and keepalive.
- [L2] Explicit cross-venue funding REST warning; [L3] funding-history schema and limits.
- [L4] Native market metadata/statistics; [L5] REST/Explorer/WS limits.
- [L6] Funding formulas/hourly payments; [L7] configurable contract funding period.

[B1]: https://docs.rs/crate/binance-sdk/40.1.0/source/src/derivatives_trading_usds_futures/rest_api/mod.rs
[B2]: https://docs.rs/crate/binance-sdk/40.1.0/source/src/derivatives_trading_usds_futures/websocket_streams/mod.rs
[B3]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
[Y1]: https://bybit-exchange.github.io/docs/v5/market/tickers
[Y2]: https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
[Y3]: https://bybit-exchange.github.io/docs/v5/market/instrument
[Y4]: https://www.bybit.com/en/help-center/article/Introduction-to-Funding-Rate
[Y5]: https://bybit-exchange.github.io/docs/v5/market/history-fund-rate
[Y6]: https://bybit-exchange.github.io/docs/v5/market/open-interest
[Y7]: https://bybit-exchange.github.io/docs/v5/market/kline
[Y8]: https://bybit-exchange.github.io/docs/v5/ws/connect
[Y9]: https://bybit-exchange.github.io/docs/v5/rate-limit
[H1]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals.md
[H2]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot.md
[H3]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions.md
[H4]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits.md
[H5]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding.md
[H6]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint.md
[H7]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids.md
[A1]: https://raw.githubusercontent.com/asterdex/api-docs/master/V3(Recommended)/EN/aster-finance-futures-api-v3.md
[A2]: https://asterdex.github.io/aster-api-website/futures/websocket-market-streams/
[E1]: https://api.docs.extended.exchange/
[E2]: https://docs.extended.exchange/extended-resources/trading/funding-payments.md
[E3]: https://raw.githubusercontent.com/x10xchange/python_sdk/starknet/x10/clients/stream/stream_client.py
[E4]: https://raw.githubusercontent.com/x10xchange/python_sdk/starknet/x10/models/funding_rate.py
[E5]: https://raw.githubusercontent.com/x10xchange/python_sdk/starknet/x10/models/http.py
[L1]: https://apidocs.lighter.xyz/docs/websocket-reference.md
[L2]: https://apidocs.lighter.xyz/reference/funding-rates.md
[L3]: https://apidocs.lighter.xyz/reference/fundings.md
[L4]: https://apidocs.lighter.xyz/reference/orderbookdetails.md
[L5]: https://apidocs.lighter.xyz/docs/rate-limits.md
[L6]: https://docs.lighter.xyz/trading/funding.md
[L7]: https://docs.lighter.xyz/trading/contract-specifications.md
