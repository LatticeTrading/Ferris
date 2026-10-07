# Bitfinex integration

Public market data only, through pinned stock `ccxt`, `ccxt-base`, and `ccxt-pro`
**4.5.85**. No dependency upgrade, fork, native market-data parser, copied book
synchronizer, second socket reader, or HTTP fallback. No production deployment.
The pre-integration [research note](pending%20exchange/bitfinex.md) is preserved;
this document records the implemented decisions and qualification beyond it.

## Supported surface

| Surface | Support and limits |
| --- | --- |
| Catalog / aliases | Spot (including margin-enabled spot) and linear perpetuals, one bulk metadata scope. `type: spot`, `swap`/`perp`; `category: spot`, `linear`. Inverse, dated futures and options are rejected. Funding-currency `f...` rows are not trading markets. |
| REST trades | Stock `fetch_trades`, maximum 10,000. `since` and `until` (then `endTime`) become native `start`/`end`; results newest-first. No additional historical store or automatic pagination. |
| REST candles | Stock `fetch_ohlcv`, maximum 10,000, oldest-first. With `since`, stock requests earliest records after it, not a window ending now. Trade candles only. |
| REST books | Stock `fetch_order_book` with **P0**, default 100, maximum 250 per side. Acquire the next native depth in 1/25/100/250 and project requested top-N. No raw-order/R0 or precision selector. |
| `/v1/ws` trades / candles | Stock Pro `watch_trades` / `watch_ohlcv` and incremental caches. Initial upstream snapshots can contain a history window; reconnect is not cross-session exactly-once delivery. |
| `/v1/ws` books | Stock Pro `watch_order_book`, fixed **100-level P0** backing state, display default 25 / maximum 100. Viewer depths share one acquisition and never truncate the backing book. |
| Statistics REST / `/v1/ws` | Shared 30-second bulk polling, existing 90-second freshness and snapshot/revisioned-delta delivery. No per-market REST polling or singular ticker WebSocket fanout. |

Timeframes (REST and WS): `1m`, `5m`, `15m`, `30m`, `1h`, `3h`, `4h`, `6h`,
`12h`, `1d`, `1w`, `2w`, `1M`. Native aliases `1D`, `7D`, `14D` are accepted
and canonicalized. `3m`, `2h`, `8h`, etc. are not substituted or resampled.
Mark/index/premium-index candles are unsupported; a statistics mark does not
imply mark-candle support.

Usual Ferris selectors and validation apply. Ambiguous/conflicting identities
fail; unavailable options, timeframes, and excessive book depth return
`UNSUPPORTED_FEATURE` (HTTP 501), invalid products/symbols return `BAD_SYMBOL`
(400), and live preparation errors return `INVALID_TOPIC`. Unsupported metrics
are explicit field states, not whole-request failures or invented values.

Configuration:

- `BITFINEX_REST_BASE_URL=https://api-pub.bitfinex.com` (no `/v2` suffix)
- `BITFINEX_WS_URL=wss://api-pub.bitfinex.com/ws/2`

## Identity and metadata

Stock market/currency configuration calls each run once per shared catalog
load, not once per viewer or product selector. Default catalog/data scope can
resolve spot and perpetuals together. Use a product selector or unambiguous
identity where display pairs overlap. Default all-market statistics select
perpetuals; use `params.type: spot` for spot statistics. Selected statistics use
catalog **marketIds**, not a `symbols` request field.

Examples:

| Kind | Native identity | Bare native alias | Stock symbol / response symbol |
| --- | --- | --- | --- |
| Spot | `tBTCUSD` | `BTCUSD` | `BTC/USD` |
| Perpetual | `tBTCF0:USTF0` | `BTCF0:USTF0` | `BTC/USDT:USDT` |

Opaque market IDs retain the `t` prefix, e.g.
`["bitfinex","perp",null,null,"tBTCF0:USTF0"]`. Display symbols remain
`BASE/QUOTE`. Aliases come from catalog metadata; Ferris does not strip arbitrary
punctuation or guess missing collateral/quote tokens. Stock maps `UST` to
`USDT`; perpetual native settlement identity comes from **quoteId** (`USTF0`),
not stock's already-normalized `settleId`. BTC-collateral *linear* products are
not inverse merely because collateral is BTC.

- `contractSize = 1` is stock's multiplier, consistent with the exchange's
  linear position-size × price-difference examples. Do not treat the minimum
  order size as a multiplier.
- Minimum order quantity comes from native config. Price precision is stock's
  five **significant digits**, not a fixed tick; `tickSize` is absent. Amount
  precision is also stock-provided, not a dynamically discovered lot step.
- `active = true` means present in current stock config, **not** independently
  verified tradeability, listing history, or a delisting/prelaunch classifier.
- Book payloads do not supply exchange timestamps or nonces. Ferris removes
  stock REST's fabricated receipt timestamp; both REST and live books expose
  null time/nonce. Candle timestamps and trade timestamps are native milliseconds.

## Truthful statistics

Each shared acquisition performs **two bulk HTTP calls**:

1. `fetch_tickers(null)` → `/v2/tickers?symbols=ALL`, including non-market
   funding-currency rows that never become catalog markets.
2. `fetch_open_interests(null)` → `/v2/status/deriv?keys=ALL`.

The original status arrays retained in OI `info` are passed to stock
`parse_funding_rate` for funding/mark. This avoids both a duplicate status fetch
and the pinned `fetch_funding_rates(null)` failure (it requires symbols).
Parser work shares the status receipt, not a fictitious new upstream observation.

| Field | Spot | Perpetual | Provenance / meaning |
| --- | --- | --- | --- |
| Last price | Yes | Yes | Stock top-level `last`, native ticker LAST_PRICE, not midpoint |
| Volume 24h | Yes | Yes | Base quantity only, stock multiplier one; quote volume null, never last × base volume |
| Funding | Not applicable | Yes | Status CURRENT_FUNDING `[12]`, eight-hour decimal fraction, `currentUnclassified` |
| Mark price | Not applicable | Yes | Stock `markPrice`, status MARK_PRICE `[15]`, based on BFX Composite Index |
| Open interest | Not applicable | Yes | Status `[18]`: outstanding derivative **contracts**; amount only, no synthetic notional |
| Index price | Not applicable | Unsupported | Stock `indexPrice` is DERIV_PRICE `[3]`, the derivative-book midpoint, not an index |
| Last settled funding | Not applicable | Unsupported | No qualified settlement timestamp in this observation; do not infer it by subtracting eight hours |

Ticker exchange time is absent; freshness uses receipt time. Status `MTS` is
native milliseconds; `NEXT_FUNDING_EVT_MTS` is used only as next payment time.
Missing values remain unavailable; genuine zero funding/volume/OI remains zero.
Malformed/nonfinite/negative nonnegative metrics are explicitly invalid. Short
status rows are rejected for OI because stock can mistake a 23-cell status for
a history row. Source failure preserves partial success using the shared
statistics failure/staleness policy.

### Funding qualification

The official status schema says CURRENT_FUNDING is funding applied in the
**current eight-hour period**; NEXT_FUNDING_ACCRUED `[9]` is the **partial accrual
for the next period**. Ferris publishes the former, not an annualized APR,
next-period prediction, or asserted settled payment. `paymentTimestamp` stays
null; rate/payment intervals are 28,800,000 ms. Standard Ferris equivalents are
simple arithmetic comparisons, not realized returns.

The official [Funding Payment Summary](https://www.bitfinex.com/legal/derivative/funding)
expresses the rate as a multiplier of position size × mark price, gives payment
times 00:00/08:00/16:00 UTC, and quotes default deadband/cap 0.05%/0.25%.
Status exposes these same thresholds as 0.0005/0.0025 (SOL cap 0.025 for 2.50%).
Together with the stock parser's unscaled values and observed capped status
rates, this qualifies **decimal fraction**, not percent. Thresholds and caps
can vary by instrument; Ferris does not recompute or clamp the exchange's rate.

## Stock versus native control

See [shared streaming boundaries](streaming-control-boundaries.md). Apex's
existing `Controlled<C,P>` outer driver is extended for server channel IDs;
stock still owns transport, subscriptions, parsers, caches, and book updates.
All three Bitfinex client channels select **Native** unsubscribe:

- Stock advertises book unwatch but has no implementation. Its trade/candle
  unwatch can send before assignment and leaves routing/reverse lookups behind;
  stock capability flags alone are not qualification.
- Stock subscriptions use `trades:<nativeId>`, `book:<nativeId>`, and
  `candles:<native interval>:<nativeId>`. Subscribe ACKs supply numeric `chanId`.
- Native unsubscribe is `{"event":"unsubscribe","chanId":7}`. Early removal
  waits for the subscribe ACK, then queues the send on the existing stock
  deferred queue. No guessed topic-as-ID or second reader.
- Removing one viewer does not retire the feed. Re-add while retiring waits
  for `unsubscribed` with matching ID and `status: OK`. Successful retirement
  removes channel routing and reverse aliases, then the shared owner removes
  cache, futures and settled results before reconciling returning demand.
- Duplicate/unmatched retirement ACKs and old retired-ID data are suppressed.
  Re-added books require a new snapshot; deltas cannot populate an empty book.
- **Bitfinex reuses channel IDs**, observed in a bounded public probe and a
  Ferris smoke. An ID cannot safely identify multiple generations if delayed
  ACKs/data are allowed: reuse therefore reconnects the shared connection,
  even after successful retirement. It does not guarantee sibling continuity
  for every re-add. Unique-ID retirements leave unrelated channels connected.
- Data before assignment, conflicting/missing assignments, exchange error or
  restart/maintenance frames, unsent/rejected unsubscribe, and a missing ACK
  after 10 seconds cause reconnect/backoff with `UPSTREAM_ERROR`. Removing all
  demand closes immediately, even while retirement is pending.
- Admission is 30 distinct native feeds per connection (viewer depths do not
  consume more feeds). Retiring feeds count toward server slots; a demand
  transition exceeding slots rebuilds the URL. Seen IDs are bounded at 4096
  per session; reaching that history limit also reconnects.

### Book limitations

REST explicitly selects P0 because stock's R0 parser drops order IDs without
aggregating repeated prices. Live uses stock counted P0 books, including
COUNT=0 deletion. Stock checksum remains **enabled** (conf flag 131072) and
validated in loopback against a known CRC32, with mismatch→reconnect coverage.
Checksums cover only the **top 25 per side**, not all 100 levels; updates may be
published before a following checksum arrives. Stock does not enable/validate
SEQ_ALL for this path. No full-depth sequence-gap or long-duration integrity
qualification is claimed. Ferris's snapshot gate does not repair a stock
synchronizer or add a REST seed/diff fallback.

## Verification and official references

Deterministic `tests/bitfinex.rs`, `tests/bitfinex/lifecycle.rs`, and
`tests/bitfinex/downstream.rs` exercise actual pinned stock methods against
loopback HTTP/WS: native identity/aliases, ambiguous products, bounds and
unsupported paths, scalar provenance, zero/missing/invalid/partial statistics,
shared bulk call counts, mixed/shared feeds and depths, deferred retirements,
rejected/missing/duplicate/unmatched ACKs, retired data, fresh snapshots,
channel-ID reuse, transport/service interruption, CRC32 mismatch/recovery,
public `/v1/ws` envelopes/errors and shutdown. Shared controller unit tests
assert routing cleanup, invalid assignments and bounded history; existing Apex
and stock-venue regressions continue to run.

Bounded **public** checks were separate from these deterministic tests:

- A local Ferris process (loopback bind, 6-second upstream timeout) returned
  catalog and BTC spot/perpetual REST trades, P0 books and candles. Selected
  perpetual statistics returned funding, mark, last, OI and base volume with
  complete coverage and explicit unsupported index.
- `/v1/ws` produced perpetual trades, books and daily candles. Book removal and
  re-add exercised native retirement; one run observed ID reuse and three
  continuity errors, another did not. This is bounded reachability/behavior
  evidence, not a guarantee that re-add preserves the socket.
- A direct public WebSocket probe stopped at immediate channel-ID reuse; it
  did **not** complete a nine-retirement ordering qualification.
- Some initial official documentation requests returned HTTP 403. Markdown
  endpoints/cache-busting enabled research. A first statistics smoke used an
  invalid `symbols` field and received expected 400; rerun with catalog
  `marketIds` passed. Neither obstacle was worked around in production code.
- No capacity, cross-IP rate-limit, all-instrument, or soak qualification; no
  deployment. Default automated tests do not require public network access.

Final validation:

- `cargo fmt -- --check`: passed.
- `cargo test --locked`: **170 passed**, 5 opt-in public tests ignored. The
  initial 300-second command budget interrupted the existing long-running
  statistics suite; a rerun with a sufficient budget completed successfully.
- `cargo build --locked`: passed.
- `cargo clippy --locked --all-targets`: passed with warnings in existing code;
  none reported in the new Bitfinex implementation or tests.
- `git diff --check`: passed. Existing deletions and untracked research notes
  were left intact; `Cargo.lock` and dependency pins were not changed.

Official material reviewed alongside the actual pinned Rust methods:

- [Configs](https://docs.bitfinex.com/reference/rest-public-conf)
- [REST trades](https://docs.bitfinex.com/reference/rest-public-trades),
  [candles](https://docs.bitfinex.com/reference/rest-public-candles),
  [books](https://docs.bitfinex.com/reference/rest-public-book),
  [tickers](https://docs.bitfinex.com/reference/rest-public-tickers)
- [Derivatives status](https://docs.bitfinex.com/reference/rest-public-derivatives-status)
- [WS general/control](https://docs.bitfinex.com/docs/ws-general),
  [books](https://docs.bitfinex.com/reference/ws-public-books),
  [trades](https://docs.bitfinex.com/reference/ws-public-trades),
  [candles](https://docs.bitfinex.com/reference/ws-public-candles),
  [checksum](https://docs.bitfinex.com/docs/ws-websocket-checksum)
- [Derivative product descriptions](https://www.bitfinex.com/legal/derivative/product)
  and [funding terms](https://www.bitfinex.com/legal/derivative/funding).
  The site's public content is also delivered through
  `/v2/conf/pub:legal:terms:derivative_product` and
  `/v2/conf/pub:legal:terms:derivative_funding`.
