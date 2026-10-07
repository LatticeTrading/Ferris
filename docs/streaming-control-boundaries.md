# Streaming unsubscribe policy and control compatibility

Ferris keeps one stock CCXT Pro receive/dispatch driver per owned URL. An
exchange can be integrated without every `un_watch_*` method: unsubscribe policy
is selected **per channel** by `venues/<venue>/stream.rs::unsubscribe_mode`.

| Mode | Behavior | Current use |
| --- | --- | --- |
| `Stock` | Existing stock unwatch, cleanup and late-ack repair | Binance, Bybit, Hyperliquid, Aster, Lighter (supported channels) |
| `Native` | Small venue control protocol wrapped around the stock driver | Apex trades, books, candles |
| `Reconnect` | Rebuild URL from current demand without calling missing methods | Extended; reserved for unsupported Apex WS statistics (statistics remain REST-only) |

Removing one viewer of a shared acquisition does not retire it. No remaining
demand closes the URL immediately. Reconnect-only demand changes invalidate
continuity but do not accumulate exponential failure backoff. Genuine control
or transport failure retains ordinary reconnect/backoff and `UPSTREAM_ERROR`.
Continuity errors remain deliverable to existing leases after the next session
advances the data epoch; stale market data is still discarded.

## Shared boundaries

- `src/exchanges/ccxt/live.rs` owns demand, delivery epochs, retirement deadlines,
  re-add serialization, cache/settlement cleanup, and reconnect fallback.
- `src/exchanges/ccxt/stream/control.rs` defines `UnsubscribeMode`, `LiveEvent`,
  and `Controlled<C, P>`. The wrapper is only the **outer** stock `ws_run` core.
  Dynamic watch/parser methods delegate to the original core. Nested same-URL
  watches register on CCXT's existing client rather than read another socket.
- `Protocol` supplies native topic identity, the unsubscribe frame, and
  control/data classification. This is not a replacement market-data parser,
  copied synchronizer, dependency fork, or second receive loop.
- Unsubscribe commands use CCXT's deferred queue so they follow already-queued
  watches, even if removal races initial connection/registration.
- Native re-add waits for retirement acknowledgment. Success removes stock
  subscription bookkeeping, futures/settled results, and that feed's cache,
  then reconciles current demand. Unknown/retiring-topic data and book deltas
  preceding a fresh snapshot are discarded before stock parsing.
- Rejection, send failure, or a **10-second** native unsubscribe timeout rebuilds
  the URL. Unrelated feeds keep running while acknowledgment is pending, but a
  fallback reconnect necessarily interrupts them. At most 200 pending
  retirements are retained per owner in addition to its existing demand limit;
  overflow reconnects instead of accumulating unused upstream subscriptions.

Existing stock-venue lifecycle behavior remains separate: this adapter does
not claim to improve the correctness of their native unwatch implementations.

## Apex protocol and ordering

Apex supports the documented `{"op":"unsubscribe","args":[topic]}` operation;
pinned CCXT 4.5.85 does not implement it, and its subscription-status handler
ignores successful acknowledgments. `venues/apex/stream.rs::ApexControl` supplies
only the missing compatibility control:

- `recentlyTrade.H.<id2>`
- `orderBook200.H.<id2>` (all displayed depths share this acquisition)
- `candle.<stock timeframe mapping>.<id2>`

Acknowledgments echo `request.op`, `request.args`, and `success`. Public probes
supplied a `reqId`, but the response did **not** echo it. Operations for a topic
have retirement/re-add cycles serialized; no imaginary request-ID correlation
is assumed.
**Snapshots/data can precede the subscribe acknowledgment**, including many
book deltas. Waiting for that acknowledgment would discard the only snapshot
and leave the book unusable. It is the successful **unsubscribe** acknowledgment
that separates old and new subscription generations, not the subscribe ACK.

This topic-only adapter requires ordered unsubscribe semantics. It ignores
unmatched/duplicate ACKs while a topic is absent, active, or not yet sent for
retirement. A delayed duplicate spanning an entire subsequent retirement of
the same topic cannot be distinguished without echoed IDs. Likewise, arbitrary
old data arriving after a new subscription's snapshot cannot be identified
without upstream generation/sequence metadata. These are protocol assumptions,
not stronger exactly-once guarantees. Future venues must qualify their ordering
before reusing `Controlled`; choose `Reconnect` or implement ID-aware control
when topic-only ordering is unsafe. Apex stock book sequence-gap validation is
still absent and outside this change.

## Verification

- `tests/apex.rs` / `tests/apex/`: actual stock methods over loopback transport;
  mixed feeds, shared viewers/depths, all three unsubscribe topics, mapped
  monthly candles, quick re-add, delayed/rejected/missing ACKs, duplicate and
  unmatched ACKs, late retirement frames, fresh book snapshots, snapshots
  before subscribe ACK, timeout recovery, shutdown, and sibling-feed isolation.
- `stream/control/tests.rs`: per-channel policy, topic-only state transitions,
  batched acknowledgments, and send failure. `src/realtime/tests.rs` checks that
  slow readers cannot silently miss reconnect errors due to epoch changes.
- Full `cargo test --locked`: **159 passed**, 5 opt-in live tests ignored.
  Formatting and locked build pass; Clippy completes with baseline warnings.
- Public backend check: trades/books/candles received; repeated unsubscribe and
  re-add for all three channels, plus immediate book re-add, retained the same
  upstream connection and produced no continuity errors on sibling feeds.
- Direct public probe: three cycles per topic (nine total), snapshots preceding
  subscribe ACK, no topic data observed in the short quiet window following
  unsubscribe ACK. This is bounded observation, not a protocol proof, capacity
  qualification, or long-running soak test.
