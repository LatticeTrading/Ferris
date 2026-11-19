# TxFlow fixtures

Captured from TxFlow mainnet (`rpc.txflow.com`) on 2026-11-17 with
`x-api-version: 2026-08-16`, unauthenticated public reads only. Point-in-time
observations, not contracts. `users` addresses in trade payloads are replaced
with placeholders; nothing else is edited except that `perpMeta.json` keeps a
subset of the universe (crypto, negative `szDecimals`, delisted+halted,
stock, index, commodity, FX and pre-IPO rows).

- `perpMeta.json`, `marketTags.json`, `l2Book.json`, `trades.json`,
  `candles.json`, `candles_empty.json` (valid window with no bars, also what a
  >3000-bar window returns), `l2Book_unknown.json` (code 21001),
  `webData2.json` / `unknown_type.json` (code 10000, served with HTTP 403).
- `ws_l2BookDepth.jsonl`: `/ws l2BookDepth {coin:"1"}` frames in arrival order:
  snapshot, contiguous deltas, and unsolicited mid-stream snapshots.
- `ws_trades.jsonl`: `/ws trades {coin:"1"}` frames (no snapshot on subscribe).
  WS `tid`s are a different id space from REST `tid`s for the same fill.
- `ws_api_candle.jsonl`: `/api-ws candle {coin:"1",interval:"1m"}`, including a
  bar rollover; `s` echoes `"1"` on the first frame and `"BTC"` afterwards.
- `ws_allAssetCtxs.json`: one complete `/ws allAssetCtxs` push (241 rows).
