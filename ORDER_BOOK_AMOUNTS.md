# Order-book amount audit and proposed normalization

Scope: inspected working tree (HEAD `4da6a6a`), including its existing dependency update to locked `ccxt`/`ccxt-pro` **4.5.92** and native realtime work, on 2026-11-16. This covers registered adapters, not every product an exchange might offer. Source inspection and public metadata checks are not a production qualification of every live product.

## Current units and conversion

`a` = returned level amount; `p` = that level's price; `c` = **validated, product-specific** contract face size. “Base” means the catalog instrument's base (including named baskets/scaled instruments), not an inferred underlying token or option delta exposure. REST and live have the same amount semantics where both are implemented.

| Venue | Product | REST amount today | Live amount today | Normalize to base? / how |
| --- | --- | --- | --- | --- |
| Binance | Spot | Base | Base | Already base; identity |
| Binance | Linear perpetual / dated future | Base | Base | Already base; identity, not arbitrary multiplication |
| Binance | Inverse perpetual / dated future (COIN-M) | Contracts | Contracts | Yes: `a * c / p`; native `info.contractSize` is USD/contract |
| Binance | Options | Contracts | Contracts¹ | Yes: `a * c`; native `info.unit` is underlying units/contract; **not** division by option premium |
| Bybit | Spot | Base | Base | Already base; identity |
| Bybit | Linear perpetual / dated future | Base | Base | Already base; identity |
| Bybit | Inverse perpetual / dated future | Contracts (venue quantity is USD-face contracts) | Contracts (venue quantity is USD-face contracts) | **Not reliably from current catalog:** need a verified USD face size `c`; then `a * c / p`. Do not multiply by the catalog's order-minimum-derived size |
| Bybit | Options | Base (one-underlying-unit contract convention) | Base | Already base; identity |
| KuCoin | Spot | Base | Base | Already base; remove the contract warning for spot |
| KuCoin | Linear perpetual / dated future | Contracts | Contracts | Yes: `a * abs(info.multiplier)` in base/contract |
| KuCoin | Inverse perpetual / dated future | Contracts | Contracts | Yes: `a * abs(info.multiplier) / p` in USD/contract |
| Bitfinex | Spot / linear perpetual | Base | Base | Already base; stock takes absolute signed amount; `COUNT` is not a multiplier |
| Hyperliquid | Spot / linear perpetual (including HIP3) | Base | Base | Already base (`sz`); retain the actual instrument identity |
| Aster | Spot / linear perpetual | Base | Base | Already base; identity |
| Lighter (`lighterxyz`) | Spot / linear perpetual | Base, individual orders | Base, price levels | Already base: REST `remaining_base_amount`, live `size`; do not apply `quote_multiplier` |
| Apex Omni | Linear perpetual | Base | Base | Already base; do not multiply by `minOrderSize` |
| Extended | Linear perpetual, order-book markets | Base | Base | Already base; ordinary book, not inherently indicative |
| Extended | Linear perpetual, RFQ markets | Base | Base | Already base, **but remains indicative** |
| Nado | Spot / linear perpetual | Base | Unsupported | REST already base; normalization cannot enable a missing live implementation |
| Phoenix Eternal | Linear perpetual | Base | Base | Already human-unit amounts; no base-lot multiplication in book paths |
| Perpl | Linear perpetual | Decimal-scaled size | Decimal-scaled size | Already base for base-denominated `size_units`; both divide native integers by `10^size_decimals`. Validate `size_units` before promising this for every listing |
| Variational | Perpetual / swap listings | No order book | No order book | Not applicable; indicative quote tiers are deliberately not exposed as L2 |

Products not listed for a venue are unsupported by the current Ferris integration, including KuCoin options, Bitfinex inverse/futures/options, and inverse/options on the perp-only adapters. “Perpetual” does not imply dated-future support. ¹ Binance option stream routing exists and does not scale quantities; live options have not been end-to-end qualified in this audit. Contract counts equal base amounts numerically when `c = 1`, but that is not a safe general assumption. A public Binance metadata check returned option units **1, 100 and 1,000**, including XRP `unit=100` and DOGE `unit=1000`.

### What already converts, and what prevents a universal rule?

- **No contract multiplier is applied by Ferris's CCXT book path, or by the inspected stock REST/snapshot/delta book parsers.** REST calls `convert_book` from `rest.rs`; live calls the same function from `live.rs`. It copies price/amount, validates numbers, and drops extra cells. Bybit full/RPI REST books use the same stock parser and converter. Live top-N serialization does not change amounts.
- Perpl already performs decimal decoding on REST and live sizes. Bitfinex removes the side sign. Neither is contract-face normalization. Phoenix base-lot metadata conversions are for other fields, not book amounts.
- **Apex:** stock incorrectly derives `contractSize` from `minOrderSize`; Ferris intentionally omits it. Not a book blocker: quantities are already base.
- **Lighter:** stock exposes `quote_multiplier` as `contractSize`. It is not a factor to apply to these already-base book fields. Missing or non-one values do not justify changing book amounts.
- **Bybit inverse:** book quantities are contract quantities with USD face value, but stock derives `contractSize` from `minTradingQty`/`minOrderQty`, not an independently qualified face value. Current public inverse metadata had minimum 1, but that coincidence is not a conversion contract. Do not ship `a / p` as a universal rule; require a verified product face size and use `a * c / p`, otherwise report normalization unavailable.
- **Binance inverse/options:** stock defaults missing `contractSize`/`unit` to 1. Validate the original field rather than trusting this fallback. Missing, nonfinite, zero, or contradictory face metadata means normalization is unavailable for that market.
- **KuCoin contracts:** stock already stores `abs(multiplier)` as catalog `contractSize`; it does **not** apply it to books. Require a finite positive magnitude and unambiguous linear/inverse classification. A missing multiplier cannot safely be inferred from tick size, lot size or minimum order size.
- **Perpl:** `size_units` is deserialized but currently unused; the catalog uses `name` as base. The public context checked here matched them for all 11 listings. Require verified unit identity for unfamiliar/mismatched listings instead of declaring all scaled sizes base.

Thus normalization is feasible for the qualified supported books, but **not safely from today's generic `contractSize` alone**, and not unconditionally for missing/ambiguous metadata. Use each level's price, never last/mark/mid, for inverse conversion. Reject invalid/nonfinite conversion results. Options require underlying-size conversion, not the inverse-contract formula; their premium-price convention is separate from size, so normalizing amounts alone must not promise that `price * amount` is quote notional.

## Proposed additive API (not implemented)

Keep `/v1`'s existing amounts by default. Add `params.amountUnit: "native" | "base"` to **both** `fetchOrderBook` and `orderbook` subscriptions; default `native`. Here “native” means today's Ferris output, after existing sign/decimal decoding. A base request must either return base amounts or explicitly fail normalization—never silently return contracts labeled base.

Add the same metadata to every REST book and WS `data` object, including legacy/native responses:

```json
{
  "amountUnit": "base",
  "amountCurrency": "BTC",
  "sourceAmountUnit": "contracts",
  "amountConversion": {
    "method": "multiply",
    "contractSizeApplied": 0.001,
    "contractSizeCurrency": "BTC",
    "source": "kucoin:info.multiplier"
  },
  "liquidityKind": "orderBook"
}
```

- `amountUnit` / `sourceAmountUnit`: `base | contracts | quote | unknown`. Absence on old servers is **unknown**, not base. `amountCurrency` identifies base/quote currency; null for contracts/unknown.
- Conversion methods: `identity | multiply | divideByPrice | multiplyDivideByPrice | unavailable`. `contractSizeApplied` is null when no contract multiplier was used, including already-base books and Bybit USD-to-base conversion; it is not a copy of the catalog field. The currency distinguishes base/contract from quote/contract. Unavailable conversion should include a machine-readable reason.
- `liquidityKind`: `orderBook | indicative | unknown`, independent of amount units and connection health. “Order book” describes the source, **not a guarantee of execution**.
- Lattice shows a unit warning for non-base/unknown amounts and an indicative warning for indicative liquidity, even if amounts are base. Keep integrity/staleness separately visible. A single `amountUnit != "base"` condition would incorrectly remove the Extended RFQ warning.

Implementation boundary: build one venue/product-qualified conversion policy from the resolved catalog, use it for REST and live publication, and extend both `CcxtOrderBook` and the separate borrowed `WsOrderBookView`. Preserve raw stock state for delta/deletion/checksum processing; normalize an owned output once, not the stock cache and not independently per client. Unit selection is a downstream view/cache distinction, not a new upstream subscription. Strip the Ferris selector before upstream calls. Refresh/revalidate metadata (especially RFQ transitions), invalidate stale classifications, and test non-one sizes, inverse per-level prices, REST seeds/live deltas, and missing metadata.

**Breaking change:** changing existing `asks[][1]` / `bids[][1]` semantics in place would break clients that already convert contracts, even though the JSON shape stays identical. It also changes depth totals and potentially notional calculations. Use opt-in now or a versioned default later. Added metadata is additive for clients that tolerate unknown properties. Trades, candles, tickers and OI are outside this proposal and keep their existing units.

## Bitfinex: units versus integrity

Amounts are base; no “indicative” label is needed on account of their units. Native channel-ID reuse is fenced by a shared reconnect and fresh snapshot, rather than intentionally publishing cross-channel values. This belongs in connection status.

However, **partial checksum coverage is not solely connection reliability**: stock validates only the top 25 bids/asks of a retained 100-level book. Deeper values can be wrong without detection, and even covered values are not an execution guarantee. A mismatch causes rejection/recovery; updates can precede a later checksum. Keep an integrity-coverage warning/status, and treat `UPSTREAM_ERROR` as loss of trusted continuity until a fresh snapshot—not merely a cosmetic reconnect icon on a still-trusted old book. These caveats do not change the amount unit or make the source RFQ/indicative.

## Extended: correction to the exchange-wide premise

The [official RFQ documentation](https://api.docs.extended.exchange/#the-indicative-and-the-real-order-book) explicitly says the standard REST **Get market order book** and WS `/orderbooks/{market}` return the indicative book **for an RFQ market**. It contains maker quotes, not resting user orders; `/orderbooks/rfq/{market}` is the separate real-book stream, which Ferris does not use. No quantity conversion can turn the indicative book into executable RFQ depth.

This is **not true of every Extended market**. The same docs distinguish ordinary order-book markets and identify migration by `isRfq`. A public `/api/v1/info/markets` check during this audit returned 339 RFQ and 81 non-RFQ listings; **BTC-USD and ETH-USD were `isRfq: false`**. Counts/flags can change. Ferris already exposes `markets[].info.isRfq` via `convert_market`, but not on each book. Keep the tag for Extended **RFQ books**, not automatically all Extended books. If classification is unavailable/stale, keep a conservative unknown warning. Extended is the indicative-book case among the book-producing adapters audited here; Variational exposes no L2 book at all.

## Evidence and changes

Key Ferris paths:

- [`convert/snapshot.rs`](src/exchanges/ccxt/convert/snapshot.rs), [`rest.rs`](src/exchanges/ccxt/rest.rs), [`live.rs`](src/exchanges/ccxt/live.rs), [`web.rs`](src/web.rs): unchanged amount flow.
- [`convert.rs`](src/exchanges/ccxt/convert.rs), [`venues/defaults.rs`](src/exchanges/ccxt/venues/defaults.rs), [`venues/apex/mod.rs`](src/exchanges/ccxt/venues/apex/mod.rs): catalog metadata and Apex suppression.
- [`venues/bitfinex/stream.rs`](src/exchanges/ccxt/venues/bitfinex/stream.rs), [`stream/control.rs`](src/exchanges/ccxt/stream/control.rs), [`tests/bitfinex/lifecycle.rs`](tests/bitfinex/lifecycle.rs): channel fences and recovery.
- [`perpl.rs`](src/exchanges/perpl.rs), [`perpl/wire.rs`](src/exchanges/perpl/wire.rs), [`phoenix.rs`](src/exchanges/phoenix.rs), [`variational.rs`](src/exchanges/variational.rs): native paths.

Dependency evidence: locked `ccxt-base-4.5.92/src/exchanges/{venue}.rs` (`fetch_order_book` and market parsers), `ccxt-pro-4.5.92/src/pro/{venue}.rs` (snapshot/delta handlers; Bitfinex `handle_checksum`), and `ccxt-common-4.5.92/src/exchange_generated.rs` (`parse_order_book_bid_ask`). Primary references: [Binance option metadata](https://eapi.binance.com/eapi/v1/exchangeInfo), [Bybit inverse convention](https://www.bybit.com/en/help-center/article/What-is-Inverse-Contract), [KuCoin contract metadata](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol), [Perpl book scaling](https://github.com/PerplFoundation/api-docs/blob/main/rest-endpoints.md), [Extended RFQ/ordinary-market distinction](https://api.docs.extended.exchange/#rfq-markets).

Validation: conversion unit tests **12 passed**; Bitfinex integration/lifecycle tests **9 passed**. KuCoin tests **4 passed, 1 failed** with runtime files untouched by this audit: `live_trades_candles_and_books_use_the_negotiated_url`, `tests/kucoin.rs:830`, nonce `Some(4)` versus expected `Some(3)`; isolated rerun also failed. This is not a normalization test pass or a full live qualification.

Changes made by this audit: this report and corrections to README's blanket Extended description, plus an audit link. **No runtime values, API/schema, dependencies, or client behavior changed by this audit; other working-tree changes were left intact.** The opt-in design above is a proposal, not a shipped feature.
