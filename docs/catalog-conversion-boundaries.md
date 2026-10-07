# Catalog conversion boundaries

Catalog interpretation lives in `src/exchanges/ccxt/venues/<exchange>/mod.rs`.
Shared conversion in `src/exchanges/ccxt/convert.rs` validates the loaded core
identity/product, assembles the final owned `UnifiedMarket`, and validates
numeric outputs. `catalog.rs` and `catalog/filter.rs` retain indexing,
collision checks, product selectors, defaults, and deterministic resolution.

## Local policies

- **Identity/settlement:** the existing `identity(market, product, info, parts)`
  policy supplies identity parts before shared Ferris market-id construction.
  It can read the loaded market, raw unified fields, and original metadata.
- **Naming:** existing `display_quote(info, quote)` and
  `raw_symbol(market, info)` policies retain their distinct roles. For example,
  Extended displays native USD while retaining the stock USDC-qualified CCXT
  symbol. This pass does not generalize naming into arbitrary field mapping.
- **Trading metadata:** `trading_metadata(market, info) -> TradingMetadata`
  interprets active state and the optional contract multiplier. The original six
  venues reexport the implementation in `venues/defaults.rs`; Apex has a local
  exception for prelaunch state and stock's minimum-order-size multiplier.
  A justified exception belongs in the venue module, replacing that reexport;
  it may reuse the default and return its own two-field result. It cannot patch
  the row or identity. Shared conversion still validates the returned contract
  size and the stock minimum order size, and interprets precision modes.
- **Aliases:** `aliases(source, converted, aliases)` receives the borrowed
  loaded `Market` and immutable, final `UnifiedMarket`. Metadata-dependent aliases
  can read `source.raw` (including `id2` and original `info`), while display and
  identity aliases use `converted`, not independently reconstructed values.
  Shared conversion still constructs display, CCXT-symbol, stock native-id,
  final native-id, Ferris-id, and original-info-name aliases. Local policies add
  only their additional owned strings.

A future `id2` alias belongs in that venue's `aliases`. An active/prelaunch or
qualified contract-size exception belongs in its `trading_metadata`. Neither
requires an exchange branch in `convert.rs` or the resolver. The test-only
example in `convert/market_tests/policy.rs` demonstrates the signatures without
registering a venue, adding a production override, or changing any public alias.

## Compatibility details

All venues retain the historical baseline active expression: stock `active`
AND NOT boolean `info.isPreListing` AND boolean `info.active` (missing/nonboolean
info flags do not constrain activity). This applies across venues and products,
not only Bybit. Apex additionally treats boolean `isPrelaunch: true` as inactive.

Contract size is still read with stock `Value::as_f64`, not JSON conversion or
string parsing. JSON would turn nonfinite numbers into null and conceal errors.
Missing/null/non-numeric contract values remain missing; zero remains zero;
negative/nonfinite numeric values remain source errors. There is no fallback to
minimum order size or an inferred multiplier. Apex deliberately omits the
incorrect stock contract size (derived upstream from `minOrderSize`).

Aliases run after final identity/naming and numeric validation. Exact resolution,
selector conflicts, ambiguous native/display aliases, duplicate CCXT symbols,
and duplicate final Ferris identities keep their existing handling. Only owned
rows, JSON and strings leave conversion; stock objects are borrowed locally.

## Registering an exchange later

Besides its local policies and tests, registration still requires:

- `Cargo.toml`: stock CCXT/Pro feature selection.
- `src/exchanges/ccxt/venue.rs`: `Venue`, `ALL`, public/CCXT identity registration.
- `src/exchanges/ccxt/venues/mod.rs`: module declaration and dispatch arm.
- `src/exchanges/ccxt/venue/provider.rs`: concrete REST provider registration.
- `src/exchanges/ccxt/stream/provider.rs`: concrete Pro registration and URL
  dispatch when integrating streaming.
- `src/config.rs` if new endpoint/configuration settings are needed.

No catalog conversion, indexing, resolution or selector changes are needed for
the metadata exceptions above. Statistics acquisition and streaming lifecycle
qualification remain separate work; this is not a claim that registering a new
provider alone completes an exchange integration. See
[streaming-control-boundaries.md](streaming-control-boundaries.md) for per-channel
unsubscribe policy and native-control adapter qualification.

## Deliberately deferred

No generic mapping framework or additional arbitrary field hooks. Apex was
subsequently integrated through these local policies (see
[pending exchange/apex.md](pending%20exchange/apex.md#5-implemented-integration-and-verification)):
API `id2` identity plus config-ID/base aliases, prelaunch filtering, and omitted
stock-derived contract size. Existing identity/naming interfaces remain in
place; requirements beyond these concrete boundaries should be evaluated
against a qualified integration, not anticipated with a mutable row-patching API.
