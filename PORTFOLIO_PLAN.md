# Portfolio Feature Plan

Status: planning specification only. No application code, dependencies, routes, or configuration are changed by this document.

Initial research date: 2026-09-10. Revised with the user's public Lighter and Aster Explorer evidence, official Lighter Explorer schemas, and documented/publicly exercised Aster Chain RPC reads. Protocol documentation and access policies are time-sensitive; qualification gates must be checked when implementation begins.

## 1. Decision and intended outcome

Add **Portfolio**, with `POST /v1/portfolio` as the primary endpoint. It should let a frontend select a wallet or particular exchange accounts and render:

- Account balances and asset holdings, including available versus locked collateral.
- Open positions, exposure, unrealized P&L, and available risk information.
- Account trade history from public protocol data or authorized account APIs, with source and coverage identified.
- Historical equity, P&L, and trading volume, with explicit definitions and coverage.
- Optional orders, funding payments, transfers, vault/pool investments, staking, and protocol-native statistics.
- Per-exchange results and a qualified aggregate, without turning missing data into zero.

The protocol set is **Hyperliquid, Extended, Lighter, and Aster**. Use Ferris's existing exchange identifiers: `hyperliquid`, `extended`, `lighterxyz`, and `aster`. Do not introduce a second `lighter` alias. Binance, Bybit, and other centralized exchanges are out of scope.

### Recommended product split

1. **Address-only access:** Hyperliquid, Lighter, and Aster have public account-data sources. Hyperliquid supplies native fills/portfolio history; Lighter Explorer supplies decoded executions/activity; Aster Explorer supplies balances, positions, staking and transaction actions, while Aster Chain RPC supplies public balances and actual fills. Scope, privacy and history limits remain source-specific.
2. **Optional connected accounts:** unlock Extended and enrich Lighter/Aster with authenticated native account APIs. Connections are not prerequisites for their already-public snapshots, activity or execution sources.
3. **Observed/reconstructed history:** explicitly track public or authorized snapshots when upstream charts are absent. Aster can support balance/equity observations without a signer; reconstruct earlier P&L only from qualified opening state, executions, cashflows and prices. Do not invent pre-observation history.

A DEX is not necessarily an address-public account API. An EVM address is a lookup key, not authorization for private data. The same address can be submitted to several protocols, but there is no single upstream API or universal account model.

The default overview must be bounded and fast. Trade pagination and chart queries get separate endpoints so an account-balance refresh does not wait for historical backfills. Selecting one exchange must execute only that exchange's work, not fetch everything and filter afterward.

### Non-goals

- Centralized exchange accounts, trade execution, order cancellation, withdrawals, or transfers.
- A general EVM wallet/token/NFT scanner or cross-chain transaction indexer.
- Discovering unrelated wallets, proving legal ownership from an address, or merging accounts based on a similar name.
- Tax accounting, cost-basis guarantees, or verified investment-performance reporting.
- Scraping frontend pages, relying on undocumented UI sessions, or bypassing private API access rules.
- Claiming complete lifetime history when the provider offers a bounded window or Ferris only recently started recording.

## 2. Feasibility and overlap

Legend: **Public** means documented or directly observed address/account access without account credentials; distinguish documentation, user-supplied evidence and executed research below. **Connected** means account authorization is required; **Unverified** means availability or semantics for the stated scope are not established. Privacy can restrict a public source. All implementation remains future work.

| Capability | Hyperliquid | Extended | Lighter (`lighterxyz`) | Aster |
| --- | --- | --- | --- | --- |
| Address to account discovery | Public user/subaccount lookup | Connected wallet/subaccount enumeration | Public L1 address to account indexes | Public lookup of the supplied address; full child enumeration remains a separate connected/unverified scope |
| Current balances/assets | Public | Connected | Public | Public Explorer balances; RPC product-separated assets, with Spot observed beyond the documented schema; privacy-sensitive |
| Current positions/unrealized P&L | Public | Connected | Public | Public Explorer/RPC positions; optional fields and privacy handling vary |
| Account fills/trades | Public, bounded recent retention | Connected, paginated | Public Explorer-derived executions; native `/api/v1/trades` requires auth for ordinary accounts | Public RPC Futures/Spot fills with optional symbol; richer signed V3 fills remain separate |
| Native equity/P&L chart | Public `portfolio` | Connected native chart endpoints | `/pnl` exists; ordinary-account access and financial semantics unverified | No chart in the supplied Explorer payload or researched RPC/V3 docs; observe/reconstruct with explicit inputs |
| Volume over time | Fill buckets; native window totals with scope qualifications | Native performance totals and fill buckets | Public Explorer executions bucketed by time; native chart buckets separately qualified | Public RPC fill buckets; Explorer order submissions/cancellations do not establish execution volume |
| Open orders/order history | Public, history bounded | Connected | Connected | Public RPC open orders; Explorer submission/cancellation activity; richer signed order history separately available |
| Funding and cashflow history | Public user ledger queries | Connected | Public Explorer deposits/withdrawals/transfers/share events; funding-prefix interpretation needs state/scaling, or connected native funding history | Signed income ledger; complete public funding/cashflow history not established by the supplied transaction excerpt |
| Vault/pool/staking enrichments | Public documented sources | Connected portfolio/vault breakdown | Public snapshot shares; some viewer-specific pool data requires auth | Public Explorer/RPC staking summary; valuation, privacy and inclusion in balances must be qualified |
| Whole-wallet historical completeness | Not guaranteed | Not guaranteed | Not guaranteed | Not guaranteed |

Sources: [H1], [H2], [H3], [E1], [E2], [L1], [L2], [L3], [L4], [L5], [L13], [A1], [A2], [A4], [A5].

**Useful common denominator:** current account state, positions, and account activity across all four when authorized. Hyperliquid, Lighter and Aster also expose public account/history sources; Extended remains connected in the researched contract. There is still no evidence-backed address-only trade-history or P&L contract shared by all four. Access belongs to a source, account scope and metric: a signed REST route does not make public Explorer/RPC data private.

## 3. Existing Ferris integration boundary

The repository already has public market-data adapters for all four target DEXs. `src/main.rs` registers them alongside Binance and Bybit and exposes snapshots, market discovery, and `/v1/ws`. These registrations do not imply account-data support.

Relevant existing patterns:

| Existing surface | Portfolio implication |
| --- | --- |
| `src/exchanges/traits.rs`: `MarketDataExchange`, explicit `Fetch*Params`, layered `ExchangeError` | Introduce a separate account-data capability boundary; do not force CEX adapters to implement fake Portfolio methods. |
| `src/exchanges/registry.rs`: exchange ID to shared adapter | Reuse the registry/`Arc` pattern for Portfolio-capable adapters. |
| `src/models.rs`: transport models, camelCase JSON, lowercase market types | Keep the new wire contract consistent. Account IDs and accounting decimals need lossless representations. |
| `src/web.rs`: validation, `AppState`, market-cache request coalescing | Validate selections before dispatch; reuse the coalescing pattern but bound account caches and lock entries. |
| `src/main.rs`: public router with permissive CORS | Connected-account routes need their own authorization/CORS boundary before credentials are accepted. |
| `src/config.rs`: deployment endpoints and a 10,000 ms default request timeout | Reuse deployment separation and transport timeouts; give a Portfolio request one bounded overall deadline. |
| `src/realtime.rs`, `src/ws_shared.rs`: shared public topics and reconnect behavior | Reuse transport lessons, not public topic keys, for private account streams. |
| Existing `fetchTrades` output/cache | This is market trade data, not the requesting wallet's private trading history. It cannot become a portfolio ledger by filtering a symbol. |
| `Cargo.toml` | No database or accounting-decimal dependency is currently declared. Durable history and decimal arithmetic are explicit implementation work. |

The existing funding/market-statistics plan concerns market-wide data. Funding **rates** and market **volume** are not a user's funding **payments** and account **volume**.

## 4. Protocol research

### 4.1 Hyperliquid

Base: `https://api.hyperliquid.xyz`. Account information is queried with `POST /info` and a JSON `type`; this is a read operation, not the trading `/exchange` API. Account selectors use the actual user's/subaccount's address, not an unverified API-agent address. [H1]

| Read request / type | Data useful to Portfolio | Important limits and interpretation |
| --- | --- | --- |
| `userRole`, `subAccounts` | Role, master/subaccount relationships, discovered accounts and embedded state | Agent, user, vault, subaccount, and missing roles are distinct. `userRole` is relatively expensive; cache discovery, not a lookup on every refresh. |
| `userAbstraction`, `userDexAbstraction` | Account mode and DEX abstraction | Required to avoid assuming every account has separate spot/perp collateral. |
| `clearinghouseState`, with `user` and optional `dex` | Margin/account values, withdrawable amount, position size, entry/liquidation price, unrealized P&L, leverage, cumulative funding | Empty `dex` is the first/native perp DEX. Query explicitly supported builder DEXs when selected; a native-DEX-only result is not all Hyperliquid. |
| `spotClearinghouseState` | Token IDs, balances, holds, entry notional | Official source of truth for trading balances under unified-account/portfolio-margin modes. Holdings are not additional collateral to blindly add to the perp summary. |
| `portfolio` | `accountValueHistory`, `pnlHistory`, and `vlm` for `day`, `week`, `month`, `allTime`, plus `perpDay`, `perpWeek`, `perpMonth`, `perpAllTime` | Native sampled windows, not arbitrary-resolution historical requests. `vlm` is a window total, not a volume time series. Inclusion of spot, builder DEXs, vaults, staking, and subaccounts must be qualified per window/account mode. |
| `userFills` | Recent user executions | At most 2,000 recent fills. Useful for a first page, not lifetime trading history. |
| `userFillsByTime`, `startTime`, optional `endTime`, `aggregateByTime` | Executions including price, size, direction, time, order/trade IDs, closed P&L, fee/fee token | At most 2,000 per response; only the 10,000 most recent fills are available. Time bounds are inclusive upstream. Use unaggregated fills for accounting. |
| `userFunding`, `userNonFundingLedgerUpdates`, `startTime`, optional `endTime` | Funding, deposits, withdrawals, transfers, and other ledger events | Classify each delta type; an unrecognized delta is not a deposit or a zero-value event. Time-based history has response caps. |
| `openOrders`, `frontendOpenOrders`, `historicalOrders`, `orderStatus` | Open orders, frontend order details, historical statuses | Historical orders are a recent bounded source, not guaranteed lifetime order retention. |
| `userFees` | Fee schedule, discounts, daily user-volume fields | Fee-tier volume can weight products and aggregate subaccounts; do not treat it as an unqualified per-account executed-notional series. |
| `userVaultEquities`, `vaultDetails` | User claims on vault equity; vault-level portfolio/performance | User claim value and full vault NAV are different scopes. |
| `delegations`, `delegatorSummary`, `delegatorHistory`, `delegatorRewards` | Staked/delegated HYPE, pending withdrawals, reward history | Value once and keep staking outside trading equity unless source inclusion is proven. |
| `borrowLendUserState`, reserve-state queries | Supplied/borrowed balances, health, lending rates/oracle data | Liabilities must not disappear from net asset value. Treat lending as an optional component. |

Sources: [H1], [H2], [H3], [H6].

**Accounting hazards:**

- In unified-account/portfolio-margin mode, the docs explicitly direct balance consumers to spot state. Summing spot totals and `marginSummary.accountValue` can count the same funds twice. Preserve mode and validate liabilities/P&L treatment before normalizing a net asset value. [H2], [H3]
- `positionValue`/notional is exposure, not another asset balance.
- `fee` already includes `builderFee` in the documented fill response. Do not subtract both. Rebates and non-USDC fees need asset-specific signs/valuation. [H1]
- Fill trade IDs are not safe as globally unique IDs on their own. The WS documentation identifies `(block_time, coin, tid)` as the global trade identity; add account identity when storing account participation. [H4]
- Fee-tier volume combines products with weighting and counts subaccount volume toward the master. A master fee-volume total plus child totals is double counting. [H6]
- `allTime` describes the provider's supplied sampled series. It is not proof of every historical fill or unlimited chart resolution.

**Realtime candidate:** `clearinghouseState`, `allDexsClearinghouseState`, `spotState`, `userFills`, `userFundings`, `userNonFundingLedgerUpdates`, and order updates. Initial account-event snapshots must be deduplicated against REST/reconnect replay. Public user-specific WS subscriptions are limited to **10 unique users** across subscriptions under the documented IP limits; an always-on stream per watched wallet is not a scalable default. [H4], [H5]

**Qualification gates:** account-mode normalization, native versus builder-DEX coverage, chart component inclusion, timestamp-boundary pagination, chart sampling/baseline behavior, and fee-volume scope. Ship per-source native series before claiming a cross-exchange comparable P&L series.

### 4.2 Extended

Current production base: `https://api.starknet.extended.exchange`; REST prefix `/api/v1`. Account reads require `X-Api-Key`; REST and WS require `User-Agent`. An EVM address, Stark key, account ID, and bridge Starknet address are different identities. No general unauthenticated EVM-address portfolio lookup was established. [E1]

| GET route under `/api/v1` | Data | Scope / limits |
| --- | --- | --- |
| `/user/account/info` | Current account ID, status, L2 key/vault, bridge address | Resolves the authenticated account. |
| `/user/accounts` | Wallet-associated subaccounts | Includes account indexes and may include a special vault-balance account. Enumeration does not prove each key can read every child route. |
| `/user/balance` | Balance, equity, trading/withdrawal availability, unrealized P&L, margin, exposure, leverage, spot equity | Docs specify a 404 for zero balance; qualify this route-specific behavior against a known authenticated account. Generic 404s must not become zero balances. |
| `/user/spot/balances`, optional repeated `accountId` | Per-asset balance, index price, notional, contribution factor, equity contribution, optional cost basis/P&L | Includes a collateral entry. No balances can produce 404. Wallet ownership of repeated IDs is required. |
| `/user/positions`, `/user/positions/history` | Current and historical positions, size/P&L/risk fields | Market and side filters; paginate history where available. |
| `/user/orders`, `/user/orders/history` | Open and historical orders | Filtered/paginated account data, not public market orders. |
| `/user/trades` | Fills with IDs, account/market/order, side, price/quantity/value, fee, liquidity role, trade/liquidation/ADL type | Docs describe a 10,000-record ceiling and pagination; establish page/window/retention semantics rather than interpreting it as lifetime retention. |
| `/user/funding/history` | Position-specific funding payments | `startTime` and market/side filters; pagination and documented record ceiling need qualification. |
| `/user/assetOperations` | Deposits, withdrawals, subaccount transfers, status, fee, counterparties, chain/transaction hash | Default 50 records/page; descending timestamps and cursor pagination. Amount examples and prose disagree about signed versus absolute values. |
| `/user/fees`, `/user/rebates/stats` | Fee rates and rebate statistics | Optional enrichments; do not add rebate totals to a P&L measure that already includes them. |

Sources: [E1], [E3].

#### Native portfolio endpoints

Extended documents significantly more than account snapshots. These routes were found in the **official API page's raw HTML**, including authentication sections, but not in the researched SDK account module or its partial, hand-maintained OpenAPI file. Reader-mode extraction omitted these sections. Their existence in official prose is useful evidence; it is not an authenticated response-schema test. [E2], [E3]

| GET route under `/api/v1/portfolio` | Proposed use |
| --- | --- |
| `/charts/equities` | Date/value equity history for selected `accountId` values. |
| `/charts/pnl` | Date/value `TOTAL_PNL` or `REALISED_PNL`; optional `instrumentType` with `ALL`, `PERPS`, or `SPOT`. |
| `/charts/pnl/percentage` | Native percentage P&L with optional benchmark price-market series. |
| `/charts/pnl/cumulative` | Running-total P&L. |
| `/charts/pnl/cumulative/percentage` | Running percentage P&L and benchmarks. |
| `/charts/vault-equities` | Equity attributable to vault positions. |
| `/charts/max-drawdown` | Per-account drawdown series. |
| `/charts/funding` | Daily net funding, optionally filtered by market. |
| `/accounts/summary` | Selected-account summary: equity/account-value breakdown, exposure, P&L, volume, maker volume, win rate, profit factor, Sharpe. |
| `/accounts/health` | Current risk/health metrics. |
| `/accounts/performance` | Per-account P&L, volume, returns, and performance metrics. Its filter is named `marketType`, not `instrumentType`. |
| `/funding/stats` | Total and per-market funding statistics. |
| `/funding/history` | Paginated interval funding history; docs list default 50/max 500. |

Common chart interval selector: `DAY|WEEK|MONTH|YEAR|ALL`. Repeated `accountId` is documented for selecting several accounts. Dates in examples are `YYYY-MM-DD`, not intraday timestamps. Query single accounts initially to preserve provenance; do not combine an upstream multi-account total with its child rows. [E2]

**Important cautions:**

- Native balance `equity` includes spot equity contributions. Non-collateral spot assets have `notionalValue` and a potentially haircut-adjusted `equityContribution`. Margin equity is not automatically market-value wealth. Do not add `spotEquity` again. [E1]
- The balance/spot examples contain timestamp magnitudes inconsistent with the prose's millisecond definition. Fix unit mapping through authenticated qualification, not a generic magnitude heuristic.
- Asset-operation examples use negative withdrawal/transfer amounts while a table calls amount absolute. Confirm direction from event type/counterparty and observed semantics before cashflow-adjusted P&L.
- Authentication prose describes an API key as read-only, but the official SDK's cancel/mass-cancel methods send API-key-authorized requests without a Stark signature in those methods. This is conflicting evidence about effective least privilege. **Do not promise that any Extended API key is harmless or strictly read-only.** Ferris's adapter must allowlist reads, and hosted key custody needs explicit permission qualification. [E4]
- Native charts avoid a fragile trade-only reconstruction. They still require validation of timezone, bucket meaning, fees/funding inclusion, earliest `ALL` date, multi-account scope, and vault inclusion.

**Realtime candidate:** the documented authenticated account stream, including order/trade/balance/position updates and spot balance events. The docs contain differing host/path examples; qualify the exact production URL/header behavior rather than guessing between them. Preserve sequence handling, ping/pong requirements, and REST reconciliation. [E1], [E3]

**Qualification gates:** an authorized disposable account, key scope across children, cancellation privilege risk, all chart response schemas, 404 handling, transfer signs, date boundaries, and the special vault-balance account.

### 4.3 Lighter

Native base: `https://mainnet.zklighter.elliot.ai`; native REST prefix `/api/v1`; WS `wss://mainnet.zklighter.elliot.ai/stream`. The separate Explorer API is `https://explorer.elliot.ai/api`, already used by Ferris for market discovery. Do not merge its access/rate contracts with the native API. [L1], [L13]

| Native GET route | Data / parameters | Access and limits |
| --- | --- | --- |
| `/accountsByL1Address?l1_address=...` | All associated indexes, account types/status/trading modes; optional cursor | Public. One wallet can resolve to multiple accounts. |
| `/account`, with `by=l1_address` or `by=index` and `value` | Collateral, available balance, account values, assets, positions, shares, pending unlocks, margin requirements | Public; `active_only` and cursor are documented. Preserve every discovered account, not just `accounts[0]`. |
| `/pnl?by=index&value=...` | Resolution, start/end timestamps, `count_back`, optional `ignore_transfers` | Schema makes authorization optional. Public-pool success does not establish ordinary-account access. |
| `/trades` | Account, market/type, role, trade type, cursor; required sort and limit | Auth required for master/subaccounts, public-pool exception. Maximum 100/page; descending results. |
| `/positionFunding` | Account funding with time, side, market filters and cursor | Ordinary-account auth required; public-pool exception. Maximum 100/page. |
| `/accountActiveOrders`, `/accountInactiveOrders` | Open/historical orders; inactive limit/cursor/filters | Auth required. Inactive maximum 100/page; retention not established. |
| `/accountOrders` | Lookup by at most 20 client-order IDs | Docs separately limit lookup support to last 10,000 active orders, or last 1,000 inactive orders in 24 hours. Do not transfer that limit to other routes without evidence. |
| `/deposit/history`, `/withdraw/history`, `/transfer/history` | Cashflows with statuses, assets, timestamps, account/route counterparties, cursors | Ordinary-account auth required; transfer history explicitly exempts public pools. Endpoint-specific rules differ. |
| `/export` | Trade/funding CSV URL | Authorized bulk export. General docs say 12 months or 1M trades; historical guide narrows funding export to 3 months. Not a fast overview operation. |
| `/publicPoolsMetadata` | Pool NAV/shares, APY, operator fee, assets and optional viewer claims | Public unless account-specific viewer information is requested. |
| `/assetDetails` | Asset identity and collateral risk parameters | Needed for margin/asset valuation, not just symbol display. |

Sources: [L1] through [L7], [L11], [L12], [L15], [L16].

#### Public Explorer history is a first-class source

**Correction:** the earlier plan wrongly generalized the native `/api/v1/trades` authentication requirement to all ordinary-account trading history and dismissed Explorer logs as audit-only. The official Explorer API exposes decoded execution and ledger payloads by **L1 address or account index**. Ferris should use these to deliver address-only history, not wait for credentials. This is a supported public data source, not scraping or an authentication bypass. [L13]

| Public Explorer GET route | Use in Portfolio | Contract / limits |
| --- | --- | --- |
| `/accounts/{addressOrIndex}/logs` | Mixed account activity: trades, liquidations, position exits, transfers, deposits, withdrawals, pool/share events and configuration changes | `pub_data_type` is a comma-separated filter; documented `limit` maximum 100 and string `offset`. Response is an array, without a documented next-cursor field. |
| `/logs/{hash}` | Selective event detail/reconciliation | Adds `block_number` and nullable `batch_number` to the log envelope. Do not fetch once per ordinary trade when the list already has the necessary fields. |
| `/markets` | Map `market_index` to symbol | Combine with existing native market metadata for product, quote currency, units and delisted-market handling. Do not hard-code index-to-symbol mappings. |

Sources: [L13], [L17], [L18]. The user's unparameterized account-log request works even though the OpenAPI marks `limit`/`offset` required. Treat defaulting and pagination mechanics as a documentation/runtime discrepancy to qualify, not evidence that public access is absent.

For trade-related history, include the user's six event types and the documented deleveraging variants:

```text
Trade,TradeWithFunding,LiquidationTrade,LiquidationTradeWithFunding,ExitPosition,ExitPositionWithFunding,Deleverage,DeleverageWithFunding
```

Keep `ExitPosition` and `ExitPositionWithFunding` in the broad trade-related feed as requested; do not narrow the query to liquidations. Their precise lifecycle meaning remains unresolved. A buy/sell can open, increase, reduce, close or reverse a position; that classification depends on prior position state, not an event name alone.

##### Event decoding and account participation

- Dispatch on `pubdata_type` and the matching decoded payload, while retaining `tx_type`. The supplied execution is `tx_type=InternalClaimOrder` but `pubdata_type=Trade`; discarding it as merely an order event would lose a real trade.
- Resolve the wallet to its account indexes before attributing events. Prefer per-index history when selecting a concrete account; qualify whether an address-level log query includes children. Match participant indexes in the payload, not just the fact that the row appeared under an address.
- For ordinary trade payloads, `is_taker_ask=1` means taker sells and maker buys; `0` reverses those sides. Determine maker/taker role from the corresponding account index. Preserve two distinct participations if both roles belong to the selected account. A fee-account-only match is not a trade participation.
- `price` and `size` are decimal strings. For a qualified linear market with base-unit size, quote notional is `abs(price * size)`; instrument metadata supplies the currency/contract convention. Emit raw market identity if symbol mapping is unavailable instead of dropping the event.
- Preserve the complete `hash`, ISO `time`, source type and status. Parse the UTC timestamp to milliseconds; frontend-local date formatting must not alter the underlying day bucket.
- The supplied trade exposes neither an order ID nor a documented native `/trades` trade ID. Keep those fields null; use a separately identified Explorer log identity rather than inventing an order ID from the hash.
- Preserve integer maker/taker fee fields as raw values. Their units/sign/scaling are not established by these schemas; normalized fee is null until qualified. Neither unknown fee nor missing realized P&L prevents returning side, price, quantity and volume.
- The status enum is `nothing_to_execute`, `committed`, `verified`, `executed` (nullable in the envelope) and describes batch lifecycle. Preserve it. Initial settled accounting uses the observed `executed` state; other states can remain visible as provisional/nonaccounting activity. Do not interpret a frontend's `no exec` text as an executed fill or as a proved failed trade. Status refreshes must update one event rather than create a second execution. [L13]

| Event family | Public payload / treatment |
| --- | --- |
| `Trade`, `TradeWithFunding` | Price, size, market, participant indexes, taker side and raw fees. Funding variant additionally carries `funding_rate_prefix_sum`; it is still one execution, not an extra trade. |
| `LiquidationTrade`, `LiquidationTradeWithFunding` | Include in the trade feed and retain liquidation classification. Decode the actual payload variant before assigning participants/amounts; an enum label alone does not establish all field semantics. |
| `ExitPosition`, `ExitPositionWithFunding` | Schema supplies account, market and `settlement_price`, plus optional funding prefix, but no standalone size or side. Return a trade-related position-exit row with unknown position effect/quantity where needed. Derive a closing/settlement amount only from a qualified preceding position state; never manufacture volume or label every exit a liquidation. |
| `Deleverage`, `DeleverageWithFunding` | Bankrupt/deleverager indexes, market, side flag, `quote`, `size`, and optional funding prefix. Preserve the event; qualify party orientation and quote/size units before normalized accounting. |
| `L1Deposit`, `L1DepositV2`, `Withdraw`, `WithdrawV2`, `L2Transfer`, `L2TransferV2` | Public funding-of-account/cashflow events. V1 payloads name USDC amounts; V2 adds asset identity, product routes and applicable transfer fee. Determine selected-account direction and ownership scope before classifying external cashflow. |
| `L2MintShares`, `BurnedShares`, staking/unstaking/unlock variants | Public investment activity with share and asset amounts. Mint/burn plus associated transfer logs can describe one economic movement; reconcile them rather than count each as a new external deposit/withdrawal. |

The trade-event filter is intentionally not the cashflow filter. For `/ledger`, fetch the relevant deposit/withdraw/transfer/investment families or an unfiltered bounded account page; trade-only logs cannot establish complete equity cashflows. Keep unknown event variants visible and mark affected accounting coverage incomplete. [L13]

##### Supplied execution: concrete normalized projection

Using account `248287`, the user's exact sample maps to a **taker sell of 0.98512 at 79,481.0**. Account `726714` is the maker buyer. The arithmetic below is in the market's quote units; reporting USDC additionally requires the market metadata. This is a projection of supplied evidence, not a response from an implemented Ferris route:

```json
{
  "accountIndex": "248287",
  "marketIndex": "1",
  "sourceLogHash": "0000001b9456d5a6000001a04332ebb4000000000000000000000000000000000000000000000000",
  "timestamp": 1787833805748,
  "sourceTime": "2026-08-27T12:30:05.748Z",
  "liquidity": "taker",
  "side": "sell",
  "positionEffect": "unknown",
  "price": "79481.0",
  "amount": "0.98512",
  "notionalInQuoteUnits": "78298.322720",
  "rawFee": 0,
  "fee": null,
  "realizedPnl": null,
  "pubdataType": "Trade",
  "txType": "InternalClaimOrder",
  "source": "lighterExplorer",
  "origin": "derivedFromEvents"
}
```

This is enough to display general trades over time and to add `78298.322720` quote units to the appropriate execution-volume bucket, without a Lighter token. It is not enough to call the sell a close or assign a realized profit without its preceding position/cost basis.

##### Public graph and P&L construction

| Deliverable | What is needed beyond these public logs |
| --- | --- |
| Trade/activity timeline | Event decoding, participant attribution, status and pagination handling. No private connection is intrinsically required. |
| Executed volume / trade count over time | Market units, exact-decimal notional and unique executions bucketed by source time. Show coverage for the traversed range; partial retrieval is not a complete day. |
| Reconstructed gross realized trading P&L | Chronological, sufficiently complete execution/settlement history and a known starting signed position/cost basis (or verified flat start). Fees and funding remain separately excluded until decoded. |
| Reconstructed net realized P&L | Gross P&L plus qualified fee/rebate amounts, funding accumulator/state interpretation, liquidations/ADL/settlements and other included expenses. |
| Reconstructed historical equity / total P&L | A known opening balance/equity and positions, complete cashflows/liabilities/investment movements, and historical mark/index/asset/share prices. Use complete public state/events where available; private credentials are one possible enrichment, not a mathematical requirement. Otherwise return observed equity from public snapshots and mark earlier gaps. |

For a qualified linear perp replay, maintain signed position `q`, average entry `e`, and signed fill `dq` at `p`. A same-direction fill updates the weighted entry and realizes no price P&L. An opposite-direction fill closes `min(abs(q), abs(dq))` and realizes `closedSize * sign(q) * (p - e)`; a reversal closes the old side and opens the residual at `p`. Preserve an unknown baseline until a complete position state is established; the first retrieved fill is not evidence of a flat start. Qualify this accounting convention against Lighter's position rules. [L9]

`funding_rate_prefix_sum` is an accumulator, not an already-denominated account payment. Deriving funding needs its scale/sign and the position's prior funding checkpoint/size; do not sum raw prefixes. Likewise, buys and sells do not reveal the value of an open position at intermediate chart times without historical marks. Missing these inputs means a particular P&L measure is unavailable or explicitly gross/partial, **not that public trade history is unavailable**.

##### Pagination, identity and provenance

- Explorer documentation gives a string `offset` but does not define whether it is a count, a hash or another token. Do not guess `offset=lastHash`, increment it blindly, or expose it as a proven cursor contract. Qualify it against the official Explorer client/provider; the user's successful first page already establishes public data access.
- No server-side time range or stable sort contract is documented on this route. Implement Ferris `since`/`until` through qualified traversal and local filtering, preserving a frozen upper bound, overlaps and progress checks. A late-arriving event can otherwise move an offset-based page boundary.
- Qualify full-hash uniqueness for internal/system events. Never use a truncated display hash, timestamp alone, array position or an all-zero hash as a unique execution key. Detailed logs add block/batch context, but do not document an intra-block execution index; ordering ambiguities can block exact P&L replay without blocking the trade timeline.
- Store source log identity separately from account participation identity. When every event is retrieved by both address and index or via overlapping filters/pages, count it once per account role. If identical placeholder-hash events cannot be distinguished, expose uncertainty rather than silently merging distinct executions.
- Report `source: lighterExplorer` and `origin: derivedFromEvents`, source/filter version and returned coverage. Native private history is `source: lighterNative`. Do not join the two sets by timestamp/price/size alone; absent a qualified shared identity, use one authoritative source per segment and keep any enrichment separate.
- Cursor state binds source, filters, frozen range and any buffered overfetch along with the account. The existing Explorer rate budget is separate: 90 weighted requests/min, account routes weight 2. Bound backfills and cache/coalesce account pages rather than performing an unlimited scan in `/portfolio`. [L12], [L13], [L17]

Unresolved pagination or rare event accounting semantics require explicit limited coverage. They do not justify categorizing the whole Explorer source as connected-only or deferring readable execution rows until native chart qualification.

#### What `/pnl` does and does not establish

Its rows include `trade_pnl`, `trade_spot_pnl`, inflow/outflow fields for trading/spot/pools/staking, pool shares, staking values, and `volume`. Resolutions are `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. The documentation does not fully define which fields are cumulative, the exact meaning of `ignore_transfers`, or how to derive economic equity across account modes. Do not rename `trade_pnl` to equity or assume all fields are interval deltas. [L3]

Read-only research observed:

- Ordinary active account `722720` and account `1`: `/account` succeeded; unauthenticated `/pnl` returned HTTP 400, code `21100`, `account not found`.
- Documented LLP public-pool index `281474976710654`: `/pnl` succeeded.
- Ordinary-account `/trades` explicitly rejected empty authentication with code `20001`.

**Conclusion:** the native ordinary-account `/api/v1/trades` route is auth-required, but public Explorer-derived trade/activity history is available without it. Ordinary-account native chart availability remains **unverified**, not a proven authentication failure: `account not found` does not establish why `/pnl` is unavailable. A read-only connection is one native-chart qualification path, not a requirement for public execution/volume graphs or a promise that adding a token fixes `/pnl`.

The pool query accepted both second- and millisecond-scale request bounds and returned second-scale point timestamps. This is a bounded observation, not a portable dual-unit contract. Export bounds are documented in milliseconds; qualify each native PnL field and normalize to Ferris milliseconds explicitly.

#### Snapshot and accounting semantics

- Position sign is `1` long / `-1` short; size, entry price, unrealized/realized P&L, funding, liquidation price, and allocated margin are available. Native realized perp P&L includes funding. Do not add funding twice. [L1], [L9]
- Classic versus Unified accounts differ in shared spot/perp collateral. Isolated allocated margin belongs to the account, but must not be added again to a total that already includes it. [L9], [L10]
- Multi-asset margin uses LTV-discounted non-USDC collateral. Its total account value is a risk measure; keep market NAV and margin-equity concepts separate. [L10]
- `shares[]` represents investment claims. A pool operated by the wallet is not wholly owned by that wallet. Never aggregate both its full NAV and the user's share claim. Share-price/scaling and account-type classification require qualification. [L11]
- Native fill maker/taker fee fields are integers with insufficient scaling documentation in the researched schema. Return an unavailable normalized fee until verified; do not guess the scale. [L4]
- The queried account may be buyer, seller, or both. Map the account's participation, fee and realized P&L, rather than copying one side's values unconditionally.
- Trade `timestamp`, account/trade `transaction_time`, chart point timestamps, and funding timestamps use different scales in observed/examples data. Unit normalization must be field-specific.

#### Credentials and streams

Prefer a **Lighter read-only token** created by the user, not an API private key. Documented token format is `ro:{account_index}:{single|all}:{expiry_unix}:{random_hex}`. A master token with subaccount access covers children; a child token covers only that child. Read-only tokens cannot authorize trading/withdrawal transactions. Supported lifetimes are one day to ten years; Ferris should recommend short, renewable lifetimes rather than the maximum. [L8]

WS opportunities include `account_all`, `account_all_trades`, `account_all_positions`, `account_all_assets`, and `user_stats`. Some subscription examples omit `auth`; that omission is not evidence of public normal-account access. Research connections failed the WS upgrade, so those channels' ambiguous access rules remain unverified. Explicitly authenticated channels must stay private. WS may refresh state after a REST bootstrap; it is not historical retention. [L14]

The documented Explorer assets/positions endpoints also provide public snapshots. Public logs are a first-class historical source as specified above, with less normalized detail than the authenticated native API. The source distinction must remain visible without making authentication a prerequisite for data already published publicly. [L13]

**Qualification gates:** Explorer offset/sort/retention behavior, address-to-child scope, internal-event identity and rare liquidation/exit/deleverage payload semantics; fee/share/funding-prefix scales and replay baselines for P&L; ordinary-account native `/pnl` access/semantics; token scope, native timestamp/cursor/retention/export behavior and ambiguous WS authentication. Public executions/volume do not wait on the native `/pnl` or private-connection gates.

### 4.4 Aster

#### Public Explorer account state and transaction activity

**Correction:** Aster is not connected-only. The user's successful `POST https://explorer.asterdex.com/explorer` supplies public account state and transaction history. This is a read request despite using POST; do not replay the trading actions contained in its response. The request is:

```json
{
  "type": "userDetails",
  "user": "0xb6F576165Ec156be28F2cc895bcb6d880cd4d997"
}
```

No `signer`, signature, nonce or API key appears in this read contract. The supplied response is accepted as observed; it was not re-requested to confirm the user's finding. A standalone Explorer schema, pagination contract and provider rate budget were not established by this research. [A4]

| Supplied field | Portfolio use | Normalization boundary |
| --- | --- | --- |
| `balanceDetails` | Public holdings: USDT `1866718.45789089`, AFEE `0.00007721`, USD1 `309.11047960`, ASTER `15123.09169797` | Parse JSON numbers losslessly into decimal strings. Asset quantities are not one account equity number; product partition, prices, liabilities and inclusion of unrealized P&L/staking are not supplied here. |
| `positions[]` | `SOLUSDT` and `SOLUSD1` positions with amount, side, notional, unrealized profit, `cumRealized`, leverage and entry price | Keep markets/denominations separate. `BOTH` denotes position mode, not a zero or two-sided quantity; qualify sign/hedge rules. Notional is exposure, not owned wealth. |
| `staking` | Total stake, pending deposits/withdrawals and nullable unlock date | Preserve observed zeros. A null unlock date does not promise immediately withdrawable funds. Qualify units and overlap with balances before adding investment value. |
| `txs[]` | Account transaction timeline with full hash, block number, user, `action`, `bizType`, request data, error and event timestamp | The sample contains `PlaceOrder` and `CancelOrder`, not fill quantities/prices. Keep transaction activity distinct from executions and cashflows. |

Preserve `cumRealized` as a provider-reported position field with unknown period/reset and fee/funding inclusion until qualified; it is not an account-level historical P&L curve. Preserve both outer transaction `timestamp` and inner request `data.timestamp`. Neither the latest transaction time nor the request time proves the balances' snapshot time; retain `retrievedAt` and leave an unavailable `sourceTimestamp` null.

For the supplied `PlaceOrder`, `quantity=0.001` and `price=76202.3` are requested order terms. `error=null` does not establish a full fill, execution price or current open-order state; IOC can partially fill or expire, and a post-only order can later be canceled. Preserve string IDs without floating-point conversion, parse string booleans explicitly, and keep placement/cancellation as distinct transactions even when they share a client order ID. A normalized projection of the supplied first action is:

```json
{
  "source": "asterExplorer",
  "origin": "upstreamNative",
  "kind": "orderSubmission",
  "sourceAction": "PlaceOrder",
  "sourceBizType": "PERP",
  "sourceLogHash": "0x92ef1465aed03e465fde73c10bffe135b881bc80e7e820a0ecbd08552032d95e",
  "blockNumber": "328062350",
  "timestamp": 1789115718300,
  "requestTimestamp": 1789115718256,
  "nativeSymbol": "BTCUSDT",
  "marketType": "perp",
  "orderId": null,
  "clientOrderId": "16500436755621487988",
  "requestedSide": "buy",
  "requestedAmount": "0.001",
  "requestedPrice": "76202.3",
  "orderType": "LIMIT",
  "timeInForce": "IOC",
  "reduceOnly": false,
  "sourceError": null,
  "executionStatus": "unverified",
  "executedAmount": null,
  "executedPrice": null,
  "realizedPnl": null
}
```

This projection supports an account activity feed without pretending the order is a trade. It is not an implemented Ferris response. Decode known `bizType` values (`PERP` to `perp`); retain unknown actions/products visibly rather than guessing a cashflow or dropping them. Sanitize retained native request data: credentials/signatures are not frontend activity fields.

#### Documented public Chain RPC: balances, orders and actual fills

Official documentation provides a second public source: JSON-RPC 2.0 at `POST https://tapi.asterdex.com/info`, with `Content-Type: application/json`. The methods below take an address without account credentials. They are separate from both Explorer's `{type,user}` protocol and signed REST V3. Each documented method has weight 1; an aggregate RPC/Explorer rate ceiling was not established. [A5]

| Method | Positional `params` | Data / bounds |
| --- | --- | --- |
| `aster_getBalance` | `[address, "latest"]` | Documented perp asset balances, grouped positions, staking and `accountPrivacy`; the research response also included separate `spotAssets`. |
| `aster_openOrders` | `[address, symbolOrNull, "latest"]` | Futures open orders, up to 1,000; null/empty symbol requests all symbols. Only orders created at/after the documented genesis boundary. |
| `aster_userFills` | `[address, symbolOrNull, fromMs, toMs, "latest"]` | Futures execution symbol, side, price, quantity and time; up to 1,000 per response, maximum seven-day window. |
| `aster_spotOpenOrders` | `[address, symbolOrNull, "latest"]` | Separate Spot open orders; same documented all-symbol option, cap and genesis restriction. |
| `aster_spotUserFills` | `[address, symbolOrNull, fromMs, toMs, "latest"]` | Separate Spot executions; same documented all-symbol option, cap and maximum window. |

For fill queries, omitted bounds default to a seven-day interval as documented. Send explicit bounds from Ferris instead. The documented earliest `from` is `1772678119418`; a query starting before that boundary returns empty. Clip a spanning request to the supported lower bound and report earlier missing coverage rather than silently interpreting the original range as having no trades. Only `"latest"` is established for `blockTag`; do not promise historical balances at arbitrary blocks. [A5]

The following new read-only research request returned HTTP 200 and three Futures fills without credentials:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "aster_userFills",
  "params": [
    "0xb6F576165Ec156be28F2cc895bcb6d880cd4d997",
    null,
    1789115700000,
    1789115720000,
    "latest"
  ]
}
```

Observed fills were SOLUSDT buys of `14.6` at `100.02`, `15.25` at `100.04`, and `12.96` at `100.04`. Assuming qualified linear base-asset quantities, exact-decimal price-times-quantity gives `4282.4204` quote units across the three returned rows. This checks the conversion arithmetic, not instrument-unit qualification, lifetime/window completeness or P&L. No trade ID, order ID, fee, funding, realized P&L or transaction hash was returned in these fill rows; unavailable fields stay null. Do not attach an Explorer order hash using timestamp/price similarity.

The independent `aster_getBalance` probe also returned HTTP 200 with `accountPrivacy=disabled`, perp and Spot asset arrays, positions and staking. Its position rows omitted several fields present in the documentation's example (including entry/mark price and leverage), while including `cumRealized`; `spotAssets` was present beyond the documented field table. Treat these as optional observed fields, not a reason to reject useful positions or claim every deployment returns the complete example. Preserve negative wallet balances (the observed perp USD1 value was `-210.13412114`). Different retrieval times are different snapshots, not evidence that Explorer and RPC balances should be added or forced to match.

#### Privacy, identity and source selection

- Public capabilities are conditional on the account/product's visible scope. RPC returns `accountPrivacy`. The docs state that privacy for open orders/fills applies to new orders after enablement; older completed orders remain visible. `enabled` with some rows is therefore possible. Return visible data with `PRIVACY_RESTRICTED` coverage, not fabricated zero history or a claim that the whole account needs a signer. [A5]
- The RPC staking documentation specifies null amount fields for zero amounts and omission under privacy. Only apply that zero convention to the qualified, present, public staking fields. Omitted/hidden sections and an unknown schema do not become zero assets.
- A privacy-restricted result is not a transport failure: do not work around it by switching to another public source, replaying a stale unrestricted snapshot, or requesting a generic signer. Previously visible historical records follow their documented visibility; never silently describe them as complete current history. Qualify cache invalidation/retention when privacy changes.
- Use provider sources `asterExplorer`, `asterRpc`, and `asterV3` within the existing exchange ID `aster`. Prefer documented, product-separated RPC state for the default overview; Explorer remains a first-class source for transaction activity and its observed snapshot fields. Reuse an already-needed `userDetails` response across sections, but do not fetch every source automatically or add duplicate balances. Record field/section provenance when enriching one snapshot.
- Public lookup covers the supplied address. No public master/child enumeration is established here. With `includeSubaccounts=true`, retain this account's data and report unresolved child coverage instead of inventing children, claiming complete discovery or rejecting the public account wholesale. Signed child discovery remains optional.

#### Historical coverage and reconstruction

| Deliverable | Public path and remaining inputs |
| --- | --- |
| Holdings/positions/staking now | Explorer `userDetails` or RPC state; retain raw units, negative balances, field gaps and privacy scope. |
| Transaction/action timeline | Explorer `txs`; exposes the supplied orders/cancellations without a private connection. Upstream pagination, retention and sort remain unqualified. |
| Trades and volume/count over time | RPC fill methods, separated by product and denomination. Bucket actual executions, not `PlaceOrder` quantities, cancellations or changes in open positions. |
| Balance curve from observation | Opt-in snapshots per asset and ledger; report native quantity, observation start and gaps. No valuation is needed for a per-asset balance curve. |
| Equity curve from observation | Qualify asset prices, liabilities, collateral mode, unrealized P&L and staking inclusion; avoid adding exposure/notional or an already-included component twice. |
| Earlier gross realized trading P&L | Complete ordered executions and a known opening signed position/cost basis or verified flat start; hedge/settlement rules must be known. The public RPC fields alone do not establish these inputs. |
| Net realized / total P&L or historical equity | Additionally require qualified fees/funding, external cashflows, liabilities/staking movements and historical marks as applicable. Public data may supply these after qualification; signed income/fill APIs are an optional enrichment, not an inherent requirement. |

Explorer `userDetails.txs` has no demonstrated continuation fields in the supplied excerpt. A finite returned slice is useful, but no guessed `offset`, `page`, timestamp parameter or extra `type` may be treated as supported. Local `since`/`until` filtering does not prove range exhaustion; report `hasMore: null` when continuation/completeness is unknown. Polling an overlapping recent slice cannot guarantee that no transactions were missed between observations.

RPC fills have a documented cap but no documented cursor, stable fill ID, tie-break ordering or precise time-bound inclusivity. Partition qualified requests into at most seven-day windows; split capped windows further under the work budget. Once boundary semantics are qualified, assign each returned timestamp to one half-open owned window and preserve row multiplicity. Do not deduplicate identical `(time,symbol,side,price,qty)` rows: they can be distinct fills. Buffer a fetched window for Ferris page continuations or replace its persisted window atomically on refresh; a response-local row position is not a native execution ID. If a timestamp bucket still exceeds the cap or ordering cannot be resolved, report `HISTORY_GAP` rather than skip rows or promise exact replay. Pin the provider/product/window in cursors and do not concatenate RPC and V3 executions without a qualified shared identity. [A5]

No native equity/P&L chart is established by these sources. Deliver public state, action history and actual fills now in the public rollout; make specific chart measures depend on accounting evidence, not on collecting credentials.

#### Optional signed REST enrichment

Use **V3** for new signed REST integrations. Futures base is `https://fapi.asterdex.com`; Spot base is `https://sapi.asterdex.com`. These V3 account reads require `user`, approved `signer`, a microsecond `nonce`, and an EIP-712 signature. A wallet address alone is insufficient for these signed routes, not for the public Explorer/RPC sources above. [A1], [A2]

| Signed read | Portfolio data | Bounds / scope |
| --- | --- | --- |
| `GET /fapi/v3/balance` | Asset balance, cross-wallet balance/P&L, availability and withdrawal capacity | Futures collateral, not Spot inventory. Weight 5. |
| `GET /fapi/v3/accountWithJoinMargin` | Account totals, assets, positions, margin use and account flags | Totals include USDT-focused fields; retain denomination and margin mode. Weight 5. |
| `GET /fapi/v3/positionRisk` | Entry/mark/liquidation price, side, amount, leverage, isolated/cross margin, unrealized P&L | Symbol optional; zero-position rows are not holdings. Preserve `BOTH`, `LONG`, `SHORT`. Weight 5. |
| `GET /fapi/v3/userTrades` | Private executions, commission asset/amount, realized P&L, order/trade IDs, side, position side, quote quantity | **Symbol required**. Default 500/max 1,000. Time window at most seven days; `fromId` cannot be combined with time filters. Weight 5. |
| `GET /fapi/v3/income` | Realized P&L, funding, commissions, transfers and other income types | Default 100/max 1,000; default recent seven days. Qualify actual backfill retention and pagination. Weight 30. |
| `GET /fapi/v3/openOrders`, `/fapi/v3/allOrders`, order lookup | Current/historical orders | All-orders history is symbol-specific with short query windows. Unfilled canceled/expired orders older than seven days disappear under documented rules. |
| `GET /fapi/v3/getSubAccountList` | Authorized child account IDs/names/parent identity | Relationship discovery does not imply arbitrary public account access or every child read permission. |
| `GET /api/v3/account` | Spot balances | Separate Spot API, authorization and ledger. Futures calls do not cover Spot. Weight 5. |
| `GET /api/v3/userTrades` | Spot executions, quote quantity, commission, account participation and IDs | Default 500/max 1,000; default recent seven days and maximum seven-day time interval. `fromId` excludes time filters. The parameter table makes symbol optional while the description refers to a trading pair; qualify all-symbol support separately. Weight 5. |
| `GET /api/v3/openOrders`, `/api/v3/allOrders` | Spot open/historical orders | Qualify Spot-specific filters and retention instead of inheriting Futures rules. |

Sources: [A1], [A2].

The V3 nonce section specifies microsecond timestamps, a plus/minus 60-second validity window and the most recent 100 nonces per agent, while the example text specifies a 10-second tolerance. Treat this discrepancy as a signing qualification gate; do not hard-code an unverified tolerance or reuse one agent nonce concurrently. [A1], [A2]

**Signed REST lifecycle requirements:**

- Since 2026-03-25, new V1 API-key creation is disabled; existing keys remain documented as usable. Legacy HMAC is not the recommended new Portfolio integration. [A3]
- Since 2026-09-01, authenticated Spot/Futures V3 account/order/trade routes require a prior deposit by the linked main wallet. Until then they return `-5050`, `This function can only be used after deposit`. Public market, Agent Wallet and Builder endpoints are exceptions. Ferris should explain the prerequisite, never initiate a deposit. [A3]
- A missing-signature research request returned `-1000`, `Signature check failed` on V3 balance only. That observation does not restrict the separate public Explorer/RPC sources.

Signed trades/income can enrich fees, execution IDs and P&L components, but do not independently prove every historical marked-to-market balance. The snapshot/reconstruction rules above apply to both public and connected data. No native account equity curve was found in the researched V3 documentation either.

**Read-only security gate for signed enrichment only:** validate an agent with trading/withdrawal permissions disabled against every intended USER_DATA read. Documentation does not establish that such an agent can read all required routes. The hosted service must not request a wallet seed, master private key, or uploaded agent private key. A user-controlled/server-operator-controlled signer can be a later approved connection mechanism, but it must bind signatures to exact allowlisted read operations and account identity. If the vendor signature does not cryptographically bind method/path, enforce restrictions at a trusted signer boundary and assess cross-route replay. Do not call a generic signing proxy read-only. Public Aster support does not wait for this gate.

Aster Builder account endpoints cover an authorized builder relationship and may respect privacy restrictions; they are not the public-address source used here. Transfer-creation routes are mutations, not cashflow-history sources, and must never be called by Portfolio. Public Explorer and Chain RPC reads have their own access contracts. [A1], [A4], [A5]

**Realtime candidate:** signed listen-key creation/renewal and account updates on the Futures user stream. A listen key lasts 60 minutes; a WS connection has a 24-hour lifetime. Renew, reconnect, preserve event timestamps, and reconcile with REST after gaps; arrival order alone is insufficient. [A1]

**Qualification gates:** public privacy/zero/field semantics, product and child scope, Explorer continuation/retention, RPC boundary/cap/identity behavior, rate budgets, balance/staking inclusion, `cumRealized` semantics, and cashflow/fee/funding/mark inputs for charts. Separately qualify least-privilege signed reads, deposited-main-wallet requirements, signing/nonce tolerance and richer V3 history. Signed-source gates must not disable the already-public sources.

## 5. Account selection and scope

### 5.1 Two selection forms

Every multi-source read uses exactly one of:

1. `address`: a 20-byte EVM wallet address. Public discovery runs only on selected exchanges. No stored credentials are silently attached to an address.
2. `accounts`: explicit protocol selectors, optionally carrying an opaque `connectionId`. This supports private accounts and direct Lighter account indexes without requiring an EVM address in every request.

Protocol selectors:

| Exchange | Accepted target identity |
| --- | --- |
| `hyperliquid` | `address`; optionally resolve verified subaccounts. |
| `lighterxyz` | `address` for discovery, or decimal-string `accountIndex` for one concrete account; optional read-only `connectionId`. |
| `extended` | `connectionId`; optional decimal-string `accountId`. Without a concrete ID, resolve the connection's permitted accounts. |
| `aster` | User `address` for public Explorer/RPC reads; optional `connectionId` for signed enrichment. Child enumeration is separately qualified, not implied by an address lookup. |

An explicit account index/ID targets only that account. Wallet selectors may use `includeSubaccounts` (default `true`). A subaccount selector must not silently expand upward to the master. Mainnet is the initial deployment scope; a testnet deployment must use a different namespace/cache/store and never mix with mainnet results.

### 5.2 Resolution rules

- Validate address bytes; keep the submitted/checksummed form for display, normalize comparisons by bytes. Do not treat a Starknet address as an EVM address or infer address format from string length alone.
- Resolve the complete selected account set within resource limits. If it is too large, return `ACCOUNT_SELECTION_REQUIRED` and a discovery continuation, not a silently chosen first account or first N accounts.
- Discovery is separately pageable through `/v1/portfolio/accounts` so a frontend can select accounts before requesting an expensive overview.
- Verify connection authorization against upstream account identity. A claimed account ID or wallet in JSON is not proof that the connection may access it.
- Deduplicate account selections after resolution. Conflicting connection identities/scopes for the same target are a validation error, not an opportunity to try several credentials.
- Classify personal accounts, children, vaults, public pools, staking pools, and system accounts before aggregation. Unknown types remain visible but are excluded from personal-wealth totals with a reason.
- Canonical account identity includes exchange, environment/network, and native account ID/address. Canonical component identity adds ledger/product/DEX scope as needed.
- An operated vault or pool is a separate entity. Count only the user's ownership claim in personal wealth; do not count other investors' capital.
- For Aster public lookup, the requested address can resolve even when its child relationships cannot. Return the known account with explicit incomplete child coverage when requested; `includeSubaccounts=false` avoids promising that wider scope. Do not infer a parent account ID from a position-ID prefix.

### 5.3 Product and Hyperliquid DEX scope

The overview covers the selected account's supported **trading envelope**, not a fictitious sum of independent spot/perp wallets. Report included ledgers, supported Hyperliquid DEX names, account mode, and excluded components.

Reuse the existing `spot` and `perp` market-type labels for positions/activity. Filtering positions to perps does not make unified collateral perp-only. If a requested history/product partition cannot be separated reliably, return `UNSUPPORTED_SCOPE` for that series rather than relabel an all-account curve.

For Hyperliquid, `params.hyperliquid.dexes` may select native/builder DEXs after capability qualification. Default to the explicitly advertised qualified set. Unknown/unqualified names are rejected or reported as unsupported; never label native-DEX-only coverage as all DEXs. Fills returned across DEXs must be filtered by the same selected scope without refetching them once per DEX.

## 6. Proposed endpoints

All routes below are proposals, not current Ferris functionality.

| Route | Purpose and reason for separation |
| --- | --- |
| `POST /v1/portfolio` | Fast selected-account overview: summaries, balances, positions, optional open orders and investments. No implicit history scan. |
| `POST /v1/portfolio/history` | Per-asset balance, equity, P&L, volume/count and funding series, queried independently of the overview. Multi-exchange or single-exchange. |
| `POST /v1/portfolio/trades` | Paginated executions and trade-related position events for **one concrete account**, optionally one symbol. Includes public Lighter Explorer history and Aster RPC fills; does not turn order submissions/cancellations into fills. |
| `POST /v1/portfolio/ledger` | Paginated funding/cashflow/income/investment events for **one concrete account** and a selected event family. Supports public Lighter Explorer activity as well as authorized native sources. |
| `POST /v1/portfolio/activity` | Bounded transaction/action timeline for **one concrete account**; initially Aster Explorer `txs`. Separate from executions, current open-order state and the economic ledger; no implied cross-source merged feed. |
| `POST /v1/portfolio/accounts` | Wallet/connection discovery and account selection. Does not imply fetching every account's detailed history. |
| `GET /v1/portfolio/capabilities` | Locally served supported identities, access requirements, components, history modes, scopes and limits. No account secrets or live fanout. |

Order-state history can later use a dedicated `/v1/portfolio/orders` route if consumers need it. `/activity` is the transaction/action log, not an alias for that state history; current open orders belong in the overview. Do not add aliases such as `fetchPortfolio` alongside the canonical Portfolio routes.

Private connection management and tracking have separate lifecycle boundaries; see Sections 11 and 13. They are not market-data parameters.

### 6.1 Overview request

Fields:

| Field | Meaning / default |
| --- | --- |
| `address` or `accounts` | Exactly one selection form from Section 5. |
| `exchanges` | Address mode: omitted means the four Portfolio candidates in canonical order. Explicit-account mode: omitted means the listed exchanges. An explicit filter cannot contradict the account list. |
| `includeSubaccounts` | `true` for wallet/connection discovery; does not expand a concrete account selector. |
| `include` | Default `summary`, `balances`, `positions`. Optional `openOrders`, `investments`. Only requested sections and necessary dependencies are fetched. |
| `valuation` | Optional quote-conversion policy. Native-denomination values are always retained. See Section 9. |
| `params` | Optional object of supported exchange-specific read options, initially qualified Hyperliquid DEX selection. Not arbitrary URL/header/query passthrough. |

Unknown fields, exchange IDs, sections, contradictory selectors, blank selections, and invalid limits are rejected before network work. CEX IDs are not valid Portfolio selections even though those adapters exist elsewhere in Ferris.

Address-only overview, with two selected exchanges:

```json
{
  "address": "0x1111111111111111111111111111111111111111",
  "exchanges": ["hyperliquid", "lighterxyz"],
  "includeSubaccounts": true,
  "include": ["summary", "balances", "positions"]
}
```

Fast single-exchange overview:

```json
{
  "address": "0x1111111111111111111111111111111111111111",
  "exchanges": ["hyperliquid"],
  "includeSubaccounts": false,
  "include": ["summary", "positions"]
}
```

Aster public overview, using the supplied address without a connection:

```json
{
  "address": "0xb6F576165Ec156be28F2cc895bcb6d880cd4d997",
  "exchanges": ["aster"],
  "includeSubaccounts": false,
  "include": ["summary", "balances", "positions", "investments"]
}
```

Return public source values even when normalized NAV or particular margin fields remain unavailable. Reuse each fetched account payload across requested sections. Do not call signed endpoints first or require a deposit/signature for this public query.

Explicit connected-account selection; identifiers below are illustrative, not real credentials:

```json
{
  "accounts": [
    {
      "exchange": "lighterxyz",
      "accountIndex": "722720",
      "connectionId": "pc_example_lighter"
    },
    {
      "exchange": "extended",
      "accountId": "123",
      "connectionId": "pc_example_extended"
    }
  ],
  "include": ["summary", "balances", "positions", "openOrders"]
}
```

For an address-only request selecting Extended, return an `AUTH_REQUIRED` source outcome immediately without probing private routes. Aster instead uses public Explorer/RPC capabilities and reports source-specific privacy/coverage limits. Missing a connection alone is not an Aster-wide error; a separately requested signed-only component can still report `AUTH_REQUIRED` without erasing public results.

### 6.2 History request

Reuse multi-source selection. Add:

- `metrics`: subset of `balance`, `equity`, `pnl`, `realizedPnl`, `volume`, `tradeCount`, `funding`. `balance` is a per-asset, per-ledger quantity series, not mixed-currency NAV. Trade counts count qualified executions/participations, not transaction actions or every trade-related log row.
- `range`: either `{ "period": "day|week|month|year|all" }` or `{ "since": milliseconds, "until": milliseconds }`.
- `interval`: `native` by default for upstream chart series; qualified `1h` or `1d` resolution for resampling or event-derived volume/count/P&L buckets. When an event-derived series is requested with `native`, use `1d` buckets and report the effective interval explicitly; never upsample a native chart beyond its source resolution.
- `aggregate`: `false` by default. `true` requests an additional compatible aggregate; it never removes the per-source series.
- Optional qualified `marketType` (`spot` or `perp`). Unsupported partitions are explicit outcomes. For Aster execution-derived metrics, omission requests separate qualified product series with independent coverage, not an unlabeled sum of Spot and Futures; selecting one product limits the work to it.

Period requests select provider-native windows where available. Explicit ranges are half-open `[since, until)` in Ferris. Never imply that a rolling upstream window supports an arbitrary old range; report actual coverage and unsupported sections.

```json
{
  "address": "0x1111111111111111111111111111111111111111",
  "exchanges": ["hyperliquid", "lighterxyz"],
  "metrics": ["equity", "pnl", "volume"],
  "range": { "period": "month" },
  "interval": "native",
  "aggregate": false
}
```

Lighter public execution-volume history uses the same `/history` route, without a connection:

```json
{
  "accounts": [{ "exchange": "lighterxyz", "accountIndex": "248287" }],
  "metrics": ["volume", "tradeCount"],
  "range": { "since": 1787788800000, "until": 1787961600000 },
  "interval": "1d",
  "aggregate": false
}
```

The response identifies `lighterExplorer` / `derivedFromEvents` and actual traversal coverage. Requesting `pnl` additionally requires the reconstruction inputs in Section 4.3; insufficient inputs produce an accounting/coverage reason, not `AUTH_REQUIRED` merely because no connection was provided. Derived `realizedPnl` must state `grossTrading` versus `netRealized` basis and the fee/funding exclusions.

Aster uses the same history contract. Public RPC fills support `volume` and `tradeCount` with `origin: derivedFromEvents`; `balance` uses opted-in per-asset observations, and `equity`/P&L require the accounting inputs in Section 4.4. No observed baseline produces `HISTORY_GAP`, not a fictional historical zero or a blanket `AUTH_REQUIRED`. A public fills request can omit `symbol`; signed Futures V3's symbol requirement does not apply to RPC history.

### 6.3 Trade, ledger and activity requests

Use one resolved `account` selector, the same object shape used in `accounts[]`. First-page fields are optional `since`/`until`, `limit` (default 100, maximum 500), and optional `symbol`; provider limits can be lower. Freeze an omitted `until` to request-start time, and an omitted `since` to seven days before that upper bound. Reject reversed ranges. A continuation repeats the identical selection/filters and adds an opaque `cursor`; omitted bounds retain the frozen cursor bounds rather than being recomputed.

`marketType` selects `spot` or `perp` where supported and is required for Aster execution pages and product-specific signed ledger reads because their endpoints differ. It is optional for Aster Explorer `/activity`, where omission retains visible mixed transaction products and a supplied filter uses qualified `bizType` mappings. Do not infer the ledger from an ambiguous native symbol. Unsupported event-family/product combinations return `UNSUPPORTED_SCOPE`.

```json
{
  "account": {
    "exchange": "hyperliquid",
    "address": "0x1111111111111111111111111111111111111111"
  },
  "since": 1788825600000,
  "until": 1789084800000,
  "limit": 100
}
```

Lighter public first-page example using a concrete index from the supplied trade:

```json
{
  "account": { "exchange": "lighterxyz", "accountIndex": "248287" },
  "since": 1787788800000,
  "until": 1787961600000,
  "limit": 100
}
```

No `connectionId` is required. The initial Lighter history source is Explorer. A supplied connection can select qualified native history for enrichment, but the selected source is reported and pinned for pagination; do not switch or concatenate sources mid-cursor. A native auth failure cannot retroactively make public Explorer capability private.

Aster public RPC Futures and Spot fills accept an omitted symbol (`symbolRequired: false`). Signed Futures V3 `/userTrades` separately requires one (`symbolRequired: true`); all-symbol private enrichment still needs a qualified instrument-history index and bounded fanout. Neither that native restriction nor currently open positions should limit the public all-symbol query. Do not silently switch sources midway through a cursor.

`/ledger` adds `family`: `funding`, `cashflows`, `income`, or `investments`. The response reports actual included event types, source and accounting status. A Lighter public cashflow/investment request uses the corresponding Explorer event families, not the trade-only filter. Do not sum mint/burn representations with their associated transfers or concatenate overlapping income and execution data and sum both.

Aster `/activity` uses the same concrete account, range, optional symbol/product and bounded page fields, without `family`. It returns typed transaction actions rather than completed orders or fills. Example:

```json
{
  "account": { "exchange": "aster", "address": "0xb6F576165Ec156be28F2cc895bcb6d880cd4d997" },
  "since": 1789115700000,
  "until": 1789115720000,
  "limit": 100
}
```

The same account/range plus `marketType: perp` requests public Futures executions from `/trades`; it still needs neither a symbol nor a connection. `/activity` filters only the supplied/qualified Explorer slice until a real upstream continuation is established. Retained rows can have `coverage: unknown`, `hasMore: null` and no cursor; do not represent that as an exhausted account history. Unknown/nonexecution action types remain visible and do not contribute to executed volume or the cashflow ledger.

Trade/ledger/activity page responses contain `timestamp`, `status`, `accountKey`, provider `source`, `items`, `order` (qualified ascending or descending, otherwise unknown), frozen `since`/`until`, `nextCursor`, `hasMore`, and `coverage`. `hasMore` is `true` when continuation is available, `false` only when the requested range is known exhausted, and `null` when caps/retention/unqualified pagination prevent establishing exhaustion. An unavailable page has `items: null`; an authoritative empty page has `items: []`. Returned items can coexist with partial/unknown coverage; local filtering never proves upstream exhaustion.

Discovery responses group `accounts`, `nextCursor`, `coverage` and access reasons by exchange. Each discovery continuation selects one exchange and preserves the original wallet/connection authorization. There is no global cursor spanning four unrelated discovery mechanisms.

Client-side merged activity is formed from independently paginated account pages. Do not advertise a globally sorted complete all-exchange activity cursor until a persisted, indexed merged ledger exists.

## 7. Response and capability contract

### 7.1 Envelope and availability

Overview/history responses contain:

- `timestamp`: Ferris response generation time, not the age of every upstream value.
- `status`: `ok`, `partial`, or `unavailable` for requested work.
- `selection`: normalized exchanges and resolved account keys, plus discovery completeness.
- `sources`: ordered source results, one per concrete account or unresolved exchange-level outcome.
- `aggregate`: compatible known totals/series and explicit excluded sources; `null` when nothing can be aggregated.

Each source includes `exchange`, `accountKey` if resolved, account/component scope, `retrievedAt`, available upstream `sourceTimestamp`, and requested `sections`. Sections/rows/series identify their provider (`asterExplorer`, `asterRpc`, `asterV3`, etc.) when an account uses multiple sources; repeated descriptions of the same asset/position are enrichments, not additive holdings.

Each requested section has:

| Field | Contract |
| --- | --- |
| `status` | `ok`, `empty`, `partial`, or `unavailable`. `empty` requires a successful, authoritative empty result. |
| `data` | Typed object/list or `null`. Unavailable data is not `0`, `[]`, or a fabricated unchanged value. |
| `reason` | Stable code when not fully available; nullable for successful complete data. |
| `coverage` | `complete`, `partial`, or `unknown`, with scope, exclusions, bounds, and continuation where applicable. Complete always means complete for a named request/scope, not lifetime completeness. |
| `freshness` | Source/retrieval times, cache age, and stale flag where relevant. Unknown source time remains unknown. |

Structural capabilities are separate from runtime status. A timeout does not make a supported feature unsupported. An unimplemented or unverified feature is not a temporary upstream failure.

Stable reasons include `AUTH_REQUIRED`, `AUTH_EXPIRED`, `AUTH_SCOPE_MISMATCH`, `PRIVACY_RESTRICTED`, `ACCOUNT_NOT_FOUND`, `ACCOUNT_DEPOSIT_REQUIRED`, `ACCOUNT_SELECTION_REQUIRED`, `UNSUPPORTED_SCOPE`, `NOT_IMPLEMENTED`, `UNVERIFIED`, `UPSTREAM_TIMEOUT`, `UPSTREAM_DATA_INVALID`, `RATE_LIMITED`, `RETENTION_LIMIT`, `VALUATION_UNAVAILABLE`, and `HISTORY_GAP`.

An account that exists with zero equity is different from a missing account. Extended's documented zero-balance 404 is a narrowly qualified adapter translation, not a global HTTP rule. A Lighter chart's `account not found` must not erase a separately successful account snapshot.

### 7.2 Typed data

| Type | Fields / semantics |
| --- | --- |
| Account summary | Native reported balance/equity, equity basis, normalized NAV when qualified, unrealized P&L, available collateral, withdrawal capacity, gross/net exposure, margin requirements, account mode, included components. |
| Asset balance | Native asset ID and symbol, total/free/locked quantities where meaningful, liabilities, native mark/index price, market value and risk-adjusted collateral contribution separately. |
| Position | Stable position identity, account, native and unified symbol, market/DEX, side, absolute quantity and signed exposure, entry/mark/liquidation price, notional, leverage/margin mode, unrealized/realized P&L and funding with scope. |
| Trade / position event | Account participation identity, source log hash/type and nullable native trade/order IDs, timestamp, native/unified market, side, quantity, price, quote notional, liquidity role, fee asset/amount, realized P&L, position effect and provider source. Preserve raw status/fees. For position-exit or partially decoded events, unavailable side/quantity/P&L are null and position effect is unknown; event inclusion is independent of accounting eligibility. |
| Transaction activity | Account, provider source, full transaction hash/block, raw action/product, event/request timestamps, error and qualified action kind. For order actions, retain order/client IDs and requested terms separately from nullable execution details. A successful submission/cancellation is not an execution or economic cashflow. |
| Ledger event | Account, native event ID, event family/type/status, asset and signed amount, separately represented fee, source/destination account/product, transaction hash where available, event/update timestamps. |
| Investment | Vault/pool/staking/lending component identity, ownership quantity/share claim, current value, cost basis if available, lock/unlock state, source inclusion in parent totals. |
| Native statistics | Metric name/value/unit, provider period, account/product scope, native methodology. Win rate, ROI, Sharpe, profit factor and margin ratios are not additive. |

Native statistics from chart/performance sources can be returned with history results for the same requested period. Do not fetch them implicitly on every overview refresh.

All monetary amounts, prices, quantities, rates and native IDs that may exceed JavaScript's exact integer range are **decimal strings**. Time values are integer Unix milliseconds; dates from date-only APIs retain a source date/timezone field until conversion is qualified. Parse upstream JSON numeric accounting values losslessly where possible; serializing an already rounded `f64` as a string does not recover precision.

Use existing market catalogs and symbol mappings. Keep unknown/delisted raw market and asset IDs visible, with `symbol: null` and a normalization reason where necessary. Dropping an unrecognized holding would make the portfolio falsely complete.

### 7.3 Partial aggregate example

This is a **synthetic contract example**, not an upstream response. It illustrates a known net asset value in USDC with an excluded exchange; detailed source sections are represented here only by their summary projection.

```json
{
  "timestamp": 1789084800000,
  "status": "partial",
  "selection": {
    "exchanges": ["hyperliquid", "extended"],
    "accountKeys": ["hyperliquid:mainnet:0x1111111111111111111111111111111111111111"],
    "discoveryComplete": false
  },
  "sources": [
    {
      "exchange": "hyperliquid",
      "accountKey": "hyperliquid:mainnet:0x1111111111111111111111111111111111111111",
      "status": "ok",
      "retrievedAt": 1789084799000,
      "sourceTimestamp": 1789084798000,
      "sections": {
        "summary": {
          "status": "ok",
          "reason": null,
          "data": {
            "netAssetValue": { "amount": "1250.00", "currency": "USDC" },
            "equityBasis": "netAssetValue",
            "includedComponents": ["trading"]
          },
          "coverage": { "status": "complete", "scope": "selectedTradingAccount" },
          "freshness": { "stale": false }
        }
      }
    },
    {
      "exchange": "extended",
      "accountKey": null,
      "status": "unavailable",
      "reason": "AUTH_REQUIRED",
      "sections": {
        "summary": {
          "status": "unavailable",
          "reason": "AUTH_REQUIRED",
          "data": null,
          "coverage": { "status": "unknown", "scope": "unresolvedAccount" },
          "freshness": null
        }
      }
    }
  ],
  "aggregate": {
    "totalsByCurrency": [{ "currency": "USDC", "knownNetAssetValue": "1250.00" }],
    "complete": false,
    "includedAccountKeys": ["hyperliquid:mainnet:0x1111111111111111111111111111111111111111"],
    "excluded": [{ "exchange": "extended", "reason": "AUTH_REQUIRED" }]
  }
}
```

### 7.4 HTTP semantics

- `400`: malformed/contradictory request, unknown field/section, invalid range/cursor/identity.
- `422`: a syntactically valid but unsupported exchange or explicit impossible selection, consistent with the newer `fetchMarkets` invalid-exchange pattern.
- `401`/`403`: Ferris caller authentication/authorization failure for private connections or tracking. Do not leak whether another tenant's connection ID exists.
- `429`: Ferris admission/quota failure before execution; bounded upstream throttling appears in source outcomes after execution has begun.
- `200`: valid execution envelope, including `partial` and `unavailable`. This rule is the same for one exchange and several; it allows useful diagnostics even when every selected source needs credentials.
- `5xx`: Ferris execution/infrastructure failure that prevents a valid result envelope, not merely one failed exchange.

Request errors use the established nested shape `{ "error": { "code": "...", "message": "..." } }` used by `fetchMarkets`. Do not change existing endpoints' error formats as part of this feature.

### 7.5 Capability metadata

`/capabilities` returns versioned, deployment-local metadata, not account results. Each exchange entry lists accepted identity forms, account/environment scope, qualified components, and feature records:

| Capability field | Meaning |
| --- | --- |
| `support` | `supported`, `unsupported`, or `unverified` for the stated upstream/account scope. |
| `implementation` | `available`, `disabled`, or `notImplemented` in this Ferris deployment. Researched support is not implemented support. |
| `access` | `public`, `readOnlyConnection`, `connection`, `signedConnection`, or `unknown`. Use generic `connection` for Extended until effective least privilege is established. |
| `accountTypes` / `components` | The concrete kinds/products/DEXs covered; a public-pool exception is a separate scope from ordinary accounts. |
| `historyModes` / `source` | Qualified `upstreamNative`, `derivedFromEvents`, and/or `observedSnapshots`, plus provider (`lighterExplorer`/`lighterNative`, `asterExplorer`/`asterRpc`/`asterV3`). List source-specific capability/access records; no delivered history source is an empty history-mode set, not a demand for credentials by default. |
| `limits` | Page/range/retention bounds, resolutions, sort, required symbol/product selectors, and supported filters. Unknown upstream retention is explicit, not unlimited. |
| `updateMode` | On-demand REST, native stream, observed polling, or unavailable; Ferris WS delivery alone does not imply a native upstream stream. |

For example, ordinary-account Lighter snapshots and Explorer execution/activity history have public upstream support; execution volume/count are `derivedFromEvents`. Native `/api/v1/trades` is a separate connected-source capability. Ordinary-account native `/pnl` remains unverified; public-pool chart success must not change that record. Reconstructed gross/net P&L has its own state/scaling/coverage prerequisites, not an unconditional connection requirement. Runtime credential failure and timeout remain response outcomes, not edits to these structural facts. [L1], [L3], [L4], [L13]

Aster public Explorer snapshots/activity and RPC balances/fills are distinct from signed V3 enrichment. Advertise RPC all-symbol support, product/window/genesis/cap bounds and privacy conditions; public Spot fills/open orders are documented but were not exercised by the research probes. Native RPC fill rows are `upstreamNative`, their volume buckets are `derivedFromEvents`, and collected balance curves are `observedSnapshots`. Public privacy restrictions and unknown accounting inputs are not equivalent to a missing connection. [A4], [A5]

## 8. History and pagination semantics

### 8.1 Series metadata

Every series identifies:

- Account/component scope, metric and native currency; balance-quantity series additionally identify the asset/ledger and `unit: assetQuantity`.
- `kind`: `snapshot`, `cumulative`, or `interval`.
- `basis`: e.g. `assetQuantity`, `netAssetValue`, `marginEquity`, `exchangeReportedPnl`, `cashflowAdjustedPnl`, `grossTrading`, `netRealized`, or `executedNotional`. Gross reconstructed trading P&L explicitly excludes unqualified fees/funding and is never silently summed with a net native measure.
- `origin`: `upstreamNative`, `derivedFromEvents`, or `observedSnapshots`, plus provider source. A graph built from public Explorer events is derived history even though the source events themselves are public upstream data.
- Requested and actual start/end, source period/resolution, timezone/bucket boundaries, baseline, earliest available observation, and gaps.
- Fee/funding/cashflow/product inclusion when known; otherwise explicit unknown semantics.
- Whether a point is current/still forming and whether the response is sampled or downsampled.

A balance/equity curve is already sufficient for the requested Portfolio graph. Do not make its delivery depend on proving a more complex P&L calculation.

### 8.2 Bounded pagination

- A Ferris cursor is opaque, versioned, expiring, integrity-protected and bound to account, exchange, provider source, connection authorization scope where applicable, filters, frozen upper time bound and sort direction. It contains no secret or caller-supplied URL. Sources without a qualified continuation must not fabricate one.
- Continuations cannot change account, symbol, event family, time range, or sort. Credential rotation/revocation rechecks authorization even if a cursor is otherwise valid.
- Provider-native sort/window constraints are advertised. Do not claim one universal descending order if an adapter has not implemented it without losing data.
- Time-based upstream pagination must overlap/own boundaries according to qualified inclusivity and stable event identity where available. For ID-less Aster RPC fills, preserve within-window multiplicity and buffer/replace complete owned windows instead of tuple-based event deduplication. Never blindly advance `lastTimestamp + 1` when a full page may contain more records at that timestamp.
- If an upstream timestamp bucket exceeds its retrievable cap and has no tie-break cursor, return explicit incomplete coverage rather than a false `hasMore: false`.
- Freeze `until` on the first activity page so newly arriving trades do not move the page boundary. Track sparse pages, repeated cursors and nonprogress without an infinite loop.
- For Hyperliquid, distinguish a response cap from the last-10,000-fill retention limit. For Aster RPC, enforce the genesis floor/seven-day windows/1,000-row cap and qualify splitting; Explorer has a separate unresolved continuation contract; signed V3 has its own symbol/window rules. For Lighter/Extended native APIs, preserve native cursor semantics. Lighter Explorer's string `offset` is separate and not yet fully specified; do not silently change provider source.
- A finite request may return fewer rows than the caller's limit with a continuation. Completion is determined by provider exhaustion/qualified coverage, not only by page length.
- Native chart `all`/`allTime` results have their own retention/sampling metadata and no invented record cursor.

## 9. Financial definitions and aggregation

### 9.1 Three different values

1. **Balance:** a provider ledger/collateral amount; may exclude unrealized P&L or non-collateral assets.
2. **Margin equity:** provider risk-adjusted equity, potentially with haircuts and special collateral rules.
3. **Net asset value (NAV):** economic market value of owned assets/claims plus derivative P&L, minus liabilities, with every component counted once.

Expose native values even when normalized NAV is unavailable. Cross-exchange totals must not mix margin equity from one venue with unhaircut NAV from another and call the result total wealth.

For a simple cash-settled perp account, equity is often collateral plus unrealized P&L. That formula is not universal for unified, multi-collateral, isolated, borrowing, vault, or spot accounts. Establish a source inclusion map before using any formula.

### 9.2 Currency policy

Default aggregates are `totalsByCurrency` over compatible **normalized NAV** values. Preserve USDC, USDT, and other denominations; do not silently add unlike currencies.

For frontend display, an optional initial policy may request:

```json
{
  "valuation": {
    "currency": "USD",
    "stablecoinPolicy": "parity"
  }
}
```

This produces an explicitly **estimated** USD value for qualified USD-stablecoin-denominated components. It must include `valuationMethod: stablecoinParity`, an estimate flag, component prices/times and any unpriced assets. USDC/USDT parity is a chosen approximation, not verified USD exchange value. A depeg-aware FX/price source is a separate prerequisite before advertising exact USD valuation. Other quote currencies are initially unsupported.

Spot/claim valuation can use a qualified protocol mark/index in the source collateral currency. Missing/stale prices leave assets visible and valuations unavailable; a missing price is not zero. Pool/vault ownership and borrowing liabilities must be included before a component qualifies for NAV.

### 9.3 P&L versus balance change

For an explicitly defined scope and currency, with complete matched cashflows:

`P&L(t0,t1) = Equity(t1) - Equity(t0) - NetExternalCashflow(t0,t1)`

Use cashflows in `(t0,t1]`, with deposits positive and withdrawals negative, after reconciling source timestamp conventions. A deposit of 100 with no market movement raises equity by 100 and produces zero P&L, not a profit of 100.

- A transfer between two selected components is internal only when both legs and ownership are reconciled. The same transfer can be external to a single-exchange view and internal to a selected cross-exchange view.
- Transfer/withdrawal fees are expenses, not silently removed with transferred principal. Define whether flow amounts are gross/net and account for fees exactly once.
- Pending/failed withdrawals are not automatically completed outflows. Lighter secure withdrawals can remain `claimable`; define the accounting recognition point instead of relying on one status name.
- Incomplete flows or mismatched equity scope make normalized P&L unavailable. An observed equity graph can still be returned.
- Qualified native P&L can avoid reconstructing accounting, but it is not the only valid source. Public execution replay can produce explicitly gross/net realized P&L and, with balances/flows/marks, equity/P&L. Label methodology and prerequisites; unknown fee/funding/cashflow inclusion prevents mixing derived and native values into a supposedly comparable total.
- Never subtract fees/funding again from a native net measure that already includes them. Lighter realized perp P&L includes funding; Aster income and trade realized-P&L rows can overlap.
- Aster Explorer/RPC `cumRealized` is a position-reported field with unqualified reset/inclusion rules, not a ready cumulative account series. Never sum it across repeated snapshots, drop closed positions from a supposed lifetime total, or manufacture cashflows from `PlaceOrder`/`CancelOrder` events.
- Rebase cumulative series at a qualified common baseline when comparing a range; do not sum different lifetime baselines. Interval P&L is the difference of a cumulative measure only when that measure is actually cumulative.
- Percentage returns, win rates, Sharpe ratios, leverage and margin ratios are not additive. No aggregate ROI is promised without a stated cashflow-aware return methodology.

### 9.4 Volume

Normalized execution volume is positive quote notional from unique account executions, in the execution's quote currency. Use verified native notional when available; otherwise apply the instrument's quantity/contract convention, not an unconditional price-times-contracts formula.

Bucket executions in UTC over half-open time ranges. Do not count an order submission, cancellation, deposit, market-wide volume, both REST and WS copies, or both fee and trade records as new execution volume.

A combined **account activity volume** sums the selected accounts' fills, including separate maker/taker participations when both selected accounts traded. It is not unique venue turnover. If unique cross-account trade volume is later offered, use a separate metric and qualified shared execution identity.

Prefer qualified native volume buckets when they match the requested account/product/bucket scope; otherwise bucket unique normalized executions. Hyperliquid fee-tier volume may be weighted/parent-aggregated. Lighter Explorer gives public execution price/size/time for derived volume even though native chart volume semantics remain unverified. Do not spread a native window total uniformly across days or count a size-less exit, share burn or funding wrapper as another execution.

Aster RPC fills can drive public volume/count buckets. Explorer order action quantities/prices describe intent; `error: null` does not turn their product into executed notional. Preserve identical-looking RPC fill rows within a fetched window because no unique fill ID is supplied, and disclose uncertainty across capped/refetched windows instead of silently deduplicating distinct trades.

### 9.5 Aggregate charts and missing data

- Keep a fixed contributor cohort for the selected range. Do not produce apparent gains/losses when a source appears/disappears or a child account is discovered late.
- Align compatible currencies, valuation policies, account components and timestamps before summing.
- Do not interpolate cashflow or volume events. For equity, native points are the default; any bounded last-known-value resampling must disclose maximum age and never fill before the first observation or across known gaps.
- A missing contributor yields a null/incomplete aggregate point plus known subtotal/coverage, not zero. Per-exchange series remain available.
- Date-only Extended observations require a qualified timezone and day-boundary definition before joining intraday curves.
- Never upsample a daily native chart into purported measured hourly history.
- Product/account-mode changes, schema changes, and valuation-policy changes start explicit coverage segments rather than silently joining incompatible data.

## 10. Execution, limits and caching

### 10.1 Query plan

1. Validate and normalize selectors/sections locally.
2. Enforce Ferris caller authorization for each connection.
3. Select exchanges **before** discovery, cache access, or network dispatch.
4. Resolve the selected account scope, or return a bounded discovery/selection outcome.
5. Build only the upstream requests needed for requested sections, reusing shared responses.
6. Execute selected exchanges concurrently with per-host/account limits and one deadline.
7. Normalize, assess freshness/coverage, then aggregate only qualified components.
8. Return independent failures without discarding successful sources.

A one-exchange overview must not call other adapters, warm their account caches, or wait for unrelated credential checks. Historical range work must not sit on the overview's critical path. Avoid automatic retry trees; only qualified idempotent reads can receive bounded retries within the original deadline.

### 10.2 Proposed initial service bounds

These are Ferris design defaults, **not vendor limits**. Tune from qualification measurements without weakening coverage reporting.

| Policy | Initial choice |
| --- | --- |
| Selected exchanges | Maximum 4; canonical supported set only. |
| Detailed accounts per overview | Maximum 20; larger discovery requires explicit selection, not truncation. |
| Request body | 32 KiB maximum. |
| Foreground deadline | 10 seconds total, with each transport timeout capped by remaining time. |
| Concurrency | At most 4 exchange tasks; at most 2 concurrent account-data reads per host initially, also subject to weighted budgets. |
| Activity page | Default 100/max 500 Ferris rows; obey lower native caps and bounded request work. |
| Chart output | Maximum 2,000 points/series and 10,000 total points; require coarser qualified resolution or a narrower range rather than silently dropping points. |
| Response size | Bounded; reject/mark a limited section with a continuation or actionable query-size reason before exhausting memory. |
| Cache freshness | Discovery about 60 seconds; overview about 5 seconds; live history about 30 seconds. Publish actual age. |
| Stale fallback | Only for transient failures, explicitly marked and bounded (initially 60 seconds for snapshots). Never bypass expired/revoked authorization. |

### 10.3 Upstream budgets

| Protocol | Researched limits relevant to design |
| --- | --- |
| Hyperliquid | 1,200 REST weight/min/IP; state reads listed at weight 2, `userRole` 60, other documented info reads generally 20, some histories add response-based weight. WS includes 10 unique user subscriptions. [H5] |
| Extended | Default 1,000 requests/min/IP; higher tiers exist by arrangement. Required User-Agent; 429 on throttling. [E1] |
| Lighter | Standard 60 unweighted requests/min, subject to the lower premium-derived per-endpoint ceiling; Plus/Premium 24,000 weighted/min; Builder 240,000 weighted/min. Trades weight 600, most account reads 300, inactive orders 100. Separate Explorer budget: 90 weighted/min, account routes weight 2. Authenticated native requests can use L1-based rather than IP-based limits; preserve tenant/account attribution. 429 or 405 can signal rate limiting, and REST/WS throttling interact. [L12] |
| Aster | Signed V3 route weights/IP rules: account reads typically 5, income 30; honor documented 429/418 behavior. Public Chain RPC methods above have weight 1, but no total RPC rate budget was established; Explorer rate limits are also unqualified. Maintain separate provider-host budgets and conservative bounded concurrency, not inherited Futures quotas or unlimited public scans. [A1], [A5] |

Share upstream host budgets with existing public-market workloads. Portfolio must not starve order-book/trade subscriptions or multiply the same wallet query per frontend. Never assume a public hosted service can query hundreds of Lighter accounts every few seconds on a standard IP budget.

For Lighter public historical scans, reserve and meter the Explorer bucket independently of native account reads. A maximum of 100 logs/page at weight 2 means at most 45 account-log calls/minute if that bucket is otherwise unused, before other Explorer work and safety headroom. Filter early, reuse market mappings, coalesce overlapping account pages and return continuations/coverage rather than claim an unlimited backfill fits the overview deadline.

### 10.4 Cache isolation

Cache by normalized exchange/environment/account/provider source/component/parameters, section, time window and valuation policy. Private data additionally requires tenant and connection authorization scope/version. Public account snapshots may share a public-source cache; private enrichments must not leak into it. Privacy-restricted results must not fall back to cached unrestricted current state.

Use bounded TTL/LRU entries and bounded request-coalescing locks. Evict unused lock keys; arbitrary public addresses must not grow a permanent map. Keep secrets out of cache keys, URLs, logs, metrics and cursors. A fresh HTTP fetch does not make an unchanged old upstream event freshly observed economic state.

## 11. Optional connected-account security

Address-only reads need no wallet signature. Connections are a separate, opt-in authorization feature; the initial public Portfolio release does not require a credential store.

### Recommended connection model

- Frontend/integrator authenticates to Ferris before creating/listing/revoking connections.
- A connection belongs to one tenant/principal and exchange/environment; its account permissions are verified upstream.
- Portfolio requests carry only an opaque `connectionId`, never raw secrets.
- A protected connection-management endpoint accepts a Lighter read-only token or a qualified Extended API key over TLS. Secrets are write-only in this API and encrypted at rest using a key kept outside the database.
- Aster signed V3 enrichment uses a separately qualified delegated signer connection, not a generic credential blob. Only this signed capability stays disabled until the boundary and least-privilege reads are proven; public Explorer/RPC access does not depend on it.
- In a self-hosted single-operator deployment, operator-provided local credentials/signers may be configured instead, still behind adapter read allowlists. Do not expose arbitrary signer URLs supplied by an unauthenticated caller.

Proposed lifecycle routes when connections ship: `POST /v1/portfolio/connections`, `GET /v1/portfolio/connections`, `DELETE /v1/portfolio/connections/{id}`. Creation returns nonsecret metadata, verified account scope, expiry, and capability/access outcomes. Never return stored tokens/keys. A new creation can replace a rotated credential; no compatibility aliases are needed.

### Mandatory controls before hosted private access

- No wallet seeds, master private keys, Stark private keys, or imported trading-agent private keys.
- Explicit risk disclosure for Extended keys until the SDK/prose privilege discrepancy is resolved. An adapter's read-only behavior does not make a stolen upstream key read-only.
- Allowlist exact upstream hosts, HTTP methods, read paths and body discriminators (Aster Explorer `userDetails` and qualified Chain RPC methods). Prohibit arbitrary `params` passthrough, arbitrary JSON-RPC methods, user-selected base URLs, and redirects carrying credentials. Never execute actions embedded in a public history response.
- TLS, authenticated connection ownership checks, restrictive private-route CORS, appropriate CSRF protection for cookie-based callers, and no credential-bearing query strings.
- Public `/v1/ws` topics must not gain private payloads. Private subscription keys include authorization scope, and every subscriber is checked.
- Redact keys/tokens/signatures, sensitive request bodies, and upstream error details. Public wallet queries also reveal user interests; avoid raw wallet labels in telemetry.
- Expiry/revocation/rotation invalidates connection caches, cursors as needed, workers and live subscriptions. Never serve private stale data after access revocation.
- Track private-data retention and deletion, including historical observations and backup policy. Deleting a Ferris connection does not necessarily revoke the upstream token; report that distinction and provide the vendor revocation instructions.
- Signed operations need clock synchronization, bounded nonce reuse, and provider-specific signature validation. No fallback to weaker or legacy auth on failure.
- Exports returning presigned URLs are secrets. If Ferris downloads an export, allowlist hosts, bound size/time, and do not relay arbitrary download URLs as a generic fetch service.

No credentials are needed to finish this planning document. Authenticated fixtures/vendor confirmations are implementation prerequisites, not reasons to block writing the spec.

## 12. Architecture for implementation

Suggested ownership, following existing repository organization:

| Area | Proposed responsibility |
| --- | --- |
| `src/models.rs` | Portfolio request/response/account/activity/series wire types and explicit typed adapter parameters; split into a model submodule only if size warrants it. |
| `src/exchanges/traits.rs` | Separate `PortfolioExchange` capability contract and account-specific errors. Keep `MarketDataExchange` and CEX behavior unchanged. |
| `src/exchanges/registry.rs` or a focused Portfolio registry module | Explicitly registered Portfolio adapters/capabilities. No synthetic unsupported implementations for every market adapter. |
| Existing four DEX adapter modules, with focused child modules as needed | Identity resolution, allowlisted account transports, native DTOs, pagination and normalization. Reuse market catalogs/clients where safe. |
| A focused `src/portfolio` service module | Selection, bounded fanout, section dependencies, cache/singleflight, provenance/coverage, aggregation, and history coordination. Not an ever-growing public realtime match statement. |
| `src/web.rs`, `src/main.rs` | Validation/handlers/router registration, private auth boundary, service injection into `AppState`. |
| `src/config.rs` | Only necessary limits, feature enablement, storage settings and deployment URLs. Document actual defaults when implemented. |
| Optional history/storage module | Durable observations, events, coverage/checkpoints, tracking lifecycle and migrations. |
| Existing tests/smoke workflows | Preserve old endpoints; add behavior-focused Portfolio coverage and live account smoke cases with consented fixtures. |

Core separation: transport models describe the API; adapters report native data plus provenance; the service controls work and aggregates. Adapters must not own global multi-exchange math. Avoid a single enormous `fetch_everything` method: account resolution, snapshot sections, native series and activity pages have different costs and access requirements.

Within `src/exchanges/lighterxyz`, keep native account history and Explorer event decoding as separate source paths behind the same Portfolio contract. Reuse the existing Explorer market catalog; add decoded-event mapping, account participation, settlement status, bounded offset traversal and source provenance. A credential failure in the native client must not disable the public Explorer client. Raw events can be retained for replay/reconciliation without requiring every frontend to implement protocol accounting.

Within `src/exchanges/aster`, distinguish public Explorer `{type,user}` reads, public Chain RPC envelopes and optional signed V3 transports behind the same Portfolio contract. Prefer product-separated RPC state; reuse a needed Explorer response for snapshot fields/activity without adding duplicate balances or doing implicit history scans. Keep JSON-RPC errors, provider privacy, schema omissions and signed authentication errors separate. Normalize transaction intent and actual fills into different types; reuse the existing public market catalog to qualify units/symbols. Source-specific history logic owns Explorer coverage and RPC window buffering/splitting, not a generic guessed cursor.

Use decimal arithmetic for financial transformations, bounded Tokio concurrency and tight lock scopes. Do not hold locks across network calls. Reuse parsed/shared upstream responses instead of cloning large account histories per section.

Before modifying exported Rust symbols during implementation, inspect references and migrate affected callsites. The plan itself does not change those symbols.

## 13. Durable observed history

### 13.1 When it is necessary

- Aster has public snapshots and fills but no established native equity chart. Opted-in public observations can supply per-asset balances and qualified equity; signed credentials are not inherently required.
- Hyperliquid fills have a bounded retention ceiling.
- Lighter public Explorer history benefits from indexed replay/checkpoints and retained market/state context; native charts and both sources' long-range coverage need qualification. Public trade pages do not require tracking enrollment or a private connection.
- Users may want consistent long-term comparisons beyond any provider window.

A plain Portfolio GET-like read must not silently register an indefinite tracking job. Tracking requires explicit opt-in, authenticated ownership of the Ferris tracking record, quotas and retention disclosure, even when the upstream data is public.

Proposed lifecycle when storage ships: `POST /v1/portfolio/tracking` to register a concrete account/component scope, `GET /v1/portfolio/tracking` for status/coverage, and `DELETE /v1/portfolio/tracking/{id}` to stop collection and apply the requested retention/deletion policy. A caller can track a publicly visible wallet without claiming to own that wallet; private tracking still needs a permitted connection.

### 13.2 Storage model and defaults

Use SQLite for an initial single-node self-hosted deployment, with transactions/migrations and a bounded async-safe writer. Multi-replica deployment requires a shared database/worker-ownership design before enabling distributed collection; do not pretend a local SQLite file coordinates replicas.

Logical records:

- Tracked accounts/component scope, consent/principal, collection state and start time.
- Snapshot observations with source/retrieval times, account mode, native values, prices/policy, normalized values and quality.
- Immutable/deduplicated account events where stable identity is qualified, with separate state updates for pending-to-settled transitions. ID-less Aster RPC fills instead retain multiplicity in versioned owned-window snapshots; replace a refreshed window atomically rather than append duplicates or invent native fill IDs.
- Coverage intervals/gaps, provider retention floors, checkpoints/cursors, last successful collection and normalization version.
- Nonsecret connection metadata; encrypted credentials are isolated from public data and accounting rows.

Suggested initial observation cadence is **60 seconds**, admission-controlled by actual upstream budgets. High-rate events/streams may supplement it, but event-only collection misses mark-to-market equity changes. Reserve budget for foreground queries and periodically reconcile REST snapshots after stream gaps.

Suggested retention: 30 days of minute observations plus 2 years of daily rollups, configurable and explicitly disclosed. Daily equity uses a defined closing observation with freshness; activity rollups sum complete interval events. Never compact across a known gap and call the day complete. Event retention must support the advertised derived-history period or preserve already-reconciled rollups and their provenance.

Expose `observedSince`, `lastSuccessfulObservation`, expected cadence and gaps. A restart/network outage cannot become a flat, apparently measured equity segment. Changing selected scope or valuation method creates a new segment.

### 13.3 Backfill

Prefer qualified native charts for old chart history and public Explorer/RPC/native event traversal for retained events. Backfills are budgeted background work, not unbounded overview work. Provider limits, missing older symbols, privacy, unresolved continuations and denied signed scopes remain visible. Lighter Explorer and Aster RPC can supply executions before Ferris begins observing; do not artificially restrict those trades to `observedSince` or mistake Aster's recent Explorer action slice for exhaustive history.

Pre-observation equity may be reconstructed when a complete known opening state, public execution/cashflow history, funding/fee interpretation, liabilities/investments and historical marks are available. Today's positions plus a partial set of old trades do not meet that requirement. If inputs are incomplete, still return public trades, derived volume and qualified realized-P&L segments, alongside native/observed equity and explicit gaps. No synthetic prehistory is allowed.

## 14. Rollout and acceptance

Each stage is a complete, explicitly scoped capability; unavailable components remain honest. Later stages are not excuses to mark an unimplemented first-stage behavior supported.

### Stage A: qualify adapters and freeze contracts

- Capture consented/public fixtures for ordinary accounts, zero accounts, subaccounts, unified/isolated/multi-asset modes and investment claims.
- Resolve field units, identity, source inclusion maps and each protocol's open gates.
- Freeze request/response types, decimal/ID handling, error states, pagination and capability reporting.
- Demonstrate that selected-exchange/selected-section planning performs no unrelated account calls.

### Stage B: useful public Portfolio

- Deliver `/portfolio`, `/accounts`, `/capabilities`, Hyperliquid public history/trades/ledger, and Lighter public snapshots plus Explorer-derived trades, execution volume/count and decoded cashflow/investment activity.
- Include Aster public Explorer/RPC snapshots and staking, Explorer `/activity`, and qualified RPC Futures/Spot fills, open orders and derived volume/count. Keep documented-but-not-exercised product methods behind their own implementation qualification, not a signer prerequisite.
- Return an explicit access outcome for Extended and source-specific privacy/field/history gaps for Aster/Lighter. Private-source gates must not turn public data into `AUTH_REQUIRED` or defer it to the connection stage.
- Show public assets/positions and activity, native Hyperliquid graphs, and Lighter/Aster execution-volume views without credential collection. Keep position-exit unknowns and order-intent versus execution distinctions visible.
- Keep current market-data endpoints and public WS behavior unchanged.

### Stage C: connected DEX accounts

- Add the protected connection lifecycle, optional Lighter native trade/order/funding/export enrichment and qualified native charts, and Extended account/native-chart coverage. This improves Lighter's source detail; it does not unlock its already-public Explorer history.
- Do not assert that a Lighter token fixes an unqualified `/pnl` route.
- Add richer signed Aster V3 reads only after signer/permission/deposit qualification; public Aster support is already in Stage B. Keep V3 product/symbol limits separate from all-symbol public RPC capabilities.
- Verify revocation, wrong-account access, tenant isolation, error redaction and private-cache behavior.

### Stage D: durable history and qualified aggregation

- Add explicit tracking, migrations, collection quotas, restore/restart behavior and retention controls.
- Deliver Aster per-asset balance observations and qualified equity from the first public or authorized observation. Earlier gross realized P&L needs a known position baseline and complete fills; net/total P&L additionally needs reconciled flows, fees/funding and marks. Unknown `cumRealized` semantics or order submissions are not substitutes.
- Add public Lighter chronological replay/checkpoints for qualified gross realized trading P&L; extend to net P&L/equity only after fee/funding/baseline/mark/cashflow requirements are met. This stage requires accounting evidence, not necessarily a private connection.
- Add cross-exchange chart aggregation only for compatible currencies, cohorts, scopes, bases and timestamps.
- Preserve per-source native statistics; do not aggregate incompatible returns or risk ratios.

### Stage E: optional enrichment/realtime

- Add qualified vault/pool/staking/lending claims, order history, bulk exports or private live updates when consumers require them.
- Respect Hyperliquid unique-user stream limits and protocol auth/reconnect semantics.
- Do not create always-on workers/streams for every anonymous address query.

### Behavioral acceptance checklist

| Scenario | Required observation |
| --- | --- |
| One exchange selected | No account requests/discovery for any unselected exchange; no latency dependency on them. |
| Summary/positions only | No fill/chart/backfill calls solely for unrequested sections. |
| One source times out or needs auth | Other results survive; aggregate lists exclusions and is not falsely complete. |
| All selected sources unavailable | Valid envelope with reasons and null aggregate, not a fabricated zero portfolio. |
| Empty account versus missing account | Distinct consumer-visible outcomes; route-specific empty semantics are tested. |
| Address has multiple children | Every selected child appears, or account selection is explicitly required; no `[0]` shortcut. |
| Unified account / risk haircut | Shared collateral counted once; risk equity not mislabeled NAV. |
| Pool operator also owns shares | Only the owned claim counts toward personal wealth; no full pool NAV addition. |
| Deposit of 100, no market movement | Equity increases 100; qualified P&L remains 0. |
| Transfer between selected accounts | Principal cancels internally only with matched legs; fees still affect P&L once. |
| Native net P&L plus fee/funding rows | No duplicate deductions/income. |
| Replayed fill / overlapping pages / WS bootstrap | One account execution contribution after deduplication. |
| Full page with identical timestamps | No skipped boundary events; unresolvable cap is marked incomplete. |
| Large native IDs / decimal quantities | Lossless wire representation and stable equality/deduplication. |
| Lighter address/index without a connection | Public Explorer trades and volume are planned/executed without calling private native history first. |
| `InternalClaimOrder` with `pubdata_type=Trade` | Returned as an execution; maker/taker side follows participant index and `is_taker_ask`, not the transaction name. |
| Maker buyer and taker seller in the supplied sample | Correct per-account role/side and quote notional; fee-only participant does not gain a trade. |
| `ExitPosition` or `ExitPositionWithFunding` | Included in broad trade-related history; missing quantity/P&L remains unknown rather than fabricating a close, open or liquidation. |
| `TradeWithFunding` or two representations of one transfer/share movement | Execution counted once; raw funding prefix not added as an amount; cashflow principal not counted twice. |
| Explorer committed/verified row later becomes executed | Raw activity remains visible; settled accounting counts the same event once. A `nothing_to_execute` status alone does not establish an execution. |
| Explorer hash placeholder/collision or equal-time events | No truncated-hash deduplication or arbitrary P&L ordering; report ambiguity where identity/order cannot be resolved. |
| Earliest retrieved trade begins mid-position | Trade/volume still available; realized P&L requires a known state, not a fictional zero opening position. |
| Explorer offset page overlaps or native source is later connected | No source switch inside a cursor; preserve gaps and deduplicate only with qualified event identity. |
| Unknown asset / stale price | Holding remains visible; valuation/aggregate is incomplete, not zero. |
| Native series lacks a day or a source | No synthetic zero or silently changing contributor cohort. |
| Aster public RPC versus signed Futures V3 query without symbol | Public RPC supports all-symbol fills; only the signed V3 source has the required-symbol restriction. Neither snapshots nor Explorer activity inherit it. |
| Aster address without a connection | Public balances/positions/staking/activity and visible RPC fills remain available; no signed probe, deposit or nonce prerequisite. |
| Aster order submission/cancellation with `error: null` | Visible transaction actions, but execution quantity/price/P&L remain unknown; no invented volume or open-order state. |
| Aster client-order ID above JavaScript's safe integer range | Exact string retained; cancel and placement remain distinct transactions even when linked by the same client ID. |
| Aster `cumRealized`, exposure and mixed assets | No artificial P&L curve, no notional added to wealth, no unpriced AFEE/ASTER or unlike collateral silently treated as dollars. |
| Aster RPC missing documented position fields or negative asset balance | Useful raw state survives; unknown fields remain null and liabilities retain their sign. |
| Aster privacy enabled with visible old fills | Keep permitted rows with restricted coverage; omitted state is not zero and stale/public fallback does not bypass privacy. |
| Aster RPC identical fill rows or a capped timestamp bucket | Preserve row multiplicity; split qualified windows within budget or report a gap, not tuple-deduplicate or skip the boundary. |
| Aster Explorer action slice has no qualified cursor | Return bounded useful activity and unknown continuation/coverage, not complete lifetime history or guessed pagination. |
| Revoked/expired/wrong-tenant connection | No private cache, cursor or stream data escapes; error does not disclose another tenant. |
| Process restart/outage while tracking | Checkpoints resume idempotently and observation gaps remain visible. |
| Scope/account-mode/valuation change | Explicit coverage segment, not an unexplained jump presented as profit. |

Implementation verification should use deterministic tests for these plausible failure modes, plus consented live smoke requests for successful account paths, auth failures and source-specific pagination. Use the existing `cargo fmt -- --check`, `cargo test`, and endpoint smoke conventions after code is implemented. Do not add tests that merely pin wording or check mocked field forwarding.

Update `README.md` and `INTEGRATION_README.md` with actual routes, access requirements, examples, env settings and coverage once they exist. The present planning change intentionally leaves executable documentation and source code unchanged.

## 15. Research evidence and remaining gates

### 15.1 Executed read-only checks

These probes were research requests to upstream APIs, **not tests of a Ferris Portfolio implementation**. No authenticated private account was used, no mutation was sent, and no browser automation was used.

| Probe | Observed result | What it proves / does not prove |
| --- | --- | --- |
| Hyperliquid `POST /info`, `type=portfolio`, zero address | HTTP 200; all eight documented window labels and `accountValueHistory`, `pnlHistory`, `vlm` fields | Public route/shape evidence only. Not economic correctness for funded/unified accounts. |
| Lighter `/account` for indexes `1` and `722720` | HTTP 200; account `722720` active, type 0 | Public snapshot access to those sampled accounts. |
| Lighter `/pnl` for the same ordinary accounts | HTTP 400, `21100`, `account not found` | Native chart access/availability unresolved; not a demonstrated auth error. |
| Lighter `/pnl` for LLP `281474976710654` | HTTP 200; second-scale point timestamps, including trading/pool/flow/volume fields | Public-pool chart example only; no extrapolation to ordinary accounts. |
| Lighter native `/api/v1/trades` for `722720` without credentials | HTTP 400, `20001`, empty auth header/query message | Auth rejection for that native route only; does not restrict public Explorer account logs. |
| Extended current-production equity chart without key | HTTP 401 | Authentication enforced on the probed route; schema not validated. Other researched portfolio routes also rejected unauthenticated requests. |
| Aster signed Futures V3 balance without signature | HTTP 400, `-1000`, `Signature check failed` | Rejection on the signed route only, not a limit on public Explorer/RPC data. No read-only signer qualification. |
| Lighter account WS probes | Upgrade failed with an expected-101 transport error | No conclusion about normal-account channel authorization. |
| Aster public RPC `aster_getBalance` for the user's address | HTTP 200, `accountPrivacy=disabled`, perp/Spot assets, positions and staking | Public state for this account; observed optional-field differences and negative USD1 balance. Not complete privacy/schema/NAV qualification. |
| Aster public RPC `aster_userFills`, null symbol, `1789115700000` to `1789115720000` | HTTP 200, same returned bounds, three SOLUSDT buy fills | Actual public Futures executions without credentials or a symbol filter. Not a Spot test, retention test, complete pagination proof or P&L validation. |

User-provided public Explorer evidence, accepted as observed without re-requesting the accounts to confirm it:

- [Address activity supplied by the user](https://explorer.elliot.ai/api/accounts/0x61CEeF212fF4a86933C69fb6aca2fe35D8F2A62B/logs): decoded execution payload and activity examples including deposits, transfers, share mint/burns and settlement states.
- [Filtered public trade-related history supplied by the user](https://explorer.elliot.ai/api/accounts/0xE887c1fa720698B55803a22011140cE140bed00a/logs?pub_data_type=LiquidationTrade,LiquidationTradeWithFunding,ExitPosition,ExitPositionWithFunding,Trade,TradeWithFunding): evidence that liquidation/trade/exit families can be requested together. The plan additionally includes documented deleveraging families.
- Official Explorer account-log, detail and market schemas were read directly for the correction. They establish public event fields and declared filters/limits, not a complete offset/retention/order contract. [L13], [L17], [L18]
- A throwaway exact-decimal mapping of the supplied trade produced taker sell `0.98512` at `79481.0`, timestamp `1787833805748`, and notional `78298.322720` quote units; it also exercised maker/taker reversal, fee-only account exclusion, nonexecuted-accounting exclusion, one execution for the funding variant and preservation of two-role participation. This validates the example transformation, not an implemented backend or complete historical P&L.
- Aster `POST https://explorer.asterdex.com/explorer` with `type=userDetails` and the user's address: supplied balances, two positions, staking and three shown transaction actions are accepted as public evidence. No repeat POST was used to confirm them. The excerpt does not establish all event families or pagination. [A4]
- Official Aster Chain RPC documentation was read, then the two separate read-only probes above were executed with no credentials. Spot fill/open-order methods and privacy-enabled behavior are documented, not live-tested here. [A5]
- An in-memory Aster projection preserved the supplied decimal balances, large IDs, string `false`, event/request times and distinct placement/cancellation actions without assigning execution data. The three observed RPC fills produced `4282.4204` quote units using exact-decimal arithmetic; identical-looking fill rows retained multiplicity. This exercises example conversion, not a Ferris implementation, complete accounting or upstream uniqueness guarantees.

### 15.2 Decisions that do not require a user clarification now

Proceed with public-first, optional connected DEX accounts; a fast overview plus separate history/activity; native per-exchange values and honest coverage; explicit opt-in storage; and no private signing-key collection. These choices preserve useful functionality without claiming data the protocols do not expose.

Before claiming full four-DEX coverage, obtain the following evidence:

1. **Hyperliquid:** confirmed balance/P&L inclusion for each account mode and selected builder DEX; qualified chart ranges and fee-volume scope.
2. **Extended:** authorized chart payloads and date/bucket semantics; effective key privileges; account/subaccount permission scope; zero-balance and transfer-sign reconciliation.
3. **Lighter:** qualify Explorer offsets/sort/retention, address/child scope, full event identity and exit/liquidation/deleverage mapping; fee/share/funding-prefix scales, replay baseline and marks for P&L. Native `/pnl`, token scope, native history/export behavior and ambiguous WS access remain separate optional-source gates, not prerequisites for public trades/volume.
4. **Aster:** public privacy/field/zero/product/child-scope semantics; Explorer continuation/retention; RPC genesis/boundary/cap/identity/sort and host quotas; balance/staking inclusion and P&L baseline/flows/fees/funding/marks. Separately qualify signed USER_DATA privilege/deposit/signature rules and richer V3 history. Do not make signed gates prerequisites for public account data.
5. **Aggregation:** source inclusion maps, denomination/valuation policy, fixed cohorts, observed-history retention and quota behavior.

These are **implementation qualification gates**, not assertions that research has verified private data or a reason to withhold the completed plan.

## 16. Primary sources

References identify official protocol documentation or official SDK code used in this research. Repository statements refer to the inspected Ferris paths in Section 3. No internal agent artifacts are required to follow the sources.

[H1]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint.md
[H2]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals.md
[H3]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot.md
[H4]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions.md
[H5]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits.md
[H6]: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees.md

- [H1] Hyperliquid general account info: fills, roles, subaccounts, portfolio, fees, vaults, staking, abstraction and lending.
- [H2] Hyperliquid perpetual account state, DEX selectors and user ledger/funding.
- [H3] Hyperliquid spot balances and unified-account source-of-truth warning.
- [H4] Hyperliquid WS subscriptions, snapshot semantics and trade identity.
- [H5] Hyperliquid request weights and user-specific stream limits.
- [H6] Hyperliquid fee-volume weighting and subaccount aggregation.

[E1]: https://api.docs.extended.exchange/
[E2]: https://api.docs.extended.exchange/#get-account-equity-history
[E3]: https://github.com/x10xchange/python_sdk/tree/starknet
[E4]: https://raw.githubusercontent.com/x10xchange/python_sdk/starknet/x10/clients/rest/modules/order_management_module.py

- [E1] Extended official API: authentication, account/spot balances, positions, activity, streams and limits. Inspect the raw HTML if reader extraction omits portfolio sections.
- [E2] Extended native portfolio entry point. Related official anchors: [PnL](https://api.docs.extended.exchange/#get-account-pnl-history), [cumulative PnL](https://api.docs.extended.exchange/#get-cumulative-account-pnl-history), [summary](https://api.docs.extended.exchange/#get-account-portfolio-summary), [health](https://api.docs.extended.exchange/#get-account-health), [performance](https://api.docs.extended.exchange/#get-account-performance), [funding history](https://api.docs.extended.exchange/#get-account-funding-history).
- [E3] Official Starknet SDK. The researched [partial OpenAPI](https://github.com/x10xchange/python_sdk/blob/starknet/specs/rest-api.openapi.yaml) and [account module](https://github.com/x10xchange/python_sdk/blob/starknet/x10/clients/rest/modules/account_module.py) do not establish the raw-page chart schemas.
- [E4] Official cancel/mass-cancel client behavior; [HTTP helper](https://raw.githubusercontent.com/x10xchange/python_sdk/starknet/x10/utils/http.py) supplies API-key headers. This is the least-privilege discrepancy to resolve.

[L1]: https://apidocs.lighter.xyz/reference/account-1.md
[L2]: https://apidocs.lighter.xyz/reference/accountsbyl1address.md
[L3]: https://apidocs.lighter.xyz/reference/pnl.md
[L4]: https://apidocs.lighter.xyz/reference/trades.md
[L5]: https://apidocs.lighter.xyz/reference/positionfunding.md
[L6]: https://apidocs.lighter.xyz/docs/historical-data.md
[L7]: https://apidocs.lighter.xyz/reference/export.md
[L8]: https://apidocs.lighter.xyz/docs/api-keys.md
[L9]: https://docs.lighter.xyz/trading/pnl-and-total-account-value.md
[L10]: https://docs.lighter.xyz/trading/multi-asset-margin.md
[L11]: https://apidocs.lighter.xyz/docs/manage-public-pools-shares.md
[L12]: https://apidocs.lighter.xyz/docs/rate-limits.md
[L13]: https://apidocs.lighter.xyz/reference/get_accounts-param-logs.md
[L14]: https://apidocs.lighter.xyz/docs/websocket-reference.md
[L15]: https://apidocs.lighter.xyz/reference/accountinactiveorders.md
[L16]: https://apidocs.lighter.xyz/reference/transfer_history.md
[L17]: https://apidocs.lighter.xyz/reference/get_logs-hash.md
[L18]: https://apidocs.lighter.xyz/reference/get_markets.md

- [L1], [L2] Native account fields and wallet-to-index discovery.
- [L3], [L4], [L5] Native charts, private trades and funding history/access rules.
- [L6], [L7] History/export windows and the funding-window discrepancy.
- [L8] API-key versus read-only-token permissions; [token creation and scope](https://apidocs.lighter.xyz/reference/tokens_create.md).
- [L9], [L10] P&L/funding inclusion, isolated collateral, risk-adjusted multi-asset value; [Unified Trading Accounts](https://docs.lighter.xyz/trading/unified-trading-accounts.md).
- [L11] Pool shares and ownership; [pool metadata](https://apidocs.lighter.xyz/reference/publicpoolsmetadata.md).
- [L12] Native/Explorer weights, tier limits and cooldown (page updated 2026-09-08).
- [L13] Documented public account-log filters and decoded trade/exit/deleverage/cashflow/share payloads; a first-class public history source. Also [assets](https://apidocs.lighter.xyz/reference/get_accounts-param-assets.md), [positions](https://apidocs.lighter.xyz/reference/get_accounts-param-positions.md).
- [L14] Account stream fields and authentication examples.
- [L15] Inactive orders; [active orders](https://apidocs.lighter.xyz/reference/accountactiveorders.md), [client-ID lookup restrictions](https://apidocs.lighter.xyz/reference/accountorders.md).
- [L16] Transfers; [deposits](https://apidocs.lighter.xyz/reference/deposit_history.md), [withdrawals](https://apidocs.lighter.xyz/reference/withdraw_history.md).
- [L17], [L18] Detailed log block/batch metadata and Explorer market-index/symbol catalog for normalization and ordering qualification.
- Additional discovery references: [official API index](https://apidocs.lighter.xyz/llms.txt), [official SDK](https://github.com/elliottech/lighter-python), [asset metadata](https://apidocs.lighter.xyz/reference/assetdetails.md).

[A1]: <https://raw.githubusercontent.com/asterdex/api-docs/master/V3(Recommended)/EN/aster-finance-futures-api-v3.md>
[A2]: <https://raw.githubusercontent.com/asterdex/api-docs/master/V3(Recommended)/EN/aster-finance-spot-api-v3.md>
[A3]: https://raw.githubusercontent.com/asterdex/api-docs/master/README.md
[A4]: https://explorer.asterdex.com/explorer
[A5]: https://raw.githubusercontent.com/asterdex/api-docs/master/RPC/aster-chain-rpc.md

- [A1] Recommended Aster Futures V3: signing, balances, positions, private trades/income, orders, subaccounts, builder scope and user streams.
- [A2] Recommended Aster Spot V3: separate product base and signed account/activity access.
- [A3] Official lifecycle notice: V1 key creation and the September 2026 deposit prerequisite. Legacy routes are research context, not the recommended new integration.
- [A4] Aster Explorer POST source supplied by the user: `userDetails` balances, positions, staking and transaction actions. This link is an API endpoint, not a GET schema/documentation page; public evidence does not establish a complete pagination contract.
- [A5] Official Aster Chain RPC: address-public balances, Futures/Spot open orders and fills; privacy, genesis, seven-day windows and 1,000-row limits. Futures balance/fill probes were exercised separately; additional observed fields and untested scopes are identified above.
