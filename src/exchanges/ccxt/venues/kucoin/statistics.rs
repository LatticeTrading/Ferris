//! KuCoin bulk statistics. Spot and contract tickers are separate stock
//! endpoints, so the shared acquisition issues one call per product family.
use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{finish, run_call, Acquired},
            fields::{
                funding_field, invalid_pair, missing_pair, not_requested_pair, observed,
                price_value, volume_field,
            },
            scalars::{nonneg_number, positive_integer, raw_lexical},
            Receipt,
        },
        statistics_profile::Profile,
        venue::{CatalogScope, Provider, Venue},
    },
    models::{
        FetchMarketStatsParams, FundingKind, FundingRateUnit, MarketStatsField,
        MarketStatsFieldName, MarketStatsSourceFailure, MarketStatsValue, OpenInterestValue,
        UnifiedMarketType,
    },
};
use ccxt::{value::get_value_k, Params, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

pub(in crate::exchanges::ccxt) use super::super::defaults::{
    statistics_noop_param as accepts_noop_param, statistics_selection as selection_matches,
};

const TICKERS_SPOT: &str = "kucoin:ccxt:fetchTickers(spot)";
const TICKERS_CONTRACT: &str = "kucoin:ccxt:fetchTickers(contract)";
const FUNDING: &str = "kucoin:ccxt:fetchFundingRates";
const OPEN_INTEREST: &str = "kucoin:ccxt:fetchOpenInterests";

use crate::models::UnifiedMarketType::{Future, Perp, Spot};

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    if !params.include_bulk {
        return;
    }
    let venue = Venue::Kucoin;
    let products = catalog_products(scope);
    if products.contains(&Spot) {
        let result = run_call(
            provider,
            venue,
            "fetch_tickers",
            vec![
                Value::Null,
                Params::new().with_str("type", "spot").into_value_object(),
            ],
        )
        .await;
        let rows = finish(venue, TICKERS_SPOT, result, acquired, failures);
        acquired.rows.insert(TICKERS_SPOT, rows);
    }
    if products
        .iter()
        .any(|product| matches!(product, Perp | Future))
    {
        let result = run_call(
            provider,
            venue,
            "fetch_tickers",
            vec![
                Value::Null,
                Params::new().with_str("type", "swap").into_value_object(),
            ],
        )
        .await;
        let rows = finish(venue, TICKERS_CONTRACT, result, acquired, failures);
        acquired.rows.insert(TICKERS_CONTRACT, rows);

        let result = run_call(
            provider,
            venue,
            "fetch_funding_rates",
            vec![Value::Null, Params::none().into_value_object()],
        )
        .await;
        let rows = finish(venue, FUNDING, result, acquired, failures);
        acquired.rows.insert(FUNDING, rows);

        let result = run_call(
            provider,
            venue,
            "fetch_open_interests",
            vec![Value::Null, Params::none().into_value_object()],
        )
        .await;
        let rows = finish(venue, OPEN_INTEREST, result, acquired, failures);
        acquired.rows.insert(OPEN_INTEREST, rows);
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let spot = entry.market.market_type == Spot;
    let (source, ticker) = if spot {
        (TICKERS_SPOT, data.row(TICKERS_SPOT, entry))
    } else {
        (TICKERS_CONTRACT, data.row(TICKERS_CONTRACT, entry))
    };
    match name {
        MarketStatsFieldName::LastPrice => ticker_price(
            source,
            ticker.row,
            ticker.error,
            ticker.receipt,
            if spot { "last" } else { "lastTradePrice" },
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            source,
            ticker.row,
            ticker.error,
            ticker.receipt,
            if spot { "vol" } else { "volumeOf24h" },
            if spot { "volValue" } else { "turnoverOf24h" },
        ),
        MarketStatsFieldName::MarkPrice if !spot => ticker_price(
            source,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "markPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::IndexPrice if !spot => ticker_price(
            source,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "indexPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::Funding if !spot => {
            let funding = data.row(FUNDING, entry);
            let interval_ms = funding
                .row
                .map(|row| {
                    positive_integer(&get_value_k(
                        &get_value_k(row, "info"),
                        "currentGranularity",
                    ))
                })
                .unwrap_or(None);
            funding_field(
                FUNDING,
                funding.row,
                funding.error,
                funding.receipt,
                "nextFundingRate",
                FundingRateUnit::DecimalFraction,
                FundingKind::Estimate,
                interval_ms,
                Some("fundingTime"),
            )
        }
        MarketStatsFieldName::OpenInterest if !spot => {
            let interest = data.row(OPEN_INTEREST, entry);
            let Some(row) = interest.row else {
                return missing_pair(OPEN_INTEREST, interest.error);
            };
            let info = get_value_k(row, "info");
            match nonneg_number(&get_value_k(&info, "openInterest")) {
                Err(()) => invalid_pair(OPEN_INTEREST, interest.receipt),
                Ok(None) => missing_pair(OPEN_INTEREST, interest.error),
                Ok(Some(amount)) => (
                    observed(
                        OPEN_INTEREST,
                        MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: Some(amount),
                            open_interest_value: None,
                        }),
                        positive_integer(&get_value_k(row, "timestamp")),
                        interest.receipt,
                    ),
                    interest.receipt.map(|receipt| receipt.at),
                ),
            }
        }
        _ => not_requested_pair(),
    }
}

/// Price from the unified row's `timestamp` (contract `ts` nanoseconds are
/// already normalized by stock; spot bulk rows carry the aggregate `time`).
#[allow(clippy::too_many_arguments)]
fn ticker_price(
    source: &'static str,
    row: Option<&Value>,
    error: Option<&'static str>,
    receipt: Option<Receipt>,
    key: &str,
    base: &str,
    quote: &str,
) -> (MarketStatsField, Option<Instant>) {
    let Some(row) = row else {
        return missing_pair(source, error);
    };
    let info = get_value_k(row, "info");
    if !info.as_map().is_some_and(|info| info.contains_key(key)) {
        return missing_pair(source, error);
    }
    let Some(receipt) = receipt else {
        return missing_pair(source, error);
    };
    match price_value(raw_lexical(&info, key), base, quote) {
        Some(value) => (
            observed(
                source,
                value,
                positive_integer(&get_value_k(row, "timestamp")),
                Some(receipt),
            ),
            Some(receipt.at),
        ),
        None => invalid_pair(source, Some(receipt)),
    }
}

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "kucoin:ccxt:loadMarkets",
    products: &[Spot, Perp, Future],
    unsupported: &[(
        MarketStatsFieldName::LastSettledFunding,
        "last-settlement-not-provided",
    )],
    funding_kinds: &[FundingKind::Estimate],
    hourly: false,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: true,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "funding-next-period-estimate",
        "open-interest-amount-is-native-contracts",
        "contract-volume-in-native-units",
        "contract-acquisition-includes-all-contract-types",
    ],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    match scope {
        CatalogScope::Spot => &[Spot],
        CatalogScope::Default => &[Spot, Perp, Future],
        // Contract acquisition cannot separate linear/inverse/dated rows.
        _ => &[Perp, Future],
    }
}

pub(in crate::exchanges::ccxt) fn normalize(
    scope: CatalogScope,
    _product: Option<UnifiedMarketType>,
    _selectors: &Map<String, JsonValue>,
) -> JsonValue {
    // Keep the product selector in the normalized params so all-market
    // projection selects the requested product instead of defaulting to perps.
    match scope {
        CatalogScope::Spot => json!({"type": "spot"}),
        CatalogScope::Inverse => json!({"category": "inverse"}),
        CatalogScope::Linear => json!({"type": "swap"}),
        _ => json!({}),
    }
}

pub(in crate::exchanges::ccxt) fn acquisition_params(params: &JsonValue) -> JsonValue {
    params.clone()
}
