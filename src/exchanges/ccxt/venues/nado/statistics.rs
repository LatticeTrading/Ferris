//! Two bulk calls: all tickers plus contracts (through fetch_funding_rates).
//! The latter retains mark/index/OI in info; do not fetch the same endpoint a
//! second time for OI or fan out by symbol.
use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{acquire_tickers, finish, run_call, Acquired},
            fields::{
                invalid_pair, missing_pair, not_requested_pair, observed, price_field, volume_field,
            },
            scalars::{nonneg_number, positive_integer, raw_lexical},
        },
        statistics_profile::Profile,
        venue::{CatalogScope, Provider, Venue},
    },
    models::{
        FetchMarketStatsParams, FundingKind, FundingRateUnit, FundingValue, MarketStatsField,
        MarketStatsFieldName, MarketStatsSourceFailure, MarketStatsValue, OpenInterestValue,
    },
};
use ccxt::{value::get_value_k, Params, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

pub(in crate::exchanges::ccxt) use super::super::defaults::statistics_selection as selection_matches;
use crate::models::UnifiedMarketType;

pub(in crate::exchanges::ccxt) fn catalog_products(
    _: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[UnifiedMarketType::Perp, UnifiedMarketType::Spot]
}
pub(in crate::exchanges::ccxt) fn normalize(
    _: CatalogScope,
    product: Option<UnifiedMarketType>,
    selectors: &Map<String, JsonValue>,
) -> JsonValue {
    if product == Some(UnifiedMarketType::Spot)
        || selectors.get("category").and_then(JsonValue::as_str) == Some("spot")
    {
        json!({"type": "spot"})
    } else {
        json!({})
    }
}
pub(in crate::exchanges::ccxt) fn acquisition_params(_: &JsonValue) -> JsonValue {
    json!({})
}

const TICKERS: &str = "nado:ccxt:fetchTickers";
const CONTRACTS: &str = "nado:ccxt:fetchFundingRates";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    _: CatalogScope,
    _: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    if !params.include_bulk {
        return;
    }
    acquire_tickers(Venue::Nado, TICKERS, provider, acquired, failures).await;
    let result = run_call(
        provider,
        Venue::Nado,
        "fetch_funding_rates",
        vec![Value::Null, Params::none().into_value_object()],
    )
    .await;
    let rows = finish(Venue::Nado, CONTRACTS, result, acquired, failures);
    acquired.rows.insert(CONTRACTS, rows);
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let ticker = data.row(TICKERS, entry);
    let contract = data.row(CONTRACTS, entry);
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    match name {
        MarketStatsFieldName::LastPrice => price_field(
            TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "last_price",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "base_volume",
            "quote_volume",
        ),
        MarketStatsFieldName::MarkPrice | MarketStatsFieldName::IndexPrice => price_field(
            CONTRACTS,
            contract.row,
            contract.error,
            contract.receipt,
            if name == MarketStatsFieldName::MarkPrice {
                "mark_price"
            } else {
                "index_price"
            },
            base,
            quote,
        ),
        MarketStatsFieldName::Funding => {
            let Some(row) = contract.row else {
                return missing_pair(CONTRACTS, contract.error);
            };
            let info = get_value_k(row, "info");
            if get_value_k(&info, "funding_rate").is_null() {
                return missing_pair(CONTRACTS, contract.error);
            }
            let Some(rate) = raw_lexical(&info, "funding_rate") else {
                return invalid_pair(CONTRACTS, contract.receipt);
            };
            // Official API: decimal-fraction 24h rate, settled hourly. Never
            // use stock's hardcoded 24h interval as the payment interval.
            // Raw next_funding_rate_timestamp is seconds; unified is ms.
            let next_payment = positive_integer(&get_value_k(row, "fundingTimestamp"));
            (
                observed(
                    CONTRACTS,
                    MarketStatsValue::Funding(FundingValue::new(
                        rate,
                        FundingRateUnit::DecimalFraction,
                        FundingKind::CurrentUnclassified,
                        Some(86_400_000),
                        Some(3_600_000),
                        None,
                        next_payment,
                    )),
                    None,
                    contract.receipt,
                ),
                contract.receipt.map(|r| r.at),
            )
        }
        MarketStatsFieldName::OpenInterest => {
            let Some(row) = contract.row else {
                return missing_pair(CONTRACTS, contract.error);
            };
            let info = get_value_k(row, "info");
            match (
                nonneg_number(&get_value_k(&info, "open_interest")),
                nonneg_number(&get_value_k(&info, "open_interest_usd")),
            ) {
                (Err(()), _) | (_, Err(())) => invalid_pair(CONTRACTS, contract.receipt),
                (Ok(None), Ok(None)) => missing_pair(CONTRACTS, contract.error),
                (Ok(amount), Ok(value)) => (
                    observed(
                        CONTRACTS,
                        MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: amount,
                            open_interest_value: value,
                        }),
                        None,
                        contract.receipt,
                    ),
                    contract.receipt.map(|r| r.at),
                ),
            }
        }
        _ => not_requested_pair(),
    }
}

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "nado:ccxt:loadMarkets",
    products: &[
        crate::models::UnifiedMarketType::Spot,
        crate::models::UnifiedMarketType::Perp,
    ],
    unsupported: &[(
        MarketStatsFieldName::LastSettledFunding,
        "last-settlement-not-provided",
    )],
    funding_kinds: &[FundingKind::CurrentUnclassified],
    hourly: false,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "funding-rate-24h-payment-1h",
        "bulk-statistics-no-exchange-timestamp",
        "open-interest-value-in-usd",
        "native-default-edge-aggregate-metrics",
    ],
};
