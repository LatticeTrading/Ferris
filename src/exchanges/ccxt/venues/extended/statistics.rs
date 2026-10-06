//! Extended statistics: stock acquisition and native field semantics.

use ccxt::value::get_value_k;
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{acquire_tickers, Acquired},
            fields::{
                funding_field, invalid_pair, missing_pair, not_requested_pair, observed,
                price_field, volume_field,
            },
            scalars::{exchange_time, nonneg_number},
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

pub(in crate::exchanges::ccxt) use super::super::defaults::{
    statistics_noop_param as accepts_noop_param, statistics_selection as selection_matches,
};

const EXTENDED_TICKERS: &str = "extended:ccxt:fetchTickers";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    _scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Extended;
    if params.include_bulk {
        acquire_tickers(venue, EXTENDED_TICKERS, provider, acquired, failures).await;
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let ticker = data.row(EXTENDED_TICKERS, entry);
    match name {
        MarketStatsFieldName::Funding => funding_field(
            EXTENDED_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "fundingRate",
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            Some(3_600_000),
            None,
        ),
        MarketStatsFieldName::MarkPrice => price_field(
            EXTENDED_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "markPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::IndexPrice => price_field(
            EXTENDED_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "indexPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::LastPrice => price_field(
            EXTENDED_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "lastPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            EXTENDED_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "dailyVolumeBase",
            "dailyVolume",
        ),
        MarketStatsFieldName::OpenInterest => {
            let Some(row) = ticker.row else {
                return missing_pair(EXTENDED_TICKERS, ticker.error);
            };
            let info = get_value_k(row, "info");
            match (
                nonneg_number(&get_value_k(&info, "openInterestBase")),
                nonneg_number(&get_value_k(&info, "openInterest")),
            ) {
                (Err(()), _) | (_, Err(())) => invalid_pair(EXTENDED_TICKERS, ticker.receipt),
                (Ok(None), Ok(None)) => missing_pair(EXTENDED_TICKERS, ticker.error),
                (amount, value) => {
                    let amount = amount.ok().flatten();
                    let value = value.ok().flatten();
                    (
                        observed(
                            EXTENDED_TICKERS,
                            MarketStatsValue::OpenInterest(OpenInterestValue {
                                open_interest_amount: amount,
                                open_interest_value: value,
                            }),
                            exchange_time(&info),
                            ticker.receipt,
                        ),
                        ticker.receipt.map(|receipt| receipt.at),
                    )
                }
            }
        }
        MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
    }
}

use crate::models::UnifiedMarketType::Perp;

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "extended:ccxt:loadMarkets",
    products: &[Perp],
    unsupported: &[(
        MarketStatsFieldName::LastSettledFunding,
        "last-settlement-not-provided",
    )],
    funding_kinds: &[FundingKind::Estimate],
    hourly: true,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &["stock-public-API-availability-limited"],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    _scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[Perp]
}

pub(in crate::exchanges::ccxt) fn normalize(
    _scope: CatalogScope,
    _product: Option<UnifiedMarketType>,
    _selectors: &Map<String, JsonValue>,
) -> JsonValue {
    json!({})
}

pub(in crate::exchanges::ccxt) fn acquisition_params(params: &JsonValue) -> JsonValue {
    params.clone()
}
