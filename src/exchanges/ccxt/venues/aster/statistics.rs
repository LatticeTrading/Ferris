//! Aster statistics: stock acquisition and native field semantics.

use std::collections::HashMap;

use ccxt::{Params, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

use crate::{
    exchanges::{
        ccxt::{
            catalog::{Catalog, CatalogMarket},
            statistics::{
                acquisition::{finish, finish_intervals, run_call, Acquired},
                failure,
                fields::{funding_field, not_requested_pair, price_field, volume_field},
                index::index_tickers,
                Receipt,
            },
            statistics_profile::Profile,
            venue::{CatalogScope, Provider, Venue},
        },
        traits::ExchangeError,
    },
    models::{
        FetchMarketStatsParams, FundingKind, FundingRateUnit, MarketStatsField,
        MarketStatsFieldName, MarketStatsSourceFailure, UnifiedMarketType,
    },
};

pub(in crate::exchanges::ccxt) use super::super::defaults::statistics_selection as selection_matches;

const ASTER_TICKERS: &str = "aster:ccxt:fetchTickers";
const ASTER_FUNDING: &str = "aster:ccxt:fetchFundingRates";
const ASTER_INTERVALS: &str = "aster:ccxt:fetchFundingIntervals";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Aster;
    if params.include_bulk {
        let swap = run_call(
            provider,
            venue,
            "fetch_tickers",
            vec![Value::Null, Params::none().into_value_object()],
        )
        .await
        .and_then(|value| index_tickers(venue, &value));
        let spot = if catalog_products(scope).contains(&UnifiedMarketType::Spot) {
            let params = Params::new().with_str("type", "spot").into_value_object();
            Some(
                run_call(provider, venue, "fetch_tickers", vec![Value::Null, params])
                    .await
                    .and_then(|value| index_tickers(venue, &value)),
            )
        } else {
            None
        };
        let mut merged: HashMap<String, Value> = HashMap::new();
        let mut first_error: Option<ExchangeError> = None;
        for result in std::iter::once(swap).chain(spot.into_iter()) {
            match result {
                Ok(map) => {
                    acquired.any_bulk_ok = true;
                    for (key, value) in map {
                        merged.entry(key).or_insert(value);
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match (merged.is_empty(), first_error) {
            (false, error) => {
                if let Some(error) = error {
                    failures.push(failure(ASTER_TICKERS, &error));
                }
                acquired.receipts.insert(ASTER_TICKERS, Receipt::now());
                acquired.rows.insert(ASTER_TICKERS, Ok(merged));
            }
            (true, Some(error)) => {
                failures.push(failure(ASTER_TICKERS, &error));
                acquired.rows.insert(ASTER_TICKERS, Err(error));
            }
            (true, None) => {}
        }
        if catalog_products(scope).contains(&UnifiedMarketType::Perp) {
            let funding = run_call(
                provider,
                venue,
                "fetch_funding_rates",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            let rows = finish(venue, ASTER_FUNDING, funding, acquired, failures);
            acquired.rows.insert(ASTER_FUNDING, rows);
            let intervals = run_call(
                provider,
                venue,
                "fetch_funding_intervals",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            finish_intervals(
                venue,
                ASTER_INTERVALS,
                "fundingIntervalHours",
                intervals,
                acquired,
                failures,
            );
        }
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let ticker = data.row(ASTER_TICKERS, entry);
    let funding = data.row(ASTER_FUNDING, entry);
    let interval_ms = data.interval(entry);
    match name {
        MarketStatsFieldName::LastPrice => price_field(
            ASTER_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "lastPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            ASTER_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "volume",
            "quoteVolume",
        ),
        MarketStatsFieldName::Funding => funding_field(
            ASTER_FUNDING,
            funding.row,
            funding.error,
            funding.receipt,
            "lastFundingRate",
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            interval_ms,
            Some("nextFundingTime"),
        ),
        MarketStatsFieldName::MarkPrice => price_field(
            ASTER_FUNDING,
            funding.row,
            funding.error,
            funding.receipt,
            "markPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::IndexPrice => price_field(
            ASTER_FUNDING,
            funding.row,
            funding.error,
            funding.receipt,
            "indexPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::OpenInterest | MarketStatsFieldName::LastSettledFunding => {
            not_requested_pair()
        }
    }
}

use crate::models::UnifiedMarketType::{Perp, Spot};

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "aster:ccxt:loadMarkets",
    products: &[Spot, Perp],
    unsupported: &[
        (
            MarketStatsFieldName::LastSettledFunding,
            "last-settlement-not-provided",
        ),
        (
            MarketStatsFieldName::OpenInterest,
            "stock-method-not-supported",
        ),
    ],
    funding_kinds: &[FundingKind::Estimate],
    hourly: false,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: true,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "funding-intervals-per-market",
        "open-interest-stock-method-not-supported",
    ],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    _scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[Perp, Spot]
}

pub(in crate::exchanges::ccxt) fn normalize(
    _scope: CatalogScope,
    product: Option<UnifiedMarketType>,
    selectors: &Map<String, JsonValue>,
) -> JsonValue {
    if product == Some(UnifiedMarketType::Spot)
        || selectors.get("category").and_then(JsonValue::as_str) == Some("spot")
    {
        json!({"type":"spot"})
    } else {
        json!({})
    }
}

pub(in crate::exchanges::ccxt) fn acquisition_params(_params: &JsonValue) -> JsonValue {
    json!({})
}
