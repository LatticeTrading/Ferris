//! Binance statistics: stock acquisition and native field semantics.

use std::collections::BTreeSet;

use ccxt::{Params, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{finish, finish_intervals, run_call, Acquired},
            failure, failure_reason,
            fields::{
                funding_field, missing_pair, not_requested_pair, price_field, price_info_field,
                volume_field,
            },
            scalars::lexical_f64,
            Receipt,
        },
        statistics_profile::{field_support, Profile},
        venue::{CatalogScope, Provider, Venue},
    },
    models::{
        CapabilityState, FetchMarketStatsParams, FundingKind, FundingRateUnit, MarketStatsField,
        MarketStatsFieldName, MarketStatsSourceFailure, UnifiedMarketType,
    },
};

const BINANCE_TICKERS: &str = "binance:ccxt:fetchTickers";
const BINANCE_FUNDING: &str = "binance:ccxt:fetchFundingRates";
const BINANCE_INTERVALS: &str = "binance:ccxt:fetchFundingIntervals";
const BINANCE_MARK: &str = "binance:ccxt:fetchMarkPrices";
const BINANCE_OPTION_INDEX: &str = "binance:ccxt:eapiPublicGetIndex";
const BINANCE_OI: &str = "binance:ccxt:fetchOpenInterest";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    scope: CatalogScope,
    catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Binance;
    if params.include_bulk {
        let tickers = run_call(
            provider,
            venue,
            "fetch_tickers",
            vec![Value::Null, Params::none().into_value_object()],
        )
        .await;
        let rows = finish(venue, BINANCE_TICKERS, tickers, acquired, failures);
        acquired.rows.insert(BINANCE_TICKERS, rows);
        if matches!(scope, CatalogScope::Linear | CatalogScope::Inverse) {
            let funding = run_call(
                provider,
                venue,
                "fetch_funding_rates",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            let rows = finish(venue, BINANCE_FUNDING, funding, acquired, failures);
            acquired.rows.insert(BINANCE_FUNDING, rows);
            let intervals = run_call(
                provider,
                venue,
                "fetch_funding_intervals",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            finish_intervals(
                venue,
                BINANCE_INTERVALS,
                "fundingIntervalHours",
                intervals,
                acquired,
                failures,
            );
        } else if scope == CatalogScope::Option {
            let marks = run_call(
                provider,
                venue,
                "fetch_mark_prices",
                vec![
                    Value::Null,
                    Params::new().with_str("type", "option").into_value_object(),
                ],
            )
            .await;
            let rows = finish(venue, BINANCE_MARK, marks, acquired, failures);
            acquired.rows.insert(BINANCE_MARK, rows);
            acquire_binance_option_indices(provider, catalog, acquired, failures).await;
        }
    }
    acquire_binance_open_interest(provider, catalog, params, acquired, failures).await;
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let ticker = data.row(BINANCE_TICKERS, entry);
    let funding = data.row(BINANCE_FUNDING, entry);
    let interval_ms = data.interval(entry);
    let mark = data.row(BINANCE_MARK, entry);
    match name {
        MarketStatsFieldName::LastPrice => price_field(
            BINANCE_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "lastPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            BINANCE_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            // Inverse reports base coin as `baseVolume`; never CCXT's derived
            // quote (baseVolume*weightedAvgPrice).
            if entry.inverse == Some(true) {
                "baseVolume"
            } else if entry.market.market_type == UnifiedMarketType::Option
                && entry
                    .raw
                    .get("info")
                    .and_then(|info| info.get("unit"))
                    .and_then(|unit| {
                        unit.as_f64()
                            .or_else(|| unit.as_str().and_then(lexical_f64))
                    })
                    != Some(1.0)
            {
                // Native option volume is contracts, not base coins.
                "\u{0}base-not-provided"
            } else {
                "volume"
            },
            // Option `ticker24hr` reports quote turnover as `amount`.
            if entry.market.market_type == UnifiedMarketType::Option {
                "amount"
            } else {
                "quoteVolume"
            },
        ),
        MarketStatsFieldName::OpenInterest => data.open_interest(BINANCE_OI, entry),
        MarketStatsFieldName::IndexPrice
            if entry.market.market_type == UnifiedMarketType::Option =>
        {
            let Some((result, receipt)) =
                option_underlying(entry).and_then(|key| data.single(BINANCE_OPTION_INDEX, key))
            else {
                return missing_pair(BINANCE_OPTION_INDEX, None);
            };
            match result {
                Ok(info) => price_info_field(
                    BINANCE_OPTION_INDEX,
                    info,
                    None,
                    Some(*receipt),
                    "indexPrice",
                    base,
                    quote,
                ),
                Err(error) => missing_pair(BINANCE_OPTION_INDEX, Some(failure_reason(error))),
            }
        }
        MarketStatsFieldName::Funding
        | MarketStatsFieldName::MarkPrice
        | MarketStatsFieldName::IndexPrice => {
            let option = entry.market.market_type == UnifiedMarketType::Option;
            let (source, row, error, receipt) = if option {
                (BINANCE_MARK, mark.row, mark.error, mark.receipt)
            } else {
                (BINANCE_FUNDING, funding.row, funding.error, funding.receipt)
            };
            match name {
                MarketStatsFieldName::Funding => funding_field(
                    source,
                    row,
                    error,
                    receipt,
                    "lastFundingRate",
                    FundingRateUnit::DecimalFraction,
                    FundingKind::CurrentUnclassified,
                    interval_ms,
                    Some("nextFundingTime"),
                ),
                MarketStatsFieldName::MarkPrice => {
                    price_field(source, row, error, receipt, "markPrice", base, quote)
                }
                _ => price_field(source, row, error, receipt, "indexPrice", base, quote),
            }
        }
        MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
    }
}

fn option_underlying(entry: &CatalogMarket) -> Option<&str> {
    entry.raw.get("info")?.get("underlying")?.as_str()
}

async fn acquire_binance_option_indices(
    provider: &mut Provider,
    catalog: &Catalog,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let underlyings: BTreeSet<&str> = catalog
        .entries()
        .iter()
        .filter(|entry| {
            entry.market.active && entry.market.market_type == UnifiedMarketType::Option
        })
        .filter_map(option_underlying)
        .collect();
    for underlying in underlyings {
        // One stock index call per underlying, shared by all option contracts.
        // Ticker exercisePrice changes meaning near settlement; never use it.
        let result = run_call(
            provider,
            Venue::Binance,
            "eapi_public_get_index",
            vec![Params::new()
                .with_str("underlying", underlying)
                .into_value_object()],
        )
        .await;
        let receipt = Receipt::now();
        match &result {
            Ok(_) => {
                acquired.any_bulk_ok = true;
                acquired.receipts.insert(BINANCE_OPTION_INDEX, receipt);
            }
            Err(error) => failures.push(failure(BINANCE_OPTION_INDEX, error)),
        }
        acquired.singles.insert(
            (BINANCE_OPTION_INDEX, underlying.to_string()),
            (result, receipt),
        );
    }
}

async fn acquire_binance_open_interest(
    provider: &mut Provider,
    catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    for market_id in &params.open_interest_market_ids {
        let Some(entry) = catalog.entries().iter().find(|entry| {
            entry
                .market
                .identity
                .as_ref()
                .is_some_and(|identity| &identity.market_id == market_id)
        }) else {
            continue;
        };
        if !entry.market.active
            || field_support(
                Venue::Binance,
                entry.market.market_type,
                MarketStatsFieldName::OpenInterest,
            )
            .state
                != CapabilityState::Supported
        {
            continue;
        }
        let result = run_call(
            provider,
            Venue::Binance,
            "fetch_open_interest",
            vec![
                Value::Str(entry.ccxt_symbol.clone().into()),
                Params::none().into_value_object(),
            ],
        )
        .await;
        let receipt = Receipt::now();
        if let Err(error) = &result {
            failures.push(failure(BINANCE_OI, error));
        }
        acquired
            .singles
            .insert((BINANCE_OI, market_id.clone()), (result, receipt));
    }
}

use crate::models::UnifiedMarketType::{Future, Option as OptionProduct, Perp, Spot};

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "binance:ccxt:loadMarkets",
    products: &[Spot, Future, Perp, OptionProduct],
    unsupported: &[(
        MarketStatsFieldName::LastSettledFunding,
        "last-settlement-not-provided",
    )],
    funding_kinds: &[FundingKind::CurrentUnclassified],
    hourly: false,
    percent: false,
    selected_open_interest: Some("Binance openInterest requires selected marketIds"),
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "open-interest-selected-marketIds-only",
        "funding-info-exceptions-only",
        "default-usd-m-perpetuals",
    ],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    match scope {
        CatalogScope::Spot => &[Spot],
        CatalogScope::Option => &[OptionProduct],
        _ => &[Perp, Future],
    }
}

pub(in crate::exchanges::ccxt) fn normalize(
    scope: CatalogScope,
    _product: Option<UnifiedMarketType>,
    _selectors: &Map<String, JsonValue>,
) -> JsonValue {
    match scope {
        CatalogScope::Linear => json!({}),
        CatalogScope::Spot => json!({"category":"spot"}),
        CatalogScope::Inverse => json!({"category":"inverse"}),
        CatalogScope::Option => json!({"category":"option"}),
        _ => unreachable!("Binance scope is normalized"),
    }
}

pub(in crate::exchanges::ccxt) fn acquisition_params(params: &JsonValue) -> JsonValue {
    let mut params = params.clone();
    params
        .as_object_mut()
        .expect("normalized params")
        .remove("type");
    params
}

pub(in crate::exchanges::ccxt) use super::super::defaults::{
    statistics_noop_param as accepts_noop_param, statistics_selection as selection_matches,
};

#[cfg(test)]
mod tests;
