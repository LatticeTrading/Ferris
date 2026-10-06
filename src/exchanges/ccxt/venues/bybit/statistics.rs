//! Bybit statistics: stock acquisition and native field semantics.

use std::collections::{BTreeSet, HashMap};

use ccxt::{value::get_value_k, Params, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

use crate::{
    exchanges::{
        ccxt::{
            catalog::{Catalog, CatalogMarket},
            statistics::{
                acquisition::{run_call, Acquired},
                failure,
                fields::{
                    funding_field, missing_pair, not_requested_pair, price_field, volume_field,
                },
                index::index_tickers,
                scalars::positive_integer,
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

const BYBIT_TICKERS: &str = "bybit:ccxt:fetchTickers";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    scope: CatalogScope,
    catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Bybit;
    if params.include_bulk {
        let mut results: Vec<Result<HashMap<String, Value>, ExchangeError>> = Vec::new();
        if scope == CatalogScope::Option {
            let mut bases: BTreeSet<String> = BTreeSet::new();
            for entry in catalog.entries() {
                bases.insert(entry.market.base.clone());
            }
            for base in bases {
                let params = Params::new()
                    .with_str("type", "option")
                    .with_str("baseCoin", &base)
                    .into_value_object();
                results.push(
                    run_call(provider, venue, "fetch_tickers", vec![Value::Null, params])
                        .await
                        .and_then(|value| index_tickers(venue, &value)),
                );
            }
        } else {
            results.push(
                run_call(
                    provider,
                    venue,
                    "fetch_tickers",
                    vec![Value::Null, Params::none().into_value_object()],
                )
                .await
                .and_then(|value| index_tickers(venue, &value)),
            );
        }
        let mut merged: HashMap<String, Value> = HashMap::new();
        let mut first_error: Option<ExchangeError> = None;
        let mut any = false;
        for result in results {
            match result {
                Ok(map) => {
                    any = true;
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
        if any {
            acquired.any_bulk_ok = true;
            acquired.receipts.insert(BYBIT_TICKERS, Receipt::now());
            acquired.rows.insert(BYBIT_TICKERS, Ok(merged));
        } else {
            let error = first_error.unwrap_or_else(|| {
                ExchangeError::UpstreamData("Bybit returned no ticker groups".into())
            });
            failures.push(failure(BYBIT_TICKERS, &error));
            acquired.rows.insert(BYBIT_TICKERS, Err(error));
        }
        acquire_bybit_open_interest(provider, catalog, acquired, failures).await;
        for entry in catalog.entries() {
            if !catalog_products(scope).contains(&entry.market.market_type) {
                continue;
            }
            if let Some(identity) = &entry.market.identity {
                let (_, mismatch) = bybit_interval(entry, acquired.row(BYBIT_TICKERS, entry).row);
                if mismatch {
                    failures.push(MarketStatsSourceFailure {
                        source: BYBIT_TICKERS.to_string(),
                        reason: "funding-interval-mismatch".to_string(),
                        message: format!(
                            "Bybit {} ticker funding interval differs from instrument metadata",
                            identity.exchange_market_id
                        ),
                    });
                }
            }
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
    let ticker = data.row(BYBIT_TICKERS, entry);
    let (interval_ms, _) = bybit_interval(entry, ticker.row);
    match name {
        MarketStatsFieldName::OpenInterest => match entry
            .market
            .identity
            .as_ref()
            .and_then(|id| data.single(BYBIT_TICKERS, &id.market_id))
        {
            Some(_) => data.open_interest(BYBIT_TICKERS, entry),
            None if ticker.row.is_some() => not_requested_pair(),
            None => missing_pair(BYBIT_TICKERS, ticker.error),
        },
        MarketStatsFieldName::Volume24h => volume_field(
            BYBIT_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            if entry.inverse == Some(true) {
                "turnover24h"
            } else {
                "volume24h"
            },
            if entry.inverse == Some(true) {
                "volume24h"
            } else {
                "turnover24h"
            },
        ),
        MarketStatsFieldName::Funding => funding_field(
            BYBIT_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "fundingRate",
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            interval_ms,
            Some("nextFundingTime"),
        ),
        MarketStatsFieldName::MarkPrice => price_field(
            BYBIT_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "markPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::IndexPrice => price_field(
            BYBIT_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "indexPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::LastPrice => price_field(
            BYBIT_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "lastPrice",
            base,
            quote,
        ),
        MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
    }
}

async fn acquire_bybit_open_interest(
    provider: &mut Provider,
    catalog: &Catalog,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let ticker_receipt = acquired.receipts.get(BYBIT_TICKERS).copied();
    for entry in catalog.entries() {
        if entry.market.market_type == UnifiedMarketType::Spot {
            continue;
        }
        let Some(identity) = entry.market.identity.as_ref() else {
            continue;
        };
        let Some(ticker) = acquired.row(BYBIT_TICKERS, entry).row else {
            continue;
        };
        let raw = get_value_k(ticker, "info");
        let market = Value::from_json(&entry.raw);
        let result = run_call(
            provider,
            Venue::Bybit,
            "parse_open_interest",
            vec![raw, market],
        )
        .await;
        if let Err(error) = &result {
            failures.push(failure(BYBIT_TICKERS, error));
        }
        if let Some(receipt) = ticker_receipt {
            acquired.singles.insert(
                (BYBIT_TICKERS, identity.market_id.clone()),
                (result, receipt),
            );
        }
    }
}

pub(in crate::exchanges::ccxt) fn bybit_interval(
    entry: &CatalogMarket,
    ticker: Option<&Value>,
) -> (Option<u64>, bool) {
    let ticker_interval = ticker
        .and_then(Value::as_map)
        .and_then(|ticker| ticker.get("info"))
        .and_then(Value::as_map)
        .and_then(|info| info.get("fundingIntervalHour"));
    let ticker_ms = ticker_interval
        .and_then(positive_integer)
        .and_then(|hours| hours.checked_mul(3_600_000));
    if ticker_interval.is_some() && ticker_ms.is_none() {
        return (None, false);
    }
    let instrument_ms = entry
        .raw
        .get("info")
        .and_then(|info| info.get("fundingInterval"))
        .and_then(json_positive_integer)
        .and_then(|minutes| minutes.checked_mul(60_000));
    match (ticker_ms, instrument_ms) {
        (Some(ticker), Some(instrument)) => (Some(ticker), ticker != instrument),
        (Some(ticker), None) => (Some(ticker), false),
        (None, instrument) => (instrument, false),
    }
}

fn json_positive_integer(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(number) => number.as_u64().filter(|value| *value > 0),
        JsonValue::String(text) => text.parse::<u64>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

use crate::models::UnifiedMarketType::{Future, Option as OptionProduct, Perp, Spot};

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "bybit:ccxt:loadMarkets",
    products: &[Spot, Future, Perp, OptionProduct],
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
        "default-category-linear",
        "funding-intervals-per-market",
        "inverse-open-interest-value-is-USD",
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
    json!({"category": match scope {
        CatalogScope::Spot => "spot", CatalogScope::Inverse => "inverse", CatalogScope::Option => "option", _ => "linear",
    }})
}

pub(in crate::exchanges::ccxt) fn acquisition_params(params: &JsonValue) -> JsonValue {
    let mut params = params.clone();
    params
        .as_object_mut()
        .expect("normalized params")
        .remove("type");
    params
}

pub(in crate::exchanges::ccxt) use super::super::defaults::statistics_noop_param as accepts_noop_param;

pub(in crate::exchanges::ccxt) fn selection_matches(
    _product: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    _native_id: &str,
    params: &JsonValue,
) -> bool {
    category == params.get("category").and_then(JsonValue::as_str) && dex.is_none()
}

#[cfg(test)]
mod tests;
