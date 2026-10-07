//! Two bulk stock calls; never one request per market or viewer.
use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{acquire_tickers, finish, run_call, Acquired},
            fields::{invalid_pair, missing_pair, not_requested_pair, observed, price_value},
            scalars::{lexical, nonneg_number, positive_integer, raw_lexical},
        },
        statistics_profile::Profile,
        venue::{CatalogScope, Provider, Venue},
    },
    models::{
        FetchMarketStatsParams, FundingKind, FundingRateUnit, FundingValue, MarketStatsField,
        MarketStatsFieldName, MarketStatsSourceFailure, MarketStatsValue, OpenInterestValue,
        UnifiedMarketType, Volume24hValue,
    },
};
use ccxt::{value::get_value_k, Params, Value};
use serde_json::{Map, Value as JsonValue};
use tokio::time::Instant;

pub(in crate::exchanges::ccxt) use super::super::aster::statistics::{
    acquisition_params, normalize,
};
pub(in crate::exchanges::ccxt) use super::super::defaults::{
    statistics_noop_param as accepts_noop_param, statistics_selection as selection_matches,
};
const TICKERS: &str = "bitfinex:ccxt:fetchTickers";
const STATUS: &str = "bitfinex:ccxt:fetchOpenInterests";
const MARK: &str = "bitfinex:ccxt:fetchOpenInterests+parseFundingRate";

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
    acquire_tickers(Venue::Bitfinex, TICKERS, provider, acquired, failures).await;
    let result = run_call(
        provider,
        Venue::Bitfinex,
        "fetch_open_interests",
        vec![Value::Null, Params::none().into_value_object()],
    )
    .await;
    let rows = finish(Venue::Bitfinex, STATUS, result, acquired, failures);
    // Reuse the status response and stock's parser for funding and markPrice.
    // No duplicate /status request, no custom replacement market-data parser.
    let marks = match &rows {
        Ok(rows) => {
            let mut marks = Map::new();
            for (symbol, row) in rows {
                let parsed = run_call(
                    provider,
                    Venue::Bitfinex,
                    "parse_funding_rate",
                    vec![get_value_k(row, "info"), Value::Null],
                )
                .await;
                match parsed {
                    Ok(parsed) => {
                        marks.insert(symbol.clone(), parsed.to_json());
                    }
                    Err(error) => {
                        failures.push(crate::exchanges::ccxt::statistics::failure(MARK, &error))
                    }
                }
            }
            Ok(Value::from_json(&JsonValue::Object(marks)))
        }
        Err(error) => Err(error.clone()),
    };
    let marks = finish(Venue::Bitfinex, MARK, marks, acquired, failures);
    // Parser work is not a newer upstream observation.
    if let Some(receipt) = acquired.receipts.get(STATUS).copied() {
        acquired.receipts.insert(MARK, receipt);
    }
    acquired.rows.insert(STATUS, rows);
    acquired.rows.insert(MARK, marks);
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let source = match name {
        MarketStatsFieldName::MarkPrice | MarketStatsFieldName::Funding => MARK,
        MarketStatsFieldName::OpenInterest => STATUS,
        _ => TICKERS,
    };
    let row = data.row(source, entry);
    let Some(value) = row.row else {
        return missing_pair(source, row.error);
    };
    if name == MarketStatsFieldName::Funding {
        let raw = get_value_k(value, "info");
        let cell = raw.as_array().and_then(|cells| cells.get(12));
        if cell.is_none_or(Value::is_null) {
            return missing_pair(source, row.error);
        }
        let Some(rate) = cell.and_then(lexical) else {
            return invalid_pair(source, row.receipt);
        };
        // CURRENT_FUNDING applies to the current 8h period; NEXT_FUNDING_ACCRUED
        // is a different, partial next-period observation, not this rate.
        let funding = FundingValue::new(
            rate,
            FundingRateUnit::DecimalFraction,
            FundingKind::CurrentUnclassified,
            Some(28_800_000),
            Some(28_800_000),
            None,
            positive_integer(&get_value_k(value, "nextFundingTimestamp")),
        );
        return (
            observed(
                source,
                MarketStatsValue::Funding(funding),
                positive_integer(&get_value_k(value, "timestamp")),
                row.receipt,
            ),
            row.receipt.map(|r| r.at),
        );
    }
    // Stock safe-number can conceal malformed source cells as null. The OI
    // parser also treats a 23-cell row as history; this endpoint is status only.
    let index = match name {
        MarketStatsFieldName::LastPrice => 7,
        MarketStatsFieldName::MarkPrice => 15,
        MarketStatsFieldName::Volume24h => 8,
        MarketStatsFieldName::OpenInterest => 18,
        _ => return not_requested_pair(),
    };
    let info = get_value_k(value, "info");
    if info.as_array().is_some_and(|cells| {
        (name == MarketStatsFieldName::OpenInterest && cells.len() < 24)
            || cells.get(index).is_some_and(|v| nonneg_number(v).is_err())
    }) {
        return invalid_pair(source, row.receipt);
    }
    match name {
        MarketStatsFieldName::LastPrice | MarketStatsFieldName::MarkPrice => {
            let key = if name == MarketStatsFieldName::LastPrice {
                "last"
            } else {
                "markPrice"
            };
            if get_value_k(value, key).is_null() {
                return missing_pair(source, row.error);
            }
            let Some(price) = price_value(
                raw_lexical(value, key),
                &entry.market.base,
                &entry.market.quote,
            ) else {
                return invalid_pair(source, row.receipt);
            };
            (
                observed(
                    source,
                    price,
                    positive_integer(&get_value_k(value, "timestamp")),
                    row.receipt,
                ),
                row.receipt.map(|r| r.at),
            )
        }
        MarketStatsFieldName::Volume24h | MarketStatsFieldName::OpenInterest => {
            let key = if name == MarketStatsFieldName::Volume24h {
                "baseVolume"
            } else {
                "openInterestAmount"
            };
            match nonneg_number(&get_value_k(value, key)) {
                Err(()) => invalid_pair(source, row.receipt),
                Ok(None) => missing_pair(source, row.error),
                Ok(Some(amount)) => {
                    let result = if name == MarketStatsFieldName::Volume24h {
                        MarketStatsValue::Volume24h(Volume24hValue {
                            base_volume: Some(amount),
                            quote_volume: None,
                        })
                    } else {
                        MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: Some(amount),
                            open_interest_value: None,
                        })
                    };
                    (
                        observed(
                            source,
                            result,
                            positive_integer(&get_value_k(value, "timestamp")),
                            row.receipt,
                        ),
                        row.receipt.map(|r| r.at),
                    )
                }
            }
        }
        _ => not_requested_pair(),
    }
}
pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "bitfinex:ccxt:loadMarkets",
    products: &[UnifiedMarketType::Spot, UnifiedMarketType::Perp],
    unsupported: &[
        (
            MarketStatsFieldName::LastSettledFunding,
            "last-settlement-time-not-provided",
        ),
        (
            MarketStatsFieldName::IndexPrice,
            "stock-index-price-is-derivative-midpoint",
        ),
    ],
    funding_kinds: &[FundingKind::CurrentUnclassified],
    hourly: false,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "funding-current-period-not-next-accrual",
        "funding-eight-hour-period",
        "open-interest-amount-is-contracts",
        "volume-base-only-stock-contract-size-one",
        "ticker-no-exchange-timestamp",
        "active-means-present-in-current-config",
    ],
};
pub(in crate::exchanges::ccxt) fn catalog_products(
    _: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[UnifiedMarketType::Spot, UnifiedMarketType::Perp]
}
