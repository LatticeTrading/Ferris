//! One stock bulk ticker call supplies all current Apex statistics, including OI.
use crate::{
    exchanges::ccxt::{
        catalog::{Catalog, CatalogMarket},
        statistics::{
            acquisition::{acquire_tickers, Acquired},
            fields::{
                funding_field, invalid_pair, missing_pair, not_requested_pair, observed,
                price_field, volume_field,
            },
            scalars::nonneg_number,
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
use ccxt::value::get_value_k;
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

pub(in crate::exchanges::ccxt) use super::super::defaults::statistics_selection as selection_matches;
const TICKERS: &str = "apex:ccxt:fetchTickers";

#[cfg(test)]
mod tests;

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    _scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    if params.include_bulk {
        acquire_tickers(Venue::Apex, TICKERS, provider, acquired, failures).await;
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let ticker = data.row(TICKERS, entry);
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    match name {
        MarketStatsFieldName::Funding => {
            // Omni docs' Funding Fee section declares hourly exchange of fees
            // and fee = position size * index price * rate (fraction, not %).
            // The ticker's fundingRate is not classified as predicted/settled.
            let (mut field, at) = funding_field(
                TICKERS,
                ticker.row,
                ticker.error,
                ticker.receipt,
                "fundingRate",
                FundingRateUnit::DecimalFraction,
                FundingKind::CurrentUnclassified,
                Some(3_600_000),
                None,
            );
            if let Some(MarketStatsValue::Funding(value)) = &mut field.value {
                value.next_payment_timestamp = ticker.row.and_then(|row| {
                    let info = get_value_k(row, "info");
                    let raw = get_value_k(&info, "nextFundingTime");
                    // Never assign a date to a time-only value such as 10:00:00.
                    chrono::DateTime::parse_from_rfc3339(raw.as_str()?)
                        .ok()
                        .and_then(|time| u64::try_from(time.timestamp_millis()).ok())
                        .filter(|time| *time > 0)
                });
            }
            (field, at)
        }
        MarketStatsFieldName::MarkPrice
        | MarketStatsFieldName::IndexPrice
        | MarketStatsFieldName::LastPrice => {
            let key = match name {
                MarketStatsFieldName::MarkPrice => "markPrice",
                MarketStatsFieldName::IndexPrice => "indexPrice",
                _ => "lastPrice",
            };
            price_field(
                TICKERS,
                ticker.row,
                ticker.error,
                ticker.receipt,
                key,
                base,
                quote,
            )
        }
        MarketStatsFieldName::Volume24h => volume_field(
            TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "volume24h",
            "turnover24h",
        ),
        MarketStatsFieldName::OpenInterest => {
            let Some(row) = ticker.row else {
                return missing_pair(TICKERS, ticker.error);
            };
            match nonneg_number(&get_value_k(&get_value_k(row, "info"), "openInterest")) {
                Err(()) => invalid_pair(TICKERS, ticker.receipt),
                Ok(None) => missing_pair(TICKERS, ticker.error),
                Ok(Some(amount)) => (
                    observed(
                        TICKERS,
                        MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: Some(amount),
                            open_interest_value: None,
                        }),
                        None,
                        ticker.receipt,
                    ),
                    ticker.receipt.map(|receipt| receipt.at),
                ),
            }
        }
        MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
    }
}

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "apex:ccxt:loadMarkets",
    products: &[UnifiedMarketType::Perp],
    unsupported: &[(
        MarketStatsFieldName::LastSettledFunding,
        "last-settlement-not-provided",
    )],
    funding_kinds: &[FundingKind::CurrentUnclassified],
    hourly: true,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "perpetuals-only",
        "open-interest-amount-is-base",
        "bulk-ticker-no-exchange-timestamp",
        "funding-rate-current-unclassified",
    ],
};
pub(in crate::exchanges::ccxt) fn catalog_products(
    _scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[UnifiedMarketType::Perp]
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
