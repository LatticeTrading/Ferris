//! Hyperliquid statistics: stock acquisition and native field semantics.

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

const HYPERLIQUID_TICKERS: &str = "hyperliquid:ccxt:fetchTickers";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    _scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Hyperliquid;
    if params.include_bulk {
        acquire_tickers(venue, HYPERLIQUID_TICKERS, provider, acquired, failures).await;
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let ticker = data.row(HYPERLIQUID_TICKERS, entry);
    match name {
        MarketStatsFieldName::Funding => funding_field(
            HYPERLIQUID_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "funding",
            FundingRateUnit::DecimalFraction,
            FundingKind::CurrentUnclassified,
            Some(3_600_000),
            None,
        ),
        MarketStatsFieldName::MarkPrice => price_field(
            HYPERLIQUID_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "markPx",
            base,
            quote,
        ),
        MarketStatsFieldName::IndexPrice => price_field(
            HYPERLIQUID_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "oraclePx",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            HYPERLIQUID_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            // No base volume in the swap context; never derive it.
            "\u{0}base-not-provided",
            "dayNtlVlm",
        ),
        MarketStatsFieldName::OpenInterest => {
            let Some(row) = ticker.row else {
                return missing_pair(HYPERLIQUID_TICKERS, ticker.error);
            };
            let info = get_value_k(row, "info");
            match nonneg_number(&get_value_k(&info, "openInterest")) {
                Err(()) => invalid_pair(HYPERLIQUID_TICKERS, ticker.receipt),
                Ok(None) => missing_pair(HYPERLIQUID_TICKERS, ticker.error),
                Ok(Some(amount)) => (
                    observed(
                        HYPERLIQUID_TICKERS,
                        MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: Some(amount),
                            open_interest_value: None,
                        }),
                        exchange_time(&info),
                        ticker.receipt,
                    ),
                    ticker.receipt.map(|receipt| receipt.at),
                ),
            }
        }
        MarketStatsFieldName::LastPrice | MarketStatsFieldName::LastSettledFunding => {
            not_requested_pair()
        }
    }
}

use crate::models::UnifiedMarketType::{Perp, Spot};

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "hyperliquid:ccxt:loadMarkets",
    products: &[Spot, Perp],
    unsupported: &[
        (
            MarketStatsFieldName::LastSettledFunding,
            "last-settlement-not-provided",
        ),
        (
            MarketStatsFieldName::LastPrice,
            "stock-ticker-last-is-midpoint",
        ),
    ],
    funding_kinds: &[FundingKind::CurrentUnclassified],
    hourly: true,
    percent: false,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: None,
    live_perp_fields: &[],
    limitations: &[
        "primary-dex-only",
        "volume-quote-only",
        "open-interest-amount-is-base",
        "stock-ticker-last-is-midpoint",
    ],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    match scope {
        CatalogScope::Spot => &[Spot],
        CatalogScope::Linear => &[Perp],
        _ => &[Perp, Spot],
    }
}

pub(in crate::exchanges::ccxt) fn normalize(
    scope: CatalogScope,
    _product: Option<UnifiedMarketType>,
    _selectors: &Map<String, JsonValue>,
) -> JsonValue {
    let mut params = json!({"dex":""});
    if scope == CatalogScope::Spot {
        params["type"] = json!("spot");
    }
    if scope == CatalogScope::Linear {
        params["type"] = json!("swap");
    }
    params
}

pub(in crate::exchanges::ccxt) fn acquisition_params(params: &JsonValue) -> JsonValue {
    params.clone()
}

pub(in crate::exchanges::ccxt) fn accepts_noop_param(key: &str, value: &JsonValue) -> bool {
    key == "dex" && value.as_str() == Some("")
}

pub(in crate::exchanges::ccxt) fn selection_matches(
    product: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    _native_id: &str,
    _params: &JsonValue,
) -> bool {
    category.is_none()
        && if product == UnifiedMarketType::Perp {
            dex == Some("")
        } else {
            dex.is_none()
        }
}

#[cfg(test)]
mod tests;
