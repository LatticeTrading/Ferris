//! Qualified stock statistics coverage and request profiles. No upstream I/O.

use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::{
    exchanges::traits::ExchangeError,
    models::{
        CapabilityState, FeatureCapability, FundingKind, MarketStatsAllMarketsCapability,
        MarketStatsCapabilities, MarketStatsFieldName, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSupportedCapabilities,
        MarketStatsWsCapability, UnifiedMarketType,
    },
};

use super::{
    catalog::catalog_scope, stream::statistics::StatisticsFeed, venues, CatalogScope, Venue,
};

pub(super) const FIELDS: [MarketStatsFieldName; 7] = [
    MarketStatsFieldName::Funding,
    MarketStatsFieldName::LastSettledFunding,
    MarketStatsFieldName::MarkPrice,
    MarketStatsFieldName::IndexPrice,
    MarketStatsFieldName::LastPrice,
    MarketStatsFieldName::Volume24h,
    MarketStatsFieldName::OpenInterest,
];
pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const STALE_AFTER: Duration = Duration::from_secs(90);

/// Qualified statistics policy. Per-exchange values live next to acquisition.
pub(in crate::exchanges::ccxt) struct Profile {
    pub catalog_source: &'static str,
    pub products: &'static [UnifiedMarketType],
    pub unsupported: &'static [(MarketStatsFieldName, &'static str)],
    pub funding_kinds: &'static [FundingKind],
    pub hourly: bool,
    pub percent: bool,
    /// Fields deliberately excluded from perpetual bulk polls, not all live output.
    pub live_perp_fields: &'static [MarketStatsFieldName],
    pub live: Option<&'static LiveStatisticsPolicy>,
    /// Selected OI is the only demand acquisition supported by the request API.
    /// The value is the venue's validation message when selection is absent.
    pub selected_open_interest: Option<&'static str>,
    pub expire_funding_at_payment: bool,
    pub limitations: &'static [&'static str],
}

/// Maintenance and failure policy is independent of polling exclusions: a live
/// feed can also replace bulk observations, which fail only while live-sourced.
pub(crate) struct LiveStatisticsPolicy {
    pub(in crate::exchanges::ccxt) feed: StatisticsFeed,
    pub source: &'static str,
    pub scope: CatalogScope,
    /// Only active rows of these products are invalidated on live failure.
    pub failure_products: &'static [UnifiedMarketType],
    /// Fail these even before a live receipt, plus any field currently sourced
    /// from `source`. Neither list is inferred from the polling exclusions.
    pub failure_fields: &'static [MarketStatsFieldName],
}

pub(crate) fn live_statistics(venue: Venue) -> Option<&'static LiveStatisticsPolicy> {
    venues::statistics_profile(venue).live
}

pub(crate) fn selected_open_interest(venue: Venue) -> Option<&'static str> {
    venues::statistics_profile(venue).selected_open_interest
}

pub(crate) fn expire_funding_at_payment(venue: Venue) -> bool {
    venues::statistics_profile(venue).expire_funding_at_payment
}

pub(crate) fn selection_matches(
    venue: Venue,
    product: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    native_id: &str,
    params: &Value,
) -> bool {
    venues::dispatch!(venue, exchange => exchange::statistics::selection_matches(
        product, category, dex, native_id, params
    ))
}

pub(crate) fn catalog_source(venue: Venue) -> &'static str {
    venues::statistics_profile(venue).catalog_source
}

pub(crate) fn catalog_products(venue: Venue, scope: CatalogScope) -> &'static [UnifiedMarketType] {
    venues::dispatch!(venue, exchange => exchange::statistics::catalog_products(scope))
}

pub(crate) fn field_support(
    venue: Venue,
    product: UnifiedMarketType,
    field: MarketStatsFieldName,
) -> FeatureCapability {
    use CapabilityState::*;
    use MarketStatsFieldName::*;
    use UnifiedMarketType::*;
    let (state, reason) = match (product, field) {
        (Spot, Funding | LastSettledFunding | OpenInterest)
        | (Future | Option, Funding | LastSettledFunding) => {
            (NotApplicable, Some("not-applicable"))
        }
        (Spot, MarkPrice | IndexPrice) => (NotApplicable, Some("not-applicable")),
        _ if venues::statistics_profile(venue)
            .unsupported
            .iter()
            .any(|(name, _)| *name == field) =>
        {
            (
                Unsupported,
                venues::statistics_profile(venue)
                    .unsupported
                    .iter()
                    .find(|(name, _)| *name == field)
                    .map(|(_, reason)| *reason),
            )
        }
        _ => (Supported, None),
    };
    FeatureCapability {
        state,
        reason: reason.map(str::to_string),
    }
}

pub(crate) fn normalize_params(venue: Venue, input: &Value) -> Result<Value, ExchangeError> {
    let mut selectors = Map::new();
    let mut product = None;
    if !input.is_null() {
        let input = input.as_object().ok_or_else(|| {
            ExchangeError::BadSymbol("market statistics params must be an object or null".into())
        })?;
        for (key, value) in input {
            if venue == Venue::Hyperliquid && key == "dex" {
                let dex = value
                    .as_str()
                    .ok_or_else(|| ExchangeError::BadSymbol("`dex` must be a string".into()))?;
                selectors.insert(key.clone(), Value::String(dex.trim().to_string()));
                continue;
            }
            if !matches!(key.as_str(), "type" | "category" | "subType") {
                return Err(ExchangeError::BadSymbol(format!(
                    "unsupported market statistics parameter `{key}`"
                )));
            }
            let value = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("`{key}` must be a nonempty string"))
                })?;
            let value = value.to_ascii_lowercase();
            let value = if key == "type" {
                let (name, kind) = match value.as_str() {
                    "spot" => ("spot", UnifiedMarketType::Spot),
                    "swap" | "perp" | "perpetual" => ("swap", UnifiedMarketType::Perp),
                    "future" | "futures" | "delivery" => ("future", UnifiedMarketType::Future),
                    "option" | "options" => ("option", UnifiedMarketType::Option),
                    _ => return Err(ExchangeError::BadSymbol("unsupported market type".into())),
                };
                product = Some(kind);
                name.to_string()
            } else {
                value
            };
            selectors.insert(key.clone(), Value::String(value));
        }
    }
    let scope = catalog_scope(venue, &Value::Object(selectors.clone()))?;
    if product.is_some_and(|product| !catalog_products(venue, scope).contains(&product)) {
        return Err(ExchangeError::BadSymbol(
            "unqualified statistics product".into(),
        ));
    }
    let mut result = venues::dispatch!(venue, exchange => exchange::statistics::normalize(scope, product, &selectors));
    if product == Some(UnifiedMarketType::Future) {
        result["type"] = json!("future");
    }
    Ok(result)
}

pub(crate) fn scope(venue: Venue, params: &Value) -> Result<CatalogScope, ExchangeError> {
    let scope = catalog_scope(venue, params)?;
    Ok(venue.data_scope(scope))
}

pub(crate) fn acquisition_params(venue: Venue, params: &Value) -> Value {
    venues::dispatch!(venue, exchange => exchange::statistics::acquisition_params(params))
}

pub(crate) fn all_market_product(params: &Value) -> UnifiedMarketType {
    match params
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| params.get("category").and_then(Value::as_str))
    {
        Some("spot") => UnifiedMarketType::Spot,
        Some("future") => UnifiedMarketType::Future,
        Some("option") => UnifiedMarketType::Option,
        _ => UnifiedMarketType::Perp,
    }
}

#[cfg(test)]
mod tests;

pub(super) fn capabilities(venue: Venue) -> MarketStatsCapabilities {
    let profile = venues::statistics_profile(venue);
    let products = profile.products;
    let hourly = profile.hourly;
    let mut limitations = vec!["receipt-time-freshness".to_string()];
    if !profile.funding_kinds.is_empty() {
        limitations.push(
            if profile.percent {
                "rate-unit-percent"
            } else {
                "rate-unit-decimal-fraction"
            }
            .into(),
        );
    }
    limitations.extend(profile.limitations.iter().map(|value| (*value).to_string()));
    MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
        scope: MarketStatsScope {
            exchange: venue.public_id().to_string(),
            params: normalize_params(venue, &Value::Null).expect("default profile"),
        },
        all_markets: MarketStatsAllMarketsCapability {
            types: products.to_vec(),
            active_only: true,
        },
        selected_markets: MarketStatsSelectedMarketsCapability {
            types: products.to_vec(),
            limit: 100,
        },
        fields: products
            .iter()
            .map(|&product| {
                (
                    product,
                    FIELDS
                        .into_iter()
                        .map(|field| (field, field_support(venue, product, field)))
                        .collect(),
                )
            })
            .collect(),
        upstream_mode: if profile.live.is_some() {
            "sharedPollingAndWebSocket"
        } else {
            "sharedPolling"
        }
        .into(),
        poll_interval_ms: POLL_INTERVAL.as_millis() as u64,
        stale_after_ms: STALE_AFTER.as_millis() as u64,
        ws: MarketStatsWsCapability {
            snapshot: true,
            delta: true,
            max_subscriptions_per_connection: 16,
        },
        funding_kinds: profile.funding_kinds.to_vec(),
        rate_interval_ms: hourly.then_some(3_600_000),
        payment_interval_ms: hourly.then_some(3_600_000),
        limitations,
    })
}
