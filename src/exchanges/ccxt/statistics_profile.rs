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

use super::{catalog::catalog_scope, CatalogScope, Venue};

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

pub(crate) fn catalog_source(venue: Venue) -> &'static str {
    match venue {
        Venue::Binance => "binance:ccxt:loadMarkets",
        Venue::Bybit => "bybit:ccxt:loadMarkets",
        Venue::Aster => "aster:ccxt:loadMarkets",
        Venue::Hyperliquid => "hyperliquid:ccxt:loadMarkets",
        Venue::Lighter => "lighterxyz:ccxt:loadMarkets",
        Venue::Extended => "extended:ccxt:loadMarkets",
    }
}

pub(crate) fn catalog_products(venue: Venue, scope: CatalogScope) -> &'static [UnifiedMarketType] {
    use UnifiedMarketType::*;
    match (venue, scope) {
        (Venue::Binance | Venue::Bybit, CatalogScope::Spot) => &[Spot],
        (Venue::Binance | Venue::Bybit, CatalogScope::Option) => &[Option],
        (Venue::Binance | Venue::Bybit, _) => &[Perp, Future],
        (Venue::Hyperliquid, CatalogScope::Spot) => &[Spot],
        (Venue::Hyperliquid, CatalogScope::Linear) | (Venue::Extended, _) => &[Perp],
        (Venue::Aster | Venue::Lighter | Venue::Hyperliquid, _) => &[Perp, Spot],
    }
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
        (_, LastSettledFunding) if venue != Venue::Lighter => {
            (Unsupported, Some("last-settlement-not-provided"))
        }
        (Spot, MarkPrice | IndexPrice) => (NotApplicable, Some("not-applicable")),
        (_, OpenInterest) if venue == Venue::Aster => {
            (Unsupported, Some("stock-method-not-supported"))
        }
        (_, OpenInterest) if venue == Venue::Lighter => {
            (Unsupported, Some("open-interest-units-unqualified"))
        }
        (_, LastPrice) if venue == Venue::Hyperliquid => {
            (Unsupported, Some("stock-ticker-last-is-midpoint"))
        }
        _ => (Supported, None),
    };
    FeatureCapability {
        state,
        reason: reason.map(str::to_string),
    }
}

/// Normalize only documented product selectors; the catalog parser rejects
/// conflicts so catalog lookup and statistics cannot disagree about a market.
pub(crate) fn normalize_params(venue: Venue, input: &Value) -> Result<Value, ExchangeError> {
    let mut selectors = Map::new();
    let mut product = None;
    if !input.is_null() {
        let input = input.as_object().ok_or_else(|| {
            ExchangeError::BadSymbol("market statistics params must be an object or null".into())
        })?;
        for (key, value) in input {
            if key == "dex" && venue == Venue::Hyperliquid && value.as_str() == Some("") {
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
    let mut result = match venue {
        Venue::Binance => match scope {
            CatalogScope::Linear => json!({}),
            CatalogScope::Spot => json!({"category":"spot"}),
            CatalogScope::Inverse => json!({"category":"inverse"}),
            CatalogScope::Option => json!({"category":"option"}),
            _ => unreachable!("Binance scope is normalized"),
        },
        Venue::Bybit => json!({"category": match scope {
            CatalogScope::Spot => "spot",
            CatalogScope::Inverse => "inverse",
            CatalogScope::Option => "option",
            _ => "linear",
        }}),
        Venue::Hyperliquid => {
            let mut params = json!({"dex":""});
            if scope == CatalogScope::Spot {
                params["type"] = json!("spot");
            }
            if scope == CatalogScope::Linear {
                params["type"] = json!("swap");
            }
            params
        }
        Venue::Aster | Venue::Lighter => {
            if product == Some(UnifiedMarketType::Spot)
                || selectors.get("category").and_then(Value::as_str) == Some("spot")
            {
                json!({"type":"spot"})
            } else {
                json!({})
            }
        }
        Venue::Extended => json!({}),
    };
    if product == Some(UnifiedMarketType::Future) {
        result["type"] = json!("future");
    }
    Ok(result)
}

pub(crate) fn scope(venue: Venue, params: &Value) -> Result<CatalogScope, ExchangeError> {
    let scope = catalog_scope(venue, params)?;
    Ok(if venue == Venue::Bybit && scope == CatalogScope::Default {
        CatalogScope::Linear
    } else {
        scope
    })
}

/// Product selection is a projection; Aster/Lighter always acquire both catalogs.
/// A futures-only view likewise shares the linear/inverse ticker source.
pub(crate) fn acquisition_params(venue: Venue, params: &Value) -> Value {
    match venue {
        Venue::Aster | Venue::Lighter => json!({}),
        Venue::Binance | Venue::Bybit => {
            let mut params = params.clone();
            params
                .as_object_mut()
                .expect("normalized params")
                .remove("type");
            params
        }
        _ => params.clone(),
    }
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

pub(super) fn capabilities(venue: Venue) -> MarketStatsCapabilities {
    use UnifiedMarketType::*;
    let products: &[UnifiedMarketType] = match venue {
        Venue::Binance | Venue::Bybit => &[Spot, Future, Perp, Option],
        Venue::Extended => &[Perp],
        _ => &[Spot, Perp],
    };
    let hourly = matches!(venue, Venue::Hyperliquid | Venue::Lighter | Venue::Extended);
    let mut limitations = vec!["receipt-time-freshness".to_string()];
    limitations.push(
        if venue == Venue::Lighter {
            "rate-unit-percent"
        } else {
            "rate-unit-decimal-fraction"
        }
        .into(),
    );
    let extra: &[&str] = match venue {
        Venue::Binance => &[
            "open-interest-selected-marketIds-only",
            "funding-info-exceptions-only",
            "default-usd-m-perpetuals",
        ],
        Venue::Bybit => &[
            "default-category-linear",
            "funding-intervals-per-market",
            "inverse-open-interest-value-is-USD",
        ],
        Venue::Aster => &[
            "funding-intervals-per-market",
            "open-interest-stock-method-not-supported",
        ],
        Venue::Hyperliquid => &[
            "primary-dex-only",
            "volume-quote-only",
            "open-interest-amount-is-base",
            "stock-ticker-last-is-midpoint",
        ],
        Venue::Lighter => &[
            "funding-prices-via-stock-watchTickers",
            "open-interest-units-unqualified",
        ],
        Venue::Extended => &["stock-public-API-availability-limited"],
    };
    limitations.extend(extra.iter().map(|value| (*value).to_string()));
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
        upstream_mode: if venue == Venue::Lighter {
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
        funding_kinds: match venue {
            Venue::Binance | Venue::Hyperliquid => vec![FundingKind::CurrentUnclassified],
            Venue::Lighter => vec![FundingKind::Estimate, FundingKind::Settled],
            _ => vec![FundingKind::Estimate],
        },
        rate_interval_ms: hourly.then_some(3_600_000),
        payment_interval_ms: hourly.then_some(3_600_000),
        limitations,
    })
}
