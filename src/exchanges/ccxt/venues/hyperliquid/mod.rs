//! Hyperliquid provider configuration and catalog identity policy.

use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            convert::{json_string, IdentityParts},
            venue::{CatalogScope, Venue},
        },
        traits::ExchangeError,
    },
    models::{UnifiedMarket, UnifiedMarketType},
};

use super::scope_error;

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    Ok(json!({
        "public": config.hyperliquid_base_url,
        "private": config.hyperliquid_base_url
    }))
}

pub(in crate::exchanges::ccxt) fn configure(
    config: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // HIP3 market parsing needs the stock currency cache for collateral names.
    value["has"] = json!({"fetchCurrencies": true});
    let base = config.hyperliquid_base_url.trim_end_matches('/');
    let ws = if let Some(host) = base.strip_prefix("https://") {
        format!("wss://{host}/ws")
    } else if let Some(host) = base.strip_prefix("http://") {
        format!("ws://{host}/ws")
    } else {
        return Err(ExchangeError::Internal("invalid Hyperliquid URL".into()));
    };
    value["urls"]["api"]["ws"] = json!({"public": ws, "private": ws});
    Ok(())
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Spot | CatalogScope::Linear => Ok(scope),
        _ => Err(scope_error(Venue::Hyperliquid, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn market_types(
    scope: CatalogScope,
) -> Option<&'static [&'static str]> {
    match scope {
        CatalogScope::Default => Some(&["swap", "spot", "hip3"]),
        CatalogScope::Spot => Some(&["spot"]),
        CatalogScope::Linear => Some(&["swap", "hip3"]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{market_types, CatalogScope};

    #[test]
    fn default_and_linear_load_hip3_markets() {
        assert_eq!(
            market_types(CatalogScope::Default),
            Some(&["swap", "spot", "hip3"][..])
        );
        assert_eq!(
            market_types(CatalogScope::Linear),
            Some(&["swap", "hip3"][..])
        );
        assert_eq!(market_types(CatalogScope::Spot), Some(&["spot"][..]));
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    _market: &Market,
    product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    match product {
        UnifiedMarketType::Perp => {
            parts.native_id = json_string(info, "name").ok_or_else(|| {
                ExchangeError::UpstreamData("Hyperliquid perpetual has no native name".into())
            })?;
            let dex = json_string(info, "dex").unwrap_or_default();
            if !dex.is_empty() && json_string(info, "collateralTokenName").is_none() {
                return Err(ExchangeError::UpstreamData(
                    "HIP3 collateral metadata is not qualified".into(),
                ));
            }
            parts.dex = Some(dex);
        }
        UnifiedMarketType::Spot => {
            let index = hyperliquid_spot_index(info).ok_or_else(|| {
                ExchangeError::UpstreamData("Hyperliquid spot market has no native index".into())
            })?;
            parts.native_id = format!("@{index}");
        }
        _ => {}
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn raw_symbol(market: &Market, info: &JsonValue) -> String {
    json_string(info, "name").unwrap_or_else(|| market.id.clone())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    aliases.push(converted.base.clone());
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;

fn hyperliquid_spot_index(raw_info: &JsonValue) -> Option<String> {
    match raw_info.get("index") {
        Some(JsonValue::Number(number)) => number.as_u64().map(|value| value.to_string()),
        Some(JsonValue::String(value)) => value.parse::<u64>().ok().map(|index| index.to_string()),
        _ => None,
    }
}

pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, data_default, display_quote, trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
