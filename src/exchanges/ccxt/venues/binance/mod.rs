//! Binance provider configuration and catalog identity policy.

use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            convert::{json_string, IdentityParts},
            venue::CatalogScope,
        },
        traits::ExchangeError,
    },
    models::{UnifiedMarket, UnifiedMarketType},
};

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    Ok({
        let base = config.binance_base_url.trim_end_matches('/');
        let mut urls = json!({
            "fapiPublic": format!("{base}/fapi/v1"),
            "fapiPrivate": format!("{base}/fapi/v1"),
            "fapiPublicV2": format!("{base}/fapi/v2"),
            "fapiPrivateV2": format!("{base}/fapi/v2"),
            "fapiPublicV3": format!("{base}/fapi/v3"),
            "fapiPrivateV3": format!("{base}/fapi/v3")
        });
        // A custom gateway serves all public products. With the default
        // USD-M host, leave the other stock product hosts intact.
        if base != "https://fapi.binance.com" {
            urls["dapiPublic"] = json!(format!("{base}/dapi/v1"));
            urls["eapiPublic"] = json!(format!("{base}/eapi/v1"));
            urls["public"] = json!(format!("{base}/api/v3"));
        }
        urls
    })
}

pub(in crate::exchanges::ccxt) fn configure(
    _config: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    value["options"]["fetchCurrencies"] = json!(false);
    Ok(())
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default => Ok(CatalogScope::Linear),
        _ => Ok(scope),
    }
}

pub(in crate::exchanges::ccxt) fn market_types(
    scope: CatalogScope,
) -> Option<&'static [&'static str]> {
    match scope {
        CatalogScope::Spot => Some(&["spot"]),
        CatalogScope::Linear => Some(&["linear"]),
        CatalogScope::Inverse => Some(&["inverse"]),
        CatalogScope::Option => Some(&["option"]),
        CatalogScope::Default => None,
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    if market.contract {
        parts.settle = json_string(info, "marginAsset").or(parts.settle.take());
        parts.settlement_asset_id = parts.settle.clone();
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    if converted.market_type == UnifiedMarketType::Perp && converted.quote == "USDT" {
        aliases.push(converted.base.clone());
    }
}

pub(in crate::exchanges::ccxt) fn catalog_default(entry: &CatalogMarket) -> bool {
    entry.linear == Some(true) && entry.market.market_type != UnifiedMarketType::Option
}

pub(in crate::exchanges::ccxt) fn data_default() -> (Option<UnifiedMarketType>, Option<&'static str>)
{
    (None, Some("linear"))
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;

pub(in crate::exchanges::ccxt) use super::defaults::{
    category, display_quote, raw_symbol, trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
