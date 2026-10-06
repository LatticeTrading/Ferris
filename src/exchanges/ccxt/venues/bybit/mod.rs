//! Bybit provider configuration and catalog identity policy.

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
    Ok(json!({
        "spot": config.bybit_base_url,
        "futures": config.bybit_base_url,
        "v2": config.bybit_base_url,
        "public": config.bybit_base_url,
        "private": config.bybit_base_url
    }))
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    Ok(scope)
}

pub(in crate::exchanges::ccxt) fn market_types(
    scope: CatalogScope,
) -> Option<&'static [&'static str]> {
    match scope {
        CatalogScope::Spot => Some(&["spot"]),
        CatalogScope::Linear => Some(&["linear"]),
        CatalogScope::Inverse => Some(&["inverse"]),
        CatalogScope::Option => Some(&["option"]),
        CatalogScope::Default => Some(&["linear", "inverse", "spot"]),
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    parts.category = category(market);
    if market.contract {
        parts.settle = json_string(info, "settleCoin").or(parts.settle.take());
        parts.settlement_asset_id = parts.settle.clone();
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn category(market: &Market) -> Option<String> {
    Some(
        if market.option {
            "option"
        } else if market.spot {
            "spot"
        } else if market.linear == Some(true) {
            "linear"
        } else {
            "inverse"
        }
        .to_string(),
    )
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
    entry.market.market_type != UnifiedMarketType::Option
}

pub(in crate::exchanges::ccxt) fn data_default() -> (Option<UnifiedMarketType>, Option<&'static str>)
{
    (None, Some("linear"))
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;

pub(in crate::exchanges::ccxt) use super::defaults::{
    configure, display_quote, raw_symbol, trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Linear;
