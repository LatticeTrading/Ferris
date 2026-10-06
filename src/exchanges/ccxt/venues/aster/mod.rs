//! Aster provider configuration and catalog identity policy.

use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
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
    Ok({
        let base = config.aster_base_url.trim_end_matches('/');
        let mut urls = json!({
            "fapiPublic": format!("{base}/fapi"),
            "fapiPrivate": format!("{base}/fapi")
        });
        if base != "https://fapi.asterdex.com" {
            urls["sapiPublic"] = json!(format!("{base}/api"));
        }
        urls
    })
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Spot | CatalogScope::Linear => {
            Ok(CatalogScope::Default)
        }
        _ => Err(scope_error(Venue::Aster, scope)),
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

pub(in crate::exchanges::ccxt) fn category(market: &Market) -> Option<String> {
    Some(if market.spot { "spot" } else { "futures" }.to_string())
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
    entry.market.market_type == UnifiedMarketType::Perp
}

pub(in crate::exchanges::ccxt) fn data_default() -> (Option<UnifiedMarketType>, Option<&'static str>)
{
    (Some(UnifiedMarketType::Perp), None)
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;

pub(in crate::exchanges::ccxt) use super::defaults::{
    configure, display_quote, market_types, raw_symbol, trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
