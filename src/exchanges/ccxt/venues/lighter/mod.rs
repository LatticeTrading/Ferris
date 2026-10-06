//! Lighter provider configuration and catalog identity policy.

use std::borrow::Cow;

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
        "root": config.lighter_rest_base_url,
        "public": config.lighter_rest_base_url,
        "private": config.lighter_rest_base_url,
        "ws": config.lighter_ws_url
    }))
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Spot | CatalogScope::Linear => {
            Ok(CatalogScope::Default)
        }
        _ => Err(scope_error(Venue::Lighter, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    if market.id.parse::<u64>().is_err() {
        return Err(ExchangeError::UpstreamData(
            "invalid Lighter market id".into(),
        ));
    }
    parts.settlement_asset_id = info.get("quote_asset_id").and_then(|value| {
        value
            .as_u64()
            .map(|id| id.to_string())
            .or_else(|| value.as_str().map(str::to_string))
    });
    Ok(())
}

pub(in crate::exchanges::ccxt) fn raw_symbol(market: &Market, info: &JsonValue) -> String {
    json_string(info, "symbol").unwrap_or_else(|| market.id.clone())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    aliases.push(converted.base.clone());
    if converted.market_type == UnifiedMarketType::Perp {
        aliases.push(format!("{}/USD", converted.base));
    }
}

pub(in crate::exchanges::ccxt) fn symbol_override(
    params: &JsonValue,
) -> Result<Option<Cow<'_, str>>, ExchangeError> {
    params
        .get("market_id")
        .or_else(|| params.get("marketId"))
        .filter(|value| !value.is_null())
        .map(|value| match value {
            JsonValue::String(value) if value.trim().parse::<u64>().is_ok() => {
                Ok(std::borrow::Cow::Borrowed(value.trim()))
            }
            JsonValue::Number(value) => value
                .as_u64()
                .map(|id| std::borrow::Cow::Owned(id.to_string()))
                .ok_or_else(|| ExchangeError::BadSymbol("invalid Lighter market id".into())),
            _ => Err(ExchangeError::BadSymbol("invalid Lighter market id".into())),
        })
        .transpose()
}

pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, configure, data_default, display_quote, market_types,
    trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
