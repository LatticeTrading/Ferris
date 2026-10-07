//! ApeX Omni perpetuals. Stock owns transport/parsing; these are Ferris policies.
use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            convert::{json_bool, json_string, IdentityParts, TradingMetadata},
            venue::{CatalogScope, Venue},
        },
        traits::ExchangeError,
    },
    models::{UnifiedMarket, UnifiedMarketType},
};

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    // Unlike Extended, stock signing appends only /v3/...; retain /api here.
    Ok(
        json!({"public": config.apex_rest_base_url, "private": config.apex_rest_base_url,
        "ws": {"public": config.apex_ws_url}}),
    )
}

pub(in crate::exchanges::ccxt) fn configure(
    _config: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // Spot currencies are not used by this perpetual-only catalog. Avoid a
    // duplicate /v3/symbols request on every load/reload.
    value["has"] = json!({"fetchCurrencies": false});
    Ok(())
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Linear => Ok(CatalogScope::Default),
        _ => Err(super::scope_error(Venue::Apex, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    // id is the config name (BTC-USDT); id2 is the native REST/WS instrument
    // (BTCUSDT). Keep both resolvable, with the API instrument in the identity.
    parts.native_id = ccxt::value::get_value_k(&market.raw, "id2")
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeError::UpstreamData("Apex market has no id2".into()))?;
    parts.settlement_asset_id = json_string(info, "settleAssetId");
    parts.contract_type = Some("PERPETUAL".into());
    Ok(())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    aliases.push(converted.base.clone());
}

pub(in crate::exchanges::ccxt) fn trading_metadata(
    market: &Market,
    info: &JsonValue,
) -> TradingMetadata {
    let defaults = super::defaults::trading_metadata(market, info);
    TradingMetadata {
        active: defaults.active && json_bool(info, "isPrelaunch") != Some(true),
        // 4.5.85 substitutes minOrderSize, which is NOT a contract multiplier.
        // Do not invent one or scale trades/volume/OI by the order minimum.
        contract_size: None,
    }
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;
pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, data_default, display_quote, market_types, raw_symbol,
};
pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
