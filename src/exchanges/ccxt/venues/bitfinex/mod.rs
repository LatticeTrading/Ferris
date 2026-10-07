//! Bitfinex public spot and linear perpetual markets.
use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            convert::IdentityParts,
            venue::{CatalogScope, Venue},
        },
        traits::ExchangeError,
    },
    models::{UnifiedMarket, UnifiedMarketType},
};
use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    Ok(json!({"public":config.bitfinex_rest_base_url, "ws":{"public":config.bitfinex_ws_url}}))
}
pub(in crate::exchanges::ccxt) fn configure(
    _: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // Raw R0 drops order IDs but does not aggregate duplicate prices in stock.
    value["options"]["fetchOrderBook"] = json!({"precision":"P0"});
    Ok(())
}
pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Spot | CatalogScope::Linear => {
            Ok(CatalogScope::Default)
        }
        _ => Err(super::scope_error(Venue::Bitfinex, scope)),
    }
}
pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _: UnifiedMarketType,
    _: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    if market.contract {
        // Stock settleId is the unified quote, NOT the native collateral ID.
        parts.settlement_asset_id = ccxt::value::get_value_k(&market.raw, "quoteId")
            .as_str()
            .map(str::to_string);
        parts.contract_type = Some("PERPETUAL".into());
    }
    Ok(())
}
pub(in crate::exchanges::ccxt) fn aliases(
    source: &Market,
    _: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    if let Some(pair) = source.id.strip_prefix('t') {
        aliases.push(pair.to_string());
    }
}
pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;
pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, data_default, display_quote, market_types, raw_symbol,
    trading_metadata,
};
pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
