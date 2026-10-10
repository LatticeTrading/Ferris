//! Nado public spot and linear perpetual markets. All wire acquisition and
//! parsing stays in the pinned CCXT provider.
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

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    let gateway = config.nado_gateway_base_url.trim_end_matches('/');
    let archive = config.nado_archive_base_url.trim_end_matches('/');
    Ok(json!({
        "gateway": format!("{gateway}/v1"),
        "gatewayV2": format!("{gateway}/v2"),
        "archive": format!("{archive}/v1"),
        "archiveV2": format!("{archive}/v2"),
        "ws": {"subscriptions": config.nado_ws_url}
    }))
}

pub(in crate::exchanges::ccxt) fn configure(
    _: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // fetch_markets already acquires assets, pairs and symbols. The separate
    // currencies load repeats assets and is not used by Ferris.
    value["has"] = json!({"fetchCurrencies": false});
    Ok(())
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        // Stock loads both families regardless of the selector.
        CatalogScope::Default | CatalogScope::Spot | CatalogScope::Linear => {
            Ok(CatalogScope::Default)
        }
        _ => Err(super::scope_error(Venue::Nado, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _: UnifiedMarketType,
    _: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    if market.id.parse::<u32>().is_err() {
        return Err(ExchangeError::UpstreamData(
            "invalid Nado product id".into(),
        ));
    }
    if market.contract {
        parts.settlement_asset_id = json_string(&market.raw.to_json(), "settleId");
        parts.contract_type = Some("PERPETUAL".into());
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn raw_symbol(market: &Market, info: &JsonValue) -> String {
    json_string(info, "ticker_id").unwrap_or_else(|| market.id.clone())
}

pub(in crate::exchanges::ccxt) fn aliases(
    source: &Market,
    _: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    if let Some(ticker) = json_string(&source.raw.to_json()["info"], "ticker_id") {
        aliases.push(ticker);
    }
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;
pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, data_default, display_quote, market_types, trading_metadata,
};
pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
