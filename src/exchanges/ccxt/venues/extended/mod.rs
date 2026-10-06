//! Extended provider configuration and catalog identity policy.

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
    Ok({
        // Ferris config includes /api/v1; stock signing adds /api/{version}.
        let base = config.extended_rest_base_url.trim_end_matches('/');
        let base = base.strip_suffix("/api/v1").ok_or_else(|| {
            ExchangeError::Internal(
                "EXTENDED_REST_BASE_URL must end in /api/v1 for stock CCXT signing".into(),
            )
        })?;
        json!({"rest": base, "ws": config.extended_ws_url})
    })
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default | CatalogScope::Linear => Ok(CatalogScope::Default),
        _ => Err(scope_error(Venue::Extended, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    _market: &Market,
    product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    parts.settle = json_string(info, "collateralAssetName").or(parts.settle.take());
    parts.settlement_asset_id = info
        .pointer("/l2Config/collateralId")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    if product == UnifiedMarketType::Perp {
        parts.contract_type = Some("PERPETUAL".into());
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn display_quote(info: &JsonValue, quote: &str) -> String {
    // Native collateral is USD; stock CCXT remaps it to USDC. Keep the familiar
    // display pair while identity() above retains native settlement metadata.
    json_string(info, "collateralAssetName").unwrap_or_else(|| quote.to_string())
}

pub(in crate::exchanges::ccxt) fn category(_market: &Market) -> Option<String> {
    Some("perpetual".to_string())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    let (base, quote) = (&converted.base, &converted.quote);
    aliases.push(base.clone());
    aliases.push(format!("{base}-{quote}"));
    aliases.push(format!("{base}/{quote}:{quote}"));
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;

pub(in crate::exchanges::ccxt) fn rest_provider(config: Option<ccxt::Value>) -> ccxt::Extended {
    let mut core = ccxt::exchanges::extended::ExtendedCore::new(config);
    // Extended rejects a missing User-Agent; 4.5.85 ignores its config key.
    core.exchange.userAgent = ccxt::Value::from("Ferris/1.0");
    ccxt::Extended::from_core(core)
}

pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, configure, data_default, market_types, raw_symbol, trading_metadata,
};

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
