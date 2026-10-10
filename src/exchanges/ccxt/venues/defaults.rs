//! Default catalog/configuration policies reused by built-in exchanges.
use crate::exchanges::ccxt::{
    catalog::CatalogMarket,
    convert::{json_bool, TradingMetadata},
    venue::CatalogScope,
};
use crate::{config::Config, exchanges::traits::ExchangeError, models::UnifiedMarketType};
use ccxt::types::Market;
use serde_json::Value as JsonValue;

pub(in crate::exchanges::ccxt) fn statistics_selection(
    _product: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    _native_id: &str,
    _params: &JsonValue,
) -> bool {
    category.is_none() && dex.is_none()
}

pub(in crate::exchanges::ccxt) fn configure(
    _config: &Config,
    _value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}

/// Default interpretation, deliberately shared by every existing venue.
/// A venue with a justified exception can wrap/replace this function locally;
/// it receives the loaded market (including raw unified fields) and its info.
/// Validation and final owned-row assembly still belong to conversion.
pub(in crate::exchanges::ccxt) fn trading_metadata(
    market: &Market,
    info: &JsonValue,
) -> TradingMetadata {
    TradingMetadata {
        // Preserve the historical application of these flags to ALL venues and
        // products, not just the venue associated with the isPreListing name.
        active: market.active
            && json_bool(info, "isPreListing")
                .map(|prelisting| !prelisting)
                .unwrap_or(true)
            && json_bool(info, "active").unwrap_or(true),
        // Read the stock Value, not its JSON projection: JSON turns nonfinite
        // numbers into null, which would hide an invalid contract size.
        // Do not fall back to order limits or infer a multiplier.
        contract_size: ccxt::value::get_value_k(&market.raw, "contractSize").as_f64(),
    }
}

pub(in crate::exchanges::ccxt) fn display_quote(_info: &JsonValue, quote: &str) -> String {
    quote.to_string()
}
pub(in crate::exchanges::ccxt) fn raw_symbol(market: &Market, _info: &JsonValue) -> String {
    market.id.clone()
}
pub(in crate::exchanges::ccxt) fn category(_market: &Market) -> Option<String> {
    None
}
pub(in crate::exchanges::ccxt) fn catalog_default(_entry: &CatalogMarket) -> bool {
    true
}
pub(in crate::exchanges::ccxt) fn data_default() -> (Option<UnifiedMarketType>, Option<&'static str>)
{
    (None, None)
}
pub(in crate::exchanges::ccxt) fn market_types(
    _scope: CatalogScope,
) -> Option<&'static [&'static str]> {
    None
}
