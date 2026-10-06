//! Owned conversion from stock CCXT market metadata into Ferris catalog rows.
//!
//! Nothing in this module lets a `ccxt::Value` or `ccxt::types::Market` escape
//! into shared Ferris state: every row owns a [`UnifiedMarket`] plus owned raw
//! JSON. Identity, settlement, contract size and precision are derived from the
//! loaded stock metadata (including each market's original `info` block), never
//! from ticker text or a row's array position.
//!
//! Precision is interpreted by the caller-supplied CCXT precision mode. All six
//! target venues ship `TICK_SIZE`, but a decimal-places or significant-digits
//! mode is handled truthfully rather than relabeled a fixed tick.

use serde_json::Value as JsonValue;

use ccxt::runtime::{DECIMAL_PLACES, SIGNIFICANT_DIGITS, TICK_SIZE};

use crate::exchanges::traits::ExchangeError;
use crate::market_stats::make_market_id;
use crate::models::{MarketIdentity, UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType};

use super::catalog::CatalogMarket;
use super::venue::Venue;

/// Convert one loaded stock market into an owned catalog row plus the public
/// aliases it may be resolved by.
///
/// Invalid core identity (missing id/base/quote/symbol) is a source failure and
/// is rejected instead of being patched over.
pub(super) fn convert_market(
    venue: Venue,
    market: &ccxt::types::Market,
    precision_mode: i64,
) -> Result<(CatalogMarket, Vec<String>), ExchangeError> {
    let raw = market.raw.to_json();
    let raw_info = raw.get("info").unwrap_or(&JsonValue::Null);

    let exchange_market_id = require_nonempty(&market.id, "market id")?;
    let base = require_nonempty(&market.base, "base asset")?;
    let quote = require_nonempty(&market.quote, "quote asset")?;
    let ccxt_symbol = require_nonempty(&market.symbol, "unified symbol")?;

    let market_type = unified_market_type(market)?;

    // Venue naming policy may distinguish native display and unified assets.
    let display_quote =
        super::venues::dispatch!(venue, exchange => exchange::display_quote(&raw_info, &quote));
    let display_symbol = format!("{base}/{display_quote}");

    let identity = build_identity(venue, market, market_type, &raw_info)?;

    // Policies interpret loaded metadata, but cannot patch identity or bypass
    // shared numeric validation. All built-in venues use the same defaults.
    let metadata =
        super::venues::dispatch!(venue, exchange => exchange::trading_metadata(market, raw_info));
    let min_order_size = nonnegative(market.limits.amount.min, "minimum order size")?;
    let tick_size = tick_size_from_precision(market.precision.price, precision_mode);
    let contract_size = nonnegative(metadata.contract_size, "contract size")?;

    let unified = UnifiedMarket {
        exchange: venue.public_id().to_string(),
        symbol: display_symbol,
        base,
        quote: display_quote,
        market_type,
        active: metadata.active,
        min_order_size,
        tick_size,
        contract_size,
        info: UnifiedMarketInfo {
            category: venue_category(venue, market),
            raw_symbol: Some(raw_symbol(venue, market, &raw_info)),
            exchange_symbol: Some(exchange_market_id),
            is_rfq: json_bool(&raw_info, "isRfq"),
            is_off_hours: json_bool(raw_info, "isOffHours"),
        },
        identity: Some(identity),
    };

    // Alias policy sees the final identity/display values, never an intermediate
    // identity or an independently reconstructed display pair.
    let aliases = build_aliases(venue, market, &unified, raw_info);

    Ok((
        CatalogMarket {
            market: unified,
            ccxt_symbol,
            linear: market.linear,
            inverse: market.inverse,
            raw,
        },
        aliases,
    ))
}

/// Map a CCXT precision value to a fixed tick size, honoring the mode.
///
/// `TICK_SIZE` precision *is* the tick; decimal places are converted to their
/// tick; significant digits cannot be expressed as a single fixed tick and are
/// therefore reported as missing rather than mislabeled.
pub(super) fn tick_size_from_precision(precision: Option<f64>, precision_mode: i64) -> Option<f64> {
    let precision = precision?;
    if !precision.is_finite() {
        return None;
    }
    match precision_mode {
        TICK_SIZE => (precision > 0.0).then_some(precision),
        DECIMAL_PLACES if precision.fract() == 0.0 && (-308.0..=308.0).contains(&precision) => {
            let tick = 10f64.powi(-(precision as i32));
            (tick.is_finite() && tick > 0.0).then_some(tick)
        }
        SIGNIFICANT_DIGITS => None,
        _ => None,
    }
}

fn unified_market_type(market: &ccxt::types::Market) -> Result<UnifiedMarketType, ExchangeError> {
    match market.market_type.as_str() {
        "spot" if market.spot => Ok(UnifiedMarketType::Spot),
        "swap" if market.swap => Ok(UnifiedMarketType::Perp),
        "future" if market.future => Ok(UnifiedMarketType::Future),
        "option" if market.option => Ok(UnifiedMarketType::Option),
        _ => Err(ExchangeError::UpstreamData(format!(
            "unknown or inconsistent market type for {}",
            market.symbol
        ))),
    }
}

fn build_identity(
    venue: Venue,
    market: &ccxt::types::Market,
    market_type: UnifiedMarketType,
    raw_info: &JsonValue,
) -> Result<MarketIdentity, ExchangeError> {
    let mut parts = IdentityParts {
        native_id: market.id.clone(),
        dex: None,
        category: None,
        settle: nonempty(market.settle.clone()),
        settlement_asset_id: None,
        contract_type: json_string(raw_info, "contractType"),
    };
    super::venues::dispatch!(venue, exchange => exchange::identity(market, market_type, raw_info, &mut parts))?;
    let IdentityParts {
        native_id,
        dex,
        category,
        settle,
        settlement_asset_id,
        contract_type,
    } = parts;
    Ok(MarketIdentity {
        market_id: make_market_id(
            venue.public_id(),
            market_type,
            category.as_deref(),
            dex.as_deref(),
            &native_id,
        )?,
        exchange_market_id: native_id,
        category,
        dex,
        contract_type,
        settle,
        settlement_asset_id,
    })
}

fn raw_symbol(venue: Venue, market: &ccxt::types::Market, raw_info: &JsonValue) -> String {
    super::venues::dispatch!(venue, exchange => exchange::raw_symbol(market, raw_info))
}

fn venue_category(venue: Venue, market: &ccxt::types::Market) -> Option<String> {
    super::venues::dispatch!(venue, exchange => exchange::category(market))
}

fn build_aliases(
    venue: Venue,
    source: &ccxt::types::Market,
    converted: &UnifiedMarket,
    raw_info: &JsonValue,
) -> Vec<String> {
    let mut aliases = vec![
        converted.symbol.clone(),
        source.symbol.clone(),
        // The native exchange id is a resolution alias for every product, not a
        // uniqueness key: Bybit spot and linear share `BTCUSDT` on purpose.
        source.id.clone(),
    ];
    if let Some(identity) = converted.identity.as_ref() {
        aliases.push(identity.exchange_market_id.clone());
        aliases.push(identity.market_id.clone());
    }
    if let Some(name) = json_string(raw_info, "name") {
        aliases.push(name);
    }

    // The borrowed source includes raw unified fields (e.g. id2) and original
    // info. Only owned alias strings leave the local policy boundary.
    super::venues::dispatch!(venue, exchange => exchange::aliases(source, converted, &mut aliases));

    aliases.retain(|alias| !alias.trim().is_empty());
    aliases
}

fn require_nonempty(value: &str, label: &str) -> Result<String, ExchangeError> {
    if value.is_empty() || value.trim() != value {
        return Err(ExchangeError::UpstreamData(format!(
            "market is missing a valid {label}"
        )));
    }
    Ok(value.to_string())
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

pub(in crate::exchanges::ccxt) fn json_string(value: &JsonValue, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(in crate::exchanges::ccxt) fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

fn nonnegative(value: Option<f64>, name: &str) -> Result<Option<f64>, ExchangeError> {
    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(ExchangeError::UpstreamData(format!("invalid {name}")));
    }
    Ok(value)
}

mod snapshot;
pub(super) use snapshot::{convert_book, convert_candles, convert_trades};

#[cfg(test)]
mod market_tests;
#[cfg(test)]
mod tests;

/// Venue interpretation of trading metadata, not a mutable row/identity patch.
/// Contract size remains unvalidated here: shared conversion rejects negative
/// and nonfinite values, and preserves missing values and zero.
pub(in crate::exchanges::ccxt) struct TradingMetadata {
    pub active: bool,
    pub contract_size: Option<f64>,
}

pub(in crate::exchanges::ccxt) struct IdentityParts {
    pub native_id: String,
    pub dex: Option<String>,
    pub category: Option<String>,
    pub settle: Option<String>,
    pub settlement_asset_id: Option<String>,
    pub contract_type: Option<String>,
}
