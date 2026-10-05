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
use crate::models::{
    CcxtFee, CcxtOhlcv, CcxtOrderBook, CcxtTrade, MarketIdentity, UnifiedMarket, UnifiedMarketInfo,
    UnifiedMarketType,
};

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

    // Extended's native collateral naming is `USD`; CCXT remaps it to `USDC`.
    // Keep the familiar display pair while the identity keeps the native settle.
    let display_quote = if matches!(venue, Venue::Extended) {
        json_string(&raw_info, "collateralAssetName").unwrap_or_else(|| quote.clone())
    } else {
        quote.clone()
    };
    let display_symbol = format!("{base}/{display_quote}");

    let identity = build_identity(venue, market, market_type, &raw_info)?;

    // A pre-listing derivative and an explicitly inactive row are not tradable
    // even when the venue's coarse `active` flag is true.
    let active = market.active
        && json_bool(&raw_info, "isPreListing")
            .map(|prelisting| !prelisting)
            .unwrap_or(true)
        && json_bool(&raw_info, "active").unwrap_or(true);

    let min_order_size = nonnegative(market.limits.amount.min, "minimum order size")?;
    let tick_size = tick_size_from_precision(market.precision.price, precision_mode);
    let contract_size = nonnegative(
        ccxt::value::get_value_k(&market.raw, "contractSize").as_f64(),
        "contract size",
    )?;

    let unified = UnifiedMarket {
        exchange: venue.public_id().to_string(),
        symbol: display_symbol.clone(),
        base: base.clone(),
        quote: display_quote.clone(),
        market_type,
        active,
        min_order_size,
        tick_size,
        contract_size,
        info: UnifiedMarketInfo {
            category: venue_category(venue, market),
            raw_symbol: Some(raw_symbol(venue, market, &raw_info)),
            exchange_symbol: Some(exchange_market_id.clone()),
            is_rfq: json_bool(&raw_info, "isRfq"),
            is_off_hours: json_bool(raw_info, "isOffHours"),
        },
        identity: Some(identity),
    };

    let aliases = build_aliases(
        venue,
        market_type,
        &market.id,
        &base,
        &display_quote,
        &display_symbol,
        &ccxt_symbol,
        unified.identity.as_ref(),
        &raw_info,
    );

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
    let mut native_id = market.id.clone();
    let mut dex = None;
    let category = (venue == Venue::Bybit)
        .then(|| venue_category(venue, market))
        .flatten();
    let mut settle = nonempty(market.settle.clone());
    let mut settlement_asset_id = None;
    let mut contract_type = json_string(raw_info, "contractType");
    match venue {
        Venue::Binance | Venue::Aster => {
            if market.contract {
                settle = json_string(raw_info, "marginAsset").or(settle);
                settlement_asset_id = settle.clone();
            }
        }
        Venue::Bybit => {
            if market.contract {
                settle = json_string(raw_info, "settleCoin").or(settle);
                settlement_asset_id = settle.clone();
            }
        }
        Venue::Hyperliquid => match market_type {
            UnifiedMarketType::Perp => {
                native_id = json_string(raw_info, "name").ok_or_else(|| {
                    ExchangeError::UpstreamData("Hyperliquid perpetual has no native name".into())
                })?;
                let market_dex = json_string(raw_info, "dex").unwrap_or_default();
                if !market_dex.is_empty() && json_string(raw_info, "collateralTokenName").is_none()
                {
                    return Err(ExchangeError::UpstreamData(
                        "HIP3 collateral metadata is not qualified".into(),
                    ));
                }
                dex = Some(market_dex);
            }
            UnifiedMarketType::Spot => {
                let index = hyperliquid_spot_index(raw_info).ok_or_else(|| {
                    ExchangeError::UpstreamData(
                        "Hyperliquid spot market has no native index".into(),
                    )
                })?;
                native_id = format!("@{index}");
            }
            _ => {}
        },
        Venue::Lighter => {
            if market.id.parse::<u64>().is_err() {
                return Err(ExchangeError::UpstreamData(
                    "invalid Lighter market id".into(),
                ));
            }
            settlement_asset_id = raw_info.get("quote_asset_id").and_then(|value| {
                value
                    .as_u64()
                    .map(|id| id.to_string())
                    .or_else(|| value.as_str().map(str::to_string))
            });
        }
        Venue::Extended => {
            settle = json_string(raw_info, "collateralAssetName").or(settle);
            settlement_asset_id = raw_info
                .pointer("/l2Config/collateralId")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            if market_type == UnifiedMarketType::Perp {
                contract_type = Some("PERPETUAL".into());
            }
        }
    }
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
    match venue {
        Venue::Lighter => json_string(raw_info, "symbol").unwrap_or_else(|| market.id.clone()),
        Venue::Hyperliquid => json_string(raw_info, "name").unwrap_or_else(|| market.id.clone()),
        _ => market.id.clone(),
    }
}

fn venue_category(venue: Venue, market: &ccxt::types::Market) -> Option<String> {
    match venue {
        Venue::Bybit => Some(if market.option {
            "option".to_string()
        } else if market.spot {
            "spot".to_string()
        } else if market.linear == Some(true) {
            "linear".to_string()
        } else {
            "inverse".to_string()
        }),
        Venue::Aster => Some(if market.spot { "spot" } else { "futures" }.to_string()),
        Venue::Extended => Some("perpetual".to_string()),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_aliases(
    venue: Venue,
    market_type: UnifiedMarketType,
    native_id: &str,
    base: &str,
    display_quote: &str,
    display_symbol: &str,
    ccxt_symbol: &str,
    identity: Option<&MarketIdentity>,
    raw_info: &JsonValue,
) -> Vec<String> {
    let mut aliases = vec![
        display_symbol.to_string(),
        ccxt_symbol.to_string(),
        // The native exchange id is a resolution alias for every product, not a
        // uniqueness key: Bybit spot and linear share `BTCUSDT` on purpose.
        native_id.to_string(),
    ];
    if let Some(identity) = identity {
        aliases.push(identity.exchange_market_id.clone());
        aliases.push(identity.market_id.clone());
    }
    if let Some(name) = json_string(raw_info, "name") {
        aliases.push(name);
    }

    if matches!(venue, Venue::Hyperliquid | Venue::Lighter | Venue::Extended)
        || (matches!(venue, Venue::Binance | Venue::Bybit | Venue::Aster)
            && market_type == UnifiedMarketType::Perp
            && display_quote == "USDT")
    {
        aliases.push(base.to_string());
    }
    if venue == Venue::Extended {
        aliases.push(format!("{base}-{display_quote}"));
        aliases.push(format!("{base}/{display_quote}:{display_quote}"));
    }
    if venue == Venue::Lighter && market_type == UnifiedMarketType::Perp {
        aliases.push(format!("{base}/USD"));
    }

    aliases.retain(|alias| !alias.trim().is_empty());
    aliases
}

fn hyperliquid_spot_index(raw_info: &JsonValue) -> Option<String> {
    match raw_info.get("index") {
        Some(JsonValue::Number(number)) => number.as_u64().map(|value| value.to_string()),
        Some(JsonValue::String(value)) => value.parse::<u64>().ok().map(|index| index.to_string()),
        _ => None,
    }
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

fn json_string(value: &JsonValue, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

fn nonnegative(value: Option<f64>, name: &str) -> Result<Option<f64>, ExchangeError> {
    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(ExchangeError::UpstreamData(format!("invalid {name}")));
    }
    Ok(value)
}

// Use the stock unified Values rather than the pin's lossy typed candle/book
// wrappers: those fill missing OHLCV cells with zero or drop malformed levels.
pub(super) fn convert_trades(
    value: ccxt::Value,
    symbol: &str,
) -> Result<Vec<CcxtTrade>, ExchangeError> {
    let rows = value
        .as_array()
        .ok_or_else(|| invalid_snapshot("trades must be an array"))?;
    rows.iter()
        .map(|row| {
            let row = snapshot_object(row)?;
            let fee = match row.get("fee") {
                None | Some(ccxt::Value::Null) => None,
                Some(value) => {
                    let fee = snapshot_object(value)?;
                    Some(CcxtFee {
                        currency: snapshot_string(fee, "currency")?,
                        cost: snapshot_number(fee, "cost")?,
                        rate: snapshot_number(fee, "rate")?,
                    })
                }
            };
            Ok(CcxtTrade {
                info: row
                    .get("info")
                    .map(ccxt::Value::to_json)
                    .unwrap_or(JsonValue::Null),
                amount: snapshot_number(row, "amount")?,
                datetime: snapshot_string(row, "datetime")?,
                id: snapshot_string(row, "id")?,
                order: snapshot_string(row, "order")?,
                price: snapshot_number(row, "price")?,
                timestamp: snapshot_integer(row, "timestamp")?,
                trade_type: snapshot_string(row, "type")?,
                side: snapshot_string(row, "side")?,
                symbol: snapshot_symbol(row, symbol)?,
                taker_or_maker: snapshot_string(row, "takerOrMaker")?,
                cost: snapshot_number(row, "cost")?,
                fee,
            })
        })
        .collect()
}

pub(super) fn convert_candles(value: ccxt::Value) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
    let rows = value
        .as_array()
        .ok_or_else(|| invalid_snapshot("candles must be an array"))?;
    rows.iter()
        .map(|row| {
            let cells = row
                .as_array()
                .filter(|cells| cells.len() >= 6)
                .ok_or_else(|| invalid_snapshot("candle must contain six cells"))?;
            let volume = match &cells[5] {
                ccxt::Value::Null => None,
                value => Some(snapshot_f64(value)?),
            };
            Ok((
                snapshot_u64(&cells[0])?,
                snapshot_f64(&cells[1])?,
                snapshot_f64(&cells[2])?,
                snapshot_f64(&cells[3])?,
                snapshot_f64(&cells[4])?,
                volume,
            ))
        })
        .collect()
}

pub(super) fn convert_book(
    value: ccxt::Value,
    symbol: &str,
) -> Result<CcxtOrderBook, ExchangeError> {
    snapshot_object(&value)?;
    let levels = |key: &str| -> Result<Vec<(f64, f64)>, ExchangeError> {
        let side = ccxt::value::get_value_k(&value, key);
        if let Some(pairs) = ccxt::value::side_price_amounts(&side) {
            if Some(pairs.len() as i64) != ccxt::runtime::get_array_length(&side).as_i64()
                || pairs.iter().any(|[price, amount]| {
                    !price.is_finite() || !amount.is_finite() || *price <= 0.0 || *amount < 0.0
                })
            {
                return Err(invalid_snapshot("invalid stock book side"));
            }
            return Ok(pairs
                .into_iter()
                .map(|[price, amount]| (price, amount))
                .collect());
        }
        let levels = side
            .as_array()
            .ok_or_else(|| invalid_snapshot("book side must be an array"))?;
        levels
            .iter()
            .map(|level| {
                let cells = level
                    .as_array()
                    .filter(|cells| cells.len() >= 2)
                    .ok_or_else(|| invalid_snapshot("book level must contain price and amount"))?;
                let price = snapshot_f64(&cells[0])?;
                let amount = snapshot_f64(&cells[1])?;
                if price <= 0.0 || amount < 0.0 {
                    return Err(invalid_snapshot("invalid book price or amount"));
                }
                Ok((price, amount))
            })
            .collect()
    };
    // A live book keeps scalars in the stock shared metadata store, not in the
    // shallow Dict. Read them while this owner exclusively holds the core.
    let integer = |key| match ccxt::value::get_value_k(&value, key) {
        ccxt::Value::Null => Ok(None),
        value => snapshot_u64(&value).map(Some),
    };
    let string = |key| match ccxt::value::get_value_k(&value, key) {
        ccxt::Value::Null => Ok(None),
        value => value
            .as_str()
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| invalid_snapshot("book contains a non-string field")),
    };
    let actual_symbol = string("symbol")?;
    if actual_symbol
        .as_deref()
        .is_some_and(|actual| actual != symbol)
    {
        return Err(invalid_snapshot(
            "stock snapshot symbol disagrees with resolved market",
        ));
    }
    Ok(CcxtOrderBook {
        bids: levels("bids")?,
        asks: levels("asks")?,
        datetime: string("datetime")?,
        timestamp: integer("timestamp")?,
        nonce: integer("nonce")?,
        symbol: actual_symbol,
    })
}

fn snapshot_object(
    value: &ccxt::Value,
) -> Result<&ccxt::value::HashMap<String, ccxt::Value>, ExchangeError> {
    value
        .as_map()
        .ok_or_else(|| invalid_snapshot("snapshot row must be an object"))
}

fn snapshot_f64(value: &ccxt::Value) -> Result<f64, ExchangeError> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| invalid_snapshot("snapshot contains a missing or nonfinite number"))
}

fn snapshot_u64(value: &ccxt::Value) -> Result<u64, ExchangeError> {
    match value {
        ccxt::Value::Int(value) => u64::try_from(*value).ok(),
        ccxt::Value::Float(value)
            if value.is_finite()
                && value.fract() == 0.0
                && (0.0..=9_007_199_254_740_991.0).contains(value) =>
        {
            Some(*value as u64)
        }
        ccxt::Value::Str(value) => value.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| invalid_snapshot("snapshot contains an invalid unsigned integer"))
}

fn snapshot_number(
    row: &ccxt::value::HashMap<String, ccxt::Value>,
    key: &str,
) -> Result<Option<f64>, ExchangeError> {
    row.get(key)
        .filter(|value| !matches!(value, ccxt::Value::Null))
        .map(snapshot_f64)
        .transpose()
}

fn snapshot_integer(
    row: &ccxt::value::HashMap<String, ccxt::Value>,
    key: &str,
) -> Result<Option<u64>, ExchangeError> {
    row.get(key)
        .filter(|value| !matches!(value, ccxt::Value::Null))
        .map(snapshot_u64)
        .transpose()
}

fn snapshot_string(
    row: &ccxt::value::HashMap<String, ccxt::Value>,
    key: &str,
) -> Result<Option<String>, ExchangeError> {
    row.get(key)
        .filter(|value| !matches!(value, ccxt::Value::Null))
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_snapshot("snapshot contains a non-string field"))
        })
        .transpose()
}

fn snapshot_symbol(
    row: &ccxt::value::HashMap<String, ccxt::Value>,
    expected: &str,
) -> Result<Option<String>, ExchangeError> {
    let symbol = snapshot_string(row, "symbol")?;
    if symbol.as_ref().is_some_and(|symbol| symbol != expected) {
        return Err(invalid_snapshot(
            "stock snapshot symbol disagrees with resolved market",
        ));
    }
    Ok(symbol)
}

fn invalid_snapshot(message: &str) -> ExchangeError {
    ExchangeError::UpstreamData(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_size_respects_precision_mode() {
        assert_eq!(tick_size_from_precision(Some(0.5), TICK_SIZE), Some(0.5));
        assert_eq!(tick_size_from_precision(Some(0.0), TICK_SIZE), None);
        assert_eq!(tick_size_from_precision(None, TICK_SIZE), None);

        assert_eq!(
            tick_size_from_precision(Some(2.0), DECIMAL_PLACES),
            Some(0.01)
        );
        assert_eq!(tick_size_from_precision(Some(2.5), DECIMAL_PLACES), None);
        assert_eq!(
            tick_size_from_precision(Some(-1.0), DECIMAL_PLACES),
            Some(10.0)
        );

        // Significant digits are not a fixed tick and must not be relabeled one.
        assert_eq!(
            tick_size_from_precision(Some(3.0), SIGNIFICANT_DIGITS),
            None
        );
        assert_eq!(tick_size_from_precision(Some(f64::NAN), TICK_SIZE), None);
    }

    #[test]
    fn incomplete_or_nonfinite_candles_are_not_fabricated() {
        use ccxt::Value;
        let valid = vec![
            Value::Int(1_700_000_000_000),
            Value::Float(10.0),
            Value::Float(12.0),
            Value::Float(9.0),
            Value::Float(11.0),
            Value::Float(3.0),
        ];
        for missing in 0..5 {
            let mut cells = valid.clone();
            cells[missing] = Value::Null;
            assert!(matches!(
                convert_candles(Value::from(vec![Value::from(cells)])),
                Err(ExchangeError::UpstreamData(_))
            ));
        }
        let mut cells = valid.clone();
        cells[5] = Value::Float(f64::NAN);
        assert!(matches!(
            convert_candles(Value::from(vec![Value::from(cells)])),
            Err(ExchangeError::UpstreamData(_))
        ));
        assert_eq!(
            convert_candles(Value::from(vec![Value::from(valid)])).unwrap(),
            vec![(1_700_000_000_000, 10.0, 12.0, 9.0, 11.0, Some(3.0))]
        );
        let no_volume = serde_json::json!([[1_700_000_000_000u64, 10, 12, 9, 11, null]]);
        let candles = convert_candles(Value::from_json(&no_volume)).unwrap();
        assert_eq!(
            candles,
            vec![(1_700_000_000_000, 10.0, 12.0, 9.0, 11.0, None)]
        );
        assert!(serde_json::to_value(candles).unwrap()[0][5].is_null());
    }

    #[test]
    fn invalid_levels_fail_the_entire_book_and_foreign_symbols_are_rejected() {
        use serde_json::json;
        let mut book = json!({"symbol":"BTC/USDT:USDT", "bids":[[10,2],[9,1]], "asks":[[11,3]]});
        for bad_level in [
            json!([9]),
            json!([9, null]),
            json!([9, -1]),
            json!(["NaN", 1]),
        ] {
            book["bids"][1] = bad_level;
            assert!(matches!(
                convert_book(ccxt::Value::from_json(&book), "BTC/USDT:USDT"),
                Err(ExchangeError::UpstreamData(_))
            ));
        }
        book["bids"][1] = json!([9, 1]);
        assert!(matches!(
            convert_book(ccxt::Value::from_json(&book), "ETH/USDT:USDT"),
            Err(ExchangeError::UpstreamData(_))
        ));
        let book = convert_book(ccxt::Value::from_json(&book), "BTC/USDT:USDT").unwrap();
        assert_eq!(book.bids, vec![(10.0, 2.0), (9.0, 1.0)]);
        assert_eq!(book.timestamp, None);
        assert_eq!(book.nonce, None);
    }
}
