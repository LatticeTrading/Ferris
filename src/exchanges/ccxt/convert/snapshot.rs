use serde_json::Value as JsonValue;

use crate::{
    exchanges::traits::ExchangeError,
    models::{CcxtFee, CcxtOhlcv, CcxtOrderBook, CcxtTrade},
};

// Use the stock unified Values rather than the pin's lossy typed candle/book
// wrappers: those fill missing OHLCV cells with zero or drop malformed levels.
pub(in crate::exchanges::ccxt) fn convert_trades(
    venue: super::Venue,
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
                // Apex does not publish cost. Stock safe_trade invents it using
                // minOrderSize as contractSize, yielding a wrong notional. Keep
                // the optional field absent rather than publish or recompute it.
                cost: if venue == super::Venue::Apex {
                    None
                } else {
                    snapshot_number(row, "cost")?
                },
                fee,
            })
        })
        .collect()
}

pub(in crate::exchanges::ccxt) fn convert_candles(
    value: ccxt::Value,
) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
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

pub(in crate::exchanges::ccxt) fn convert_book(
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
