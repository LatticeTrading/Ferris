use ccxt::Value;
use serde_json::Value as JsonValue;

use crate::exchanges::{ccxt::venue::Venue, traits::ExchangeError};

pub(in crate::exchanges::ccxt) fn string<'a>(
    input: &'a JsonValue,
    keys: &[&str],
) -> Result<Option<&'a str>, ExchangeError> {
    for key in keys {
        match input.get(key) {
            None | Some(JsonValue::Null) => continue,
            Some(JsonValue::String(value)) if !value.trim().is_empty() => {
                return Ok(Some(value.trim()))
            }
            _ => {
                return Err(ExchangeError::BadSymbol(format!(
                    "{key} must be a nonempty string"
                )))
            }
        }
    }
    Ok(None)
}

pub(in crate::exchanges::ccxt) fn integer(
    input: &JsonValue,
    keys: &[&str],
) -> Result<Option<u64>, ExchangeError> {
    for key in keys {
        if let Some(value) = input.get(key).filter(|value| !value.is_null()) {
            return value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("{key} must be a nonnegative integer"))
                });
        }
    }
    Ok(None)
}

pub(in crate::exchanges::ccxt) fn unsupported(venue: Venue, message: &str) -> ExchangeError {
    ExchangeError::UnsupportedFeature(format!("{}: {message}", venue.public_id()))
}

pub(in crate::exchanges::ccxt) fn field(value: &Value, path: &[&str]) -> Value {
    path.iter().fold(value.clone(), |value, key| {
        ccxt::value::get_value_k(&value, key)
    })
}
