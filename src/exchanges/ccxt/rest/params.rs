use serde_json::Value as JsonValue;

use crate::exchanges::{ccxt::venue::Venue, traits::ExchangeError};

pub(in crate::exchanges::ccxt) fn bounded_limit(
    limit: Option<usize>,
    default: usize,
    max: usize,
) -> Result<usize, ExchangeError> {
    if limit == Some(0) {
        return Err(ExchangeError::BadSymbol("limit must be positive".into()));
    }
    Ok(limit.unwrap_or(default).min(max))
}

pub(in crate::exchanges::ccxt) fn timestamp(
    value: Option<u64>,
    field: &str,
) -> Result<Option<i64>, ExchangeError> {
    value
        .map(|value| {
            i64::try_from(value)
                .map_err(|_| ExchangeError::BadSymbol(format!("{field} is too large")))
        })
        .transpose()
}

pub(in crate::exchanges::ccxt) fn check_time_range(
    since: Option<i64>,
    until: Option<i64>,
) -> Result<(), ExchangeError> {
    if matches!((since, until), (Some(start), Some(end)) if start > end) {
        return Err(ExchangeError::BadSymbol("end time precedes since".into()));
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn within(
    time: Option<u64>,
    since: Option<i64>,
    until: Option<i64>,
) -> bool {
    since.is_none_or(|since| time.is_some_and(|time| time >= since as u64))
        && until.is_none_or(|until| time.is_some_and(|time| time <= until as u64))
}

pub(in crate::exchanges::ccxt) fn integer_param(
    params: &JsonValue,
    keys: &[&str],
) -> Result<Option<u64>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("`{key}` must be a nonnegative integer"))
                });
        }
    }
    Ok(None)
}

pub(in crate::exchanges::ccxt) fn string_param<'a>(
    params: &'a JsonValue,
    keys: &[&str],
) -> Result<Option<&'a str>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("`{key}` must be a nonempty string"))
                });
        }
    }
    Ok(None)
}

pub(in crate::exchanges::ccxt) fn bool_param(
    params: &JsonValue,
    keys: &[&str],
) -> Result<Option<bool>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_bool()
                .map(Some)
                .ok_or_else(|| ExchangeError::BadSymbol(format!("`{key}` must be a boolean")));
        }
    }
    Ok(None)
}

pub(in crate::exchanges::ccxt) fn unsupported(venue: Venue, message: &str) -> ExchangeError {
    ExchangeError::UnsupportedFeature(format!("{}: {message}", venue.public_id()))
}
