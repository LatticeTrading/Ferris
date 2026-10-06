//! Index stock rows without losing native aliases or accepting collisions.

use std::collections::HashMap;

use ccxt::{value::get_value_k, Value};

use crate::exchanges::{ccxt::venue::Venue, traits::ExchangeError};

use super::scalars::positive_integer;

pub(in crate::exchanges::ccxt) fn index_tickers(
    venue: Venue,
    value: &Value,
) -> Result<HashMap<String, Value>, ExchangeError> {
    let map = value.as_map().ok_or_else(|| {
        ExchangeError::UpstreamData(format!(
            "{} statistics tickers response is not an object",
            venue.public_id()
        ))
    })?;
    let mut index: HashMap<String, Value> = HashMap::with_capacity(map.len());
    for (key, ticker) in map.iter() {
        for alias in identity_aliases(key, ticker) {
            match index.get(&alias) {
                Some(existing) if !same_row(existing, ticker) => {
                    return Err(ExchangeError::UpstreamData(format!(
                        "{} statistics response aliases {alias} to conflicting rows",
                        venue.public_id()
                    )));
                }
                Some(_) => {}
                None => {
                    index.insert(alias, ticker.clone());
                }
            }
        }
    }
    Ok(index)
}

pub(in crate::exchanges::ccxt) fn index_intervals(
    venue: Venue,
    value: &Value,
    key: &str,
) -> Result<HashMap<String, u64>, ExchangeError> {
    let map = value.as_map().ok_or_else(|| {
        ExchangeError::UpstreamData(format!(
            "{} funding-interval response is not an object",
            venue.public_id()
        ))
    })?;
    let mut index = HashMap::with_capacity(map.len());
    for (alias, row) in map.iter() {
        let info = get_value_k(row, "info");
        let Some(ms) = positive_integer(&get_value_k(&info, key))
            .and_then(|hours| hours.checked_mul(3_600_000))
        else {
            continue;
        };
        for alias in identity_aliases(alias, row) {
            index.entry(alias).or_insert(ms);
        }
    }
    Ok(index)
}

fn identity_aliases(key: &str, row: &Value) -> Vec<String> {
    let mut aliases = Vec::with_capacity(6);
    if !key.is_empty() {
        aliases.push(key.to_string());
    }
    if let Some(symbol) = get_value_k(row, "symbol").as_str() {
        if !symbol.is_empty() {
            aliases.push(symbol.to_string());
        }
    }
    let info = get_value_k(row, "info");
    for field in ["symbol", "name", "id", "market_id", "marketId", "baseId"] {
        let raw = get_value_k(&info, field);
        if let Some(text) = raw.as_str() {
            if !text.is_empty() {
                aliases.push(text.to_string());
            }
        } else if let Some(number) = raw.as_i64() {
            aliases.push(number.to_string());
        }
    }
    aliases
}

fn same_row(left: &Value, right: &Value) -> bool {
    let left_symbol = get_value_k(left, "symbol");
    let right_symbol = get_value_k(right, "symbol");
    if !left_symbol.is_null() && left_symbol == right_symbol {
        return true;
    }
    let left_id = get_value_k(&get_value_k(left, "info"), "symbol");
    let right_id = get_value_k(&get_value_k(right, "info"), "symbol");
    !left_id.is_null() && left_id == right_id
}
