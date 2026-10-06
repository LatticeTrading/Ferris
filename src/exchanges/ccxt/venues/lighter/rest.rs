//! Lighter REST request policy and stock-method exceptions.

use ccxt::{value::get_value_k, Params, Value};
use serde_json::{json, Value as JsonValue};

use crate::exchanges::{
    ccxt::{
        catalog::CatalogMarket,
        rest::{params::bool_param, Operation},
        venue::{exchange_error, Provider, Venue},
        venues::shared_rest,
    },
    traits::ExchangeError,
};

const VENUE: Venue = Venue::Lighter;
pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = true;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] =
    &["until", "endTimestamp", "end_timestamp", "endTime"];

pub(in crate::exchanges::ccxt) fn allows(operation: Operation, key: &str) -> bool {
    match key {
        "market_id" | "marketId" => operation != Operation::Markets,
        "endTimestamp" | "end_timestamp" | "set_timestamp_to_end" | "setTimestampToEnd" => {
            operation == Operation::Ohlcv
        }
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn trades_max(_market: &CatalogMarket) -> usize {
    100
}

pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    500
}

pub(in crate::exchanges::ccxt) fn book_max(_market: &CatalogMarket) -> u64 {
    100
}

pub(in crate::exchanges::ccxt) fn trade_params(_until: Option<i64>) -> Params {
    Params::none()
}

pub(in crate::exchanges::ccxt) async fn trades(
    provider: &mut Provider,
    market: &CatalogMarket,
    _since: Option<i64>,
    limit: i64,
    _params: Params,
) -> Result<Value, ExchangeError> {
    // This pin ships the endpoint and parser, but no unified method.
    let result = provider
        .call(
            "public_get_recent_trades",
            vec![Value::from_json(
                &json!({"market_id": market.raw["id"], "limit":limit}),
            )],
        )
        .await
        .map_err(|error| exchange_error(VENUE, error))?;
    shared_rest::parse_trades(VENUE, provider, market, get_value_k(&result, "trades")).await
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
) -> Result<Params, ExchangeError> {
    let mut params = shared_rest::candle_price(VENUE, market, input, false, false)?;
    if let Some(value) = bool_param(input, &["set_timestamp_to_end", "setTimestampToEnd"])? {
        params = params.with_bool("set_timestamp_to_end", value);
    }
    Ok(params)
}

pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    _input: &JsonValue,
) -> Result<Value, ExchangeError> {
    let params = Params::none();
    shared_rest::book(VENUE, provider, market, limit, params).await
}
