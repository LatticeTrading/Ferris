//! Apex public snapshot limits and supported parameters.
use crate::exchanges::{
    ccxt::{
        catalog::CatalogMarket,
        rest::Operation,
        venue::{Provider, Venue},
        venues::shared_rest,
    },
    traits::ExchangeError,
};
use ccxt::{Params, Value};
use serde_json::Value as JsonValue;

pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = false;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = false;
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = false;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] = &["until", "endTime"];

pub(in crate::exchanges::ccxt) fn allows(operation: Operation, key: &str) -> bool {
    key == "coin" && operation != Operation::Markets
}
pub(in crate::exchanges::ccxt) fn trades_max(_market: &CatalogMarket) -> usize {
    1_000
}
pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    200
}
pub(in crate::exchanges::ccxt) fn book_max(_market: &CatalogMarket) -> u64 {
    200
}

// The venue offers only newest-N trades. Time bounds are filters on that
// window, never promises of historical coverage or native request parameters.
pub(in crate::exchanges::ccxt) fn trade_params(_until: Option<i64>) -> Params {
    Params::none()
}
pub(in crate::exchanges::ccxt) async fn trades(
    provider: &mut Provider,
    market: &CatalogMarket,
    since: Option<i64>,
    limit: i64,
    params: Params,
) -> Result<Value, ExchangeError> {
    shared_rest::trades(Venue::Apex, provider, market, since, limit, params).await
}
pub(in crate::exchanges::ccxt) fn candle_params(
    _market: &CatalogMarket,
    _input: &JsonValue,
) -> Result<Params, ExchangeError> {
    Ok(Params::none())
}
pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    _input: &JsonValue,
) -> Result<Value, ExchangeError> {
    shared_rest::book(Venue::Apex, provider, market, limit, Params::none()).await
}
