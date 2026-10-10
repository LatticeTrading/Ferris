//! Nado REST snapshot policy. No invented start-time pagination or price source.
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
// max_time is supported, but since is only a filter over the newest-N window.
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = false;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] = &["until", "endTime"];

pub(in crate::exchanges::ccxt) fn allows(_: Operation, _: &str) -> bool {
    false
}
pub(in crate::exchanges::ccxt) fn trades_max(_: &CatalogMarket) -> usize {
    500
}
pub(in crate::exchanges::ccxt) fn ohlcv_max(_: &CatalogMarket) -> usize {
    500
}
pub(in crate::exchanges::ccxt) fn book_max(_: &CatalogMarket) -> u64 {
    // Conservative Ferris qualification, not an asserted exchange maximum.
    100
}
pub(in crate::exchanges::ccxt) fn trade_params(_: Option<i64>) -> Params {
    Params::none()
}
pub(in crate::exchanges::ccxt) async fn trades(
    provider: &mut Provider,
    market: &CatalogMarket,
    since: Option<i64>,
    limit: i64,
    params: Params,
) -> Result<Value, ExchangeError> {
    shared_rest::trades(Venue::Nado, provider, market, since, limit, params).await
}
pub(in crate::exchanges::ccxt) fn candle_params(
    _: &CatalogMarket,
    _: &JsonValue,
) -> Result<Params, ExchangeError> {
    Ok(Params::none())
}
pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    _: &JsonValue,
) -> Result<Value, ExchangeError> {
    shared_rest::book(Venue::Nado, provider, market, limit, Params::none()).await
}
