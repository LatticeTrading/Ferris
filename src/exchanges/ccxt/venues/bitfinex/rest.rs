//! Stock REST history and aggregated price-level books.
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
pub(in crate::exchanges::ccxt) fn allows(_: Operation, _: &str) -> bool {
    false
}
pub(in crate::exchanges::ccxt) fn trades_max(_: &CatalogMarket) -> usize {
    10_000
}
pub(in crate::exchanges::ccxt) use trades_max as ohlcv_max;
pub(in crate::exchanges::ccxt) fn book_max(_: &CatalogMarket) -> u64 {
    250
}
pub(in crate::exchanges::ccxt) fn trade_params(until: Option<i64>) -> Params {
    until
        .map(|end| Params::none().with_int("until", end))
        .unwrap_or_else(Params::none)
}
pub(in crate::exchanges::ccxt) async fn trades(
    provider: &mut Provider,
    market: &CatalogMarket,
    since: Option<i64>,
    limit: i64,
    params: Params,
) -> Result<Value, ExchangeError> {
    shared_rest::trades(Venue::Bitfinex, provider, market, since, limit, params).await
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
    let native = [1, 25, 100, 250]
        .into_iter()
        .find(|depth| *depth >= limit)
        .expect("validated depth");
    let mut book =
        shared_rest::book(Venue::Bitfinex, provider, market, native, Params::none()).await?;
    // Stock invents a receipt timestamp. The payload contains no exchange time.
    for key in ["timestamp", "datetime"] {
        ccxt::set_value(&mut book, &Value::from(key), Value::Null);
    }
    Ok(book)
}
