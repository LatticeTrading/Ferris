//! KuCoin public snapshot limits and supported parameters.
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
    // `fetch_trades` ignores `since`/`limit`; the venue returns a recent window.
    100
}
pub(in crate::exchanges::ccxt) fn ohlcv_max(market: &CatalogMarket) -> usize {
    // Stock contract candles cap at 200; spot/UTA at 1500.
    if market.raw["contract"] == true {
        200
    } else {
        1500
    }
}
pub(in crate::exchanges::ccxt) fn book_max(_: &CatalogMarket) -> u64 {
    100
}

// `since`/`until` are local filters over the newest-N window: the venue has no
// native time-bounded public trade request.
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
    shared_rest::trades(Venue::Kucoin, provider, market, since, limit, params).await
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
    // KuCoin only accepts native depth 20 or 100; acquire the smallest tier
    // that covers the requested top-N and let the shared layer truncate.
    let native = if limit <= 20 { 20 } else { 100 };
    shared_rest::book(Venue::Kucoin, provider, market, native, Params::none()).await
}
