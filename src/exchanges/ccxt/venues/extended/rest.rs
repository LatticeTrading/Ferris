//! Extended REST request policy and stock-method exceptions.

use ccxt::{Params, Value};
use serde_json::Value as JsonValue;

use crate::exchanges::{
    ccxt::{
        catalog::CatalogMarket,
        rest::{
            params::{string_param, unsupported},
            Operation,
        },
        venue::{Provider, Venue},
        venues::shared_rest,
    },
    traits::ExchangeError,
};

const VENUE: Venue = Venue::Extended;
pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = false;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] = &["endTime", "until"];

pub(in crate::exchanges::ccxt) fn allows(operation: Operation, key: &str) -> bool {
    match key {
        "coin" => operation != Operation::Markets,
        "price" => operation == Operation::Ohlcv,
        "candleType" => operation == Operation::Ohlcv,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn trades_max(_market: &CatalogMarket) -> usize {
    1_000
}

pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    10_000
}

pub(in crate::exchanges::ccxt) fn book_max(_market: &CatalogMarket) -> u64 {
    1_000
}

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
    shared_rest::trades(VENUE, provider, market, since, limit, params).await
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
) -> Result<Params, ExchangeError> {
    let mut params = shared_rest::candle_price(VENUE, market, input, false, false)?;
    if let Some(kind) = string_param(input, &["candleType"])? {
        if !matches!(kind, "trades" | "mark-prices" | "index-prices") {
            return Err(unsupported(VENUE, "unsupported candleType"));
        }
        // Explicit candleType takes precedence over price in stock CCXT.
        params = params.with_str("candleType", kind);
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
