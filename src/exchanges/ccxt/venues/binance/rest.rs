//! Binance REST request policy and stock-method exceptions.

use ccxt::{Params, Value};
use serde_json::Value as JsonValue;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            rest::{
                params::{bool_param, unsupported},
                Operation,
            },
            venue::{Provider, Venue},
            venues::shared_rest,
        },
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
};

const VENUE: Venue = Venue::Binance;
pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = false;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = false;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] = &["until", "endTime"];

pub(in crate::exchanges::ccxt) fn allows(operation: Operation, key: &str) -> bool {
    match key {
        "coin" => operation != Operation::Markets,
        "price" => operation == Operation::Ohlcv,
        "rpi" => operation == Operation::Book,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn trades_max(_market: &CatalogMarket) -> usize {
    1_000
}

pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    1_000
}

pub(in crate::exchanges::ccxt) fn book_max(market: &CatalogMarket) -> u64 {
    if market.market.market_type == UnifiedMarketType::Spot {
        5_000
    } else {
        1_000
    }
}

pub(in crate::exchanges::ccxt) fn trade_params(until: Option<i64>) -> Params {
    until
        .map(|until| Params::none().with_int("until", until))
        .unwrap_or_else(Params::none)
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
    let params = shared_rest::candle_price(VENUE, market, input, true, false)?;
    Ok(params)
}

pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    input: &JsonValue,
) -> Result<Value, ExchangeError> {
    let mut params = Params::none();
    if let Some(rpi) = bool_param(input, &["rpi"])? {
        if rpi && market.linear != Some(true) {
            return Err(unsupported(VENUE, "RPI books require a linear contract"));
        }
        params = params.with_bool("rpi", rpi);
    }
    let limit = if market.market.market_type != UnifiedMarketType::Spot {
        [5, 10, 20, 50, 100, 500, 1_000]
            .into_iter()
            .find(|n| *n >= limit)
            .expect("bounded depth")
    } else {
        limit
    };
    shared_rest::book(VENUE, provider, market, limit, params).await
}
