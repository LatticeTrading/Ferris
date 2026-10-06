//! Bybit REST request policy and stock-method exceptions.

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
            venue::{exchange_error, Provider, Venue},
            venues::shared_rest,
        },
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
};

const VENUE: Venue = Venue::Bybit;
pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = false;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = false;
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

pub(in crate::exchanges::ccxt) fn trades_max(market: &CatalogMarket) -> usize {
    if market.market.market_type == UnifiedMarketType::Spot {
        60
    } else {
        1_000
    }
}

pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    1_000
}

pub(in crate::exchanges::ccxt) fn book_max(market: &CatalogMarket) -> u64 {
    if market.market.market_type == UnifiedMarketType::Option {
        25
    } else {
        10_000
    }
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
    let params = shared_rest::candle_price(VENUE, market, input, true, true)?;
    Ok(params)
}

pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    input: &JsonValue,
) -> Result<Value, ExchangeError> {
    let params = Params::none();
    let rpi = bool_param(input, &["rpi"])? == Some(true);
    let regular_max = if market.market.market_type == UnifiedMarketType::Spot {
        200
    } else {
        1_000
    };
    if limit > regular_max || rpi {
        return fetch_bybit_special_book(provider, market, limit, rpi).await;
    }
    shared_rest::book(VENUE, provider, market, limit, params).await
}

async fn fetch_bybit_special_book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    rpi: bool,
) -> Result<Value, ExchangeError> {
    let category = market
        .market
        .info
        .category
        .as_deref()
        .ok_or_else(|| ExchangeError::UpstreamData("missing Bybit market category".into()))?;
    let native_id = market
        .raw
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ExchangeError::UpstreamData("missing Bybit market id".into()))?;
    let mut params = Params::none()
        .with_str("category", category)
        .with_str("symbol", native_id);
    let method = if rpi {
        if !matches!(category, "linear" | "inverse") || limit > 1_000 {
            return Err(unsupported(
                Venue::Bybit,
                "RPI books require a contract and at most 1000 levels",
            ));
        }
        params = params.with_int("limit", limit as i64);
        "public_get_v5_market_rpi_orderbook"
    } else {
        "public_get_v5_market_full_orderbook"
    };
    let response = provider
        .call(method, vec![params.into_value_object()])
        .await
        .map_err(|error| exchange_error(Venue::Bybit, error))?;
    let result = ccxt::value::get_value_k(&response, "result");
    let row = result
        .as_map()
        .filter(|row| {
            row.get("b").and_then(Value::as_array).is_some()
                && row.get("a").and_then(Value::as_array).is_some()
        })
        .ok_or_else(|| ExchangeError::UpstreamData("invalid Bybit book response".into()))?;
    let timestamp = row.get("ts").cloned().unwrap_or_default();
    provider
        .call(
            "parse_order_book",
            vec![
                result,
                Value::from(market.ccxt_symbol.as_str()),
                timestamp,
                Value::from("b"),
                Value::from("a"),
            ],
        )
        .await
        .map_err(|error| exchange_error(Venue::Bybit, error))
}
