//! Aster Pro subscription policy, hashes and URL selection.

use ccxt::Value;
use serde_json::Value as JsonValue;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{params::field, LiveChannel, LiveSpec},
            venue::Venue,
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

const VENUE: Venue = Venue::Aster;
pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = true;

pub(in crate::exchanges::ccxt) fn allows(_channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "coin" => true,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    _market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    (20, 20, Some(20))
}

pub(in crate::exchanges::ccxt) fn book_params(
    _market: &CatalogMarket,
    _input: &JsonValue,
    _params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    super::super::shared_stream::candle_params(VENUE, market, input, params, false)
}

pub(in crate::exchanges::ccxt) fn candle_cache_key(
    _timeframe: Option<&str>,
    _params: &JsonValue,
) -> Option<String> {
    None
}

pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Statistics => "tickers".to_string(),
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade::{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv:{}:{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    super::super::shared_stream::unsubscribe_hash(spec)
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::aster::AsterCore,
    spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok(field(
        &core.urls,
        &[
            "api",
            "ws",
            "public",
            spec.market["type"].as_str().unwrap_or("swap"),
        ],
    ))
}
