//! Bybit Pro subscription policy, hashes and URL selection.

use ccxt::Value;
use serde_json::Value as JsonValue;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{LiveChannel, LiveSpec},
            venue::Venue,
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

pub(in crate::exchanges::ccxt) use crate::exchanges::ccxt::stream::control::stock_unsubscribe as unsubscribe_mode;

const VENUE: Venue = Venue::Bybit;
pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = true;

pub(in crate::exchanges::ccxt) fn allows(_channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "coin" => true,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    if market.raw["option"] == true {
        (100, 100, Some(100))
    } else {
        (1_000, 1_000, Some(1_000))
    }
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
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv::{}::{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    if spec.channel == LiveChannel::Client(RealtimeChannel::Ohlcv) {
        format!("unsubscribe::{}", spec.hash)
    } else {
        format!("unsubscribe:{}", spec.hash)
    }
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::bybit::BybitCore,
    spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok({
        core.get_url_by_market_type(&[
            Value::from(spec.symbol.as_str()),
            Value::Bool(false),
            Value::from("watchOrderBook"),
            Value::from_json(&spec.params),
        ])
        .await
    })
}
