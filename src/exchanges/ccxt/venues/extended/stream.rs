//! Extended Pro subscription policy, hashes and URL selection.

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

const VENUE: Venue = Venue::Extended;

pub(in crate::exchanges::ccxt) fn unsubscribe_mode(
    _channel: LiveChannel,
) -> crate::exchanges::ccxt::stream::control::UnsubscribeMode {
    crate::exchanges::ccxt::stream::control::UnsubscribeMode::Reconnect
}
pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;

pub(in crate::exchanges::ccxt) fn allows(channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "coin" => true,
        "price" | "candleType" => channel == RealtimeChannel::Ohlcv,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    _market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    (20, 1_000, Some(1_000))
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
    timeframe: Option<&str>,
    params: &JsonValue,
) -> Option<String> {
    timeframe
        .zip(params["candleType"].as_str())
        .map(|(timeframe, kind)| format!("{timeframe}:{kind}"))
}

pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Statistics => "tickers".to_string(),
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trades:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv:{}:{}:{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap(),
            spec.params["candleType"].as_str().unwrap_or("trades")
        ),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    format!("unsubscribe:{}", spec.hash)
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::extended::ExtendedCore,
    spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok({
        let base = field(&core.urls, &["api", "ws"]);
        let base = base
            .as_str()
            .ok_or_else(|| ExchangeError::UpstreamData("missing stock websocket URL".into()))?;
        let id = spec.market["id"]
            .as_str()
            .ok_or_else(|| ExchangeError::UpstreamData("missing stock market id".into()))?;
        Value::from(match spec.channel {
            LiveChannel::Client(RealtimeChannel::OrderBook) => {
                format!("{base}/orderbooks/{id}")
            }
            LiveChannel::Client(RealtimeChannel::Trades) => {
                format!("{base}/publicTrades/{id}")
            }
            LiveChannel::Client(RealtimeChannel::Ohlcv) => {
                let kind = spec.params["candleType"].as_str().unwrap_or("trades");
                let timeframe = spec.timeframe.as_deref().unwrap();
                let interval = ccxt::value::get_value_k(&core.timeframes, timeframe);
                format!(
                    "{base}/candles/{id}/{kind}?interval={}",
                    interval.as_str().unwrap_or(timeframe)
                )
            }
            LiveChannel::Statistics => unreachable!("statistics is Lighter-only"),
        })
    })
}

pub(in crate::exchanges::ccxt) fn provider(
    config: Option<Value>,
) -> ccxt_pro::pro::extended::ExtendedCore {
    let mut core = ccxt_pro::pro::extended::ExtendedCore::new(config);
    core.parent.exchange.userAgent = Value::from("Ferris/1.0");
    core
}
