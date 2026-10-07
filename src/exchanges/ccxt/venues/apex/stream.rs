//! Fixed-depth stock Apex streams with native subscription control.
use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{
                control::{Controlled, Incoming, Protocol, Subscription, UnsubscribeMode},
                params::field,
                LiveChannel, LiveSpec,
            },
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};
use ccxt::Value;
use serde_json::Value as JsonValue;

pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;
pub(in crate::exchanges::ccxt) fn allows(_channel: RealtimeChannel, key: &str) -> bool {
    key == "coin"
}
pub(in crate::exchanges::ccxt) fn book_depth(
    _market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    // One topic/cache per symbol. Arbitrary display slices share orderBook200;
    // requesting another native depth would collide on stock's message hash.
    (25, 200, Some(200))
}
pub(in crate::exchanges::ccxt) fn book_params(
    _market: &CatalogMarket,
    _input: &JsonValue,
    _params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}
pub(in crate::exchanges::ccxt) use book_params as candle_params;
pub(in crate::exchanges::ccxt) fn candle_cache_key(
    _timeframe: Option<&str>,
    _params: &JsonValue,
) -> Option<String> {
    None
}
pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv::{}::{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
        LiveChannel::Statistics => unreachable!("Apex statistics use bulk REST polling"),
    }
}
pub(in crate::exchanges::ccxt) use super::super::shared_stream::unsubscribe_hash;
pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::apex::ApexCore,
    _spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    // Stable owner identity. The driver adds a new timestamp for each session
    // and binds it to options.wsPublicUrl before any stock watch is queued.
    Ok(field(&core.urls, &["api", "ws", "public"]))
}
pub(in crate::exchanges::ccxt) type Provider =
    Controlled<ccxt_pro::pro::apex::ApexCore, ApexControl>;

pub(in crate::exchanges::ccxt) fn provider(config: Option<Value>) -> Provider {
    let mut core = ccxt_pro::pro::apex::ApexCore::new(config);
    index_market_ids(&mut core);
    Controlled::new(core)
}

pub(in crate::exchanges::ccxt) fn unsubscribe_mode(channel: LiveChannel) -> UnsubscribeMode {
    match channel {
        LiveChannel::Client(_) => UnsubscribeMode::Native,
        LiveChannel::Statistics => UnsubscribeMode::Reconnect,
    }
}

pub(in crate::exchanges::ccxt) struct ApexControl;
impl Protocol for ApexControl {
    fn subscription(spec: &LiveSpec, timeframes: &Value) -> Result<Subscription, ExchangeError> {
        let id = spec.market["id2"]
            .as_str()
            .ok_or_else(|| ExchangeError::UpstreamData("missing Apex id2".into()))?;
        let topic = match spec.channel {
            LiveChannel::Client(RealtimeChannel::Trades) => format!("recentlyTrade.H.{id}"),
            LiveChannel::Client(RealtimeChannel::OrderBook) => {
                format!("orderBook{}.H.{id}", spec.depth.unwrap_or(25))
            }
            LiveChannel::Client(RealtimeChannel::Ohlcv) => {
                let timeframe = spec
                    .timeframe
                    .as_deref()
                    .ok_or_else(|| ExchangeError::Internal("missing candle timeframe".into()))?;
                let interval = ccxt::value::get_value_k(timeframes, timeframe);
                let interval = interval
                    .as_str()
                    .ok_or_else(|| ExchangeError::UpstreamData("missing Apex interval".into()))?;
                format!("candle.{interval}.{id}")
            }
            LiveChannel::Statistics => {
                return Err(ExchangeError::Internal(
                    "Apex statistics are REST-only".into(),
                ))
            }
        };
        Ok(Subscription {
            topic,
            hash: spec.hash.clone(),
            snapshot_required: spec.channel == LiveChannel::Client(RealtimeChannel::OrderBook),
        })
    }

    fn unsubscribe(topic: &str) -> Value {
        Value::from_json(&serde_json::json!({"op":"unsubscribe", "args":[topic]}))
    }

    fn incoming(message: &Value) -> Incoming {
        let request = ccxt::value::get_value_k(message, "request");
        let op = ccxt::value::get_value_k(&request, "op");
        if matches!(op.as_str(), Some("subscribe" | "unsubscribe")) {
            let args = ccxt::value::get_value_k(&request, "args");
            let topics = args
                .as_array()
                .map(|args| {
                    args.iter()
                        .filter_map(|arg| arg.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            return Incoming::Ack {
                topics,
                unsubscribe: op.as_str() == Some("unsubscribe"),
                accepted: ccxt::value::get_value_k(message, "success").as_bool() == Some(true),
            };
        }
        if let Some(topic) = ccxt::value::get_value_k(message, "topic").as_str() {
            return Incoming::Data {
                topic: topic.to_string(),
                snapshot: ccxt::value::get_value_k(message, "type").as_str() == Some("snapshot"),
            };
        }
        Incoming::Other
    }
}

pub(in crate::exchanges::ccxt) fn index_market_ids(core: &mut ccxt_pro::pro::apex::ApexCore) {
    // Stock load_markets adds id2 aliases AFTER set_markets, but constructor
    // seeding and set_markets alone do not. Mirror only that metadata index;
    // do not rewrite market IDs or replace any stock data parser/cache.
    if let Some(markets) = core.markets.clone().as_map() {
        for market in markets.values() {
            let id2 = ccxt::value::get_value_k(market, "id2");
            if id2.as_str().is_some_and(|id| !id.is_empty()) {
                ccxt::set_value(
                    &mut core.markets_by_id,
                    &id2,
                    Value::from(vec![market.clone()]),
                );
            }
        }
    }
}

pub(in crate::exchanges::ccxt) fn session_url(base: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("valid receipt clock")
        .as_millis();
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}timestamp={timestamp}")
}
