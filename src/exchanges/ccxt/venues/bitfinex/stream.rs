//! Channel-ID control only; stock owns all market-data parsing and caches.
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
use serde_json::{json, Value as JsonValue};

pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;
pub(in crate::exchanges::ccxt) fn allows(_: RealtimeChannel, _: &str) -> bool {
    false
}
pub(in crate::exchanges::ccxt) fn book_depth(_: &CatalogMarket) -> (usize, usize, Option<usize>) {
    (25, 100, Some(100))
}
pub(in crate::exchanges::ccxt) fn book_params(
    _: &CatalogMarket,
    _: &JsonValue,
    _: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}
pub(in crate::exchanges::ccxt) use book_params as candle_params;
pub(in crate::exchanges::ccxt) fn candle_cache_key(
    _: Option<&str>,
    _: &JsonValue,
) -> Option<String> {
    None
}
fn interval(timeframe: &str) -> &str {
    match timeframe {
        "1d" => "1D",
        "1w" => "7D",
        "2w" => "14D",
        other => other,
    }
}
pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    let id = spec.market["id"].as_str().expect("catalog id");
    match spec.channel {
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trades:{id}"),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("book:{id}"),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "candles:{}:{id}",
            interval(spec.timeframe.as_deref().unwrap())
        ),
        LiveChannel::Statistics => unreachable!("bulk REST statistics"),
    }
}
pub(in crate::exchanges::ccxt) use super::super::shared_stream::unsubscribe_hash;
pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::bitfinex::BitfinexCore,
    _: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok(field(&core.urls, &["api", "ws", "public"]))
}
pub(in crate::exchanges::ccxt) type Provider =
    Controlled<ccxt_pro::pro::bitfinex::BitfinexCore, BitfinexControl>;
pub(in crate::exchanges::ccxt) fn provider(config: Option<Value>) -> Provider {
    Controlled::new(ccxt_pro::pro::bitfinex::BitfinexCore::new(config))
}
pub(in crate::exchanges::ccxt) fn unsubscribe_mode(channel: LiveChannel) -> UnsubscribeMode {
    match channel {
        LiveChannel::Client(_) => UnsubscribeMode::Native,
        _ => UnsubscribeMode::Reconnect,
    }
}
pub(in crate::exchanges::ccxt) struct BitfinexControl;
impl Protocol for BitfinexControl {
    type Core = ccxt_pro::pro::bitfinex::BitfinexCore;
    const CHANNEL_IDS: bool = true;
    fn subscription(spec: &LiveSpec, _: &Value) -> Result<Subscription, ExchangeError> {
        Ok(Subscription {
            topic: spec.hash.clone(),
            hash: spec.hash.clone(),
            snapshot_required: spec.channel == LiveChannel::Client(RealtimeChannel::OrderBook),
        })
    }
    fn unsubscribe(id: &str) -> Value {
        Value::from_json(
            &json!({"event":"unsubscribe", "chanId":id.parse::<u64>().expect("validated channel ID")}),
        )
    }
    fn incoming(message: &Value) -> Incoming {
        // Inspect only routing/snapshot shape, without copying the market data.
        if let Some(data) = message.as_array() {
            return match data.first().and_then(Value::as_i64).filter(|id| *id > 0) {
                Some(id) => Incoming::RoutedData {
                    id: id.to_string(),
                    snapshot: data
                        .get(1)
                        .and_then(Value::as_array)
                        .is_some_and(|rows| rows.is_empty() || rows[0].as_array().is_some()),
                },
                None => Incoming::Failure("invalid Bitfinex channel ID".into()),
            };
        }
        let m = message.to_json();
        let id = |v: &JsonValue| v.as_u64().filter(|id| *id > 0).map(|id| id.to_string());
        match m["event"].as_str() {
            Some("subscribed") => {
                let Some(id) = id(&m["chanId"]) else {
                    return Incoming::Failure("missing Bitfinex channel ID".into());
                };
                let channel = m["channel"].as_str().unwrap_or("");
                let (topic, cleanup) = if channel == "candles" {
                    let key = m["key"].as_str().unwrap_or("");
                    let Some(key_tail) = key.strip_prefix("trade:") else {
                        return Incoming::Failure("invalid Bitfinex candle key".into());
                    };
                    (
                        format!("candles:{key_tail}"),
                        vec![format!("unsubscribe:{key}")],
                    )
                } else {
                    let symbol = m["symbol"].as_str().unwrap_or("");
                    (format!("{channel}:{symbol}"), vec![])
                };
                Incoming::Subscribed { topic, id, cleanup }
            }
            Some("unsubscribed") => match id(&m["chanId"]) {
                Some(id) => Incoming::Retired {
                    id,
                    accepted: m["status"] == "OK",
                },
                None => Incoming::Failure("missing Bitfinex retirement ID".into()),
            },
            Some("error") => Incoming::Failure(format!("Bitfinex control error: {m}")),
            Some("info")
                if matches!(m["code"].as_i64(), Some(20051 | 20060 | 20061))
                    || m["platform"]["status"] == 0 =>
            {
                Incoming::Failure(format!("Bitfinex service interruption: {m}"))
            }
            _ => Incoming::Other,
        }
    }
    /// Workaround for ccxt 4.5.93 (still on master at 82b5d53fe): stock
    /// `handle_ohlcv` takes the market ID as `key.split(':')[2]`, truncating
    /// IDs that contain a colon (every perp such as `tBTCF0:USTF0`, and spot
    /// pairs such as `tAVAX:USD`). Those candles then resolve
    /// `candles:1D:tBTCF0` and land in another market's cache, so no watcher
    /// ever sees them. For exactly those IDs, run stock's own steps (stock
    /// `parse_ohlcv`, stock cache) with the full ID. Colon-free IDs stay on
    /// stock. Delete once upstream restores the full key tail.
    fn handle_data(core: &mut Self::Core, topic: &str, message: &Value) -> Option<Value> {
        use ccxt::{exchange_generated::ExchangeBase, pro::cache::ArrayCacheByTimestamp};
        let (interval, id) = topic.strip_prefix("candles:")?.split_once(':')?;
        if !id.contains(':') {
            return None;
        }
        let data = message.as_array()?.get(1)?.as_array()?;
        // Snapshot: newest-first list of rows. Update: a single row.
        let rows: Vec<Value> = if data.first().is_some_and(|row| row.as_array().is_some()) {
            data.to_vec()
        } else {
            vec![Value::from(data.to_vec())]
        };
        let market = core.safe_market(&[Value::from(id)]);
        let symbol = ccxt::value::get_value_k(&market, "symbol");
        let timeframe = core.find_timeframe(Value::from(interval), &[]);
        if symbol.as_str().is_none() || timeframe.as_str().is_none() {
            return None;
        }
        let mut by_timeframe = ccxt::get_value(&core.ohlcvs, &symbol);
        if by_timeframe.as_map().is_none() {
            by_timeframe = Value::from_json(&json!({}));
        }
        let mut stored = ccxt::get_value(&by_timeframe, &timeframe);
        if stored == Value::Null {
            let limit = core.safe_integer_k(core.options.clone(), "OHLCVLimit", &[Value::Int(1000)]);
            stored = ArrayCacheByTimestamp::new(limit);
            ccxt::set_value(&mut by_timeframe, &timeframe, stored.clone());
        }
        if core.ohlcvs.as_map().is_none() {
            core.ohlcvs = Value::from_json(&json!({}));
        }
        ccxt::set_value(&mut core.ohlcvs, &symbol, by_timeframe);
        for row in rows.into_iter().rev() {
            stored.append(ExchangeBase::parse_ohlcv(&*core, row, &[market.clone()]));
        }
        Some(stored)
    }
}
