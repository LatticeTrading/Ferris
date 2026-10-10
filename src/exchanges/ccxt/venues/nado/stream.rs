//! Stock trade/candle streams. The production gateway currently requires
//! permessage-deflate, absent from the pinned transport (HTTP 403); loopback
//! endpoints still qualify the adapter independently of that transport gap.
//! Books are deliberately rejected: 4.5.85 fetches a snapshot BEFORE subscribing and
//! cannot bridge it to the first delta (no snapshot maxTimestamp). Nado's
//! documented subscribe/buffer/snapshot/replay procedure is not implemented.
use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{
                control::{Controlled, Incoming, Protocol, Subscription},
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
pub(in crate::exchanges::ccxt) fn allows(_: RealtimeChannel, _: &str) -> bool {
    false
}
pub(in crate::exchanges::ccxt) fn book_depth(_: &CatalogMarket) -> (usize, usize, Option<usize>) {
    (100, 100, Some(100))
}
pub(in crate::exchanges::ccxt) fn book_params(
    _: &CatalogMarket,
    _: &JsonValue,
    _: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Err(ExchangeError::UnsupportedFeature("Nado live books require snapshot/delta synchronization absent from CCXT 4.5.85; use fetchOrderBook".into()))
}
pub(in crate::exchanges::ccxt) fn candle_params(
    _: &CatalogMarket,
    _: &JsonValue,
    _: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}
pub(in crate::exchanges::ccxt) fn candle_cache_key(
    _: Option<&str>,
    _: &JsonValue,
) -> Option<String> {
    None
}
pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv:{}:{}",
            spec.timeframe.as_deref().unwrap(),
            spec.symbol
        ),
        LiveChannel::Statistics => unreachable!("Nado statistics use bulk REST polling"),
    }
}
pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    format!("unsubscribe:{}", spec.hash)
}
pub(in crate::exchanges::ccxt) fn unsubscribe_mode(
    _: LiveChannel,
) -> crate::exchanges::ccxt::stream::control::UnsubscribeMode {
    // Stock unwatch clears the data hash but leaves subscribe:<stream JSON>
    // owned, so re-add sends no subscription. Candle cleanup also splits the
    // settlement colon. Rebuild the URL owner instead of reusing stale state.
    crate::exchanges::ccxt::stream::control::UnsubscribeMode::Reconnect
}
pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::nado::NadoCore,
    _: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok(field(&core.urls, &["api", "ws", "subscriptions"]))
}
pub(in crate::exchanges::ccxt) type Provider =
    Controlled<ccxt_pro::pro::nado::NadoCore, NadoControl>;
pub(in crate::exchanges::ccxt) fn provider(config: Option<Value>) -> Provider {
    Controlled::new(ccxt_pro::pro::nado::NadoCore::new(config))
}

pub(in crate::exchanges::ccxt) struct NadoControl;
impl Protocol for NadoControl {
    type Core = ccxt_pro::pro::nado::NadoCore;
    const HEARTBEAT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(15));
    fn ping(core: &mut Self::Core) -> Value {
        core.ping(Value::Null)
    }
    fn pong(message: &Value) -> Option<String> {
        if field(message, &["result", "method"]).as_str() != Some("pong") {
            return None;
        }
        let id = field(message, &["id"]);
        id.as_str()
            .map(str::to_owned)
            .or_else(|| id.as_i64().map(|id| id.to_string()))
    }
    fn subscription(spec: &LiveSpec, _: &Value) -> Result<Subscription, ExchangeError> {
        Ok(Subscription {
            topic: spec.hash.clone(),
            hash: spec.hash.clone(),
            snapshot_required: false,
        })
    }
    fn unsubscribe(_: &str) -> Value {
        Value::Null
    }
    fn incoming(message: &Value) -> Incoming {
        // A rejected subscription may settle a stock control hash, not a data
        // hash. Surface it to the single reader rather than hanging quietly.
        if !field(message, &["error"]).is_null()
            || field(message, &["status"]).as_str() == Some("failure")
        {
            Incoming::Failure(format!("Nado subscription error: {}", message.to_json()))
        } else {
            Incoming::Other
        }
    }
}
