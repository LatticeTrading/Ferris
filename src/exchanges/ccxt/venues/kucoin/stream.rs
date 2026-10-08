//! KuCoin public streams: stock Pro owns transport, subscriptions, parsers and
//! book maintenance. The public socket URL is minted per session by a REST
//! `bullet-public` negotiation; `url()` is only a stable owner identity.
//!
//! `Controlled` bridges the snapshot driver's base methods to the actual
//! KuCoin cache-index and delta handlers. It also schedules stock JSON pings:
//! the pinned transport only emits WebSocket Ping, unsupported by KuCoin.
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

pub(in crate::exchanges::ccxt) fn allows(_: RealtimeChannel, _: &str) -> bool {
    false
}
pub(in crate::exchanges::ccxt) fn book_depth(_: &CatalogMarket) -> (usize, usize, Option<usize>) {
    // One incremental topic/cache per symbol. Native depths 5/50 would select
    // partial-depth spot streams with different message hashes; 100 keeps a
    // single backing book that arbitrary display slices share.
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

pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trades:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "candles:{}:{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
        LiveChannel::Statistics => unreachable!("KuCoin statistics use bulk REST polling"),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    // Stock un_watch hashes are `unsubscribe:` + the subscription hash.
    format!("unsubscribe:{}", spec.hash)
}

/// Stable owner identity per product. The actual connected URL is negotiated
/// fresh for every session and seeded into stock's options by `bind_url`.
pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::kucoin::KucoinCore,
    spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    let key = if spec.market["contract"].as_bool() == Some(true) {
        "futures"
    } else {
        "spot"
    };
    Ok(field(&core.urls, &["api", "ws", key]))
}

/// Negotiate the tokenized public socket URL for this session. Called once per
/// (re)connect; the token can expire across a long-lived owner.
pub(in crate::exchanges::ccxt) async fn session_url(
    core: &mut ccxt_pro::pro::kucoin::KucoinCore,
    spec: &LiveSpec,
) -> Result<String, ExchangeError> {
    let is_futures = spec.market["contract"].as_bool() == Some(true);
    let url = core
        .negotiate(Value::Bool(false), &[Value::Bool(is_futures)])
        .await;
    url.as_str()
        .filter(|url| url.starts_with("ws://") || url.starts_with("wss://"))
        .map(str::to_string)
        .ok_or_else(|| {
            ExchangeError::UpstreamData("KuCoin negotiate returned no websocket URL".into())
        })
}

/// Seed stock's `options.urls` so every watch/unwatch reuses the negotiated
/// session URL instead of minting a second token and a different registry key.
pub(in crate::exchanges::ccxt) fn bind_url(
    core: &mut ccxt_pro::pro::kucoin::KucoinCore,
    url: &str,
) {
    let mut urls = ccxt::value::get_value_k(&core.options, "urls");
    if urls.as_map().is_none() {
        urls = Value::from_json(&serde_json::json!({}));
    }
    for connect in ["public", "publicFutures"] {
        ccxt::set_value(&mut urls, &Value::from(connect), Value::from(url));
    }
    ccxt::set_value(&mut core.options, &Value::from("urls"), urls);
}

pub(in crate::exchanges::ccxt) fn unsubscribe_mode(channel: LiveChannel) -> UnsubscribeMode {
    match channel {
        LiveChannel::Client(_) => UnsubscribeMode::Stock,
        LiveChannel::Statistics => UnsubscribeMode::Reconnect,
    }
}

pub(in crate::exchanges::ccxt) type Provider =
    Controlled<ccxt_pro::pro::kucoin::KucoinCore, KucoinControl>;

pub(in crate::exchanges::ccxt) fn provider(config: Option<Value>) -> Provider {
    Controlled::new(ccxt_pro::pro::kucoin::KucoinCore::new(config))
}

pub(in crate::exchanges::ccxt) struct KucoinControl;

impl Protocol for KucoinControl {
    type Core = ccxt_pro::pro::kucoin::KucoinCore;
    // Below KuCoin's normal 18s public bullet interval. The pinned negotiate
    // method discards keepAlive on a temporary Value; ws_client ignores it.
    const HEARTBEAT: Option<std::time::Duration> = Some(std::time::Duration::from_secs(5));

    fn ping(core: &mut Self::Core) -> Value {
        core.ping(Value::Null)
    }

    fn pong(message: &Value) -> Option<String> {
        (ccxt::value::get_value_k(message, "type").as_str() == Some("pong"))
            .then(|| {
                ccxt::value::get_value_k(message, "id")
                    .as_str()
                    .map(str::to_owned)
            })
            .flatten()
    }

    fn subscription(spec: &LiveSpec, _: &Value) -> Result<Subscription, ExchangeError> {
        // KuCoin uses stock unsubscribe; this identity is never retired.
        Ok(Subscription {
            topic: spec.hash.clone(),
            hash: spec.hash.clone(),
            snapshot_required: false,
        })
    }

    fn unsubscribe(_: &str) -> Value {
        Value::Null
    }

    fn incoming(_: &Value) -> Incoming {
        Incoming::Other
    }

    /// KuCoin's dynamic dispatcher exposes the single-delta handler, while the
    /// inherited batch handler is not routed by `KucoinCore::call_dynamic`.
    const REPLAY_BOOK_DELTAS: bool = true;

    fn replay_book_delta(core: &mut Self::Core, orderbook: Value, delta: Value) {
        // Call the exposed inherent handler directly: unlike string dispatch,
        // a missing/renamed method cannot silently return Null. No parser copy.
        core.handle_book_delta(orderbook, delta);
    }

    /// Preserve the pinned KuCoin implementation's snapshot selection exactly.
    /// This calls `KucoinCore::get_cache_index`, including its end-of-cache
    /// return value and malformed-entry handling.
    fn get_cache_index(core: &Self::Core, orderbook: &Value, deltas: &Value) -> Option<Value> {
        // Use the pinned venue implementation directly. This retains its
        // sequence/sequenceStart/sequenceEnd fallbacks and malformed-entry
        // behavior for both spot and futures feeds without copying book logic.
        Some(core.get_cache_index(orderbook.clone(), deltas.clone()))
    }
}
