use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::{
    sync::{broadcast, watch, RwLock},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};

use crate::{
    models::CcxtTrade,
    ws_shared::{
        infer_hyperliquid_coin_from_symbol as infer_hyperliquid_coin_from_symbol_shared,
        parse_binance_trades as parse_binance_trades_shared,
        parse_bybit_trades as parse_bybit_trades_shared,
        parse_hyperliquid_trades as parse_hyperliquid_trades_shared,
        resolve_binance_ws_symbol as resolve_binance_ws_symbol_shared,
        resolve_bybit_ws_symbol as resolve_bybit_ws_symbol_shared,
        resolve_public_symbol as resolve_public_symbol_shared, WsTrade, BINANCE_QUOTES,
        BYBIT_QUOTES,
    },
};

const DEFAULT_EXCHANGE: &str = "hyperliquid";
const BINANCE_FUTURES_WS_BASE_URL: &str = "wss://fstream.binance.com/ws";
const BYBIT_WS_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_WS_LINEAR_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_WS_INVERSE_URL: &str = "wss://stream.bybit.com/v5/public/inverse";
const BYBIT_WS_OPTION_URL: &str = "wss://stream.bybit.com/v5/public/option";

const TOPIC_BROADCAST_CAPACITY: usize = 512;
const INITIAL_RECONNECT_DELAY_MS: u64 = 500;
const MAX_RECONNECT_DELAY_MS: u64 = 15_000;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradesTopic {
    pub exchange: String,
    pub symbol: String,
    pub params: Value,
}

impl TradesTopic {
    pub fn from_client_request(
        exchange: Option<String>,
        symbol: Option<String>,
        params: Value,
    ) -> Result<Self, String> {
        let exchange = exchange.unwrap_or_else(|| DEFAULT_EXCHANGE.to_string());
        let normalized_exchange = exchange.trim().to_ascii_lowercase();
        if normalized_exchange.is_empty() {
            return Err("`exchange` cannot be empty".to_string());
        }

        let symbol = symbol.unwrap_or_default();
        let normalized_symbol = symbol.trim().to_string();
        if normalized_symbol.is_empty() {
            return Err("`symbol` cannot be empty".to_string());
        }

        let normalized_params = normalize_topic_params(params)?;

        Ok(Self {
            exchange: normalized_exchange,
            symbol: normalized_symbol,
            params: normalized_params,
        })
    }
}

pub struct TradesSubscription {
    pub key: String,
    pub topic: TradesTopic,
    pub receiver: broadcast::Receiver<Arc<Vec<CcxtTrade>>>,
}

#[derive(Clone)]
pub struct TradesTopicManager {
    inner: Arc<TradesTopicManagerInner>,
}

struct TradesTopicManagerInner {
    topics: RwLock<HashMap<String, ActiveTopic>>,
    runner: Arc<dyn TradesUpstreamRunner>,
    broadcast_capacity: usize,
}

struct ActiveTopic {
    sender: broadcast::Sender<Arc<Vec<CcxtTrade>>>,
    subscribers: usize,
    shutdown_sender: watch::Sender<bool>,
}

impl TradesTopicManager {
    pub fn new(hyperliquid_base_url: String) -> Self {
        let runner = Arc::new(RealTradesUpstreamRunner::new(hyperliquid_base_url));
        Self {
            inner: Arc::new(TradesTopicManagerInner {
                topics: RwLock::new(HashMap::new()),
                runner,
                broadcast_capacity: TOPIC_BROADCAST_CAPACITY,
            }),
        }
    }

    pub fn resolve_topic_key(&self, topic: TradesTopic) -> Result<String, String> {
        self.inner
            .runner
            .resolve(topic)
            .map(|resolved| resolved.key)
    }

    pub async fn subscribe(&self, topic: TradesTopic) -> Result<TradesSubscription, String> {
        let resolved = self.inner.runner.resolve(topic)?;

        let mut new_topic = None;
        let receiver = {
            let mut topics = self.inner.topics.write().await;
            if let Some(active) = topics.get_mut(&resolved.key) {
                active.subscribers = active.subscribers.saturating_add(1);
                active.sender.subscribe()
            } else {
                let (sender, receiver) = broadcast::channel(self.inner.broadcast_capacity);
                let (shutdown_sender, shutdown_receiver) = watch::channel(false);

                topics.insert(
                    resolved.key.clone(),
                    ActiveTopic {
                        sender: sender.clone(),
                        subscribers: 1,
                        shutdown_sender,
                    },
                );

                new_topic = Some((resolved.clone(), sender, shutdown_receiver));
                receiver
            }
        };

        if let Some((resolved_topic, sender, shutdown_receiver)) = new_topic {
            let runner = self.inner.runner.clone();
            tokio::spawn(async move {
                runner.run(resolved_topic, sender, shutdown_receiver).await;
            });
        }

        Ok(TradesSubscription {
            key: resolved.key,
            topic: resolved.topic,
            receiver,
        })
    }

    pub async fn unsubscribe_by_key(&self, key: &str) {
        let removed = {
            let mut topics = self.inner.topics.write().await;
            match topics.get_mut(key) {
                Some(active) if active.subscribers > 1 => {
                    active.subscribers -= 1;
                    None
                }
                Some(_) => topics.remove(key),
                None => None,
            }
        };

        if let Some(active) = removed {
            let _ = active.shutdown_sender.send(true);
        }
    }

    #[cfg(test)]
    fn with_runner(runner: Arc<dyn TradesUpstreamRunner>) -> Self {
        Self {
            inner: Arc::new(TradesTopicManagerInner {
                topics: RwLock::new(HashMap::new()),
                runner,
                broadcast_capacity: TOPIC_BROADCAST_CAPACITY,
            }),
        }
    }

    #[cfg(test)]
    async fn active_topic_count(&self) -> usize {
        self.inner.topics.read().await.len()
    }
}

#[derive(Clone)]
struct ResolvedTradesTopic {
    key: String,
    topic: TradesTopic,
    stream: UpstreamTradesStream,
}

#[derive(Clone)]
enum UpstreamTradesStream {
    Hyperliquid {
        ws_endpoint: String,
        coin: String,
    },
    Binance {
        ws_endpoint: String,
    },
    Bybit {
        ws_endpoint: String,
        subscribe_topic: String,
    },
}

#[async_trait]
trait TradesUpstreamRunner: Send + Sync {
    fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String>;

    async fn run(
        &self,
        topic: ResolvedTradesTopic,
        sender: broadcast::Sender<Arc<Vec<CcxtTrade>>>,
        shutdown: watch::Receiver<bool>,
    );
}

struct RealTradesUpstreamRunner {
    hyperliquid_ws_endpoint: String,
}

impl RealTradesUpstreamRunner {
    fn new(hyperliquid_base_url: String) -> Self {
        Self {
            hyperliquid_ws_endpoint: to_hyperliquid_ws_url(&hyperliquid_base_url),
        }
    }
}

#[async_trait]
impl TradesUpstreamRunner for RealTradesUpstreamRunner {
    fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
        match topic.exchange.as_str() {
            "hyperliquid" => resolve_hyperliquid_topic(topic, &self.hyperliquid_ws_endpoint),
            "binance" => resolve_binance_topic(topic),
            "bybit" => resolve_bybit_topic(topic),
            other => Err(format!(
                "exchange `{other}` is not supported for realtime trades"
            )),
        }
    }

    async fn run(
        &self,
        topic: ResolvedTradesTopic,
        sender: broadcast::Sender<Arc<Vec<CcxtTrade>>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

        loop {
            if *shutdown.borrow() {
                return;
            }

            let connection_outcome = run_single_connection(&topic, &sender, &mut shutdown).await;

            match connection_outcome {
                ConnectionOutcome::Stop => return,
                ConnectionOutcome::Reconnect { reset_backoff } => {
                    if reset_backoff {
                        reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);
                    }

                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_ok() && *shutdown.borrow() {
                                return;
                            }
                        }
                        _ = sleep(reconnect_delay) => {}
                    }

                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                }
            }
        }
    }
}

enum ConnectionOutcome {
    Stop,
    Reconnect { reset_backoff: bool },
}

async fn run_single_connection(
    topic: &ResolvedTradesTopic,
    sender: &broadcast::Sender<Arc<Vec<CcxtTrade>>>,
    shutdown: &mut watch::Receiver<bool>,
) -> ConnectionOutcome {
    let ws_endpoint = match &topic.stream {
        UpstreamTradesStream::Hyperliquid { ws_endpoint, .. }
        | UpstreamTradesStream::Binance { ws_endpoint }
        | UpstreamTradesStream::Bybit { ws_endpoint, .. } => ws_endpoint.as_str(),
    };

    let Ok((mut stream, _response)) = connect_async(ws_endpoint).await else {
        tracing::warn!(
            topic_key = %topic.key,
            ws_endpoint = %ws_endpoint,
            "failed to connect upstream trades websocket"
        );
        return ConnectionOutcome::Reconnect {
            reset_backoff: false,
        };
    };

    tracing::info!(topic_key = %topic.key, "connected upstream trades websocket");

    match &topic.stream {
        UpstreamTradesStream::Hyperliquid { coin, .. } => {
            if let Err(err) = send_hyperliquid_subscription(&mut stream, coin).await {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Hyperliquid trade subscription");
                return ConnectionOutcome::Reconnect {
                    reset_backoff: false,
                };
            }
        }
        UpstreamTradesStream::Bybit {
            subscribe_topic, ..
        } => {
            if let Err(err) = send_bybit_trade_subscription(&mut stream, subscribe_topic).await {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Bybit trade subscription");
                return ConnectionOutcome::Reconnect {
                    reset_backoff: false,
                };
            }
        }
        UpstreamTradesStream::Binance { .. } => {}
    }

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    let _ = stream.close(None).await;
                    return ConnectionOutcome::Stop;
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(TungsteniteMessage::Text(text))) => {
                        forward_trades(&topic.stream, text.as_ref(), &topic.topic.symbol, sender);
                    }
                    Some(Ok(TungsteniteMessage::Binary(binary))) => {
                        if let Ok(text) = String::from_utf8(binary.to_vec()) {
                            forward_trades(&topic.stream, &text, &topic.topic.symbol, sender);
                        }
                    }
                    Some(Ok(TungsteniteMessage::Ping(payload))) => {
                        if stream.send(TungsteniteMessage::Pong(payload)).await.is_err() {
                            return ConnectionOutcome::Reconnect { reset_backoff: true };
                        }
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) => {
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    Some(Err(err)) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "upstream trades websocket error");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    None => {
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    _ => {}
                }
            }
        }
    }
}

fn forward_trades(
    stream: &UpstreamTradesStream,
    payload: &str,
    symbol: &str,
    sender: &broadcast::Sender<Arc<Vec<CcxtTrade>>>,
) {
    let trades = match stream {
        UpstreamTradesStream::Hyperliquid { .. } => {
            parse_hyperliquid_trades_message(payload, symbol)
        }
        UpstreamTradesStream::Binance { .. } => parse_binance_trades_message(payload, symbol),
        UpstreamTradesStream::Bybit { .. } => parse_bybit_trades_message(payload, symbol),
    };

    if !trades.is_empty() {
        let _ = sender.send(Arc::new(trades));
    }
}

fn resolve_hyperliquid_topic(
    topic: TradesTopic,
    hyperliquid_ws_endpoint: &str,
) -> Result<ResolvedTradesTopic, String> {
    let coin = extract_non_empty_string(&topic.params, "coin")
        .or_else(|| infer_hyperliquid_coin_from_symbol_shared(&topic.symbol))
        .ok_or_else(|| {
            format!(
                "unable to infer Hyperliquid coin from symbol `{}`; pass `params.coin`",
                topic.symbol
            )
        })?;

    Ok(ResolvedTradesTopic {
        key: format!("hyperliquid|trades|coin:{}", coin.to_ascii_lowercase()),
        topic,
        stream: UpstreamTradesStream::Hyperliquid {
            ws_endpoint: hyperliquid_ws_endpoint.to_string(),
            coin,
        },
    })
}

fn resolve_binance_topic(topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_binance_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let stream_symbol = market_symbol.to_ascii_lowercase();
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BINANCE_QUOTES);

    let ws_endpoint = format!(
        "{}/{}@trade",
        BINANCE_FUTURES_WS_BASE_URL.trim_end_matches('/'),
        stream_symbol,
    );

    Ok(ResolvedTradesTopic {
        key: format!("binance|trades|symbol:{}", stream_symbol),
        topic: TradesTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream: UpstreamTradesStream::Binance { ws_endpoint },
    })
}

fn resolve_bybit_topic(topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
    let category = parse_bybit_category(&topic.params)?;
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_bybit_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BYBIT_QUOTES);

    let ws_endpoint = match category {
        BybitCategory::Spot => BYBIT_WS_SPOT_URL,
        BybitCategory::Linear => BYBIT_WS_LINEAR_URL,
        BybitCategory::Inverse => BYBIT_WS_INVERSE_URL,
        BybitCategory::Option => BYBIT_WS_OPTION_URL,
    };

    let subscribe_topic = format!("publicTrade.{market_symbol}");

    Ok(ResolvedTradesTopic {
        key: format!(
            "bybit|trades|category:{}|symbol:{}",
            category.as_str(),
            market_symbol.to_ascii_lowercase()
        ),
        topic: TradesTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream: UpstreamTradesStream::Bybit {
            ws_endpoint: ws_endpoint.to_string(),
            subscribe_topic,
        },
    })
}

async fn send_hyperliquid_subscription(stream: &mut WsStream, coin: &str) -> Result<(), String> {
    let payload = json!({
        "method": "subscribe",
        "subscription": {
            "type": "trades",
            "coin": coin,
        }
    });

    stream
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .map_err(|err| err.to_string())
}

async fn send_bybit_trade_subscription(stream: &mut WsStream, topic: &str) -> Result<(), String> {
    let payload = json!({
        "op": "subscribe",
        "args": [topic],
    });

    stream
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .map_err(|err| err.to_string())
}

fn parse_hyperliquid_trades_message(payload: &str, symbol: &str) -> Vec<CcxtTrade> {
    parse_hyperliquid_trades_shared(payload)
        .into_iter()
        .map(|trade| ws_trade_to_ccxt(trade, symbol))
        .collect()
}

fn parse_binance_trades_message(payload: &str, symbol: &str) -> Vec<CcxtTrade> {
    parse_binance_trades_shared(payload)
        .into_iter()
        .map(|trade| ws_trade_to_ccxt(trade, symbol))
        .collect()
}

fn parse_bybit_trades_message(payload: &str, symbol: &str) -> Vec<CcxtTrade> {
    parse_bybit_trades_shared(payload)
        .into_iter()
        .map(|trade| ws_trade_to_ccxt(trade, symbol))
        .collect()
}

fn ws_trade_to_ccxt(trade: WsTrade, symbol: &str) -> CcxtTrade {
    CcxtTrade {
        info: trade.info,
        amount: trade.amount,
        datetime: trade.timestamp.and_then(iso8601_millis),
        id: trade.id,
        order: None,
        price: trade.price,
        timestamp: trade.timestamp,
        trade_type: None,
        side: trade.side,
        symbol: Some(symbol.to_string()),
        taker_or_maker: None,
        cost: trade.cost,
        fee: None,
    }
}

fn normalize_topic_params(params: Value) -> Result<Value, String> {
    if params.is_null() {
        return Ok(Value::Object(Map::new()));
    }

    if !params.is_object() {
        return Err("`params` must be an object or null".to_string());
    }

    Ok(canonicalize_json(&params))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries = map
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut out = Map::new();
            for (key, value) in entries {
                out.insert(key, value);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
}

fn extract_non_empty_string(params: &Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

#[derive(Clone, Copy)]
enum BybitCategory {
    Spot,
    Linear,
    Inverse,
    Option,
}

impl BybitCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
            Self::Option => "option",
        }
    }
}

fn parse_bybit_category(params: &Value) -> Result<BybitCategory, String> {
    let Some(raw) = params.get("category").and_then(Value::as_str) else {
        return Ok(BybitCategory::Linear);
    };

    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(BybitCategory::Linear);
    }

    match normalized.as_str() {
        "spot" => Ok(BybitCategory::Spot),
        "linear" => Ok(BybitCategory::Linear),
        "inverse" => Ok(BybitCategory::Inverse),
        "option" | "options" => Ok(BybitCategory::Option),
        other => Err(format!("unsupported Bybit websocket category `{other}`")),
    }
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn next_reconnect_delay(current: Duration) -> Duration {
    let doubled = current.as_millis().saturating_mul(2);
    let bounded = doubled.min(MAX_RECONNECT_DELAY_MS as u128) as u64;
    Duration::from_millis(bounded)
}

fn to_hyperliquid_ws_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://") {
        return format!("wss://{rest}/ws");
    }
    if let Some(rest) = trimmed.strip_prefix("http://") {
        return format!("ws://{rest}/ws");
    }
    format!("wss://{trimmed}/ws")
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use super::*;
    use tokio::time::timeout;

    struct MockRunner {
        run_count: Arc<AtomicUsize>,
        stop_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TradesUpstreamRunner for MockRunner {
        fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
            let key = format!(
                "{}|{}",
                topic.exchange,
                topic.symbol.trim().to_ascii_lowercase()
            );
            Ok(ResolvedTradesTopic {
                key,
                topic,
                stream: UpstreamTradesStream::Binance {
                    ws_endpoint: "wss://example.invalid/ws".to_string(),
                },
            })
        }

        async fn run(
            &self,
            _topic: ResolvedTradesTopic,
            _sender: broadcast::Sender<Arc<Vec<CcxtTrade>>>,
            mut shutdown: watch::Receiver<bool>,
        ) {
            self.run_count.fetch_add(1, Ordering::SeqCst);
            let _ = shutdown.changed().await;
            self.stop_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn manager_uses_single_runner_for_shared_topic() {
        let run_count = Arc::new(AtomicUsize::new(0));
        let stop_count = Arc::new(AtomicUsize::new(0));

        let manager = TradesTopicManager::with_runner(Arc::new(MockRunner {
            run_count: run_count.clone(),
            stop_count: stop_count.clone(),
        }));

        let topic_a = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTCUSDT".to_string()),
            Value::Null,
        )
        .expect("topic should be valid");

        let topic_b = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("btcusdt".to_string()),
            Value::Null,
        )
        .expect("topic should be valid");

        let subscription_a = manager
            .subscribe(topic_a)
            .await
            .expect("first subscribe should succeed");
        let subscription_b = manager
            .subscribe(topic_b)
            .await
            .expect("second subscribe should succeed");

        assert_eq!(subscription_a.key, subscription_b.key);

        timeout(Duration::from_millis(200), async {
            while run_count.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runner should start");

        assert_eq!(run_count.load(Ordering::SeqCst), 1);
        assert_eq!(manager.active_topic_count().await, 1);

        manager.unsubscribe_by_key(&subscription_a.key).await;
        assert_eq!(stop_count.load(Ordering::SeqCst), 0);
        assert_eq!(manager.active_topic_count().await, 1);

        manager.unsubscribe_by_key(&subscription_b.key).await;

        timeout(Duration::from_millis(200), async {
            while stop_count.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runner should stop after last unsubscribe");

        assert_eq!(stop_count.load(Ordering::SeqCst), 1);
        assert_eq!(manager.active_topic_count().await, 0);
    }

    #[test]
    fn normalizes_topic_params_and_validates_shape() {
        let topic = TradesTopic::from_client_request(
            None,
            Some("BTC/USDT:USDT".to_string()),
            json!({"b": 2, "a": {"z": 1, "x": 0}}),
        )
        .expect("topic should be valid");

        assert_eq!(topic.exchange, "hyperliquid");
        assert_eq!(topic.params, json!({"a": {"x": 0, "z": 1}, "b": 2}));

        let invalid = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTCUSDT".to_string()),
            json!([1, 2, 3]),
        );
        assert!(invalid.is_err());
    }
}
