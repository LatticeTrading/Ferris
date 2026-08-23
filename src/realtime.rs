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
    binance_orderbook::{
        parse_binance_depth_event, parse_binance_partial_orderbook,
        resolve_binance_orderbook_stream, BinanceEventOutcome, BinanceOrderBookSnapshotProvider,
        BinanceOrderBookStreamKind, BinanceOrderBookSync, BinanceSnapshotOutcome,
        BINANCE_FUTURES_WS_BASE_URL, BINANCE_MAX_ORDERBOOK_LEVELS,
    },
    bybit_full_orderbook::{
        fetch_bybit_full_snapshot, parse_bybit_full_delta, BufferDeltaOutcome, BybitFullBookSync,
        SnapshotSyncOutcome, BYBIT_FULL_DEPTH_THRESHOLD, BYBIT_MAX_FULL_DEPTH, BYBIT_REST_BASE_URL,
    },
    exchanges::lighterxyz::LighterMarketCatalogService,
    models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade},
    ws_shared::{
        infer_hyperliquid_coin_from_symbol as infer_hyperliquid_coin_from_symbol_shared,
        parse_binance_trades as parse_binance_trades_shared,
        parse_bybit_trades as parse_bybit_trades_shared, parse_f64_lossy as parse_f64_lossy_shared,
        parse_hyperliquid_trades as parse_hyperliquid_trades_shared,
        parse_lighter_trades as parse_lighter_trades_shared,
        parse_u64_lossy as parse_u64_lossy_shared,
        resolve_binance_ws_symbol as resolve_binance_ws_symbol_shared,
        resolve_bybit_ws_symbol as resolve_bybit_ws_symbol_shared,
        resolve_public_symbol as resolve_public_symbol_shared, WsTrade, BINANCE_QUOTES,
        BYBIT_QUOTES,
    },
};

const DEFAULT_EXCHANGE: &str = "hyperliquid";

const BYBIT_WS_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_WS_LINEAR_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_WS_INVERSE_URL: &str = "wss://stream.bybit.com/v5/public/inverse";
const BYBIT_WS_OPTION_URL: &str = "wss://stream.bybit.com/v5/public/option";

const TOPIC_BROADCAST_CAPACITY: usize = 512;
const INITIAL_RECONNECT_DELAY_MS: u64 = 500;
const MAX_RECONNECT_DELAY_MS: u64 = 15_000;
const DEFAULT_OHLCV_TIMEFRAME: &str = "1m";
const BINANCE_WS_MAX_ORDERBOOK_LEVELS: usize = 1_000;
const BYBIT_WS_DEFAULT_ORDERBOOK_LEVELS: usize = 1_000;
const BYBIT_WS_MAX_ORDERBOOK_LEVELS: usize = 1_000;
const BYBIT_WS_MAX_OPTION_ORDERBOOK_LEVELS: usize = 100;
const HYPERLIQUID_WS_MAX_ORDERBOOK_LEVELS: usize = 20;
const DEFAULT_ORDERBOOK_LEVELS: usize = 20;

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
    pub fn new(
        hyperliquid_base_url: String,
        lighter_ws_endpoint: String,
        lighter_catalog_service: Arc<LighterMarketCatalogService>,
    ) -> Self {
        let runner = Arc::new(RealTradesUpstreamRunner::new(
            hyperliquid_base_url,
            lighter_ws_endpoint,
            lighter_catalog_service,
        ));
        Self {
            inner: Arc::new(TradesTopicManagerInner {
                topics: RwLock::new(HashMap::new()),
                runner,
                broadcast_capacity: TOPIC_BROADCAST_CAPACITY,
            }),
        }
    }

    pub async fn resolve_topic_key(&self, topic: TradesTopic) -> Result<String, String> {
        self.inner
            .runner
            .resolve(topic)
            .await
            .map(|resolved| resolved.key)
    }

    pub async fn subscribe(&self, topic: TradesTopic) -> Result<TradesSubscription, String> {
        let resolved = self.inner.runner.resolve(topic).await?;

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
    Lighter {
        ws_endpoint: String,
        market_id: u64,
    },
}

#[async_trait]
trait TradesUpstreamRunner: Send + Sync {
    async fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String>;

    async fn run(
        &self,
        topic: ResolvedTradesTopic,
        sender: broadcast::Sender<Arc<Vec<CcxtTrade>>>,
        shutdown: watch::Receiver<bool>,
    );
}

struct RealTradesUpstreamRunner {
    hyperliquid_ws_endpoint: String,
    lighter_ws_endpoint: String,
    lighter_catalog_service: Arc<LighterMarketCatalogService>,
}

impl RealTradesUpstreamRunner {
    fn new(
        hyperliquid_base_url: String,
        lighter_ws_endpoint: String,
        lighter_catalog_service: Arc<LighterMarketCatalogService>,
    ) -> Self {
        Self {
            hyperliquid_ws_endpoint: to_hyperliquid_ws_url(&hyperliquid_base_url),
            lighter_ws_endpoint,
            lighter_catalog_service,
        }
    }
}

#[async_trait]
impl TradesUpstreamRunner for RealTradesUpstreamRunner {
    async fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
        match topic.exchange.as_str() {
            "hyperliquid" => resolve_hyperliquid_topic(topic, &self.hyperliquid_ws_endpoint),
            "binance" => resolve_binance_topic(topic),
            "bybit" => resolve_bybit_topic(topic),
            "lighterxyz" => {
                resolve_lighter_trades_topic(
                    topic,
                    &self.lighter_ws_endpoint,
                    self.lighter_catalog_service.clone(),
                )
                .await
            }
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
        | UpstreamTradesStream::Bybit { ws_endpoint, .. }
        | UpstreamTradesStream::Lighter { ws_endpoint, .. } => ws_endpoint.as_str(),
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
        UpstreamTradesStream::Lighter { market_id, .. } => {
            if let Err(err) = send_lighter_subscription(&mut stream, "trade", *market_id).await {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Lighter trade subscription");
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
        UpstreamTradesStream::Lighter { .. } => parse_lighter_trades_message(payload, symbol),
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

async fn resolve_lighter_trades_topic(
    topic: TradesTopic,
    lighter_ws_endpoint: &str,
    lighter_catalog_service: Arc<LighterMarketCatalogService>,
) -> Result<ResolvedTradesTopic, String> {
    let market = lighter_catalog_service
        .resolve_market(&topic.symbol, &topic.params)
        .await
        .map_err(|err| err.to_string())?;

    Ok(ResolvedTradesTopic {
        key: format!("lighterxyz|trades|market:{}", market.market_id),
        topic: TradesTopic {
            exchange: topic.exchange,
            symbol: market.symbol,
            params: topic.params,
        },
        stream: UpstreamTradesStream::Lighter {
            ws_endpoint: lighter_ws_endpoint.to_string(),
            market_id: market.market_id,
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
    send_bybit_subscription(stream, &[topic], "trade").await
}

async fn send_lighter_subscription(
    stream: &mut WsStream,
    channel_prefix: &str,
    market_id: u64,
) -> Result<(), String> {
    let payload = json!({
        "type": "subscribe",
        "channel": format!("{channel_prefix}/{market_id}"),
    });

    stream
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .map_err(|err| format!("failed to send Lighter websocket subscription: {err}"))
}

async fn send_bybit_subscription(
    stream: &mut WsStream,
    topics: &[&str],
    channel_label: &str,
) -> Result<(), String> {
    let payload = json!({
        "op": "subscribe",
        "args": topics,
    });

    stream
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .map_err(|err| {
            format!("failed to send Bybit {channel_label} websocket subscription: {err}")
        })
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

fn parse_lighter_trades_message(payload: &str, symbol: &str) -> Vec<CcxtTrade> {
    parse_lighter_trades_shared(payload)
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

pub type OrderBookTopic = TradesTopic;
pub type OhlcvTopic = TradesTopic;

pub struct OrderBookSubscription {
    pub key: String,
    pub topic: OrderBookTopic,
    pub levels_limit: usize,
    pub receiver: broadcast::Receiver<Arc<CcxtOrderBook>>,
}

#[derive(Clone)]
pub struct OrderBookTopicManager {
    inner: Arc<OrderBookTopicManagerInner>,
}

struct OrderBookTopicManagerInner {
    topics: RwLock<HashMap<String, ActiveOrderBookTopic>>,
    runner: Arc<dyn OrderBookUpstreamRunner>,
    broadcast_capacity: usize,
}
struct ActiveOrderBookTopic {
    sender: broadcast::Sender<Arc<CcxtOrderBook>>,
    subscribers: usize,
    shutdown_sender: watch::Sender<bool>,
}

impl OrderBookTopicManager {
    pub fn new(
        hyperliquid_base_url: String,
        binance_snapshot_provider: Arc<dyn BinanceOrderBookSnapshotProvider>,
        lighter_ws_endpoint: String,
        lighter_catalog_service: Arc<LighterMarketCatalogService>,
    ) -> Self {
        let runner = Arc::new(RealOrderBookUpstreamRunner::new(
            hyperliquid_base_url,
            binance_snapshot_provider,
            lighter_ws_endpoint,
            lighter_catalog_service,
        ));
        Self {
            inner: Arc::new(OrderBookTopicManagerInner {
                topics: RwLock::new(HashMap::new()),
                runner,
                broadcast_capacity: TOPIC_BROADCAST_CAPACITY,
            }),
        }
    }

    pub async fn resolve_topic_key(&self, topic: OrderBookTopic) -> Result<String, String> {
        self.inner
            .runner
            .resolve(topic)
            .await
            .map(|resolved| resolved.key)
    }

    pub async fn subscribe(&self, topic: OrderBookTopic) -> Result<OrderBookSubscription, String> {
        let resolved = self.inner.runner.resolve(topic).await?;

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
                    ActiveOrderBookTopic {
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

        Ok(OrderBookSubscription {
            key: resolved.key,
            topic: resolved.topic,
            levels_limit: resolved.display_levels_limit,
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
}

#[derive(Clone)]
struct ResolvedOrderBookTopic {
    key: String,
    topic: OrderBookTopic,
    stream: UpstreamOrderBookStream,
    levels_limit: usize,
    display_levels_limit: usize,
}

#[derive(Clone)]
enum UpstreamOrderBookStream {
    Hyperliquid {
        ws_endpoint: String,
        coin: String,
        n_sig_figs: Option<u64>,
        mantissa: Option<u64>,
    },
    BinancePartial {
        ws_endpoint: String,
    },
    BinanceDiff {
        ws_endpoint: String,
        market_symbol: String,
    },
    BybitLimited {
        ws_endpoint: String,
        subscribe_topic: String,
    },
    BybitFull {
        ws_endpoint: String,
        subscribe_topic: String,
        category: String,
        market_symbol: String,
    },
    Lighter {
        ws_endpoint: String,
        market_id: u64,
    },
}

#[async_trait]
trait OrderBookUpstreamRunner: Send + Sync {
    async fn resolve(&self, topic: OrderBookTopic) -> Result<ResolvedOrderBookTopic, String>;

    async fn run(
        &self,
        topic: ResolvedOrderBookTopic,
        sender: broadcast::Sender<Arc<CcxtOrderBook>>,
        shutdown: watch::Receiver<bool>,
    );
}

struct RealOrderBookUpstreamRunner {
    hyperliquid_ws_endpoint: String,
    bybit_http_client: reqwest::Client,
    binance_snapshot_provider: Arc<dyn BinanceOrderBookSnapshotProvider>,
    lighter_ws_endpoint: String,
    lighter_catalog_service: Arc<LighterMarketCatalogService>,
}

impl RealOrderBookUpstreamRunner {
    fn new(
        hyperliquid_base_url: String,
        binance_snapshot_provider: Arc<dyn BinanceOrderBookSnapshotProvider>,
        lighter_ws_endpoint: String,
        lighter_catalog_service: Arc<LighterMarketCatalogService>,
    ) -> Self {
        Self {
            hyperliquid_ws_endpoint: to_hyperliquid_ws_url(&hyperliquid_base_url),
            bybit_http_client: reqwest::Client::new(),
            binance_snapshot_provider,
            lighter_ws_endpoint,
            lighter_catalog_service,
        }
    }
}

#[async_trait]
impl OrderBookUpstreamRunner for RealOrderBookUpstreamRunner {
    async fn resolve(&self, topic: OrderBookTopic) -> Result<ResolvedOrderBookTopic, String> {
        match topic.exchange.as_str() {
            "hyperliquid" => {
                resolve_hyperliquid_orderbook_topic(topic, &self.hyperliquid_ws_endpoint)
            }
            "binance" => resolve_binance_orderbook_topic(topic),
            "bybit" => resolve_bybit_orderbook_topic(topic),
            "lighterxyz" => {
                resolve_lighter_orderbook_topic(
                    topic,
                    &self.lighter_ws_endpoint,
                    self.lighter_catalog_service.clone(),
                )
                .await
            }
            other => Err(format!(
                "exchange `{other}` is not supported for realtime orderbook"
            )),
        }
    }

    async fn run(
        &self,
        topic: ResolvedOrderBookTopic,
        sender: broadcast::Sender<Arc<CcxtOrderBook>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

        loop {
            if *shutdown.borrow() {
                return;
            }
            let connection_outcome = run_single_orderbook_connection(
                &topic,
                &sender,
                &mut shutdown,
                &self.bybit_http_client,
                self.binance_snapshot_provider.clone(),
            )
            .await;

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

async fn run_single_orderbook_connection(
    topic: &ResolvedOrderBookTopic,
    sender: &broadcast::Sender<Arc<CcxtOrderBook>>,
    shutdown: &mut watch::Receiver<bool>,
    bybit_http_client: &reqwest::Client,
    binance_snapshot_provider: Arc<dyn BinanceOrderBookSnapshotProvider>,
) -> ConnectionOutcome {
    let ws_endpoint = match &topic.stream {
        UpstreamOrderBookStream::Hyperliquid { ws_endpoint, .. }
        | UpstreamOrderBookStream::BinancePartial { ws_endpoint }
        | UpstreamOrderBookStream::BinanceDiff { ws_endpoint, .. }
        | UpstreamOrderBookStream::BybitLimited { ws_endpoint, .. }
        | UpstreamOrderBookStream::BybitFull { ws_endpoint, .. }
        | UpstreamOrderBookStream::Lighter { ws_endpoint, .. } => ws_endpoint.as_str(),
    };
    let Ok((mut stream, _response)) = connect_async(ws_endpoint).await else {
        tracing::warn!(
            topic_key = %topic.key,
            ws_endpoint = %ws_endpoint,
            "failed to connect upstream orderbook websocket"
        );
        return ConnectionOutcome::Reconnect {
            reset_backoff: false,
        };
    };

    tracing::info!(topic_key = %topic.key, "connected upstream orderbook websocket");

    match &topic.stream {
        UpstreamOrderBookStream::Hyperliquid {
            coin,
            n_sig_figs,
            mantissa,
            ..
        } => {
            if let Err(err) =
                send_hyperliquid_orderbook_subscription(&mut stream, coin, *n_sig_figs, *mantissa)
                    .await
            {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Hyperliquid orderbook subscription");
                return ConnectionOutcome::Reconnect {
                    reset_backoff: false,
                };
            }
        }
        UpstreamOrderBookStream::BybitLimited {
            subscribe_topic, ..
        }
        | UpstreamOrderBookStream::BybitFull {
            subscribe_topic, ..
        } => {
            if let Err(err) =
                send_bybit_subscription(&mut stream, &[subscribe_topic], "orderbook").await
            {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Bybit orderbook subscription");
                return ConnectionOutcome::Reconnect {
                    reset_backoff: false,
                };
            }
        }
        UpstreamOrderBookStream::BinancePartial { .. } => {}
        UpstreamOrderBookStream::BinanceDiff { market_symbol, .. } => {
            return run_binance_diff_orderbook_connection(
                stream,
                topic,
                sender,
                shutdown,
                binance_snapshot_provider,
                market_symbol,
            )
            .await;
        }
        UpstreamOrderBookStream::Lighter { market_id, .. } => {
            if let Err(err) = send_lighter_subscription(&mut stream, "order_book", *market_id).await
            {
                tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Lighter orderbook subscription");
                return ConnectionOutcome::Reconnect {
                    reset_backoff: false,
                };
            }
        }
    }

    if let UpstreamOrderBookStream::BybitFull {
        category,
        market_symbol,
        ..
    } = &topic.stream
    {
        return run_bybit_full_orderbook_connection(
            stream,
            topic,
            sender,
            shutdown,
            bybit_http_client,
            category,
            market_symbol,
        )
        .await;
    }

    let mut bybit_state: Option<CcxtOrderBook> = None;
    let mut lighter_state: Option<LighterOrderBookState> = None;

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
                        match parse_orderbook_update(
                            &topic.stream,
                            text.as_ref(),
                            &topic.topic.symbol,
                            topic.levels_limit,
                            &mut bybit_state,
                            &mut lighter_state,
                        ) {
                            Ok(Some(book)) => {
                                let _ = sender.send(Arc::new(book));
                            }
                            Ok(None) => {}
                            Err(err) => {
                                tracing::warn!(topic_key = %topic.key, error = %err, "failed to process orderbook update");
                                return ConnectionOutcome::Reconnect { reset_backoff: true };
                            }
                        }
                    }
                    Some(Ok(TungsteniteMessage::Binary(binary))) => {
                        if let Ok(text) = String::from_utf8(binary.to_vec()) {
                            match parse_orderbook_update(
                                &topic.stream,
                                &text,
                                &topic.topic.symbol,
                                topic.levels_limit,
                                &mut bybit_state,
                                &mut lighter_state,
                            ) {
                                Ok(Some(book)) => {
                                    let _ = sender.send(Arc::new(book));
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    tracing::warn!(topic_key = %topic.key, error = %err, "failed to process orderbook update");
                                    return ConnectionOutcome::Reconnect { reset_backoff: true };
                                }
                            }
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
                        tracing::warn!(topic_key = %topic.key, error = %err, "upstream orderbook websocket error");
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

async fn run_binance_diff_orderbook_connection(
    mut stream: WsStream,
    topic: &ResolvedOrderBookTopic,
    sender: &broadcast::Sender<Arc<CcxtOrderBook>>,
    shutdown: &mut watch::Receiver<bool>,
    snapshot_provider: Arc<dyn BinanceOrderBookSnapshotProvider>,
    market_symbol: &str,
) -> ConnectionOutcome {
    use std::{future::Future, pin::Pin};

    type SnapshotFuture = Pin<
        Box<
            dyn Future<Output = Result<crate::binance_orderbook::BinanceDepthSnapshot, String>>
                + Send,
        >,
    >;

    let mut sync = BinanceOrderBookSync::new();
    let mut snapshot_request: Option<SnapshotFuture> = Some({
        let provider = snapshot_provider.clone();
        let market_symbol = market_symbol.to_string();
        Box::pin(async move {
            provider
                .fetch_binance_order_book_snapshot(&market_symbol)
                .await
        })
    });

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    let _ = stream.close(None).await;
                    return ConnectionOutcome::Stop;
                }
            }
            snapshot = async {
                match snapshot_request.as_mut() {
                    Some(request) => Some(request.await),
                    None => std::future::pending::<Option<Result<crate::binance_orderbook::BinanceDepthSnapshot, String>>>().await,
                }
            } => {
                snapshot_request = None;
                let Some(result) = snapshot else { continue; };
                match result {
                    Ok(snapshot) => match sync.apply_snapshot(snapshot) {
                        BinanceSnapshotOutcome::Synchronized => {
                            if let Some(book) = sync.to_ccxt(&topic.topic.symbol, topic.levels_limit) {
                                let _ = sender.send(Arc::new(book));
                            }
                        }
                        BinanceSnapshotOutcome::NeedMoreEvents => {}
                        BinanceSnapshotOutcome::RefetchSnapshot => {
                            let provider = snapshot_provider.clone();
                            let market_symbol = market_symbol.to_string();
                            snapshot_request = Some(Box::pin(async move {
                                provider
                                    .fetch_binance_order_book_snapshot(&market_symbol)
                                    .await
                            }));
                        }
                    },
                    Err(err) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "failed to fetch Binance orderbook snapshot");
                        return ConnectionOutcome::Reconnect { reset_backoff: false };
                    }
                }
            }
            message = stream.next() => {
                let payload = match message {
                    Some(Ok(TungsteniteMessage::Text(text))) => Some(text.to_string()),
                    Some(Ok(TungsteniteMessage::Binary(binary))) => String::from_utf8(binary.to_vec()).ok(),
                    Some(Ok(TungsteniteMessage::Ping(payload))) => {
                        if stream.send(TungsteniteMessage::Pong(payload)).await.is_err() {
                            return ConnectionOutcome::Reconnect { reset_backoff: true };
                        }
                        None
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => {
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    Some(Err(err)) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "upstream Binance orderbook websocket error");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    _ => None,
                };
                let Some(payload) = payload else { continue; };
                let event = match parse_binance_depth_event(&payload) {
                    Ok(Some(event)) => event,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "invalid Binance depth event");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                };
                match sync.push_event(event) {
                    BinanceEventOutcome::Updated => {
                        if let Some(book) = sync.to_ccxt(&topic.topic.symbol, topic.levels_limit) {
                            let _ = sender.send(Arc::new(book));
                        }
                    }
                    BinanceEventOutcome::Buffered => {
                        match sync.try_pending_snapshot() {
                            BinanceSnapshotOutcome::Synchronized => {
                                if let Some(book) = sync.to_ccxt(&topic.topic.symbol, topic.levels_limit) {
                                    let _ = sender.send(Arc::new(book));
                                }
                            }
                            BinanceSnapshotOutcome::RefetchSnapshot if snapshot_request.is_none() => {
                                let provider = snapshot_provider.clone();
                                let market_symbol = market_symbol.to_string();
                                snapshot_request = Some(Box::pin(async move {
                                    provider
                                        .fetch_binance_order_book_snapshot(&market_symbol)
                                        .await
                                }));
                            }
                            BinanceSnapshotOutcome::NeedMoreEvents
                            | BinanceSnapshotOutcome::RefetchSnapshot => {}
                        }
                    }
                    BinanceEventOutcome::NeedsSnapshot if snapshot_request.is_none() => {
                        let provider = snapshot_provider.clone();
                        let market_symbol = market_symbol.to_string();
                        snapshot_request = Some(Box::pin(async move {
                            provider
                                .fetch_binance_order_book_snapshot(&market_symbol)
                                .await
                        }));
                    }
                    BinanceEventOutcome::NeedsSnapshot | BinanceEventOutcome::Ignored => {}
                }
            }
        }
    }
}

async fn run_bybit_full_orderbook_connection(
    mut stream: WsStream,
    topic: &ResolvedOrderBookTopic,
    sender: &broadcast::Sender<Arc<CcxtOrderBook>>,
    shutdown: &mut watch::Receiver<bool>,
    http_client: &reqwest::Client,
    category: &str,
    market_symbol: &str,
) -> ConnectionOutcome {
    let mut sync = BybitFullBookSync::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    let _ = stream.close(None).await;
                    return ConnectionOutcome::Stop;
                }
            }
            message = stream.next() => {
                let payload = match message {
                    Some(Ok(TungsteniteMessage::Text(text))) => Some(text.to_string()),
                    Some(Ok(TungsteniteMessage::Binary(binary))) => String::from_utf8(binary.to_vec()).ok(),
                    Some(Ok(TungsteniteMessage::Ping(payload))) => {
                        if stream.send(TungsteniteMessage::Pong(payload)).await.is_err() {
                            return ConnectionOutcome::Reconnect { reset_backoff: true };
                        }
                        None
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => {
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    Some(Err(err)) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "upstream Bybit full orderbook websocket error");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    _ => None,
                };

                let Some(payload) = payload else {
                    continue;
                };
                let delta = match parse_bybit_full_delta(&payload) {
                    Ok(Some(delta)) => delta,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(topic_key = %topic.key, error = %err, "invalid Bybit full orderbook delta");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                };

                match sync.push_delta(delta) {
                    BufferDeltaOutcome::Restart => {
                        tracing::warn!(topic_key = %topic.key, "Bybit full orderbook update sequence requires resync");
                        return ConnectionOutcome::Reconnect { reset_backoff: true };
                    }
                    BufferDeltaOutcome::Updated => {
                        if let Some(book) = sync.to_ccxt(&topic.topic.symbol, topic.levels_limit) {
                            let _ = sender.send(Arc::new(book));
                        }
                    }
                    BufferDeltaOutcome::Ignored => {}
                    BufferDeltaOutcome::Buffered => {
                        let mut sync_outcome = sync.try_pending_snapshot();
                        while matches!(sync_outcome, SnapshotSyncOutcome::RefetchSnapshot)
                            || sync.needs_snapshot()
                        {
                            let snapshot = match fetch_bybit_full_snapshot(
                                http_client,
                                BYBIT_REST_BASE_URL,
                                category,
                                market_symbol,
                            )
                            .await
                            {
                                Ok(snapshot) => snapshot,
                                Err(err) => {
                                    tracing::warn!(topic_key = %topic.key, error = %err, "failed to fetch Bybit full orderbook snapshot");
                                    return ConnectionOutcome::Reconnect { reset_backoff: false };
                                }
                            };
                            sync_outcome = sync.apply_snapshot(snapshot);
                        }

                        if matches!(sync_outcome, SnapshotSyncOutcome::Synchronized) {
                            if let Some(book) = sync.to_ccxt(&topic.topic.symbol, topic.levels_limit) {
                                let _ = sender.send(Arc::new(book));
                            }
                        }
                    }
                }
            }
        }
    }
}
fn parse_orderbook_update(
    stream: &UpstreamOrderBookStream,
    payload: &str,
    symbol: &str,
    levels_limit: usize,
    bybit_state: &mut Option<CcxtOrderBook>,
    lighter_state: &mut Option<LighterOrderBookState>,
) -> Result<Option<CcxtOrderBook>, String> {
    match stream {
        UpstreamOrderBookStream::Hyperliquid { .. } => Ok(parse_hyperliquid_orderbook_message(
            payload,
            symbol,
            levels_limit,
        )),
        UpstreamOrderBookStream::BinancePartial { .. } => Ok(parse_binance_partial_orderbook(
            payload,
            symbol,
            levels_limit,
        )),
        UpstreamOrderBookStream::BinanceDiff { .. } => Ok(None),
        UpstreamOrderBookStream::BybitLimited { .. } => {
            let Some(event) = parse_bybit_orderbook_message(payload) else {
                return Ok(None);
            };
            let updated = apply_bybit_orderbook_event(bybit_state, event, levels_limit, symbol);
            Ok(updated.then(|| bybit_state.clone()).flatten())
        }
        UpstreamOrderBookStream::Lighter { .. } => {
            let Some(event) = parse_lighter_orderbook_message(payload) else {
                return Ok(None);
            };
            apply_lighter_orderbook_event(lighter_state, event, levels_limit, symbol)
        }
        UpstreamOrderBookStream::BybitFull { .. } => Ok(None),
    }
}

fn resolve_hyperliquid_orderbook_topic(
    topic: OrderBookTopic,
    hyperliquid_ws_endpoint: &str,
) -> Result<ResolvedOrderBookTopic, String> {
    let coin = extract_non_empty_string(&topic.params, "coin")
        .or_else(|| infer_hyperliquid_coin_from_symbol_shared(&topic.symbol))
        .ok_or_else(|| {
            format!(
                "unable to infer Hyperliquid coin from symbol `{}`; pass `params.coin`",
                topic.symbol
            )
        })?;
    let levels_limit = HYPERLIQUID_WS_MAX_ORDERBOOK_LEVELS;
    let n_sig_figs = extract_u64_param(&topic.params, "nSigFigs")
        .or_else(|| extract_u64_param(&topic.params, "n_sig_figs"));
    let mantissa = extract_u64_param(&topic.params, "mantissa");
    let n_sig_figs_key = n_sig_figs
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string());
    let mantissa_key = mantissa
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string());

    Ok(ResolvedOrderBookTopic {
        key: format!(
            "hyperliquid|orderbook|coin:{}|levels:{}|nSigFigs:{}|mantissa:{}",
            coin.to_ascii_lowercase(),
            levels_limit,
            n_sig_figs_key,
            mantissa_key,
        ),
        topic,
        stream: UpstreamOrderBookStream::Hyperliquid {
            ws_endpoint: hyperliquid_ws_endpoint.to_string(),
            coin,
            n_sig_figs,
            mantissa,
        },
        levels_limit,
        display_levels_limit: levels_limit,
    })
}

fn resolve_binance_orderbook_topic(
    topic: OrderBookTopic,
) -> Result<ResolvedOrderBookTopic, String> {
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_binance_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let stream_symbol = market_symbol.to_ascii_lowercase();
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BINANCE_QUOTES);
    let requested_levels = resolve_orderbook_levels(&topic.params);
    let resolved = resolve_binance_orderbook_stream(
        BINANCE_FUTURES_WS_BASE_URL,
        &market_symbol,
        requested_levels,
    );
    let (key, stream, levels_limit) = match resolved.kind {
        BinanceOrderBookStreamKind::Partial { depth } => (
            format!("binance|orderbook|symbol:{stream_symbol}|depth:{depth}"),
            UpstreamOrderBookStream::BinancePartial {
                ws_endpoint: resolved.ws_endpoint,
            },
            depth,
        ),
        BinanceOrderBookStreamKind::Diff => (
            format!("binance|orderbook|symbol:{stream_symbol}|depth:diff1000"),
            UpstreamOrderBookStream::BinanceDiff {
                ws_endpoint: resolved.ws_endpoint,
                market_symbol: market_symbol.clone(),
            },
            BINANCE_MAX_ORDERBOOK_LEVELS,
        ),
    };

    Ok(ResolvedOrderBookTopic {
        key,
        topic: OrderBookTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream,
        levels_limit,
        display_levels_limit: resolved.display_levels,
    })
}

fn resolve_bybit_orderbook_topic(topic: OrderBookTopic) -> Result<ResolvedOrderBookTopic, String> {
    let category = parse_bybit_category(&topic.params)?;
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_bybit_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BYBIT_QUOTES);
    let display_levels_limit = resolve_bybit_orderbook_levels(&topic.params, category);

    let ws_endpoint = match category {
        BybitCategory::Spot => BYBIT_WS_SPOT_URL,
        BybitCategory::Linear => BYBIT_WS_LINEAR_URL,
        BybitCategory::Inverse => BYBIT_WS_INVERSE_URL,
        BybitCategory::Option => BYBIT_WS_OPTION_URL,
    };
    let is_full = display_levels_limit > BYBIT_FULL_DEPTH_THRESHOLD
        && !matches!(category, BybitCategory::Option);
    let subscribe_topic = if is_full {
        format!("orderbook.full.{market_symbol}")
    } else {
        format!(
            "orderbook.{}.{market_symbol}",
            to_bybit_ws_depth_levels(display_levels_limit)
        )
    };
    let key = if is_full {
        format!(
            "bybit|orderbook|category:{}|symbol:{}|depth:full",
            category.as_str(),
            market_symbol.to_ascii_lowercase()
        )
    } else {
        format!(
            "bybit|orderbook|category:{}|symbol:{}|depth:{}",
            category.as_str(),
            market_symbol.to_ascii_lowercase(),
            to_bybit_ws_depth_levels(display_levels_limit)
        )
    };
    let stream = if is_full {
        UpstreamOrderBookStream::BybitFull {
            ws_endpoint: ws_endpoint.to_string(),
            subscribe_topic,
            category: category.as_str().to_string(),
            market_symbol: market_symbol.clone(),
        }
    } else {
        UpstreamOrderBookStream::BybitLimited {
            ws_endpoint: ws_endpoint.to_string(),
            subscribe_topic,
        }
    };

    Ok(ResolvedOrderBookTopic {
        key,
        topic: OrderBookTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream,
        levels_limit: if is_full {
            BYBIT_MAX_FULL_DEPTH
        } else {
            to_bybit_ws_depth_levels(display_levels_limit)
        },
        display_levels_limit,
    })
}

async fn resolve_lighter_orderbook_topic(
    topic: OrderBookTopic,
    lighter_ws_endpoint: &str,
    lighter_catalog_service: Arc<LighterMarketCatalogService>,
) -> Result<ResolvedOrderBookTopic, String> {
    let market = lighter_catalog_service
        .resolve_market(&topic.symbol, &topic.params)
        .await
        .map_err(|err| err.to_string())?;
    let levels_limit = extract_usize_param(&topic.params, "levels")
        .or_else(|| extract_usize_param(&topic.params, "depth"))
        .or_else(|| extract_usize_param(&topic.params, "limit"))
        .unwrap_or(usize::MAX)
        .max(1);
    let levels_key = if levels_limit == usize::MAX {
        "all".to_string()
    } else {
        levels_limit.to_string()
    };

    Ok(ResolvedOrderBookTopic {
        key: format!(
            "lighterxyz|orderbook|market:{}|levels:{}",
            market.market_id, levels_key
        ),
        topic: OrderBookTopic {
            exchange: topic.exchange,
            symbol: market.symbol,
            params: topic.params,
        },
        stream: UpstreamOrderBookStream::Lighter {
            ws_endpoint: lighter_ws_endpoint.to_string(),
            market_id: market.market_id,
        },
        levels_limit,
        display_levels_limit: levels_limit,
    })
}

async fn send_hyperliquid_orderbook_subscription(
    stream: &mut WsStream,
    coin: &str,
    n_sig_figs: Option<u64>,
    mantissa: Option<u64>,
) -> Result<(), String> {
    let mut subscription = Map::new();
    subscription.insert("type".to_string(), Value::String("l2Book".to_string()));
    subscription.insert("coin".to_string(), Value::String(coin.to_string()));

    if let Some(n_sig_figs) = n_sig_figs {
        subscription.insert("nSigFigs".to_string(), Value::from(n_sig_figs));
    }
    if let Some(mantissa) = mantissa {
        subscription.insert("mantissa".to_string(), Value::from(mantissa));
    }

    let payload = json!({
        "method": "subscribe",
        "subscription": subscription,
    });

    stream
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .map_err(|err| err.to_string())
}

fn parse_hyperliquid_orderbook_message(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<CcxtOrderBook> {
    let value = serde_json::from_str::<Value>(payload).ok()?;
    if value.get("channel").and_then(Value::as_str)? != "l2Book" {
        return None;
    }

    let data = value.get("data")?;
    let timestamp = data.get("time").and_then(parse_u64_lossy_shared);
    let datetime = timestamp.and_then(iso8601_millis);

    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }

    let bids = parse_hyperliquid_levels(levels.first()?, levels_limit, true);
    let asks = parse_hyperliquid_levels(levels.get(1)?, levels_limit, false);

    Some(CcxtOrderBook {
        asks,
        bids,
        datetime,
        timestamp,
        nonce: None,
        symbol: Some(symbol.to_string()),
    })
}

fn parse_hyperliquid_levels(levels: &Value, limit: usize, descending: bool) -> Vec<(f64, f64)> {
    let Some(levels) = levels.as_array() else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let Some(px) = level.get("px").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(sz) = level.get("sz").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        parsed.push((px, sz));
    }

    if descending {
        parsed.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        parsed.sort_by(|left, right| left.0.total_cmp(&right.0));
    }

    if parsed.len() > limit {
        parsed.truncate(limit);
    }

    parsed
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BybitOrderBookEventType {
    Snapshot,
    Delta,
}

#[derive(Debug)]
struct BybitOrderBookEvent {
    event_type: BybitOrderBookEventType,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    timestamp: Option<u64>,
}

fn parse_bybit_orderbook_message(payload: &str) -> Option<BybitOrderBookEvent> {
    let value = serde_json::from_str::<Value>(payload).ok()?;

    let topic = value.get("topic")?.as_str()?;
    if !topic.starts_with("orderbook.") {
        return None;
    }

    let event_type = match value.get("type").and_then(Value::as_str)? {
        "snapshot" => BybitOrderBookEventType::Snapshot,
        "delta" => BybitOrderBookEventType::Delta,
        _ => return None,
    };

    let data = value.get("data")?;
    let bids = parse_bybit_orderbook_side(data.get("b"));
    let asks = parse_bybit_orderbook_side(data.get("a"));

    let timestamp = value
        .get("ts")
        .and_then(parse_u64_lossy_shared)
        .or_else(|| data.get("cts").and_then(parse_u64_lossy_shared));

    Some(BybitOrderBookEvent {
        event_type,
        bids,
        asks,
        timestamp,
    })
}

fn parse_bybit_orderbook_side(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut output = Vec::with_capacity(levels.len());
    for row in levels {
        let Some(row) = row.as_array() else {
            continue;
        };
        if row.len() < 2 {
            continue;
        }

        let Some(price) = row.first().and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(size) = row.get(1).and_then(parse_f64_lossy_shared) else {
            continue;
        };

        output.push((price, size));
    }

    output
}

fn apply_bybit_orderbook_event(
    state: &mut Option<CcxtOrderBook>,
    event: BybitOrderBookEvent,
    levels_limit: usize,
    default_symbol: &str,
) -> bool {
    let symbol = default_symbol.to_string();
    let datetime = event.timestamp.and_then(iso8601_millis);

    match event.event_type {
        BybitOrderBookEventType::Snapshot => {
            let mut bids = event.bids;
            let mut asks = event.asks;

            normalize_bybit_book_side(&mut bids, true, levels_limit);
            normalize_bybit_book_side(&mut asks, false, levels_limit);

            *state = Some(CcxtOrderBook {
                asks,
                bids,
                datetime,
                timestamp: event.timestamp,
                nonce: None,
                symbol: Some(symbol),
            });
            true
        }
        BybitOrderBookEventType::Delta => {
            let Some(book) = state.as_mut() else {
                return false;
            };

            apply_bybit_level_updates(&mut book.bids, &event.bids, true);
            apply_bybit_level_updates(&mut book.asks, &event.asks, false);

            normalize_bybit_book_side(&mut book.bids, true, levels_limit);
            normalize_bybit_book_side(&mut book.asks, false, levels_limit);

            book.timestamp = event.timestamp;
            book.datetime = datetime;
            book.symbol = Some(symbol);
            true
        }
    }
}

fn apply_bybit_level_updates(
    levels: &mut Vec<(f64, f64)>,
    updates: &[(f64, f64)],
    descending: bool,
) {
    for (price, size) in updates {
        let existing_index = levels
            .iter()
            .position(|(existing_price, _)| bybit_price_eq(*existing_price, *price));

        if *size <= 0.0 {
            if let Some(index) = existing_index {
                levels.remove(index);
            }
            continue;
        }

        if let Some(index) = existing_index {
            levels[index].1 = *size;
        } else {
            levels.push((*price, *size));
        }
    }

    sort_bybit_book_side(levels, descending);
}

fn normalize_bybit_book_side(levels: &mut Vec<(f64, f64)>, descending: bool, levels_limit: usize) {
    levels.retain(|(_, size)| *size > 0.0);
    sort_bybit_book_side(levels, descending);
    if levels.len() > levels_limit {
        levels.truncate(levels_limit);
    }
}

fn sort_bybit_book_side(levels: &mut [(f64, f64)], descending: bool) {
    if descending {
        levels.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        levels.sort_by(|left, right| left.0.total_cmp(&right.0));
    }
}

fn bybit_price_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

#[derive(Debug, Clone)]
struct LighterOrderBookState {
    nonce: u64,
    timestamp: Option<u64>,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

#[derive(Debug)]
struct LighterOrderBookEvent {
    begin_nonce: Option<u64>,
    nonce: u64,
    timestamp: Option<u64>,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

fn parse_lighter_orderbook_message(payload: &str) -> Option<LighterOrderBookEvent> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return None;
    };

    if value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        != "update/order_book"
    {
        return None;
    }

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !channel.starts_with("order_book:") {
        return None;
    }

    let order_book = value.get("order_book")?;
    let code = order_book
        .get("code")
        .and_then(parse_u64_lossy_shared)
        .unwrap_or_default();
    if code != 0 {
        return None;
    }

    let nonce = order_book.get("nonce").and_then(parse_u64_lossy_shared)?;
    let begin_nonce = order_book
        .get("begin_nonce")
        .and_then(parse_u64_lossy_shared);
    let bids = parse_lighter_orderbook_side(order_book.get("bids"));
    let asks = parse_lighter_orderbook_side(order_book.get("asks"));
    let timestamp = value.get("timestamp").and_then(parse_u64_lossy_shared);

    Some(LighterOrderBookEvent {
        begin_nonce,
        nonce,
        timestamp,
        bids,
        asks,
    })
}

fn parse_lighter_orderbook_side(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(price) = row.get("price").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(size) = row.get("size").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        parsed.push((price, size));
    }

    parsed
}

fn apply_lighter_orderbook_event(
    state: &mut Option<LighterOrderBookState>,
    event: LighterOrderBookEvent,
    levels_limit: usize,
    symbol: &str,
) -> Result<Option<CcxtOrderBook>, String> {
    match state {
        Some(current) => {
            let begin_nonce = event
                .begin_nonce
                .ok_or_else(|| "lighterxyz orderbook delta missing begin_nonce".to_string())?;
            if begin_nonce != current.nonce {
                return Err(format!(
                    "lighterxyz orderbook nonce gap: expected begin_nonce {}, got {}",
                    current.nonce, begin_nonce
                ));
            }

            apply_lighter_level_updates(&mut current.bids, &event.bids, true);
            apply_lighter_level_updates(&mut current.asks, &event.asks, false);
            current.nonce = event.nonce;
            current.timestamp = event.timestamp.or(current.timestamp);

            Ok(Some(lighter_state_to_orderbook(
                current,
                symbol,
                levels_limit,
            )))
        }
        None => {
            let mut bids = event.bids;
            let mut asks = event.asks;
            normalize_lighter_book_side(&mut bids, true, usize::MAX);
            normalize_lighter_book_side(&mut asks, false, usize::MAX);

            let initial_state = LighterOrderBookState {
                nonce: event.nonce,
                timestamp: event.timestamp,
                bids,
                asks,
            };
            let book = lighter_state_to_orderbook(&initial_state, symbol, levels_limit);
            *state = Some(initial_state);
            Ok(Some(book))
        }
    }
}

fn apply_lighter_level_updates(
    levels: &mut Vec<(f64, f64)>,
    updates: &[(f64, f64)],
    descending: bool,
) {
    for (price, size) in updates {
        let existing_index = levels
            .iter()
            .position(|(existing_price, _)| lighter_price_eq(*existing_price, *price));

        if *size <= 0.0 {
            if let Some(index) = existing_index {
                levels.remove(index);
            }
            continue;
        }

        if let Some(index) = existing_index {
            levels[index].1 = *size;
        } else {
            levels.push((*price, *size));
        }
    }

    sort_lighter_book_side(levels, descending);
}

fn lighter_state_to_orderbook(
    state: &LighterOrderBookState,
    symbol: &str,
    levels_limit: usize,
) -> CcxtOrderBook {
    let mut bids = state.bids.clone();
    let mut asks = state.asks.clone();
    normalize_lighter_book_side(&mut bids, true, levels_limit);
    normalize_lighter_book_side(&mut asks, false, levels_limit);

    let timestamp = state.timestamp;
    let datetime = timestamp.and_then(iso8601_millis);

    CcxtOrderBook {
        asks,
        bids,
        datetime,
        timestamp,
        nonce: Some(state.nonce),
        symbol: Some(symbol.to_string()),
    }
}

fn normalize_lighter_book_side(
    levels: &mut Vec<(f64, f64)>,
    descending: bool,
    levels_limit: usize,
) {
    levels.retain(|(_, size)| *size > 0.0);
    sort_lighter_book_side(levels, descending);
    if levels_limit != usize::MAX && levels.len() > levels_limit {
        levels.truncate(levels_limit);
    }
}

fn sort_lighter_book_side(levels: &mut [(f64, f64)], descending: bool) {
    if descending {
        levels.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        levels.sort_by(|left, right| left.0.total_cmp(&right.0));
    }
}

fn lighter_price_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

pub struct OhlcvSubscription {
    pub key: String,
    pub topic: OhlcvTopic,
    pub receiver: broadcast::Receiver<Arc<Vec<CcxtOhlcv>>>,
}

#[derive(Clone)]
pub struct OhlcvTopicManager {
    inner: Arc<OhlcvTopicManagerInner>,
}

struct OhlcvTopicManagerInner {
    topics: RwLock<HashMap<String, ActiveOhlcvTopic>>,
    runner: Arc<dyn OhlcvUpstreamRunner>,
    broadcast_capacity: usize,
}

struct ActiveOhlcvTopic {
    sender: broadcast::Sender<Arc<Vec<CcxtOhlcv>>>,
    subscribers: usize,
    shutdown_sender: watch::Sender<bool>,
}

impl OhlcvTopicManager {
    pub fn new(hyperliquid_base_url: String) -> Self {
        let runner = Arc::new(RealOhlcvUpstreamRunner::new(hyperliquid_base_url));
        Self {
            inner: Arc::new(OhlcvTopicManagerInner {
                topics: RwLock::new(HashMap::new()),
                runner,
                broadcast_capacity: TOPIC_BROADCAST_CAPACITY,
            }),
        }
    }

    pub async fn resolve_topic_key(&self, topic: OhlcvTopic) -> Result<String, String> {
        self.inner
            .runner
            .resolve(topic)
            .await
            .map(|resolved| resolved.key)
    }

    pub async fn subscribe(&self, topic: OhlcvTopic) -> Result<OhlcvSubscription, String> {
        let resolved = self.inner.runner.resolve(topic).await?;

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
                    ActiveOhlcvTopic {
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

        Ok(OhlcvSubscription {
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
}

#[derive(Clone)]
struct ResolvedOhlcvTopic {
    key: String,
    topic: OhlcvTopic,
    stream: UpstreamOhlcvStream,
}

#[derive(Clone)]
enum UpstreamOhlcvStream {
    Binance {
        ws_endpoint: String,
    },
    Bybit {
        ws_endpoint: String,
        subscribe_topic: String,
    },
}

#[async_trait]
trait OhlcvUpstreamRunner: Send + Sync {
    async fn resolve(&self, topic: OhlcvTopic) -> Result<ResolvedOhlcvTopic, String>;

    async fn run(
        &self,
        topic: ResolvedOhlcvTopic,
        sender: broadcast::Sender<Arc<Vec<CcxtOhlcv>>>,
        shutdown: watch::Receiver<bool>,
    );
}

struct RealOhlcvUpstreamRunner;

impl RealOhlcvUpstreamRunner {
    fn new(_hyperliquid_base_url: String) -> Self {
        Self
    }
}

#[async_trait]
impl OhlcvUpstreamRunner for RealOhlcvUpstreamRunner {
    async fn resolve(&self, topic: OhlcvTopic) -> Result<ResolvedOhlcvTopic, String> {
        match topic.exchange.as_str() {
            "binance" => resolve_binance_ohlcv_topic(topic),
            "bybit" => resolve_bybit_ohlcv_topic(topic),
            "hyperliquid" => {
                Err("exchange `hyperliquid` is not supported for realtime ohlcv yet".to_string())
            }
            other => Err(format!(
                "exchange `{other}` is not supported for realtime ohlcv"
            )),
        }
    }

    async fn run(
        &self,
        topic: ResolvedOhlcvTopic,
        sender: broadcast::Sender<Arc<Vec<CcxtOhlcv>>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

        loop {
            if *shutdown.borrow() {
                return;
            }

            let connection_outcome =
                run_single_ohlcv_connection(&topic, &sender, &mut shutdown).await;

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

async fn run_single_ohlcv_connection(
    topic: &ResolvedOhlcvTopic,
    sender: &broadcast::Sender<Arc<Vec<CcxtOhlcv>>>,
    shutdown: &mut watch::Receiver<bool>,
) -> ConnectionOutcome {
    let ws_endpoint = match &topic.stream {
        UpstreamOhlcvStream::Binance { ws_endpoint }
        | UpstreamOhlcvStream::Bybit { ws_endpoint, .. } => ws_endpoint.as_str(),
    };

    let Ok((mut stream, _response)) = connect_async(ws_endpoint).await else {
        tracing::warn!(
            topic_key = %topic.key,
            ws_endpoint = %ws_endpoint,
            "failed to connect upstream ohlcv websocket"
        );
        return ConnectionOutcome::Reconnect {
            reset_backoff: false,
        };
    };

    tracing::info!(topic_key = %topic.key, "connected upstream ohlcv websocket");

    if let UpstreamOhlcvStream::Bybit {
        subscribe_topic, ..
    } = &topic.stream
    {
        if let Err(err) = send_bybit_subscription(&mut stream, &[subscribe_topic], "kline").await {
            tracing::warn!(topic_key = %topic.key, error = %err, "failed to send Bybit ohlcv subscription");
            return ConnectionOutcome::Reconnect {
                reset_backoff: false,
            };
        }
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
                        forward_ohlcv_update(&topic.stream, text.as_ref(), sender);
                    }
                    Some(Ok(TungsteniteMessage::Binary(binary))) => {
                        if let Ok(text) = String::from_utf8(binary.to_vec()) {
                            forward_ohlcv_update(&topic.stream, &text, sender);
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
                        tracing::warn!(topic_key = %topic.key, error = %err, "upstream ohlcv websocket error");
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

fn forward_ohlcv_update(
    stream: &UpstreamOhlcvStream,
    payload: &str,
    sender: &broadcast::Sender<Arc<Vec<CcxtOhlcv>>>,
) {
    let candles = match stream {
        UpstreamOhlcvStream::Binance { .. } => parse_binance_ohlcv_message(payload),
        UpstreamOhlcvStream::Bybit { .. } => parse_bybit_ohlcv_message(payload),
    };

    if !candles.is_empty() {
        let _ = sender.send(Arc::new(candles));
    }
}

fn resolve_binance_ohlcv_topic(topic: OhlcvTopic) -> Result<ResolvedOhlcvTopic, String> {
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_binance_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let stream_symbol = market_symbol.to_ascii_lowercase();
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BINANCE_QUOTES);
    let timeframe = resolve_ohlcv_timeframe(&topic.params);
    let interval = to_binance_ws_interval(&timeframe)?;

    let ws_endpoint = format!(
        "{}/{}@kline_{}",
        BINANCE_FUTURES_WS_BASE_URL.trim_end_matches('/'),
        stream_symbol,
        interval,
    );

    Ok(ResolvedOhlcvTopic {
        key: format!(
            "binance|ohlcv|symbol:{}|timeframe:{}",
            stream_symbol,
            normalize_ohlcv_timeframe(&timeframe)
        ),
        topic: OhlcvTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream: UpstreamOhlcvStream::Binance { ws_endpoint },
    })
}

fn resolve_bybit_ohlcv_topic(topic: OhlcvTopic) -> Result<ResolvedOhlcvTopic, String> {
    let category = parse_bybit_category(&topic.params)?;
    let coin_override = extract_non_empty_string(&topic.params, "coin");
    let market_symbol = resolve_bybit_ws_symbol_shared(&topic.symbol, coin_override.as_deref())?;
    let public_symbol = resolve_public_symbol_shared(&topic.symbol, &market_symbol, BYBIT_QUOTES);
    let timeframe = resolve_ohlcv_timeframe(&topic.params);
    let interval = to_bybit_ws_interval(&timeframe)?;

    let ws_endpoint = match category {
        BybitCategory::Spot => BYBIT_WS_SPOT_URL,
        BybitCategory::Linear => BYBIT_WS_LINEAR_URL,
        BybitCategory::Inverse => BYBIT_WS_INVERSE_URL,
        BybitCategory::Option => BYBIT_WS_OPTION_URL,
    };

    let subscribe_topic = format!("kline.{interval}.{market_symbol}");

    Ok(ResolvedOhlcvTopic {
        key: format!(
            "bybit|ohlcv|category:{}|symbol:{}|timeframe:{}",
            category.as_str(),
            market_symbol.to_ascii_lowercase(),
            normalize_ohlcv_timeframe(&timeframe),
        ),
        topic: OhlcvTopic {
            exchange: topic.exchange,
            symbol: public_symbol,
            params: topic.params,
        },
        stream: UpstreamOhlcvStream::Bybit {
            ws_endpoint: ws_endpoint.to_string(),
            subscribe_topic,
        },
    })
}

fn parse_binance_ohlcv_message(payload: &str) -> Vec<CcxtOhlcv> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let data = value.get("data").unwrap_or(&value);
    if data.get("e").and_then(Value::as_str).unwrap_or_default() != "kline" {
        return Vec::new();
    }

    let Some(kline) = data.get("k") else {
        return Vec::new();
    };

    let Some(timestamp) = kline.get("t").and_then(parse_u64_lossy_shared) else {
        return Vec::new();
    };
    let Some(open) = kline.get("o").and_then(parse_f64_lossy_shared) else {
        return Vec::new();
    };
    let Some(high) = kline.get("h").and_then(parse_f64_lossy_shared) else {
        return Vec::new();
    };
    let Some(low) = kline.get("l").and_then(parse_f64_lossy_shared) else {
        return Vec::new();
    };
    let Some(close) = kline.get("c").and_then(parse_f64_lossy_shared) else {
        return Vec::new();
    };
    let Some(volume) = kline.get("v").and_then(parse_f64_lossy_shared) else {
        return Vec::new();
    };

    vec![(timestamp, open, high, low, close, volume)]
}

fn parse_bybit_ohlcv_message(payload: &str) -> Vec<CcxtOhlcv> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !topic.starts_with("kline.") {
        return Vec::new();
    }

    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(timestamp) = row.get("start").and_then(parse_u64_lossy_shared) else {
            continue;
        };
        let Some(open) = row.get("open").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(high) = row.get("high").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(low) = row.get("low").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(close) = row.get("close").and_then(parse_f64_lossy_shared) else {
            continue;
        };
        let Some(volume) = row.get("volume").and_then(parse_f64_lossy_shared) else {
            continue;
        };

        parsed.push((timestamp, open, high, low, close, volume));
    }

    parsed.sort_by_key(|candle| candle.0);
    parsed
}

fn resolve_orderbook_levels(params: &Value) -> usize {
    extract_usize_param(params, "levels")
        .or_else(|| extract_usize_param(params, "depth"))
        .or_else(|| extract_usize_param(params, "limit"))
        .unwrap_or(DEFAULT_ORDERBOOK_LEVELS)
        .clamp(1, 1_000)
}
fn resolve_bybit_orderbook_levels(params: &Value, category: BybitCategory) -> usize {
    let requested = extract_usize_param(params, "levels")
        .or_else(|| extract_usize_param(params, "depth"))
        .or_else(|| extract_usize_param(params, "limit"))
        .unwrap_or(DEFAULT_ORDERBOOK_LEVELS);

    match category {
        BybitCategory::Option => requested.clamp(1, 25),
        BybitCategory::Spot | BybitCategory::Linear | BybitCategory::Inverse => {
            requested.clamp(1, BYBIT_MAX_FULL_DEPTH)
        }
    }
}
fn resolve_ohlcv_timeframe(params: &Value) -> String {
    let timeframe = extract_non_empty_string(params, "timeframe")
        .or_else(|| extract_non_empty_string(params, "interval"))
        .unwrap_or_else(|| DEFAULT_OHLCV_TIMEFRAME.to_string());
    normalize_ohlcv_timeframe(&timeframe)
}

fn normalize_ohlcv_timeframe(timeframe: &str) -> String {
    let trimmed = timeframe.trim();
    if trimmed == "1M" {
        return "1M".to_string();
    }
    trimmed.to_ascii_lowercase()
}

fn extract_u64_param(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(parse_u64_lossy_shared)
}

fn extract_usize_param(params: &Value, key: &str) -> Option<usize> {
    params.get(key).and_then(|value| {
        parse_u64_lossy_shared(value).and_then(|numeric| usize::try_from(numeric).ok())
    })
}

fn to_bybit_ws_depth_levels(levels: usize) -> usize {
    if levels <= 1 {
        1
    } else if levels <= 50 {
        50
    } else if levels <= 200 {
        200
    } else {
        1_000
    }
}

fn to_binance_ws_interval(timeframe: &str) -> Result<&'static str, String> {
    let trimmed = timeframe.trim();
    if trimmed == "1M" {
        return Ok("1M");
    }
    let normalized = trimmed.to_ascii_lowercase();
    match normalized.as_str() {
        "1m" => Ok("1m"),
        "3m" => Ok("3m"),
        "5m" => Ok("5m"),
        "15m" => Ok("15m"),
        "30m" => Ok("30m"),
        "1h" => Ok("1h"),
        "2h" => Ok("2h"),
        "4h" => Ok("4h"),
        "6h" => Ok("6h"),
        "8h" => Ok("8h"),
        "12h" => Ok("12h"),
        "1d" => Ok("1d"),
        "3d" => Ok("3d"),
        "1w" => Ok("1w"),
        _ => Err(format!(
            "unsupported Binance OHLCV timeframe `{timeframe}`; supported: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M"
        )),
    }
}

fn to_bybit_ws_interval(timeframe: &str) -> Result<&'static str, String> {
    let trimmed = timeframe.trim();
    if trimmed == "1M" {
        return Ok("M");
    }
    let normalized = trimmed.to_ascii_lowercase();
    match normalized.as_str() {
        "1m" => Ok("1"),
        "3m" => Ok("3"),
        "5m" => Ok("5"),
        "15m" => Ok("15"),
        "30m" => Ok("30"),
        "1h" => Ok("60"),
        "2h" => Ok("120"),
        "4h" => Ok("240"),
        "6h" => Ok("360"),
        "12h" => Ok("720"),
        "1d" => Ok("D"),
        "1w" => Ok("W"),
        _ => Err(format!(
            "unsupported Bybit OHLCV timeframe `{timeframe}`; supported: 1m,3m,5m,15m,30m,1h,2h,4h,6h,12h,1d,1w,1M"
        )),
    }
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

    fn lighter_catalog_service() -> Arc<LighterMarketCatalogService> {
        Arc::new(
            LighterMarketCatalogService::new(
                1_000,
                "https://explorer.elliot.ai/api/markets".to_string(),
                60_000,
            )
            .expect("lighter catalog service should build"),
        )
    }

    struct MockRunner {
        run_count: Arc<AtomicUsize>,
        stop_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TradesUpstreamRunner for MockRunner {
        async fn resolve(&self, topic: TradesTopic) -> Result<ResolvedTradesTopic, String> {
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

    struct MockBinanceSnapshotProvider;

    #[async_trait]
    impl BinanceOrderBookSnapshotProvider for MockBinanceSnapshotProvider {
        async fn fetch_binance_order_book_snapshot(
            &self,
            _market_symbol: &str,
        ) -> Result<crate::binance_orderbook::BinanceDepthSnapshot, String> {
            Ok(crate::binance_orderbook::BinanceDepthSnapshot {
                last_update_id: 0,
                bids: Vec::new(),
                asks: Vec::new(),
                timestamp: None,
            })
        }
    }

    fn orderbook_manager() -> OrderBookTopicManager {
        OrderBookTopicManager::new(
            "https://api.hyperliquid.xyz".to_string(),
            Arc::new(MockBinanceSnapshotProvider),
            "wss://mainnet.zklighter.elliot.ai/stream".to_string(),
            lighter_catalog_service(),
        )
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

    #[test]
    fn orderbook_resolution_normalizes_depth_to_shared_topic_key() {
        let manager = orderbook_manager();

        let topic_a = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"levels": 7}),
        )
        .expect("topic should be valid");
        let topic_b = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTCUSDT".to_string()),
            json!({"levels": 10}),
        )
        .expect("topic should be valid");

        let key_a = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(topic_a))
            .expect("first key should resolve");
        let key_b = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(topic_b))
            .expect("second key should resolve");

        assert_eq!(key_a, key_b);
        assert!(key_a.contains("orderbook"));
    }

    #[test]
    fn bybit_perp_orderbook_resolution_uses_requested_depth_tier() {
        let manager = orderbook_manager();

        let topic_a = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"category": "linear", "levels": 50}),
        )
        .expect("topic should be valid");
        let topic_b = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTCUSDT".to_string()),
            json!({"category": "linear", "levels": 1000}),
        )
        .expect("topic should be valid");

        let key_a = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(topic_a))
            .expect("first key should resolve");
        let key_b = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(topic_b))
            .expect("second key should resolve");

        assert_ne!(key_a, key_b);
        assert!(key_a.contains("depth:50"));
        assert!(key_b.contains("depth:1000"));
    }

    #[test]
    fn bybit_perp_orderbook_resolution_defaults_to_max_depth_tier() {
        let manager = orderbook_manager();

        let topic = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"category": "linear"}),
        )
        .expect("topic should be valid");

        let key = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(topic))
            .expect("key should resolve");

        assert!(key.contains("depth:50"));
    }

    #[test]
    fn lighter_trade_parser_accepts_single_object_payloads() {
        let trades = parse_lighter_trades_message(
            &json!({
                "channel": "trade:0",
                "trades": {
                    "trade_id": 14035051,
                    "price": "3335.65",
                    "size": "0.1187",
                    "usd_amount": "13.67",
                    "is_maker_ask": false,
                    "timestamp": 1722339648u64
                },
                "type": "update/trade"
            })
            .to_string(),
            "ETH/USD",
        );

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].id.as_deref(), Some("14035051"));
        assert_eq!(trades[0].side.as_deref(), Some("sell"));
        assert_eq!(trades[0].timestamp, Some(1_722_339_648_000));
        assert_eq!(trades[0].symbol.as_deref(), Some("ETH/USD"));
    }

    #[test]
    fn lighter_orderbook_applies_deltas_and_rejects_nonce_gaps() {
        let snapshot = parse_lighter_orderbook_message(
            &json!({
                "channel": "order_book:0",
                "timestamp": 1766434222583u64,
                "type": "update/order_book",
                "order_book": {
                    "code": 0,
                    "bids": [{"price": "3338.80", "size": "10.2898"}],
                    "asks": [{"price": "3327.46", "size": "29.0915"}],
                    "nonce": 100u64,
                    "begin_nonce": 90u64
                }
            })
            .to_string(),
        )
        .expect("snapshot should parse");

        let mut state = None;
        let initial = apply_lighter_orderbook_event(&mut state, snapshot, 50, "ETH/USD")
            .expect("snapshot should apply")
            .expect("snapshot should emit a book");
        assert_eq!(initial.nonce, Some(100));

        let delta = parse_lighter_orderbook_message(
            &json!({
                "channel": "order_book:0",
                "timestamp": 1766434222600u64,
                "type": "update/order_book",
                "order_book": {
                    "code": 0,
                    "bids": [{"price": "3338.80", "size": "11.0000"}],
                    "asks": [{"price": "3327.46", "size": "0"}],
                    "nonce": 101u64,
                    "begin_nonce": 100u64
                }
            })
            .to_string(),
        )
        .expect("delta should parse");

        let updated = apply_lighter_orderbook_event(&mut state, delta, 50, "ETH/USD")
            .expect("delta should apply")
            .expect("delta should emit a book");
        assert_eq!(updated.nonce, Some(101));
        assert_eq!(updated.bids.first().map(|row| row.1), Some(11.0));
        assert!(updated.asks.is_empty());

        let gap = parse_lighter_orderbook_message(
            &json!({
                "channel": "order_book:0",
                "timestamp": 1766434222650u64,
                "type": "update/order_book",
                "order_book": {
                    "code": 0,
                    "bids": [],
                    "asks": [],
                    "nonce": 102u64,
                    "begin_nonce": 99u64
                }
            })
            .to_string(),
        )
        .expect("gap payload should parse");

        let gap_result = apply_lighter_orderbook_event(&mut state, gap, 50, "ETH/USD");
        assert!(gap_result.is_err());
    }
    #[test]
    fn binance_deep_depth_shares_diff_topic_and_preserves_display_limit() {
        let partial = resolve_binance_orderbook_topic(
            TradesTopic::from_client_request(
                Some("binance".to_string()),
                Some("BTCUSDT".to_string()),
                json!({"levels": 20}),
            )
            .expect("partial topic should be valid"),
        )
        .expect("partial topic should resolve");
        assert!(partial.key.ends_with("depth:20"));
        assert_eq!(partial.levels_limit, 20);
        assert!(matches!(
            partial.stream,
            UpstreamOrderBookStream::BinancePartial { .. }
        ));

        let resolved = [21, 50, 1_000]
            .into_iter()
            .map(|levels| {
                resolve_binance_orderbook_topic(
                    TradesTopic::from_client_request(
                        Some("binance".to_string()),
                        Some("BTCUSDT".to_string()),
                        json!({"levels": levels}),
                    )
                    .expect("deep topic should be valid"),
                )
                .expect("deep topic should resolve")
            })
            .collect::<Vec<_>>();

        assert!(resolved
            .iter()
            .all(|topic| topic.key.ends_with("depth:diff1000")));
        assert!(resolved
            .iter()
            .all(|topic| topic.levels_limit == BINANCE_MAX_ORDERBOOK_LEVELS));
        assert_eq!(
            resolved
                .iter()
                .map(|topic| topic.display_levels_limit)
                .collect::<Vec<_>>(),
            vec![21, 50, 1_000]
        );
        for topic in resolved {
            let UpstreamOrderBookStream::BinanceDiff { ws_endpoint, .. } = topic.stream else {
                panic!("deep Binance topic should use diff stream");
            };
            assert!(ws_endpoint.ends_with("@depth@100ms"));
            assert!(!ws_endpoint.contains("depth1000"));
        }
    }

    #[test]
    fn bybit_full_depth_shares_upstream_across_display_limits() {
        let manager = orderbook_manager();
        let topic_a = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"category": "linear", "levels": 1001}),
        )
        .expect("topic should be valid");
        let topic_b = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTCUSDT".to_string()),
            json!({"category": "linear", "levels": 10000}),
        )
        .expect("topic should be valid");

        let resolved_a = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.inner.runner.resolve(topic_a))
            .expect("first full topic should resolve");
        let resolved_b = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.inner.runner.resolve(topic_b))
            .expect("second full topic should resolve");
        assert_eq!(resolved_a.key, resolved_b.key);
        assert!(resolved_a.key.ends_with("depth:full"));
        assert_eq!(resolved_a.display_levels_limit, 1001);
        assert_eq!(resolved_b.display_levels_limit, 10000);
        assert!(matches!(
            resolved_a.stream,
            UpstreamOrderBookStream::BybitFull { .. }
        ));
    }

    #[test]
    fn bybit_option_depth_stays_on_limited_topic() {
        let topic = TradesTopic::from_client_request(
            Some("bybit".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"category": "option", "levels": 1001}),
        )
        .expect("topic should be valid");
        let resolved = resolve_bybit_orderbook_topic(topic).expect("topic should resolve");

        assert_eq!(resolved.display_levels_limit, 25);
        assert!(matches!(
            resolved.stream,
            UpstreamOrderBookStream::BybitLimited { .. }
        ));
    }

    #[test]
    fn ohlcv_resolution_defaults_timeframe_and_rejects_unsupported_exchange() {
        let manager = OhlcvTopicManager::new("https://api.hyperliquid.xyz".to_string());

        let default_topic = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({}),
        )
        .expect("topic should be valid");
        let explicit_topic = TradesTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTCUSDT".to_string()),
            json!({"timeframe": "1m"}),
        )
        .expect("topic should be valid");

        let default_key = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(default_topic))
            .expect("default key should resolve");
        let explicit_key = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(explicit_topic))
            .expect("explicit key should resolve");

        assert_eq!(default_key, explicit_key);
        assert!(default_key.contains("ohlcv"));

        let unsupported_topic = TradesTopic::from_client_request(
            Some("hyperliquid".to_string()),
            Some("BTC/USDC:USDC".to_string()),
            json!({"timeframe": "1m"}),
        )
        .expect("topic should be valid");
        let unsupported = tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(manager.resolve_topic_key(unsupported_topic));
        assert!(unsupported.is_err());
    }
}
