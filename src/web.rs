use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{
        rejection::JsonRejection,
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::{
    sync::{
        broadcast,
        mpsc::{channel, error::TrySendError, Sender},
        Mutex, Notify, RwLock,
    },
    task::JoinHandle,
};
use tracing::warn;

use crate::{
    errors::ApiError,
    exchanges::{registry::ExchangeRegistry, traits::ExchangeError},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchMarketsParams, FetchMarketsRequest,
        FetchMarketsResponse, FetchOhlcvRequest, FetchOrderBookRequest, FetchTradesRequest,
        HealthResponse,
    },
    realtime::{
        OhlcvTopic, OhlcvTopicManager, OrderBookTopic, OrderBookTopicManager, TradesTopic,
        TradesTopicManager,
    },
};

#[derive(Clone)]
pub struct AppState {
    exchange_registry: Arc<ExchangeRegistry>,
    trades_topic_manager: TradesTopicManager,
    order_book_topic_manager: OrderBookTopicManager,
    ohlcv_topic_manager: OhlcvTopicManager,
    markets_cache: MarketsCache,
}

impl AppState {
    pub fn new(
        exchange_registry: Arc<ExchangeRegistry>,
        trades_topic_manager: TradesTopicManager,
        order_book_topic_manager: OrderBookTopicManager,
        ohlcv_topic_manager: OhlcvTopicManager,
    ) -> Self {
        Self {
            exchange_registry,
            trades_topic_manager,
            order_book_topic_manager,
            ohlcv_topic_manager,
            markets_cache: MarketsCache::new(Duration::from_secs(30)),
        }
    }
}

#[derive(Clone)]
struct MarketsCache {
    inner: Arc<MarketsCacheInner>,
}

struct MarketsCacheInner {
    ttl: Duration,
    entries: RwLock<HashMap<String, CachedMarketsResponse>>,
    key_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

#[derive(Clone)]
struct CachedMarketsResponse {
    response: FetchMarketsResponse,
    expires_at: Instant,
}

impl MarketsCache {
    fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(MarketsCacheInner {
                ttl,
                entries: RwLock::new(HashMap::new()),
                key_locks: Mutex::new(HashMap::new()),
            }),
        }
    }

    async fn get(&self, key: &str) -> Option<FetchMarketsResponse> {
        let now = Instant::now();

        {
            let entries = self.inner.entries.read().await;
            if let Some(entry) = entries.get(key) {
                if entry.expires_at > now {
                    return Some(entry.response.clone());
                }
            } else {
                return None;
            }
        }

        let mut entries = self.inner.entries.write().await;
        if let Some(entry) = entries.get(key) {
            if entry.expires_at > now {
                return Some(entry.response.clone());
            }
            entries.remove(key);
        }

        None
    }

    async fn insert(&self, key: String, response: FetchMarketsResponse) {
        let expires_at = Instant::now() + self.inner.ttl;
        let mut entries = self.inner.entries.write().await;
        entries.insert(
            key,
            CachedMarketsResponse {
                response,
                expires_at,
            },
        );
    }

    async fn lock_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut key_locks = self.inner.key_locks.lock().await;
        key_locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FetchMarketsApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("invalid exchange: {0}")]
    InvalidExchange(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
}

#[derive(Debug, Serialize)]
struct FetchMarketsErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug, Serialize)]
struct FetchMarketsErrorEnvelope {
    error: FetchMarketsErrorBody,
}

impl IntoResponse for FetchMarketsApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            FetchMarketsApiError::Validation(message) => {
                (StatusCode::BAD_REQUEST, "VALIDATION_ERROR", message)
            }
            FetchMarketsApiError::InvalidExchange(exchange_id) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_EXCHANGE",
                format!("Exchange '{exchange_id}' is not supported"),
            ),
            FetchMarketsApiError::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", message)
            }
            FetchMarketsApiError::Exchange(exchange_error) => match exchange_error {
                ExchangeError::BadSymbol(message) => {
                    (StatusCode::BAD_REQUEST, "BAD_SYMBOL", message)
                }
                ExchangeError::UpstreamRequest(message) => {
                    (StatusCode::BAD_GATEWAY, "UPSTREAM_REQUEST_FAILED", message)
                }
                ExchangeError::UpstreamData(message) => {
                    (StatusCode::BAD_GATEWAY, "UPSTREAM_DATA_INVALID", message)
                }
                ExchangeError::Internal(message) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_EXCHANGE_ERROR",
                    message,
                ),
            },
        };

        (
            status,
            Json(FetchMarketsErrorEnvelope {
                error: FetchMarketsErrorBody { code, message },
            }),
        )
            .into_response()
    }
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn fetch_trades(
    State(state): State<AppState>,
    Json(request): Json<FetchTradesRequest>,
) -> Result<Json<Vec<CcxtTrade>>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let trades = exchange.fetch_trades(request.into_params()).await?;
    Ok(Json(trades))
}

pub async fn fetch_ohlcv(
    State(state): State<AppState>,
    Json(request): Json<FetchOhlcvRequest>,
) -> Result<Json<Vec<CcxtOhlcv>>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(timeframe) = request.timeframe.as_ref() {
        if timeframe.trim().is_empty() {
            return Err(ApiError::Validation(
                "`timeframe` cannot be empty when provided".to_string(),
            ));
        }
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let candles = exchange.fetch_ohlcv(request.into_params()).await?;
    Ok(Json(candles))
}

pub async fn fetch_order_book(
    State(state): State<AppState>,
    Json(request): Json<FetchOrderBookRequest>,
) -> Result<Json<CcxtOrderBook>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let order_book = exchange.fetch_order_book(request.into_params()).await?;
    Ok(Json(order_book))
}

pub async fn fetch_markets(
    State(state): State<AppState>,
    payload: Result<Json<FetchMarketsRequest>, JsonRejection>,
) -> Result<Json<FetchMarketsResponse>, FetchMarketsApiError> {
    let Json(request) = payload.map_err(|err| {
        FetchMarketsApiError::Validation(format!("invalid request payload: {err}"))
    })?;

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(FetchMarketsApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    if exchange_id.is_empty() {
        return Err(FetchMarketsApiError::Validation(
            "`exchange` cannot be empty".to_string(),
        ));
    }

    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(FetchMarketsApiError::InvalidExchange(exchange_id));
    };

    let canonical_params = normalize_markets_params(request.params);
    let include_inactive = request.include_inactive;
    let cache_key = markets_cache_key(&exchange_id, &canonical_params, include_inactive)?;

    if let Some(cached) = state.markets_cache.get(&cache_key).await {
        return Ok(Json(cached));
    }

    let key_lock = state.markets_cache.lock_for(&cache_key).await;
    let _key_guard = key_lock.lock().await;

    if let Some(cached) = state.markets_cache.get(&cache_key).await {
        return Ok(Json(cached));
    }

    let markets = exchange
        .fetch_markets(FetchMarketsParams {
            params: canonical_params,
            include_inactive,
        })
        .await?;

    let response = FetchMarketsResponse {
        exchange: exchange_id,
        markets,
        timestamp: now_unix_millis(),
    };

    state
        .markets_cache
        .insert(cache_key, response.clone())
        .await;

    Ok(Json(response))
}

fn normalize_markets_params(params: Value) -> Value {
    if params.is_null() {
        return Value::Object(Map::new());
    }

    canonicalize_json(&params)
}

fn markets_cache_key(
    exchange: &str,
    params: &Value,
    include_inactive: bool,
) -> Result<String, FetchMarketsApiError> {
    let encoded_params = serde_json::to_string(params).map_err(|err| {
        FetchMarketsApiError::Internal(format!("failed to encode markets params: {err}"))
    })?;

    Ok(format!(
        "exchange={exchange}|includeInactive={include_inactive}|params={encoded_params}"
    ))
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

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientStreamCommand {
    op: String,
    channel: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
    #[serde(default)]
    params: Value,
}

enum ParsedStreamCommand {
    Subscribe {
        channel: RealtimeChannel,
        topic: TradesTopic,
    },
    Unsubscribe {
        channel: RealtimeChannel,
        topic: TradesTopic,
    },
    Ping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RealtimeChannel {
    Trades,
    OrderBook,
    Ohlcv,
}

impl RealtimeChannel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::OrderBook => "orderbook",
            Self::Ohlcv => "ohlcv",
        }
    }

    fn from_client_value(value: &str) -> Option<Self> {
        match value {
            "trades" => Some(Self::Trades),
            "orderbook" | "order_book" => Some(Self::OrderBook),
            "ohlcv" | "candles" | "kline" | "klines" => Some(Self::Ohlcv),
            _ => None,
        }
    }
}

struct ClientSubscription {
    channel: RealtimeChannel,
    upstream_key: String,
    topic: TradesTopic,
    forward_task: JoinHandle<()>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsErrorMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsAckMessage<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    op: &'static str,
    topic: &'a TradesTopic,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsTradesUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a TradesTopic,
    data: &'a [CcxtTrade],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsOrderBookUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a TradesTopic,
    data: &'a CcxtOrderBook,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsOhlcvUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a TradesTopic,
    data: &'a [CcxtOhlcv],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsLagWarning<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    code: &'static str,
    message: String,
    topic: &'a TradesTopic,
    dropped_messages: u64,
}

#[derive(Serialize)]
struct WsPongMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
}

const CLIENT_OUTGOING_QUEUE_CAPACITY: usize = 256;

pub async fn trades_stream_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_trades_stream_socket(socket, state))
}

async fn handle_trades_stream_socket(socket: WebSocket, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (outgoing_sender, mut outgoing_receiver) =
        channel::<Message>(CLIENT_OUTGOING_QUEUE_CAPACITY);
    let close_signal = Arc::new(Notify::new());

    let writer_task = tokio::spawn(async move {
        while let Some(message) = outgoing_receiver.recv().await {
            if ws_sender.send(message).await.is_err() {
                break;
            }
        }
    });

    let mut subscriptions: HashMap<String, ClientSubscription> = HashMap::new();

    loop {
        let next_message = tokio::select! {
            _ = close_signal.notified() => {
                warn!("closing websocket client after forwarder backpressure");
                break;
            }
            next_message = ws_receiver.next() => next_message,
        };

        let Some(next_message) = next_message else {
            break;
        };

        let message = match next_message {
            Ok(message) => message,
            Err(err) => {
                warn!(error = %err, "client websocket read error");
                break;
            }
        };

        match message {
            Message::Ping(payload) => {
                if outgoing_sender.try_send(Message::Pong(payload)).is_err() {
                    break;
                }
            }
            Message::Close(_) => {
                break;
            }
            other => {
                let text = match ws_message_to_text(other) {
                    Ok(Some(text)) => text,
                    Ok(None) => continue,
                    Err(err) => {
                        if !send_ws_error(&outgoing_sender, "INVALID_MESSAGE", err) {
                            break;
                        }
                        continue;
                    }
                };

                let command = match parse_stream_command(&text) {
                    Ok(command) => command,
                    Err(err) => {
                        if !send_ws_error(&outgoing_sender, "INVALID_COMMAND", err) {
                            break;
                        }
                        continue;
                    }
                };

                if !handle_stream_command(
                    &state,
                    command,
                    &outgoing_sender,
                    &close_signal,
                    &mut subscriptions,
                )
                .await
                {
                    break;
                }
            }
        }
    }

    for (_key, subscription) in subscriptions {
        subscription.forward_task.abort();
        unsubscribe_channel(&state, subscription.channel, &subscription.upstream_key).await;
    }

    drop(outgoing_sender);
    let _ = writer_task.await;
}

async fn handle_stream_command(
    state: &AppState,
    command: ParsedStreamCommand,
    outgoing_sender: &Sender<Message>,
    close_signal: &Arc<Notify>,
    subscriptions: &mut HashMap<String, ClientSubscription>,
) -> bool {
    match command {
        ParsedStreamCommand::Ping => send_ws_json(
            outgoing_sender,
            &WsPongMessage {
                message_type: "pong",
            },
        ),
        ParsedStreamCommand::Subscribe { channel, topic } => {
            let topic_key = match resolve_topic_key_by_channel(state, channel, topic.clone()) {
                Ok(topic_key) => topic_key,
                Err(err) => return send_ws_error(outgoing_sender, "INVALID_TOPIC", err),
            };

            let local_key = to_local_subscription_key(channel, &topic_key);

            if let Some(existing) = subscriptions.get(&local_key) {
                return send_ws_json(
                    outgoing_sender,
                    &WsAckMessage {
                        message_type: "alreadySubscribed",
                        op: "subscribe",
                        topic: &existing.topic,
                    },
                );
            }

            match channel {
                RealtimeChannel::Trades => {
                    let subscription = match state.trades_topic_manager.subscribe(topic).await {
                        Ok(subscription) => subscription,
                        Err(err) => {
                            return send_ws_error(outgoing_sender, "SUBSCRIBE_FAILED", err);
                        }
                    };

                    let topic = subscription.topic.clone();
                    let forward_task = spawn_trades_topic_forwarder(
                        topic.clone(),
                        subscription.receiver,
                        outgoing_sender.clone(),
                        close_signal.clone(),
                    );

                    subscriptions.insert(
                        local_key,
                        ClientSubscription {
                            channel,
                            upstream_key: subscription.key,
                            topic: topic.clone(),
                            forward_task,
                        },
                    );

                    send_ws_json(
                        outgoing_sender,
                        &WsAckMessage {
                            message_type: "subscribed",
                            op: "subscribe",
                            topic: &topic,
                        },
                    )
                }
                RealtimeChannel::OrderBook => {
                    let subscription = match state
                        .order_book_topic_manager
                        .subscribe(topic.clone())
                        .await
                    {
                        Ok(subscription) => subscription,
                        Err(err) => {
                            return send_ws_error(outgoing_sender, "SUBSCRIBE_FAILED", err);
                        }
                    };

                    let topic = subscription.topic.clone();
                    let forward_task = spawn_orderbook_topic_forwarder(
                        topic.clone(),
                        subscription.receiver,
                        outgoing_sender.clone(),
                        close_signal.clone(),
                    );

                    subscriptions.insert(
                        local_key,
                        ClientSubscription {
                            channel,
                            upstream_key: subscription.key,
                            topic: topic.clone(),
                            forward_task,
                        },
                    );

                    send_ws_json(
                        outgoing_sender,
                        &WsAckMessage {
                            message_type: "subscribed",
                            op: "subscribe",
                            topic: &topic,
                        },
                    )
                }
                RealtimeChannel::Ohlcv => {
                    let subscription = match state.ohlcv_topic_manager.subscribe(topic).await {
                        Ok(subscription) => subscription,
                        Err(err) => {
                            return send_ws_error(outgoing_sender, "SUBSCRIBE_FAILED", err);
                        }
                    };

                    let topic = subscription.topic.clone();
                    let forward_task = spawn_ohlcv_topic_forwarder(
                        topic.clone(),
                        subscription.receiver,
                        outgoing_sender.clone(),
                        close_signal.clone(),
                    );

                    subscriptions.insert(
                        local_key,
                        ClientSubscription {
                            channel,
                            upstream_key: subscription.key,
                            topic: topic.clone(),
                            forward_task,
                        },
                    );

                    send_ws_json(
                        outgoing_sender,
                        &WsAckMessage {
                            message_type: "subscribed",
                            op: "subscribe",
                            topic: &topic,
                        },
                    )
                }
            }
        }
        ParsedStreamCommand::Unsubscribe { channel, topic } => {
            let topic_key = match resolve_topic_key_by_channel(state, channel, topic.clone()) {
                Ok(topic_key) => topic_key,
                Err(err) => return send_ws_error(outgoing_sender, "INVALID_TOPIC", err),
            };

            let local_key = to_local_subscription_key(channel, &topic_key);
            let Some(existing) = subscriptions.remove(&local_key) else {
                return send_ws_error(
                    outgoing_sender,
                    "NOT_SUBSCRIBED",
                    "topic is not currently subscribed on this connection",
                );
            };

            existing.forward_task.abort();
            unsubscribe_channel(state, channel, &topic_key).await;

            send_ws_json(
                outgoing_sender,
                &WsAckMessage {
                    message_type: "unsubscribed",
                    op: "unsubscribe",
                    topic: &existing.topic,
                },
            )
        }
    }
}

fn resolve_topic_key_by_channel(
    state: &AppState,
    channel: RealtimeChannel,
    topic: TradesTopic,
) -> Result<String, String> {
    match channel {
        RealtimeChannel::Trades => state.trades_topic_manager.resolve_topic_key(topic),
        RealtimeChannel::OrderBook => state.order_book_topic_manager.resolve_topic_key(topic),
        RealtimeChannel::Ohlcv => state.ohlcv_topic_manager.resolve_topic_key(topic),
    }
}

async fn unsubscribe_channel(state: &AppState, channel: RealtimeChannel, key: &str) {
    match channel {
        RealtimeChannel::Trades => state.trades_topic_manager.unsubscribe_by_key(key).await,
        RealtimeChannel::OrderBook => state.order_book_topic_manager.unsubscribe_by_key(key).await,
        RealtimeChannel::Ohlcv => state.ohlcv_topic_manager.unsubscribe_by_key(key).await,
    }
}

fn to_local_subscription_key(channel: RealtimeChannel, key: &str) -> String {
    format!("{}|{key}", channel.as_str())
}

fn spawn_trades_topic_forwarder(
    topic: TradesTopic,
    mut receiver: broadcast::Receiver<Arc<Vec<CcxtTrade>>>,
    outgoing_sender: Sender<Message>,
    close_signal: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(trades) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsTradesUpdate {
                            message_type: "trades",
                            topic: &topic,
                            data: trades.as_ref(),
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsLagWarning {
                            message_type: "warning",
                            code: "CLIENT_LAGGED",
                            message: "client lagged behind realtime stream".to_string(),
                            topic: &topic,
                            dropped_messages: skipped,
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return;
                }
            }
        }
    })
}

fn spawn_orderbook_topic_forwarder(
    topic: OrderBookTopic,
    mut receiver: broadcast::Receiver<Arc<CcxtOrderBook>>,
    outgoing_sender: Sender<Message>,
    close_signal: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(orderbook) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsOrderBookUpdate {
                            message_type: "orderbook",
                            topic: &topic,
                            data: orderbook.as_ref(),
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsLagWarning {
                            message_type: "warning",
                            code: "CLIENT_LAGGED",
                            message: "client lagged behind realtime stream".to_string(),
                            topic: &topic,
                            dropped_messages: skipped,
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return;
                }
            }
        }
    })
}

fn spawn_ohlcv_topic_forwarder(
    topic: OhlcvTopic,
    mut receiver: broadcast::Receiver<Arc<Vec<CcxtOhlcv>>>,
    outgoing_sender: Sender<Message>,
    close_signal: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(candles) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsOhlcvUpdate {
                            message_type: "ohlcv",
                            topic: &topic,
                            data: candles.as_ref(),
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsLagWarning {
                            message_type: "warning",
                            code: "CLIENT_LAGGED",
                            message: "client lagged behind realtime stream".to_string(),
                            topic: &topic,
                            dropped_messages: skipped,
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return;
                }
            }
        }
    })
}

fn parse_stream_command(payload: &str) -> Result<ParsedStreamCommand, String> {
    let command = serde_json::from_str::<ClientStreamCommand>(payload)
        .map_err(|err| format!("invalid JSON command: {err}"))?;

    let op = command.op.trim().to_ascii_lowercase();
    match op.as_str() {
        "ping" => Ok(ParsedStreamCommand::Ping),
        "subscribe" | "unsubscribe" => {
            let channel_value = command
                .channel
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();

            let channel = if channel_value.is_empty() {
                RealtimeChannel::Trades
            } else if let Some(channel) = RealtimeChannel::from_client_value(&channel_value) {
                channel
            } else {
                return Err(format!(
                    "unsupported channel `{channel_value}`; supported channels: `trades`, `orderbook`, `ohlcv`"
                ));
            };

            let topic =
                TradesTopic::from_client_request(command.exchange, command.symbol, command.params)?;

            if op == "subscribe" {
                Ok(ParsedStreamCommand::Subscribe { channel, topic })
            } else {
                Ok(ParsedStreamCommand::Unsubscribe { channel, topic })
            }
        }
        other => Err(format!(
            "unsupported op `{other}`; expected `subscribe`, `unsubscribe`, or `ping`"
        )),
    }
}

fn ws_message_to_text(message: Message) -> Result<Option<String>, String> {
    match message {
        Message::Text(text) => Ok(Some(text.to_string())),
        Message::Binary(binary) => String::from_utf8(binary.to_vec())
            .map(Some)
            .map_err(|err| format!("invalid UTF-8 websocket payload: {err}")),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => Ok(None),
    }
}

fn send_ws_error(
    outgoing_sender: &Sender<Message>,
    code: &'static str,
    message: impl Into<String>,
) -> bool {
    send_ws_json(
        outgoing_sender,
        &WsErrorMessage {
            message_type: "error",
            code,
            message: message.into(),
        },
    )
}

fn send_ws_json<T: Serialize>(outgoing_sender: &Sender<Message>, payload: &T) -> bool {
    let encoded = match serde_json::to_string(payload) {
        Ok(encoded) => encoded,
        Err(err) => {
            warn!(error = %err, "failed to serialize websocket message");
            return false;
        }
    };

    match outgoing_sender.try_send(Message::Text(encoded.into())) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            warn!("closing websocket client: outgoing queue is full");
            false
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::time::timeout;

    use super::*;

    fn sample_trade() -> CcxtTrade {
        CcxtTrade {
            info: json!({"tid": 1}),
            amount: Some(0.1),
            datetime: Some("2026-01-01T00:00:00.000Z".to_string()),
            id: Some("1".to_string()),
            order: None,
            price: Some(100.0),
            timestamp: Some(1),
            trade_type: None,
            side: Some("buy".to_string()),
            symbol: Some("BTC/USDC:USDC".to_string()),
            taker_or_maker: None,
            cost: Some(10.0),
            fee: None,
        }
    }

    #[tokio::test]
    async fn topic_forwarder_signals_close_when_outgoing_queue_is_full() {
        let topic = TradesTopic::from_client_request(
            Some("hyperliquid".to_string()),
            Some("BTC/USDC:USDC".to_string()),
            Value::Null,
        )
        .expect("topic should be valid");

        let (broadcast_sender, receiver) = broadcast::channel::<Arc<Vec<CcxtTrade>>>(8);
        let (outgoing_sender, _outgoing_receiver) = channel::<Message>(1);
        outgoing_sender
            .try_send(Message::Text("filled".to_string().into()))
            .expect("queue preload should succeed");

        let close_signal = Arc::new(Notify::new());
        let forwarder =
            spawn_trades_topic_forwarder(topic, receiver, outgoing_sender, close_signal.clone());

        let _ = broadcast_sender
            .send(Arc::new(vec![sample_trade()]))
            .expect("broadcast should succeed");

        timeout(Duration::from_secs(1), close_signal.notified())
            .await
            .expect("close signal should be notified");

        let _ = timeout(Duration::from_secs(1), forwarder)
            .await
            .expect("forwarder task should complete");
    }

    #[test]
    fn parse_stream_command_supports_all_realtime_channels() {
        let orderbook_payload = json!({
            "op": "subscribe",
            "channel": "orderbook",
            "exchange": "bybit",
            "symbol": "BTC/USDT:USDT",
            "params": {"category": "linear", "levels": 50}
        })
        .to_string();

        let ohlcv_payload = json!({
            "op": "unsubscribe",
            "channel": "ohlcv",
            "exchange": "binance",
            "symbol": "BTC/USDT:USDT",
            "params": {"timeframe": "1m"}
        })
        .to_string();

        let orderbook_command = parse_stream_command(&orderbook_payload)
            .expect("orderbook command should parse successfully");
        let ohlcv_command =
            parse_stream_command(&ohlcv_payload).expect("ohlcv command should parse successfully");

        assert!(matches!(
            orderbook_command,
            ParsedStreamCommand::Subscribe {
                channel: RealtimeChannel::OrderBook,
                ..
            }
        ));

        assert!(matches!(
            ohlcv_command,
            ParsedStreamCommand::Unsubscribe {
                channel: RealtimeChannel::Ohlcv,
                ..
            }
        ));

        let unsupported = parse_stream_command(
            &json!({
                "op": "subscribe",
                "channel": "funding",
                "exchange": "bybit",
                "symbol": "BTC/USDT:USDT",
                "params": {}
            })
            .to_string(),
        );
        assert!(unsupported.is_err());
    }

    #[test]
    fn markets_cache_key_is_stable_for_equivalent_params() {
        let params_a = normalize_markets_params(json!({
            "b": 1,
            "a": {
                "y": 2,
                "x": 1
            }
        }));
        let params_b = normalize_markets_params(json!({
            "a": {
                "x": 1,
                "y": 2
            },
            "b": 1
        }));

        let key_a = markets_cache_key("bybit", &params_a, false).expect("key should build");
        let key_b = markets_cache_key("bybit", &params_b, false).expect("key should build");

        assert_eq!(key_a, key_b);
    }
}
