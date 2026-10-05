use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
        watch, Mutex, Notify, RwLock,
    },
    task::JoinHandle,
};
use tracing::warn;

use crate::{
    errors::ApiError,
    exchanges::{registry::ExchangeRegistry, traits::ExchangeError},
    market_stats::{projection_key, MarketStatsCoordinator},
    models::{
        CapabilitiesResponse, CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTrade,
        ExchangeCapabilities, FeatureCapability, FetchMarketStatsRequest, FetchMarketsParams,
        FetchMarketsRequest, FetchMarketsResponse, FetchOhlcvRequest, FetchOrderBookRequest,
        FetchTradesRequest, HealthResponse, MarketStatsCapabilities, MarketStatsSnapshot,
        MarketStatsTopic,
    },
    realtime::{RealtimeChannel, RealtimeReceiver, RealtimeService, RealtimeTopic, RealtimeUpdate},
};

mod market_stats_stream;
use market_stats_stream::spawn_market_stats_forwarder;

#[derive(Clone)]
pub struct AppState {
    exchange_registry: Arc<ExchangeRegistry>,
    realtime: RealtimeService,
    markets_cache: MarketsCache,
    market_stats: MarketStatsCoordinator,
    sockets: watch::Sender<SocketLifecycle>,
}

impl AppState {
    pub fn new(exchange_registry: Arc<ExchangeRegistry>, realtime: RealtimeService) -> Self {
        Self {
            market_stats: MarketStatsCoordinator::new(exchange_registry.clone()),
            exchange_registry,
            realtime,
            markets_cache: MarketsCache::new(Duration::from_secs(30)),
            sockets: watch::channel(SocketLifecycle::default()).0,
        }
    }

    pub async fn shutdown_market_stats(&self) {
        self.market_stats.shutdown().await;
    }

    pub async fn shutdown_realtime(&self) -> Result<(), ExchangeError> {
        self.realtime.shutdown().await
    }

    /// Reject new upgrades, cancel pending commands, and join client forwarders/writers.
    pub async fn shutdown_websockets(&self) {
        self.sockets.send_modify(|state| state.stopping = true);
        let _ = self
            .sockets
            .subscribe()
            .wait_for(|state| state.active == 0)
            .await;
    }
}

#[derive(Default)]
struct SocketLifecycle {
    stopping: bool,
    active: usize,
}

struct SocketLease(watch::Sender<SocketLifecycle>);

impl Drop for SocketLease {
    fn drop(&mut self) {
        self.0.send_modify(|state| state.active -= 1);
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
                ExchangeError::UnsupportedFeature(message) => {
                    (StatusCode::NOT_IMPLEMENTED, "UNSUPPORTED_FEATURE", message)
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

pub async fn fetch_market_stats(
    State(state): State<AppState>,
    payload: Result<Json<FetchMarketStatsRequest>, JsonRejection>,
) -> Result<Json<MarketStatsSnapshot>, ApiError> {
    let Json(request) = payload.map_err(|error| ApiError::Validation(error.body_text()))?;
    state.market_stats.snapshot(request).await.map(Json)
}

pub async fn capabilities(State(state): State<AppState>) -> Json<CapabilitiesResponse> {
    let exchanges = state
        .exchange_registry
        .ids()
        .into_iter()
        .filter_map(|id| {
            let exchange = state.exchange_registry.get(id)?;
            let market_stats = exchange
                .market_stats_source()
                .map(|source| source.capabilities())
                .unwrap_or_else(|| MarketStatsCapabilities::Unsupported {
                    reason: "adapter-not-implemented".to_string(),
                });
            Some(ExchangeCapabilities {
                exchange: id.to_string(),
                market_stats,
                funding_rate_history: FeatureCapability {
                    state: CapabilityState::Unsupported,
                    reason: Some("adapter-not-implemented".to_string()),
                },
            })
        })
        .collect();
    Json(CapabilitiesResponse { exchanges })
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
    #[serde(default)]
    market_ids: Value,
    #[serde(default)]
    fields: Value,
}

enum ParsedStreamCommand {
    Subscribe {
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    },
    Unsubscribe {
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    },
    SubscribeMarketStats(FetchMarketStatsRequest),
    UnsubscribeMarketStats(FetchMarketStatsRequest),
    Ping,
}

struct ClientSubscription {
    channel: RealtimeChannel,
    topic: RealtimeTopic,
    request_topic: RealtimeTopic,
    forward_task: JoinHandle<()>,
}

struct MarketStatsClientSubscription {
    topic: MarketStatsTopic,
    key: String,
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
struct WsAckMessage<'a, T: Serialize> {
    #[serde(rename = "type")]
    message_type: &'static str,
    op: &'static str,
    topic: &'a T,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsTradesUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a RealtimeTopic,
    data: &'a [CcxtTrade],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsOrderBookUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a RealtimeTopic,
    data: WsOrderBookView<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsOrderBookView<'a> {
    asks: &'a [(f64, f64)],
    bids: &'a [(f64, f64)],
    datetime: Option<&'a str>,
    timestamp: Option<u64>,
    nonce: Option<u64>,
    symbol: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsOhlcvUpdate<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    topic: &'a RealtimeTopic,
    data: &'a [CcxtOhlcv],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WsLagWarning<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    code: &'static str,
    message: String,
    topic: &'a RealtimeTopic,
    dropped_messages: u64,
}

#[derive(Serialize)]
struct WsPongMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
}

const CLIENT_OUTGOING_QUEUE_CAPACITY: usize = 256;
const CLIENT_REALTIME_SUBSCRIPTION_LIMIT: usize = 200;

pub async fn trades_stream_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_trades_stream_socket(socket, state))
}

async fn handle_trades_stream_socket(socket: WebSocket, state: AppState) {
    if !state.sockets.send_if_modified(|lifecycle| {
        if lifecycle.stopping {
            return false;
        }
        lifecycle.active += 1;
        true
    }) {
        return;
    }
    let _lease = SocketLease(state.sockets.clone());
    let mut stopping = state.sockets.subscribe();
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (outgoing_sender, mut outgoing_receiver) =
        channel::<Message>(CLIENT_OUTGOING_QUEUE_CAPACITY);
    let close_signal = Arc::new(Notify::new());
    let force_close = Arc::new(AtomicBool::new(false));

    let mut writer_task = tokio::spawn(async move {
        while let Some(message) = outgoing_receiver.recv().await {
            if ws_sender.send(message).await.is_err() {
                break;
            }
        }
    });

    let mut subscriptions: HashMap<String, ClientSubscription> = HashMap::new();
    let mut stats_subscriptions: HashMap<String, MarketStatsClientSubscription> = HashMap::new();

    loop {
        let next_message = tokio::select! {
            _ = stopping.wait_for(|state| state.stopping) => {
                force_close.store(true, Ordering::Release);
                break;
            }
            _ = close_signal.notified() => {
                force_close.store(true, Ordering::Release);
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
                    force_close.store(true, Ordering::Release);
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
                            force_close.store(true, Ordering::Release);
                            break;
                        }
                        continue;
                    }
                };

                let command = match parse_stream_command(&text) {
                    Ok(command) => command,
                    Err(err) => {
                        if !send_ws_error(&outgoing_sender, "INVALID_COMMAND", err) {
                            force_close.store(true, Ordering::Release);
                            break;
                        }
                        continue;
                    }
                };

                let keep_open = tokio::select! {
                    _ = stopping.wait_for(|state| state.stopping) => false,
                    keep_open = handle_stream_command(
                        &state,
                        command,
                        &outgoing_sender,
                        &close_signal,
                        &mut subscriptions,
                        &mut stats_subscriptions,
                        &force_close,
                    ) => keep_open,
                };
                if !keep_open {
                    force_close.store(true, Ordering::Release);
                    break;
                }
            }
        }
    }

    for (_key, subscription) in subscriptions {
        subscription.forward_task.abort();
        let _ = subscription.forward_task.await;
    }
    for (_, subscription) in stats_subscriptions {
        subscription.forward_task.abort();
        let _ = subscription.forward_task.await;
        state
            .market_stats
            .unsubscribe_by_key(&subscription.key)
            .await;
    }

    drop(outgoing_sender);
    if force_close.load(Ordering::Acquire) {
        writer_task.abort();
    }
    tokio::select! {
        _ = &mut writer_task => {},
        _ = async { let _ = stopping.wait_for(|state| state.stopping).await; } => {
            writer_task.abort();
            let _ = writer_task.await;
        },
    }
}

async fn handle_stream_command(
    state: &AppState,
    command: ParsedStreamCommand,
    outgoing_sender: &Sender<Message>,
    close_signal: &Arc<Notify>,
    subscriptions: &mut HashMap<String, ClientSubscription>,
    stats_subscriptions: &mut HashMap<String, MarketStatsClientSubscription>,
    force_close: &Arc<AtomicBool>,
) -> bool {
    match command {
        ParsedStreamCommand::SubscribeMarketStats(request) => {
            let topic = match state.market_stats.normalize_request(request.clone()) {
                Ok(topic) => topic,
                Err(error) => return send_stats_error(outgoing_sender, error),
            };
            let key = match projection_key(&topic) {
                Ok(key) => key,
                Err(error) => return send_stats_error(outgoing_sender, error),
            };
            if let Some(existing) = stats_subscriptions.get(&key) {
                return send_ws_json(
                    outgoing_sender,
                    &WsAckMessage {
                        message_type: "alreadySubscribed",
                        op: "subscribe",
                        topic: &existing.topic,
                    },
                );
            }
            if stats_subscriptions.len() >= 16 {
                return send_ws_error(
                    outgoing_sender,
                    "SUBSCRIPTION_LIMIT",
                    "at most 16 statistics subscriptions are allowed per connection",
                );
            }
            let result = tokio::select! {
                _ = close_signal.notified() => return false,
                result = state.market_stats.subscribe(request) => result,
            };
            let subscription = match result {
                Ok(subscription) => subscription,
                Err(error) => return send_stats_error(outgoing_sender, error),
            };
            if !send_ws_json(
                outgoing_sender,
                &WsAckMessage {
                    message_type: "subscribed",
                    op: "subscribe",
                    topic: &subscription.topic,
                },
            ) {
                state
                    .market_stats
                    .unsubscribe_by_key(&subscription.key)
                    .await;
                force_close.store(true, Ordering::Release);
                close_signal.notify_one();
                return false;
            }
            let forward_task = spawn_market_stats_forwarder(
                subscription.topic.clone(),
                subscription.receiver,
                outgoing_sender.clone(),
                close_signal.clone(),
                force_close.clone(),
            );
            stats_subscriptions.insert(
                key,
                MarketStatsClientSubscription {
                    topic: subscription.topic,
                    key: subscription.key,
                    forward_task,
                },
            );
            true
        }
        ParsedStreamCommand::UnsubscribeMarketStats(request) => {
            // Canonicalize syntax/scope only: vanished identities must remain unsubscribable.
            let topic = match state.market_stats.normalize_request(request) {
                Ok(topic) => topic,
                Err(error) => return send_stats_error(outgoing_sender, error),
            };
            let key = match projection_key(&topic) {
                Ok(key) => key,
                Err(error) => return send_stats_error(outgoing_sender, error),
            };
            let Some(existing) = stats_subscriptions.remove(&key) else {
                return send_ws_error(
                    outgoing_sender,
                    "NOT_SUBSCRIBED",
                    "topic is not currently subscribed on this connection",
                );
            };
            existing.forward_task.abort();
            let _ = existing.forward_task.await;
            state.market_stats.unsubscribe_by_key(&existing.key).await;
            send_ws_json(
                outgoing_sender,
                &WsAckMessage {
                    message_type: "unsubscribed",
                    op: "unsubscribe",
                    topic: &existing.topic,
                },
            )
        }
        ParsedStreamCommand::Ping => send_ws_json(
            outgoing_sender,
            &WsPongMessage {
                message_type: "pong",
            },
        ),
        ParsedStreamCommand::Subscribe { channel, topic } => {
            let request_topic = topic.clone();

            // Resolve owned metadata/policy once, then attach without resolving
            // twice. Cancellation during either wait leaves no orphaned demand:
            // the RAII receiver only exists once a subscription is owned.
            let prepared = tokio::select! {
                _ = close_signal.notified() => return false,
                result = state.realtime.prepare(channel, topic) => result,
            };
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(err) => {
                    return send_ws_error(outgoing_sender, "INVALID_TOPIC", err.to_string());
                }
            };

            let client_key = prepared.client_key.clone();

            if let Some(existing) = subscriptions.get(&client_key) {
                return send_ws_json(
                    outgoing_sender,
                    &WsAckMessage {
                        message_type: "alreadySubscribed",
                        op: "subscribe",
                        topic: &existing.topic,
                    },
                );
            }
            if subscriptions.len() >= CLIENT_REALTIME_SUBSCRIPTION_LIMIT {
                return send_ws_error(
                    outgoing_sender,
                    "SUBSCRIPTION_LIMIT",
                    "at most 200 realtime subscriptions are allowed per connection",
                );
            }

            let subscription = tokio::select! {
                _ = close_signal.notified() => return false,
                result = state.realtime.subscribe_prepared(prepared) => result,
            };
            let subscription = match subscription {
                Ok(subscription) => subscription,
                Err(err) => {
                    return send_ws_error(outgoing_sender, "SUBSCRIBE_FAILED", err.to_string());
                }
            };

            let key = subscription.key;
            let topic = subscription.topic;
            let levels_limit = subscription.levels_limit;
            let receiver = subscription.receiver;

            // Enqueue the acknowledgement before the forwarder exists: it is the
            // first frame in the queue, so the first update can never overtake
            // `subscribed` for this topic.
            if !send_ws_json(
                outgoing_sender,
                &WsAckMessage {
                    message_type: "subscribed",
                    op: "subscribe",
                    topic: &topic,
                },
            ) {
                // Returning drops the still-owned receiver, releasing demand.
                force_close.store(true, Ordering::Release);
                close_signal.notify_one();
                return false;
            }

            let forward_task = spawn_realtime_forwarder(
                topic.clone(),
                levels_limit,
                receiver,
                outgoing_sender.clone(),
                close_signal.clone(),
            );

            subscriptions.insert(
                key,
                ClientSubscription {
                    channel,
                    topic,
                    request_topic,
                    forward_task,
                },
            );

            true
        }
        ParsedStreamCommand::Unsubscribe { channel, topic } => {
            let client_key =
                match client_key_for_unsubscribe(state, channel, &topic, subscriptions).await {
                    Ok(client_key) => client_key,
                    Err(err) => return send_ws_error(outgoing_sender, "INVALID_TOPIC", err),
                };

            let Some(existing) = subscriptions.remove(&client_key) else {
                return send_ws_error(
                    outgoing_sender,
                    "NOT_SUBSCRIBED",
                    "topic is not currently subscribed on this connection",
                );
            };

            // Abort+await releases the receiver (and thus this viewer's demand)
            // before acknowledging, without any network unsubscribe round trip.
            existing.forward_task.abort();
            let _ = existing.forward_task.await;

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

/// Canonical client key for an unsubscribe. The exact original request topic or
/// the previously canonicalized topic already held on this connection is matched
/// locally, so releasing a known subscription never needs metadata/network (it
/// still works during a metadata outage). Unmatched aliases fall back to
/// `prepare`, which canonicalizes them exactly like subscribe does.
async fn client_key_for_unsubscribe(
    state: &AppState,
    channel: RealtimeChannel,
    topic: &RealtimeTopic,
    subscriptions: &HashMap<String, ClientSubscription>,
) -> Result<String, String> {
    for (client_key, existing) in subscriptions {
        if existing.channel == channel
            && (topics_equal(&existing.request_topic, topic)
                || topics_equal(&existing.topic, topic))
        {
            return Ok(client_key.clone());
        }
    }

    state
        .realtime
        .prepare(channel, topic.clone())
        .await
        .map(|prepared| prepared.client_key)
        .map_err(|err| err.to_string())
}

fn topics_equal(left: &RealtimeTopic, right: &RealtimeTopic) -> bool {
    left.exchange == right.exchange && left.symbol == right.symbol && left.params == right.params
}

/// Serializes a borrowed top-N view of the owned shared book. The forwarder
/// never clones the full-depth vectors merely to truncate them per client.
fn orderbook_view(orderbook: &CcxtOrderBook, levels_limit: usize) -> WsOrderBookView<'_> {
    WsOrderBookView {
        asks: &orderbook.asks[..orderbook.asks.len().min(levels_limit)],
        bids: &orderbook.bids[..orderbook.bids.len().min(levels_limit)],
        datetime: orderbook.datetime.as_deref(),
        timestamp: orderbook.timestamp,
        nonce: orderbook.nonce,
        symbol: orderbook.symbol.as_deref(),
    }
}

/// One forwarder over the RAII receiver: it owns this viewer's demand, and
/// dropping it (abort/unsubscribe/disconnect) removes exactly this viewer.
fn spawn_realtime_forwarder(
    topic: RealtimeTopic,
    levels_limit: usize,
    mut receiver: RealtimeReceiver,
    outgoing_sender: Sender<Message>,
    close_signal: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(RealtimeUpdate::Trades(trades)) => {
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
                Ok(RealtimeUpdate::OrderBook(orderbook)) => {
                    if !send_ws_json(
                        &outgoing_sender,
                        &WsOrderBookUpdate {
                            message_type: "orderbook",
                            topic: &topic,
                            data: orderbook_view(&orderbook, levels_limit),
                        },
                    ) {
                        close_signal.notify_one();
                        return;
                    }
                }
                Ok(RealtimeUpdate::Ohlcv(candles)) => {
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
                // Internal ticker acquisition is projected by the revisioned
                // statistics forwarder, never emitted as a public realtime topic.
                Ok(RealtimeUpdate::Statistics(_)) => unreachable!("statistics demand is internal"),
                // Source lifecycle error: surface it as a WS error envelope and
                // never invent a successful update.
                Ok(RealtimeUpdate::Error(error)) => {
                    if !send_ws_error(&outgoing_sender, "UPSTREAM_ERROR", error.to_string()) {
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

            if channel_value == "marketstats" {
                if command
                    .symbol
                    .as_deref()
                    .is_some_and(|symbol| !symbol.is_empty())
                {
                    return Err("marketstats uses marketIds, not symbol".to_string());
                }
                let mut value = serde_json::json!({
                    "marketIds": command.market_ids, "fields": command.fields, "params": command.params,
                });
                if let Some(exchange) = command.exchange {
                    value["exchange"] = Value::String(exchange);
                }
                let request = serde_json::from_value(value)
                    .map_err(|error| format!("invalid statistics command: {error}"))?;
                return Ok(if op == "subscribe" {
                    ParsedStreamCommand::SubscribeMarketStats(request)
                } else {
                    ParsedStreamCommand::UnsubscribeMarketStats(request)
                });
            }

            let channel = if channel_value.is_empty() {
                RealtimeChannel::Trades
            } else if let Some(channel) = RealtimeChannel::from_client_value(&channel_value) {
                channel
            } else {
                return Err(format!(
                    "unsupported channel `{channel_value}`; supported channels: `trades`, `orderbook`, `ohlcv`, `marketstats`"
                ));
            };

            let topic = RealtimeTopic::from_client_request(
                command.exchange,
                command.symbol,
                command.params,
            )?;

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

fn send_stats_error(outgoing_sender: &Sender<Message>, error: ApiError) -> bool {
    let code = match &error {
        ApiError::Validation(_) => "INVALID_TOPIC",
        ApiError::UnsupportedExchange(_) => "UNSUPPORTED_EXCHANGE",
        ApiError::UnsupportedFeature(_) => "UNSUPPORTED_FEATURE",
        ApiError::Exchange(_) => "SUBSCRIBE_FAILED",
    };
    send_ws_error(outgoing_sender, code, error.to_string())
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
    use serde_json::json;

    use super::*;

    #[test]
    fn orderbook_view_serializes_borrowed_top_n_without_cloning() {
        let topic = RealtimeTopic::from_client_request(
            Some("binance".to_string()),
            Some("BTC/USDT:USDT".to_string()),
            json!({"levels": 21}),
        )
        .expect("topic should be valid");
        let source = Arc::new(CcxtOrderBook {
            asks: (1..=30).map(|level| (100.0 + level as f64, 1.0)).collect(),
            bids: (1..=30)
                .rev()
                .map(|level| (100.0 - level as f64, 2.0))
                .collect(),
            datetime: Some("2026-01-01T00:00:00.000Z".to_string()),
            timestamp: Some(1_700_000_000_000),
            nonce: Some(123),
            symbol: Some("BTC/USDT:USDT".to_string()),
        });

        let view = orderbook_view(&source, 21);
        assert_eq!(view.bids.len(), 21);
        assert_eq!(view.asks.len(), 21);

        let value = serde_json::to_value(WsOrderBookUpdate {
            message_type: "orderbook",
            topic: &topic,
            data: view,
        })
        .expect("update should serialize");
        assert_eq!(value["type"], "orderbook");
        assert_eq!(value["topic"]["exchange"], "binance");
        assert_eq!(value["data"]["bids"].as_array().unwrap().len(), 21);
        assert_eq!(value["data"]["asks"].as_array().unwrap().len(), 21);
        assert_eq!(value["data"]["nonce"], 123);
        assert_eq!(value["data"]["symbol"], "BTC/USDT:USDT");
        // The owned source book stays full-depth; only the view is truncated.
        assert_eq!(source.bids.len(), 30);
        assert_eq!(source.asks.len(), 30);
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
