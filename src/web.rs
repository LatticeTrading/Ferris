use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{
        broadcast,
        mpsc::{channel, error::TrySendError, Sender},
        Notify,
    },
    task::JoinHandle,
};
use tracing::warn;

use crate::{
    errors::ApiError,
    exchanges::registry::ExchangeRegistry,
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvRequest, FetchOrderBookRequest,
        FetchTradesRequest, HealthResponse,
    },
    realtime::{TradesTopic, TradesTopicManager},
};

#[derive(Clone)]
pub struct AppState {
    exchange_registry: Arc<ExchangeRegistry>,
    trades_topic_manager: TradesTopicManager,
}

impl AppState {
    pub fn new(
        exchange_registry: Arc<ExchangeRegistry>,
        trades_topic_manager: TradesTopicManager,
    ) -> Self {
        Self {
            exchange_registry,
            trades_topic_manager,
        }
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
    Subscribe(TradesTopic),
    Unsubscribe(TradesTopic),
    Ping,
}

struct ClientSubscription {
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

    for (key, subscription) in subscriptions {
        subscription.forward_task.abort();
        state.trades_topic_manager.unsubscribe_by_key(&key).await;
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
        ParsedStreamCommand::Subscribe(topic) => {
            let topic_key = match state.trades_topic_manager.resolve_topic_key(topic.clone()) {
                Ok(topic_key) => topic_key,
                Err(err) => return send_ws_error(outgoing_sender, "INVALID_TOPIC", err),
            };

            if let Some(existing) = subscriptions.get(&topic_key) {
                return send_ws_json(
                    outgoing_sender,
                    &WsAckMessage {
                        message_type: "alreadySubscribed",
                        op: "subscribe",
                        topic: &existing.topic,
                    },
                );
            }

            let subscription = match state.trades_topic_manager.subscribe(topic).await {
                Ok(subscription) => subscription,
                Err(err) => return send_ws_error(outgoing_sender, "SUBSCRIBE_FAILED", err),
            };

            let topic = subscription.topic.clone();
            let forward_task = spawn_topic_forwarder(
                topic.clone(),
                subscription.receiver,
                outgoing_sender.clone(),
                close_signal.clone(),
            );

            subscriptions.insert(
                subscription.key,
                ClientSubscription {
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
        ParsedStreamCommand::Unsubscribe(topic) => {
            let topic_key = match state.trades_topic_manager.resolve_topic_key(topic.clone()) {
                Ok(topic_key) => topic_key,
                Err(err) => return send_ws_error(outgoing_sender, "INVALID_TOPIC", err),
            };

            let Some(existing) = subscriptions.remove(&topic_key) else {
                return send_ws_error(
                    outgoing_sender,
                    "NOT_SUBSCRIBED",
                    "topic is not currently subscribed on this connection",
                );
            };

            existing.forward_task.abort();
            state
                .trades_topic_manager
                .unsubscribe_by_key(&topic_key)
                .await;

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

fn spawn_topic_forwarder(
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

fn parse_stream_command(payload: &str) -> Result<ParsedStreamCommand, String> {
    let command = serde_json::from_str::<ClientStreamCommand>(payload)
        .map_err(|err| format!("invalid JSON command: {err}"))?;

    let op = command.op.trim().to_ascii_lowercase();
    match op.as_str() {
        "ping" => Ok(ParsedStreamCommand::Ping),
        "subscribe" | "unsubscribe" => {
            let channel = command
                .channel
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if channel != "trades" {
                return Err(format!(
                    "unsupported channel `{channel}`; only `trades` is supported right now"
                ));
            }

            let topic =
                TradesTopic::from_client_request(command.exchange, command.symbol, command.params)?;

            if op == "subscribe" {
                Ok(ParsedStreamCommand::Subscribe(topic))
            } else {
                Ok(ParsedStreamCommand::Unsubscribe(topic))
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
            spawn_topic_forwarder(topic, receiver, outgoing_sender, close_signal.clone());

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
}
