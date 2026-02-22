use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::{
        ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use ferris_market_data_backend::{
    exchanges::registry::ExchangeRegistry,
    realtime::{OhlcvTopicManager, OrderBookTopicManager, TradesTopicManager},
    web::{self, AppState},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};

#[derive(Clone)]
struct MockUpstreamState {
    connection_count: Arc<AtomicUsize>,
    active_connections: Arc<AtomicUsize>,
    subscription_count: Arc<AtomicUsize>,
    outbound_trades: broadcast::Sender<String>,
}

async fn spawn_server(app: Router) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should expose address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("server should run");
    });

    (format!("127.0.0.1:{}", addr.port()), shutdown_sender, task)
}

async fn upstream_ws_handler(socket: WebSocket, state: MockUpstreamState) {
    state.connection_count.fetch_add(1, Ordering::SeqCst);
    state.active_connections.fetch_add(1, Ordering::SeqCst);

    let (mut sender, mut receiver) = socket.split();
    let mut outbound_trades = state.outbound_trades.subscribe();
    let mut subscribed_once = false;

    let writer = tokio::spawn(async move {
        while let Ok(payload) = outbound_trades.recv().await {
            if sender
                .send(AxumWsMessage::Text(payload.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(message) = receiver.next().await {
        let Ok(message) = message else {
            break;
        };

        match message {
            AxumWsMessage::Text(text) => {
                if is_hyperliquid_trade_subscribe(&text) && !subscribed_once {
                    subscribed_once = true;
                    state.subscription_count.fetch_add(1, Ordering::SeqCst);
                }
            }
            AxumWsMessage::Binary(binary) => {
                if let Ok(text) = String::from_utf8(binary.to_vec()) {
                    if is_hyperliquid_trade_subscribe(&text) && !subscribed_once {
                        subscribed_once = true;
                        state.subscription_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            AxumWsMessage::Ping(_) => {}
            AxumWsMessage::Close(_) => break,
            _ => {}
        }
    }

    writer.abort();
    state.active_connections.fetch_sub(1, Ordering::SeqCst);
}

async fn upstream_route(
    ws: WebSocketUpgrade,
    State(state): State<MockUpstreamState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| upstream_ws_handler(socket, state))
}

fn is_hyperliquid_trade_subscribe(payload: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return false;
    };

    value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method == "subscribe")
        && value
            .get("subscription")
            .and_then(|subscription| subscription.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|topic_type| topic_type == "trades")
}

async fn recv_message_of_type(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    expected_type: &str,
) -> Value {
    timeout(Duration::from_secs(3), async {
        loop {
            let message = stream
                .next()
                .await
                .expect("websocket should stay open")
                .expect("websocket frame should decode");

            let text = match message {
                TungsteniteMessage::Text(text) => text.to_string(),
                TungsteniteMessage::Binary(binary) => {
                    String::from_utf8(binary.to_vec()).expect("binary frame should be utf8")
                }
                TungsteniteMessage::Ping(payload) => {
                    stream
                        .send(TungsteniteMessage::Pong(payload))
                        .await
                        .expect("pong should send");
                    continue;
                }
                TungsteniteMessage::Pong(_) => continue,
                TungsteniteMessage::Close(_) => panic!("websocket closed before expected message"),
                _ => continue,
            };

            let value = serde_json::from_str::<Value>(&text).expect("ws payload should be JSON");
            if value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|value| value == expected_type)
            {
                return value;
            }
        }
    })
    .await
    .expect("timed out waiting for websocket message")
}

#[tokio::test]
async fn websocket_fanout_shares_upstream_for_same_topic() {
    let connection_count = Arc::new(AtomicUsize::new(0));
    let active_connections = Arc::new(AtomicUsize::new(0));
    let subscription_count = Arc::new(AtomicUsize::new(0));
    let (upstream_sender, _) = broadcast::channel::<String>(32);

    let upstream_state = MockUpstreamState {
        connection_count: connection_count.clone(),
        active_connections: active_connections.clone(),
        subscription_count: subscription_count.clone(),
        outbound_trades: upstream_sender.clone(),
    };

    let upstream_app = Router::new()
        .route("/ws", get(upstream_route))
        .with_state(upstream_state);
    let (upstream_bind, upstream_shutdown, upstream_task) = spawn_server(upstream_app).await;

    let topic_manager = TradesTopicManager::new(format!("http://{upstream_bind}"));
    let app_state = AppState::new(
        Arc::new(ExchangeRegistry::new()),
        topic_manager,
        OrderBookTopicManager::new(format!("http://{upstream_bind}")),
        OhlcvTopicManager::new(format!("http://{upstream_bind}")),
    );
    let app = Router::new()
        .route("/v1/ws", get(web::trades_stream_ws))
        .with_state(app_state);
    let (backend_bind, backend_shutdown, backend_task) = spawn_server(app).await;

    let backend_ws_url = format!("ws://{backend_bind}/v1/ws");
    let (mut client_a, _) = connect_async(&backend_ws_url)
        .await
        .expect("first client should connect");
    let (mut client_b, _) = connect_async(&backend_ws_url)
        .await
        .expect("second client should connect");

    let subscribe = json!({
        "op": "subscribe",
        "channel": "trades",
        "exchange": "hyperliquid",
        "symbol": "BTC/USDC:USDC",
        "params": {},
    });

    client_a
        .send(TungsteniteMessage::Text(subscribe.to_string().into()))
        .await
        .expect("first subscribe should send");
    client_b
        .send(TungsteniteMessage::Text(subscribe.to_string().into()))
        .await
        .expect("second subscribe should send");

    let _ = recv_message_of_type(&mut client_a, "subscribed").await;
    let _ = recv_message_of_type(&mut client_b, "subscribed").await;

    timeout(Duration::from_secs(3), async {
        loop {
            if subscription_count.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("backend should subscribe upstream");

    assert_eq!(connection_count.load(Ordering::SeqCst), 1);
    assert_eq!(active_connections.load(Ordering::SeqCst), 1);
    assert_eq!(subscription_count.load(Ordering::SeqCst), 1);

    let upstream_trade = json!({
        "channel": "trades",
        "data": [
            {
                "coin": "BTC",
                "side": "B",
                "px": "100.5",
                "sz": "0.25",
                "time": 1700000000000u64,
                "tid": 123456u64
            }
        ]
    });

    upstream_sender
        .send(upstream_trade.to_string())
        .expect("mock upstream should broadcast trade");

    let trades_a = recv_message_of_type(&mut client_a, "trades").await;
    let trades_b = recv_message_of_type(&mut client_b, "trades").await;

    assert_eq!(
        trades_a
            .pointer("/data/0/id")
            .and_then(Value::as_str)
            .expect("client A trade id should exist"),
        "123456"
    );
    assert_eq!(
        trades_b
            .pointer("/data/0/id")
            .and_then(Value::as_str)
            .expect("client B trade id should exist"),
        "123456"
    );

    assert_eq!(
        trades_a
            .pointer("/data/0/symbol")
            .and_then(Value::as_str)
            .expect("client A symbol should exist"),
        "BTC/USDC:USDC"
    );
    assert_eq!(
        trades_b
            .pointer("/data/0/symbol")
            .and_then(Value::as_str)
            .expect("client B symbol should exist"),
        "BTC/USDC:USDC"
    );

    let _ = client_a.close(None).await;
    let _ = client_b.close(None).await;

    let _ = backend_shutdown.send(());
    let _ = upstream_shutdown.send(());
    let _ = backend_task.await;
    let _ = upstream_task.await;
}
