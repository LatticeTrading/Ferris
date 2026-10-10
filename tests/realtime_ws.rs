//! Realtime streaming coverage for the CCXT-owned service.
//!
//! Each test drives the *stock* `ccxt-pro` Hyperliquid Pro handlers over a local
//! TCP fixture that answers the stock `/info` metadata endpoint and the Pro
//! websocket endpoint on the same listener (`http://host/base` ->
//! `ws://host/base/ws`). Nothing inside Ferris is stubbed: the observed frames
//! are the ones stock CCXT emits, and the assertions are about the delivered
//! `RealtimeUpdate` values plus the upstream subscribe/unsubscribe/connection
//! frames the fixture actually saw.
//!
//! Scope note: the caller accepted the known stock order-book continuity
//! defects across exchanges. These tests cover Ferris demand/generation
//! ownership, not Binance-style resynchronization correctness.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
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
    routing::{get, post},
    Json, Router,
};
use ferris_market_data_backend::{
    config::Config,
    exchanges::{ccxt::CcxtService, registry::ExchangeRegistry},
    models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade},
    realtime::{
        RealtimeChannel, RealtimeReceiver, RealtimeService, RealtimeSubscription, RealtimeTopic,
        RealtimeUpdate,
    },
    web::{self, AppState},
};
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as TungsteniteMessage, MaybeTlsStream, WebSocketStream,
};

const BTC: &str = "BTC/USDC:USDC";
const ETH: &str = "ETH/USDC:USDC";
const EXCHANGE: &str = "hyperliquid";
/// Bounded observation window for every fixture/owner interaction.
const WINDOW: Duration = Duration::from_secs(15);
/// Quiet window used to prove that no further (stale) update follows.
const QUIET: Duration = Duration::from_millis(300);

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Stock metadata fixture (/info)
// ---------------------------------------------------------------------------

fn swap_metadata() -> Value {
    json!([
        {"universe": [
            {"name": "BTC", "szDecimals": 5, "maxLeverage": 50, "onlyIsolated": false},
            {"name": "ETH", "szDecimals": 4, "maxLeverage": 50, "onlyIsolated": false}
        ]},
        [
            {"dayNtlVlm": "1", "funding": "0", "markPx": "100", "midPx": "100",
             "openInterest": "1", "oraclePx": "99", "premium": "0", "prevDayPx": "100"},
            {"dayNtlVlm": "1", "funding": "0", "markPx": "2000", "midPx": "2000",
             "openInterest": "1", "oraclePx": "1999", "premium": "0", "prevDayPx": "2000"}
        ]
    ])
}

fn spot_metadata() -> Value {
    json!([
        {
            "tokens": [
                {"name": "USDC", "szDecimals": 8, "weiDecimals": 8, "index": 0,
                 "tokenId": "0xusdc", "isCanonical": true},
                {"name": "PURR", "szDecimals": 1, "weiDecimals": 5, "index": 1,
                 "tokenId": "0xpurr", "isCanonical": true}
            ],
            "universe": [
                {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}
            ]
        },
        [
            {"dayNtlVlm": "8906.0", "markPx": "0.14", "midPx": "0.209265", "prevDayPx": "0.20432"}
        ]
    ])
}

// ---------------------------------------------------------------------------
// Upstream fixture
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counters {
    connections: usize,
    active: usize,
    closed: usize,
}

struct UpstreamState {
    frames: Mutex<Vec<Value>>,
    counters: Mutex<Counters>,
    revision: watch::Sender<u64>,
    outbound: broadcast::Sender<String>,
    close: broadcast::Sender<()>,
    withhold_unsubs: AtomicBool,
}

/// Test handle for the fixture: observes upstream frames and drives data.
#[derive(Clone)]
struct Upstream(Arc<UpstreamState>);

impl Upstream {
    fn start() -> Self {
        let (revision, _) = watch::channel(0u64);
        let (outbound, _) = broadcast::channel(256);
        let (close, _) = broadcast::channel(8);
        Self(Arc::new(UpstreamState {
            frames: Mutex::new(Vec::new()),
            counters: Mutex::new(Counters::default()),
            revision,
            outbound,
            close,
            withhold_unsubs: AtomicBool::new(false),
        }))
    }

    fn bump(&self) {
        self.0.revision.send_modify(|revision| *revision += 1);
    }

    fn counters(&self) -> Counters {
        *self.0.counters.lock()
    }

    fn frames(&self) -> Vec<Value> {
        self.0.frames.lock().clone()
    }

    fn subscribe_frames(&self, kind: &str, coin: &str) -> Vec<Value> {
        self.frames()
            .into_iter()
            .filter(|frame| {
                frame["method"] == "subscribe"
                    && frame["subscription"]["type"] == kind
                    && frame["subscription"]["coin"] == coin
            })
            .collect()
    }

    fn unsubscribe_frames(&self, kind: &str, coin: &str) -> Vec<Value> {
        self.frames()
            .into_iter()
            .filter(|frame| {
                frame["method"] == "unsubscribe"
                    && frame["subscription"]["type"] == kind
                    && frame["subscription"]["coin"] == coin
            })
            .collect()
    }

    fn withhold_unsubscribe_acks(&self) {
        self.0.withhold_unsubs.store(true, Ordering::SeqCst);
    }

    fn release_unsubscribe_acks(&self) {
        self.0.withhold_unsubs.store(false, Ordering::SeqCst);
    }

    /// Deliver the acknowledgement for an unsubscribe frame the fixture saw
    /// while acknowledgements were withheld.
    fn ack_unsubscribe(&self, kind: &str, coin: &str) {
        self.emit(json!({
            "channel": "subscriptionResponse",
            "data": {"method": "unsubscribe", "subscription": {"type": kind, "coin": coin}}
        }));
    }

    /// Close every currently open upstream connection (simulates an upstream drop).
    fn drop_connections(&self) {
        let _ = self.0.close.send(());
    }

    fn emit(&self, frame: Value) {
        let _ = self.0.outbound.send(frame.to_string());
    }

    fn emit_trade(&self, coin: &str, id: u64, price: f64, size: f64, time: u64) {
        self.emit(json!({
            "channel": "trades",
            "data": [{
                "coin": coin,
                "side": "B",
                "px": format!("{price}"),
                "sz": format!("{size}"),
                "time": time,
                "tid": id
            }]
        }));
    }

    fn emit_book(&self, coin: &str, time: u64, levels: usize) {
        let bids: Vec<Value> = (0..levels)
            .map(|index| {
                json!({
                    "px": format!("{}", 100.0 + index as f64),
                    "sz": format!("{}", 1.0 + index as f64),
                    "n": index + 1
                })
            })
            .collect();
        let asks: Vec<Value> = (0..levels)
            .map(|index| {
                json!({
                    "px": format!("{}", 200.0 + index as f64),
                    "sz": format!("{}", 2.0 + index as f64),
                    "n": index + 1
                })
            })
            .collect();
        self.emit(json!({
            "channel": "l2Book",
            "data": {"coin": coin, "time": time, "levels": [bids, asks]}
        }));
    }

    fn emit_candle(&self, coin: &str, time: u64, close: f64) {
        let open = close - 1.0;
        self.emit(json!({
            "channel": "candle",
            "data": {
                "t": time,
                "T": time + 59_999,
                "s": coin,
                "i": "1m",
                "o": format!("{open}"),
                "c": format!("{close}"),
                "h": format!("{}", close + 1.0),
                "l": format!("{}", open - 1.0),
                "v": "5",
                "n": 3
            }
        }));
    }

    fn note_frame(&self, frame: Value) {
        self.0.frames.lock().push(frame);
        self.bump();
    }

    fn set_counters(&self, update: impl FnOnce(&mut Counters)) {
        update(&mut self.0.counters.lock());
        self.bump();
    }

    fn on_frame(&self, text: &str) {
        let Ok(frame) = serde_json::from_str::<Value>(text) else {
            return;
        };
        self.note_frame(frame.clone());
        if frame["method"] == "unsubscribe" && !self.0.withhold_unsubs.load(Ordering::SeqCst) {
            self.emit(json!({
                "channel": "subscriptionResponse",
                "data": {"method": "unsubscribe", "subscription": frame["subscription"]}
            }));
        }
    }

    /// Bounded, channel-driven wait for an observable fixture condition.
    async fn wait_until(&self, label: &str, predicate: impl Fn(&Upstream) -> bool) {
        let mut revision = self.0.revision.subscribe();
        timeout(WINDOW, async {
            loop {
                if predicate(self) {
                    return;
                }
                revision.changed().await.expect("fixture stays alive");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
    }

    /// Wait until the owner stops emitting upstream control frames, so a
    /// re-subscribe churn has finished before new data is published.
    async fn wait_settled(&self) {
        let mut revision = self.0.revision.subscribe();
        while timeout(QUIET, revision.changed()).await.is_ok() {}
    }
}

async fn info_handler(State(upstream): State<Upstream>, Json(request): Json<Value>) -> Json<Value> {
    upstream.bump();
    match request["type"].as_str() {
        Some("spotMeta") => Json(spot_metadata()[0].clone()),
        Some("perpDexs") => Json(json!([null])),
        Some("metaAndAssetCtxs") => Json(swap_metadata()),
        Some("spotMetaAndAssetCtxs") => Json(spot_metadata()),
        other => panic!("unexpected upstream /info request: {other:?}"),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(upstream): State<Upstream>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve_socket(socket, upstream))
}

async fn serve_socket(socket: WebSocket, upstream: Upstream) {
    upstream.set_counters(|counters| {
        counters.connections += 1;
        counters.active += 1;
    });

    let (mut sender, mut receiver) = socket.split();
    let mut outbound = upstream.0.outbound.subscribe();
    let mut writer_close = upstream.0.close.subscribe();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = writer_close.recv() => break,
                payload = outbound.recv() => match payload {
                    Ok(payload) => {
                        if sender.send(AxumWsMessage::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                },
            }
        }
        let _ = sender.close().await;
    });

    let mut reader_close = upstream.0.close.subscribe();
    loop {
        tokio::select! {
            _ = reader_close.recv() => break,
            message = receiver.next() => match message {
                Some(Ok(AxumWsMessage::Text(text))) => upstream.on_frame(&text),
                Some(Ok(AxumWsMessage::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            },
        }
    }

    writer.abort();
    upstream.set_counters(|counters| {
        counters.active -= 1;
        counters.closed += 1;
    });
}

async fn spawn_server(app: Router) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fixture listener should bind");
    let addr = listener.local_addr().expect("fixture address");
    let (shutdown, receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receiver.await;
            })
            .await
            .expect("fixture server should run");
    });
    (format!("127.0.0.1:{}", addr.port()), shutdown, server)
}

// ---------------------------------------------------------------------------
// Service harness
// ---------------------------------------------------------------------------

fn config(base: &str) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        hyperliquid_base_url: base.to_string(),
        extended_rest_base_url: format!("{base}/api/v1"),
        extended_ws_url: "ws://127.0.0.1:1".into(),
        lighter_rest_base_url: "http://127.0.0.1:1".into(),
        lighter_ws_url: "ws://127.0.0.1:1".into(),
        binance_base_url: "http://127.0.0.1:1".into(),
        bybit_base_url: "http://127.0.0.1:1".into(),
        aster_base_url: "http://127.0.0.1:1".into(),
        apex_rest_base_url: "http://127.0.0.1:1/api".into(),
        apex_ws_url: "ws://127.0.0.1:1".into(),
        bitfinex_rest_base_url: "http://127.0.0.1:1".into(),
        bitfinex_ws_url: "ws://127.0.0.1:1".into(),
        kucoin_rest_base_url: "http://127.0.0.1:1".into(),
        kucoin_futures_rest_base_url: "http://127.0.0.1:1".into(),
        kucoin_ws_url: "ws://127.0.0.1:1".into(),
        kucoin_futures_ws_url: "ws://127.0.0.1:1".into(),
        nado_gateway_base_url: "http://127.0.0.1:1".into(),
        nado_archive_base_url: "http://127.0.0.1:1".into(),
        nado_ws_url: "ws://127.0.0.1:1".into(),
        request_timeout_ms: 5_000,
    }
}

fn topic(symbol: &str, params: Value) -> RealtimeTopic {
    RealtimeTopic {
        exchange: EXCHANGE.to_string(),
        symbol: symbol.to_string(),
        params,
    }
}

struct Harness {
    upstream: Upstream,
    realtime: RealtimeService,
    ccxt: CcxtService,
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<()>,
}

impl Harness {
    async fn start() -> Self {
        let upstream = Upstream::start();
        let app = Router::new()
            .route("/info", post(info_handler))
            .route("/ws", get(ws_handler))
            .with_state(upstream.clone());
        let (bind, shutdown, server) = spawn_server(app).await;
        let ccxt = CcxtService::start(&config(&format!("http://{bind}")))
            .expect("CCXT service should start");
        let realtime = RealtimeService::new(ccxt.clone());
        Self {
            upstream,
            realtime,
            ccxt,
            shutdown,
            server,
        }
    }

    async fn subscribe(
        &self,
        channel: RealtimeChannel,
        symbol: &str,
        params: Value,
    ) -> RealtimeSubscription {
        timeout(
            WINDOW,
            self.realtime.subscribe(channel, topic(symbol, params)),
        )
        .await
        .expect("subscribe should resolve within the window")
        .expect("subscribe should succeed")
    }

    async fn stop(self) {
        let _ = self.realtime.shutdown().await;
        let _ = self.ccxt.shutdown().await;
        let _ = self.shutdown.send(());
        let _ = self.server.await;
    }
}

// ---------------------------------------------------------------------------
// Update helpers
// ---------------------------------------------------------------------------

fn update_kind(update: &RealtimeUpdate) -> &'static str {
    match update {
        RealtimeUpdate::Trades(_) => "trades",
        RealtimeUpdate::OrderBook(_) => "orderbook",
        RealtimeUpdate::Ohlcv(_) => "ohlcv",
        RealtimeUpdate::Statistics(_) => "statistics",
        RealtimeUpdate::Error(_) => "error",
    }
}

async fn recv_update(receiver: &mut RealtimeReceiver) -> RealtimeUpdate {
    timeout(WINDOW, receiver.recv())
        .await
        .expect("update should arrive within the window")
        .expect("receiver should stay open")
}

/// Next order-book update. Transient source errors are skipped: they are a
/// legitimate part of a reconnect and are asserted separately.
async fn recv_order_book(receiver: &mut RealtimeReceiver) -> Arc<CcxtOrderBook> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::OrderBook(book) => return book,
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected an order book update, got {}", update_kind(&other)),
        }
    }
}

async fn recv_trades(receiver: &mut RealtimeReceiver) -> Arc<Vec<CcxtTrade>> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::Trades(trades) => return trades,
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected a trades update, got {}", update_kind(&other)),
        }
    }
}

fn trade_ids_of(trades: &[CcxtTrade]) -> Vec<u64> {
    trades
        .iter()
        .map(|trade| {
            trade
                .id
                .as_deref()
                .expect("stock trade id")
                .parse::<u64>()
                .expect("numeric stock trade id")
        })
        .collect()
}

async fn recv_candles(receiver: &mut RealtimeReceiver) -> Vec<CcxtOhlcv> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::Ohlcv(candles) => return candles.as_ref().clone(),
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected an ohlcv update, got {}", update_kind(&other)),
        }
    }
}

/// Collect order-book timestamps until `final_time` arrives, then prove silence.
async fn book_timestamps_until(receiver: &mut RealtimeReceiver, final_time: u64) -> Vec<u64> {
    let mut seen = Vec::new();
    while seen.last() != Some(&final_time) {
        seen.push(
            recv_order_book(receiver)
                .await
                .timestamp
                .expect("book timestamp"),
        );
    }
    seen.extend(drain_book_timestamps(receiver).await);
    seen
}

async fn drain_book_timestamps(receiver: &mut RealtimeReceiver) -> Vec<u64> {
    let mut extra = Vec::new();
    loop {
        match timeout(QUIET, receiver.recv()).await {
            Ok(Ok(RealtimeUpdate::OrderBook(book))) => {
                extra.push(book.timestamp.expect("book timestamp"))
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => break,
        }
    }
    extra
}

async fn drain_candle_times(receiver: &mut RealtimeReceiver) -> Vec<u64> {
    let mut extra = Vec::new();
    loop {
        match timeout(QUIET, receiver.recv()).await {
            Ok(Ok(RealtimeUpdate::Ohlcv(candles))) => {
                extra.extend(candles.iter().map(|candle| candle.0))
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => break,
        }
    }
    extra
}

// ---------------------------------------------------------------------------
// Websocket client helpers (/v1/ws)
// ---------------------------------------------------------------------------

async fn send_json(client: &mut Client, payload: Value) {
    client
        .send(TungsteniteMessage::Text(payload.to_string().into()))
        .await
        .expect("client command should send");
}

async fn next_json(client: &mut Client) -> Value {
    timeout(WINDOW, async {
        loop {
            match client.next().await {
                Some(Ok(TungsteniteMessage::Text(text))) => {
                    return serde_json::from_str::<Value>(&text).expect("ws payload should be JSON")
                }
                Some(Ok(TungsteniteMessage::Binary(bytes))) => {
                    return serde_json::from_slice::<Value>(&bytes)
                        .expect("ws payload should be JSON")
                }
                Some(Ok(TungsteniteMessage::Ping(payload))) => {
                    client
                        .send(TungsteniteMessage::Pong(payload))
                        .await
                        .expect("pong should send");
                }
                Some(Ok(TungsteniteMessage::Pong(_))) => {}
                Some(Ok(TungsteniteMessage::Close(_))) | None => {
                    panic!("client websocket closed before the expected message")
                }
                Some(Err(error)) => panic!("client websocket error: {error}"),
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .expect("client message within the window")
}

async fn next_type(client: &mut Client, expected: &str) -> Value {
    loop {
        let value = next_json(client).await;
        if value["type"] == expected {
            return value;
        }
        assert_ne!(
            value["type"], "error",
            "unexpected websocket error while waiting for {expected}: {value}"
        );
    }
}

async fn next_orderbook(client: &mut Client) -> Value {
    loop {
        let value = next_json(client).await;
        if value["type"] == "orderbook" {
            return value;
        }
        assert_ne!(
            value["type"], "error",
            "unexpected websocket error while waiting for an order book: {value}"
        );
    }
}

// ---------------------------------------------------------------------------
// 1. Book source sharing, warm continuity, and retired feeds
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn book_source_is_shared_across_display_depths_and_retired_feeds_do_not_leak() {
    let harness = Harness::start().await;
    let upstream = harness.upstream.clone();

    let mut shallow = harness
        .subscribe(RealtimeChannel::OrderBook, BTC, json!({"levels": 5}))
        .await;
    let mut deep = harness
        .subscribe(RealtimeChannel::OrderBook, BTC, json!({"levels": 20}))
        .await;

    assert_eq!(shallow.topic.symbol, BTC);
    assert_eq!(shallow.topic.params, json!({"levels": 5}));
    assert_eq!(shallow.levels_limit, 5);
    assert_eq!(deep.levels_limit, 20);
    assert_ne!(
        shallow.key, deep.key,
        "display depth must be part of the canonical client key"
    );

    // Both viewers attach to one upstream source for the same topic.
    upstream
        .wait_until("shared l2Book subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "BTC").len() == 1
        })
        .await;

    upstream.emit_book("BTC", 1_700_000_000_001, 20);
    let shallow_book = recv_order_book(&mut shallow.receiver).await;
    let deep_book = recv_order_book(&mut deep.receiver).await;
    assert_eq!(shallow_book.timestamp, Some(1_700_000_000_001));
    assert_eq!(deep_book.timestamp, Some(1_700_000_000_001));
    assert_eq!(deep_book.symbol.as_deref(), Some(BTC));
    assert_eq!(
        deep_book.bids.len(),
        20,
        "the deep viewer must receive every published level"
    );
    assert_eq!(deep_book.asks.len(), 20);
    assert!(
        (5..=20).contains(&shallow_book.bids.len()),
        "the shallow viewer must receive at least its display depth, got {}",
        shallow_book.bids.len()
    );
    assert_eq!(
        shallow_book.bids[..],
        deep_book.bids[..shallow_book.bids.len().min(deep_book.bids.len())],
        "both viewers must observe the same shared source book"
    );

    // A silent (cold) ETH feed joins, leaves with the acknowledgement withheld,
    // and is re-added while the BTC feed stays warm on the same URL.
    let cold = harness
        .subscribe(RealtimeChannel::OrderBook, ETH, json!({"levels": 5}))
        .await;
    upstream
        .wait_until("ETH subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "ETH").len() == 1
        })
        .await;

    upstream.withhold_unsubscribe_acks();
    drop(cold);
    upstream
        .wait_until("ETH unsubscribe frame", |upstream| {
            upstream.unsubscribe_frames("l2Book", "ETH").len() == 1
        })
        .await;

    // The warm BTC feed must not be disturbed by the retiring ETH feed.
    upstream.emit_book("BTC", 1_700_000_000_002, 20);
    assert_eq!(
        recv_order_book(&mut shallow.receiver).await.timestamp,
        Some(1_700_000_000_002)
    );
    assert_eq!(
        recv_order_book(&mut deep.receiver).await.timestamp,
        Some(1_700_000_000_002)
    );

    // Re-add while the unsubscribe acknowledgement is still pending. Nothing may
    // be published for the readded feed before stock has actually re-subscribed.
    let mut readded = harness
        .subscribe(RealtimeChannel::OrderBook, ETH, json!({"levels": 5}))
        .await;
    assert!(
        timeout(QUIET, readded.receiver.recv()).await.is_err(),
        "no book may be published for a readded feed before stock re-subscribes"
    );

    // Let the late acknowledgement arrive; the owner must observe it and
    // re-subscribe rather than letting the stale ack retire the readded feed.
    upstream.ack_unsubscribe("l2Book", "ETH");
    upstream.release_unsubscribe_acks();
    upstream
        .wait_until("readded ETH re-subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "ETH").len() >= 2
        })
        .await;
    upstream.wait_settled().await;

    upstream.emit_book("ETH", 1_700_000_000_007, 20);
    let eth_book = recv_order_book(&mut readded.receiver).await;
    assert_eq!(
        eth_book.timestamp,
        Some(1_700_000_000_007),
        "the readded feed must publish its own new generation after the late ack"
    );
    assert_eq!(eth_book.symbol.as_deref(), Some(ETH));

    upstream.emit_book("ETH", 1_700_000_000_008, 20);
    assert_eq!(
        recv_order_book(&mut readded.receiver).await.timestamp,
        Some(1_700_000_000_008),
        "the late unsubscribe acknowledgement must not retire the readded feed"
    );

    upstream.emit_book("BTC", 1_700_000_000_003, 20);
    assert_eq!(
        recv_order_book(&mut shallow.receiver).await.timestamp,
        Some(1_700_000_000_003)
    );
    assert_eq!(
        recv_order_book(&mut deep.receiver).await.timestamp,
        Some(1_700_000_000_003)
    );
    assert_eq!(
        upstream.subscribe_frames("l2Book", "BTC").len(),
        1,
        "the warm BTC source must never be re-subscribed for another viewer"
    );

    // Retire a feed that has already published, then re-add it: the new viewer
    // must receive the new generation only. Stock must have re-subscribed after
    // the unsubscribe acknowledgement before anything can be published.
    drop(readded);
    upstream
        .wait_until("second ETH unsubscribe frame", |upstream| {
            upstream.unsubscribe_frames("l2Book", "ETH").len() >= 2
        })
        .await;
    let mut replaced = harness
        .subscribe(RealtimeChannel::OrderBook, ETH, json!({"levels": 5}))
        .await;
    upstream
        .wait_until("replaced ETH re-subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "ETH").len() >= 3
        })
        .await;
    upstream.wait_settled().await;
    upstream.emit_book("ETH", 1_700_000_000_009, 20);
    assert_eq!(
        recv_order_book(&mut replaced.receiver).await.timestamp,
        Some(1_700_000_000_009),
        "a retired generation's book must not be replayed to the new viewer"
    );

    // Boundary: retire and immediately re-add ETH with no synchronization at
    // all (the unsubscribe frame may not even have been sent yet) while BTC
    // stays warm. The readded viewer must still receive its own new generation.
    drop(replaced);
    let mut immediate = harness
        .subscribe(RealtimeChannel::OrderBook, ETH, json!({"levels": 5}))
        .await;
    upstream.emit_book("ETH", 1_700_000_000_010, 20);
    upstream.emit_book("ETH", 1_700_000_000_011, 20);
    loop {
        let timestamp = recv_order_book(&mut immediate.receiver).await.timestamp;
        assert!(
            timestamp.is_some_and(|time| (1_700_000_000_010..=1_700_000_000_011).contains(&time)),
            "the readded feed published an unexpected generation: {timestamp:?}"
        );
        if timestamp == Some(1_700_000_000_011) {
            break;
        }
    }

    // The warm BTC feed is still untouched by the whole ETH churn.
    upstream.emit_book("BTC", 1_700_000_000_004, 20);
    assert_eq!(
        recv_order_book(&mut shallow.receiver).await.timestamp,
        Some(1_700_000_000_004)
    );
    assert_eq!(
        recv_order_book(&mut deep.receiver).await.timestamp,
        Some(1_700_000_000_004)
    );

    assert_eq!(upstream.counters().connections, 1);

    drop(shallow);
    drop(deep);
    drop(immediate);
    harness.stop().await;
}

// ---------------------------------------------------------------------------
// 2. Mixed channels on one URL
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_channels_share_one_url_and_deliver_final_events_without_replay() {
    let harness = Harness::start().await;
    let upstream = harness.upstream.clone();

    let mut trades = harness
        .subscribe(RealtimeChannel::Trades, BTC, json!({}))
        .await;
    let mut book = harness
        .subscribe(RealtimeChannel::OrderBook, BTC, json!({"levels": 20}))
        .await;
    let mut candles = harness
        .subscribe(RealtimeChannel::Ohlcv, BTC, json!({"timeframe": "1m"}))
        .await;

    upstream
        .wait_until("all three channel subscriptions", |upstream| {
            upstream.subscribe_frames("trades", "BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "BTC").len() == 1
                && upstream.subscribe_frames("candle", "BTC").len() == 1
        })
        .await;
    assert_eq!(
        upstream.counters().connections,
        1,
        "all channels of one venue share a single upstream connection"
    );
    assert_eq!(trades.topic.symbol, BTC);
    assert_eq!(candles.topic.params, json!({"timeframe": "1m"}));

    // Settle every feed, then send a finite burst and go silent.
    upstream.emit_trade("BTC", 100, 100.5, 0.25, 1_700_000_000_000);
    upstream.emit_book("BTC", 1_700_000_000_001, 20);
    upstream.emit_candle("BTC", 1_700_000_000_000, 1_000.0);

    let settled_trades = recv_trades(&mut trades.receiver).await;
    assert_eq!(settled_trades.len(), 1);
    let settled_trade = &settled_trades[0];
    assert_eq!(settled_trade.id.as_deref(), Some("100"));
    assert_eq!(settled_trade.symbol.as_deref(), Some(BTC));
    assert_eq!(settled_trade.side.as_deref(), Some("buy"));
    assert_eq!(settled_trade.price, Some(100.5));
    assert_eq!(settled_trade.amount, Some(0.25));
    assert_eq!(settled_trade.cost, Some(25.125));
    assert_eq!(settled_trade.timestamp, Some(1_700_000_000_000));
    assert_eq!(
        settled_trade.datetime.as_deref(),
        Some("2023-11-14T22:13:20.000Z")
    );
    assert_eq!(
        recv_order_book(&mut book.receiver).await.timestamp,
        Some(1_700_000_000_001)
    );
    let settled = recv_candles(&mut candles.receiver).await;
    assert_eq!(
        settled,
        vec![(1_700_000_000_000, 999.0, 1_001.0, 998.0, 1_000.0, Some(5.0))]
    );

    upstream.emit_trade("BTC", 101, 100.5, 0.25, 1_700_000_000_001);
    upstream.emit_trade("BTC", 102, 100.5, 0.25, 1_700_000_000_002);
    upstream.emit_trade("BTC", 103, 100.5, 0.25, 1_700_000_000_003);
    upstream.emit_book("BTC", 1_700_000_000_002, 20);
    upstream.emit_book("BTC", 1_700_000_000_003, 20);
    upstream.emit_book("BTC", 1_700_000_000_004, 20);
    upstream.emit_candle("BTC", 1_700_000_000_002, 2_000.0);
    upstream.emit_candle("BTC", 1_700_000_000_003, 3_000.0);
    upstream.emit_candle("BTC", 1_700_000_000_004, 4_000.0);

    let mut trade_ids = Vec::new();
    while !trade_ids.contains(&103) {
        trade_ids.extend(trade_ids_of(&recv_trades(&mut trades.receiver).await));
    }
    assert_eq!(
        trade_ids,
        vec![101, 102, 103],
        "each later trade is delivered exactly once: no loss and no cache replay"
    );

    let book_times = book_timestamps_until(&mut book.receiver, 1_700_000_000_004).await;
    assert_eq!(
        book_times.last(),
        Some(&1_700_000_000_004),
        "the final book event of the burst must be delivered"
    );
    assert!(
        book_times.windows(2).all(|pair| pair[0] <= pair[1]),
        "book timestamps must never regress: {book_times:?}"
    );
    assert_eq!(
        book_times,
        vec![1_700_000_000_002, 1_700_000_000_003, 1_700_000_000_004]
    );

    // Candles may legitimately be re-published for the same interval while the
    // burst is live; what must hold is forward progress to the final candle and
    // no replay of an already-consumed candle.
    let mut candle_times: Vec<u64> = Vec::new();
    while candle_times.last() != Some(&1_700_000_000_004) {
        let batch = recv_candles(&mut candles.receiver).await;
        for candle in &batch {
            assert!(
                candle.0 >= 1_700_000_000_002,
                "a pre-burst candle was replayed: {batch:?}"
            );
            assert!(
                candle.0 <= 1_700_000_000_004,
                "a candle was delivered before it was published: {batch:?}"
            );
            if candle_times.last() != Some(&candle.0) {
                candle_times.push(candle.0);
            }
        }
    }
    assert_eq!(
        candle_times,
        vec![1_700_000_000_002, 1_700_000_000_003, 1_700_000_000_004],
        "the final candle event of the burst must be delivered after the earlier ones"
    );
    let extra = drain_candle_times(&mut candles.receiver).await;
    assert!(
        extra.iter().all(|time| *time == 1_700_000_000_004),
        "no candle other than the final event may follow it: {extra:?}"
    );

    drop(trades);
    drop(book);
    drop(candles);
    harness.stop().await;
}

// ---------------------------------------------------------------------------
// 3. Reconnect, retirement, and shared shutdown
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_resets_the_snapshot_and_the_final_viewer_retires_the_connection() {
    let harness = Harness::start().await;
    let upstream = harness.upstream.clone();

    let mut book = harness
        .subscribe(RealtimeChannel::OrderBook, BTC, json!({"levels": 20}))
        .await;
    upstream
        .wait_until("initial l2Book subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "BTC").len() == 1
        })
        .await;
    upstream.emit_book("BTC", 1_700_000_000_001, 20);
    assert_eq!(
        recv_order_book(&mut book.receiver).await.timestamp,
        Some(1_700_000_000_001)
    );

    // Drop the upstream socket: the owner must reconnect, re-subscribe, and
    // publish a fresh snapshot instead of the retired one. The observation
    // window is opened before the drop so no reconnect step can be missed.
    let mut counts = upstream.0.revision.subscribe();
    let observed = timeout(WINDOW, async {
        let mut connections = upstream.counters().connections;
        let mut closed = upstream.counters().closed;
        upstream.drop_connections();
        loop {
            let counters = upstream.counters();
            if counters.closed > closed {
                closed = counters.closed;
                assert!(
                    connections <= 2,
                    "the owner must not dial more than one replacement connection"
                );
            }
            if counters.connections > connections {
                connections = counters.connections;
                assert_eq!(
                    connections, 2,
                    "the owner must reconnect exactly once for one upstream drop"
                );
            }
            if counters.connections == 2
                && counters.active == 1
                && counters.closed >= 1
                && upstream.subscribe_frames("l2Book", "BTC").len() == 2
            {
                return;
            }
            counts.changed().await.expect("fixture stays alive");
        }
    })
    .await;
    observed.expect("the owner should reconnect and re-subscribe within the window");

    upstream.emit_book("BTC", 1_700_000_000_009, 20);
    let snapshot = recv_order_book(&mut book.receiver).await;
    assert_eq!(
        snapshot.timestamp,
        Some(1_700_000_000_009),
        "the reconnected generation must publish its own snapshot"
    );
    let leaked = drain_book_timestamps(&mut book.receiver).await;
    assert!(
        leaked.is_empty(),
        "stale snapshots leaked after the reconnect: {leaked:?}"
    );
    assert_eq!(upstream.counters().closed, 1);

    // The final viewer retiring closes the dedicated upstream connection and
    // re-adding opens a fresh one.
    drop(book);
    upstream
        .wait_until("retired connection", |upstream| {
            upstream.counters().active == 0
        })
        .await;
    assert_eq!(
        upstream.counters().connections,
        2,
        "retiring the last viewer must not dial a new upstream connection"
    );

    let mut again = harness
        .subscribe(RealtimeChannel::OrderBook, BTC, json!({"levels": 20}))
        .await;
    upstream
        .wait_until("fresh upstream connection", |upstream| {
            let counters = upstream.counters();
            counters.connections == 3 && counters.active == 1
        })
        .await;
    upstream
        .wait_until("fresh l2Book subscribe", |upstream| {
            upstream.subscribe_frames("l2Book", "BTC").len() == 3
        })
        .await;
    upstream.emit_book("BTC", 1_700_000_000_011, 20);
    assert_eq!(
        recv_order_book(&mut again.receiver).await.timestamp,
        Some(1_700_000_000_011)
    );

    // Shared service shutdown closes the upstream and every live receiver.
    harness
        .realtime
        .shutdown()
        .await
        .expect("shutdown should succeed");
    upstream
        .wait_until("upstream closed by shutdown", |upstream| {
            upstream.counters().active == 0
        })
        .await;
    let closed = timeout(WINDOW, again.receiver.recv())
        .await
        .expect("receiver must resolve on shutdown");
    assert!(
        matches!(
            closed,
            Err(tokio::sync::broadcast::error::RecvError::Closed)
        ),
        "shutdown must close every receiver"
    );

    drop(again);
    harness.stop().await;
}

// ---------------------------------------------------------------------------
// 4. Client protocol over /v1/ws
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_protocol_delivers_acks_depth_views_errors_and_shutdown() {
    let harness = Harness::start().await;
    let upstream = harness.upstream.clone();
    let state = AppState::new(Arc::new(ExchangeRegistry::new()), harness.realtime.clone());
    let state_shutdown = state.clone();
    let app = Router::new()
        .route("/v1/ws", get(web::trades_stream_ws))
        .with_state(state);
    let (bind, shutdown, server) = spawn_server(app).await;

    let (mut client, _) = connect_async(format!("ws://{bind}/v1/ws"))
        .await
        .expect("client should connect");

    let subscribe = |symbol: &str, params: Value| {
        json!({
            "op": "subscribe",
            "channel": "orderbook",
            "exchange": EXCHANGE,
            "symbol": symbol,
            "params": params,
        })
    };

    send_json(&mut client, subscribe(BTC, json!({"levels": 5}))).await;
    let ack = next_type(&mut client, "subscribed").await;
    assert_eq!(ack["op"], "subscribe");
    assert_eq!(ack["topic"]["exchange"], EXCHANGE);
    assert_eq!(ack["topic"]["symbol"], BTC);
    assert_eq!(ack["topic"]["params"], json!({"levels": 5}));

    send_json(&mut client, subscribe(BTC, json!({"levels": 20}))).await;
    let deep_ack = next_type(&mut client, "subscribed").await;
    assert_eq!(deep_ack["topic"]["params"], json!({"levels": 20}));

    // A duplicate of the exact same display depth is acknowledged, not resubscribed.
    send_json(&mut client, subscribe(BTC, json!({"levels": 5}))).await;
    let duplicate = next_type(&mut client, "alreadySubscribed").await;
    assert_eq!(duplicate["topic"]["params"], json!({"levels": 5}));

    upstream
        .wait_until("single shared upstream subscription", |upstream| {
            upstream.subscribe_frames("l2Book", "BTC").len() == 1
        })
        .await;

    send_json(&mut client, json!({"op": "ping"})).await;
    next_type(&mut client, "pong").await;

    // Canonicalization/validation failures keep their documented codes.
    send_json(
        &mut client,
        subscribe("NOT-A-MARKET/USDC:USDC", json!({"levels": 5})),
    )
    .await;
    let invalid_topic = next_type(&mut client, "error").await;
    assert_eq!(invalid_topic["code"], "INVALID_TOPIC");

    send_json(&mut client, subscribe(BTC, json!({"levels": 0}))).await;
    let failed = next_type(&mut client, "error").await;
    assert_eq!(
        failed["code"], "INVALID_TOPIC",
        "prepare failures use INVALID_TOPIC: {failed}"
    );

    send_json(
        &mut client,
        json!({
            "op": "unsubscribe",
            "channel": "orderbook",
            "exchange": EXCHANGE,
            "symbol": ETH,
            "params": {"levels": 5},
        }),
    )
    .await;
    let not_subscribed = next_type(&mut client, "error").await;
    assert_eq!(not_subscribed["code"], "NOT_SUBSCRIBED");

    // One shared source, two display depths, exact per-viewer truncation.
    upstream.emit_book("BTC", 1_700_000_000_500, 20);
    let first = next_orderbook(&mut client).await;
    let second = next_orderbook(&mut client).await;
    let mut views = [first, second];
    views.sort_by_key(|view| view["data"]["bids"].as_array().expect("bids").len());
    assert_eq!(views[0]["data"]["bids"].as_array().expect("bids").len(), 5);
    assert_eq!(views[0]["data"]["asks"].as_array().expect("asks").len(), 5);
    assert_eq!(views[1]["data"]["bids"].as_array().expect("bids").len(), 20);
    assert_eq!(views[1]["data"]["asks"].as_array().expect("asks").len(), 20);
    assert_eq!(views[0]["data"]["timestamp"], views[1]["data"]["timestamp"]);
    assert_eq!(views[0]["data"]["symbol"], BTC);

    // Unsubscribing the shallow view leaves the deep view streaming.
    send_json(&mut client, subscribe(BTC, json!({"levels": 5}))).await;
    let duplicate = next_type(&mut client, "alreadySubscribed").await;
    assert_eq!(duplicate["topic"]["params"], json!({"levels": 5}));
    send_json(
        &mut client,
        json!({
            "op": "unsubscribe",
            "channel": "orderbook",
            "exchange": EXCHANGE,
            "symbol": BTC,
            "params": {"levels": 5},
        }),
    )
    .await;
    let unsubscribed = next_type(&mut client, "unsubscribed").await;
    assert_eq!(unsubscribed["topic"]["params"], json!({"levels": 5}));

    upstream.emit_book("BTC", 1_700_000_000_501, 20);
    let remaining = next_orderbook(&mut client).await;
    assert_eq!(
        remaining["data"]["bids"].as_array().expect("bids").len(),
        20
    );
    assert_eq!(remaining["data"]["timestamp"], json!(1_700_000_000_501u64));

    // Shared shutdown closes the upstream connection and every live receiver.
    let mut direct = harness
        .subscribe(RealtimeChannel::Trades, ETH, json!({}))
        .await;
    upstream
        .wait_until("direct ETH trades subscribe", |upstream| {
            upstream.subscribe_frames("trades", "ETH").len() == 1
        })
        .await;

    harness
        .realtime
        .shutdown()
        .await
        .expect("shutdown should succeed");
    upstream
        .wait_until("upstream closed by shutdown", |upstream| {
            upstream.counters().active == 0
        })
        .await;
    let closed = timeout(WINDOW, direct.receiver.recv())
        .await
        .expect("receiver must resolve on shutdown");
    assert!(
        matches!(
            closed,
            Err(tokio::sync::broadcast::error::RecvError::Closed)
        ),
        "shutdown must close every receiver"
    );

    // No order book frame may be produced after the service is gone, and the
    // client socket stays open and usable (the handler never signals a close).
    let mut open = true;
    let mut after_shutdown = Vec::new();
    loop {
        match timeout(QUIET, client.next()).await {
            Ok(Some(Ok(TungsteniteMessage::Text(text)))) => {
                after_shutdown
                    .push(serde_json::from_str::<Value>(&text).expect("ws payload should be JSON"));
            }
            Ok(Some(Ok(TungsteniteMessage::Ping(payload)))) => {
                let _ = client.send(TungsteniteMessage::Pong(payload)).await;
            }
            Ok(Some(Ok(TungsteniteMessage::Close(_)))) | Ok(None) => {
                open = false;
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Err(_) => break,
        }
    }
    assert!(
        after_shutdown
            .iter()
            .all(|frame| frame["type"] != "orderbook"),
        "no order book may be published after shutdown: {after_shutdown:?}"
    );
    assert!(
        open,
        "the shared-service shutdown must not close the client websocket"
    );
    send_json(&mut client, json!({"op": "ping"})).await;
    next_type(&mut client, "pong").await;

    let _ = client.close(None).await;
    drop(client);
    state_shutdown.shutdown_market_stats().await;
    let _ = shutdown.send(());
    let _ = server.await;
    harness.stop().await;
}

#[derive(Clone)]
struct ExtendedStreams {
    frames: broadcast::Sender<(String, Value)>,
    connected: watch::Sender<Vec<String>>,
}

async fn extended_stream(
    ws: WebSocketUpgrade,
    State(streams): State<ExtendedStreams>,
    uri: axum::http::Uri,
) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let path = uri.path().to_string();
        let mut frames = streams.frames.subscribe();
        streams.connected.send_modify(|paths| paths.push(path.clone()));
        loop {
            tokio::select! {
                frame = frames.recv() => match frame {
                    Ok((target, frame)) if target == path => {
                        if socket.send(AxumWsMessage::Text(frame.to_string())).await.is_err() { break; }
                    },
                    Ok(_) => {},
                    Err(_) => break,
                },
                frame = socket.recv() => match frame {
                    Some(Ok(AxumWsMessage::Close(_))) | Some(Err(_)) | None => break,
                    _ => {},
                },
            }
        }
        streams.connected.send_modify(|paths| paths.retain(|value| value != &path));
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_delivers_trades_and_separate_price_candle_caches() {
    let (frames, _) = broadcast::channel(16);
    let (connected, mut connections) = watch::channel(Vec::new());
    let streams = ExtendedStreams { frames, connected };
    let app = Router::new()
        .route("/api/v1/info/assets", get(|| async { Json(json!({"status":"OK","data":[
            {"id":1,"name":"USD","symbol":"USD","precision":6,"isActive":true,"type":"SPOT"}
        ]})) }))
        .route("/api/v1/info/markets", get(|| async { Json(json!({"status":"OK","data":[{
            "name":"BTC-USD","assetName":"BTC","collateralAssetName":"USD","status":"ACTIVE","active":true,
            "tradingConfig":{"minOrderSize":"0.0001","minOrderSizeChange":"0.00001","minPriceChange":"1"}
        }]})) }))
        .route("/stream/orderbooks/:market", get(extended_stream))
        .route("/stream/publicTrades/:market", get(extended_stream))
        .route("/stream/candles/:market/:kind", get(extended_stream))
        .with_state(streams.clone());
    let (bind, shutdown, server) = spawn_server(app).await;
    let mut config = config(&format!("http://{bind}"));
    config.extended_ws_url = format!("ws://{bind}/stream");
    let ccxt = CcxtService::start(&config).unwrap();
    let realtime = RealtimeService::new(ccxt.clone());
    let subscribe = |channel, params| {
        realtime.subscribe(
            channel,
            RealtimeTopic {
                exchange: "extended".into(),
                symbol: "BTC-USD".into(),
                params,
            },
        )
    };
    let mut trades = subscribe(RealtimeChannel::Trades, json!({})).await.unwrap();
    let mut book = subscribe(RealtimeChannel::OrderBook, json!({"levels":1}))
        .await
        .unwrap();
    let mut candles = subscribe(RealtimeChannel::Ohlcv, json!({"timeframe":"1m"}))
        .await
        .unwrap();
    let mut mark = subscribe(
        RealtimeChannel::Ohlcv,
        json!({"timeframe":"1m","candleType":"mark-prices"}),
    )
    .await
    .unwrap();
    timeout(WINDOW, connections.wait_for(|paths| paths.len() == 4))
        .await
        .unwrap()
        .unwrap();
    for sequence in [1, 2] {
        let time = 1_700_000_000_000u64 + sequence;
        streams.frames.send(("/stream/publicTrades/BTC-USD".into(), json!({"ts":time,"seq":sequence,
            "data":[{"m":"BTC-USD","S":"BUY","tT":"TRADE","T":time,"p":"60000","q":"0.2","i":sequence}]}))).unwrap();
        streams.frames.send(("/stream/orderbooks/BTC-USD".into(), json!({"ts":time,"seq":sequence,
            "type":if sequence == 1 {"SNAPSHOT"} else {"DELTA"},
            "data":{"m":"BTC-USD","b":[{"p":"60000","q":sequence.to_string()}],"a":[{"p":"60001","q":"3"}]}}))).unwrap();
        streams.frames.send(("/stream/candles/BTC-USD/trades".into(), json!({"ts":time,"seq":sequence,
            "data":[{"T":1_700_000_000_000u64,"o":"60000","h":"60002","l":"59999","c":(60000+sequence).to_string(),"v":"3"}]}))).unwrap();
        streams.frames.send(("/stream/candles/BTC-USD/mark-prices".into(), json!({"ts":time,"seq":sequence,
            "data":[{"T":1_700_000_000_000u64,"o":"59000","h":"59002","l":"58999","c":(59000+sequence).to_string()}]}))).unwrap();
        let rows = recv_trades(&mut trades.receiver).await;
        assert_eq!(trade_ids_of(&rows), [sequence]);
        assert_eq!(rows[0].symbol.as_deref(), Some(BTC));
        assert_eq!(rows[0].price, Some(60_000.0));
        let snapshot = recv_order_book(&mut book.receiver).await;
        assert_eq!(snapshot.bids, [(60_000.0, sequence as f64)]);
        assert_eq!(snapshot.nonce, Some(sequence));
        assert_eq!(
            serde_json::to_value(recv_candles(&mut candles.receiver).await).unwrap(),
            json!([[
                1_700_000_000_000u64,
                60000.0,
                60002.0,
                59999.0,
                (60000 + sequence) as f64,
                3.0
            ]])
        );
        assert_eq!(
            serde_json::to_value(recv_candles(&mut mark.receiver).await).unwrap(),
            json!([[
                1_700_000_000_000u64,
                59000.0,
                59002.0,
                58999.0,
                (59000 + sequence) as f64,
                null
            ]])
        );
    }
    assert!(
        timeout(QUIET, trades.receiver.recv()).await.is_err(),
        "trade cache must not replay during silence"
    );
    drop((trades, book, candles, mark));
    realtime.shutdown().await.unwrap();
    timeout(WINDOW, connections.wait_for(Vec::is_empty))
        .await
        .unwrap()
        .unwrap();
    ccxt.shutdown().await.unwrap();
    let _ = shutdown.send(());
    server.await.unwrap();
}
