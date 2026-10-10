//! Hyperliquid HIP3 market-data coverage over a real loopback upstream.
//!
//! The fixture answers the stock `/info` metadata endpoint and the stock
//! websocket endpoint from one listener (`http://host` -> `ws://host/ws`), and
//! drives the *stock* `ccxt-pro` Hyperliquid Pro handlers. Nothing inside
//! Ferris is stubbed: assertions are on delivered `RealtimeUpdate` values, the
//! upstream subscribe/unsubscribe frames the fixture actually saw, and the
//! shared connection lifecycle.
//!
//! The catalog is exercised across two builder-deployed dexes that both list
//! the same underlying asset (`BTC`): `xyz` settles in USDC and `abc` settles
//! in USDH. The USDH dex is the one that exposes a stock mapping defect: stock
//! resolves an incoming websocket `coin` back to a unified symbol through
//! `options.hip3TokensByName`, and a live core that only had markets seeded
//! (no dex metadata) falls back to the default USDC quote, so `abc:BTC` frames
//! can never reach the market they belong to.

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::{
    extract::{
        ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use ferris_market_data_backend::{
    config::Config,
    exchanges::{
        ccxt::{CcxtExchange, CcxtService, Venue},
        registry::ExchangeRegistry,
    },
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
    sync::{broadcast, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    connect_async, tungstenite::Message as TungsteniteMessage, MaybeTlsStream, WebSocketStream,
};

const EXCHANGE: &str = "hyperliquid";
/// Bounded observation window for every fixture/owner interaction.
const WINDOW: Duration = Duration::from_secs(15);
/// Quiet window used to prove that no further (stale) update follows.
const QUIET: Duration = Duration::from_millis(300);
const TIME: u64 = 1_700_000_000_000;

/// Primary dex perpetual (empty dex selector).
const BTC: &str = "BTC/USDC:USDC";
/// `xyz` builder dex, USDC collateral.
const XYZ: &str = "XYZ-BTC/USDC:USDC";
/// `abc` builder dex, USDH collateral.
const ABC: &str = "ABC-BTC/USDH:USDH";

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Stock metadata fixture (/info)
// ---------------------------------------------------------------------------

fn spot_meta() -> Value {
    json!({
        "tokens": [
            {"name": "USDC", "szDecimals": 8, "weiDecimals": 8, "index": 0,
             "tokenId": "0xusdc", "isCanonical": true},
            {"name": "USDH", "szDecimals": 8, "weiDecimals": 8, "index": 1,
             "tokenId": "0xusdh", "isCanonical": true},
            {"name": "PURR", "szDecimals": 1, "weiDecimals": 5, "index": 2,
             "tokenId": "0xpurr", "isCanonical": true}
        ],
        "universe": [
            {"name": "PURR/USDC", "tokens": [2, 0], "index": 0, "isCanonical": true}
        ]
    })
}

fn context(mark: &str) -> Value {
    json!({
        "dayNtlVlm": "1000.0",
        "funding": "0.00001",
        "impactPxs": [mark, mark],
        "markPx": mark,
        "midPx": mark,
        "openInterest": "10.0",
        "oraclePx": mark,
        "premium": "0.0",
        "prevDayPx": mark
    })
}

/// Primary dex `metaAndAssetCtxs`.
fn swap_meta() -> Value {
    json!([
        {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 50, "onlyIsolated": false}]},
        [context("100")]
    ])
}

fn perp_dex(name: &str) -> Value {
    json!({
        "name": name,
        "fullName": name.to_uppercase(),
        "deployer": "0x0000000000000000000000000000000000000001",
        "oracleUpdater": "0x0000000000000000000000000000000000000002"
    })
}

/// Builder-dex `metaAndAssetCtxs`: `collateralToken` is the spot token index,
/// so `0` is USDC and `1` is USDH.
fn hip3_meta(name: &str, collateral: u64, mark: &str) -> Value {
    json!([
        {
            "universe": [{"name": name, "szDecimals": 5, "maxLeverage": 10, "onlyIsolated": false}],
            "collateralToken": collateral
        },
        [context(mark)]
    ])
}

// ---------------------------------------------------------------------------
// Upstream fixture (HTTP + websocket on one listener)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counters {
    connections: usize,
    active: usize,
    closed: usize,
}

struct UpstreamState {
    info: Mutex<Vec<Value>>,
    frames: Mutex<Vec<Value>>,
    counters: watch::Sender<Counters>,
    revision: watch::Sender<u64>,
    outbound: broadcast::Sender<Value>,
    close: broadcast::Sender<()>,
}

/// Test handle for the fixture: observes upstream traffic and drives data.
#[derive(Clone)]
struct Upstream(Arc<UpstreamState>);

impl Upstream {
    fn start() -> Self {
        let (counters, _) = watch::channel(Counters::default());
        let (revision, _) = watch::channel(0u64);
        let (outbound, _) = broadcast::channel(256);
        let (close, _) = broadcast::channel(8);
        Self(Arc::new(UpstreamState {
            info: Mutex::new(Vec::new()),
            frames: Mutex::new(Vec::new()),
            counters,
            revision,
            outbound,
            close,
        }))
    }

    fn bump(&self) {
        self.0.revision.send_modify(|revision| *revision += 1);
    }

    fn counters(&self) -> Counters {
        *self.0.counters.borrow()
    }

    fn info_requests(&self) -> Vec<Value> {
        self.0.info.lock().clone()
    }

    fn frames(&self) -> Vec<Value> {
        self.0.frames.lock().clone()
    }

    fn note_info(&self, request: Value) {
        self.0.info.lock().push(request);
        self.bump();
    }

    fn note_frame(&self, frame: Value) {
        self.0.frames.lock().push(frame);
        self.bump();
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

    fn set_counters(&self, update: impl FnOnce(&mut Counters)) {
        self.0.counters.send_modify(update);
        self.bump();
    }

    /// Deliver an upstream data/control frame to every live connection.
    fn emit(&self, frame: Value) {
        let _ = self.0.outbound.send(frame);
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

    /// A two-sided book whose first bid price identifies the source dex.
    fn emit_book(&self, coin: &str, time: u64, price: f64, levels: usize) {
        let bids: Vec<Value> = (0..levels)
            .map(|index| {
                json!({
                    "px": format!("{}", price - index as f64),
                    "sz": format!("{}", 1.0 + index as f64),
                    "n": index + 1
                })
            })
            .collect();
        let asks: Vec<Value> = (0..levels)
            .map(|index| {
                json!({
                    "px": format!("{}", price + 1.0 + index as f64),
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

    fn emit_candle(&self, coin: &str, time: u64, close: f64, interval: &str) {
        let open = close - 1.0;
        self.emit(json!({
            "channel": "candle",
            "data": {
                "t": time,
                "T": time + 59_999,
                "s": coin,
                "i": interval,
                "o": format!("{open}"),
                "c": format!("{close}"),
                "h": format!("{}", close + 1.0),
                "l": format!("{}", open - 1.0),
                "v": "5",
                "n": 3
            }
        }));
    }

    /// Close every currently open upstream connection (simulates an upstream drop).
    fn drop_connections(&self) {
        let _ = self.0.close.send(());
    }

    fn on_frame(&self, text: &str) {
        let Ok(frame) = serde_json::from_str::<Value>(text) else {
            return;
        };
        self.note_frame(frame.clone());
        match frame["method"].as_str() {
            // Hyperliquid acknowledges only unsubscriptions; subscribes are silent.
            Some("unsubscribe") => self.emit(json!({
                "channel": "subscriptionResponse",
                "data": {"method": "unsubscribe", "subscription": frame["subscription"]}
            })),
            Some("ping") => self.emit(json!({"channel": "pong"})),
            _ => {}
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
}

async fn info_handler(State(upstream): State<Upstream>, Json(request): Json<Value>) -> Json<Value> {
    upstream.note_info(request.clone());
    let response = match request["type"].as_str() {
        Some("spotMeta") => spot_meta(),
        Some("spotMetaAndAssetCtxs") => json!([spot_meta(), [context("0.14")]]),
        Some("perpDexs") => json!([null, perp_dex("xyz"), perp_dex("abc")]),
        Some("metaAndAssetCtxs") => match request["dex"].as_str() {
            None => swap_meta(),
            Some("xyz") => hip3_meta("xyz:BTC", 0, "200"),
            Some("abc") => hip3_meta("abc:BTC", 1, "300"),
            Some(other) => panic!("unexpected upstream dex: {other}"),
        },
        other => panic!("unexpected upstream /info request: {other:?}"),
    };
    Json(response)
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
                    Ok(frame) => {
                        if sender
                            .send(AxumWsMessage::Text(frame.to_string().into()))
                            .await
                            .is_err()
                        {
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

// ---------------------------------------------------------------------------
// Service harness
// ---------------------------------------------------------------------------

fn config(base: &str) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        hyperliquid_base_url: base.to_string(),
        extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
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

struct Harness {
    upstream: Upstream,
    realtime: RealtimeService,
    service: CcxtService,
    state: AppState,
    base: String,
    tasks: Vec<JoinHandle<()>>,
}

impl Harness {
    async fn start() -> Self {
        let upstream = Upstream::start();
        let app = Router::new()
            .route("/info", post(info_handler))
            .route("/ws", get(ws_handler))
            .with_state(upstream.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let service = CcxtService::start(&config(&format!("http://{addr}"))).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        registry.register(Arc::new(CcxtExchange::new(
            Venue::Hyperliquid,
            service.clone(),
        )));
        let state = AppState::new(Arc::new(registry), realtime.clone());
        let app = Router::new()
            .route("/v1/fetchMarkets", post(web::fetch_markets))
            .route("/v1/fetchOrderBook", post(web::fetch_order_book))
            .route("/v1/ws", get(web::trades_stream_ws))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let backend_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        Self {
            upstream,
            realtime,
            service,
            state,
            base,
            tasks: vec![upstream_task, backend_task],
        }
    }

    async fn post(&self, endpoint: &str, mut body: Value) -> (StatusCode, Value) {
        body["exchange"] = json!(EXCHANGE);
        let response = reqwest::Client::new()
            .post(format!("{}/v1/{endpoint}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value = response.json().await.unwrap();
        (status, value)
    }

    /// Public catalog rows as the consumer sees them.
    async fn catalog(&self) -> Vec<Value> {
        let (status, body) = self.post("fetchMarkets", json!({"params": {}})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["markets"].as_array().cloned().unwrap_or_default()
    }

    async fn subscribe(
        &self,
        channel: RealtimeChannel,
        symbol: &str,
        params: Value,
    ) -> RealtimeSubscription {
        timeout(
            WINDOW,
            self.realtime.subscribe(
                channel,
                RealtimeTopic {
                    exchange: EXCHANGE.into(),
                    symbol: symbol.into(),
                    params,
                },
            ),
        )
        .await
        .expect("subscribe should resolve within the window")
        .expect("subscribe should succeed")
    }

    async fn stop(self) {
        self.state.shutdown_websockets().await;
        self.state.shutdown_market_stats().await;
        self.service.shutdown().await.unwrap();
        let mut rx = self.upstream.0.counters.subscribe();
        timeout(WINDOW, rx.wait_for(|counters| counters.active == 0))
            .await
            .expect("upstream connections should close on shutdown")
            .unwrap();
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

fn row<'a>(rows: &'a [Value], exchange_market_id: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["exchangeMarketId"] == json!(exchange_market_id))
        .unwrap_or_else(|| panic!("catalog is missing market `{exchange_market_id}`: {rows:?}"))
}

// ---------------------------------------------------------------------------
// Update helpers
// ---------------------------------------------------------------------------

fn kind(update: &RealtimeUpdate) -> &'static str {
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

/// Next trades update. Transient source errors are skipped: they are a
/// legitimate part of a reconnect and are asserted separately.
async fn recv_trades(receiver: &mut RealtimeReceiver) -> Arc<Vec<CcxtTrade>> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::Trades(trades) => return trades,
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected trades, got {}", kind(&other)),
        }
    }
}

async fn recv_order_book(receiver: &mut RealtimeReceiver) -> Arc<CcxtOrderBook> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::OrderBook(book) => return book,
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected an order book, got {}", kind(&other)),
        }
    }
}

async fn recv_candles(receiver: &mut RealtimeReceiver) -> Vec<CcxtOhlcv> {
    loop {
        match recv_update(receiver).await {
            RealtimeUpdate::Ohlcv(candles) => return candles.as_ref().clone(),
            RealtimeUpdate::Error(_) => continue,
            other => panic!("expected ohlcv, got {}", kind(&other)),
        }
    }
}

/// Prove that this receiver publishes nothing further.
async fn expect_quiet(receiver: &mut RealtimeReceiver, label: &str) {
    match timeout(QUIET, receiver.recv()).await {
        Err(_) => {}
        Ok(Ok(update)) => panic!("{label} received an unexpected update: {}", kind(&update)),
        Ok(Err(error)) => panic!("{label} receiver closed unexpectedly: {error}"),
    }
}

// ---------------------------------------------------------------------------
// Websocket client helpers (/v1/ws)
// ---------------------------------------------------------------------------

fn command(op: &str, channel: &str, symbol: &str, params: Value) -> Value {
    json!({"op": op, "channel": channel, "exchange": EXCHANGE, "symbol": symbol, "params": params})
}

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

/// Next frame of `message_type` for `symbol`, failing on a source error.
async fn next_typed(client: &mut Client, message_type: &str, symbol: &str) -> Value {
    loop {
        let value = next_json(client).await;
        assert_ne!(
            value["type"], "error",
            "unexpected websocket error while waiting for {message_type} {symbol}: {value}"
        );
        if value["type"] == message_type && value["topic"]["symbol"] == json!(symbol) {
            return value;
        }
    }
}

// ---------------------------------------------------------------------------
// 1. Catalog identity and explicit dex selection
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_exposes_hip3_identity_and_requires_explicit_dex_selection() {
    let h = Harness::start().await;

    let rows = h.catalog().await;
    let primary = row(&rows, "BTC").clone();
    let xyz = row(&rows, "xyz:BTC").clone();
    let abc = row(&rows, "abc:BTC").clone();

    assert_eq!(primary["dex"], json!(""));
    assert_eq!(primary["symbol"], json!("BTC/USDC"));
    assert_eq!(
        primary["marketId"],
        json!(r#"["hyperliquid","perp",null,"","BTC"]"#)
    );

    assert_eq!(xyz["dex"], json!("xyz"));
    assert_eq!(xyz["symbol"], json!("XYZ-BTC/USDC"));
    assert_eq!(xyz["quote"], json!("USDC"));
    assert_eq!(
        xyz["marketId"],
        json!(r#"["hyperliquid","perp",null,"xyz","xyz:BTC"]"#)
    );

    assert_eq!(abc["dex"], json!("abc"));
    assert_eq!(abc["symbol"], json!("ABC-BTC/USDH"));
    assert_eq!(abc["quote"], json!("USDH"));
    assert_eq!(abc["settle"], json!("USDH"));
    assert_eq!(
        abc["marketId"],
        json!(r#"["hyperliquid","perp",null,"abc","abc:BTC"]"#)
    );

    // The same underlying asset keeps three distinct opaque identities.
    let ids: HashSet<String> = [&primary, &xyz, &abc]
        .into_iter()
        .map(|row| row["marketId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 3);

    // Explicit dex selection narrows the catalog to that deployer only.
    for (dex, native) in [("xyz", "xyz:BTC"), ("abc", "abc:BTC")] {
        let (status, body) = h
            .post("fetchMarkets", json!({"params": {"dex": dex}}))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let selected = body["markets"].as_array().unwrap();
        assert_eq!(selected.len(), 1, "{body}");
        assert_eq!(selected[0]["exchangeMarketId"], json!(native), "{body}");
    }

    // The catalog was built from the builder-dex metadata, not from ticker text.
    let requests = h.upstream.info_requests();
    let asked = |kind: &str, dex: Option<&str>| {
        requests.iter().any(|request| {
            request["type"] == json!(kind) && request.get("dex").map(Value::as_str).flatten() == dex
        })
    };
    assert!(
        asked("perpDexs", None),
        "perpDexs was not requested: {requests:?}"
    );
    assert!(asked("metaAndAssetCtxs", None));
    assert!(asked("metaAndAssetCtxs", Some("xyz")));
    assert!(asked("metaAndAssetCtxs", Some("abc")));

    // A display-only base never selects a HIP3 row: dex is required.
    let (status, body) = h
        .post(
            "fetchOrderBook",
            json!({"symbol": "BTC", "limit": 2, "params": {"dex": "xyz"}}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], json!("BAD_SYMBOL"), "{body}");

    // The primary dex is addressable by the empty selector.
    let (status, body) = h.post("fetchMarkets", json!({"params": {"dex": ""}})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let selected = body["markets"].as_array().unwrap();
    assert_eq!(selected.len(), 1, "{body}");
    assert_eq!(selected[0]["exchangeMarketId"], json!("BTC"), "{body}");

    h.stop().await;
}

// ---------------------------------------------------------------------------
// 2. Primary + USDC builder dex share one connection
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_and_usdc_hip3_channels_share_one_connection_and_route_native_coins() {
    let h = Harness::start().await;
    let rows = h.catalog().await;
    let primary = row(&rows, "BTC").clone();
    let xyz = row(&rows, "xyz:BTC").clone();

    let mut pt = h
        .subscribe(
            RealtimeChannel::Trades,
            primary["marketId"].as_str().unwrap(),
            json!({}),
        )
        .await;
    let mut pb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            primary["symbol"].as_str().unwrap(),
            json!({"levels": 20}),
        )
        .await;
    let mut pc = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            primary["symbol"].as_str().unwrap(),
            json!({"timeframe": "1m"}),
        )
        .await;
    let mut xt = h
        .subscribe(
            RealtimeChannel::Trades,
            xyz["marketId"].as_str().unwrap(),
            json!({"dex": "xyz"}),
        )
        .await;
    let mut xb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            xyz["marketId"].as_str().unwrap(),
            json!({"levels": 20, "dex": "xyz"}),
        )
        .await;
    let mut xc = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            xyz["symbol"].as_str().unwrap(),
            json!({"timeframe": "1m", "dex": "xyz"}),
        )
        .await;

    // The acknowledgement carries the unified symbol, never the raw dex name.
    assert_eq!(pt.topic.symbol, BTC);
    assert_eq!(pb.topic.symbol, BTC);
    assert_eq!(pc.topic.symbol, BTC);
    assert_eq!(xt.topic.symbol, XYZ);
    assert_eq!(xb.topic.symbol, XYZ);
    assert_eq!(xc.topic.symbol, XYZ);

    h.upstream
        .wait_until("all six upstream subscriptions", |upstream| {
            upstream.subscribe_frames("trades", "BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "BTC").len() == 1
                && upstream.subscribe_frames("candle", "BTC").len() == 1
                && upstream.subscribe_frames("trades", "xyz:BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "xyz:BTC").len() == 1
                && upstream.subscribe_frames("candle", "xyz:BTC").len() == 1
        })
        .await;
    assert_eq!(
        h.upstream.counters().connections,
        1,
        "one venue URL means one shared upstream connection"
    );
    assert_eq!(
        h.upstream.subscribe_frames("candle", "xyz:BTC")[0]["subscription"]["interval"],
        json!("1m")
    );

    h.upstream.emit_trade("BTC", 1, 100.0, 0.5, TIME);
    h.upstream.emit_trade("xyz:BTC", 2, 200.0, 0.25, TIME + 1);
    h.upstream.emit_book("BTC", TIME, 100.0, 20);
    h.upstream.emit_book("xyz:BTC", TIME + 1, 200.0, 20);
    h.upstream.emit_candle("BTC", TIME, 100.0, "1m");
    h.upstream.emit_candle("xyz:BTC", TIME, 200.0, "1m");

    let primary_trades = recv_trades(&mut pt.receiver).await;
    assert_eq!(primary_trades.len(), 1);
    assert_eq!(primary_trades[0].id.as_deref(), Some("1"));
    assert_eq!(primary_trades[0].symbol.as_deref(), Some(BTC));
    assert_eq!(primary_trades[0].price, Some(100.0));
    assert_eq!(primary_trades[0].amount, Some(0.5));
    assert_eq!(primary_trades[0].side.as_deref(), Some("buy"));

    let xyz_trades = recv_trades(&mut xt.receiver).await;
    assert_eq!(xyz_trades.len(), 1);
    assert_eq!(xyz_trades[0].id.as_deref(), Some("2"));
    assert_eq!(xyz_trades[0].symbol.as_deref(), Some(XYZ));
    assert_eq!(xyz_trades[0].price, Some(200.0));

    let primary_book = recv_order_book(&mut pb.receiver).await;
    assert_eq!(primary_book.symbol.as_deref(), Some(BTC));
    assert_eq!(primary_book.timestamp, Some(TIME));
    assert_eq!(primary_book.bids[0], (100.0, 1.0));

    let xyz_book = recv_order_book(&mut xb.receiver).await;
    assert_eq!(xyz_book.symbol.as_deref(), Some(XYZ));
    assert_eq!(xyz_book.timestamp, Some(TIME + 1));
    assert_eq!(xyz_book.bids[0], (200.0, 1.0));

    assert_eq!(
        recv_candles(&mut pc.receiver).await,
        vec![(TIME, 99.0, 101.0, 98.0, 100.0, Some(5.0))]
    );
    assert_eq!(
        recv_candles(&mut xc.receiver).await,
        vec![(TIME, 199.0, 201.0, 198.0, 200.0, Some(5.0))]
    );

    // No feed ever observes the sibling dex's values.
    expect_quiet(&mut pt.receiver, "primary trades").await;
    expect_quiet(&mut pb.receiver, "primary book").await;
    expect_quiet(&mut pc.receiver, "primary candles").await;
    expect_quiet(&mut xt.receiver, "xyz trades").await;
    expect_quiet(&mut xb.receiver, "xyz book").await;
    expect_quiet(&mut xc.receiver, "xyz candles").await;

    // Retiring the builder-dex feeds unsubscribes the native coin only.
    drop((xt, xb, xc));
    h.upstream
        .wait_until("xyz native-coin unsubscribes", |upstream| {
            upstream.unsubscribe_frames("trades", "xyz:BTC").len() == 1
                && upstream.unsubscribe_frames("l2Book", "xyz:BTC").len() == 1
                && upstream.unsubscribe_frames("candle", "xyz:BTC").len() == 1
        })
        .await;
    assert!(h.upstream.unsubscribe_frames("trades", "BTC").is_empty());
    assert!(h.upstream.unsubscribe_frames("l2Book", "BTC").is_empty());
    assert!(h.upstream.unsubscribe_frames("candle", "BTC").is_empty());

    // The remaining primary feeds stay warm on the same shared connection.
    h.upstream.emit_book("BTC", TIME + 10, 111.0, 20);
    assert_eq!(
        recv_order_book(&mut pb.receiver).await.timestamp,
        Some(TIME + 10)
    );
    assert_eq!(h.upstream.counters().connections, 1);

    drop((pt, pb, pc));
    h.stop().await;
}

// ---------------------------------------------------------------------------
// 3. USDH-collateral builder dex keeps its own market identity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usdh_collateral_hip3_delivers_its_own_generation() {
    let h = Harness::start().await;
    let rows = h.catalog().await;
    let abc = row(&rows, "abc:BTC").clone();
    assert_eq!(abc["dex"], json!("abc"));
    assert_eq!(abc["quote"], json!("USDH"));

    let mut t = h
        .subscribe(
            RealtimeChannel::Trades,
            abc["marketId"].as_str().unwrap(),
            json!({}),
        )
        .await;
    let mut b = h
        .subscribe(
            RealtimeChannel::OrderBook,
            abc["marketId"].as_str().unwrap(),
            json!({"levels": 20}),
        )
        .await;
    let mut c = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            abc["marketId"].as_str().unwrap(),
            json!({"timeframe": "1m"}),
        )
        .await;
    assert_eq!(t.topic.symbol, ABC);
    assert_eq!(b.topic.symbol, ABC);
    assert_eq!(c.topic.symbol, ABC);

    h.upstream
        .wait_until("abc native-coin subscriptions", |upstream| {
            upstream.subscribe_frames("trades", "abc:BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "abc:BTC").len() == 1
                && upstream.subscribe_frames("candle", "abc:BTC").len() == 1
        })
        .await;

    h.upstream.emit_trade("abc:BTC", 7, 300.0, 0.5, TIME);
    h.upstream.emit_book("abc:BTC", TIME + 1, 300.0, 20);
    h.upstream.emit_candle("abc:BTC", TIME, 300.0, "1m");

    let trades = recv_trades(&mut t.receiver).await;
    assert_eq!(trades[0].id.as_deref(), Some("7"));
    assert_eq!(trades[0].symbol.as_deref(), Some(ABC));
    assert_eq!(trades[0].price, Some(300.0));
    assert_eq!(trades[0].amount, Some(0.5));

    let book = recv_order_book(&mut b.receiver).await;
    assert_eq!(book.symbol.as_deref(), Some(ABC));
    assert_eq!(book.timestamp, Some(TIME + 1));
    assert_eq!(book.bids[0], (300.0, 1.0));

    assert_eq!(
        recv_candles(&mut c.receiver).await,
        vec![(TIME, 299.0, 301.0, 298.0, 300.0, Some(5.0))]
    );

    expect_quiet(&mut t.receiver, "abc trades").await;
    expect_quiet(&mut b.receiver, "abc book").await;
    expect_quiet(&mut c.receiver, "abc candles").await;

    drop((t, b, c));
    h.stop().await;
}

// ---------------------------------------------------------------------------
// 4. Same underlying asset across dexes stays isolated
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_underlying_across_dexes_isolates_channels_and_retired_frames() {
    let h = Harness::start().await;
    let rows = h.catalog().await;
    let primary = row(&rows, "BTC").clone();
    let xyz = row(&rows, "xyz:BTC").clone();
    let abc = row(&rows, "abc:BTC").clone();

    let mut pt = h
        .subscribe(
            RealtimeChannel::Trades,
            primary["marketId"].as_str().unwrap(),
            json!({}),
        )
        .await;
    let mut xt = h
        .subscribe(
            RealtimeChannel::Trades,
            xyz["marketId"].as_str().unwrap(),
            json!({"dex": "xyz"}),
        )
        .await;
    let mut at = h
        .subscribe(
            RealtimeChannel::Trades,
            abc["marketId"].as_str().unwrap(),
            json!({"dex": "abc"}),
        )
        .await;
    let mut pb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            primary["symbol"].as_str().unwrap(),
            json!({"levels": 20}),
        )
        .await;
    let mut xb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            xyz["symbol"].as_str().unwrap(),
            json!({"levels": 20}),
        )
        .await;
    let mut ab = h
        .subscribe(
            RealtimeChannel::OrderBook,
            abc["marketId"].as_str().unwrap(),
            json!({"levels": 20, "dex": "abc"}),
        )
        .await;

    h.upstream
        .wait_until("three-dex subscriptions", |upstream| {
            upstream.subscribe_frames("trades", "BTC").len() == 1
                && upstream.subscribe_frames("trades", "xyz:BTC").len() == 1
                && upstream.subscribe_frames("trades", "abc:BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "xyz:BTC").len() == 1
                && upstream.subscribe_frames("l2Book", "abc:BTC").len() == 1
        })
        .await;
    assert_eq!(h.upstream.counters().connections, 1);

    h.upstream.emit_trade("BTC", 11, 100.0, 1.0, TIME);
    h.upstream.emit_trade("xyz:BTC", 12, 200.0, 2.0, TIME + 1);
    h.upstream.emit_trade("abc:BTC", 13, 300.0, 3.0, TIME + 2);
    h.upstream.emit_book("BTC", TIME, 100.0, 20);
    h.upstream.emit_book("xyz:BTC", TIME, 200.0, 20);
    h.upstream.emit_book("abc:BTC", TIME, 300.0, 20);

    let primary_trades = recv_trades(&mut pt.receiver).await;
    assert_eq!(primary_trades[0].symbol.as_deref(), Some(BTC));
    assert_eq!(primary_trades[0].price, Some(100.0));
    let xyz_trades = recv_trades(&mut xt.receiver).await;
    assert_eq!(xyz_trades[0].symbol.as_deref(), Some(XYZ));
    assert_eq!(xyz_trades[0].price, Some(200.0));
    let abc_trades = recv_trades(&mut at.receiver).await;
    assert_eq!(abc_trades[0].symbol.as_deref(), Some(ABC));
    assert_eq!(abc_trades[0].price, Some(300.0));
    assert_eq!(abc_trades[0].amount, Some(3.0));

    assert_eq!(
        recv_order_book(&mut pb.receiver).await.bids[0],
        (100.0, 1.0)
    );
    assert_eq!(
        recv_order_book(&mut xb.receiver).await.bids[0],
        (200.0, 1.0)
    );
    let abc_book = recv_order_book(&mut ab.receiver).await;
    assert_eq!(abc_book.symbol.as_deref(), Some(ABC));
    assert_eq!(abc_book.bids[0], (300.0, 1.0));

    // Retire the whole xyz dex; only its native coin is unsubscribed.
    drop((xt, xb));
    h.upstream
        .wait_until("xyz dex retirement", |upstream| {
            upstream.unsubscribe_frames("trades", "xyz:BTC").len() == 1
                && upstream.unsubscribe_frames("l2Book", "xyz:BTC").len() == 1
        })
        .await;
    assert!(h.upstream.unsubscribe_frames("trades", "BTC").is_empty());
    assert!(h
        .upstream
        .unsubscribe_frames("trades", "abc:BTC")
        .is_empty());
    assert!(h.upstream.unsubscribe_frames("l2Book", "BTC").is_empty());
    assert!(h
        .upstream
        .unsubscribe_frames("l2Book", "abc:BTC")
        .is_empty());

    // A late frame for the retired dex must not be published to any other dex.
    h.upstream.emit_trade("xyz:BTC", 14, 999.0, 9.0, TIME + 3);
    h.upstream.emit_book("xyz:BTC", TIME + 3, 999.0, 20);
    expect_quiet(&mut pt.receiver, "primary trades after xyz retirement").await;
    expect_quiet(&mut at.receiver, "abc trades after xyz retirement").await;
    expect_quiet(&mut pb.receiver, "primary book after xyz retirement").await;
    expect_quiet(&mut ab.receiver, "abc book after xyz retirement").await;

    // Primary and abc keep advancing on their own generations.
    h.upstream.emit_trade("abc:BTC", 15, 301.0, 1.0, TIME + 4);
    let abc_next = recv_trades(&mut at.receiver).await;
    assert!(
        abc_next
            .iter()
            .any(|trade| trade.id.as_deref() == Some("15")),
        "abc must keep receiving its own trades: {abc_next:?}"
    );
    assert!(
        abc_next
            .iter()
            .all(|trade| trade.symbol.as_deref() == Some(ABC)),
        "abc must never receive another dex's trades: {abc_next:?}"
    );
    h.upstream.emit_book("BTC", TIME + 5, 101.0, 20);
    assert_eq!(
        recv_order_book(&mut pb.receiver).await.timestamp,
        Some(TIME + 5)
    );

    drop((pt, pb, at, ab));
    h.stop().await;
}

// ---------------------------------------------------------------------------
// 5. Reconnect re-subscribes every channel and dex
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_resubscribes_every_channel_and_dex() {
    let h = Harness::start().await;
    let rows = h.catalog().await;
    let primary = row(&rows, "BTC").clone();
    let xyz = row(&rows, "xyz:BTC").clone();
    let abc = row(&rows, "abc:BTC").clone();

    let mut pt = h
        .subscribe(
            RealtimeChannel::Trades,
            primary["marketId"].as_str().unwrap(),
            json!({}),
        )
        .await;
    let mut pb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            primary["symbol"].as_str().unwrap(),
            json!({"levels": 20}),
        )
        .await;
    let mut pc = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            primary["symbol"].as_str().unwrap(),
            json!({"timeframe": "1m"}),
        )
        .await;
    let mut xt = h
        .subscribe(
            RealtimeChannel::Trades,
            xyz["marketId"].as_str().unwrap(),
            json!({"dex": "xyz"}),
        )
        .await;
    let mut xb = h
        .subscribe(
            RealtimeChannel::OrderBook,
            xyz["marketId"].as_str().unwrap(),
            json!({"levels": 20, "dex": "xyz"}),
        )
        .await;
    let mut xc = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            xyz["marketId"].as_str().unwrap(),
            json!({"timeframe": "1m", "dex": "xyz"}),
        )
        .await;
    let mut at = h
        .subscribe(
            RealtimeChannel::Trades,
            abc["marketId"].as_str().unwrap(),
            json!({"dex": "abc"}),
        )
        .await;
    let mut ab = h
        .subscribe(
            RealtimeChannel::OrderBook,
            abc["marketId"].as_str().unwrap(),
            json!({"levels": 20, "dex": "abc"}),
        )
        .await;
    let mut ac = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            abc["marketId"].as_str().unwrap(),
            json!({"timeframe": "1m", "dex": "abc"}),
        )
        .await;

    h.upstream
        .wait_until("nine upstream subscriptions", |upstream| {
            count_channels(upstream, 1)
        })
        .await;
    assert_eq!(h.upstream.counters().connections, 1);

    h.upstream.emit_book("BTC", TIME, 100.0, 20);
    assert_eq!(
        recv_order_book(&mut pb.receiver).await.timestamp,
        Some(TIME)
    );

    // Drop the socket: the owner must reconnect and re-subscribe all nine feeds.
    h.upstream.drop_connections();
    h.upstream
        .wait_until("one replacement connection", |upstream| {
            let counters = upstream.counters();
            counters.connections >= 2 && counters.active == 1
        })
        .await;
    h.upstream
        .wait_until("nine upstream re-subscriptions", |upstream| {
            count_channels(upstream, 2)
        })
        .await;
    assert_eq!(h.upstream.counters().connections, 2);
    assert_eq!(h.upstream.counters().active, 1);

    // Fresh generations only: the pre-reconnect book is never replayed.
    h.upstream.emit_book("BTC", TIME + 100, 150.0, 20);
    let snapshot = recv_order_book(&mut pb.receiver).await;
    assert_eq!(snapshot.timestamp, Some(TIME + 100));
    assert_eq!(snapshot.bids[0], (150.0, 1.0));
    expect_quiet(&mut pb.receiver, "primary book after reconnect").await;

    h.upstream.emit_trade("BTC", 31, 101.0, 1.0, TIME + 101);
    h.upstream.emit_trade("xyz:BTC", 32, 201.0, 2.0, TIME + 102);
    h.upstream.emit_trade("abc:BTC", 33, 301.0, 3.0, TIME + 103);
    h.upstream.emit_book("xyz:BTC", TIME + 104, 202.0, 20);
    h.upstream.emit_book("abc:BTC", TIME + 105, 302.0, 20);
    h.upstream.emit_candle("BTC", TIME, 102.0, "1m");
    h.upstream.emit_candle("xyz:BTC", TIME, 202.0, "1m");
    h.upstream.emit_candle("abc:BTC", TIME, 302.0, "1m");

    assert_eq!(recv_trades(&mut pt.receiver).await[0].price, Some(101.0));
    assert_eq!(recv_trades(&mut xt.receiver).await[0].price, Some(201.0));
    let abc_trades = recv_trades(&mut at.receiver).await;
    assert_eq!(abc_trades[0].price, Some(301.0));
    assert_eq!(abc_trades[0].symbol.as_deref(), Some(ABC));

    let xyz_book = recv_order_book(&mut xb.receiver).await;
    assert_eq!(xyz_book.timestamp, Some(TIME + 104));
    assert_eq!(xyz_book.bids[0], (202.0, 1.0));
    let abc_book = recv_order_book(&mut ab.receiver).await;
    assert_eq!(abc_book.timestamp, Some(TIME + 105));
    assert_eq!(abc_book.bids[0], (302.0, 1.0));

    assert_eq!(
        recv_candles(&mut pc.receiver).await,
        vec![(TIME, 101.0, 103.0, 100.0, 102.0, Some(5.0))]
    );
    assert_eq!(
        recv_candles(&mut xc.receiver).await,
        vec![(TIME, 201.0, 203.0, 200.0, 202.0, Some(5.0))]
    );
    assert_eq!(
        recv_candles(&mut ac.receiver).await,
        vec![(TIME, 301.0, 303.0, 300.0, 302.0, Some(5.0))]
    );

    drop((pt, pb, pc, xt, xb, xc, at, ab, ac));
    h.stop().await;
}

/// Every channel/coin pair has been subscribed at least `count` times.
fn count_channels(upstream: &Upstream, count: usize) -> bool {
    let expect = |kind: &str, coin: &str| upstream.subscribe_frames(kind, coin).len() >= count;
    expect("trades", "BTC")
        && expect("l2Book", "BTC")
        && expect("candle", "BTC")
        && expect("trades", "xyz:BTC")
        && expect("l2Book", "xyz:BTC")
        && expect("candle", "xyz:BTC")
        && expect("trades", "abc:BTC")
        && expect("l2Book", "abc:BTC")
        && expect("candle", "abc:BTC")
}

// ---------------------------------------------------------------------------
// 6. Public websocket protocol: opaque ids, dex params, aggregation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_protocol_selects_by_opaque_market_id_and_dex_and_aggregates_books() {
    let h = Harness::start().await;
    let rows = h.catalog().await;
    let primary = row(&rows, "BTC").clone();
    let xyz = row(&rows, "xyz:BTC").clone();
    let abc = row(&rows, "abc:BTC").clone();

    let (mut client, _) = connect_async(format!("{}/v1/ws", h.base.replace("http:", "ws:")))
        .await
        .unwrap();

    // Primary book, requested with stock aggregation parameters.
    send_json(
        &mut client,
        command(
            "subscribe",
            "orderbook",
            primary["marketId"].as_str().unwrap(),
            json!({"levels": 5, "nSigFigs": 5}),
        ),
    )
    .await;
    let ack = next_typed(&mut client, "subscribed", BTC).await;
    assert_eq!(ack["topic"]["params"], json!({"levels": 5, "nSigFigs": 5}));

    // Builder dex selected by the opaque catalog marketId plus its dex selector.
    send_json(
        &mut client,
        command(
            "subscribe",
            "trades",
            abc["marketId"].as_str().unwrap(),
            json!({"dex": "abc"}),
        ),
    )
    .await;
    let ack = next_typed(&mut client, "subscribed", ABC).await;
    assert_eq!(ack["topic"]["symbol"], json!(ABC));
    assert_eq!(ack["topic"]["params"], json!({"dex": "abc"}));

    // Builder dex selected by its native name plus the same dex selector.
    send_json(
        &mut client,
        command(
            "subscribe",
            "ohlcv",
            "xyz:BTC",
            json!({"dex": "xyz", "timeframe": "1m"}),
        ),
    )
    .await;
    let ack = next_typed(&mut client, "subscribed", XYZ).await;
    assert_eq!(ack["topic"]["symbol"], json!(XYZ));

    // The same identity is acknowledged, never re-subscribed.
    send_json(
        &mut client,
        command(
            "subscribe",
            "trades",
            abc["marketId"].as_str().unwrap(),
            json!({"dex": "abc"}),
        ),
    )
    .await;
    let duplicate = next_typed(&mut client, "alreadySubscribed", ABC).await;
    assert_eq!(duplicate["topic"]["symbol"], json!(ABC));

    h.upstream
        .wait_until("client-driven upstream subscriptions", |upstream| {
            upstream.subscribe_frames("l2Book", "BTC").len() == 1
                && upstream.subscribe_frames("trades", "abc:BTC").len() == 1
                && upstream.subscribe_frames("candle", "xyz:BTC").len() == 1
        })
        .await;
    assert_eq!(h.upstream.counters().connections, 1);

    // Aggregation is routed to stock, not silently dropped.
    let book = h.upstream.subscribe_frames("l2Book", "BTC");
    assert_eq!(book[0]["subscription"]["coin"], json!("BTC"));
    assert_eq!(book[0]["subscription"]["nSigFigs"], json!(5));
    assert_eq!(book[0]["subscription"]["mantissa"], Value::Null);

    h.upstream.emit_book("BTC", TIME, 100.0, 20);
    let view = next_typed(&mut client, "orderbook", BTC).await;
    assert_eq!(view["topic"]["symbol"], json!(BTC));
    assert_eq!(view["data"]["bids"].as_array().unwrap().len(), 5);
    assert_eq!(view["data"]["bids"][0], json!([100.0, 1.0]));
    assert_eq!(view["data"]["timestamp"], json!(TIME));

    h.upstream.emit_trade("abc:BTC", 21, 300.0, 0.5, TIME);
    let trades = next_typed(&mut client, "trades", ABC).await;
    assert_eq!(trades["data"][0]["symbol"], json!(ABC));
    assert_eq!(trades["data"][0]["price"], json!(300.0));

    h.upstream.emit_candle("xyz:BTC", TIME, 200.0, "1m");
    let candles = next_typed(&mut client, "ohlcv", XYZ).await;
    assert_eq!(candles["data"][0][0], json!(TIME));
    assert_eq!(candles["data"][0][4], json!(200.0));

    // Unsubscribing releases exactly this viewer and routes the native coin.
    send_json(
        &mut client,
        command(
            "unsubscribe",
            "orderbook",
            primary["marketId"].as_str().unwrap(),
            json!({"levels": 5, "nSigFigs": 5}),
        ),
    )
    .await;
    let unsubscribed = next_typed(&mut client, "unsubscribed", BTC).await;
    assert_eq!(unsubscribed["topic"]["symbol"], json!(BTC));
    h.upstream
        .wait_until("primary book native-coin unsubscribe", |upstream| {
            upstream.unsubscribe_frames("l2Book", "BTC").len() == 1
        })
        .await;
    let frame = &h.upstream.unsubscribe_frames("l2Book", "BTC")[0];
    assert_eq!(frame["subscription"]["coin"], json!("BTC"));
    assert_eq!(frame["subscription"]["nSigFigs"], json!(5));
    assert!(h
        .upstream
        .unsubscribe_frames("trades", "abc:BTC")
        .is_empty());
    assert!(h
        .upstream
        .unsubscribe_frames("candle", "xyz:BTC")
        .is_empty());

    client.close(None).await.unwrap();
    drop(client);
    h.stop().await;
}
