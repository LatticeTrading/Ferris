//! Actual pinned stock acquisition/parsers over loopback HTTP and WebSocket.
use axum::{
    extract::{
        ws::{Message as ServerMessage, WebSocketUpgrade},
        State,
    },
    http::{StatusCode, Uri},
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
    realtime::{
        RealtimeChannel, RealtimeService, RealtimeSubscription, RealtimeTopic, RealtimeUpdate,
    },
    web::{self, AppState},
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Mutex},
    time::timeout,
};

const WINDOW: Duration = Duration::from_secs(8);
const TIME: u64 = 1_700_000_000_000;
const NATIVE: &str = "tBTCF0:USTF0";
const SYMBOL: &str = "BTC/USDT:USDT";
const TRADES: &str = "trades:tBTCF0:USTF0";
const BOOK: &str = "book:tBTCF0:USTF0";
const CANDLES: &str = "candles:1D:tBTCF0:USTF0";
#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<Uri>>>,
    frames: broadcast::Sender<Value>,
    commands: watch::Sender<Vec<(usize, Value)>>,
    connections: watch::Sender<Vec<usize>>,
    serial: Arc<AtomicUsize>,
    ack_mode: Arc<AtomicUsize>, // 0 immediate; 1 missing; 2 rejected
    subscribe_hold: Arc<AtomicUsize>,
    reuse: Arc<AtomicUsize>,
    stats_mode: Arc<AtomicUsize>,
}
fn topic(v: &Value) -> String {
    if v["channel"] == "candles" {
        format!(
            "candles:{}",
            v["key"].as_str().unwrap().strip_prefix("trade:").unwrap()
        )
    } else {
        format!(
            "{}:{}",
            v["channel"].as_str().unwrap(),
            v["symbol"].as_str().unwrap()
        )
    }
}
fn pair(name: &str) -> Value {
    json!([name, [null, null, null, "0.001", "10000"]])
}
fn status() -> Value {
    json!([
        NATIVE,
        TIME,
        null,
        60010,
        60000,
        null,
        1000000,
        null,
        TIME + 28800000,
        0.00002,
        3,
        null,
        0.00001,
        null,
        null,
        60005,
        null,
        null,
        123.5,
        null,
        null,
        null,
        0.0005,
        0.0025
    ])
}
async fn http(State(mock): State<Mock>, uri: Uri) -> axum::response::Response {
    mock.calls.lock().await.push(uri.clone());
    if uri.path() == "/v2/status/deriv" && mock.stats_mode.load(Ordering::SeqCst) == 3 {
        return (StatusCode::SERVICE_UNAVAILABLE, "status unavailable").into_response();
    }
    Json(if uri.path().contains("pub:info:pair") {
        json!([
            [pair("BTCUSD"), pair("BTCUST"), pair("ETHUSD")],
            [pair("BTCF0:USTF0")],
            [],
            ["BTCUSD"]
        ])
    } else if uri.path().contains("pub:list:currency") {
        json!([
            ["BTC", "ETH", "USD", "UST", "BTCF0", "USTF0"],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            []
        ])
    } else if uri.path() == "/v2/tickers" {
        assert_eq!(uri.query(), Some("symbols=ALL"));
        json!([
            ["tBTCUSD", 60000, 1, 60001, 2, 10, 0.01, 60002, 12.5, 61000, 59000],
            ["tBTCUST", 60000, 1, 60001, 2, 10, 0.01, 60002, 12.5, 61000, 59000],
            [NATIVE, 60000, 1, 60001, 2, 10, 0.01, 60003, 14.5, 61000, 59000],
            ["fUSD", 0.01, 0.01, 2, 1, 0.02, 2, 2, 0, 0, 0.02, 10, 0.03, 0.01, null, null, 4]
        ])
    } else if uri.path() == "/v2/status/deriv" {
        assert_eq!(uri.query(), Some("keys=ALL"));
        let mut row = status();
        match mock.stats_mode.load(Ordering::SeqCst) {
            1 => {
                row[12] = json!(0);
                row[15] = Value::Null;
                row[18] = json!(0);
            }
            2 => {
                row[12] = json!("NaN");
                row[15] = json!(-1);
                row[18] = json!("broken");
            }
            4 => {
                row.as_array_mut().unwrap().pop();
            } // stock history/status ambiguity
            _ => {}
        }
        json!([row])
    } else if uri.path().starts_with("/v2/trades/") {
        json!([[2, TIME + 2, -0.02, 60001], [1, TIME + 1, 0.01, 60000]])
    } else if uri.path().starts_with("/v2/book/") {
        assert!(uri.path().ends_with("/P0"));
        json!([[60000, 2, 3], [59999, 1, 4], [60001, 1, -5]])
    } else if uri.path().starts_with("/v2/candles/") {
        json!([
            [TIME + 60000, 60001, 60002, 60003, 60000, 4],
            [TIME, 60000, 60001, 60002, 59999, 3]
        ])
    } else {
        panic!("unexpected Bitfinex request: {uri}")
    })
    .into_response()
}
async fn stream(ws: WebSocketUpgrade, State(mock): State<Mock>) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let conn = mock.serial.fetch_add(1,Ordering::SeqCst);
        let mut frames = mock.frames.subscribe();
        let mut ids = HashMap::new();
        let mut next = 10;
        mock.connections.send_modify(|v|v.push(conn));
        loop { tokio::select! {
            input = socket.recv() => match input {
                Some(Ok(ServerMessage::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["event"] == "subscribe" {
                        let key = topic(&v);
                        let id = if mock.reuse.load(Ordering::SeqCst) != 0 { *ids.entry(key).or_insert_with(|| {next+=1; next}) } else {next+=1; ids.insert(key,next);next};
                        let mut ack = v.clone(); ack["event"] = json!("subscribed"); ack["chanId"] = json!(id);
                        if mock.subscribe_hold.load(Ordering::SeqCst) == 0 && socket.send(ServerMessage::Text(ack.to_string())).await.is_err() {break;}
                        // Record the actual server allocation alongside the request.
                        let mut recorded = v; recorded["chanId"] = json!(id);
                        mock.commands.send_modify(|rows| rows.push((conn,recorded)));
                    } else {
                        mock.commands.send_modify(|rows| rows.push((conn,v.clone())));
                        if v["event"] == "unsubscribe" {
                            let mode = mock.ack_mode.load(Ordering::SeqCst);
                            if mode != 1 {
                                let ack = json!({"event":"unsubscribed","chanId":v["chanId"],"status":if mode==2 {"FAIL"}else{"OK"}});
                                if socket.send(ServerMessage::Text(ack.to_string())).await.is_err(){break;}
                            }
                        } else { assert_eq!(v["event"],"conf"); }
                    }
                }
                None | Some(Err(_)) | Some(Ok(ServerMessage::Close(_))) => break,
                _ => {}
            },
            frame = frames.recv() => match frame {
                Ok(v) if v == "close" => break,
                Ok(v) => {if socket.send(ServerMessage::Text(v.to_string())).await.is_err(){break;}},
                Err(_) => break
            }
        }}
        let _ = socket.close().await;
        mock.connections.send_modify(|v|v.retain(|id|*id!=conn));
    })
}
struct Harness {
    mock: Mock,
    service: CcxtService,
    realtime: RealtimeService,
    state: AppState,
    base: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Harness {
    async fn start() -> Self {
        let mock = Mock {
            calls: Arc::default(),
            frames: broadcast::channel(64).0,
            commands: watch::channel(vec![]).0,
            connections: watch::channel(vec![]).0,
            serial: Arc::default(),
            ack_mode: Arc::default(),
            subscribe_hold: Arc::default(),
            reuse: Arc::default(),
            stats_mode: Arc::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/ws/2", get(stream))
            .fallback(http)
            .with_state(mock.clone());
        let upstream = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            request_timeout_ms: 2000,
            hyperliquid_base_url: "http://127.0.0.1:1".into(),
            extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
            extended_ws_url: "ws://127.0.0.1:1".into(),
            lighter_rest_base_url: "http://127.0.0.1:1".into(),
            lighter_ws_url: "ws://127.0.0.1:1".into(),
            binance_base_url: "http://127.0.0.1:1".into(),
            bybit_base_url: "http://127.0.0.1:1".into(),
            aster_base_url: "http://127.0.0.1:1".into(),
            apex_rest_base_url: "http://127.0.0.1:1".into(),
            apex_ws_url: "ws://127.0.0.1:1".into(),
            bitfinex_rest_base_url: format!("http://{addr}"),
            bitfinex_ws_url: format!("ws://{addr}/ws/2"),
            kucoin_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_futures_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_ws_url: "ws://127.0.0.1:1".into(),
            kucoin_futures_ws_url: "ws://127.0.0.1:1".into(),
        };
        let service = CcxtService::start(&config).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        registry.register(Arc::new(CcxtExchange::new(
            Venue::Bitfinex,
            service.clone(),
        )));
        let state = AppState::new(Arc::new(registry), realtime.clone());
        let app = Router::new()
            .route("/v1/fetchMarkets", post(web::fetch_markets))
            .route("/v1/fetchTrades", post(web::fetch_trades))
            .route("/v1/fetchOrderBook", post(web::fetch_order_book))
            .route("/v1/fetchOHLCV", post(web::fetch_ohlcv))
            .route("/v1/fetchMarketStats", post(web::fetch_market_stats))
            .route("/v1/ws", get(web::trades_stream_ws))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let backend = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            mock,
            service,
            realtime,
            state,
            base,
            tasks: vec![upstream, backend],
        }
    }
    async fn post(&self, endpoint: &str, mut body: Value, status: StatusCode) -> Value {
        body["exchange"] = json!("bitfinex");
        let response = reqwest::Client::new()
            .post(format!("{}/v1/{endpoint}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let actual = response.status();
        let v = response.json().await.unwrap();
        assert_eq!(actual, status, "{endpoint}: {v}");
        v
    }
    async fn subscribe(&self, channel: RealtimeChannel, params: Value) -> RealtimeSubscription {
        self.realtime
            .subscribe(
                channel,
                RealtimeTopic {
                    exchange: "bitfinex".into(),
                    symbol: NATIVE.into(),
                    params,
                },
            )
            .await
            .unwrap()
    }
    async fn command(&self, event: &str, key: Option<&str>, count: usize) -> (usize, Value) {
        let matches = |v: &Value| v["event"] == event && key.is_none_or(|key| topic(v) == key);
        let mut rx = self.mock.commands.subscribe();
        let rows = timeout(
            WINDOW,
            rx.wait_for(|rows| rows.iter().filter(|(_, v)| matches(v)).count() >= count),
        )
        .await
        .unwrap()
        .unwrap();
        rows.iter().rev().find(|(_, v)| matches(v)).unwrap().clone()
    }
    fn inject(&self, v: Value) {
        self.mock.frames.send(v).unwrap();
    }
    async fn stop(self) {
        self.state.shutdown_websockets().await;
        self.state.shutdown_market_stats().await;
        self.service.shutdown().await.unwrap();
        let mut rx = self.mock.connections.subscribe();
        timeout(WINDOW, rx.wait_for(Vec::is_empty))
            .await
            .unwrap()
            .unwrap();
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}
async fn next(sub: &mut RealtimeSubscription) -> RealtimeUpdate {
    timeout(WINDOW, sub.receiver.recv()).await.unwrap().unwrap()
}
async fn quiet(sub: &mut RealtimeSubscription) {
    assert!(timeout(Duration::from_millis(100), sub.receiver.recv())
        .await
        .is_err());
}
fn book(id: &Value, amount: f64) -> Value {
    json!([id, [[60000, 1, amount], [60001, 1, -5]]])
}
fn trade(id: &Value, serial: u64) -> Value {
    json!([id, "te", [serial, TIME, -0.02, 60000]])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_rest_aliases_bounds_and_bulk_stats() {
    let h = Harness::start().await;
    let markets = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    let rows = markets["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    let perp = rows.iter().find(|row| row["type"] == "perp").unwrap();
    assert_eq!(perp["exchangeMarketId"], NATIVE);
    assert_eq!(perp["settle"], "USDT");
    assert_eq!(perp["settlementAssetId"], "USTF0");
    assert_eq!(perp["contractSize"], 1.0);
    assert_eq!(perp["tickSize"], Value::Null);
    assert_eq!(perp["minOrderSize"], 0.001);
    for symbol in [
        NATIVE,
        "BTCF0:USTF0",
        SYMBOL,
        perp["marketId"].as_str().unwrap(),
    ] {
        let b = h
            .post(
                "fetchOrderBook",
                json!({"symbol":symbol,"limit":2}),
                StatusCode::OK,
            )
            .await;
        assert_eq!(b["symbol"], SYMBOL);
        assert_eq!(b["bids"], json!([[60000.0, 3.0], [59999.0, 4.0]]));
        assert_eq!(b["timestamp"], Value::Null);
        assert_eq!(b["nonce"], Value::Null);
    }
    h.post(
        "fetchOrderBook",
        json!({"symbol":"BTC/USDT"}),
        StatusCode::BAD_REQUEST,
    )
    .await; // ambiguous display alias
    h.post(
        "fetchOrderBook",
        json!({"symbol":"BTCUSD","params":{"type":"spot"}}),
        StatusCode::OK,
    )
    .await;
    let t = h
        .post(
            "fetchTrades",
            json!({"symbol":NATIVE,"since":TIME,"limit":20000,"params":{"until":TIME+2}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(t[0]["id"], "2");
    assert_eq!(t[0]["side"], "sell");
    assert_eq!(t[0]["amount"], 0.02);
    assert_eq!(t[0]["cost"], 1200.02);
    let c = h
        .post(
            "fetchOHLCV",
            json!({"symbol":NATIVE,"timeframe":"1D","since":TIME,"params":{"endTime":TIME+60000}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(
        c,
        json!([
            [TIME, 60000.0, 60002.0, 59999.0, 60001.0, 3.0],
            [TIME + 60000, 60001.0, 60003.0, 60000.0, 60002.0, 4.0]
        ])
    );
    let calls = h.mock.calls.lock().await.clone();
    assert_eq!(
        calls
            .iter()
            .filter(|u| u.path().contains("pub:info:pair"))
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|u| u.path().contains("pub:list:currency"))
            .count(),
        1
    );
    assert!(calls.iter().any(|u| u.path().contains("/trades/")
        && u.query().unwrap().contains("limit=10000")
        && u.query().unwrap().contains(&format!("end={}", TIME + 2))));
    assert!(calls
        .iter()
        .any(|u| u.path().contains("/book/") && u.query() == Some("len=25")));
    let before = calls.len();
    for (endpoint, body) in [
        ("fetchMarkets", json!({"params":{"type":"future"}})),
        ("fetchMarkets", json!({"params":{"category":"inverse"}})),
        ("fetchMarkets", json!({"params":{"type":"option"}})),
        ("fetchOHLCV", json!({"symbol":NATIVE,"timeframe":"3m"})),
        (
            "fetchOHLCV",
            json!({"symbol":NATIVE,"params":{"price":"mark"}}),
        ),
        ("fetchOrderBook", json!({"symbol":NATIVE,"limit":251})),
        ("fetchTrades", json!({"symbol":"fUSD"})),
    ] {
        let status = if endpoint == "fetchOHLCV" || endpoint == "fetchOrderBook" {
            StatusCode::NOT_IMPLEMENTED
        } else {
            StatusCode::BAD_REQUEST
        };
        h.post(endpoint, body, status).await;
    }
    assert_eq!(h.mock.calls.lock().await.len(), before);
    let stats = h.post("fetchMarketStats", json!({"fields":["funding","lastPrice","markPrice","indexPrice","volume24h","openInterest"]}), StatusCode::OK).await;
    let row = &stats["markets"][0];
    assert_eq!(row["marketId"], perp["marketId"]);
    assert_eq!(
        row["fields"]["lastPrice"]["value"]["amount"], "60003",
        "{stats}"
    );
    assert_eq!(row["fields"]["markPrice"]["value"]["amount"], "60005");
    assert_eq!(row["fields"]["markPrice"]["exchangeTimestamp"], TIME);
    assert_eq!(
        row["fields"]["openInterest"]["value"]["openInterestAmount"],
        123.5
    );
    assert_eq!(row["fields"]["volume24h"]["value"]["baseVolume"], 14.5);
    assert_eq!(row["fields"]["funding"]["state"], "available");
    assert_eq!(row["fields"]["funding"]["value"]["rate"], "0.00001");
    assert_eq!(
        row["fields"]["funding"]["value"]["rateUnit"],
        "decimalFraction"
    );
    assert_eq!(
        row["fields"]["funding"]["value"]["rateIntervalMs"],
        28_800_000
    );
    assert_eq!(
        row["fields"]["funding"]["value"]["paymentIntervalMs"],
        28_800_000
    );
    assert_eq!(row["fields"]["indexPrice"]["state"], "unsupported");
    h.post(
        "fetchMarketStats",
        json!({"params":{"type":"spot"}}),
        StatusCode::OK,
    )
    .await;
    let calls = h.mock.calls.lock().await;
    assert_eq!(
        calls.iter().filter(|u| u.path() == "/v2/tickers").count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|u| u.path() == "/v2/status/deriv")
            .count(),
        1
    );
    drop(calls);
    h.stop().await;
}

#[path = "bitfinex/downstream.rs"]
mod downstream;
#[path = "bitfinex/lifecycle.rs"]
mod lifecycle;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statistics_zero_missing_invalid_and_partial_source_failure() {
    for mode in [1, 2, 3, 4] {
        let h = Harness::start().await;
        h.mock.stats_mode.store(mode, Ordering::SeqCst);
        let stats = h
            .post(
                "fetchMarketStats",
                json!({"fields":["funding","markPrice","lastPrice","openInterest"]}),
                StatusCode::OK,
            )
            .await;
        let fields = &stats["markets"][0]["fields"];
        assert_eq!(fields["lastPrice"]["state"], "available", "{stats}");
        match mode {
            1 => {
                assert_eq!(fields["funding"]["value"]["rate"], "0");
                assert_eq!(fields["openInterest"]["value"]["openInterestAmount"], 0.0);
                assert_eq!(fields["markPrice"]["state"], "unavailable");
            }
            2 => {
                for field in ["funding", "markPrice", "openInterest"] {
                    assert_eq!(fields[field]["state"], "unavailable", "{stats}");
                    assert_eq!(fields[field]["reason"], "invalid-upstream-value", "{stats}");
                }
            }
            3 => {
                assert_eq!(fields["funding"]["state"], "unavailable", "{stats}");
                assert!(
                    stats["coverage"]["sourceFailures"]
                        .as_array()
                        .is_some_and(|rows| !rows.is_empty()),
                    "{stats}"
                );
            }
            4 => assert_eq!(fields["openInterest"]["state"], "unavailable", "{stats}"),
            _ => unreachable!(),
        }
        h.stop().await;
    }
}
