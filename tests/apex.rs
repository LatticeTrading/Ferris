//! Stock Apex integration over loopback HTTP/WS, including native unsubscribe.
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
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
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
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[path = "apex/ordering.rs"]
mod ordering;
#[path = "apex/unsubscribe.rs"]
mod unsubscribe;

const WINDOW: Duration = Duration::from_secs(8);
const SYMBOL: &str = "BTC/USDT:USDT";
const TRADES: &str = "recentlyTrade.H.BTCUSDT";
const BOOK: &str = "orderBook200.H.BTCUSDT";
const CANDLES: &str = "candle.1.BTCUSDT";
const TIME: u64 = 1_700_000_000_000;

#[derive(Clone, Debug)]
struct Connection {
    id: usize,
    uri: String,
    topics: Vec<String>,
}
#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<Uri>>>,
    connections: watch::Sender<Vec<Connection>>,
    serial: Arc<AtomicUsize>,
    frames: broadcast::Sender<(String, Value)>,
    commands: watch::Sender<Vec<(usize, Value)>>,
    ack_mode: Arc<AtomicUsize>, // 0 immediate, 1 withheld, 2 rejected
    before_ack: Arc<Mutex<std::collections::HashMap<String, Vec<Value>>>>,
}
fn market(base: &str) -> Value {
    json!({"symbol":format!("{base}-USDT"), "crossSymbolName":format!("{base}USDT"),
        "baseTokenId":base, "settleAssetId":"USDT", "l2PairId":"50001",
        "enableTrade":true, "isPrelaunch":false, "minOrderSize":"0.001",
        "stepSize":"0.001", "tickSize":"0.1", "category":"L1"})
}
fn ticker(base: &str) -> Value {
    json!({"symbol":format!("{base}USDT"), "fundingRate":"-0.000007030", "predictedFundingRate":"0.002",
        "nextFundingTime":"2099-01-01T01:00:00Z", "markPrice":"85381.800",
        "indexPrice":"85417.61", "lastPrice":"85382.10", "openInterest":"1586.836",
        "volume24h":"3694.438", "turnover24h":"316623462.3562"})
}
async fn http(State(mock): State<Mock>, uri: Uri) -> Json<Value> {
    mock.calls.lock().await.push(uri.clone());
    Json(match uri.path() {
        "/api/v3/symbols" => {
            assert!(uri.query().is_none());
            let mut old = market("OLD");
            old["enableTrade"] = json!(false);
            let mut pre = market("PRE");
            pre["isPrelaunch"] = json!(true);
            let mut reduce = market("REDUCE");
            reduce["enableOpenPosition"] = json!(false);
            reduce["enableDisplay"] = json!(false);
            json!({"data":{"contractConfig":{"perpetualContract":[market("BTC"), old, pre, reduce],
                "stockContract":[market("AAPL")], "predictionContract":[market("YES")]}}})
        }
        "/api/v3/data/all-ticker-info" => {
            assert!(uri.query().is_none(), "bulk must not fan out or filter");
            json!({"data":[ticker("BTC"), ticker("REDUCE"), ticker("AAPL")]})
        }
        "/api/v3/trades" => json!({"data":[
            {"i":"1", "p":"60000", "S":"Buy", "v":"0.01", "s":"BTCUSDT", "T":TIME+1},
            {"i":"2", "p":"60001", "S":"Sell", "v":"0.02", "s":"BTCUSDT", "T":TIME+2},
            {"i":"3", "p":"60002", "S":"Buy", "v":"0.03", "s":"BTCUSDT", "T":TIME+3}]}),
        "/api/v3/depth" => json!({"data":{"s":"BTCUSDT", "u":42,
            "b":[["60000","2"],["59999","3"],["59998","4"]], "a":[["60001","5"],["60002","6"]]}}),
        "/api/v3/klines" => json!({"data":{"BTCUSDT":[
            {"t":TIME,"o":"60000","h":"60002","l":"59999","c":"60001","v":"3"},
            {"t":TIME+60000,"o":"60001","h":"60003","l":"60000","c":"60002","v":"4"}]}}),
        path => panic!("unexpected Apex request: {path}"),
    })
}
async fn stream(ws: WebSocketUpgrade, State(mock): State<Mock>, uri: Uri) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let id = mock.serial.fetch_add(1, Ordering::SeqCst);
        let mut frames = mock.frames.subscribe();
        mock.connections.send_modify(|rows| rows.push(Connection { id, uri: uri.to_string(), topics: vec![] }));
        let mut topics = Vec::new();
        loop {
            tokio::select! {
                frame = socket.recv() => match frame {
                    Some(Ok(ServerMessage::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if value["op"] == "ping" { continue; }
                        let unsubscribe = value["op"] == "unsubscribe";
                        let mode = mock.ack_mode.load(Ordering::SeqCst);
                        assert!(unsubscribe || value["op"] == "subscribe");
                        for topic in value["args"].as_array().unwrap() {
                            let topic = topic.as_str().unwrap().to_string();
                            if unsubscribe {
                                if mode != 2 { topics.retain(|held| held != &topic); }
                            } else {
                                assert!(!topics.contains(&topic), "duplicate native subscription");
                                topics.push(topic);
                            }
                        }
                        if !unsubscribe {
                            for topic in value["args"].as_array().unwrap() {
                                let seeds = mock.before_ack.lock().await.remove(topic.as_str().unwrap()).unwrap_or_default();
                                for seed in seeds {
                                    socket.send(ServerMessage::Text(seed.to_string())).await.unwrap();
                                }
                            }
                        }
                        mock.commands.send_modify(|commands| commands.push((id, value.clone())));
                        mock.connections.send_modify(|rows| rows.iter_mut().find(|row| row.id == id).unwrap().topics = topics.clone());
                        if !unsubscribe || mode != 1 {
                            let ack = json!({"success":!unsubscribe || mode != 2,"ret_msg":"","request":value});
                            if socket.send(ServerMessage::Text(ack.to_string())).await.is_err() { break; }
                        }
                    }
                    Some(Ok(ServerMessage::Close(_))) | Some(Err(_)) | None => break,
                    _ => {},
                },
                frame = frames.recv() => match frame {
                    Ok((topic, _)) if topic == "close" => break,
                    Ok((topic, value)) if topics.contains(&topic) || topic == "inject" => {
                        if socket.send(ServerMessage::Text(value.to_string())).await.is_err() { break; }
                    }
                    Ok(_) => {},
                    Err(_) => break,
                },
            }
        }
        let _ = socket.close().await;
        mock.connections.send_modify(|rows| rows.retain(|row| row.id != id));
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
            connections: watch::channel(vec![]).0,
            serial: Arc::default(),
            frames: broadcast::channel(32).0,
            commands: watch::channel(vec![]).0,
            ack_mode: Arc::default(),
            before_ack: Arc::default(),
        };
        let upstream = Router::new()
            .route("/realtime_public", get(stream))
            .fallback(http)
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let up_task = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            request_timeout_ms: 2_000,
            hyperliquid_base_url: "http://127.0.0.1:1".into(),
            extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
            extended_ws_url: "ws://127.0.0.1:1".into(),
            lighter_rest_base_url: "http://127.0.0.1:1".into(),
            lighter_ws_url: "ws://127.0.0.1:1".into(),
            binance_base_url: "http://127.0.0.1:1".into(),
            bybit_base_url: "http://127.0.0.1:1".into(),
            aster_base_url: "http://127.0.0.1:1".into(),
            apex_rest_base_url: format!("http://{addr}/api"),
            apex_ws_url: format!("ws://{addr}/realtime_public?v=2"),
        };
        let service = CcxtService::start(&config).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        for venue in Venue::ALL {
            registry.register(Arc::new(CcxtExchange::new(venue, service.clone())));
        }
        let state = AppState::new(Arc::new(registry), realtime.clone());
        let app = Router::new()
            .route("/v1/capabilities", get(web::capabilities))
            .route("/v1/fetchMarkets", post(web::fetch_markets))
            .route("/v1/fetchTrades", post(web::fetch_trades))
            .route("/v1/fetchOrderBook", post(web::fetch_order_book))
            .route("/v1/fetchOHLCV", post(web::fetch_ohlcv))
            .route("/v1/fetchMarketStats", post(web::fetch_market_stats))
            .route("/v1/ws", get(web::trades_stream_ws))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            mock,
            service,
            realtime,
            state,
            base,
            tasks: vec![up_task, task],
        }
    }
    async fn post(&self, endpoint: &str, mut body: Value, expected: StatusCode) -> Value {
        body["exchange"] = json!("apex");
        let response = reqwest::Client::new()
            .post(format!("{}/v1/{endpoint}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value: Value = response.json().await.unwrap();
        assert_eq!(status, expected, "{endpoint}: {value}");
        value
    }
    async fn subscribe(&self, channel: RealtimeChannel, params: Value) -> RealtimeSubscription {
        self.realtime
            .subscribe(
                channel,
                RealtimeTopic {
                    exchange: "apex".into(),
                    symbol: "BTCUSDT".into(),
                    params,
                },
            )
            .await
            .unwrap()
    }
    async fn connected(&self, id: Option<usize>, topics: &[&str]) -> Connection {
        let mut rx = self.mock.connections.subscribe();
        let connection = timeout(
            WINDOW,
            rx.wait_for(|rows| {
                rows.len() == 1
                    && id.is_none_or(|id| rows[0].id > id)
                    && rows[0].topics.len() == topics.len()
                    && topics
                        .iter()
                        .all(|topic| rows[0].topics.contains(&topic.to_string()))
            }),
        )
        .await
        .unwrap()
        .unwrap()[0]
            .clone();
        connection
    }
    fn inject(&self, value: Value) {
        self.mock.frames.send(("inject".into(), value)).unwrap();
    }
    async fn command(&self, op: &str, topic: &str, count: usize) -> Value {
        let mut commands = self.mock.commands.subscribe();
        let rows = timeout(
            WINDOW,
            commands.wait_for(|rows| {
                rows.iter()
                    .filter(|(_, row)| {
                        row["op"] == op
                            && row["args"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|value| value == topic)
                    })
                    .count()
                    >= count
            }),
        )
        .await
        .unwrap()
        .unwrap();
        rows.iter()
            .rev()
            .find(|(_, row)| {
                row["op"] == op
                    && row["args"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|value| value == topic)
            })
            .unwrap()
            .1
            .clone()
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
fn id(base: &str) -> String {
    json!(["apex", "perp", null, null, format!("{base}USDT")]).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apex_http_snapshots_aliases_limits_and_bulk_statistics() {
    let h = Harness::start().await;
    let caps: Value = reqwest::get(format!("{}/v1/capabilities", h.base))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let caps = caps["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["exchange"] == "apex")
        .unwrap();
    assert_eq!(caps["marketStats"]["upstreamMode"], "sharedPolling");
    assert_eq!(
        caps["marketStats"]["fields"]["perp"]["openInterest"]["state"],
        "supported"
    );
    assert!(h.mock.calls.lock().await.is_empty());
    let markets = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    let rows = markets["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 2); // OLD and prelaunch excluded; hidden/reduce-only retained.
    let btc = rows.iter().find(|row| row["base"] == "BTC").unwrap();
    assert_eq!(btc["exchangeMarketId"], "BTCUSDT");
    assert_eq!(btc["contractSize"], Value::Null);
    assert_eq!(btc["minOrderSize"], 0.001);
    assert_eq!(btc["info"]["category"], Value::Null); // L1 is not a product selector.
    let all = h
        .post(
            "fetchMarkets",
            json!({"includeInactive":true}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(all["markets"].as_array().unwrap().len(), 4);
    for symbol in ["BTC", "BTC-USDT", "BTCUSDT", "BTC/USDT", SYMBOL, &id("BTC")] {
        let book = h
            .post(
                "fetchOrderBook",
                json!({"symbol":symbol,"limit":1}),
                StatusCode::OK,
            )
            .await;
        assert_eq!(book["symbol"], SYMBOL);
        assert_eq!(book["nonce"], 42);
        assert_eq!(book["bids"], json!([[60000.0, 2.0]]));
    }
    let trades = h
        .post(
            "fetchTrades",
            json!({"symbol":"BTC","since":TIME+1,"limit":1500,"params":{"until":TIME+2}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(trades.as_array().unwrap().len(), 2);
    assert_eq!(trades[0]["id"], "2");
    assert_eq!(trades[0]["amount"], 0.02);
    assert_eq!(trades[0]["cost"], Value::Null); // stock's minimum-size multiplier is invalid
    let candles = h.post("fetchOHLCV", json!({"symbol":"BTC","timeframe":"1m","since":TIME,"limit":500,"params":{"endTime":TIME+60000}}), StatusCode::OK).await;
    assert_eq!(candles.as_array().unwrap().len(), 2);
    h.post(
        "fetchOHLCV",
        json!({"symbol":"BTC","timeframe":"M"}),
        StatusCode::OK,
    )
    .await;
    let before = h.mock.calls.lock().await.len();
    for (endpoint, body) in [
        ("fetchOrderBook", json!({"symbol":"BTC","limit":201})),
        ("fetchOHLCV", json!({"symbol":"BTC","timeframe":"3m"})),
        (
            "fetchOHLCV",
            json!({"symbol":"BTC","params":{"price":"mark"}}),
        ),
        (
            "fetchOHLCV",
            json!({"symbol":"BTC","params":{"candleType":"index-prices"}}),
        ),
    ] {
        h.post(endpoint, body, StatusCode::NOT_IMPLEMENTED).await;
    }
    h.post(
        "fetchOrderBook",
        json!({"symbol":"NOPE"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchMarkets",
        json!({"params":{"type":"spot"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(h.mock.calls.lock().await.len(), before);
    let stats = h.post("fetchMarketStats", json!({"fields":["funding","markPrice","indexPrice","lastPrice","volume24h","openInterest","lastSettledFunding"]}), StatusCode::OK).await;
    assert_eq!(stats["coverage"]["expectedMarkets"], 2);
    let rows = stats["markets"].as_array().unwrap();
    let btc = rows
        .iter()
        .find(|row| row["marketId"] == id("BTC"))
        .unwrap();
    let fields = &btc["fields"];
    assert_eq!(fields["funding"]["value"]["rate"], "-0.000007030");
    assert_eq!(fields["funding"]["value"]["kind"], "currentUnclassified");
    assert_eq!(fields["funding"]["value"]["rateUnit"], "decimalFraction");
    assert_eq!(fields["funding"]["value"]["rateIntervalMs"], 3_600_000);
    assert!(
        fields["funding"]["value"]["nextPaymentTimestamp"]
            .as_u64()
            .unwrap()
            > TIME
    );
    assert_eq!(fields["lastSettledFunding"]["state"], "unsupported");
    assert_eq!(fields["markPrice"]["value"]["amount"], "85381.800");
    assert_eq!(
        fields["openInterest"]["value"],
        json!({"openInterestAmount":1586.836,"openInterestValue":null})
    );
    assert_eq!(
        fields["volume24h"]["value"],
        json!({"baseVolume":3694.438,"quoteVolume":316623462.3562})
    );
    for field in [
        "funding",
        "markPrice",
        "indexPrice",
        "lastPrice",
        "volume24h",
        "openInterest",
    ] {
        assert_eq!(fields[field]["state"], "available");
        assert_eq!(fields[field]["exchangeTimestamp"], Value::Null);
        assert!(fields[field]["receivedTimestamp"].is_u64());
    }
    h.post(
        "fetchMarketStats",
        json!({"marketIds":[id("BTC")]}),
        StatusCode::OK,
    )
    .await;
    let (mut ws, _) = connect_async(format!("{}/v1/ws", h.base.replace("http://", "ws://")))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"op":"subscribe","channel":"marketstats","exchange":"apex","params":{}}).to_string(),
    ))
    .await
    .unwrap();
    let snapshot = timeout(WINDOW, async {
        loop {
            let message = ws.next().await.unwrap().unwrap();
            if let Message::Text(text) = message {
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_ne!(value["type"], "error", "{value}");
                if value["mode"] == "snapshot" {
                    break value;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshot["type"], "marketstats");
    ws.close(None).await.unwrap();
    let calls = h.mock.calls.lock().await.clone();
    assert_eq!(
        calls
            .iter()
            .filter(|uri| uri.path() == "/api/v3/symbols")
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|uri| uri.path() == "/api/v3/data/all-ticker-info")
            .count(),
        1
    );
    let trades = calls
        .iter()
        .find(|uri| uri.path().ends_with("/trades"))
        .unwrap()
        .query()
        .unwrap();
    assert!(trades.contains("symbol=BTCUSDT") && trades.contains("limit=1000"));
    assert!(
        !trades.contains("start")
            && !trades.contains("since")
            && !trades.contains("until")
            && !trades.contains("end")
    );
    let candles: Vec<_> = calls
        .iter()
        .filter(|uri| uri.path().ends_with("/klines"))
        .map(|uri| uri.query().unwrap())
        .collect();
    assert!(
        candles[0].contains("start=1700000000")
            && candles[0].contains("end=1700000060")
            && candles[0].contains("limit=200")
    );
    assert!(candles[1].contains("interval=M"));
    h.stop().await;
}

async fn next(sub: &mut RealtimeSubscription) -> RealtimeUpdate {
    timeout(WINDOW, async {
        loop {
            match sub.receiver.recv().await.unwrap() {
                RealtimeUpdate::Error(_) => continue, // explicit invalidation on reconnect
                update => break update,
            }
        }
    })
    .await
    .unwrap()
}
fn book_frame(snapshot: bool, amount: &str) -> Value {
    json!({"topic":BOOK,"type":if snapshot {"snapshot"} else {"delta"},"ts":TIME*1000,
        "data":{"s":"BTCUSDT","u":42,"b":[["60000",amount]],"a":[["60001","3"]]}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apex_mixed_streams_unsubscribe_without_interrupting_other_feeds() {
    let h = Harness::start().await;
    for (channel, params) in [
        (RealtimeChannel::OrderBook, json!({"levels":201})),
        (RealtimeChannel::OrderBook, json!({"levels":0})),
        (RealtimeChannel::Ohlcv, json!({"price":"mark"})),
        (RealtimeChannel::Ohlcv, json!({"timeframe":"3m"})),
    ] {
        assert!(h
            .realtime
            .subscribe(
                channel,
                RealtimeTopic {
                    exchange: "apex".into(),
                    symbol: "BTC".into(),
                    params,
                }
            )
            .await
            .is_err());
    }
    assert!(h.mock.connections.borrow().is_empty());
    let mut trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let mut books = h
        .subscribe(RealtimeChannel::OrderBook, json!({"levels":1}))
        .await;
    let mut deep = h
        .subscribe(RealtimeChannel::OrderBook, json!({"levels":200}))
        .await;
    let candles_month = h
        .subscribe(RealtimeChannel::Ohlcv, json!({"timeframe":"M"}))
        .await;
    h.command("subscribe", "candle.M.BTCUSDT", 1).await;
    let mut candles = h
        .subscribe(RealtimeChannel::Ohlcv, json!({"timeframe":"1m"}))
        .await;
    assert_eq!(books.levels_limit, 1);
    assert_eq!(deep.levels_limit, 200);
    drop(candles_month);
    h.command("unsubscribe", "candle.M.BTCUSDT", 1).await;
    let first = h.connected(None, &[TRADES, BOOK, CANDLES]).await;
    assert!(first.uri.contains("v=2&timestamp="));
    for sequence in [1, 2] {
        h.mock.frames.send((TRADES.into(), json!({"topic":TRADES,"data":[{"i":sequence.to_string(),"p":"60000","S":"Buy","v":"0.02","s":"BTCUSDT","T":TIME+sequence}]}))).unwrap();
        h.mock
            .frames
            .send((
                BOOK.into(),
                book_frame(sequence == 1, &sequence.to_string()),
            ))
            .unwrap();
        h.mock.frames.send((CANDLES.into(), json!({"topic":CANDLES,"data":[{"start":TIME,"open":"60000","high":"60003","low":"59999","close":(60000+sequence).to_string(),"volume":"3"}]}))).unwrap();
        let RealtimeUpdate::Trades(rows) = next(&mut trades).await else {
            panic!("trades");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_deref(), Some(sequence.to_string().as_str()));
        assert_eq!(rows[0].symbol.as_deref(), Some(SYMBOL));
        assert_eq!(rows[0].cost, None);
        assert_eq!(rows[0].amount, Some(0.02));
        for sub in [&mut books, &mut deep] {
            let RealtimeUpdate::OrderBook(book) = next(sub).await else {
                panic!("book");
            };
            assert_eq!(book.bids, [(60000.0, sequence as f64)]);
        }
        let RealtimeUpdate::Ohlcv(rows) = next(&mut candles).await else {
            panic!("candles");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].4, (60000 + sequence) as f64);
    }
    assert!(
        timeout(Duration::from_millis(150), trades.receiver.recv())
            .await
            .is_err(),
        "no replay on silence"
    );
    drop(books); // depth viewer shares the same acquisition; no restart
    assert_eq!(
        h.connected(None, &[TRADES, BOOK, CANDLES]).await.id,
        first.id
    );
    drop(trades);
    let second = h.connected(None, &[BOOK, CANDLES]).await;
    assert_eq!(
        first.id, second.id,
        "native unsubscribe must preserve other streams"
    );
    assert_eq!(first.uri, second.uri);
    h.mock
        .frames
        .send((BOOK.into(), book_frame(false, "7")))
        .unwrap();
    let RealtimeUpdate::OrderBook(book) = timeout(WINDOW, deep.receiver.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("unrelated book interrupted by trade unsubscribe");
    };
    assert_eq!(book.bids, [(60000.0, 7.0)]);
    // Upstream disconnect also refreshes timestamp and restores current demand.
    h.mock.frames.send(("close".into(), Value::Null)).unwrap();
    let third = h.connected(Some(second.id), &[BOOK, CANDLES]).await;
    assert_ne!(second.uri, third.uri);
    // A later symbol calls stock set_markets again: the id2 index must survive
    // for both the existing BTC cache and the newly added native ID.
    let mut added = h
        .realtime
        .subscribe(
            RealtimeChannel::OrderBook,
            RealtimeTopic {
                exchange: "apex".into(),
                symbol: "REDUCE".into(),
                params: json!({"levels":1}),
            },
        )
        .await
        .unwrap();
    let reduce_topic = "orderBook200.H.REDUCEUSDT";
    assert_eq!(
        h.connected(None, &[BOOK, CANDLES, reduce_topic]).await.id,
        third.id
    );
    let mut frame = book_frame(true, "8");
    frame["topic"] = json!(reduce_topic);
    frame["data"]["s"] = json!("REDUCEUSDT");
    h.mock.frames.send((reduce_topic.into(), frame)).unwrap();
    let RealtimeUpdate::OrderBook(book) = next(&mut added).await else {
        panic!("new market");
    };
    assert_eq!(book.symbol.as_deref(), Some("REDUCE/USDT:USDT"));
    assert_eq!(book.bids, [(60000.0, 8.0)]);
    drop(added);
    let fourth = h.connected(None, &[BOOK, CANDLES]).await;
    assert_eq!(third.id, fourth.id);
    let readded = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    assert_eq!(
        h.connected(None, &[BOOK, CANDLES, TRADES]).await.id,
        fourth.id
    );
    drop(candles);
    h.command("unsubscribe", CANDLES, 1).await;
    assert_eq!(h.connected(None, &[BOOK, TRADES]).await.id, fourth.id);
    drop((deep, readded));
    let mut connections = h.mock.connections.subscribe();
    timeout(WINDOW, connections.wait_for(Vec::is_empty))
        .await
        .unwrap()
        .unwrap();
    h.stop().await;
}
