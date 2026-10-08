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
const SPOT: &str = "BTC/USDT";
const PERP: &str = "BTC/USDT:USDT";
const SPOT_NATIVE: &str = "BTC-USDT";
const PERP_NATIVE: &str = "XBTUSDTM";

#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<Uri>>>,
    frames: broadcast::Sender<Value>,
    commands: watch::Sender<Vec<(usize, Value)>>,
    connections: watch::Sender<Vec<usize>>,
    handshakes: watch::Sender<Vec<(usize, Uri)>>,
    serial: Arc<AtomicUsize>,
    tokens: Arc<AtomicUsize>,
    ws_url: Arc<Mutex<String>>,
    stats_mode: Arc<AtomicUsize>,
    expanded_catalog: Arc<std::sync::atomic::AtomicBool>,
    block_bullet: Arc<std::sync::atomic::AtomicBool>,
}

fn contract() -> Value {
    json!({
        "symbol": PERP_NATIVE, "rootSymbol": "USDT", "type": "FFWCSX",
        "firstOpenDate": 1585555200000i64, "expireDate": null, "settleDate": null,
        "baseCurrency": "XBT", "quoteCurrency": "USDT", "settleCurrency": "USDT",
        "maxOrderQty": 1000000, "maxPrice": 1000000, "lotSize": 1, "tickSize": 0.1,
        "indexPriceTickSize": 0.01, "multiplier": 0.001,
        "makerFeeRate": 0.0002, "takerFeeRate": 0.0006, "isInverse": false,
        "status": "Open", "fundingFeeRate": 0.000297, "predictedFundingFeeRate": 0.000327,
        "openInterest": "8033200", "turnoverOf24h": 659795309.25, "volumeOf24h": 9998.54,
        "markPrice": 67193.51, "indexPrice": 67184.81, "lastTradePrice": 67191.8,
        "nextFundingRateTime": 20022985, "maxLeverage": 125,
        "lowPrice": 64041.6, "highPrice": 67737.3, "priceChgPct": 0.0447, "priceChg": 2878.7
    })
}

async fn http(State(mock): State<Mock>, uri: Uri) -> axum::response::Response {
    mock.calls.lock().await.push(uri.clone());
    let is_futures = uri.path().starts_with("/futures/");
    let path = uri.path().strip_prefix("/futures").unwrap_or(uri.path());
    Json(if path == "/api/v2/symbols" {
        json!({"code":"200000","data":[{
            "symbol": SPOT_NATIVE, "name": SPOT_NATIVE, "baseCurrency": "BTC", "quoteCurrency": "USDT",
            "baseIncrement": "0.00000001", "baseMinSize": "0.00001", "baseMaxSize": "10000000000",
            "quoteIncrement": "0.01", "quoteMinSize": "0.01", "quoteMaxSize": "100000000",
            "priceIncrement": "0.1", "enableTrading": true
        }]})
    } else if path == "/api/v1/contracts/active" {
        let mut rows = vec![contract()];
        if mock.expanded_catalog.load(Ordering::SeqCst) {
            let mut inverse = contract();
            inverse["symbol"] = json!("XBTUSDM");
            inverse["quoteCurrency"] = json!("USD");
            inverse["settleCurrency"] = json!("XBT");
            inverse["isInverse"] = json!(true);
            inverse["multiplier"] = json!(-1);
            let mut dated = contract();
            dated["symbol"] = json!("XBTUSDTM-271231");
            dated["expireDate"] = json!(1830211200000u64);
            dated.as_object_mut().unwrap().remove("nextFundingRateTime");
            rows.extend([inverse, dated]);
        }
        json!({"code":"200000","data":rows})
    } else if path == "/api/v1/market/allTickers" {
        json!({"code":"200000","data":{"time":1602832092060i64,"ticker":[{
            "symbol": SPOT_NATIVE, "symbolName": SPOT_NATIVE, "buy": "67190", "sell": "67192",
            "changeRate": "0.01", "changePrice": "100", "high": "68000", "low": "66000",
            "vol": "100.5", "volValue": "6700000", "last": "67191"
        }]}})
    } else if path == "/api/ua/v1/market/open-interest" {
        let mut row = json!({"symbol": PERP_NATIVE, "openInterest": "8033200", "ts": 1774007467050i64});
        match mock.stats_mode.load(Ordering::SeqCst) {
            1 => row["openInterest"] = json!(0),
            2 => row["openInterest"] = json!("broken"),
            3 => row["openInterest"] = json!(null),
            _ => {}
        }
        json!({"code":"200000","data":[row]})
    } else if path == "/api/ua/v2/market/funding-rate" {
        json!({"code":"200000","data":[{
            "symbol": PERP_NATIVE, "nextFundingRate": "0.000297", "fundingTime": 4102444800000i64,
            "fundingRateCap": "0.003", "fundingRateFloor": "-0.003",
            "currentGranularity": 28800000, "newGranularity": 28800000,
            "newGranularityStartTime": 1750147200000i64
        }]})
    } else if path == "/api/v1/market/histories" {
        json!({"code":"200000","data":[{
            "sequence": "1548764654235", "side": "sell", "size": "0.1", "price": "67191",
            "time": 1548848575203567174u64
        }]})
    } else if path == "/api/v1/market/candles" {
        json!({"code":"200000","data":[["1700000000","67191","67200","67210","67180","10","670000"]]})
    } else if path.starts_with("/api/v1/market/orderbook/level2_") {
        json!({"code":"200000","data":{
            "sequence": "1", "time": 1550653727731i64,
            "bids": [["67190","0.5"],["67189","0.7"]], "asks": [["67192","0.3"],["67193","0.4"]]
        }})
    } else if path == "/api/v1/trade/history" {
        json!({"code":"200000","data":[{
            "sequence": 32114961, "side": "buy", "size": 39, "price": "67191.8",
            "takerOrderId": "t1", "makerOrderId": "m1", "tradeId": "tr1",
            "ts": 1640105794099993896u64
        }]})
    } else if path == "/api/v1/kline/query" {
        json!({"code":"200000","data":[[1700000000000i64,"67191","67210","67180","67200","10"]]})
    } else if path == "/api/v1/level2/depth100" || path == "/api/v1/level2/depth20" {
        json!({"code":"200000","data":{
            "symbol": PERP_NATIVE, "sequence": 1,
            "asks": [["67192","1"],["67193","2"]], "bids": [["67190","2"],["67189","3"]],
            "ts": 1604643655040584408u64
        }})
    } else if path == "/api/v1/bullet-public" {
        if mock.block_bullet.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let ws = if is_futures {
            mock.ws_url.lock().await.clone().replace("/ws", "/futures-ws")
        } else {
            mock.ws_url.lock().await.clone()
        };
        json!({"code":"200000","data":{"token":format!("tok{}", mock.tokens.fetch_add(1, Ordering::SeqCst)),"instanceServers":[{
            "endpoint": ws, "pingInterval": 18000, "encrypt": false, "protocol": "websocket"
        }]}})
    } else {
        panic!("unexpected KuCoin request: {uri}")
    })
    .into_response()
}

async fn stream(ws: WebSocketUpgrade, State(mock): State<Mock>, uri: Uri) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let conn = mock.serial.fetch_add(1, Ordering::SeqCst);
        let mut frames = mock.frames.subscribe();
        mock.connections.send_modify(|v| v.push(conn));
        mock.handshakes.send_modify(|v| v.push((conn, uri.clone())));
        loop {
            tokio::select! {
                input = socket.recv() => match input {
                    Some(Ok(ServerMessage::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap();
                        mock.commands.send_modify(|rows| rows.push((conn, v.clone())));
                        if v["type"] == "ping" {
                            let pong = json!({"id":v["id"],"type":"pong"});
                            if socket.send(ServerMessage::Text(pong.to_string())).await.is_err() { break; }
                        }
                        if v["type"] == "subscribe" || v["type"] == "unsubscribe" {
                            let ack = json!({"id": v["id"], "type": "ack"});
                            if socket.send(ServerMessage::Text(ack.to_string())).await.is_err() { break; }
                        }
                    }
                    None | Some(Err(_)) | Some(Ok(ServerMessage::Close(_))) => break,
                    _ => {}
                },
                frame = frames.recv() => match frame {
                    Ok(v) => {
                        // Deliver only to the product socket, as the exchange
                        // does. Broadcasting futures books to a spot-only core
                        // invents unknown markets and masks the actual path.
                        let futures = v["topic"].as_str().unwrap_or_default().starts_with("/contractMarket/");
                        if futures != (uri.path() == "/futures-ws") { continue; }
                        if v["close"] == true { break; }
                        if socket.send(ServerMessage::Text(v.to_string())).await.is_err() { break; }
                    },
                    Err(_) => break
                }
            }
        }
        let _ = socket.close().await;
        mock.connections.send_modify(|v| v.retain(|id| *id != conn));
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mock = Mock {
            calls: Arc::default(),
            frames: broadcast::channel(64).0,
            commands: watch::channel(vec![]).0,
            connections: watch::channel(vec![]).0,
            handshakes: watch::channel(vec![]).0,
            serial: Arc::default(),
            tokens: Arc::default(),
            ws_url: Arc::new(Mutex::new(format!("ws://{addr}/ws"))),
            stats_mode: Arc::default(),
            expanded_catalog: Arc::default(),
            block_bullet: Arc::default(),
        };
        let app = Router::new()
            .route("/ws", get(stream))
            .route("/futures-ws", get(stream))
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
            bitfinex_rest_base_url: "http://127.0.0.1:1".into(),
            bitfinex_ws_url: "ws://127.0.0.1:1".into(),
            kucoin_rest_base_url: format!("http://{addr}"),
            kucoin_futures_rest_base_url: format!("http://{addr}/futures"),
            kucoin_ws_url: format!("ws://{addr}/spot"),
            kucoin_futures_ws_url: format!("ws://{addr}/futures"),
        };
        let service = CcxtService::start(&config).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        registry.register(Arc::new(CcxtExchange::new(Venue::Kucoin, service.clone())));
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
        body["exchange"] = json!("kucoin");
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

    async fn subscribe(
        &self,
        channel: RealtimeChannel,
        symbol: &str,
        params: Value,
    ) -> RealtimeSubscription {
        self.realtime
            .subscribe(
                channel,
                RealtimeTopic {
                    exchange: "kucoin".into(),
                    symbol: symbol.into(),
                    params,
                },
            )
            .await
            .unwrap()
    }

    async fn command(&self, kind: &str, topic: &str, count: usize) -> (usize, Value) {
        let matches = |v: &Value| v["type"] == kind && v["topic"] == topic;
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

    async fn wait_call(&self, fragment: &str) {
        timeout(WINDOW, async {
            loop {
                if self
                    .mock
                    .calls
                    .lock()
                    .await
                    .iter()
                    .any(|uri| uri.path().contains(fragment))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    fn assert_handshake(&self, conn: usize, futures: bool) -> Uri {
        let uri = self
            .mock
            .handshakes
            .borrow()
            .iter()
            .find(|(id, _)| *id == conn)
            .unwrap()
            .1
            .clone();
        assert_eq!(uri.path(), if futures { "/futures-ws" } else { "/ws" });
        let connected = reqwest::Url::parse(&format!("ws://fixture.invalid{uri}")).unwrap();
        let query: std::collections::HashMap<String, String> =
            connected.query_pairs().into_owned().collect();
        assert!(query["token"].starts_with("tok"));
        assert_eq!(query["privateChannel"], "false");
        assert_eq!(
            query["connectId"],
            if futures { "publicFutures" } else { "public" }
        );
        uri
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_rest_bounds_and_bulk_stats() {
    let h = Harness::start().await;
    let markets = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    let rows = markets["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{markets}");
    let spot = rows.iter().find(|row| row["type"] == "spot").unwrap();
    assert_eq!(spot["symbol"], SPOT);
    assert_eq!(spot["exchangeMarketId"], SPOT_NATIVE);
    let perp = rows.iter().find(|row| row["type"] == "perp").unwrap();
    assert_eq!(perp["symbol"], SPOT);
    assert_eq!(perp["exchangeMarketId"], PERP_NATIVE);
    assert_eq!(perp["settle"], "USDT");
    assert_eq!(perp["settlementAssetId"], "USDT");
    assert_eq!(perp["contractSize"], 0.001);

    // Trades: spot and contract share the same public shape but different
    // native endpoints; `since`/`until` are local filters.
    let t = h
        .post(
            "fetchTrades",
            json!({"symbol":SPOT_NATIVE,"since":1548848575000u64,"until":1548848576000u64,"limit":100}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(t[0]["side"], "sell");
    assert_eq!(t[0]["amount"], 0.1);
    let t = h
        .post(
            "fetchTrades",
            json!({"symbol":PERP,"limit":100}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(t[0]["side"], "buy");
    assert_eq!(t[0]["amount"], 39.0);

    let b = h
        .post(
            "fetchOrderBook",
            json!({"symbol":SPOT_NATIVE,"limit":2}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(b["bids"], json!([[67190.0, 0.5], [67189.0, 0.7]]));
    assert_eq!(b["asks"], json!([[67192.0, 0.3], [67193.0, 0.4]]));
    assert_eq!(b["timestamp"], 1550653727731u64);

    let b = h
        .post(
            "fetchOrderBook",
            json!({"symbol":PERP,"limit":2}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(b["bids"], json!([[67190.0, 2.0], [67189.0, 3.0]]));
    assert_eq!(b["nonce"], 1);

    let c = h
        .post(
            "fetchOHLCV",
            json!({"symbol":SPOT_NATIVE,"timeframe":"1m","since":1700000000000u64,"limit":10}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(
        c,
        json!([[1700000000000u64, 67191.0, 67210.0, 67180.0, 67200.0, 10.0]])
    );
    let c = h
        .post(
            "fetchOHLCV",
            json!({"symbol":PERP,"timeframe":"1m","since":1700000000000u64,"limit":10}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(
        c,
        json!([[1700000000000u64, 67191.0, 67210.0, 67180.0, 67200.0, 10.0]])
    );

    for (endpoint, body, status) in [
        (
            "fetchMarkets",
            json!({"params":{"type":"option"}}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "fetchOHLCV",
            json!({"symbol":SPOT_NATIVE,"timeframe":"3m"}),
            StatusCode::OK,
        ),
        (
            "fetchOrderBook",
            json!({"symbol":SPOT_NATIVE,"limit":101}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            "fetchTrades",
            json!({"symbol":"NOPE"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "fetchOHLCV",
            json!({"symbol":SPOT_NATIVE,"params":{"price":"mark"}}),
            StatusCode::NOT_IMPLEMENTED,
        ),
    ] {
        h.post(endpoint, body, status).await;
    }

    for timeframe in ["3m", "6h", "1M", "3min"] {
        h.post(
            "fetchOHLCV",
            json!({"symbol":PERP,"timeframe":timeframe}),
            StatusCode::NOT_IMPLEMENTED,
        )
        .await;
        assert!(h
            .realtime
            .subscribe(
                RealtimeChannel::Ohlcv,
                RealtimeTopic {
                    exchange: "kucoin".into(),
                    symbol: PERP.into(),
                    params: json!({"timeframe":timeframe}),
                }
            )
            .await
            .is_err());
    }
    for (symbol, limit) in [(SPOT_NATIVE, 1501), (PERP, 201)] {
        h.post(
            "fetchOHLCV",
            json!({"symbol":symbol,"limit":limit,"since":1700000000000u64}),
            StatusCode::OK,
        )
        .await;
    }
    h.post(
        "fetchTrades",
        json!({"symbol":SPOT_NATIVE,"limit":101}),
        StatusCode::OK,
    )
    .await;
    h.post(
        "fetchTrades",
        json!({"symbol":SPOT}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchTrades",
        json!({"symbol":SPOT,"params":{"type":"spot"}}),
        StatusCode::OK,
    )
    .await;
    h.post(
        "fetchTrades",
        json!({"symbol":PERP_NATIVE,"params":{"type":"spot"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Default all-market statistics select perpetuals, as documented.
    let stats = h
        .post(
            "fetchMarketStats",
            json!({"fields":["funding","lastPrice","markPrice","indexPrice","volume24h","openInterest"]}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(stats["markets"].as_array().unwrap().len(), 1, "{stats}");
    let perp_row = &stats["markets"][0];
    assert_eq!(perp_row["type"], "perp");
    assert_eq!(
        perp_row["fields"]["lastPrice"]["value"]["amount"],
        "67191.8"
    );
    assert_eq!(
        perp_row["fields"]["markPrice"]["value"]["amount"],
        "67193.51"
    );
    assert_eq!(
        perp_row["fields"]["indexPrice"]["value"]["amount"],
        "67184.81"
    );
    assert_eq!(
        perp_row["fields"]["openInterest"]["value"]["openInterestAmount"],
        8033200.0
    );
    assert_eq!(perp_row["fields"]["funding"]["value"]["rate"], "0.000297");
    assert_eq!(
        perp_row["fields"]["funding"]["value"]["rateUnit"],
        "decimalFraction"
    );
    assert_eq!(perp_row["fields"]["funding"]["value"]["kind"], "estimate");
    assert_eq!(
        perp_row["fields"]["funding"]["value"]["rateIntervalMs"],
        28800000u64
    );
    assert_eq!(
        perp_row["fields"]["funding"]["value"]["nextPaymentTimestamp"],
        4102444800000u64
    );

    // Spot statistics are selected explicitly.
    let stats = h
        .post(
            "fetchMarketStats",
            json!({"fields":["lastPrice","volume24h","funding"],"params":{"type":"spot"}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(stats["markets"].as_array().unwrap().len(), 1, "{stats}");
    let spot_row = &stats["markets"][0];
    assert_eq!(spot_row["type"], "spot");
    assert_eq!(spot_row["fields"]["lastPrice"]["value"]["amount"], "67191");
    assert_eq!(
        spot_row["fields"]["volume24h"]["value"]["baseVolume"],
        100.5
    );
    assert_eq!(
        spot_row["fields"]["volume24h"]["value"]["quoteVolume"],
        6700000.0
    );
    assert_eq!(spot_row["fields"]["funding"]["state"], "notApplicable");
    let calls = h.mock.calls.lock().await.clone();
    let count = |path: &str| calls.iter().filter(|uri| uri.path() == path).count();
    assert_eq!(count("/api/ua/v2/market/funding-rate"), 1);
    assert_eq!(count("/api/ua/v1/market/open-interest"), 1);
    assert_eq!(count("/api/v1/market/allTickers"), 2); // default + explicit spot scopes
    for uri in &calls {
        let parsed = reqwest::Url::parse(&format!("http://fixture.invalid{uri}")).unwrap();
        let query: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        match uri.path() {
            "/api/v1/market/histories" => {
                assert_eq!(query, [("symbol".into(), SPOT_NATIVE.into())].into())
            }
            "/futures/api/v1/trade/history" => {
                assert_eq!(query, [("symbol".into(), PERP_NATIVE.into())].into())
            }
            "/futures/api/v1/kline/query" => {
                assert_eq!(query["granularity"], "1");
                assert_eq!(query["from"], "1700000000000");
                assert!(["1700000600000", "1700012000000"].contains(&query["to"].as_str()));
            }
            _ => {}
        }
    }
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statistics_invalid_and_missing_values_are_unavailable() {
    for mode in [2, 3] {
        let h = Harness::start().await;
        h.mock.stats_mode.store(mode, Ordering::SeqCst);
        let stats = h
            .post(
                "fetchMarketStats",
                json!({"fields":["openInterest"],"params":{"type":"swap"}}),
                StatusCode::OK,
            )
            .await;
        let row = stats["markets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["type"] == "perp")
            .unwrap();
        assert_eq!(
            row["fields"]["openInterest"]["state"], "unavailable",
            "{stats}"
        );
        if mode == 2 {
            assert_eq!(
                row["fields"]["openInterest"]["reason"],
                "invalid-upstream-value"
            );
        }
        h.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inverse_and_dated_catalog_identity_and_selectors() {
    let h = Harness::start().await;
    h.mock.expanded_catalog.store(true, Ordering::SeqCst);
    let all = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    assert_eq!(all["markets"].as_array().unwrap().len(), 4);
    let inverse = h
        .post(
            "fetchMarkets",
            json!({"params":{"category":"inverse"}}),
            StatusCode::OK,
        )
        .await;
    let rows = inverse["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["exchangeMarketId"], "XBTUSDM");
    assert_eq!(rows[0]["settle"], "BTC");
    assert_eq!(rows[0]["settlementAssetId"], "XBT");
    assert_eq!(rows[0]["contractSize"], 1.0);
    let dated = h
        .post(
            "fetchMarkets",
            json!({"params":{"type":"future"}}),
            StatusCode::OK,
        )
        .await;
    let rows = dated["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["exchangeMarketId"], "XBTUSDTM-271231");
    assert_eq!(rows[0]["type"], "future");
    let linear = h
        .post(
            "fetchMarkets",
            json!({"params":{"category":"linear","type":"swap"}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(linear["markets"].as_array().unwrap().len(), 1);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_open_interest_is_not_missing_and_negotiation_shutdown_is_cancellable() {
    let h = Harness::start().await;
    h.mock.stats_mode.store(1, Ordering::SeqCst);
    let stats = h
        .post(
            "fetchMarketStats",
            json!({"fields":["openInterest"]}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(
        stats["markets"][0]["fields"]["openInterest"]["value"]["openInterestAmount"],
        0.0
    );
    h.mock.block_bullet.store(true, Ordering::SeqCst);
    let _sub = h.subscribe(RealtimeChannel::Trades, PERP, json!({})).await;
    h.wait_call("bullet-public").await;
    timeout(Duration::from_millis(750), h.stop())
        .await
        .expect("shutdown must cancel negotiation, not wait for HTTP timeout");
}

fn trade_frame(topic: &str, subject: &str, data: Value) -> Value {
    json!({"type":"message","topic":topic,"subject":subject,"data":data})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_trades_candles_and_books_use_the_negotiated_url() {
    let h = Harness::start().await;

    // Spot trades.
    let mut trades = h
        .subscribe(RealtimeChannel::Trades, SPOT_NATIVE, json!({}))
        .await;
    let (spot_conn, _) = h.command("subscribe", "/market/match:BTC-USDT", 1).await;
    h.assert_handshake(spot_conn, false);
    h.inject(trade_frame(
        "/market/match:BTC-USDT",
        "trade.l3match",
        json!({"sequence":"1","symbol":SPOT_NATIVE,"side":"buy","size":"0.1","price":"67191",
               "takerOrderId":"t1","makerOrderId":"m1","time":"1580559434436443257","tradeId":"tr1"}),
    ));
    match next(&mut trades).await {
        RealtimeUpdate::Trades(rows) => {
            assert_eq!(rows[0].symbol.as_deref(), Some(SPOT));
            assert_eq!(rows[0].side.as_deref(), Some("buy"));
        }
        _ => panic!("expected trades update"),
    }

    // Spot candles.
    let mut candles = h
        .subscribe(
            RealtimeChannel::Ohlcv,
            SPOT_NATIVE,
            json!({"timeframe":"1m"}),
        )
        .await;
    h.command("subscribe", "/market/candles:BTC-USDT_1min", 1)
        .await;
    h.inject(trade_frame(
        "/market/candles:BTC-USDT_1min",
        "trade.candles.update",
        json!({"symbol":SPOT_NATIVE,"candles":["1700000060","67191","67200","67210","67180","10","670000"],"time":1700000060000u64}),
    ));
    match next(&mut candles).await {
        RealtimeUpdate::Ohlcv(rows) => {
            assert_eq!(rows[0].0, 1700000060000u64);
            assert_eq!(rows[0].4, 67200.0);
        }
        _ => panic!("expected candles update"),
    }

    // Spot book: stock buffers deltas until its snapshot delay, then loads the
    // REST snapshot and replays them. Inject the buffered window first.
    let mut book = h
        .subscribe(RealtimeChannel::OrderBook, SPOT_NATIVE, json!({"levels":2}))
        .await;
    h.command("subscribe", "/market/level2:BTC-USDT", 1).await;
    for sequence in 2..8 {
        h.inject(trade_frame(
            "/market/level2:BTC-USDT",
            "trade.l2update",
            json!({"sequenceStart":sequence,"sequenceEnd":sequence + 1,"symbol":SPOT_NATIVE,
                   "changes":{"asks":[["67192","1",sequence]],"bids":[["67190","2.0",sequence + 1]]}}),
        ));
    }
    h.wait_call("orderbook/level2_100").await;
    match next(&mut book).await {
        RealtimeUpdate::OrderBook(rows) => {
            // The REST seed is 67190 @ 0.5. This distinctive value is only
            // present when the buffered sequence was replayed into the stock
            // order book (a stale REST snapshot would retain 0.5).
            assert_eq!(
                rows.bids.iter().find(|(price, _)| *price == 67190.0),
                Some(&(67190.0, 2.0))
            );
            // The first cached update was sequence 2..3; seeing nonce 3
            // proves the REST snapshot was replayed before publication.
            assert_eq!(rows.nonce, Some(3));
            assert_eq!(
                rows.asks.iter().find(|(price, _)| *price == 67192.0),
                Some(&(67192.0, 1.0))
            );
        }
        _ => panic!("expected book update"),
    }

    // A contract feed uses the futures bullet-public negotiation and stock's
    // contract topics, rather than silently falling back to spot. Return a
    // distinct negotiated endpoint so using the spot URL would fail.
    let mut contract_trades = h.subscribe(RealtimeChannel::Trades, PERP, json!({})).await;
    h.wait_call("/futures/api/v1/bullet-public").await;
    let (contract_conn, _) = h
        .command("subscribe", "/contractMarket/execution:XBTUSDTM", 1)
        .await;
    let old_uri = h.assert_handshake(contract_conn, true);
    assert_ne!(spot_conn, contract_conn);
    h.inject(trade_frame(
        "/contractMarket/execution:XBTUSDTM",
        "match",
        json!({"sequence":"2","symbol":PERP_NATIVE,"side":"sell","size":"3","price":"67191.8",
               "takerOrderId":"ct1","makerOrderId":"cm1","ts":1580559434436443257u64,"tradeId":"ctr1"}),
    ));
    assert!(
        matches!(next(&mut contract_trades).await, RealtimeUpdate::Trades(rows)
            if rows[0].amount == Some(3.0) && rows[0].timestamp == Some(1580559434436)
                && rows[0].symbol.as_deref() == Some(PERP))
    );

    let mut contract_candles = h
        .subscribe(RealtimeChannel::Ohlcv, PERP, json!({"timeframe":"1m"}))
        .await;
    h.command("subscribe", "/contractMarket/limitCandle:XBTUSDTM_1min", 1)
        .await;
    h.inject(trade_frame(
        "/contractMarket/limitCandle:XBTUSDTM_1min",
        "trade.candles.update",
        json!({"symbol":PERP_NATIVE,"candles":["1700000060","67191","67200","67210","67180","10"],"time":1700000060000u64}),
    ));
    assert!(
        matches!(next(&mut contract_candles).await, RealtimeUpdate::Ohlcv(rows) if rows[0].0 == 1700000060000)
    );

    // Add the contract book to the already-running futures owner while all
    // spot feeds remain live. Use KuCoin's native sequence/change payload.
    let mut contract_book = h
        .subscribe(RealtimeChannel::OrderBook, PERP, json!({"levels":2}))
        .await;
    h.wait_call("/futures/api/v1/bullet-public").await;
    let (book_conn, _) = h
        .command("subscribe", "/contractMarket/level2:XBTUSDTM", 1)
        .await;
    assert_eq!(book_conn, contract_conn);
    h.inject(trade_frame(
        "/contractMarket/level2:XBTUSDTM",
        "level2",
        json!({"sequence":2,"change":"67190,buy,4.0","timestamp":1700000060000u64}),
    ));
    h.wait_call("level2/depth100").await;
    match next(&mut contract_book).await {
        RealtimeUpdate::OrderBook(rows) => assert_eq!(
            rows.bids.iter().find(|(price, _)| *price == 67190.0),
            Some(&(67190.0, 4.0))
        ),
        _ => panic!("expected contract book update"),
    }
    // Transport loss must negotiate a new token and require a fresh seed.
    h.inject(json!({"topic":"/contractMarket/close", "close":true}));
    assert!(matches!(
        next(&mut contract_book).await,
        RealtimeUpdate::Error(_)
    ));
    let (reconnected, _) = h
        .command("subscribe", "/contractMarket/level2:XBTUSDTM", 2)
        .await;
    assert_ne!(reconnected, contract_conn);
    let new_uri = h.assert_handshake(reconnected, true);
    assert_ne!(old_uri.query(), new_uri.query());
    let endpoint = h.mock.ws_url.lock().await.clone();
    let origin = endpoint.strip_suffix("/ws").unwrap();
    assert!(ccxt_pro::pro::ws_client::get_client(&format!("{origin}{old_uri}")).is_none());
    h.inject(trade_frame(
        "/contractMarket/level2:XBTUSDTM",
        "level2",
        json!({"sequence":2,"change":"67190,buy,7.0","timestamp":1700000061000u64}),
    ));
    assert!(matches!(
        next(&mut contract_book).await,
        RealtimeUpdate::OrderBook(rows)
            if rows.bids.contains(&(67190.0, 7.0))
    ));

    // Stock unsubscribe retires the spot feed without restarting siblings.
    drop(book);
    let (unsub_conn, _) = h.command("unsubscribe", "/market/level2:BTC-USDT", 1).await;
    assert_eq!(unsub_conn, spot_conn);
    // Let the real stock driver timer fire on an otherwise quiet socket.
    let mut commands = h.mock.commands.subscribe();
    timeout(
        WINDOW,
        commands.wait_for(|rows| {
            rows.iter()
                .any(|(id, v)| *id == spot_conn && v["type"] == "ping")
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(h.mock.serial.load(Ordering::SeqCst), 3); // spot + futures + futures reconnect
    drop(contract_book);
    drop(contract_candles);
    drop(contract_trades);
    h.stop().await;
}
