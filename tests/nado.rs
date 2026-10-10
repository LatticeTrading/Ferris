//! Pinned stock Nado methods exercised through Ferris HTTP/WS over loopback.
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
const PERP: &str = "BTC/USDT0:USDT0";
const SPOT: &str = "BTC/USDT0";
const NATIVE: &str = "BTC-PERP_USDT0";
const TIME: u64 = 1700000040000;

#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<(Uri, Value)>>>,
    frames: broadcast::Sender<Value>,
    commands: watch::Sender<Vec<(usize, Value)>>,
    connections: watch::Sender<Vec<usize>>,
    serial: Arc<AtomicUsize>,
    mode: Arc<AtomicUsize>,
}
fn symbol(id: u32, name: &str, kind: &str) -> Value {
    json!({"product_id":id,"symbol":name,"type":kind,"trading_status":"live",
        "price_increment_x18":"100000000000000000","size_increment":"1000000000000000",
        "min_size":"100000000000000000000","maker_fee_rate_x18":"100000000000000",
        "taker_fee_rate_x18":"300000000000000"})
}
fn candle() -> Value {
    json!({"product_id":2,"granularity":60,"timestamp":"1700000040",
        "open_x18":"60000000000000000000000","high_x18":"60002000000000000000000",
        "low_x18":"59999000000000000000000","close_x18":"60001000000000000000000",
        "volume":"2500000000000000000"})
}
fn trade(id: u32, ts: u64) -> Value {
    json!({"type":"trade","product_id":id,"timestamp":format!("{}", ts * 1_000_000),
        "price":"60000000000000000000000","taker_qty":"500000000000000000",
        "maker_qty":"500000000000000000","is_taker_buyer":true})
}
async fn http(
    State(mock): State<Mock>,
    uri: Uri,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    mock.calls.lock().await.push((uri.clone(), body.clone()));
    let query: std::collections::HashMap<String, String> =
        reqwest::Url::parse(&format!("http://fixture{uri}"))
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
    let spot = query.get("ticker_id").is_some_and(|v| v == "BTC_USDT0");
    let id = if spot { 1 } else { 2 };
    let payload = match uri.path() {
        "/gateway/v1/symbols" => json!([symbol(1, "BTC", "spot"), symbol(2, "BTC-PERP", "perp")]),
        "/gateway/v2/pairs" => json!([
            {"product_id":1,"ticker_id":"BTC_USDT0","base":"BTC","quote":"USDT0"},
            {"product_id":2,"ticker_id":NATIVE,"base":"BTC-PERP","quote":"USDT0"}
        ]),
        "/gateway/v2/assets" => json!([
            {"product_id":0,"symbol":"USDT0","name":"USDT0","can_deposit":true,"can_withdraw":true},
            {"product_id":1,"symbol":"BTC","name":"Bitcoin","can_deposit":true,"can_withdraw":true},
            {"product_id":2,"symbol":"BTC-PERP","name":"Bitcoin Perpetual"}
        ]),
        "/gateway/v2/orderbook" => json!({"product_id":id,"ticker_id":query["ticker_id"],
            "bids":[[60000,2],[59999,3]],"asks":[[60001,4],[60002,5]],"timestamp":TIME}),
        "/archive/v2/trades" => json!([
            {"product_id":id,"trade_id":7,"price":60000,"base_filled":-0.5,"quote_filled":30000,
             "timestamp":1700000040,"trade_type":"sell"},
            {"product_id":id,"trade_id":8,"price":60001,"base_filled":0.25,"quote_filled":15000.25,
             "timestamp":1700000041,"trade_type":"buy"}
        ]),
        "/archive/v1" => json!({"candlesticks":[candle()]}),
        "/archive/v2/tickers" => json!({
            "BTC_USDT0":{"product_id":1,"last_price":"60000.25","base_volume":10,"quote_volume":600000},
            "BTC-PERP_USDT0":{"product_id":2,"last_price":"60001.50","base_volume":20,"quote_volume":1200000}
        }),
        "/archive/v2/contracts" => {
            if mock.mode.load(Ordering::SeqCst) == 4 {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error":"fixture outage"})),
                )
                    .into_response();
            }
            let mut row = json!({"product_id":2,"ticker_id":NATIVE,"base_currency":"BTC-PERP","quote_currency":"USDT0",
                "mark_price":"60002.125","index_price":"60000.125","funding_rate":"-0.0024",
                "next_funding_rate_timestamp":4102444800u64,"open_interest":12.5,"open_interest_usd":750000});
            match mock.mode.load(Ordering::SeqCst) {
                1 => {
                    row["open_interest"] = json!(0);
                    row["open_interest_usd"] = json!(0);
                    row["funding_rate"] = json!("0");
                }
                2 => {
                    row["open_interest"] = json!("broken");
                    row["mark_price"] = json!("NaN");
                    row["funding_rate"] = json!("broken");
                }
                3 => {
                    row.as_object_mut().unwrap().remove("open_interest");
                    row.as_object_mut().unwrap().remove("open_interest_usd");
                }
                _ => {}
            }
            json!({NATIVE:row})
        }
        _ => panic!("unexpected Nado request: {uri} {body}"),
    };
    Json(payload).into_response()
}
async fn stream(ws: WebSocketUpgrade, State(mock): State<Mock>) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let conn = mock.serial.fetch_add(1, Ordering::SeqCst);
        let mut frames = mock.frames.subscribe();
        mock.connections.send_modify(|v| v.push(conn));
        loop {
            tokio::select! {
                input = socket.recv() => match input {
                    Some(Ok(ServerMessage::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap();
                        mock.commands.send_modify(|rows| rows.push((conn,v.clone())));
                        let ack = if v["method"] == "ping" {
                            json!({"id":v["id"],"result":{"method":"pong","client_time":v["client_time"],"server_time":"1700000040000"}})
                        } else { json!({"id":v["id"],"result":null}) };
                        if socket.send(ServerMessage::Text(ack.to_string())).await.is_err() { break; }
                    },
                    None | Some(Err(_)) | Some(Ok(ServerMessage::Close(_))) => break,
                    _ => {}
                },
                frame = frames.recv() => match frame {
                    Ok(v) => {
                        if v["close"] == true { break; }
                        if socket.send(ServerMessage::Text(v.to_string())).await.is_err() { break; }
                    },
                    Err(_) => break,
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
            serial: Arc::default(),
            mode: Arc::default(),
        };
        let app = Router::new()
            .route("/ws", get(stream))
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
            kucoin_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_futures_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_ws_url: "ws://127.0.0.1:1".into(),
            kucoin_futures_ws_url: "ws://127.0.0.1:1".into(),
            nado_gateway_base_url: format!("http://{addr}/gateway"),
            nado_archive_base_url: format!("http://{addr}/archive"),
            nado_ws_url: format!("ws://{addr}/ws"),
        };
        let service = CcxtService::start(&config).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        registry.register(Arc::new(CcxtExchange::new(Venue::Nado, service.clone())));
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
        body["exchange"] = json!("nado");
        let res = reqwest::Client::new()
            .post(format!("{}/v1/{endpoint}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let actual = res.status();
        let v = res.json().await.unwrap();
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
                    exchange: "nado".into(),
                    symbol: symbol.into(),
                    params,
                },
            )
            .await
            .unwrap()
    }
    async fn command(
        &self,
        kind: &str,
        id: u32,
        granularity: Option<u32>,
        count: usize,
    ) -> (usize, Value) {
        let matches = |v: &Value| {
            v["method"] == "subscribe"
                && v["stream"]["type"] == kind
                && v["stream"]["product_id"] == id
                && granularity.is_none_or(|n| v["stream"]["granularity"] == n)
        };
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_rest_and_bulk_statistics() {
    let h = Harness::start().await;
    let markets = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    let rows = markets["markets"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{markets}");
    let perp = rows.iter().find(|r| r["type"] == "perp").unwrap();
    assert_eq!(perp["exchangeMarketId"], "2");
    assert_eq!(perp["settle"], "USDT0");
    assert_eq!(perp["settlementAssetId"], "0");
    assert_eq!(perp["contractSize"], 1.0);
    assert_eq!(perp["tickSize"], 0.1);
    assert!(perp["minOrderSize"].is_null());
    assert_eq!(perp["info"]["rawSymbol"], NATIVE);
    for symbol in [
        NATIVE,
        PERP,
        "2",
        perp["marketId"].as_str().unwrap(),
        "BTC_USDT0",
        "1",
    ] {
        let t = h
            .post(
                "fetchTrades",
                json!({"symbol":symbol,"limit":999,"since":TIME}),
                StatusCode::OK,
            )
            .await;
        assert_eq!(t[0]["id"], "8");
        assert_eq!(t[1]["amount"], 0.5);
        assert_eq!(t[1]["cost"], 30000.0);
        assert_eq!(t[1]["timestamp"], TIME);
        assert_eq!(t[1]["side"], "sell");
        assert!(t[1]["takerOrMaker"].is_null());
    }
    let b = h
        .post(
            "fetchOrderBook",
            json!({"symbol":NATIVE,"limit":1}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(b["symbol"], PERP);
    assert_eq!(b["bids"], json!([[60000.0, 2.0]]));
    assert_eq!(b["timestamp"], TIME);
    assert!(b["nonce"].is_null());
    for timeframe in ["1m", "5m", "15m", "1h", "2h", "4h", "1d", "1w", "4w"] {
        let c = h.post("fetchOHLCV",json!({"symbol":NATIVE,"timeframe":timeframe,"since":TIME,"limit":999,"params":{"until":TIME+60000}}),StatusCode::OK).await;
        assert_eq!(c, json!([[TIME, 60000.0, 60002.0, 59999.0, 60001.0, 2.5]]));
    }
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
        json!({"symbol":NATIVE,"params":{"type":"spot"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchTrades",
        json!({"symbol":"BTC/USDT:USDT"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    for params in [json!({"category":"inverse"}), json!({"type":"option"})] {
        h.post(
            "fetchMarkets",
            json!({"params":params}),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for timeframe in ["3m", "30m", "6h", "8h", "12h", "1M"] {
        h.post(
            "fetchOHLCV",
            json!({"symbol":NATIVE,"timeframe":timeframe}),
            StatusCode::NOT_IMPLEMENTED,
        )
        .await;
    }
    h.post(
        "fetchOHLCV",
        json!({"symbol":NATIVE,"params":{"price":"mark"}}),
        StatusCode::NOT_IMPLEMENTED,
    )
    .await;
    h.post(
        "fetchOrderBook",
        json!({"symbol":NATIVE,"limit":101}),
        StatusCode::NOT_IMPLEMENTED,
    )
    .await;
    assert!(h
        .realtime
        .subscribe(
            RealtimeChannel::OrderBook,
            RealtimeTopic {
                exchange: "nado".into(),
                symbol: NATIVE.into(),
                params: json!({})
            }
        )
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("synchronization"));
    let s = h.post("fetchMarketStats",json!({"fields":["funding","lastSettledFunding","lastPrice","markPrice","indexPrice","volume24h","openInterest"]}),StatusCode::OK).await;
    assert_eq!(s["markets"].as_array().unwrap().len(), 1, "{s}");
    let f = &s["markets"][0]["fields"];
    assert_eq!(f["lastPrice"]["value"]["amount"], "60001.50");
    assert_eq!(f["markPrice"]["value"]["amount"], "60002.125");
    assert_eq!(f["indexPrice"]["value"]["amount"], "60000.125");
    assert_eq!(f["markPrice"]["value"]["quoteAsset"], "USDT0");
    assert_eq!(
        f["openInterest"]["value"],
        json!({"openInterestAmount":12.5,"openInterestValue":750000.0})
    );
    assert_eq!(
        f["volume24h"]["value"],
        json!({"baseVolume":20.0,"quoteVolume":1200000.0})
    );
    let funding = &f["funding"]["value"];
    assert_eq!(funding["rate"], "-0.0024");
    assert_eq!(funding["rateUnit"], "decimalFraction");
    assert_eq!(funding["rateIntervalMs"], 86400000);
    assert_eq!(funding["paymentIntervalMs"], 3600000);
    assert_eq!(funding["nextPaymentTimestamp"], 4102444800000u64);
    assert_eq!(funding["kind"], "currentUnclassified");
    assert_eq!(
        funding["equivalents"]["oneHourPercent"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap(),
        -0.01
    );
    assert_eq!(f["lastSettledFunding"]["state"], "unsupported");
    for name in [
        "funding",
        "lastPrice",
        "markPrice",
        "indexPrice",
        "volume24h",
        "openInterest",
    ] {
        assert_eq!(f[name]["state"], "available", "{s}");
        assert!(f[name]["exchangeTimestamp"].is_null());
        assert!(f[name]["receivedTimestamp"].as_u64().is_some());
    }
    assert_eq!(f["openInterest"]["source"], "nado:ccxt:fetchFundingRates");
    let s = h.post("fetchMarketStats",json!({"params":{"type":"spot"},"fields":["funding","openInterest","markPrice","lastPrice"]}),StatusCode::OK).await;
    assert_eq!(s["markets"][0]["type"], "spot");
    assert_eq!(
        s["markets"][0]["fields"]["funding"]["state"],
        "notApplicable"
    );
    // Exact opaque selections reuse the same bulk acquisition; no singular OI.
    let selected = h
        .post(
            "fetchMarketStats",
            json!({"marketIds":[perp["marketId"]],"fields":["openInterest"]}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(selected["markets"][0]["marketId"], perp["marketId"]);
    assert_eq!(
        selected["markets"][0]["fields"]["openInterest"]["value"]["openInterestAmount"],
        12.5
    );
    let calls = h.mock.calls.lock().await.clone();
    for path in [
        "/gateway/v1/symbols",
        "/gateway/v2/pairs",
        "/gateway/v2/assets",
        "/archive/v2/tickers",
        "/archive/v2/contracts",
    ] {
        assert_eq!(
            calls.iter().filter(|(u, _)| u.path() == path).count(),
            1,
            "{path}: {calls:?}"
        );
    }
    assert!(calls
        .iter()
        .any(|(u, _)| u.query().is_some_and(|q| q.contains("limit=500"))));
    for (_, body) in calls.iter().filter(|(u, _)| u.path() == "/archive/v1") {
        assert_eq!(body["candlesticks"]["product_id"], 2);
        assert_eq!(body["candlesticks"]["limit"], 500);
        assert_eq!(body["candlesticks"]["max_time"], (TIME + 60000) / 1000);
        assert!(body["candlesticks"]["since"].is_null());
    }
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_zero_missing_and_failed_bulk_rows_are_distinct() {
    for mode in 1..=4 {
        let h = Harness::start().await;
        h.mock.mode.store(mode, Ordering::SeqCst);
        let s = h
            .post(
                "fetchMarketStats",
                json!({"fields":["funding","markPrice","lastPrice","openInterest"]}),
                StatusCode::OK,
            )
            .await;
        let f = &s["markets"][0]["fields"];
        assert_eq!(f["lastPrice"]["state"], "available", "{s}");
        if mode == 1 {
            assert_eq!(
                f["openInterest"]["value"],
                json!({"openInterestAmount":0.0,"openInterestValue":0.0})
            );
            assert_eq!(f["funding"]["value"]["rate"], "0");
        } else {
            assert_eq!(f["openInterest"]["state"], "unavailable", "{s}");
        }
        if mode == 2 {
            for key in ["funding", "markPrice", "openInterest"] {
                assert_eq!(f[key]["reason"], "invalid-upstream-value", "{s}");
            }
        }
        if mode == 3 {
            assert_eq!(f["openInterest"]["reason"], "missing-upstream-row");
        }
        if mode == 4 {
            assert_eq!(s["coverage"]["sourceFailures"].as_array().unwrap().len(), 1);
        }
        h.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_streams_share_connection_reconnect_and_readd_without_stale_cache() {
    let h = Harness::start().await;
    let mut trades = h
        .subscribe(RealtimeChannel::Trades, NATIVE, json!({}))
        .await;
    let mut candles = h
        .subscribe(RealtimeChannel::Ohlcv, NATIVE, json!({"timeframe":"1m"}))
        .await;
    let mut spot = h
        .subscribe(RealtimeChannel::Trades, "BTC_USDT0", json!({}))
        .await;
    let mut five_minute = h
        .subscribe(RealtimeChannel::Ohlcv, NATIVE, json!({"timeframe":"5m"}))
        .await;
    let (conn, _) = h.command("trade", 2, None, 1).await;
    assert_eq!(
        h.command("latest_candlestick", 2, Some(60), 1).await.0,
        conn
    );
    assert_eq!(h.command("trade", 1, None, 1).await.0, conn);
    assert_eq!(
        h.command("latest_candlestick", 2, Some(300), 1).await.0,
        conn
    );
    h.inject(trade(2, TIME));
    match next(&mut trades).await {
        RealtimeUpdate::Trades(t) => {
            assert_eq!(t[0].symbol.as_deref(), Some(PERP));
            assert_eq!(t[0].price, Some(60000.0));
            assert_eq!(t[0].amount, Some(0.5));
            assert_eq!(t[0].timestamp, Some(TIME));
            assert!(t[0].id.is_none());
        }
        _ => panic!("expected trades"),
    }
    let mut c = candle();
    c["type"] = json!("latest_candlestick");
    h.inject(c.clone());
    assert!(
        matches!(next(&mut candles).await,RealtimeUpdate::Ohlcv(ref c) if c[0].0 == TIME && c[0].5 == Some(2.5))
    );
    let mut five = c.clone();
    five["granularity"] = json!(300);
    five["volume"] = json!("7000000000000000000");
    h.inject(five);
    assert!(
        matches!(next(&mut five_minute).await, RealtimeUpdate::Ohlcv(ref c) if c[0].5 == Some(7.0))
    );
    assert!(timeout(Duration::from_millis(50), candles.receiver.recv())
        .await
        .is_err());
    h.inject(trade(1, TIME));
    assert!(
        matches!(next(&mut spot).await,RealtimeUpdate::Trades(ref t) if t[0].symbol.as_deref() == Some(SPOT))
    );
    h.inject(json!({"close":true}));
    assert!(matches!(
        next(&mut trades).await,
        RealtimeUpdate::Error { .. }
    ));
    assert!(matches!(
        next(&mut candles).await,
        RealtimeUpdate::Error { .. }
    ));
    assert!(matches!(
        next(&mut spot).await,
        RealtimeUpdate::Error { .. }
    ));
    assert!(matches!(
        next(&mut five_minute).await,
        RealtimeUpdate::Error(_)
    ));
    let (conn2, _) = h.command("trade", 2, None, 2).await;
    assert_ne!(conn, conn2);
    h.command("latest_candlestick", 2, Some(60), 2).await;
    h.command("trade", 1, None, 2).await;
    h.command("latest_candlestick", 2, Some(300), 2).await;
    h.inject(trade(2, TIME + 1000));
    assert!(
        matches!(next(&mut trades).await,RealtimeUpdate::Trades(ref t) if t.len() == 1 && t[0].timestamp == Some(TIME+1000))
    );
    drop(candles); // stock unwatch is unsafe; remaining feeds reconnect.
    let (conn3, _) = h.command("trade", 2, None, 3).await;
    assert_ne!(conn2, conn3);
    let mut candles = h
        .subscribe(RealtimeChannel::Ohlcv, NATIVE, json!({"timeframe":"1m"}))
        .await;
    h.command("latest_candlestick", 2, Some(60), 3).await;
    c["timestamp"] = json!("1700000100");
    h.inject(c);
    assert!(
        matches!(next(&mut candles).await,RealtimeUpdate::Ohlcv(ref c) if c.len() == 1 && c[0].0 == TIME+60000)
    );
    h.stop().await;
}

#[path = "nado/downstream.rs"]
mod downstream;
#[path = "nado/live.rs"]
mod live;
