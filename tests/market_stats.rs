use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::{
        hyperliquid::HyperliquidExchange,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::{FetchMarketStatsParams, FetchMarketsParams, MarketStatsFieldName, MarketStatsValue},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message as WsMessage},
    MaybeTlsStream, WebSocketStream,
};

#[derive(Clone)]
struct Reply {
    status: StatusCode,
    body: Value,
    barrier: Option<Arc<Semaphore>>,
}

impl Reply {
    fn ok(body: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body,
            barrier: None,
        }
    }
}

#[derive(Clone)]
struct InfoMock {
    primary: Arc<RwLock<Reply>>,
    spot: Arc<RwLock<Reply>>,
    primary_count: Arc<AtomicUsize>,
    spot_count: Arc<AtomicUsize>,
}

fn primary_fixture() -> Value {
    json!([
        {"collateralToken": 7, "universe": [
            {"name":"ETH","szDecimals":4},
            {"name":"OLD","isDelisted":true},
            {"name":"BTC","szDecimals":5},
            {"name":"HYPE"}, {"name":"A-B"}, {"name":"AB"}
        ]},
        [
            {"funding":"0","markPx":"2000","oraclePx":"1999"},
            {"funding":"0.03","markPx":"1","oraclePx":"1"},
            {"funding":"-0.0000125","markPx":"60000","oraclePx":"59999"},
            {"funding":"0.0000125","markPx":"20","oraclePx":"19"},
            {"funding":"0.1","markPx":"2","oraclePx":"1"},
            {"funding":"0.2","markPx":"3","oraclePx":"2"}
        ]
    ])
}

fn spot_fixture() -> Value {
    json!({"tokens":[
        {"index":42,"name":"BTC","tokenId":"btc"},
        {"index":7,"name":"USDC","tokenId":"native-usdc"}
    ], "universe":[{"index":0,"name":"BTC/USDC","tokens":[42,7]}]})
}

impl InfoMock {
    fn new() -> Self {
        Self {
            primary: Arc::new(RwLock::new(Reply::ok(primary_fixture()))),
            spot: Arc::new(RwLock::new(Reply::ok(spot_fixture()))),
            primary_count: Arc::new(AtomicUsize::new(0)),
            spot_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

async fn info_handler(
    State(state): State<InfoMock>,
    Json(request): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let reply = match request["type"].as_str() {
        Some("metaAndAssetCtxs") => {
            state.primary_count.fetch_add(1, Ordering::SeqCst);
            state.primary.read().await.clone()
        }
        Some("spotMeta") => {
            state.spot_count.fetch_add(1, Ordering::SeqCst);
            state.spot.read().await.clone()
        }
        _ => panic!("unexpected upstream acquisition: {request}"),
    };
    if let Some(barrier) = reply.barrier {
        barrier.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

async fn spawn_server(app: Router) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });
    (format!("http://{addr}"), tx, task)
}

async fn source_server(
    mock: &InfoMock,
) -> (
    Arc<HyperliquidExchange>,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    let (url, tx, task) = spawn_server(
        Router::new()
            .route("/info", post(info_handler))
            .with_state(mock.clone()),
    )
    .await;
    (
        Arc::new(HyperliquidExchange::new(url, 2_000, 100, 60_000, false).unwrap()),
        tx,
        task,
    )
}

fn source_params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: json!({"dex":""}),
    }
}

async fn wait_count(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// Keep the paused runtime runnable while actual localhost I/O is in flight. Tests
// explicitly advance deadlines rather than allowing Tokio to skip to HTTP timeout.
fn keep_time_manual() -> JoinHandle<()> {
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}

#[tokio::test]
async fn market_stats_bulk_source_and_catalog_share_observations() {
    let mock = InfoMock::new();
    let primary_gate = Arc::new(Semaphore::new(0));
    let spot_gate = Arc::new(Semaphore::new(0));
    mock.primary.write().await.barrier = Some(primary_gate.clone());
    mock.spot.write().await.barrier = Some(spot_gate.clone());
    let (source, stop, server) = source_server(&mock).await;
    let mut consumers = Vec::new();
    for _ in 0..20 {
        let source = source.clone();
        consumers.push(tokio::spawn(async move {
            source.fetch_market_stats(source_params()).await.unwrap()
        }));
    }
    let mut catalogs = Vec::new();
    for _ in 0..5 {
        let source = source.clone();
        catalogs.push(tokio::spawn(async move {
            source
                .fetch_markets(FetchMarketsParams {
                    params: Value::Null,
                    include_inactive: true,
                })
                .await
                .unwrap()
        }));
    }
    wait_count(&mock.primary_count, 1).await;
    wait_count(&mock.spot_count, 1).await;
    primary_gate.add_permits(1);
    spot_gate.add_permits(1);
    let baseline = consumers.remove(0).await.unwrap();
    for consumer in consumers {
        assert_eq!(consumer.await.unwrap(), baseline);
    }
    for catalog in catalogs {
        for market in catalog.await.unwrap() {
            let identity = market.identity.as_ref().unwrap();
            let row = baseline
                .rows
                .iter()
                .find(|row| row.market.identity.as_ref().unwrap().market_id == identity.market_id)
                .unwrap();
            assert_eq!(market, row.market);
        }
    }
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    let btc = baseline
        .rows
        .iter()
        .find(|row| row.market.identity.as_ref().unwrap().exchange_market_id == "BTC")
        .unwrap();
    let Some(MarketStatsValue::Funding(funding)) =
        &btc.fields[&MarketStatsFieldName::Funding].value
    else {
        panic!("BTC funding missing")
    };
    assert_eq!(funding.rate, "-0.0000125");
    assert_eq!(
        btc.market.identity.as_ref().unwrap().settle.as_deref(),
        Some("USDC")
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn market_stats_source_deadlines_cache_failures_and_preserve_receipts() {
    let mock = InfoMock::new();
    let (source, stop, server) = source_server(&mock).await;
    let first = source.fetch_market_stats(source_params()).await.unwrap();
    tokio::time::pause();
    let manual = keep_time_manual();
    tokio::time::advance(Duration::from_secs(29)).await;
    let cached = source.fetch_market_stats(source_params()).await.unwrap();
    assert_eq!(cached.received_at, first.received_at);
    assert_eq!(cached.rows, first.rows);
    mock.primary.write().await.body[1][2]["funding"] = json!("-0.4");
    tokio::time::advance(Duration::from_secs(1)).await;
    let second = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(second.received_at > first.received_at);
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 2);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"error":"offline"}),
        barrier: None,
    };
    tokio::time::advance(Duration::from_secs(30)).await;
    for _ in 0..20 {
        assert!(source.fetch_market_stats(source_params()).await.is_err());
    }
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 3);
    *mock.primary.write().await = Reply::ok(primary_fixture());
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(
        source
            .fetch_market_stats(source_params())
            .await
            .unwrap()
            .contexts_valid
    );
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 4);
    manual.abort();
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn market_stats_spot_fallback_resolves_current_collateral_only() {
    let mock = InfoMock::new();
    let (source, stop, server) = source_server(&mock).await;
    let first = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(first.spot_enumeration_complete);
    tokio::time::pause();
    let manual = keep_time_manual();
    *mock.spot.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"error":"offline"}),
        barrier: None,
    };
    tokio::time::advance(Duration::from_secs(300)).await;
    let fallback = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(fallback.perp_enumeration_complete);
    assert!(!fallback.spot_enumeration_complete);
    assert!(fallback
        .source_failures
        .iter()
        .any(|failure| failure.source == "hyperliquid:spotMeta"
            && failure.reason == "upstream-failure"));
    assert!(fallback
        .rows
        .iter()
        .filter(|row| row.market.market_type
            == ferris_market_data_backend::models::UnifiedMarketType::Perp)
        .all(|row| row.market.identity.as_ref().unwrap().settle.as_deref() == Some("USDC")));
    mock.primary.write().await.body[0]["collateralToken"] = json!(42);
    tokio::time::advance(Duration::from_secs(30)).await;
    let changed = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(changed
        .rows
        .iter()
        .filter(|row| row.market.market_type
            == ferris_market_data_backend::models::UnifiedMarketType::Perp)
        .all(|row| row.market.identity.as_ref().unwrap().settle.as_deref() == Some("BTC")));
    let mut ambiguous = spot_fixture();
    ambiguous["tokens"]
        .as_array_mut()
        .unwrap()
        .push(json!({"index":42,"name":"WRONG","tokenId":"wrong"}));
    *mock.spot.write().await = Reply::ok(ambiguous);
    tokio::time::advance(Duration::from_secs(300)).await;
    let invalid = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(!invalid.spot_enumeration_complete);
    assert!(invalid
        .rows
        .iter()
        .filter(|row| row.market.market_type
            == ferris_market_data_backend::models::UnifiedMarketType::Perp)
        .all(|row| row.market.identity.as_ref().unwrap().settle.is_none()));
    assert!(invalid
        .source_failures
        .iter()
        .any(|failure| failure.reason == "settlement-unresolved"));
    manual.abort();
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
}

fn stats_request(
    ids: Option<Vec<String>>,
) -> ferris_market_data_backend::models::FetchMarketStatsRequest {
    ferris_market_data_backend::models::FetchMarketStatsRequest {
        exchange: "hyperliquid".into(),
        market_ids: ids,
        fields: Some(vec![
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::OpenInterest,
        ]),
        params: Value::Null,
    }
}

fn native_id(name: &str) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "hyperliquid",
        ferris_market_data_backend::models::UnifiedMarketType::Perp,
        None,
        Some(""),
        name,
    )
    .unwrap()
}

#[tokio::test]
async fn market_stats_coordinator_applies_failures_without_false_removals_or_resurrection() {
    use ferris_market_data_backend::{
        exchanges::registry::ExchangeRegistry,
        market_stats::{project_snapshot, MarketStatsCoordinator},
        models::MarketStatsFieldState,
    };
    let mock = InfoMock::new();
    let (source, stop, server) = source_server(&mock).await;
    let mut registry = ExchangeRegistry::new();
    registry.register(source);
    let coordinator = MarketStatsCoordinator::new(Arc::new(registry));
    let mut selected = coordinator
        .subscribe(stats_request(Some(vec![native_id("BTC")])))
        .await
        .unwrap();
    selected.receiver.borrow_and_update();
    tokio::time::pause();
    let manual = keep_time_manual();
    let baseline = coordinator.snapshot(stats_request(None)).await.unwrap();
    assert_eq!(baseline.markets.len(), 5);
    assert_eq!(baseline.coverage.expected_markets, Some(5));
    mock.primary.write().await.body[1][2]["funding"] = Value::Null;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    selected.receiver.changed().await.unwrap();
    let invalid = project_snapshot(&selected.topic, &selected.receiver.borrow_and_update());
    assert_eq!(
        invalid.markets[0].fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Unavailable
    );
    assert!(invalid.markets[0].fields[&MarketStatsFieldName::Funding]
        .value
        .is_none());
    assert_eq!(
        invalid.markets[0].fields[&MarketStatsFieldName::MarkPrice].state,
        MarketStatsFieldState::Available
    );
    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    tokio::time::advance(Duration::from_millis(30_001)).await;
    selected.receiver.changed().await.unwrap();
    let failed = project_snapshot(&selected.topic, &selected.receiver.borrow_and_update());
    assert!(!failed.coverage.enumeration_complete);
    assert_eq!(
        failed.markets[0].fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Unavailable
    );
    assert!(failed.markets[0].fields[&MarketStatsFieldName::Funding]
        .value
        .is_none());
    assert_eq!(
        failed.markets[0].fields[&MarketStatsFieldName::MarkPrice].state,
        MarketStatsFieldState::Stale
    );
    let mut mismatch = primary_fixture();
    mismatch[1].as_array_mut().unwrap().pop();
    *mock.primary.write().await = Reply::ok(mismatch);
    tokio::time::advance(Duration::from_millis(30_001)).await;
    selected.receiver.changed().await.unwrap();
    let mismatch = project_snapshot(&selected.topic, &selected.receiver.borrow_and_update());
    assert!(mismatch.coverage.enumeration_complete);
    assert_eq!(
        mismatch.markets[0].fields[&MarketStatsFieldName::MarkPrice]
            .reason
            .as_deref(),
        Some("context-mismatch")
    );
    let mut removed = primary_fixture();
    removed[0]["universe"].as_array_mut().unwrap().remove(2);
    removed[1].as_array_mut().unwrap().remove(2);
    *mock.primary.write().await = Reply::ok(removed);
    tokio::time::advance(Duration::from_millis(30_001)).await;
    selected.receiver.changed().await.unwrap();
    let removed = project_snapshot(&selected.topic, &selected.receiver.borrow_and_update());
    assert!(removed.markets.is_empty());
    assert_eq!(removed.coverage.expected_markets, Some(1));
    assert!(matches!(
        coordinator
            .snapshot(stats_request(Some(vec![native_id("BTC")])))
            .await,
        Err(ferris_market_data_backend::errors::ApiError::Validation(_))
    ));
    coordinator.unsubscribe_by_key(&selected.key).await;
    coordinator.shutdown().await;
    manual.abort();
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[derive(Clone)]
struct UnusedBook;

#[async_trait::async_trait]
impl ferris_market_data_backend::binance_orderbook::OrderBookSnapshotProvider for UnusedBook {
    async fn fetch_order_book_snapshot(
        &self,
        _: &str,
    ) -> Result<ferris_market_data_backend::binance_orderbook::BinanceDepthSnapshot, String> {
        panic!("statistics must not acquire orderbooks")
    }
}

async fn backend(
    source: Arc<HyperliquidExchange>,
) -> (
    String,
    ferris_market_data_backend::web::AppState,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    use ferris_market_data_backend::{
        exchanges::{
            aster::AsterExchange,
            binance::BinanceExchange,
            bybit::BybitExchange,
            extended::ExtendedExchange,
            lighterxyz::{LighterExchange, LighterMarketCatalogService},
            registry::ExchangeRegistry,
        },
        realtime::{OhlcvTopicManager, OrderBookTopicManager, TradesTopicManager},
        web::{self, AppState},
    };
    let mut registry = ExchangeRegistry::new();
    registry.register(source);
    registry.register(Arc::new(AsterExchange::new(1_000).unwrap()));
    registry.register(Arc::new(BinanceExchange::new(1_000).unwrap()));
    registry.register(Arc::new(BybitExchange::new(1_000).unwrap()));
    registry.register(Arc::new(
        ExtendedExchange::new("http://127.0.0.1:1".into(), 1_000).unwrap(),
    ));
    let catalog = Arc::new(
        LighterMarketCatalogService::new(1_000, "http://127.0.0.1:1".into(), 60_000).unwrap(),
    );
    registry.register(Arc::new(
        LighterExchange::new("http://127.0.0.1:1".into(), 1_000, catalog.clone()).unwrap(),
    ));
    let state = AppState::new(
        Arc::new(registry),
        TradesTopicManager::new(
            "http://127.0.0.1:1".into(),
            "ws://127.0.0.1:1".into(),
            "ws://127.0.0.1:1".into(),
            catalog.clone(),
        ),
        OrderBookTopicManager::new(
            "http://127.0.0.1:1".into(),
            "ws://127.0.0.1:1".into(),
            Arc::new(UnusedBook),
            Arc::new(UnusedBook),
            "ws://127.0.0.1:1".into(),
            catalog,
        ),
        OhlcvTopicManager::new("http://127.0.0.1:1".into(), "ws://127.0.0.1:1".into()),
    );
    let app = Router::new()
        .route("/v1/fetchMarketStats", post(web::fetch_market_stats))
        .route("/v1/fetchMarkets", post(web::fetch_markets))
        .route("/v1/capabilities", get(web::capabilities))
        .route("/v1/ws", get(web::trades_stream_ws))
        .with_state(state.clone());
    let (url, tx, task) = spawn_server(app).await;
    (url, state, tx, task)
}

async fn stats_http(
    client: &reqwest::Client,
    base: &str,
    body: Value,
    status: StatusCode,
) -> Value {
    let response = client
        .post(format!("{base}/v1/fetchMarketStats"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(actual, status, "{body}");
    body
}

#[tokio::test]
async fn market_stats_http_capabilities_bounds_and_catalog_proof() {
    let mock = InfoMock::new();
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let caps: Value = client
        .get(format!("{url}/v1/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let exchanges = caps["exchanges"].as_array().unwrap();
    assert_eq!(
        exchanges
            .iter()
            .map(|entry| entry["exchange"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "aster",
            "binance",
            "bybit",
            "extended",
            "hyperliquid",
            "lighterxyz"
        ]
    );
    for exchange in exchanges {
        assert_eq!(exchange["fundingRateHistory"]["state"], "unsupported");
        if exchange["exchange"] == "hyperliquid" {
            assert_eq!(exchange["marketStats"]["upstreamMode"], "sharedPolling");
            assert_eq!(exchange["marketStats"]["rateIntervalMs"], Value::Null);
        } else {
            assert_eq!(
                exchange["marketStats"],
                json!({"state":"unsupported","reason":"adapter-not-implemented"})
            );
            assert_eq!(
                stats_http(
                    &client,
                    &url,
                    json!({"exchange":exchange["exchange"]}),
                    StatusCode::NOT_IMPLEMENTED
                )
                .await["code"],
                "UNSUPPORTED_FEATURE"
            );
        }
    }
    for body in [
        json!({"marketIds":[]}),
        json!({"fields":[]}),
        json!({"symbol":"BTC"}),
        json!({"fields":["unknown"]}),
        json!({"params":{"dex":"other"}}),
        json!({"params":{"category":"perp"}}),
        json!({"params":[]}),
        json!({"marketIds":["bad"]}),
        json!({"marketIds":vec![native_id("BTC");101]}),
        json!({"marketIds":["[\"other\",\"perp\",null,\"\",\"BTC\"]"]}),
    ] {
        assert_eq!(
            stats_http(&client, &url, body, StatusCode::BAD_REQUEST).await["code"],
            "VALIDATION_ERROR"
        );
    }
    assert_eq!(
        stats_http(
            &client,
            &url,
            json!({"exchange":"unknown"}),
            StatusCode::BAD_REQUEST
        )
        .await["code"],
        "UNSUPPORTED_EXCHANGE"
    );
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 0);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 0);
    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    let cold = stats_http(&client, &url, json!({}), StatusCode::OK).await;
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(
        stats_http(
            &client,
            &url,
            json!({"marketIds":[native_id("BTC")]}),
            StatusCode::BAD_GATEWAY
        )
        .await["code"],
        "UPSTREAM_REQUEST_FAILED"
    );
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_http_all_selected_and_catalog_share_one_acquisition() {
    let mock = InfoMock::new();
    let gate = Arc::new(Semaphore::new(0));
    mock.primary.write().await.barrier = Some(gate.clone());
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let mut requests = Vec::new();
    for index in 0..20 {
        let (client, url) = (client.clone(), url.clone());
        requests.push(tokio::spawn(async move {
            stats_http(
                &client,
                &url,
                if index % 2 == 0 {
                    json!({})
                } else {
                    json!({"marketIds":[native_id("BTC")]})
                },
                StatusCode::OK,
            )
            .await
        }));
    }
    let (catalog_client, catalog_url) = (client.clone(), url.clone());
    let catalog = tokio::spawn(async move {
        catalog_client
            .post(format!("{catalog_url}/v1/fetchMarkets"))
            .json(&json!({"includeInactive":true}))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    wait_count(&mock.primary_count, 1).await;
    gate.add_permits(1);
    let mut observed = None;
    for request in requests {
        let result = request.await.unwrap();
        assert_eq!(result["coverage"]["enumerationComplete"], true);
        let row = result["markets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["marketId"] == native_id("BTC"))
            .unwrap();
        if let Some(previous) = &observed {
            assert_eq!(&row["fields"]["funding"], previous);
        } else {
            observed = Some(row["fields"]["funding"].clone());
        }
    }
    let catalog = catalog.await.unwrap();
    let spot = catalog["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "spot")
        .unwrap();
    let selected = stats_http(&client,&url,json!({"marketIds":[spot["marketId"],native_id("OLD")],"fields":["funding","markPrice","volume24h"]}),StatusCode::OK).await;
    for row in selected["markets"].as_array().unwrap() {
        if row["type"] == "spot" {
            assert_eq!(row["fields"]["funding"]["state"], "notApplicable");
        } else {
            assert_eq!(row["fields"]["funding"]["reason"], "inactive-market");
        }
        assert_eq!(row["fields"]["volume24h"]["state"], "unsupported");
    }
    assert_eq!(
        stats_http(
            &client,
            &url,
            json!({"marketIds":[native_id("UNKNOWN")]}),
            StatusCode::BAD_REQUEST
        )
        .await["message"],
        format!("unknown marketId: {}", native_id("UNKNOWN"))
    );
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

type StatsSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn stats_socket(base: &str) -> StatsSocket {
    connect_async(format!("{}/v1/ws", base.replacen("http://", "ws://", 1)))
        .await
        .unwrap()
        .0
}

async fn ws_send(socket: &mut StatsSocket, message: Value) {
    socket
        .send(WsMessage::Text(message.to_string().into()))
        .await
        .unwrap();
}

fn stats_command(op: &str, request: &Value) -> Value {
    let mut command = request.clone();
    command["op"] = json!(op);
    command["channel"] = json!("marketstats");
    command
}

// Real localhost I/O must time out in wall time even while the source clock is paused.
async fn ws_frame(socket: &mut StatsSocket) -> Option<Result<WsMessage, WsError>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            frame = socket.next() => return frame,
            _ = tokio::task::yield_now() => {
                assert!(std::time::Instant::now() < deadline, "timed out waiting for websocket frame");
            }
        }
    }
}

// Never skip an application message: acknowledgement ordering is part of the protocol.
async fn ws_next(socket: &mut StatsSocket) -> Value {
    loop {
        match ws_frame(socket)
            .await
            .expect("socket closed before expected message")
            .unwrap()
        {
            WsMessage::Text(text) => return serde_json::from_str(&text).unwrap(),
            WsMessage::Binary(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            WsMessage::Ping(payload) => socket.send(WsMessage::Pong(payload)).await.unwrap(),
            WsMessage::Pong(_) => {}
            message => panic!("unexpected websocket frame: {message:?}"),
        }
    }
}

async fn ws_error(socket: &mut StatsSocket, command: Value, code: &str) {
    ws_send(socket, command).await;
    let message = ws_next(socket).await;
    assert_eq!(message["type"], "error", "{message}");
    assert_eq!(message["code"], code, "{message}");
}

async fn ws_ping(socket: &mut StatsSocket) {
    ws_send(socket, json!({"op":"ping"})).await;
    assert_eq!(ws_next(socket).await, json!({"type":"pong"}));
}

async fn ws_disconnect(mut socket: StatsSocket) {
    socket.close(None).await.unwrap();
    while let Some(frame) = ws_frame(&mut socket).await {
        // A peer EOF also proves the socket writer has exited after map cleanup.
        if matches!(frame, Ok(WsMessage::Close(_)) | Err(_)) {
            break;
        }
    }
}

async fn ws_unsubscribe(socket: &mut StatsSocket, topic: &Value) {
    ws_send(socket, stats_command("unsubscribe", topic)).await;
    assert_eq!(
        ws_next(socket).await,
        json!({"type":"unsubscribed","op":"unsubscribe","topic":topic})
    );
}

#[derive(Clone, Debug, PartialEq)]
struct StatsView {
    topic: Value,
    generation: String,
    revision: u64,
    scope: Value,
    markets: BTreeMap<String, Value>,
    coverage: Value,
}

impl StatsView {
    fn from_snapshot(message: &Value) -> Self {
        assert_eq!(message["type"], "marketstats", "{message}");
        assert_eq!(message["mode"], "snapshot", "{message}");
        assert_eq!(message["revision"], 1);
        assert!(message["timestamp"].is_u64());
        let generation = message["generation"].as_str().unwrap().to_owned();
        assert!(!generation.is_empty());
        let mut markets = BTreeMap::new();
        for row in message["markets"].as_array().unwrap() {
            let id = row["marketId"].as_str().unwrap().to_owned();
            assert!(
                markets.insert(id, row.clone()).is_none(),
                "duplicate market in snapshot"
            );
        }
        Self {
            topic: message["topic"].clone(),
            generation,
            revision: 1,
            scope: message["scope"].clone(),
            markets,
            coverage: message["coverage"].clone(),
        }
    }

    fn apply_delta(&mut self, message: &Value) -> bool {
        assert_eq!(message["type"], "marketstats", "{message}");
        assert_eq!(message["mode"], "delta", "{message}");
        if message["generation"] != self.generation
            || message["previousRevision"] != self.revision
            || message["revision"] != self.revision + 1
        {
            return false;
        }
        assert_eq!(message["topic"], self.topic);
        assert_eq!(message["scope"], self.scope);
        assert!(message["timestamp"].is_u64());
        for update in message["updates"].as_array().unwrap() {
            let id = update["marketId"].as_str().unwrap().to_owned();
            let mut next = update.clone();
            let fields = next["fields"].as_object_mut().unwrap();
            if let Some(previous) = self.markets.get(&id) {
                for (name, field) in previous["fields"].as_object().unwrap() {
                    fields.entry(name.clone()).or_insert_with(|| field.clone());
                }
            }
            self.markets.insert(id, next);
        }
        for id in message["removedMarketIds"].as_array().unwrap() {
            assert!(
                self.markets.remove(id.as_str().unwrap()).is_some(),
                "removed unknown market"
            );
        }
        self.coverage = message["coverage"].clone();
        self.revision += 1;
        true
    }

    fn assert_matches_snapshot(&self, snapshot: &Value) {
        assert_eq!(self.scope, snapshot["scope"]);
        assert_eq!(self.coverage, snapshot["coverage"]);
        assert_eq!(
            self.markets.values().collect::<Vec<_>>(),
            snapshot["markets"]
                .as_array()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        );
    }
}

async fn ws_initial(socket: &mut StatsSocket) -> StatsView {
    let ack = ws_next(socket).await;
    assert_eq!(
        ack["type"], "subscribed",
        "data must not precede the acknowledgement: {ack}"
    );
    assert_eq!(ack["op"], "subscribe");
    let snapshot = ws_next(socket).await;
    assert_eq!(snapshot["topic"], ack["topic"]);
    StatsView::from_snapshot(&snapshot)
}

async fn ws_delta(socket: &mut StatsSocket, view: &mut StatsView) -> Value {
    let message = ws_next(socket).await;
    assert!(
        view.apply_delta(&message),
        "broken per-subscription revision chain: {message}"
    );
    message
}

async fn assert_ws_matches_http(client: &reqwest::Client, base: &str, view: &StatsView) {
    view.assert_matches_snapshot(
        &stats_http(client, base, view.topic.clone(), StatusCode::OK).await,
    );
}

// Tokio advancement does not advance UNIX time; cross the receipt millisecond without sleeping.
async fn after_receipt(receipt: &Value) {
    let receipt = receipt.as_u64().unwrap() as u128;
    while SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        <= receipt
    {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn market_stats_ws_shares_http_catalog_and_releases_duplicate_demand() {
    let mock = InfoMock::new();
    let primary_gate = Arc::new(Semaphore::new(0));
    let spot_gate = Arc::new(Semaphore::new(0));
    mock.primary.write().await.barrier = Some(primary_gate.clone());
    mock.spot.write().await.barrier = Some(spot_gate.clone());
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let mut all_socket = stats_socket(&url).await;
    let mut btc_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let all_request =
        json!({"exchange":"hyperliquid","params":{"dex":""},"fields":["funding","markPrice"]});
    let btc_request = json!({"marketIds":[btc],"fields":["funding","markPrice"]});
    tokio::time::pause();
    let manual = keep_time_manual();
    ws_send(&mut all_socket, stats_command("subscribe", &all_request)).await;
    ws_send(&mut btc_socket, stats_command("subscribe", &btc_request)).await;
    let mut requests = Vec::new();
    for index in 0..20 {
        let (client, url) = (client.clone(), url.clone());
        let request = if index % 2 == 0 {
            all_request.clone()
        } else {
            btc_request.clone()
        };
        requests.push(tokio::spawn(async move {
            stats_http(&client, &url, request, StatusCode::OK).await
        }));
    }
    let (catalog_client, catalog_url) = (client.clone(), url.clone());
    let catalog = tokio::spawn(async move {
        let response = catalog_client
            .post(format!("{catalog_url}/v1/fetchMarkets"))
            .json(&json!({"includeInactive":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.json::<Value>().await.unwrap()
    });
    wait_count(&mock.primary_count, 1).await;
    wait_count(&mock.spot_count, 1).await;
    primary_gate.add_permits(1);
    spot_gate.add_permits(1);
    let all = ws_initial(&mut all_socket).await;
    let mut selected = ws_initial(&mut btc_socket).await;
    assert_eq!(all.markets.len(), 5);
    assert_eq!(selected.markets.keys().collect::<Vec<_>>(), vec![&btc]);
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    for request in requests {
        let snapshot = request.await.unwrap();
        if snapshot["markets"].as_array().unwrap().len() == 1 {
            selected.assert_matches_snapshot(&snapshot);
        } else {
            all.assert_matches_snapshot(&snapshot);
        }
    }
    let catalog = catalog.await.unwrap();
    for row in all.markets.values() {
        let issued = catalog["markets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|market| market["marketId"] == row["marketId"])
            .unwrap();
        let mut identity = row.clone();
        identity.as_object_mut().unwrap().remove("fields");
        assert_eq!(&identity, issued);
    }
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    mock.primary.write().await.barrier = None;
    mock.spot.write().await.barrier = None;

    let duplicate = json!({"exchange":" HYPERLIQUID ","params":{},"marketIds":[btc,btc],"fields":["markPrice","funding","funding"]});
    ws_send(&mut btc_socket, stats_command("subscribe", &duplicate)).await;
    assert_eq!(
        ws_next(&mut btc_socket).await,
        json!({"type":"alreadySubscribed","op":"subscribe","topic":selected.topic})
    );
    ws_ping(&mut btc_socket).await;
    ws_disconnect(all_socket).await;
    after_receipt(&selected.markets[&btc]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        2,
        "remaining subscriber must keep the shared source alive"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_unsubscribe(&mut btc_socket, &selected.topic).await;
    ws_error(
        &mut btc_socket,
        stats_command("unsubscribe", &duplicate),
        "NOT_SUBSCRIBED",
    )
    .await;

    // HTTP demand expires 90 seconds after the simultaneous requests, not after reads of a WS view.
    tokio::time::advance(Duration::from_millis(60_001)).await;
    ws_ping(&mut btc_socket).await;
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 2);
    tokio::time::advance(Duration::from_millis(300_001)).await;
    ws_ping(&mut btc_socket).await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        2,
        "duplicate subscribe or socket teardown leaked demand"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);

    mock.primary.write().await.body[1][2]["funding"] = json!("-0.25");
    let mut renewed_all_socket = stats_socket(&url).await;
    ws_send(&mut btc_socket, stats_command("subscribe", &btc_request)).await;
    ws_send(
        &mut renewed_all_socket,
        stats_command("subscribe", &all_request),
    )
    .await;
    let renewed = ws_initial(&mut btc_socket).await;
    let renewed_all = ws_initial(&mut renewed_all_socket).await;
    assert_ne!(renewed.generation, selected.generation);
    assert_eq!(
        renewed.markets[&btc]["fields"]["funding"]["value"]["rate"],
        "-0.25"
    );
    assert_eq!(renewed.markets[&btc], renewed_all.markets[&btc]);
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        3,
        "renewed demand must share one new primary acquisition"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 2);
    ws_unsubscribe(&mut btc_socket, &renewed.topic).await;
    ws_unsubscribe(&mut renewed_all_socket, &renewed_all.topic).await;
    ws_disconnect(btc_socket).await;
    ws_disconnect(renewed_all_socket).await;
    state.shutdown_market_stats().await;
    manual.abort();
    let _ = manual.await;
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_reducer_tracks_receipts_clears_removals_and_reconnect() {
    let mock = InfoMock::new();
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let mut all_socket = stats_socket(&url).await;
    let mut btc_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let eth = native_id("ETH");
    let all_request = json!({"fields":["funding","markPrice","openInterest"]});
    let btc_request = json!({"marketIds":[btc],"fields":["funding","markPrice","openInterest"]});
    ws_send(&mut all_socket, stats_command("subscribe", &all_request)).await;
    ws_send(&mut btc_socket, stats_command("subscribe", &btc_request)).await;
    let mut all = ws_initial(&mut all_socket).await;
    let mut selected = ws_initial(&mut btc_socket).await;
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    let initial_generation = selected.generation.clone();
    let initial_funding = selected.markets[&btc]["fields"]["funding"].clone();
    tokio::time::pause();
    let manual = keep_time_manual();

    after_receipt(&initial_funding["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    ws_delta(&mut all_socket, &mut all).await;
    let receipt_delta = ws_delta(&mut btc_socket, &mut selected).await;
    let current_funding = &selected.markets[&btc]["fields"]["funding"];
    assert_eq!(current_funding["value"], initial_funding["value"]);
    assert!(
        current_funding["receivedTimestamp"].as_u64().unwrap()
            > initial_funding["receivedTimestamp"].as_u64().unwrap()
    );
    assert!(
        receipt_delta["updates"][0]["fields"]
            .get("openInterest")
            .is_none(),
        "unchanged unsupported fields must remain sparse"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["openInterest"]["state"],
        "unsupported"
    );
    assert_ws_matches_http(&client, &url, &all).await;
    assert_ws_matches_http(&client, &url, &selected).await;
    let before_duplicate = selected.clone();
    assert!(
        !selected.apply_delta(&receipt_delta),
        "the consumer must discard an already-applied revision"
    );
    assert_eq!(selected, before_duplicate);

    mock.primary.write().await.body[1][2]["funding"] = Value::Null;
    mock.primary.write().await.body[1][2]["markPx"] = json!("61000");
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    ws_delta(&mut all_socket, &mut all).await;
    let invalid = ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(
        invalid["updates"][0]["fields"]["funding"]["state"],
        "unavailable"
    );
    assert_eq!(
        invalid["updates"][0]["fields"]["funding"]["value"],
        Value::Null
    );
    let invalid_funding = selected.markets[&btc]["fields"]["funding"].clone();
    assert_eq!(invalid_funding["reason"], "invalid-upstream-value");
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["value"]["amount"],
        "61000"
    );
    let mark_receipt = selected.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"].clone();
    assert_ws_matches_http(&client, &url, &all).await;
    assert_ws_matches_http(&client, &url, &selected).await;

    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let all_failed = ws_delta(&mut all_socket, &mut all).await;
    let selected_failed = ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(all_failed["removedMarketIds"], json!([]));
    assert_eq!(selected_failed["removedMarketIds"], json!([]));
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"], invalid_funding,
        "failure must not resurrect an explicitly cleared value"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["state"],
        "stale"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"],
        mark_receipt
    );
    assert_eq!(all.coverage["enumerationComplete"], false);
    assert_eq!(all.coverage["expectedMarkets"], 5);
    assert_ws_matches_http(&client, &url, &all).await;
    assert_ws_matches_http(&client, &url, &selected).await;

    let mut delisted = primary_fixture();
    delisted[0]["universe"][2]["isDelisted"] = json!(true);
    *mock.primary.write().await = Reply::ok(delisted);
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let all_delisted = ws_delta(&mut all_socket, &mut all).await;
    let selected_delisted = ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(all_delisted["removedMarketIds"], json!([btc]));
    assert_eq!(selected_delisted["removedMarketIds"], json!([]));
    assert!(!all.markets.contains_key(&btc));
    assert_eq!(selected.markets[&btc]["active"], false);
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["state"],
        "unavailable"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["reason"],
        "inactive-market"
    );
    assert_eq!(
        selected_delisted["updates"][0]["fields"]
            .as_object()
            .unwrap()
            .len(),
        3,
        "an active-state change must include every requested field"
    );
    assert_eq!(all.coverage["expectedMarkets"], 4);
    assert_eq!(selected.coverage["expectedMarkets"], 1);
    assert_ws_matches_http(&client, &url, &all).await;
    assert_ws_matches_http(&client, &url, &selected).await;

    let mut removed = primary_fixture();
    removed[0]["universe"].as_array_mut().unwrap().remove(2);
    removed[1].as_array_mut().unwrap().remove(2);
    *mock.primary.write().await = Reply::ok(removed);
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let all_removed = ws_delta(&mut all_socket, &mut all).await;
    let selected_removed = ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(
        all_removed["removedMarketIds"],
        json!([]),
        "already-delisted market was not in the all-market view"
    );
    assert_eq!(selected_removed["removedMarketIds"], json!([btc]));
    assert_eq!(selected_removed["updates"], json!([]));
    selected.assert_matches_snapshot(&json!({"scope":all.scope,"markets":[],"coverage":{
        "expectedMarkets":1,"returnedMarkets":0,"enumerationComplete":true,"sourceFailures":[]
    }}));
    assert_ws_matches_http(&client, &url, &all).await;
    ws_unsubscribe(&mut btc_socket, &selected.topic).await;
    ws_error(
        &mut btc_socket,
        stats_command("subscribe", &btc_request),
        "INVALID_TOPIC",
    )
    .await;
    ws_error(
        &mut btc_socket,
        stats_command("unsubscribe", &selected.topic),
        "NOT_SUBSCRIBED",
    )
    .await;
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 6);

    *mock.primary.write().await = Reply::ok(primary_fixture());
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let added = ws_delta(&mut all_socket, &mut all).await;
    let added_btc = added["updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == btc)
        .unwrap();
    assert_eq!(
        added_btc["fields"].as_object().unwrap().len(),
        3,
        "a new market must include every requested field"
    );
    assert_eq!(all.markets[&btc]["fields"]["funding"]["state"], "available");
    assert_ws_matches_http(&client, &url, &all).await;
    ws_send(&mut btc_socket, stats_command("subscribe", &btc_request)).await;
    let replacement = ws_initial(&mut btc_socket).await;
    assert_ne!(replacement.generation, initial_generation);
    ws_disconnect(btc_socket).await;
    let mut reconnected_socket = stats_socket(&url).await;
    ws_send(
        &mut reconnected_socket,
        stats_command("subscribe", &btc_request),
    )
    .await;
    let mut reconnected = ws_initial(&mut reconnected_socket).await;
    assert_ne!(reconnected.generation, replacement.generation);
    assert_eq!(reconnected.markets[&btc], all.markets[&btc]);
    let before_old_generation = reconnected.clone();
    assert!(
        !reconnected.apply_delta(&receipt_delta),
        "old-generation deltas require replacement, not replay"
    );
    let mut gapped = receipt_delta;
    gapped["generation"] = json!(reconnected.generation);
    gapped["previousRevision"] = json!(reconnected.revision + 1);
    gapped["revision"] = json!(reconnected.revision + 2);
    assert!(!reconnected.apply_delta(&gapped));
    assert_eq!(reconnected, before_old_generation);
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 7);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_unsubscribe(&mut reconnected_socket, &reconnected.topic).await;
    ws_unsubscribe(&mut all_socket, &all.topic).await;
    ws_disconnect(reconnected_socket).await;
    ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    manual.abort();
    let _ = manual.await;
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_classifies_errors_and_limits_distinct_connection_topics() {
    let mock = InfoMock::new();
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let mut socket = stats_socket(&url).await;
    tokio::time::pause();
    let manual = keep_time_manual();
    for command in [
        json!({"op":"subscribe","channel":"funding","exchange":"hyperliquid","symbol":"BTC"}),
        json!({"op":"subscribe","channel":"marketstats","symbol":"BTC"}),
        stats_command("subscribe", &json!({"fields":["unknown"]})),
        stats_command("subscribe", &json!({"marketIds":"BTC"})),
        stats_command("subscribe", &json!({"fields":{}})),
        json!("not a command object"),
    ] {
        ws_error(&mut socket, command, "INVALID_COMMAND").await;
    }
    for request in [
        json!({"marketIds":[]}),
        json!({"fields":[]}),
        json!({"marketIds":vec![native_id("BTC");101]}),
        json!({"marketIds":["bad"]}),
        json!({"marketIds":["[\"other\",\"perp\",null,\"\",\"BTC\"]"]}),
        json!({"params":{"dex":"other"}}),
        json!({"params":{"category":"perp"}}),
        json!({"params":[]}),
    ] {
        ws_error(
            &mut socket,
            stats_command("subscribe", &request),
            "INVALID_TOPIC",
        )
        .await;
    }
    ws_error(
        &mut socket,
        stats_command("subscribe", &json!({"exchange":"unknown"})),
        "UNSUPPORTED_EXCHANGE",
    )
    .await;
    ws_error(
        &mut socket,
        stats_command("subscribe", &json!({"exchange":"binance"})),
        "UNSUPPORTED_FEATURE",
    )
    .await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        0,
        "invalid commands must not acquire source data"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 0);
    ws_error(
        &mut socket,
        stats_command("subscribe", &json!({"marketIds":[native_id("UNKNOWN")]})),
        "INVALID_TOPIC",
    )
    .await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        1,
        "catalog proof distinguishes an unknown ID from an upstream failure"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);

    let names = ["ETH", "BTC", "HYPE", "A-B", "AB"];
    let topics = (1u32..=17)
        .map(|mask| {
            let ids = names
                .iter()
                .enumerate()
                .filter_map(|(index, name)| (mask & (1 << index) != 0).then(|| native_id(name)))
                .collect::<Vec<_>>();
            json!({"marketIds":ids,"fields":["funding"]})
        })
        .collect::<Vec<_>>();
    let mut first = None;
    for (index, topic) in topics.iter().take(16).enumerate() {
        ws_send(&mut socket, stats_command("subscribe", topic)).await;
        let view = ws_initial(&mut socket).await;
        let mut ids = topic["marketIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(view.markets.keys().cloned().collect::<Vec<_>>(), ids);
        if index == 0 {
            first = Some(view);
        }
    }
    let first = first.unwrap();
    ws_error(
        &mut socket,
        stats_command("subscribe", &topics[16]),
        "SUBSCRIPTION_LIMIT",
    )
    .await;
    let duplicate = json!({"exchange":" HYPERLIQUID ","params":{},"marketIds":[native_id("ETH"),native_id("ETH")],"fields":["funding","funding"]});
    ws_send(&mut socket, stats_command("subscribe", &duplicate)).await;
    assert_eq!(
        ws_next(&mut socket).await,
        json!({"type":"alreadySubscribed","op":"subscribe","topic":first.topic})
    );
    ws_ping(&mut socket).await;
    ws_error(
        &mut socket,
        stats_command("unsubscribe", &topics[16]),
        "NOT_SUBSCRIBED",
    )
    .await;
    let mut other_socket = stats_socket(&url).await;
    ws_send(&mut other_socket, stats_command("subscribe", &json!({}))).await;
    let other = ws_initial(&mut other_socket).await;
    assert_eq!(
        other.markets.len(),
        5,
        "the subscription limit is per connection, not global"
    );
    ws_unsubscribe(&mut socket, &first.topic).await;
    ws_send(&mut socket, stats_command("subscribe", &topics[16])).await;
    let replacement = ws_initial(&mut socket).await;
    assert_eq!(replacement.markets.len(), 2);
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_disconnect(socket).await;
    ws_disconnect(other_socket).await;

    // No HTTP lease exists: closing both populated maps must release every successful lease.
    tokio::time::advance(Duration::from_millis(90_001)).await;
    let mut probe = stats_socket(&url).await;
    ws_ping(&mut probe).await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        1,
        "rejected, duplicate, or disconnected topics leaked demand"
    );
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_send(&mut probe, stats_command("subscribe", &topics[0])).await;
    let renewed = ws_initial(&mut probe).await;
    assert_ne!(renewed.generation, first.generation);
    assert_eq!(
        renewed.markets[&native_id("ETH")]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 2);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_unsubscribe(&mut probe, &renewed.topic).await;
    ws_disconnect(probe).await;
    state.shutdown_market_stats().await;
    manual.abort();
    let _ = manual.await;
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_cold_all_market_failure_recovers_without_selected_lease_leaks() {
    let mock = InfoMock::new();
    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    let (source, up_stop, up_server) = source_server(&mock).await;
    let (url, state, stop, server) = backend(source).await;
    let mut selected_socket = stats_socket(&url).await;
    let mut all_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let selected_request = json!({"marketIds":[btc]});
    tokio::time::pause();
    let manual = keep_time_manual();
    ws_error(
        &mut selected_socket,
        stats_command("subscribe", &selected_request),
        "SUBSCRIBE_FAILED",
    )
    .await;
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 1);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(30_001)).await;
    ws_ping(&mut selected_socket).await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        1,
        "failed selected subscription must not retain provisional demand"
    );

    ws_send(&mut all_socket, stats_command("subscribe", &json!({}))).await;
    let mut all = ws_initial(&mut all_socket).await;
    assert!(all.markets.is_empty());
    assert_eq!(all.coverage["expectedMarkets"], Value::Null);
    assert_eq!(all.coverage["returnedMarkets"], 0);
    assert_eq!(all.coverage["enumerationComplete"], false);
    assert!(all.coverage["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["reason"] == "upstream-failure"));
    let duplicate = json!({"marketIds":null,"fields":["funding"],"params":{"dex":""}});
    ws_send(&mut all_socket, stats_command("subscribe", &duplicate)).await;
    assert_eq!(
        ws_next(&mut all_socket).await,
        json!({"type":"alreadySubscribed","op":"subscribe","topic":all.topic})
    );
    ws_ping(&mut all_socket).await;
    ws_error(
        &mut selected_socket,
        stats_command("subscribe", &selected_request),
        "SUBSCRIBE_FAILED",
    )
    .await;
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 2);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);

    *mock.primary.write().await = Reply::ok(primary_fixture());
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let recovered = ws_delta(&mut all_socket, &mut all).await;
    assert_eq!(recovered["removedMarketIds"], json!([]));
    assert_eq!(all.markets.len(), 5);
    assert_eq!(
        all.coverage,
        json!({"expectedMarkets":5,"returnedMarkets":5,"enumerationComplete":true,"sourceFailures":[]})
    );
    assert_eq!(all.markets[&btc]["fields"]["funding"]["state"], "available");
    ws_send(
        &mut selected_socket,
        stats_command("subscribe", &selected_request),
    )
    .await;
    let selected = ws_initial(&mut selected_socket).await;
    assert_eq!(selected.markets.keys().collect::<Vec<_>>(), vec![&btc]);
    assert_eq!(selected.markets[&btc], all.markets[&btc]);
    assert_eq!(mock.primary_count.load(Ordering::SeqCst), 3);
    assert_eq!(mock.spot_count.load(Ordering::SeqCst), 1);
    ws_unsubscribe(&mut all_socket, &all.topic).await;
    ws_disconnect(selected_socket).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    ws_ping(&mut all_socket).await;
    assert_eq!(
        mock.primary_count.load(Ordering::SeqCst),
        3,
        "recovery and duplicate subscriptions must release demand exactly once"
    );
    ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    manual.abort();
    let _ = manual.await;
    tokio::time::resume();
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}
