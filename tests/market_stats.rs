//! Shared CCXT statistics harness plus stock Hyperliquid protocol coverage.
//!
//! Every venue under test is a real stock `CcxtExchange` pointed at a local
//! fixture through `ccxt_service`. Nothing here reaches a production endpoint:
//! the target venue URL is the loopback fixture and every other venue is
//! configured to an unroutable loopback port.
//! Cross-thread owners use real clocks; exact paused-time boundaries live in
//! `src/market_stats/lifecycle_tests.rs` and projection/forwarder unit tests.

#[path = "market_stats/aster.rs"]
mod aster;
#[path = "market_stats/binance.rs"]
mod binance;
#[path = "market_stats/bybit.rs"]
mod bybit;
#[path = "market_stats/extended.rs"]
mod extended;
#[path = "market_stats/lighter.rs"]
mod lighter;

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
    config::Config,
    exchanges::{
        ccxt::{CcxtExchange, CcxtService, Venue},
        registry::ExchangeRegistry,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::{
        FetchMarketStatsParams, FetchMarketStatsRequest, MarketStatsFieldName, MarketStatsValue,
        UnifiedMarketType,
    },
    realtime::RealtimeService,
    web::{self, AppState},
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

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

/// One canned upstream response. `barrier`, when set, holds the request until a
/// permit is added so concurrent demand can be observed mid-acquisition.
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

/// Stock Hyperliquid perpetual + spot metadata/contexts fixture. The contexts
/// array is index-aligned with the universe (including the delisted entry) as
/// the real `metaAndAssetCtxs` response is.
fn primary_fixture() -> Value {
    json!([
        {"collateralToken": 7, "universe": [
            {"name":"ETH","szDecimals":4},
            {"name":"OLD","isDelisted":true},
            {"name":"BTC","szDecimals":5},
            {"name":"HYPE","szDecimals":1},
            {"name":"SOL","szDecimals":2},
            {"name":"DOGE","szDecimals":0}
        ]},
        [
            {"funding":"0","markPx":"2000","oraclePx":"1999","dayNtlVlm":"1000","openInterest":"10"},
            {"funding":"0.03","markPx":"1","oraclePx":"1","dayNtlVlm":"1","openInterest":"1"},
            {"funding":"-0.0000125","markPx":"60000","oraclePx":"59999","dayNtlVlm":"500000","openInterest":"100"},
            {"funding":"0.0000125","markPx":"20","oraclePx":"19","dayNtlVlm":"200","openInterest":"5"},
            {"funding":"0.1","markPx":"2","oraclePx":"1","dayNtlVlm":"3","openInterest":"2"},
            {"funding":"0.2","markPx":"3","oraclePx":"2","dayNtlVlm":"4","openInterest":"2"}
        ]
    ])
}

fn spot_fixture() -> Value {
    json!([
        {"tokens":[
            {"index":0,"name":"USDC","szDecimals":8},
            {"index":1,"name":"BTC","szDecimals":5}
        ], "universe":[{"index":0,"name":"BTC/USDC","tokens":[1,0]}]},
        [{"dayNtlVlm":"1234.5","markPx":"60000","midPx":"60001","prevDayPx":"59900"}]
    ])
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

    fn primary_calls(&self) -> usize {
        self.primary_count.load(Ordering::SeqCst)
    }

    fn spot_calls(&self) -> usize {
        self.spot_count.load(Ordering::SeqCst)
    }
}

async fn info_handler(
    State(state): State<InfoMock>,
    Json(request): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let (count, slot) = match request["type"].as_str() {
        Some("metaAndAssetCtxs") => (&state.primary_count, &state.primary),
        Some("spotMetaAndAssetCtxs") => (&state.spot_count, &state.spot),
        other => panic!("unexpected upstream acquisition: {other:?}"),
    };
    count.fetch_add(1, Ordering::SeqCst);
    let reply = slot.read().await.clone();
    if let Some(barrier) = reply.barrier {
        barrier.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

/// Stock Hyperliquid uses `POST {base}/info` for every implicit request.
async fn hyperliquid_server() -> (InfoMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = InfoMock::new();
    let (base, stop, task) = spawn_server(
        Router::new()
            .route("/info", post(info_handler))
            .with_state(mock.clone()),
    )
    .await;
    (mock, base, stop, task)
}

// ---------------------------------------------------------------------------
// Shared server + client helpers
// ---------------------------------------------------------------------------

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

/// Every non-target venue is configured to an unroutable loopback port so a
/// regression that fans out to another venue fails fast instead of reaching the
/// network.
fn disabled_config(timeout_ms: u64) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        hyperliquid_base_url: "http://127.0.0.1:1".into(),
        extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
        extended_ws_url: "ws://127.0.0.1:1".into(),
        lighter_rest_base_url: "http://127.0.0.1:1".into(),
        lighter_ws_url: "ws://127.0.0.1:1".into(),
        binance_base_url: "http://127.0.0.1:1".into(),
        bybit_base_url: "http://127.0.0.1:1".into(),
        aster_base_url: "http://127.0.0.1:1".into(),
        request_timeout_ms: timeout_ms,
    }
}

fn ws_base(base: &str) -> String {
    base.replacen("http://", "ws://", 1)
}

/// `base` is the root fixture URL. The helper appends the venue-specific path
/// (`/api/v1` for Extended, `/stream` for the Lighter socket) so callers never
/// hand-build stock URL layouts.
fn ccxt_service(venue: Venue, base: &str, timeout_ms: u64) -> CcxtService {
    let mut config = disabled_config(timeout_ms);
    match venue {
        Venue::Hyperliquid => config.hyperliquid_base_url = base.to_string(),
        Venue::Extended => {
            config.extended_rest_base_url = format!("{base}/api/v1");
            config.extended_ws_url = ws_base(base);
        }
        Venue::Lighter => {
            config.lighter_rest_base_url = base.to_string();
            config.lighter_ws_url = format!("{}/stream", ws_base(base));
        }
        Venue::Binance => config.binance_base_url = base.to_string(),
        Venue::Bybit => config.bybit_base_url = base.to_string(),
        Venue::Aster => config.aster_base_url = base.to_string(),
    }
    CcxtService::start(&config).unwrap()
}

fn ccxt_stats_exchange(venue: Venue, base: &str, timeout_ms: u64) -> Arc<CcxtExchange> {
    Arc::new(CcxtExchange::new(
        venue,
        ccxt_service(venue, base, timeout_ms),
    ))
}

/// Registers `source` for its own exchange id and a disabled CCXT exchange for
/// the other five venues. Construction performs no I/O.
async fn backend(
    source: Arc<dyn MarketDataExchange>,
) -> (String, AppState, oneshot::Sender<()>, JoinHandle<()>) {
    let mut registry = ExchangeRegistry::new();
    registry.register(source.clone());
    let disabled = ccxt_service(Venue::Hyperliquid, "http://127.0.0.1:1", 1_000);
    for venue in Venue::ALL {
        if venue.public_id() != source.id() {
            registry.register(Arc::new(CcxtExchange::new(venue, disabled.clone())));
        }
    }
    let state = AppState::new(Arc::new(registry), RealtimeService::new(disabled));
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

fn stats_request(ids: Option<Vec<String>>) -> FetchMarketStatsRequest {
    FetchMarketStatsRequest {
        exchange: "hyperliquid".into(),
        market_ids: ids,
        fields: Some(vec![
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::Volume24h,
        ]),
        params: Value::Null,
    }
}

/// Canonical identity for a stock Hyperliquid perpetual, keyed by native name.
fn native_id(name: &str) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "hyperliquid",
        UnifiedMarketType::Perp,
        None,
        Some(""),
        name,
    )
    .unwrap()
}

/// Canonical identity for a stock Hyperliquid spot market, keyed by `@index`.
fn spot_id(index: u64) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "hyperliquid",
        UnifiedMarketType::Spot,
        None,
        None,
        &format!("@{index}"),
    )
    .unwrap()
}

fn source_params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: json!({"dex":""}),
        open_interest_market_ids: Vec::new(),
        include_bulk: true,
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

// ---------------------------------------------------------------------------
// HTTP protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn market_stats_http_capabilities_bounds_and_catalog_proof() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
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
        let stats = &exchange["marketStats"];
        assert_eq!(stats["state"], "supported", "{exchange}");
        assert_eq!(stats["pollIntervalMs"], 30_000);
        assert_eq!(stats["staleAfterMs"], 90_000);
        assert_eq!(stats["ws"]["snapshot"], true);
        assert_eq!(stats["ws"]["delta"], true);
        assert_eq!(stats["ws"]["maxSubscriptionsPerConnection"], 16);
        assert!(stats["limitations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "receipt-time-freshness"));
        match exchange["exchange"].as_str().unwrap() {
            "hyperliquid" => {
                assert_eq!(stats["upstreamMode"], "sharedPolling");
                assert_eq!(stats["scope"]["params"], json!({"dex":""}));
                assert_eq!(stats["rateIntervalMs"], 3_600_000);
                assert_eq!(stats["paymentIntervalMs"], 3_600_000);
                assert_eq!(stats["fundingKinds"], json!(["currentUnclassified"]));
                assert_eq!(stats["allMarkets"]["types"], json!(["spot", "perp"]));
                assert_eq!(stats["fields"]["perp"]["volume24h"]["state"], "supported");
                assert_eq!(
                    stats["fields"]["perp"]["openInterest"]["state"],
                    "supported"
                );
                assert_eq!(stats["fields"]["perp"]["lastPrice"]["state"], "unsupported");
                assert_eq!(
                    stats["fields"]["perp"]["lastSettledFunding"]["state"],
                    "unsupported"
                );
                assert_eq!(stats["fields"]["spot"]["funding"]["state"], "notApplicable");
                assert_eq!(
                    stats["fields"]["spot"]["markPrice"]["state"],
                    "notApplicable"
                );
                assert_eq!(
                    stats["fields"]["spot"]["openInterest"]["state"],
                    "notApplicable"
                );
                assert_eq!(stats["fields"]["spot"]["volume24h"]["state"], "supported");
            }
            "extended" => {
                assert_eq!(stats["upstreamMode"], "sharedPolling");
                assert_eq!(stats["scope"]["params"], json!({}));
                assert_eq!(stats["rateIntervalMs"], 3_600_000);
                assert_eq!(stats["paymentIntervalMs"], 3_600_000);
                assert_eq!(stats["fundingKinds"], json!(["estimate"]));
                assert_eq!(stats["allMarkets"]["types"], json!(["perp"]));
                assert_eq!(stats["selectedMarkets"]["types"], json!(["perp"]));
                for field in [
                    "funding",
                    "markPrice",
                    "indexPrice",
                    "lastPrice",
                    "volume24h",
                    "openInterest",
                ] {
                    assert_eq!(
                        stats["fields"]["perp"][field]["state"], "supported",
                        "extended {field}"
                    );
                }
                assert_eq!(
                    stats["fields"]["perp"]["lastSettledFunding"]["state"],
                    "unsupported"
                );
            }
            "lighterxyz" => {
                assert_eq!(stats["upstreamMode"], "sharedPollingAndWebSocket");
                assert_eq!(stats["scope"]["params"], json!({}));
                assert_eq!(stats["rateIntervalMs"], 3_600_000);
                assert_eq!(stats["paymentIntervalMs"], 3_600_000);
                assert_eq!(stats["fundingKinds"], json!(["estimate", "settled"]));
                assert_eq!(stats["allMarkets"]["types"], json!(["spot", "perp"]));
                assert_eq!(stats["fields"]["perp"]["lastPrice"]["state"], "supported");
                assert_eq!(stats["fields"]["perp"]["funding"]["state"], "supported");
                assert_eq!(
                    stats["fields"]["perp"]["lastSettledFunding"]["state"],
                    "supported"
                );
                assert_eq!(
                    stats["fields"]["perp"]["openInterest"]["state"],
                    "unsupported"
                );
                assert_eq!(stats["fields"]["spot"]["funding"]["state"], "notApplicable");
            }
            "binance" => {
                assert_eq!(stats["upstreamMode"], "sharedPolling");
                assert_eq!(stats["scope"]["params"], json!({}));
                assert_eq!(stats["rateIntervalMs"], Value::Null);
                assert_eq!(stats["paymentIntervalMs"], Value::Null);
                assert_eq!(stats["fundingKinds"], json!(["currentUnclassified"]));
                assert_eq!(
                    stats["allMarkets"]["types"],
                    json!(["spot", "future", "perp", "option"])
                );
            }
            "bybit" => {
                assert_eq!(stats["upstreamMode"], "sharedPolling");
                assert_eq!(stats["scope"]["params"], json!({"category":"linear"}));
                assert_eq!(stats["rateIntervalMs"], Value::Null);
                assert_eq!(stats["paymentIntervalMs"], Value::Null);
                assert_eq!(stats["fundingKinds"], json!(["estimate"]));
                assert_eq!(
                    stats["allMarkets"]["types"],
                    json!(["spot", "future", "perp", "option"])
                );
            }
            "aster" => {
                assert_eq!(stats["upstreamMode"], "sharedPolling");
                assert_eq!(stats["scope"]["params"], json!({}));
                assert_eq!(stats["rateIntervalMs"], Value::Null);
                assert_eq!(stats["paymentIntervalMs"], Value::Null);
                assert_eq!(stats["fundingKinds"], json!(["estimate"]));
                assert_eq!(stats["allMarkets"]["types"], json!(["spot", "perp"]));
                assert_eq!(
                    stats["fields"]["perp"]["openInterest"]["state"],
                    "unsupported"
                );
            }
            other => panic!("uncovered capability: {other}"),
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
    assert_eq!(mock.primary_calls(), 0);
    assert_eq!(mock.spot_calls(), 0);

    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    let cold = stats_http(&client, &url, json!({}), StatusCode::OK).await;
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert_eq!(cold["markets"], json!([]));
    assert!(cold["coverage"]["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(
            |failure| failure["source"] == "hyperliquid:ccxt:loadMarkets"
                && failure["reason"] == "upstream-failure"
        ));
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
    assert!(mock.primary_calls() >= 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_http_all_and_selected_share_one_acquisition() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let gate = Arc::new(Semaphore::new(0));
    mock.primary.write().await.barrier = Some(gate.clone());
    mock.spot.write().await.barrier = Some(gate.clone());
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
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
    wait_count(&mock.primary_count, 1).await;
    // While the first upstream call is gated, no other consumer may start an
    // independent acquisition. A per-consumer fetch would show >1 here.
    let gated_deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < gated_deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        mock.primary_calls(),
        1,
        "concurrent demand must share one acquisition"
    );
    gate.add_permits(64);
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
    // Twenty concurrent consumers share the catalog load and the bulk ticker
    // acquisition. One stock Hyperliquid acquisition issues two metadata posts
    // (catalog) plus two ticker posts, so four calls is the shared ceiling and
    // is nowhere near one acquisition per consumer.
    let shared_primary = mock.primary_calls();
    let shared_spot = mock.spot_calls();
    assert!(shared_primary <= 4, "{shared_primary}");
    assert!(shared_spot <= 4, "{shared_spot}");
    assert!(shared_primary >= 1 && shared_spot >= 1);
    // A later request inside the shared lease reuses the cached observation.
    let reused = stats_http(&client, &url, json!({}), StatusCode::OK).await;
    assert_eq!(reused["coverage"]["enumerationComplete"], true);
    assert_eq!(mock.primary_calls(), shared_primary);

    let btc = stats_http(
        &client,
        &url,
        json!({"marketIds":[native_id("BTC")],"fields":["funding","markPrice","volume24h","openInterest"]}),
        StatusCode::OK,
    )
    .await;
    let row = &btc["markets"][0];
    assert_eq!(row["fields"]["funding"]["value"]["rate"], "-0.0000125");
    assert_eq!(row["fields"]["markPrice"]["value"]["amount"], "60000");
    assert_eq!(
        row["fields"]["volume24h"]["value"]["baseVolume"],
        Value::Null
    );
    assert_eq!(row["fields"]["volume24h"]["value"]["quoteVolume"], 500000.0);
    assert_eq!(
        row["fields"]["openInterest"]["value"]["openInterestAmount"],
        100.0
    );
    assert_eq!(
        row["fields"]["openInterest"]["value"]["openInterestValue"],
        Value::Null
    );

    let selected = stats_http(
        &client,
        &url,
        json!({"marketIds":[spot_id(0),native_id("OLD")],"fields":["funding","markPrice","volume24h"]}),
        StatusCode::OK,
    )
    .await;
    for row in selected["markets"].as_array().unwrap() {
        if row["type"] == "spot" {
            // The stable spot identity is the stock metadata `index`, rendered
            // as `@index`, never the old decimal token index.
            assert_eq!(row["marketId"], json!(spot_id(0)));
            assert_eq!(row["exchangeMarketId"], "@0");
            assert_eq!(row["symbol"], "BTC/USDC");
            assert_eq!(row["fields"]["funding"]["state"], "notApplicable");
            assert_eq!(row["fields"]["markPrice"]["state"], "notApplicable");
            assert_eq!(row["fields"]["volume24h"]["state"], "available");
        } else {
            assert_eq!(row["marketId"], json!(native_id("OLD")));
            assert_eq!(row["exchangeMarketId"], "OLD");
            assert_eq!(row["active"], false);
            assert_eq!(row["fields"]["funding"]["reason"], "inactive-market");
            assert_eq!(row["fields"]["markPrice"]["reason"], "inactive-market");
        }
    }
    stats_http(
        &client,
        &url,
        json!({"marketIds":[native_id("UNKNOWN")]}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.primary_calls(), shared_primary);
    assert_eq!(mock.spot_calls(), shared_spot);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_coordinator_applies_failures_without_false_removals_or_resurrection() {
    use ferris_market_data_backend::{
        market_stats::{project_snapshot, MarketStatsCoordinator},
        models::MarketStatsFieldState,
    };
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let mut registry = ExchangeRegistry::new();
    registry.register(source);
    let coordinator = MarketStatsCoordinator::new(Arc::new(registry));
    let mut selected = coordinator
        .subscribe(stats_request(Some(vec![native_id("BTC")])))
        .await
        .unwrap();
    selected.receiver.borrow_and_update();

    let baseline = coordinator.snapshot(stats_request(None)).await.unwrap();
    assert_eq!(baseline.markets.len(), 5);
    assert_eq!(baseline.coverage.expected_markets, Some(5));
    mock.primary.write().await.body[1][2]["funding"] = Value::Null;
    tokio::time::sleep(Duration::from_secs(31)).await;
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
    tokio::time::sleep(Duration::from_secs(31)).await;
    selected.receiver.changed().await.unwrap();
    let failed = project_snapshot(&selected.topic, &selected.receiver.borrow_and_update());
    assert!(!failed.coverage.enumeration_complete);
    assert_eq!(
        failed.markets[0].fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Unavailable
    );
    assert_eq!(
        failed.markets[0].fields[&MarketStatsFieldName::MarkPrice].state,
        MarketStatsFieldState::Stale
    );
    let mut removed = primary_fixture();
    removed[0]["universe"].as_array_mut().unwrap().remove(2);
    removed[1].as_array_mut().unwrap().remove(2);
    *mock.primary.write().await = Reply::ok(removed);
    tokio::time::sleep(Duration::from_secs(31)).await;
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

    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

// ---------------------------------------------------------------------------
// WebSocket protocol
// ---------------------------------------------------------------------------

type StatsSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn stats_socket(base: &str) -> StatsSocket {
    connect_async(format!("{}/v1/ws", ws_base(base)))
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

async fn ws_frame(socket: &mut StatsSocket) -> Option<Result<WsMessage, WsError>> {
    tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("timed out waiting for websocket frame")
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

/// Locate the delta update for one market id (rows are sorted by opaque ID, so
/// callers must not assume a fixed position).
fn ws_update<'a>(delta: &'a Value, market_id: &str) -> &'a Value {
    delta["updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|update| update["marketId"] == market_id)
        .unwrap_or_else(|| panic!("no delta update for {market_id}: {delta}"))
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
async fn market_stats_ws_shares_http_and_releases_duplicate_demand() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let gate = Arc::new(Semaphore::new(0));
    mock.primary.write().await.barrier = Some(gate.clone());
    mock.spot.write().await.barrier = Some(gate.clone());
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let mut all_socket = stats_socket(&url).await;
    let mut btc_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let all_request =
        json!({"exchange":"hyperliquid","params":{"dex":""},"fields":["funding","markPrice"]});
    let btc_request = json!({"marketIds":[btc],"fields":["funding","markPrice"]});

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
    wait_count(&mock.primary_count, 1).await;
    // One gated acquisition serves both sockets and all concurrent HTTP readers.
    let gated_deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < gated_deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        mock.primary_calls(),
        1,
        "concurrent demand must share one acquisition"
    );
    gate.add_permits(64);
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
    assert!(mock.primary_calls() <= 4, "{}", mock.primary_calls());
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
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_delta(&mut btc_socket, &mut selected).await;
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["state"],
        "available"
    );
    ws_unsubscribe(&mut btc_socket, &selected.topic).await;
    ws_error(
        &mut btc_socket,
        stats_command("unsubscribe", &duplicate),
        "NOT_SUBSCRIBED",
    )
    .await;

    // With no subscriber left, only the 90-second HTTP lease can keep the source
    // alive. Once it expires the worker must exit and stop acquiring.
    tokio::time::sleep(Duration::from_secs(91)).await;
    ws_ping(&mut btc_socket).await;
    let settled = mock.primary_calls();
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_ping(&mut btc_socket).await;
    assert_eq!(
        mock.primary_calls(),
        settled,
        "duplicate subscribe or socket teardown leaked demand"
    );

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
    assert!(
        mock.primary_calls() > settled,
        "renewed demand must acquire fresh data"
    );
    ws_unsubscribe(&mut btc_socket, &renewed.topic).await;
    ws_unsubscribe(&mut renewed_all_socket, &renewed_all.topic).await;
    ws_disconnect(btc_socket).await;
    ws_disconnect(renewed_all_socket).await;
    state.shutdown_market_stats().await;

    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_reducer_tracks_receipts_clears_removals_and_reconnect() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let (url, state, stop, server) = backend(source).await;
    let client = reqwest::Client::new();
    let mut all_socket = stats_socket(&url).await;
    let mut btc_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let eth = native_id("ETH");
    let fields = ["funding", "markPrice", "volume24h", "lastSettledFunding"];
    let all_request = json!({"fields":fields});
    let btc_request = json!({"marketIds":[btc],"fields":fields});
    ws_send(&mut all_socket, stats_command("subscribe", &all_request)).await;
    ws_send(&mut btc_socket, stats_command("subscribe", &btc_request)).await;
    let mut all = ws_initial(&mut all_socket).await;
    let mut selected = ws_initial(&mut btc_socket).await;
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    let initial_generation = selected.generation.clone();
    let initial_funding = selected.markets[&btc]["fields"]["funding"].clone();

    after_receipt(&initial_funding["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
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
            .get("lastSettledFunding")
            .is_none(),
        "unchanged unsupported fields must remain sparse"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["lastSettledFunding"]["state"],
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
    mock.primary.write().await.body[1][2]["dayNtlVlm"] = json!("777777");
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_delta(&mut all_socket, &mut all).await;
    let invalid = ws_delta(&mut btc_socket, &mut selected).await;
    let invalid_update = ws_update(&invalid, &btc);
    assert_eq!(invalid_update["fields"]["funding"]["state"], "unavailable");
    assert_eq!(invalid_update["fields"]["funding"]["value"], Value::Null);
    // The new numeric volume member reaches the revisioned WS consumer too.
    assert_eq!(
        invalid_update["fields"]["volume24h"]["value"]["quoteVolume"],
        777777.0
    );
    let invalid_funding = selected.markets[&btc]["fields"]["funding"].clone();
    assert_eq!(invalid_funding["reason"], "invalid-upstream-value");
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["value"]["amount"],
        "61000"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["value"]["quoteVolume"],
        777777.0
    );
    let mark_receipt = selected.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"].clone();
    assert_ws_matches_http(&client, &url, &all).await;
    assert_ws_matches_http(&client, &url, &selected).await;

    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    tokio::time::sleep(Duration::from_secs(31)).await;
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
    tokio::time::sleep(Duration::from_secs(31)).await;
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
        4,
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
    tokio::time::sleep(Duration::from_secs(31)).await;
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

    *mock.primary.write().await = Reply::ok(primary_fixture());
    after_receipt(&all.markets[&eth]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let added = ws_delta(&mut all_socket, &mut all).await;
    let added_btc = added["updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == btc)
        .unwrap();
    assert_eq!(
        added_btc["fields"].as_object().unwrap().len(),
        4,
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
    ws_unsubscribe(&mut reconnected_socket, &reconnected.topic).await;
    ws_unsubscribe(&mut all_socket, &all.topic).await;
    ws_disconnect(reconnected_socket).await;
    ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;

    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_classifies_errors_and_limits_distinct_connection_topics() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let (url, state, stop, server) = backend(source).await;
    let mut socket = stats_socket(&url).await;

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
    assert_eq!(
        mock.primary_calls(),
        0,
        "invalid commands must not acquire source data"
    );
    assert_eq!(mock.spot_calls(), 0);
    ws_error(
        &mut socket,
        stats_command("subscribe", &json!({"marketIds":[native_id("UNKNOWN")]})),
        "INVALID_TOPIC",
    )
    .await;
    assert!(
        mock.primary_calls() >= 1,
        "catalog proof distinguishes an unknown ID from an upstream failure"
    );

    let names = ["ETH", "BTC", "HYPE", "SOL", "DOGE"];
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
    ws_disconnect(socket).await;
    ws_disconnect(other_socket).await;

    // No HTTP lease exists: closing both populated maps must release every successful lease.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let mut probe = stats_socket(&url).await;
    ws_ping(&mut probe).await;
    ws_send(&mut probe, stats_command("subscribe", &topics[0])).await;
    let renewed = ws_initial(&mut probe).await;
    assert_ne!(renewed.generation, first.generation);
    assert_eq!(
        renewed.markets[&native_id("ETH")]["fields"]["funding"]["state"],
        "available"
    );
    ws_unsubscribe(&mut probe, &renewed.topic).await;
    ws_disconnect(probe).await;
    state.shutdown_market_stats().await;

    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

#[tokio::test]
async fn market_stats_ws_cold_all_market_failure_recovers_without_selected_lease_leaks() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    *mock.primary.write().await = Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"offline":true}),
        barrier: None,
    };
    let source: Arc<dyn MarketDataExchange> =
        ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let (url, state, stop, server) = backend(source).await;
    let mut selected_socket = stats_socket(&url).await;
    let mut all_socket = stats_socket(&url).await;
    let btc = native_id("BTC");
    let selected_request = json!({"marketIds":[btc]});

    ws_error(
        &mut selected_socket,
        stats_command("subscribe", &selected_request),
        "SUBSCRIBE_FAILED",
    )
    .await;
    assert!(mock.primary_calls() >= 1);
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_ping(&mut selected_socket).await;
    assert_eq!(
        mock.primary_calls(),
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
    assert_eq!(mock.primary_calls(), 2);

    *mock.primary.write().await = Reply::ok(primary_fixture());
    tokio::time::sleep(Duration::from_secs(31)).await;
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
    ws_unsubscribe(&mut all_socket, &all.topic).await;
    ws_disconnect(selected_socket).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_ping(&mut all_socket).await;
    let after = mock.primary_calls();
    tokio::time::sleep(Duration::from_secs(31)).await;
    ws_ping(&mut all_socket).await;
    assert_eq!(
        mock.primary_calls(),
        after,
        "recovery and duplicate subscriptions must release demand exactly once"
    );
    ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;

    stop.send(()).unwrap();
    server.await.unwrap();
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}

// ---------------------------------------------------------------------------
// Direct source smoke (no native cache: the coordinator owns sharing)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn market_stats_direct_source_reports_stock_rows_and_receipts() {
    let (mock, upstream, up_stop, up_server) = hyperliquid_server().await;
    let source = ccxt_stats_exchange(Venue::Hyperliquid, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(source_params()).await.unwrap();
    assert!(snapshot.catalog_known);
    assert!(snapshot
        .complete_catalogs
        .contains(&UnifiedMarketType::Perp));
    assert!(snapshot
        .complete_catalogs
        .contains(&UnifiedMarketType::Spot));
    assert!(snapshot.received_at.is_some());
    let btc = snapshot
        .rows
        .iter()
        .find(|row| row.market.identity.as_ref().unwrap().market_id == native_id("BTC"))
        .unwrap();
    let Some(MarketStatsValue::Funding(funding)) =
        &btc.fields[&MarketStatsFieldName::Funding].value
    else {
        panic!("BTC funding missing")
    };
    assert_eq!(funding.rate, "-0.0000125");
    let Some(MarketStatsValue::Volume24h(volume)) =
        &btc.fields[&MarketStatsFieldName::Volume24h].value
    else {
        panic!("BTC volume missing")
    };
    assert_eq!(volume.base_volume, None);
    assert_eq!(volume.quote_volume, Some(500000.0));
    assert!(snapshot.rows.iter().any(|row| {
        row.market.identity.as_ref().unwrap().market_id == native_id("OLD") && !row.market.active
    }));
    assert!(mock.primary_calls() >= 1);
    assert!(mock.spot_calls() >= 1);
    up_stop.send(()).unwrap();
    up_server.await.unwrap();
}
