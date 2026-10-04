//! Stock Lighter statistics: `orderBookDetails` catalog + REST last/volume,
//! with mark/index/last/funding/settled supplied by the maintained stock
//! `watch_tickers` aggregate on the same websocket URL as public feeds.
//!
//! The fixture is the only upstream. Nothing here depends on a native Lighter
//! parser or a native statistics socket.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::{
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::{
        ccxt::{CcxtExchange, Venue},
        registry::ExchangeRegistry,
        traits::MarketDataExchange,
    },
    models::UnifiedMarketType,
    realtime::RealtimeService,
    web::{self, AppState},
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};

const FIELDS: [&str; 7] = [
    "funding",
    "markPrice",
    "indexPrice",
    "lastPrice",
    "lastSettledFunding",
    "volume24h",
    "openInterest",
];

#[derive(Clone)]
struct LighterMock {
    metadata: Arc<RwLock<super::Reply>>,
    metadata_count: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    /// Queued `market_stats/all` frames, consumed in subscribe order. Once the
    /// queue is exhausted the fixture stays silent, modelling a finite frame
    /// followed by no further observations.
    frames: Arc<RwLock<Vec<Value>>>,
    sent: Arc<AtomicUsize>,
}

fn metadata_fixture() -> Value {
    json!({
        "code": 200,
        "order_book_details": [
            {"market_id":86,"symbol":"BTC","market_type":"perp","status":"active","quote_asset_id":7,
             "last_trade_price":"50000.00","daily_base_token_volume":100.0,"daily_quote_token_volume":5000000.0},
            {"market_id":87,"symbol":"ETH","market_type":"perp","status":"inactive","quote_asset_id":7,
             "last_trade_price":"1.0","daily_base_token_volume":1.0,"daily_quote_token_volume":1.0},
            {"market_id":99,"symbol":"DOGE","market_type":"perp","status":"active","quote_asset_id":7,
             "last_trade_price":"0.10","daily_base_token_volume":200.0,"daily_quote_token_volume":20.0}
        ],
        "spot_order_book_details": [
            {"market_id":2050,"symbol":"BTC/USDC","market_type":"spot","status":"active",
             "last_trade_price":"50000.00","daily_base_token_volume":10.0,"daily_quote_token_volume":500000.0}
        ]
    })
}

/// One `market_stats/all` frame: mark/index/last/current+settled funding plus
/// volume for two perpetuals. Deliberately finite — the fixture sends it once.
fn live_frame() -> Value {
    json!({
        "type":"update/market_stats",
        "channel":"market_stats:all",
        "market_stats": {
            "86": {"market_id":86,"index_price":"59999.50","mark_price":"60000.00",
                   "open_interest":"123.0","last_trade_price":"50000.00",
                   "current_funding_rate":"0.0012","funding_rate":"0.0005",
                   "funding_timestamp":1722339600004i64,
                   "daily_base_token_volume":100.0,"daily_quote_token_volume":5000000.0},
            "99": {"market_id":99,"index_price":"0.099","mark_price":"0.100",
                   "open_interest":"9.0","last_trade_price":"0.10",
                   "current_funding_rate":"-0.0025","funding_rate":"0.0010",
                   "funding_timestamp":1722339600004i64,
                   "daily_base_token_volume":200.0,"daily_quote_token_volume":20.0}
        }
    })
}

async fn metadata_handler(State(mock): State<LighterMock>) -> (StatusCode, Json<Value>) {
    mock.metadata_count.fetch_add(1, Ordering::SeqCst);
    let reply = mock.metadata.read().await.clone();
    if let Some(gate) = reply.barrier {
        gate.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

async fn stream_handler(
    ws: WebSocketUpgrade,
    State(mock): State<LighterMock>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream_socket(socket, mock))
}

async fn stream_socket(mut socket: WebSocket, mock: LighterMock) {
    mock.connections.fetch_add(1, Ordering::SeqCst);
    while let Some(Ok(message)) = socket.next().await {
        match message {
            AxumMessage::Text(text) => {
                if text.contains("market_stats/all") {
                    let index = mock.sent.fetch_add(1, Ordering::SeqCst);
                    let frame = mock.frames.read().await.get(index).cloned();
                    if let Some(frame) = frame {
                        let _ = socket
                            .send(AxumMessage::Text(frame.to_string().into()))
                            .await;
                    }
                } else if text.contains("\"type\":\"ping\"") {
                    let _ = socket
                        .send(AxumMessage::Text(json!({"type":"pong"}).to_string().into()))
                        .await;
                }
            }
            AxumMessage::Ping(payload) => {
                let _ = socket.send(AxumMessage::Pong(payload)).await;
            }
            AxumMessage::Close(_) => break,
            _ => {}
        }
    }
}

async fn server() -> (LighterMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = LighterMock {
        metadata: Arc::new(RwLock::new(super::Reply::ok(metadata_fixture()))),
        metadata_count: Arc::new(AtomicUsize::new(0)),
        connections: Arc::new(AtomicUsize::new(0)),
        frames: Arc::new(RwLock::new(vec![live_frame()])),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let (base, stop, task) = super::spawn_server(
        Router::new()
            .route(
                "/api/v1/assetDetails",
                get(|| async {
                    Json(json!({"code":200,"asset_details":[
                        {"asset_id":7,"symbol":"USDC","decimals":6}
                    ]}))
                }),
            )
            .route("/api/v1/orderBookDetails", get(metadata_handler))
            .route("/stream", get(stream_handler))
            .with_state(mock.clone()),
    )
    .await;
    (mock, base, stop, task)
}

/// A backend whose registry and realtime service share one `CcxtService`, so a
/// public feed and the statistics aggregate can prove URL sharing.
async fn shared_backend(
    venue: Venue,
    base: &str,
    timeout_ms: u64,
) -> (String, AppState, oneshot::Sender<()>, JoinHandle<()>) {
    let service = super::ccxt_service(venue, base, timeout_ms);
    let mut registry = ExchangeRegistry::new();
    for candidate in Venue::ALL {
        registry.register(Arc::new(CcxtExchange::new(candidate, service.clone())));
    }
    let state = AppState::new(Arc::new(registry), RealtimeService::new(service));
    let app = Router::new()
        .route("/v1/fetchMarketStats", post(web::fetch_market_stats))
        .route("/v1/capabilities", get(web::capabilities))
        .route("/v1/ws", get(web::trades_stream_ws))
        .with_state(state.clone());
    let (url, tx, task) = super::spawn_server(app).await;
    (url, state, tx, task)
}

fn id(market_id: u64) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "lighterxyz",
        UnifiedMarketType::Perp,
        None,
        None,
        &market_id.to_string(),
    )
    .unwrap()
}

fn spot_id(market_id: u64) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "lighterxyz",
        UnifiedMarketType::Spot,
        None,
        None,
        &market_id.to_string(),
    )
    .unwrap()
}

fn request(ids: Option<Vec<String>>) -> Value {
    json!({"exchange":"lighterxyz", "params":{}, "marketIds":ids, "fields":FIELDS})
}

fn row<'a>(rows: &'a Value, market_id: &str) -> &'a Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == market_id)
        .unwrap()
}

async fn settle_connections(mock: &LighterMock, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while mock.connections.load(Ordering::SeqCst) < expected {
        assert!(
            std::time::Instant::now() < deadline,
            "upstream websocket never connected"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn lighter_capabilities_are_no_io_and_catalog_comes_from_order_book_details() {
    let (mock, base, stop, task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Lighter, &base, 2_000);
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let caps: Value = client
        .get(format!("{url}/v1/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let lighter = caps["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["exchange"] == "lighterxyz")
        .unwrap();
    assert_eq!(
        lighter["marketStats"]["upstreamMode"],
        "sharedPollingAndWebSocket"
    );
    assert_eq!(
        lighter["marketStats"]["fundingKinds"],
        json!(["estimate", "settled"])
    );
    assert_eq!(mock.metadata_count.load(Ordering::SeqCst), 0);
    assert_eq!(mock.connections.load(Ordering::SeqCst), 0);

    let all = super::stats_http(&client, &url, request(None), StatusCode::OK).await;
    assert_eq!(all["coverage"]["enumerationComplete"], true);
    // All-market scope is perp-only and active-only: 86 and 99, never 87 or spot.
    assert_eq!(all["markets"].as_array().unwrap().len(), 2);
    let ids: Vec<&str> = all["markets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["marketId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&id(86).as_str()));
    assert!(ids.contains(&id(99).as_str()));
    assert!(!ids.contains(&id(87).as_str()));
    assert!(!ids.contains(&spot_id(2050).as_str()));
    // Catalog and tickers are each fetched from orderBookDetails.
    assert!(
        (1..=2).contains(&mock.metadata_count.load(Ordering::SeqCst)),
        "{}",
        mock.metadata_count.load(Ordering::SeqCst)
    );
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_rest_and_live_watch_cover_distinct_fields_on_one_url() {
    let (mock, base, stop, task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Lighter, &base, 2_000);
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let all = super::stats_http(&client, &url, request(None), StatusCode::OK).await;
    assert_eq!(all["coverage"]["enumerationComplete"], true);
    let btc = row(&all["markets"], &id(86));
    // Live-only fields come from the maintained stock watch.
    assert_eq!(btc["fields"]["markPrice"]["state"], "available");
    assert_eq!(btc["fields"]["markPrice"]["value"]["amount"], "60000.00");
    assert_eq!(btc["fields"]["markPrice"]["value"]["baseAsset"], "BTC");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "59999.50");
    assert_eq!(
        btc["fields"]["markPrice"]["source"],
        "lighterxyz:ccxt:watchTickers"
    );
    // Current funding is an hourly percent estimate; settled funding is a
    // distinct hourly percent with an explicit payment timestamp.
    let funding = &btc["fields"]["funding"]["value"];
    assert_eq!(funding["rate"], "0.0012");
    assert_eq!(funding["rateUnit"], "percent");
    assert_eq!(funding["kind"], "estimate");
    assert_eq!(funding["rateIntervalMs"], 3_600_000);
    assert_eq!(funding["paymentTimestamp"], Value::Null);
    let settled = &btc["fields"]["lastSettledFunding"]["value"];
    assert_eq!(settled["rate"], "0.0005");
    assert_eq!(settled["rateUnit"], "percent");
    assert_eq!(settled["kind"], "settled");
    assert_eq!(settled["paymentTimestamp"], 1722339600004u64);
    // REST supplies last + volume.
    assert_eq!(btc["fields"]["lastPrice"]["value"]["amount"], "50000.00");
    assert_eq!(btc["fields"]["volume24h"]["value"]["baseVolume"], 100.0);
    assert_eq!(
        btc["fields"]["volume24h"]["value"]["quoteVolume"],
        5000000.0
    );
    // Open interest stays unqualified.
    assert_eq!(btc["fields"]["openInterest"]["state"], "unsupported");
    // Distinct rows carry distinct funding.
    let doge = row(&all["markets"], &id(99));
    assert_eq!(doge["fields"]["funding"]["value"]["rate"], "-0.0025");
    assert_eq!(
        doge["fields"]["lastSettledFunding"]["value"]["rate"],
        "0.0010"
    );

    // Spot is selectable but has no funding/mark/index and no contract OI.
    let selected = super::stats_http(
        &client,
        &url,
        request(Some(vec![spot_id(2050), id(87)])),
        StatusCode::OK,
    )
    .await;
    let spot = selected["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "spot")
        .unwrap();
    assert_eq!(spot["fields"]["funding"]["state"], "notApplicable");
    assert_eq!(spot["fields"]["markPrice"]["state"], "notApplicable");
    assert_eq!(spot["fields"]["openInterest"]["state"], "notApplicable");
    assert_eq!(spot["fields"]["lastPrice"]["state"], "available");
    assert_eq!(spot["fields"]["volume24h"]["state"], "available");
    let inactive = row(&selected["markets"], &id(87));
    assert_eq!(inactive["active"], false);
    // REST-sourced fields on an inactive market are unavailable, never deleted.
    assert_eq!(inactive["fields"]["lastPrice"]["reason"], "inactive-market");
    assert_eq!(inactive["fields"]["volume24h"]["reason"], "inactive-market");
    // Live-only fields are never published for an inactive market.
    assert_ne!(inactive["fields"]["funding"]["state"], "available");
    assert_eq!(inactive["fields"]["funding"]["value"], Value::Null);
    assert_eq!(mock.connections.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_public_feed_shares_the_statistics_websocket_url() {
    let (mock, base, stop, task) = server().await;
    let (url, state, backend_stop, backend_task) =
        shared_backend(Venue::Lighter, &base, 2_000).await;
    let client = reqwest::Client::new();
    // Statistics feed first, so the shared URL owner exists with the full catalog.
    let all = super::stats_http(&client, &url, request(None), StatusCode::OK).await;
    assert_eq!(all["coverage"]["enumerationComplete"], true);
    settle_connections(&mock, 1).await;
    // A public trades subscription on the same venue must reuse that connection.
    let mut socket = super::stats_socket(&url).await;
    super::ws_send(
        &mut socket,
        json!({"op":"subscribe","channel":"trades","exchange":"lighterxyz",
               "symbol":"BTC/USDC:USDC","params":{}}),
    )
    .await;
    let ack = super::ws_next(&mut socket).await;
    assert_eq!(ack["type"], "subscribed", "{ack}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    while std::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        mock.connections.load(Ordering::SeqCst),
        1,
        "statistics and public feeds must share one upstream websocket"
    );
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_finite_frame_then_silence_keeps_live_receipts_across_bulk_polls() {
    let (mock, base, stop, task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Lighter, &base, 2_000);
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&url).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let btc = id(86);
    let live_receipt = view.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"].clone();
    let live_funding = view.markets[&btc]["fields"]["funding"]["receivedTimestamp"].clone();
    let rest_receipt = view.markets[&btc]["fields"]["lastPrice"]["receivedTimestamp"].clone();
    assert!(live_receipt.is_u64());
    assert!(rest_receipt.is_u64());
    // The finite frame delivered both rows even though no later frame arrives.
    assert_eq!(
        view.markets[&id(99)]["fields"]["markPrice"]["state"],
        "available"
    );
    // Cross the receipt millisecond, then let the 30s bulk REST poll run again.
    super::after_receipt(&view.markets[&btc]["fields"]["lastPrice"]["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    super::ws_delta(&mut socket, &mut view).await;
    // REST last/volume are re-observed; the silent live fields keep their receipt.
    assert_eq!(
        view.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"], live_receipt,
        "a repeated bulk read must not renew an untouched live field"
    );
    assert_eq!(
        view.markets[&btc]["fields"]["funding"]["receivedTimestamp"],
        live_funding
    );
    assert_eq!(
        view.markets[&btc]["fields"]["lastSettledFunding"]["receivedTimestamp"],
        live_funding
    );
    assert_ne!(
        view.markets[&btc]["fields"]["lastPrice"]["receivedTimestamp"], rest_receipt,
        "REST-sourced fields do advance with a new bulk observation"
    );
    assert!(mock.metadata_count.load(Ordering::SeqCst) >= 2);
    super::assert_ws_matches_http(&client, &url, &view).await;
    // A second silent poll must still not renew the live receipts.
    super::after_receipt(&view.markets[&btc]["fields"]["lastPrice"]["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    super::ws_delta(&mut socket, &mut view).await;
    assert_eq!(
        view.markets[&btc]["fields"]["markPrice"]["receivedTimestamp"],
        live_receipt
    );
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_live_invalid_scalars_clear_only_themselves() {
    let (mock, base, stop, task) = server().await;
    // One frame: mark is invalid for 86, current funding is explicitly null,
    // while index/last/volume stay readable.
    let mut frame = live_frame();
    frame["market_stats"]["86"]["mark_price"] = json!("not-a-number");
    frame["market_stats"]["86"]["current_funding_rate"] = Value::Null;
    *mock.frames.write().await = vec![frame];
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Lighter, &base, 2_000);
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let all = super::stats_http(&client, &url, request(None), StatusCode::OK).await;
    let btc = row(&all["markets"], &id(86));
    assert_eq!(btc["fields"]["markPrice"]["state"], "unavailable");
    assert_eq!(
        btc["fields"]["markPrice"]["reason"],
        "invalid-upstream-value"
    );
    assert_eq!(btc["fields"]["markPrice"]["value"], Value::Null);
    assert_eq!(btc["fields"]["funding"]["state"], "unavailable");
    assert_eq!(btc["fields"]["funding"]["value"], Value::Null);
    // Sibling observations in the same row survive.
    assert_eq!(btc["fields"]["indexPrice"]["state"], "available");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "59999.50");
    assert_eq!(btc["fields"]["lastPrice"]["state"], "available");
    assert_eq!(btc["fields"]["lastSettledFunding"]["state"], "available");
    assert_eq!(btc["fields"]["volume24h"]["state"], "available");
    // A second row in the same frame is unaffected.
    let doge = row(&all["markets"], &id(99));
    assert_eq!(doge["fields"]["markPrice"]["state"], "available");
    assert_eq!(doge["fields"]["funding"]["value"]["rate"], "-0.0025");
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_catalog_failure_and_selected_validation() {
    let (mock, base, stop, task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Lighter, &base, 2_000);
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    *mock.metadata.write().await = super::Reply {
        status: StatusCode::BAD_GATEWAY,
        body: json!({"error":"offline"}),
        barrier: None,
    };
    let failed = super::stats_http(
        &client,
        &url,
        request(Some(vec![id(86)])),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    assert_eq!(failed["code"], "UPSTREAM_REQUEST_FAILED");
    *mock.metadata.write().await = super::Reply::ok(metadata_fixture());
    let unknown = super::stats_http(
        &client,
        &url,
        request(Some(vec![id(12345)])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(unknown["code"], "VALIDATION_ERROR");
    // A complete catalog makes an unknown ID a validation error, not an outage.
    assert!(mock.metadata_count.load(Ordering::SeqCst) >= 2);
    state.shutdown_market_stats().await;
    state.shutdown_realtime().await.unwrap();
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}
