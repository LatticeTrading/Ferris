use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    extract::{
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::lighterxyz::{LighterExchange, LighterMarketCatalogService},
    models::UnifiedMarketType,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};

#[derive(Clone)]
struct LighterMock {
    catalog: Arc<RwLock<Value>>,
    metadata: Arc<RwLock<Value>>,
    metadata_status: Arc<RwLock<StatusCode>>,
    catalog_count: Arc<AtomicUsize>,
    metadata_count: Arc<AtomicUsize>,
    ws_count: Arc<AtomicUsize>,
}

fn catalog_fixture() -> Value {
    json!([
        {"symbol":"BTC","market_index":86},
        {"symbol":"ETH","market_index":87},
        {"symbol":"BTC/USDC","market_index":2050},
        {"symbol":"DOGE","market_index":99}
    ])
}

fn metadata_fixture() -> Value {
    json!({
        "code": 200,
        "order_book_details": [
            {"market_id":86,"symbol":"BTC","market_type":"perp","status":"active","quote_asset_id":7},
            {"market_id":87,"symbol":"ETH","market_type":"perp","status":"inactive","quote_asset_id":7},
            {"market_id":99,"symbol":"DOGE","market_type":"perp","status":"active","quote_asset_id":7}
        ],
        "spot_order_book_details": [
            {"market_id":2050,"symbol":"BTC/USDC","market_type":"spot","status":"active"}
        ]
    })
}

async fn catalog_handler(State(mock): State<LighterMock>) -> Json<Value> {
    mock.catalog_count.fetch_add(1, Ordering::SeqCst);
    Json(mock.catalog.read().await.clone())
}

async fn metadata_handler(State(mock): State<LighterMock>) -> (StatusCode, Json<Value>) {
    mock.metadata_count.fetch_add(1, Ordering::SeqCst);
    (
        *mock.metadata_status.read().await,
        Json(mock.metadata.read().await.clone()),
    )
}

async fn stats_ws_handler(
    ws: WebSocketUpgrade,
    State(mock): State<LighterMock>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stats_ws(socket, mock))
}

async fn stats_ws(mut socket: WebSocket, mock: LighterMock) {
    mock.ws_count.fetch_add(1, Ordering::SeqCst);
    let Some(Ok(AxumMessage::Text(command))) = socket.next().await else {
        return;
    };
    assert!(
        command.contains("market_stats/all"),
        "unexpected native subscribe: {command}"
    );
    let _ = socket
        .send(AxumMessage::Text(
            json!({"type":"subscribed","channel":"market_stats:all"})
                .to_string()
                .into(),
        ))
        .await;
    let update = json!({
        "type":"update/market_stats", "channel":"market_stats:all", "timestamp":1722339649,
        "market_stats": {
            "86": {"market_id":86,"current_funding_rate":"-0.00100","funding_rate":"0.00050","funding_timestamp":1722339600,"mark_price":"60000.00","index_price":"59999.50","last_trade_price":"60001.00"},
            "99": {"market_id":99,"current_funding_rate":"0.00010","funding_rate":"0.00020","funding_timestamp":1722339600,"mark_price":"0.100","index_price":"0.099","last_trade_price":"0.101"}
        }
    });
    let _ = socket
        .send(AxumMessage::Text(update.to_string().into()))
        .await;
    while let Some(Ok(message)) = socket.next().await {
        match message {
            AxumMessage::Ping(payload) => {
                let _ = socket.send(AxumMessage::Pong(payload)).await;
            }
            AxumMessage::Text(text) if text.contains("\"type\":\"ping\"") => {
                let _ = socket
                    .send(AxumMessage::Text(json!({"type":"pong"}).to_string().into()))
                    .await;
            }
            AxumMessage::Close(_) => break,
            _ => {}
        }
    }
}

async fn native_server() -> (LighterMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = LighterMock {
        catalog: Arc::new(RwLock::new(catalog_fixture())),
        metadata: Arc::new(RwLock::new(metadata_fixture())),
        metadata_status: Arc::new(RwLock::new(StatusCode::OK)),
        catalog_count: Arc::new(AtomicUsize::new(0)),
        metadata_count: Arc::new(AtomicUsize::new(0)),
        ws_count: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/markets", get(catalog_handler))
        .route("/api/v1/orderBookDetails", get(metadata_handler))
        .route("/stream", get(stats_ws_handler))
        .with_state(mock.clone());
    let (url, stop, task) = super::spawn_server(app).await;
    (mock, url, stop, task)
}

fn native_id(kind: &str, id: u64) -> String {
    let product = match kind {
        "spot" => UnifiedMarketType::Spot,
        _ => UnifiedMarketType::Perp,
    };
    ferris_market_data_backend::market_stats::make_market_id(
        "lighterxyz",
        product,
        None,
        None,
        &id.to_string(),
    )
    .unwrap()
}

#[tokio::test]
async fn lighter_catalog_uses_numeric_ids_statuses_and_cold_capabilities() {
    let (native, base, stop, task) = native_server().await;
    let catalog = Arc::new(
        LighterMarketCatalogService::new(2_000, format!("{base}/markets"), 60_000).unwrap(),
    );
    let source = Arc::new(
        LighterExchange::new(base.clone(), 2_000, catalog)
            .unwrap()
            .with_stats_ws_url(format!("{base}/stream").replace("http://", "ws://"), 2_000),
    );
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
        .find(|v| v["exchange"] == "lighterxyz")
        .unwrap();
    assert_eq!(lighter["marketStats"]["upstreamMode"], "nativeWebSocket");
    assert_eq!(native.metadata_count.load(Ordering::SeqCst), 0);
    let active = client
        .post(format!("{url}/v1/fetchMarkets"))
        .json(&json!({"exchange":"lighterxyz","includeInactive":false}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let all = client
        .post(format!("{url}/v1/fetchMarkets"))
        .json(&json!({"exchange":"lighterxyz","includeInactive":true}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(active["markets"].as_array().unwrap().len(), 3);
    assert_eq!(all["markets"].as_array().unwrap().len(), 4);
    let inactive = all["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == native_id("perp", 87))
        .unwrap();
    assert_eq!(inactive["active"], false);
    assert_eq!(inactive["exchangeMarketId"], "87");
    assert_eq!(
        native.catalog_count.load(Ordering::SeqCst),
        0,
        "catalog/stats must not read Explorer"
    );
    assert_eq!(native.metadata_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    backend_stop.send(()).unwrap();
    backend_task.await.unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_http_ws_projection_shares_metadata_and_native_acquisition() {
    let (native, base, stop, task) = native_server().await;
    let catalog = Arc::new(
        LighterMarketCatalogService::new(2_000, format!("{base}/markets"), 60_000).unwrap(),
    );
    let source = Arc::new(
        LighterExchange::new(base.clone(), 2_000, catalog)
            .unwrap()
            .with_stats_ws_url(format!("{base}/stream").replace("http://", "ws://"), 2_000),
    );
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let all_req = json!({"exchange":"lighterxyz","fields":["funding","lastSettledFunding","markPrice","indexPrice","lastPrice"]});
    let selected_req = json!({"exchange":"lighterxyz","marketIds":[native_id("perp",86),native_id("spot",2050),native_id("perp",87)],"fields":["funding","lastSettledFunding","markPrice","indexPrice","lastPrice"]});
    let (all, selected) = tokio::join!(
        super::stats_http(&client, &url, all_req, StatusCode::OK),
        super::stats_http(&client, &url, selected_req, StatusCode::OK)
    );
    assert_eq!(all["coverage"]["enumerationComplete"], true);
    let btc = all["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["marketId"] == native_id("perp", 86))
        .unwrap();
    assert_eq!(btc["fields"]["funding"]["value"]["rate"], "-0.00100");
    assert_eq!(
        btc["fields"]["lastSettledFunding"]["value"]["rate"],
        "0.00050"
    );
    assert_eq!(
        btc["fields"]["lastSettledFunding"]["value"]["paymentTimestamp"],
        1722339600000u64
    );
    for name in ["markPrice", "indexPrice", "lastPrice"] {
        assert!(
            btc["fields"][name]["value"]["amount"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                > 0.0
        );
    }
    let spot = selected["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["type"] == "spot")
        .unwrap();
    assert_eq!(spot["fields"]["funding"]["state"], "notApplicable");
    let inactive = selected["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["marketId"] == native_id("perp", 87))
        .unwrap();
    assert_eq!(inactive["active"], false);
    assert_eq!(inactive["fields"]["funding"]["reason"], "inactive-market");
    assert_eq!(native.ws_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    backend_stop.send(()).unwrap();
    stop.send(()).unwrap();
    backend_task.await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn lighter_metadata_failure_rejects_selected_but_complete_unknown_is_validation_error() {
    let (native, base, stop, task) = native_server().await;
    let catalog = Arc::new(
        LighterMarketCatalogService::new(2_000, format!("{base}/markets"), 60_000).unwrap(),
    );
    let source = Arc::new(
        LighterExchange::new(base.clone(), 2_000, catalog)
            .unwrap()
            .with_stats_ws_url(format!("{base}/stream").replace("http://", "ws://"), 2_000),
    );
    let (url, state, backend_stop, backend_task) = super::backend(source).await;
    let client = reqwest::Client::new();
    *native.metadata_status.write().await = StatusCode::BAD_GATEWAY;
    let failed = super::stats_http(
        &client,
        &url,
        json!({"exchange":"lighterxyz","marketIds":[native_id("perp",86)]}),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    assert_eq!(failed["code"], "UPSTREAM_REQUEST_FAILED");
    *native.metadata_status.write().await = StatusCode::OK;
    let unknown = super::stats_http(
        &client,
        &url,
        json!({"exchange":"lighterxyz","marketIds":[native_id("perp",12345)]}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(unknown["code"], "VALIDATION_ERROR");
    assert!(native.metadata_count.load(Ordering::SeqCst) >= 2);
    state.shutdown_market_stats().await;
    backend_stop.send(()).unwrap();
    stop.send(()).unwrap();
    backend_task.await.unwrap();
    task.await.unwrap();
}
