//! Stock Extended statistics: hourly estimate funding, native collateral
//! identity, RFQ/off-hours metadata, and the new numeric volume/open-interest
//! members through real HTTP and revisioned WS consumers.
//!
//! Extended's stock `fetchCurrencies` calls `GET /api/v1/info/assets`; the real
//! public endpoint answers 403. Fixtures serve the asset list so the loader is
//! exercised, and one test proves a 403 surfaces as an upstream failure instead
//! of being silently swallowed or replaced with a native fallback.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::{Request, StatusCode},
    Json, Router,
};
use serde_json::{json, Value};
use tokio::{
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};

const IMPLEMENTED: [&str; 6] = [
    "funding",
    "markPrice",
    "indexPrice",
    "lastPrice",
    "volume24h",
    "openInterest",
];
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
struct ExtendedMock {
    markets: Arc<RwLock<super::Reply>>,
    assets: Arc<RwLock<super::Reply>>,
    market_count: Arc<AtomicUsize>,
    asset_count: Arc<AtomicUsize>,
}

impl ExtendedMock {
    async fn markets(&self, rows: Vec<Value>) {
        *self.markets.write().await = super::Reply::ok(envelope(rows));
    }

    fn calls(&self) -> usize {
        self.market_count.load(Ordering::SeqCst)
    }
}

/// Stock `marketStats` values are lexical strings; both numeric members of
/// `volume24h`/`openInterest` may be present, absent, or explicitly invalid.
fn market(name: &str, base: &str, quote: &str) -> Value {
    json!({
        "name":name, "assetName":base, "collateralAssetName":quote,
        "type":"PERPETUAL", "active":true, "status":"ACTIVE",
        "isRfq":false, "isOffHours":false,
        "l2Config":{"collateralId":"0x1"},
        "marketStats":{
            "fundingRate":"-0.00001250", "markPrice":"30000.000",
            "indexPrice":"29999.900", "lastPrice":"30001.2300",
            "nextFundingRate":1700000001000u64, "timestamp":1700000000123u64,
            "dailyVolumeBase":"12.5", "dailyVolume":"375000.00",
            "openInterestBase":"1500.4", "openInterest":"45012000.00",
            "bidPrice":"29998", "askPrice":"30002"
        }
    })
}

fn markets() -> Vec<Value> {
    vec![
        market("BTC-USD", "BTC", "USD"),
        market("ALT-USD", "ALT", "USD"),
    ]
}

fn envelope(rows: Vec<Value>) -> Value {
    json!({"status":"OK", "data":rows})
}

fn assets() -> Value {
    json!({"status":"OK", "data":[
        {"id":1,"name":"USD","symbol":"USD","precision":6,"isActive":true,"isCollateral":true,"starkexId":"0x1","type":"SPOT"},
        {"id":2,"name":"BTC","symbol":"BTC","precision":5,"isActive":true,"isCollateral":true,"starkexId":"0x2","type":"SPOT"}
    ]})
}

async fn handler(
    State(mock): State<ExtendedMock>,
    request: Request<axum::body::Body>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(request.method(), "GET");
    let (count, reply) = match request.uri().path() {
        "/api/v1/info/markets" => (&mock.market_count, mock.markets.read().await.clone()),
        "/api/v1/info/assets" => (&mock.asset_count, mock.assets.read().await.clone()),
        path => panic!("unexpected Extended endpoint {path}"),
    };
    assert!(
        request.uri().query().is_none(),
        "acquisition must be unfiltered: {}",
        request.uri()
    );
    count.fetch_add(1, Ordering::SeqCst);
    if let Some(gate) = reply.barrier {
        gate.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

async fn server() -> (ExtendedMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = ExtendedMock {
        markets: Arc::new(RwLock::new(super::Reply::ok(envelope(markets())))),
        assets: Arc::new(RwLock::new(super::Reply::ok(assets()))),
        market_count: Arc::new(AtomicUsize::new(0)),
        asset_count: Arc::new(AtomicUsize::new(0)),
    };
    let (base, stop, task) =
        super::spawn_server(Router::new().fallback(handler).with_state(mock.clone())).await;
    (mock, base, stop, task)
}

fn id(name: &str) -> String {
    json!(["extended", "perp", null, null, name]).to_string()
}

fn request(ids: Option<Vec<String>>) -> Value {
    json!({"exchange":"extended", "params":{}, "marketIds":ids, "fields":FIELDS})
}

fn row<'a>(rows: &'a Value, market_id: &str) -> &'a Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == market_id)
        .unwrap()
}

fn failure(coverage: &Value, reason: &str) -> bool {
    coverage["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| {
            failure["source"] == "extended:ccxt:loadMarkets" && failure["reason"] == reason
        })
}

/// Locate the delta update for one market id (rows are sorted by opaque ID).
fn update<'a>(delta: &'a Value, market_id: &str) -> &'a Value {
    delta["updates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|update| update["marketId"] == market_id)
        .unwrap()
}

#[tokio::test]
async fn extended_capabilities_no_io_and_identity_scope_hourly_contract() {
    let (mock, upstream, up_stop, up_task) = server().await;
    let source: Arc<dyn ferris_market_data_backend::exchanges::traits::MarketDataExchange> =
        super::ccxt_stats_exchange(super::Venue::Extended, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let caps: Value = client
        .get(format!("{base}/v1/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let extended = caps["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["exchange"] == "extended")
        .unwrap();
    assert_eq!(extended["marketStats"]["state"], "supported");
    assert_eq!(extended["marketStats"]["scope"]["params"], json!({}));
    assert_eq!(extended["marketStats"]["upstreamMode"], "sharedPolling");
    assert_eq!(extended["marketStats"]["fundingKinds"], json!(["estimate"]));
    assert_eq!(extended["marketStats"]["rateIntervalMs"], 3_600_000);
    assert_eq!(extended["marketStats"]["paymentIntervalMs"], 3_600_000);
    // Capability discovery performs no upstream I/O.
    assert_eq!(mock.calls(), 0);
    assert_eq!(mock.asset_count.load(Ordering::SeqCst), 0);

    let mut alias = market("A/B-USD", "A-B", "USD");
    alias["isRfq"] = json!(true);
    alias["isOffHours"] = json!(true);
    alias["marketStats"]["fundingRate"] = json!("0");
    alias["marketStats"]["lastPrice"] = json!("1.2400");
    let mut disabled = market("OLD-USD", "OLD", "USD");
    disabled["active"] = json!(false);
    let mut reduce_only = market("REDUCE-USD", "REDUCE", "USD");
    reduce_only["status"] = json!("REDUCE_ONLY");
    let mut spot = market("ETHSPOT-USD", "ETHSPOT", "USD");
    spot["type"] = json!("SPOT");
    mock.markets(vec![
        market("BTC-USD", "BTC", "USD"),
        alias,
        disabled,
        reduce_only,
        spot,
    ])
    .await;
    let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(snapshot["scope"]["params"], json!({}));
    assert_eq!(snapshot["coverage"]["enumerationComplete"], true);
    // Only the two active perpetuals are part of the Extended profile.
    assert_eq!(snapshot["coverage"]["expectedMarkets"], 2);
    assert_eq!(snapshot["markets"].as_array().unwrap().len(), 2);
    let btc = row(&snapshot["markets"], &id("BTC-USD"));
    assert_eq!(btc["exchangeMarketId"], "BTC-USD");
    assert_eq!(btc["symbol"], "BTC/USD");
    assert_eq!(btc["base"], "BTC");
    assert_eq!(btc["quote"], "USD");
    assert_eq!(btc["settle"], "USD");
    assert_eq!(btc["settlementAssetId"], "0x1");
    assert_eq!(btc["contractType"], "PERPETUAL");
    let funding = &btc["fields"]["funding"];
    assert_eq!(funding["state"], "available");
    assert_eq!(funding["value"]["rate"], "-0.00001250");
    assert_eq!(funding["value"]["rateUnit"], "decimalFraction");
    assert_eq!(funding["value"]["kind"], "estimate");
    assert_eq!(funding["value"]["rateIntervalMs"], 3_600_000);
    assert_eq!(funding["value"]["paymentIntervalMs"], 3_600_000);
    // `nextFundingRate` is not qualified as a payment timestamp.
    assert_eq!(funding["value"]["paymentTimestamp"], Value::Null);
    assert_eq!(funding["value"]["nextPaymentTimestamp"], Value::Null);
    for (field, amount) in [
        ("markPrice", "30000.000"),
        ("indexPrice", "29999.900"),
        ("lastPrice", "30001.2300"),
    ] {
        assert_eq!(btc["fields"][field]["value"]["amount"], amount, "{field}");
        assert_eq!(btc["fields"][field]["value"]["baseAsset"], "BTC");
        assert_eq!(btc["fields"][field]["value"]["quoteAsset"], "USD");
    }
    assert_eq!(btc["fields"]["volume24h"]["value"]["baseVolume"], 12.5);
    assert_eq!(btc["fields"]["volume24h"]["value"]["quoteVolume"], 375000.0);
    assert_eq!(
        btc["fields"]["openInterest"]["value"]["openInterestAmount"],
        1500.4
    );
    assert_eq!(
        btc["fields"]["openInterest"]["value"]["openInterestValue"],
        45012000.0
    );
    assert_eq!(btc["fields"]["lastSettledFunding"]["state"], "unsupported");
    let alias_row = row(&snapshot["markets"], &id("A/B-USD"));
    assert_eq!(alias_row["info"]["isRfq"], true);
    assert_eq!(alias_row["info"]["isOffHours"], true);
    assert_eq!(
        alias_row["fields"]["lastPrice"]["value"]["amount"],
        "1.2400"
    );
    assert_eq!(alias_row["fields"]["funding"]["value"]["rate"], "0");

    let inactive = super::stats_http(
        &client,
        &base,
        request(Some(vec![id("OLD-USD"), id("REDUCE-USD")])),
        StatusCode::OK,
    )
    .await;
    for market in inactive["markets"].as_array().unwrap() {
        assert_eq!(market["active"], false);
        for field in IMPLEMENTED {
            assert_eq!(market["fields"][field]["state"], "unavailable");
            assert_eq!(market["fields"][field]["reason"], "inactive-market");
            assert_eq!(market["fields"][field]["value"], Value::Null);
        }
    }
    // A spot market is outside the Extended profile and cannot be selected.
    super::stats_http(
        &client,
        &base,
        request(Some(vec![json!([
            "extended", "spot", null, null, "SPOT-USD"
        ])
        .to_string()])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    // One shared acquisition served every request; the spot selection is rejected
    // before any acquisition, and asset metadata is fetched exactly once.
    assert!(mock.calls() <= 2, "{}", mock.calls());
    assert_eq!(mock.asset_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    up_stop.send(()).unwrap();
    up_task.await.unwrap();
}

#[tokio::test]
async fn extended_numeric_members_preserve_null_zero_and_invalid_boundaries() {
    let (mock, upstream, up_stop, up_task) = server().await;
    let source: Arc<dyn ferris_market_data_backend::exchanges::traits::MarketDataExchange> =
        super::ccxt_stats_exchange(super::Venue::Extended, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    // Both members absent -> the metric has no observation.
    let mut missing = market("MISSING-USD", "MISSING", "USD");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("dailyVolumeBase");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("dailyVolume");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("openInterestBase");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("openInterest");
    // One member absent -> the other is preserved, not zero-filled.
    let mut half = market("HALF-USD", "HALF", "USD");
    half["marketStats"]["dailyVolumeBase"] = json!("0");
    half["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("dailyVolume");
    half["marketStats"]["openInterestBase"] = json!("0");
    half["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("openInterest");
    // Explicitly invalid scalars are not silently dropped to null.
    let mut invalid = market("INVALID-USD", "INVALID", "USD");
    invalid["marketStats"]["dailyVolume"] = json!("not-a-number");
    invalid["marketStats"]["openInterestBase"] = json!(" 1.0");
    invalid["marketStats"]["fundingRate"] = json!("1e-3");
    invalid["marketStats"]["lastPrice"] = json!("1e3");
    mock.markets(vec![
        market("BTC-USD", "BTC", "USD"),
        missing,
        half,
        invalid,
    ])
    .await;
    let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(snapshot["coverage"]["enumerationComplete"], true);
    let missing = row(&snapshot["markets"], &id("MISSING-USD"));
    for field in ["volume24h", "openInterest"] {
        assert_eq!(missing["fields"][field]["state"], "unavailable");
        assert_eq!(missing["fields"][field]["reason"], "missing-upstream-row");
        assert_eq!(missing["fields"][field]["value"], Value::Null);
    }
    let half = row(&snapshot["markets"], &id("HALF-USD"));
    assert_eq!(half["fields"]["volume24h"]["state"], "available");
    assert_eq!(half["fields"]["volume24h"]["value"]["baseVolume"], 0.0);
    assert_eq!(
        half["fields"]["volume24h"]["value"]["quoteVolume"],
        Value::Null,
        "an absent member is null, not zero"
    );
    assert_eq!(half["fields"]["openInterest"]["state"], "available");
    assert_eq!(
        half["fields"]["openInterest"]["value"]["openInterestAmount"],
        0.0
    );
    assert_eq!(
        half["fields"]["openInterest"]["value"]["openInterestValue"],
        Value::Null
    );
    // A present-but-unreadable member invalidates that observation rather than
    // silently publishing a half-filled metric.
    let invalid = row(&snapshot["markets"], &id("INVALID-USD"));
    for field in ["volume24h", "openInterest"] {
        assert_eq!(invalid["fields"][field]["state"], "unavailable");
        assert_eq!(invalid["fields"][field]["reason"], "invalid-upstream-value");
        assert_eq!(invalid["fields"][field]["value"], Value::Null);
    }
    assert_eq!(invalid["fields"]["funding"]["state"], "unavailable");
    assert_eq!(
        invalid["fields"]["funding"]["reason"],
        "invalid-upstream-value"
    );
    assert_eq!(invalid["fields"]["lastPrice"]["state"], "unavailable");
    assert_eq!(invalid["fields"]["markPrice"]["state"], "available");
    // One catalog load plus one statistics acquisition, each with a single
    // `/info/markets` request; asset metadata is fetched once for the catalog.
    assert!(mock.calls() <= 2, "{}", mock.calls());
    assert_eq!(mock.asset_count.load(Ordering::SeqCst), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    up_stop.send(()).unwrap();
    up_task.await.unwrap();
}

#[tokio::test]
async fn extended_http_ws_deltas_carry_numeric_members_and_retain_on_failure() {
    let (mock, upstream, up_stop, up_task) = server().await;
    let source: Arc<dyn ferris_market_data_backend::exchanges::traits::MarketDataExchange> =
        super::ccxt_stats_exchange(super::Venue::Extended, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let btc = id("BTC-USD");
    let all_request = request(None);
    let selected_request = request(Some(vec![btc.clone()]));
    let mut all_socket = super::stats_socket(&base).await;
    let mut selected_socket = super::stats_socket(&base).await;
    let gate = Arc::new(Semaphore::new(0));
    mock.markets.write().await.barrier = Some(gate.clone());
    super::ws_send(
        &mut all_socket,
        super::stats_command("subscribe", &all_request),
    )
    .await;
    super::ws_send(
        &mut selected_socket,
        super::stats_command("subscribe", &selected_request),
    )
    .await;
    super::wait_count(&mock.market_count, 1).await;
    gate.add_permits(16);
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(all.markets[&btc]["settlementAssetId"], "0x1");
    // Two concurrent consumers share one bulk acquisition.
    assert!(mock.calls() <= 2, "{}", mock.calls());
    let shared = mock.calls();
    let http = super::stats_http(&client, &base, all_request.clone(), StatusCode::OK).await;
    all.assert_matches_snapshot(&http);
    assert_eq!(mock.calls(), shared);

    // A new numeric observation reaches both the HTTP projection and the delta.
    let mut updated = markets();
    updated[0]["marketStats"]["dailyVolume"] = json!("999000.50");
    updated[0]["marketStats"]["openInterest"] = json!("123.75");
    mock.markets(updated).await;
    super::after_receipt(&all.markets[&btc]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let delta = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let updated_row = update(&delta, &btc);
    assert_eq!(
        updated_row["fields"]["volume24h"]["value"]["quoteVolume"],
        999000.5
    );
    assert_eq!(
        updated_row["fields"]["openInterest"]["value"]["openInterestValue"],
        123.75
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["value"]["baseVolume"],
        12.5
    );
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;

    // A total upstream failure retains the numeric observation as stale.
    let retained = selected.markets[&btc]["fields"]["volume24h"].clone();
    let receipt = retained["receivedTimestamp"].clone();
    mock.markets.write().await.status = StatusCode::BAD_GATEWAY;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], false);
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["state"],
        "stale"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["value"],
        retained["value"]
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["receivedTimestamp"],
        receipt
    );
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;

    // Recovery and an authoritative removal.
    mock.markets(markets()).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(all.coverage["enumerationComplete"], true);
    assert_eq!(
        selected.markets[&btc]["fields"]["volume24h"]["state"],
        "available"
    );
    mock.markets(vec![market("ALT-USD", "ALT", "USD")]).await;
    super::after_receipt(&all.markets[&id("ALT-USD")]["fields"]["funding"]["receivedTimestamp"])
        .await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let removed = super::ws_delta(&mut selected_socket, &mut selected).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));
    assert!(selected.markets.is_empty());
    super::stats_http(&client, &base, selected_request, StatusCode::BAD_REQUEST).await;
    super::ws_unsubscribe(&mut selected_socket, &selected.topic).await;
    super::ws_unsubscribe(&mut all_socket, &all.topic).await;
    super::ws_disconnect(selected_socket).await;
    super::ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    up_stop.send(()).unwrap();
    up_task.await.unwrap();
}

#[tokio::test]
async fn extended_public_assets_403_is_a_reported_upstream_failure() {
    let (mock, upstream, up_stop, up_task) = server().await;
    *mock.assets.write().await = super::Reply {
        status: StatusCode::FORBIDDEN,
        body: json!({"error":"forbidden"}),
        barrier: None,
    };
    let source: Arc<dyn ferris_market_data_backend::exchanges::traits::MarketDataExchange> =
        super::ccxt_stats_exchange(super::Venue::Extended, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let view = super::ws_initial(&mut socket).await;
    assert!(view.markets.is_empty());
    assert_eq!(view.coverage["enumerationComplete"], false);
    assert_eq!(view.coverage["expectedMarkets"], Value::Null);
    assert!(
        failure(&view.coverage, "upstream-failure"),
        "asset 403 must be reported, not swallowed: {}",
        view.coverage
    );
    // A selected request cannot prove an unknown ID from an incomplete catalog.
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("BTC-USD")])),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    super::assert_ws_matches_http(&client, &base, &view).await;
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    up_stop.send(()).unwrap();
    up_task.await.unwrap();
}
