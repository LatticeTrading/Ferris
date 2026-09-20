use axum::{
    extract::State,
    http::{Request, StatusCode},
    Json, Router,
};
use ferris_market_data_backend::exchanges::{
    extended::ExtendedExchange, traits::MarketDataExchange,
};
use ferris_market_data_backend::models::FetchMarketsParams;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};

const IMPLEMENTED: [&str; 4] = ["funding", "markPrice", "indexPrice", "lastPrice"];
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
    reply: Arc<RwLock<super::Reply>>,
    count: Arc<AtomicUsize>,
}

impl ExtendedMock {
    async fn markets(&self, rows: Vec<Value>) {
        *self.reply.write().await = super::Reply::ok(envelope(rows));
    }

    fn calls(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

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

async fn handler(
    State(mock): State<ExtendedMock>,
    request: Request<axum::body::Body>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(request.method(), "GET");
    assert_eq!(request.uri().path(), "/api/v1/info/markets");
    assert!(
        request.uri().query().is_none(),
        "acquisition must be unfiltered"
    );
    assert!(!request.headers()["user-agent"].to_str().unwrap().is_empty());
    let reply = mock.reply.read().await.clone();
    mock.count.fetch_add(1, Ordering::SeqCst);
    if let Some(gate) = reply.barrier {
        gate.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

async fn server() -> (ExtendedMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = ExtendedMock {
        reply: Arc::new(RwLock::new(super::Reply::ok(envelope(markets())))),
        count: Arc::new(AtomicUsize::new(0)),
    };
    let (base, stop, task) =
        super::spawn_server(Router::new().fallback(handler).with_state(mock.clone())).await;
    (mock, format!("{base}/api/v1"), stop, task)
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

fn failure(snapshot: &Value, reason: &str) -> bool {
    snapshot["coverage"]["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["source"] == "extended:info/markets" && failure["reason"] == reason)
}

async fn catalog_http(client: &reqwest::Client, base: &str, include_inactive: bool) -> Value {
    let response = client
        .post(format!("{base}/v1/fetchMarkets"))
        .json(&json!({"exchange":"extended", "params":null, "includeInactive":include_inactive}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn extended_native_identity_scope_flags_and_hourly_decimal_contract() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let mut alias = market("A/B-USD", "A-B", "USD");
    alias["marketStats"]["fundingRate"] = json!("0");
    alias["isRfq"] = json!(true);
    alias["isOffHours"] = json!(true);
    alias["marketStats"]["lastPrice"] = json!("1.2400");
    let mut native = market("哈基米-Usd", "哈基米", "Usd");
    native["marketStats"]["fundingRate"] = json!("0.00010000");
    native["l2Config"]["collateralId"] = json!("native:collateral/α");
    let mut disabled = market("OLD-USD", "OLD", "USD");
    disabled["active"] = json!(false);
    let mut reduce_only = market("REDUCE-USD", "REDUCE", "USD");
    reduce_only["status"] = json!("REDUCE_ONLY");
    let mut spot = market("SPOT-USD", "SPOT", "USD");
    spot["type"] = json!("SPOT");
    mock.markets(vec![
        market("A-B-USD", "A-B", "USD"),
        alias,
        native,
        disabled,
        reduce_only,
        spot,
    ])
    .await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(ExtendedExchange::new(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let catalog = catalog_http(&client, &base, true).await;
    let active_catalog = catalog_http(&client, &base, false).await;
    let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(snapshot["scope"]["params"], json!({}));
    assert_eq!(snapshot["coverage"]["enumerationComplete"], true);
    assert_eq!(snapshot["coverage"]["expectedMarkets"], 3);
    assert_eq!(snapshot["markets"].as_array().unwrap().len(), 3);
    assert_eq!(active_catalog["markets"].as_array().unwrap().len(), 3);
    assert_eq!(catalog["markets"].as_array().unwrap().len(), 5);
    let punctuated = row(&snapshot["markets"], &id("A-B-USD"));
    let alias = row(&snapshot["markets"], &id("A/B-USD"));
    assert_eq!(punctuated["symbol"], alias["symbol"]);
    assert_eq!(punctuated["symbol"], "A-B/USD");
    assert_eq!(
        row(&snapshot["markets"], &id("哈基米-Usd"))["symbol"],
        "哈基米/Usd"
    );
    assert_ne!(punctuated["marketId"], alias["marketId"]);
    assert_eq!(alias["info"]["isRfq"], true);
    assert_eq!(alias["info"]["isOffHours"], true);
    assert_eq!(alias["active"], true);
    assert_eq!(alias["fields"]["lastPrice"]["value"]["amount"], "1.2400");
    let decimal = |value: &Value| {
        value
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap()
    };
    for (name, base_asset, quote_asset, settlement_id, rate, equivalents) in [
        (
            "A-B-USD",
            "A-B",
            "USD",
            "0x1",
            "-0.00001250",
            ["-0.00125", "-0.01", "-0.03", "-10.95"],
        ),
        ("A/B-USD", "A-B", "USD", "0x1", "0", ["0", "0", "0", "0"]),
        (
            "哈基米-Usd",
            "哈基米",
            "Usd",
            "native:collateral/α",
            "0.00010000",
            ["0.01", "0.08", "0.24", "87.6"],
        ),
    ] {
        let market = row(&snapshot["markets"], &id(name));
        let issued = row(&catalog["markets"], &id(name));
        assert_eq!(market["exchangeMarketId"], name);
        assert_eq!(market["info"]["rawSymbol"], name);
        assert_eq!(market["base"], base_asset);
        assert_eq!(market["quote"], quote_asset);
        assert_eq!(market["settle"], quote_asset);
        assert_eq!(market["settlementAssetId"], settlement_id);
        assert_eq!(market["contractType"], "PERPETUAL");
        assert_eq!(market["category"], Value::Null);
        assert_eq!(market["dex"], Value::Null);
        for key in [
            "marketId",
            "exchangeMarketId",
            "symbol",
            "base",
            "quote",
            "settle",
            "settlementAssetId",
            "info",
        ] {
            assert_eq!(market[key], issued[key]);
        }
        let funding = &market["fields"]["funding"];
        assert_eq!(funding["state"], "available");
        assert_eq!(funding["reason"], Value::Null);
        let value = &funding["value"];
        assert_eq!(value["rate"], rate);
        assert_eq!(value["rateUnit"], "decimalFraction");
        assert_eq!(value["kind"], "estimate");
        assert_eq!(value["rateIntervalMs"], 3_600_000);
        assert_eq!(value["paymentIntervalMs"], 3_600_000);
        assert_eq!(value["paymentTimestamp"], Value::Null);
        assert_eq!(value["nextPaymentTimestamp"], Value::Null);
        for (key, expected) in [
            "oneHourPercent",
            "eightHourPercent",
            "oneDayPercent",
            "annualizedPercent",
        ]
        .into_iter()
        .zip(equivalents)
        {
            assert_eq!(
                decimal(&value["equivalents"][key]),
                expected.parse().unwrap()
            );
        }
        for field in IMPLEMENTED {
            assert_eq!(market["fields"][field]["state"], "available");
            assert_eq!(market["fields"][field]["exchangeTimestamp"], Value::Null);
            assert!(market["fields"][field]["receivedTimestamp"].is_u64());
        }
        for field in ["markPrice", "indexPrice", "lastPrice"] {
            assert_eq!(market["fields"][field]["value"]["baseAsset"], base_asset);
            assert_eq!(market["fields"][field]["value"]["quoteAsset"], quote_asset);
        }
        for field in ["lastSettledFunding", "volume24h", "openInterest"] {
            assert_eq!(market["fields"][field]["state"], "unsupported");
            assert_eq!(market["fields"][field]["reason"], "adapter-not-implemented");
            assert_eq!(market["fields"][field]["value"], Value::Null);
        }
    }
    assert_eq!(
        punctuated["fields"]["markPrice"]["value"]["amount"],
        "30000.000"
    );
    assert_eq!(
        punctuated["fields"]["indexPrice"]["value"]["amount"],
        "29999.900"
    );
    assert_eq!(
        punctuated["fields"]["lastPrice"]["value"]["amount"],
        "30001.2300"
    );
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
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("SPOT-USD")])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.calls(), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn extended_invalid_scalars_clear_independently_of_settlement_resolution() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let mut rows = Vec::new();
    for (index, invalid) in [
        json!(0),
        json!("1e-3"),
        json!(" 0.1"),
        json!("１２"),
        Value::Null,
    ]
    .into_iter()
    .enumerate()
    {
        let mut native = market(&format!("INVALID{index}-USD"), "NATIVE", "USD");
        native["marketStats"]["fundingRate"] = invalid;
        native["marketStats"]["markPrice"] = json!("1.000000000000000001");
        rows.push(native);
    }
    let mut prices = market("PRICES-USD", "PRICES", "USD");
    prices["marketStats"]["fundingRate"] = json!("-0.000000000000000001");
    prices["marketStats"]["markPrice"] = json!("0.0");
    prices["marketStats"]["indexPrice"] = json!("-1");
    prices["marketStats"]["lastPrice"] = json!("1e3");
    rows.push(prices);
    let mut missing = market("MISSING-USD", "MISSING", "USD");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("fundingRate");
    missing["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("indexPrice");
    missing["marketStats"]["lastPrice"] = json!(1);
    missing["l2Config"]
        .as_object_mut()
        .unwrap()
        .remove("collateralId");
    rows.push(missing);
    let mut unresolved = market("UNRESOLVED-USD", "UNRESOLVED", "USD");
    unresolved["l2Config"]["collateralId"] = json!(1);
    rows.push(unresolved);
    mock.markets(rows).await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(ExtendedExchange::new(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(snapshot["coverage"]["enumerationComplete"], true);
    for index in 0..5 {
        let fields = &row(&snapshot["markets"], &id(&format!("INVALID{index}-USD")))["fields"];
        assert_eq!(fields["funding"]["state"], "unavailable");
        assert_eq!(fields["funding"]["reason"], "invalid-upstream-value");
        assert_eq!(fields["funding"]["value"], Value::Null);
        assert_eq!(
            fields["markPrice"]["value"]["amount"],
            "1.000000000000000001"
        );
        assert_eq!(fields["lastPrice"]["state"], "available");
    }
    let prices = row(&snapshot["markets"], &id("PRICES-USD"));
    assert_eq!(
        prices["fields"]["funding"]["value"]["rate"],
        "-0.000000000000000001"
    );
    for (name, fields) in [
        ("PRICES-USD", vec!["markPrice", "indexPrice", "lastPrice"]),
        ("MISSING-USD", vec!["funding", "indexPrice", "lastPrice"]),
    ] {
        for field in fields {
            let value = &row(&snapshot["markets"], &id(name))["fields"][field];
            assert_eq!(value["state"], "unavailable");
            assert_eq!(value["reason"], "invalid-upstream-value");
            assert_eq!(value["value"], Value::Null);
        }
    }
    for name in ["MISSING-USD", "UNRESOLVED-USD"] {
        let native = row(&snapshot["markets"], &id(name));
        assert_eq!(native["settle"], "USD");
        assert_eq!(native["settlementAssetId"], Value::Null);
        assert_eq!(native["fields"]["markPrice"]["state"], "available");
        assert_eq!(native["fields"]["markPrice"]["value"]["quoteAsset"], "USD");
    }
    assert!(failure(&snapshot, "settlement-unresolved"));
    assert_eq!(mock.calls(), 1);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn extended_strict_topics_and_catalog_failure_keep_the_original_retry_deadline() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    mock.reply.write().await.status = StatusCode::BAD_GATEWAY;
    let source = Arc::new(ExtendedExchange::new(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source.clone()).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    let mut invalid = Vec::new();
    for params in [
        json!({"category":"perp"}),
        json!({"dex":""}),
        json!({"symbol":"BTC-USD"}),
        json!({"coin":"BTC"}),
        json!([]),
        json!(""),
        json!(false),
    ] {
        let mut body = request(None);
        body["params"] = params;
        invalid.push(body);
    }
    for market_id in [
        "not-json".to_string(),
        "[ \"extended\",\"perp\",null,null,\"BTC-USD\"]".to_string(),
        json!(["binance", "perp", null, null, "BTC-USD"]).to_string(),
        json!(["extended", "spot", null, null, "BTC-USD"]).to_string(),
        json!(["extended", "future", null, null, "BTC-USD"]).to_string(),
        json!(["extended", "perp", "perp", null, "BTC-USD"]).to_string(),
        json!(["extended", "perp", null, "", "BTC-USD"]).to_string(),
        json!(["extended", "perp", null, null, 1]).to_string(),
        id(""),
    ] {
        invalid.push(request(Some(vec![market_id])));
    }
    invalid.push(request(Some(vec![])));
    invalid.push(request(Some(
        (0..101).map(|index| id(&format!("M{index}-USD"))).collect(),
    )));
    for body in invalid {
        super::stats_http(&client, &base, body.clone(), StatusCode::BAD_REQUEST).await;
        super::ws_error(
            &mut socket,
            super::stats_command("subscribe", &body),
            "INVALID_TOPIC",
        )
        .await;
    }
    assert_eq!(mock.calls(), 0);
    // Catalog demand receives the failure at t=0, before a coordinator consumer exists.
    assert!(source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true
        })
        .await
        .is_err());
    assert_eq!(mock.calls(), 1);
    tokio::time::advance(Duration::from_secs(29)).await;
    let cold = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert!(failure(&cold, "upstream-failure"));
    let selected = request(Some(vec![id("BTC-USD")]));
    super::stats_http(&client, &base, selected.clone(), StatusCode::BAD_GATEWAY).await;
    super::ws_error(
        &mut socket,
        super::stats_command("subscribe", &selected),
        "SUBSCRIBE_FAILED",
    )
    .await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    view.assert_matches_snapshot(&cold);
    assert_eq!(mock.calls(), 1);
    mock.markets(markets()).await;
    tokio::time::advance(Duration::from_millis(1_001)).await;
    // A consumer at t=29 must not move the failed receipt's next poll to t=59.
    super::ws_delta(&mut socket, &mut view).await;
    assert_eq!(view.coverage["enumerationComplete"], true);
    assert_eq!(
        view.markets[&id("BTC-USD")]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(mock.calls(), 2);
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("UNKNOWN-USD")])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    super::assert_ws_matches_http(&client, &base, &view).await;
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn extended_shared_bulk_wire_reduces_row_failures_clears_recovery_and_removals() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(ExtendedExchange::new(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let btc = id("BTC-USD");
    let alt = id("ALT-USD");
    let all_request = request(None);
    let selected_request = request(Some(vec![btc.clone()]));
    let mut all_socket = super::stats_socket(&base).await;
    let mut selected_socket = super::stats_socket(&base).await;
    let gate = Arc::new(Semaphore::new(0));
    mock.reply.write().await.barrier = Some(gate.clone());
    let mut requests = Vec::new();
    for index in 0..6 {
        let (client, base) = (client.clone(), base.clone());
        let mut body = if index % 2 == 0 {
            all_request.clone()
        } else {
            selected_request.clone()
        };
        body["params"] = if index % 2 == 0 {
            Value::Null
        } else {
            json!({})
        };
        requests.push(tokio::spawn(async move {
            super::stats_http(&client, &base, body, StatusCode::OK).await
        }));
    }
    let (catalog_client, catalog_base) = (client.clone(), base.clone());
    let catalog =
        tokio::spawn(async move { catalog_http(&catalog_client, &catalog_base, true).await });
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
    super::wait_count(&mock.count, 1).await;
    mock.reply.write().await.barrier = None;
    // Release enough permits to fail by call count, rather than hang, if single-flight regresses.
    gate.add_permits(10);
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    assert_eq!(all.topic["params"], json!({}));
    assert_eq!(selected.topic["params"], json!({}));
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(
        row(&catalog.await.unwrap()["markets"], &btc)["settlementAssetId"],
        "0x1"
    );
    for (index, request) in requests.into_iter().enumerate() {
        let snapshot = request.await.unwrap();
        if index % 2 == 0 {
            all.assert_matches_snapshot(&snapshot);
        } else {
            selected.assert_matches_snapshot(&snapshot);
        }
    }
    assert_eq!(mock.calls(), 1);
    let baseline = selected.markets[&btc].clone();
    // Missing/nonobject contexts retain only that row; valid siblings remain fresh.
    for nonobject in [None, Some(Value::Null)] {
        let mut rows = markets();
        rows[0].as_object_mut().unwrap().remove("marketStats");
        if let Some(value) = nonobject {
            rows[0]["marketStats"] = value;
        }
        rows[1]["marketStats"]["lastPrice"] = json!("4.2500");
        mock.markets(rows).await;
        super::after_receipt(&all.markets[&alt]["fields"]["funding"]["receivedTimestamp"]).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        let delta = super::ws_delta(&mut all_socket, &mut all).await;
        // The second failure need not change the already-stale selected projection.
        if selected.markets[&btc]["fields"]["funding"]["state"] == "available" {
            super::ws_delta(&mut selected_socket, &mut selected).await;
        }
        assert_eq!(delta["removedMarketIds"], json!([]));
        assert_eq!(all.coverage["enumerationComplete"], true);
        for field in IMPLEMENTED {
            let retained = &all.markets[&btc]["fields"][field];
            assert_eq!(retained["state"], "stale");
            assert_eq!(retained["reason"], "context-mismatch");
            assert_eq!(retained["value"], baseline["fields"][field]["value"]);
            assert_eq!(
                retained["receivedTimestamp"],
                baseline["fields"][field]["receivedTimestamp"]
            );
            assert_eq!(all.markets[&alt]["fields"][field]["state"], "available");
        }
        assert_eq!(
            all.markets[&alt]["fields"]["lastPrice"]["value"]["amount"],
            "4.2500"
        );
        super::assert_ws_matches_http(&client, &base, &all).await;
        super::assert_ws_matches_http(&client, &base, &selected).await;
    }
    let mut invalid = markets();
    invalid[0]["marketStats"]["fundingRate"] = json!(12);
    invalid[0]["marketStats"]
        .as_object_mut()
        .unwrap()
        .remove("lastPrice");
    invalid[0]["marketStats"]["markPrice"] = json!("31000.000");
    mock.markets(invalid).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let cleared = selected.markets[&btc].clone();
    for field in ["funding", "lastPrice"] {
        assert_eq!(cleared["fields"][field]["state"], "unavailable");
        assert_eq!(cleared["fields"][field]["reason"], "invalid-upstream-value");
        assert_eq!(cleared["fields"][field]["value"], Value::Null);
    }
    assert_eq!(
        cleared["fields"]["markPrice"]["value"]["amount"],
        "31000.000"
    );
    mock.reply.write().await.status = StatusCode::BAD_GATEWAY;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], false);
    for field in ["funding", "lastPrice"] {
        assert_eq!(
            selected.markets[&btc]["fields"][field],
            cleared["fields"][field]
        );
    }
    for field in ["markPrice", "indexPrice"] {
        let retained = &selected.markets[&btc]["fields"][field];
        assert_eq!(retained["state"], "stale");
        assert_eq!(retained["value"], cleared["fields"][field]["value"]);
        assert_eq!(
            retained["receivedTimestamp"],
            cleared["fields"][field]["receivedTimestamp"]
        );
    }
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;
    let mut recovered = markets();
    recovered[0]["marketStats"]["lastPrice"] = json!("32000.1200");
    mock.markets(recovered.clone()).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(all.coverage["enumerationComplete"], true);
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["lastPrice"]["value"]["amount"],
        "32000.1200"
    );
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;
    recovered[0]["status"] = json!("REDUCE_ONLY");
    mock.markets(recovered.clone()).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let inactive = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(inactive["removedMarketIds"], json!([btc]));
    for field in IMPLEMENTED {
        assert_eq!(
            selected.markets[&btc]["fields"][field]["reason"],
            "inactive-market"
        );
    }
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;
    recovered.remove(0);
    mock.markets(recovered).await;
    super::after_receipt(&all.markets[&alt]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let removed = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));
    assert!(selected.markets.is_empty());
    super::stats_http(&client, &base, selected_request, StatusCode::BAD_REQUEST).await;
    super::assert_ws_matches_http(&client, &base, &all).await;
    assert_eq!(mock.calls(), 8);
    super::ws_unsubscribe(&mut selected_socket, &selected.topic).await;
    super::ws_unsubscribe(&mut all_socket, &all.topic).await;
    super::ws_ping(&mut selected_socket).await;
    super::ws_disconnect(selected_socket).await;
    super::ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn extended_incomplete_catalogs_never_delete_or_replace_normalized_membership() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(ExtendedExchange::new(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let mut conflicting = market("BTC-USD", "WRONG", "OTHER");
    conflicting["marketStats"]["fundingRate"] = json!("99");
    let mut other_product = market("ALT-USD", "ALT", "USD");
    other_product["type"] = json!("FUTURE");
    for invalid in [
        json!({"status":"OK", "data":{}}),
        envelope(vec![]),
        envelope(vec![markets()[0].clone(), conflicting]),
        envelope(vec![markets()[0].clone(), other_product]),
    ] {
        let previous = view.markets.clone();
        *mock.reply.write().await = super::Reply::ok(invalid);
        tokio::time::advance(Duration::from_millis(30_001)).await;
        let delta = super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(delta["removedMarketIds"], json!([]));
        assert_eq!(view.coverage["enumerationComplete"], false);
        assert_eq!(view.coverage["expectedMarkets"], 2);
        assert_eq!(
            view.markets.keys().collect::<Vec<_>>(),
            previous.keys().collect::<Vec<_>>()
        );
        for (market_id, prior) in &previous {
            assert_eq!(view.markets[market_id]["base"], prior["base"]);
            assert_eq!(view.markets[market_id]["settle"], prior["settle"]);
            for field in IMPLEMENTED {
                let retained = &view.markets[market_id]["fields"][field];
                assert_eq!(retained["state"], "stale");
                assert_eq!(retained["value"], prior["fields"][field]["value"]);
                assert_eq!(
                    retained["receivedTimestamp"],
                    prior["fields"][field]["receivedTimestamp"]
                );
            }
        }
        let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
        view.assert_matches_snapshot(&snapshot);
        assert!(failure(&snapshot, "invalid-upstream-data"));
        let calls = mock.calls();
        super::stats_http(
            &client,
            &base,
            request(Some(vec![id("UNKNOWN-USD")])),
            StatusCode::BAD_GATEWAY,
        )
        .await;
        assert_eq!(mock.calls(), calls);
        mock.markets(markets()).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(view.coverage["enumerationComplete"], true);
        for market in view.markets.values() {
            for field in IMPLEMENTED {
                assert_eq!(market["fields"][field]["state"], "available");
            }
        }
    }
    assert_eq!(mock.calls(), 9);
    super::assert_ws_matches_http(&client, &base, &view).await;
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn extended_pending_bulk_refresh_expires_last_price_at_90_seconds_without_blocking_readers() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(ExtendedExchange::new(upstream, 120_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let baseline = view.markets.clone();
    let gate = Arc::new(Semaphore::new(0));
    mock.reply.write().await.barrier = Some(gate.clone());
    tokio::time::advance(Duration::from_secs(30)).await;
    super::wait_count(&mock.count, 2).await;
    tokio::time::advance(Duration::from_secs(59)).await;
    let wall_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let before = tokio::select! {
        snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK) => snapshot,
        _ = async {
            while std::time::Instant::now() < wall_deadline {
                tokio::task::yield_now().await;
            }
        } => panic!("cached REST reader blocked behind the pending bulk request"),
    };
    view.assert_matches_snapshot(&before);
    for market in before["markets"].as_array().unwrap() {
        for field in IMPLEMENTED {
            assert_eq!(market["fields"][field]["state"], "available");
        }
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    super::ws_delta(&mut socket, &mut view).await;
    for (market_id, original) in &baseline {
        for field in IMPLEMENTED {
            let stale = &view.markets[market_id]["fields"][field];
            assert_eq!(stale["state"], "stale");
            assert_eq!(stale["reason"], "stale-threshold");
            assert_eq!(stale["value"], original["fields"][field]["value"]);
            assert_eq!(
                stale["receivedTimestamp"],
                original["fields"][field]["receivedTimestamp"]
            );
        }
    }
    assert_eq!(mock.calls(), 2);
    super::assert_ws_matches_http(&client, &base, &view).await;
    super::after_receipt(&baseline[&id("BTC-USD")]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    mock.reply.write().await.barrier = None;
    gate.add_permits(1);
    super::ws_delta(&mut socket, &mut view).await;
    for (market_id, original) in &baseline {
        for field in IMPLEMENTED {
            let fresh = &view.markets[market_id]["fields"][field];
            assert_eq!(fresh["state"], "available");
            assert_eq!(fresh["value"], original["fields"][field]["value"]);
            assert_ne!(
                fresh["receivedTimestamp"],
                original["fields"][field]["receivedTimestamp"]
            );
        }
    }
    // Completion at t=91, not request start at t=30, sets the next bulk deadline.
    tokio::time::advance(Duration::from_secs(29)).await;
    super::assert_ws_matches_http(&client, &base, &view).await;
    assert_eq!(mock.calls(), 2);
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}
