use axum::{
    extract::State,
    http::{Request, StatusCode},
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::{
        aster::AsterExchange,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::{FetchMarketStatsParams, FetchMarketsParams},
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
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};

const IMPLEMENTED: [&str; 3] = ["funding", "markPrice", "indexPrice"];
const FIELDS: [&str; 7] = [
    "funding",
    "markPrice",
    "indexPrice",
    "lastPrice",
    "lastSettledFunding",
    "volume24h",
    "openInterest",
];
const SOURCES: [&str; 3] = [
    "aster:exchangeInfo",
    "aster:premiumIndex",
    "aster:fundingInfo",
];

#[derive(Clone)]
struct AsterMock {
    replies: Arc<[RwLock<super::Reply>; 3]>,
    counts: Arc<[AtomicUsize; 3]>,
}

impl AsterMock {
    async fn reply(&self, endpoint: usize, status: StatusCode, body: Value) {
        *self.replies[endpoint].write().await = super::Reply {
            status,
            body,
            barrier: None,
        };
    }

    async fn catalog(&self, rows: Vec<Value>) {
        self.reply(0, StatusCode::OK, json!({"symbols":rows})).await;
    }

    fn calls(&self) -> [usize; 3] {
        self.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst))
    }
}

fn instrument(symbol: &str, base: &str, quote: &str, margin: &str) -> Value {
    json!({
        "symbol":symbol, "contractType":"PERPETUAL", "status":"TRADING",
        "baseAsset":base, "quoteAsset":quote, "marginAsset":margin
    })
}

fn instruments() -> Vec<Value> {
    vec![
        instrument("BTCUSDT", "BTC", "USDT", "USDT"),
        instrument("ALTUSD1", "ALT", "USD1", "USD1"),
    ]
}

fn premium(symbol: &str, rate: &str, mark: &str, index: &str) -> Value {
    json!({
        "symbol":symbol, "lastFundingRate":rate, "markPrice":mark, "indexPrice":index,
        "time":1700000000000u64, "nextFundingTime":1700028800000u64,
        "interestRate":"0.0001"
    })
}

fn premiums() -> Value {
    json!([
        premium("ALTUSD1", "0", "1.2500", "1.200"),
        premium("BTCUSDT", "-0.00001250", "30000.000", "29999.900")
    ])
}

fn intervals() -> Value {
    json!([
        {"symbol":"BTCUSDT", "fundingIntervalHours":8},
        {"symbol":"ALTUSD1", "fundingIntervalHours":1}
    ])
}

async fn handler(
    State(mock): State<AsterMock>,
    request: Request<axum::body::Body>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(request.method(), "GET");
    assert!(
        request.uri().query().is_none(),
        "bulk acquisition must not filter: {}",
        request.uri()
    );
    let endpoint = match request.uri().path() {
        "/fapi/v3/exchangeInfo" => 0,
        "/fapi/v3/premiumIndex" => 1,
        "/fapi/v3/fundingInfo" => 2,
        other => panic!("unexpected Aster route {other}"),
    };
    let reply = mock.replies[endpoint].read().await.clone();
    mock.counts[endpoint].fetch_add(1, Ordering::SeqCst);
    if let Some(gate) = reply.barrier {
        gate.acquire().await.unwrap().forget();
    }
    (reply.status, Json(reply.body))
}

async fn server() -> (AsterMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = AsterMock {
        replies: Arc::new([
            RwLock::new(super::Reply::ok(json!({"symbols":instruments()}))),
            RwLock::new(super::Reply::ok(premiums())),
            RwLock::new(super::Reply::ok(intervals())),
        ]),
        counts: Arc::new(Default::default()),
    };
    let (base, stop, task) =
        super::spawn_server(Router::new().fallback(handler).with_state(mock.clone())).await;
    (mock, base, stop, task)
}

fn id(symbol: &str) -> String {
    json!(["aster", "perp", null, null, symbol]).to_string()
}

fn request(ids: Option<Vec<String>>) -> Value {
    json!({"exchange":"aster", "params":{}, "marketIds":ids, "fields":FIELDS})
}

fn params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: Value::Null,
    }
}

fn row<'a>(rows: &'a Value, market_id: &str) -> &'a Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == market_id)
        .unwrap()
}

fn failure(snapshot: &Value, endpoint: usize, reason: &str) -> bool {
    snapshot["coverage"]["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["source"] == SOURCES[endpoint] && failure["reason"] == reason)
}

async fn catalog_http(client: &reqwest::Client, base: &str, inactive: bool) -> Value {
    let response = client
        .post(format!("{base}/v1/fetchMarkets"))
        .json(&json!({"exchange":"aster", "params":{}, "includeInactive":inactive}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn aster_native_identity_product_selection_and_denominations_are_independent() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let native = [
        (
            "B-MONEYUSDT",
            "B-MONEY",
            "USDT",
            "USDT",
            "-0.00001250",
            "1.000000000000000001",
        ),
        ("BMONEYUSDT", "BMONEY", "USDT", "USD1", "0", "2.000"),
        (
            "哈基米USD1",
            "哈基米",
            "USD1",
            "USD1",
            "0.00010000",
            "3.2500",
        ),
        ("BTCU", "BTC", "U", "USDT", "-0.000000000000000001", "4.50"),
    ];
    let mut catalog = Vec::new();
    let mut marks = Vec::new();
    let mut configs = Vec::new();
    for (symbol, base, quote, margin, rate, mark) in native {
        catalog.push(instrument(symbol, base, quote, margin));
        marks.push(premium(symbol, rate, mark, "0.9900"));
        configs.push(json!({"symbol":symbol, "fundingIntervalHours":4}));
    }
    let mut unresolved = instrument("UNKNOWNMARGIN", "RAW", "U", "U");
    unresolved.as_object_mut().unwrap().remove("marginAsset");
    catalog.push(unresolved);
    let mut invalid_margin = instrument("INVALIDMARGIN", "RAW", "USD1", "USD1");
    invalid_margin["marginAsset"] = json!(1);
    catalog.push(invalid_margin);
    for symbol in ["UNKNOWNMARGIN", "INVALIDMARGIN"] {
        marks.push(premium(symbol, "0.01", "5.00", "5.01"));
        configs.push(json!({"symbol":symbol, "fundingIntervalHours":1}));
    }
    let mut old = instrument("OLDUSDT", "OLD", "USDT", "USDT");
    old["status"] = json!("SETTLING");
    catalog.push(old);
    for (symbol, contract) in [
        ("FUTUSDT", "CURRENT_QUARTER"),
        ("PENDINGUSDT", ""),
        ("LOWERUSDT", "perpetual"),
    ] {
        let mut other = instrument(symbol, symbol, "USDT", "USDT");
        other["contractType"] = json!(contract);
        catalog.push(other);
        marks.push(premium(symbol, "99", "99", "99"));
    }
    marks.reverse();
    mock.catalog(catalog).await;
    mock.reply(1, StatusCode::OK, json!(marks)).await;
    mock.reply(2, StatusCode::OK, json!(configs)).await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(AsterExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let catalog = catalog_http(&client, &base, true).await;
    let active_catalog = catalog_http(&client, &base, false).await;
    assert!(!active_catalog["markets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|market| market["exchangeMarketId"] == "OLDUSDT"));
    let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(snapshot["coverage"]["expectedMarkets"], 6);
    assert_eq!(snapshot["markets"].as_array().unwrap().len(), 6);
    assert_eq!(
        row(&snapshot["markets"], &id("B-MONEYUSDT"))["symbol"],
        row(&snapshot["markets"], &id("BMONEYUSDT"))["symbol"]
    );
    for (symbol, base_asset, quote, margin, rate, mark) in native {
        let market = row(&snapshot["markets"], &id(symbol));
        assert_eq!(market["exchangeMarketId"], symbol);
        assert_eq!(market["base"], base_asset);
        assert_eq!(market["quote"], quote);
        assert_eq!(market["settle"], margin);
        assert_eq!(market["settlementAssetId"], margin);
        assert_eq!(market["category"], Value::Null);
        assert_eq!(market["dex"], Value::Null);
        let mut identity = market.clone();
        identity.as_object_mut().unwrap().remove("fields");
        assert_eq!(&identity, row(&catalog["markets"], &id(symbol)));
        let funding = &market["fields"]["funding"]["value"];
        assert_eq!(funding["rate"], rate);
        assert_eq!(funding["kind"], "estimate");
        assert_eq!(funding["rateUnit"], "decimalFraction");
        assert_eq!(funding["paymentTimestamp"], Value::Null);
        for (field, amount) in [("markPrice", mark), ("indexPrice", "0.9900")] {
            assert_eq!(market["fields"][field]["value"]["amount"], amount);
            assert_eq!(market["fields"][field]["value"]["baseAsset"], base_asset);
            assert_eq!(market["fields"][field]["value"]["quoteAsset"], quote);
        }
        for field in [
            "lastPrice",
            "lastSettledFunding",
            "volume24h",
            "openInterest",
        ] {
            assert_eq!(market["fields"][field]["state"], "unsupported");
            assert_eq!(market["fields"][field]["reason"], "adapter-not-implemented");
        }
    }
    for symbol in ["UNKNOWNMARGIN", "INVALIDMARGIN"] {
        let market = row(&snapshot["markets"], &id(symbol));
        assert_eq!(market["settle"], Value::Null);
        assert_eq!(market["settlementAssetId"], Value::Null);
        assert_eq!(market["fields"]["markPrice"]["state"], "available");
    }
    assert!(failure(&snapshot, 0, "settlement-unresolved"));
    let inactive = super::stats_http(
        &client,
        &base,
        request(Some(vec![id("OLDUSDT")])),
        StatusCode::OK,
    )
    .await;
    for field in IMPLEMENTED {
        let value = &inactive["markets"][0]["fields"][field];
        assert_eq!(value["state"], "unavailable");
        assert_eq!(value["reason"], "inactive-market");
        assert_eq!(value["value"], Value::Null);
    }
    for symbol in ["FUTUSDT", "PENDINGUSDT", "LOWERUSDT"] {
        assert!(!catalog["markets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|market| market["marketId"] == id(symbol)));
        super::stats_http(
            &client,
            &base,
            request(Some(vec![id(symbol)])),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    assert_eq!(mock.calls(), [1, 1, 1]);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn aster_strict_topics_reject_before_acquisition_and_cold_membership_retries_on_receipt() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    mock.reply(0, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    let source = Arc::new(AsterExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source.clone()).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    let mut invalid = Vec::new();
    for scope in [
        json!({"category":"perp"}),
        json!({"dex":""}),
        json!({"coin":"BTC"}),
        json!({"symbol":"BTCUSDT"}),
        json!([]),
        json!(false),
        json!(""),
    ] {
        let mut body = request(None);
        body["params"] = scope;
        invalid.push(body);
    }
    for market_id in [
        "not-json".to_string(),
        "[ \"aster\",\"perp\",null,null,\"BTCUSDT\"]".to_string(),
        json!(["binance", "perp", null, null, "BTCUSDT"]).to_string(),
        json!(["aster", "spot", null, null, "BTCUSDT"]).to_string(),
        json!(["aster", "future", null, null, "BTCUSDT"]).to_string(),
        json!(["aster", "perp", "perp", null, "BTCUSDT"]).to_string(),
        json!(["aster", "perp", null, "", "BTCUSDT"]).to_string(),
        json!(["aster", "perp", null, null, 1]).to_string(),
        id(""),
    ] {
        invalid.push(request(Some(vec![market_id])));
    }
    invalid.push(request(Some(vec![])));
    invalid.push(request(Some(
        (0..101).map(|index| id(&format!("M{index}"))).collect(),
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
    assert_eq!(mock.calls(), [0, 0, 0]);
    assert!(source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true
        })
        .await
        .is_err());
    tokio::time::advance(Duration::from_secs(29)).await;
    let cold = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert!(failure(&cold, 0, "upstream-failure"));
    let selected = request(Some(vec![id("BTCUSDT")]));
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
    assert_eq!(mock.calls()[0], 1);
    mock.catalog(instruments()).await;
    tokio::time::advance(Duration::from_millis(1_001)).await;
    super::ws_delta(&mut socket, &mut view).await;
    assert_eq!(view.coverage["enumerationComplete"], true);
    assert_eq!(
        view.markets[&id("BTCUSDT")]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(mock.calls(), [2, 1, 1]);
    let unknown = request(Some(vec![id("UNKNOWN")]));
    super::stats_http(&client, &base, unknown.clone(), StatusCode::BAD_REQUEST).await;
    super::ws_error(
        &mut socket,
        super::stats_command("subscribe", &unknown),
        "INVALID_TOPIC",
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
async fn aster_exact_scalars_native_timestamps_and_variable_schedules_never_guess_units() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    // Distinct representation and checked-arithmetic boundaries, not magnitude heuristics.
    let cases = [
        (
            "ONE",
            Some(json!(1)),
            Some(3_600_000u64),
            json!(1700000000000u64),
        ),
        (
            "FOUR",
            Some(json!(4)),
            Some(14_400_000),
            json!(1700000000000u64),
        ),
        (
            "EIGHT",
            Some(json!(8)),
            Some(28_800_000),
            json!(1700000000000u64),
        ),
        ("MISSING", None, None, Value::Null),
        ("STRING", Some(json!("8")), None, json!("1700000000000")),
        ("ZERO", Some(json!(0)), None, json!(0)),
        ("NEGATIVE", Some(json!(-1)), None, json!(-1)),
        ("FRACTION", Some(json!(1.5)), None, json!(1.5)),
        ("OVERFLOW", Some(json!(u64::MAX)), None, json!(false)),
    ];
    let mut catalog = Vec::new();
    let mut quotes = Vec::new();
    let mut config = Vec::new();
    for (symbol, hours, _, timestamp) in &cases {
        catalog.push(instrument(symbol, symbol, "USDT", "USDT"));
        let mut quote = premium(symbol, "-0.00080000", "1.000000000000000001", "1.2300");
        quote["time"] = timestamp.clone();
        quote["nextFundingTime"] = timestamp.clone();
        quotes.push(quote);
        if let Some(hours) = hours {
            config.push(json!({"symbol":symbol, "fundingIntervalHours":hours}));
        }
    }
    mock.catalog(catalog.clone()).await;
    mock.reply(1, StatusCode::OK, json!(quotes)).await;
    mock.reply(2, StatusCode::OK, json!(config)).await;
    let source = AsterExchange::with_base_url(upstream, 2_000).unwrap();
    let snapshot = source.fetch_market_stats(params()).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    for (symbol, _, interval, _) in &cases {
        let fields = &row(&rows, &id(symbol))["fields"];
        assert_eq!(fields["funding"]["state"], "available");
        let value = &fields["funding"]["value"];
        assert_eq!(value["rate"], "-0.00080000");
        assert_eq!(value["rateIntervalMs"], json!(interval), "{symbol}");
        assert_eq!(value["paymentIntervalMs"], json!(interval), "{symbol}");
        assert_eq!(value["paymentTimestamp"], Value::Null);
        let expected_time = if interval.is_some() {
            json!(1700000000000u64)
        } else {
            Value::Null
        };
        assert_eq!(value["nextPaymentTimestamp"], expected_time, "{symbol}");
        for field in IMPLEMENTED {
            assert_eq!(
                fields[field]["exchangeTimestamp"], expected_time,
                "{symbol}/{field}"
            );
        }
        if interval.is_none() {
            assert_eq!(value["equivalents"], Value::Null, "{symbol}");
        }
    }
    for (symbol, hourly) in [("ONE", "-0.08"), ("FOUR", "-0.02"), ("EIGHT", "-0.01")] {
        let actual = row(&rows, &id(symbol))["fields"]["funding"]["value"]["equivalents"]
            ["oneHourPercent"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap();
        assert_eq!(actual, hourly.parse::<rust_decimal::Decimal>().unwrap());
    }
    assert!(snapshot
        .source_failures
        .iter()
        .any(|failure| failure.source == SOURCES[2]
            && failure.reason == "funding-interval-unavailable"));

    // A successful fresh observation replaces the schedule, even if the new basis is unknown.
    mock.reply(
        2,
        StatusCode::OK,
        json!([
            {"symbol":"ONE", "fundingIntervalHours":8},
            {"symbol":"FOUR", "fundingIntervalHours":1},
            {"symbol":"EIGHT", "fundingIntervalHours":4}
        ]),
    )
    .await;
    tokio::time::advance(Duration::from_secs(30)).await;
    let changed = source.fetch_market_stats(params()).await.unwrap();
    let rows = serde_json::to_value(changed.rows).unwrap();
    for (symbol, interval) in [
        ("ONE", 28_800_000),
        ("FOUR", 3_600_000),
        ("EIGHT", 14_400_000),
    ] {
        assert_eq!(
            row(&rows, &id(symbol))["fields"]["funding"]["value"]["rateIntervalMs"],
            interval
        );
    }
    for (status, body, reason) in [
        (
            StatusCode::BAD_GATEWAY,
            json!({"offline":true}),
            "upstream-failure",
        ),
        (
            StatusCode::OK,
            json!([{"symbol":"ONE", "fundingIntervalHours":1}, {"symbol":"ONE", "fundingIntervalHours":8}]),
            "invalid-upstream-data",
        ),
    ] {
        mock.reply(2, status, body).await;
        tokio::time::advance(Duration::from_secs(30)).await;
        let failed = source.fetch_market_stats(params()).await.unwrap();
        assert!(failed
            .source_failures
            .iter()
            .any(|failure| failure.source == SOURCES[2] && failure.reason == reason));
        let rows = serde_json::to_value(failed.rows).unwrap();
        for market in rows.as_array().unwrap() {
            let funding = &market["fields"]["funding"];
            assert_eq!(funding["state"], "available");
            assert_eq!(funding["value"]["rate"], "-0.00080000");
            assert_eq!(funding["value"]["rateIntervalMs"], Value::Null);
            assert_eq!(funding["value"]["paymentIntervalMs"], Value::Null);
            assert_eq!(funding["value"]["equivalents"], Value::Null);
            assert_eq!(
                market["fields"]["markPrice"]["value"]["amount"],
                "1.000000000000000001"
            );
        }
        let calls = mock.calls();
        source.fetch_market_stats(params()).await.unwrap();
        assert_eq!(
            mock.calls(),
            calls,
            "failed configurations share the same success/failure TTL"
        );
    }

    // Lexical invalidity clears exactly the present scalar, not its valid siblings.
    let bad_rates = [
        json!(0),
        json!("1e-3"),
        json!(" 0.1"),
        json!("１２"),
        Value::Null,
    ];
    let mut quotes = Vec::new();
    for ((symbol, _, _, _), invalid) in cases.iter().zip(bad_rates) {
        let mut quote = premium(symbol, "0", "1.000000000000000001", "1.2500");
        quote["lastFundingRate"] = invalid;
        quotes.push(quote);
    }
    let mut zero = premium("ZERO", "0.0000", "0.0", "-1");
    zero["nextFundingTime"] = json!(0);
    quotes.push(zero);
    let mut negative = premium("NEGATIVE", "-0.000000000000000001", "1e3", "1");
    negative["indexPrice"] = json!(1);
    quotes.push(negative);
    let mut missing = premium("FRACTION", "0.1", "1.25", "1.2");
    missing.as_object_mut().unwrap().remove("lastFundingRate");
    missing.as_object_mut().unwrap().remove("indexPrice");
    quotes.push(missing);
    mock.reply(1, StatusCode::OK, json!(quotes)).await;
    mock.reply(2, StatusCode::OK, json!([])).await;
    tokio::time::advance(Duration::from_secs(30)).await;
    let cleared = source.fetch_market_stats(params()).await.unwrap();
    let rows = serde_json::to_value(cleared.rows).unwrap();
    for (symbol, _, _, _) in &cases[..5] {
        let fields = &row(&rows, &id(symbol))["fields"];
        assert_eq!(fields["funding"]["state"], "unavailable");
        assert_eq!(fields["funding"]["reason"], "invalid-upstream-value");
        assert_eq!(fields["funding"]["value"], Value::Null);
        assert_eq!(
            fields["markPrice"]["value"]["amount"],
            "1.000000000000000001"
        );
        assert_eq!(fields["indexPrice"]["state"], "available");
    }
    for (symbol, rate) in [("ZERO", "0.0000"), ("NEGATIVE", "-0.000000000000000001")] {
        let fields = &row(&rows, &id(symbol))["fields"];
        assert_eq!(fields["funding"]["value"]["rate"], rate);
        for field in ["markPrice", "indexPrice"] {
            assert_eq!(fields[field]["value"], Value::Null);
            assert_eq!(fields[field]["reason"], "invalid-upstream-value");
        }
    }
    let fields = &row(&rows, &id("FRACTION"))["fields"];
    for field in ["funding", "indexPrice"] {
        assert_eq!(fields[field]["value"], Value::Null);
        assert_eq!(fields[field]["reason"], "invalid-upstream-value");
    }
    assert_eq!(fields["markPrice"]["state"], "available");
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn aster_shared_bulk_wire_preserves_sparse_rows_clears_and_recovery() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(AsterExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let btc = id("BTCUSDT");
    let alt = id("ALTUSD1");
    let all_request = request(None);
    let selected_request = request(Some(vec![btc.clone()]));
    let mut all_socket = super::stats_socket(&base).await;
    let mut selected_socket = super::stats_socket(&base).await;
    let gate = Arc::new(Semaphore::new(0));
    for reply in mock.replies.iter() {
        reply.write().await.barrier = Some(gate.clone());
    }
    let mut pending = Vec::new();
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
        pending.push(tokio::spawn(async move {
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
    super::wait_count(&mock.counts[0], 1).await;
    for reply in mock.replies.iter() {
        reply.write().await.barrier = None;
    }
    // Spare permits make duplicate acquisition fail the counter assertion rather than hang.
    gate.add_permits(30);
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    assert_eq!(all.topic["params"], json!({}));
    assert_eq!(selected.topic["params"], json!({}));
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(
        row(&catalog.await.unwrap()["markets"], &btc)["settlementAssetId"],
        "USDT"
    );
    for (index, pending) in pending.into_iter().enumerate() {
        let snapshot = pending.await.unwrap();
        if index % 2 == 0 {
            all.assert_matches_snapshot(&snapshot);
        } else {
            selected.assert_matches_snapshot(&snapshot);
        }
    }
    assert_eq!(mock.calls(), [1, 1, 1]);
    let original = selected.markets[&btc].clone();

    // Native identity joins, not array position; a missing row is stale, not an explicit clear.
    mock.reply(
        1,
        StatusCode::OK,
        json!([premium("ALTUSD1", "0.0002", "4.2500", "4.20")]),
    )
    .await;
    mock.reply(
        2,
        StatusCode::OK,
        json!([
            {"symbol":"BTCUSDT", "fundingIntervalHours":1},
            {"symbol":"ALTUSD1", "fundingIntervalHours":4}
        ]),
    )
    .await;
    super::after_receipt(&original["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let sparse = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(sparse["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], true);
    for field in IMPLEMENTED {
        let retained = &selected.markets[&btc]["fields"][field];
        assert_eq!(retained["state"], "stale");
        assert_eq!(retained["reason"], "missing-upstream-row");
        assert_eq!(retained["value"], original["fields"][field]["value"]);
        assert_eq!(
            retained["receivedTimestamp"],
            original["fields"][field]["receivedTimestamp"]
        );
        assert_eq!(all.markets[&alt]["fields"][field]["state"], "available");
    }
    assert_eq!(
        all.markets[&alt]["fields"]["markPrice"]["value"]["amount"],
        "4.2500"
    );
    assert_eq!(
        all.markets[&alt]["fields"]["funding"]["value"]["rateIntervalMs"],
        14_400_000
    );
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;

    let mut clears = premium("BTCUSDT", "0.01", "31000.000", "30999");
    clears["lastFundingRate"] = json!(12);
    clears.as_object_mut().unwrap().remove("indexPrice");
    mock.reply(1, StatusCode::OK, json!([clears, premiums()[0].clone()]))
        .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let cleared = selected.markets[&btc].clone();
    for field in ["funding", "indexPrice"] {
        assert_eq!(cleared["fields"][field]["state"], "unavailable");
        assert_eq!(cleared["fields"][field]["reason"], "invalid-upstream-value");
        assert_eq!(cleared["fields"][field]["value"], Value::Null);
    }
    assert_eq!(
        cleared["fields"]["markPrice"]["value"]["amount"],
        "31000.000"
    );

    mock.reply(1, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    mock.reply(2, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], true);
    for field in ["funding", "indexPrice"] {
        assert_eq!(
            selected.markets[&btc]["fields"][field],
            cleared["fields"][field]
        );
    }
    let retained = &selected.markets[&btc]["fields"]["markPrice"];
    assert_eq!(retained["state"], "stale");
    assert_eq!(retained["value"], cleared["fields"]["markPrice"]["value"]);
    assert_eq!(
        retained["receivedTimestamp"],
        cleared["fields"]["markPrice"]["receivedTimestamp"]
    );
    let snapshot = super::stats_http(&client, &base, all_request.clone(), StatusCode::OK).await;
    assert!(failure(&snapshot, 1, "upstream-failure"));
    assert!(failure(&snapshot, 2, "upstream-failure"));
    all.assert_matches_snapshot(&snapshot);
    assert_eq!(mock.calls(), [4, 4, 4]);

    // Config failure must not hide the next fresh valid native rate or either price.
    mock.reply(1, StatusCode::OK, premiums()).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let funding = &selected.markets[&btc]["fields"]["funding"];
    assert_eq!(funding["state"], "available");
    assert_eq!(funding["value"]["rate"], "-0.00001250");
    assert_eq!(funding["value"]["rateIntervalMs"], Value::Null);
    assert_eq!(funding["value"]["equivalents"], Value::Null);
    for field in ["markPrice", "indexPrice"] {
        assert_eq!(
            selected.markets[&btc]["fields"][field]["state"],
            "available"
        );
    }
    mock.reply(2, StatusCode::OK, intervals()).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["value"]["rateIntervalMs"],
        28_800_000
    );
    assert_eq!(all.coverage["sourceFailures"], json!([]));

    let mut remaining = instruments();
    remaining[0]["status"] = json!("SETTLING");
    mock.catalog(remaining.clone()).await;
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
    remaining.remove(0);
    mock.catalog(remaining).await;
    super::after_receipt(&all.markets[&alt]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let removed = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));
    assert!(selected.markets.is_empty());
    super::stats_http(&client, &base, selected_request, StatusCode::BAD_REQUEST).await;
    super::assert_ws_matches_http(&client, &base, &all).await;
    assert_eq!(mock.calls(), [8, 8, 8]);
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
async fn aster_invalid_catalogs_and_mark_batches_retain_only_coordinator_observations() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(AsterExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let mut conflicting = instruments()[0].clone();
    conflicting["baseAsset"] = json!("WRONG");
    conflicting["marginAsset"] = json!("OTHER");
    let mut missing_metadata = instruments()[0].clone();
    missing_metadata
        .as_object_mut()
        .unwrap()
        .remove("baseAsset");
    let mut missing_status = instruments()[0].clone();
    missing_status.as_object_mut().unwrap().remove("status");
    let mut missing_contract = instruments()[0].clone();
    missing_contract
        .as_object_mut()
        .unwrap()
        .remove("contractType");
    let mut empty_symbol = instruments()[0].clone();
    empty_symbol["symbol"] = json!("");
    let mut duplicate_mark = premiums()[1].clone();
    duplicate_mark["lastFundingRate"] = json!("99");
    let cases = [
        (0, json!({"symbols":{}})),
        (0, json!({"symbols":[]})),
        (
            0,
            json!({"symbols":[instruments()[0].clone(), conflicting]}),
        ),
        (0, json!({"symbols":[missing_metadata]})),
        (0, json!({"symbols":[missing_status]})),
        (0, json!({"symbols":[missing_contract]})),
        (0, json!({"symbols":[empty_symbol]})),
        (1, json!({"symbol":"BTCUSDT", "markPrice":"99"})),
        (1, json!([premiums()[1].clone(), duplicate_mark])),
        (1, json!([premiums()[1].clone(), null])),
        (1, json!([premiums()[1].clone(), {"markPrice":"99"}])),
    ];
    for (endpoint, invalid) in cases {
        let previous = view.markets.clone();
        mock.reply(endpoint, StatusCode::OK, invalid).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        let delta = super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(delta["removedMarketIds"], json!([]));
        assert_eq!(view.coverage["enumerationComplete"], endpoint != 0);
        assert_eq!(view.coverage["expectedMarkets"], 2);
        assert_eq!(
            view.markets.keys().collect::<Vec<_>>(),
            previous.keys().collect::<Vec<_>>()
        );
        for (market_id, original) in &previous {
            assert_eq!(view.markets[market_id]["base"], original["base"]);
            assert_eq!(view.markets[market_id]["settle"], original["settle"]);
            for field in IMPLEMENTED {
                let retained = &view.markets[market_id]["fields"][field];
                assert_eq!(retained["state"], "stale");
                assert_eq!(retained["value"], original["fields"][field]["value"]);
                assert_eq!(
                    retained["receivedTimestamp"],
                    original["fields"][field]["receivedTimestamp"]
                );
            }
        }
        let snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
        assert!(failure(&snapshot, endpoint, "invalid-upstream-data"));
        view.assert_matches_snapshot(&snapshot);
        let counts = mock.calls();
        super::stats_http(
            &client,
            &base,
            request(Some(vec![id("UNKNOWN")])),
            if endpoint == 0 {
                StatusCode::BAD_GATEWAY
            } else {
                StatusCode::BAD_REQUEST
            },
        )
        .await;
        assert_eq!(
            mock.calls(),
            counts,
            "invalid batches must also cache their failures"
        );
        mock.catalog(instruments()).await;
        mock.reply(1, StatusCode::OK, premiums()).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(view.coverage["enumerationComplete"], true);
        for market in view.markets.values() {
            for field in IMPLEMENTED {
                assert_eq!(market["fields"][field]["state"], "available");
            }
        }
    }
    // Only a later complete catalog may actually remove identities.
    mock.catalog(vec![instruments()[0].clone()]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let removed = super::ws_delta(&mut socket, &mut view).await;
    assert_eq!(removed["removedMarketIds"], json!([id("ALTUSD1")]));
    assert_eq!(view.coverage["expectedMarkets"], 1);
    super::assert_ws_matches_http(&client, &base, &view).await;
    let mut pending = instrument("PENDINGUSDT", "PENDING", "USDT", "USDT");
    pending["contractType"] = json!("");
    mock.catalog(vec![pending]).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let empty = super::ws_delta(&mut socket, &mut view).await;
    assert_eq!(empty["removedMarketIds"], json!([id("BTCUSDT")]));
    assert_eq!(view.coverage["enumerationComplete"], true);
    assert_eq!(view.coverage["expectedMarkets"], 0);
    assert!(view.markets.is_empty());
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
async fn aster_catalog_delay_cannot_retimestamp_premium_or_move_its_failure_deadline() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    let source = Arc::new(AsterExchange::with_base_url(upstream, 120_000).unwrap());
    source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true,
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(29)).await;
    let original = source.fetch_market_stats(params()).await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    mock.replies[0].write().await.barrier = Some(gate.clone());
    tokio::time::advance(Duration::from_secs(1)).await;
    let refreshing_source = source.clone();
    let refreshing = tokio::spawn(async move {
        refreshing_source
            .fetch_market_stats(params())
            .await
            .unwrap()
    });
    super::wait_count(&mock.counts[0], 2).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    mock.replies[0].write().await.barrier = None;
    gate.add_permits(1);
    let delayed = refreshing.await.unwrap();
    assert_eq!(delayed.rows, original.rows);
    assert_eq!(delayed.received_at, original.received_at);
    assert_eq!(delayed.field_received_at, original.field_received_at);
    assert_eq!(mock.calls(), [2, 1, 1]);
    mock.reply(1, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    mock.reply(2, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    tokio::time::advance(Duration::from_secs(19)).await;
    let failed = source.fetch_market_stats(params()).await.unwrap();
    for endpoint in [1, 2] {
        assert!(failed
            .source_failures
            .iter()
            .any(|failure| failure.source == SOURCES[endpoint]
                && failure.reason == "upstream-failure"));
    }
    assert_eq!(mock.calls(), [2, 2, 2]);
    // The catalog's t=70 completion must not renew either t=59 failure receipt.
    tokio::time::advance(Duration::from_secs(11)).await;
    source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true,
        })
        .await
        .unwrap();
    source.fetch_market_stats(params()).await.unwrap();
    assert_eq!(mock.calls(), [3, 2, 2]);
    mock.reply(1, StatusCode::OK, premiums()).await;
    mock.reply(2, StatusCode::OK, intervals()).await;
    tokio::time::advance(Duration::from_secs(18)).await;
    source.fetch_market_stats(params()).await.unwrap();
    assert_eq!(mock.calls(), [3, 2, 2]);
    tokio::time::advance(Duration::from_secs(1)).await;
    let recovered = source.fetch_market_stats(params()).await.unwrap();
    assert!(recovered.source_failures.is_empty());
    assert!(recovered.received_at > original.received_at);
    assert_eq!(mock.calls(), [3, 3, 3]);
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn aster_payment_boundary_and_stale_expiry_remain_live_during_pending_premium() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let mut initial = premiums();
    initial[1]["nextFundingTime"] = json!(1700000035000u64);
    mock.reply(1, StatusCode::OK, initial).await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(AsterExchange::with_base_url(upstream, 120_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request(None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let original = view.markets.clone();
    let btc = id("BTCUSDT");
    let gate = Arc::new(Semaphore::new(0));
    // Capture the recovery body when the blocked request starts, not when its gate opens.
    mock.reply(1, StatusCode::OK, premiums()).await;
    mock.replies[1].write().await.barrier = Some(gate.clone());
    tokio::time::advance(Duration::from_secs(30)).await;
    super::wait_count(&mock.counts[1], 2).await;
    tokio::time::advance(Duration::from_secs(4)).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let before = tokio::select! {
        snapshot = super::stats_http(&client, &base, request(None), StatusCode::OK) => snapshot,
        _ = async { while std::time::Instant::now() < deadline { tokio::task::yield_now().await; } } => {
            panic!("cached REST reader blocked behind pending premium")
        }
    };
    view.assert_matches_snapshot(&before);
    assert_eq!(
        row(&before["markets"], &btc)["fields"]["funding"]["state"],
        "available"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    super::ws_delta(&mut socket, &mut view).await;
    let funding = &view.markets[&btc]["fields"]["funding"];
    assert_eq!(funding["state"], "stale");
    assert_eq!(funding["reason"], "funding-payment-passed");
    assert_eq!(
        funding["value"],
        original[&btc]["fields"]["funding"]["value"]
    );
    assert_eq!(
        funding["receivedTimestamp"],
        original[&btc]["fields"]["funding"]["receivedTimestamp"]
    );
    assert_eq!(funding["value"]["kind"], "estimate");
    assert_eq!(funding["value"]["paymentTimestamp"], Value::Null);
    for field in ["markPrice", "indexPrice"] {
        assert_eq!(view.markets[&btc]["fields"][field]["state"], "available");
    }
    assert_eq!(
        view.markets[&id("ALTUSD1")]["fields"]["funding"]["state"],
        "available"
    );
    super::assert_ws_matches_http(&client, &base, &view).await;
    tokio::time::advance(Duration::from_secs(55)).await;
    super::ws_delta(&mut socket, &mut view).await;
    for (market_id, prior) in &original {
        for field in IMPLEMENTED {
            let retained = &view.markets[market_id]["fields"][field];
            assert_eq!(retained["state"], "stale");
            assert_eq!(
                retained["reason"],
                if market_id == &btc && field == "funding" {
                    "funding-payment-passed"
                } else {
                    "stale-threshold"
                }
            );
            assert_eq!(retained["value"], prior["fields"][field]["value"]);
            assert_eq!(
                retained["receivedTimestamp"],
                prior["fields"][field]["receivedTimestamp"]
            );
        }
    }
    assert_eq!(mock.calls(), [2, 2, 2]);
    super::assert_ws_matches_http(&client, &base, &view).await;
    super::after_receipt(&original[&btc]["fields"]["funding"]["receivedTimestamp"]).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    mock.replies[1].write().await.barrier = None;
    gate.add_permits(1);
    super::ws_delta(&mut socket, &mut view).await;
    for (market_id, prior) in &original {
        for field in IMPLEMENTED {
            let fresh = &view.markets[market_id]["fields"][field];
            assert_eq!(fresh["state"], "available");
            assert_ne!(
                fresh["receivedTimestamp"],
                prior["fields"][field]["receivedTimestamp"]
            );
        }
    }
    // Catalog/config expired while premium waited. Their catch-up must not re-fetch premium.
    super::wait_count(&mock.counts[0], 3).await;
    super::wait_count(&mock.counts[2], 3).await;
    assert_eq!(mock.calls()[1], 2);
    // Catalog/config catch-up cannot re-age the cached premium observation.
    let recovered_receipt = view.markets[&btc]["fields"]["funding"]["receivedTimestamp"].clone();
    tokio::time::advance(Duration::from_secs(29)).await;
    let cached = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(
        row(&cached["markets"], &btc)["fields"]["funding"]["receivedTimestamp"],
        recovered_receipt
    );
    assert_eq!(mock.calls()[1], 2);
    view.assert_matches_snapshot(&cached);
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
async fn aster_pending_configuration_does_not_lock_catalog_or_restart_failure_ttl() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    let source = Arc::new(AsterExchange::with_base_url(upstream, 120_000).unwrap());
    source.fetch_market_stats(params()).await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    mock.reply(2, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    mock.replies[2].write().await.barrier = Some(gate.clone());
    tokio::time::advance(Duration::from_secs(30)).await;
    let refreshing_source = source.clone();
    let refreshing = tokio::spawn(async move {
        refreshing_source
            .fetch_market_stats(params())
            .await
            .unwrap()
    });
    super::wait_count(&mock.counts[2], 2).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let catalog = tokio::select! {
        catalog = source.fetch_markets(FetchMarketsParams { params: Value::Null, include_inactive: true }) => catalog.unwrap(),
        _ = async { while std::time::Instant::now() < deadline { tokio::task::yield_now().await; } } => {
            panic!("independent catalog acquisition blocked on configuration")
        }
    };
    assert!(catalog.iter().any(|market| market
        .identity
        .as_ref()
        .is_some_and(|identity| identity.market_id == id("BTCUSDT"))));
    tokio::time::advance(Duration::from_secs(10)).await;
    mock.replies[2].write().await.barrier = None;
    gate.add_permits(1);
    let failed = refreshing.await.unwrap();
    assert!(failed
        .source_failures
        .iter()
        .any(|failure| failure.source == SOURCES[2] && failure.reason == "upstream-failure"));
    let rows = serde_json::to_value(failed.rows).unwrap();
    let fields = &row(&rows, &id("BTCUSDT"))["fields"];
    assert_eq!(fields["funding"]["state"], "available");
    assert_eq!(fields["funding"]["value"]["rate"], "-0.00001250");
    assert_eq!(fields["funding"]["value"]["rateIntervalMs"], Value::Null);
    assert_eq!(fields["markPrice"]["value"]["amount"], "30000.000");
    assert_eq!(mock.calls(), [2, 2, 2]);
    mock.reply(
        2,
        StatusCode::OK,
        json!([
            {"symbol":"BTCUSDT", "fundingIntervalHours":4},
            {"symbol":"ALTUSD1", "fundingIntervalHours":1}
        ]),
    )
    .await;
    tokio::time::advance(Duration::from_secs(29)).await;
    let cached_failure = source.fetch_market_stats(params()).await.unwrap();
    assert!(cached_failure
        .source_failures
        .iter()
        .any(|failure| failure.source == SOURCES[2]));
    assert_eq!(
        mock.calls()[2],
        2,
        "config deadline starts on completion at t=40, not request start at t=30"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    let recovered = source.fetch_market_stats(params()).await.unwrap();
    assert!(recovered.source_failures.is_empty());
    let recovered = serde_json::to_value(recovered.rows).unwrap();
    assert_eq!(
        row(&recovered, &id("BTCUSDT"))["fields"]["funding"]["value"]["rateIntervalMs"],
        14_400_000
    );
    assert_eq!(mock.calls()[2], 3);
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}
