use axum::{
    extract::{Query, State},
    http::{Request, StatusCode},
    response::Response,
    Router,
};
use ferris_market_data_backend::{
    exchanges::{
        bybit::BybitExchange,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::{FetchMarketStatsParams, FetchMarketsParams},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};

const CURSOR: &str = "page +/=?:two";
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
type Reply = (StatusCode, Value);

#[derive(Clone)]
struct BybitMock {
    pages: Arc<RwLock<BTreeMap<(String, String), Reply>>>,
    tickers: Arc<RwLock<BTreeMap<String, Reply>>>,
    // Each category has an instruments-info counter followed by a tickers counter.
    counts: Arc<[AtomicUsize; 6]>,
    gates: Arc<RwLock<BTreeMap<usize, Arc<Semaphore>>>>,
}

impl BybitMock {
    async fn catalog(&self, category: &str, rows: Vec<Value>) {
        self.page(category, "", StatusCode::OK, envelope(category, rows, ""))
            .await;
    }

    async fn page(&self, category: &str, cursor: &str, status: StatusCode, body: Value) {
        self.pages
            .write()
            .await
            .insert((category.into(), cursor.into()), (status, body));
    }

    async fn ticker(&self, category: &str, status: StatusCode, body: Value) {
        self.tickers
            .write()
            .await
            .insert(category.into(), (status, body));
    }

    fn calls(&self) -> [usize; 6] {
        self.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst))
    }
}

fn instrument(category: &str, symbol: &str, base: &str, quote: &str, settle: &str) -> Value {
    json!({
        "symbol":symbol,
        "contractType":if category == "linear" { "LinearPerpetual" } else { "InversePerpetual" },
        "status":"Trading", "baseCoin":base, "quoteCoin":quote, "settleCoin":settle,
        "isPreListing":false, "fundingInterval":480
    })
}

fn linear_catalog() -> Vec<Value> {
    let btc = instrument("linear", "BTCUSDT", "BTC", "USDT", "USDT");
    let mut alias = instrument("linear", "BTC-ALT", "BTC", "USDT", "USDC");
    alias["fundingInterval"] = json!(60);
    let mut old = instrument("linear", "OLDUSDT", "OLD", "USDT", "USDT");
    old["status"] = json!("Settling");
    let mut pre = instrument("linear", "PREUSDT", "PRE", "USDT", "USDT");
    pre["isPreListing"] = json!(true);
    let mut future = instrument("linear", "FUTUSDT", "FUT", "USDT", "USDT");
    future["contractType"] = json!("LinearFutures");
    vec![btc, alias, old, pre, future]
}

fn inverse_catalog() -> Vec<Value> {
    vec![instrument("inverse", "BTCUSDT", "BTC", "USD", "BTC")]
}

fn ticker(symbol: &str, rate: &str, mark: &str, index: &str, last: &str) -> Value {
    json!({
        "symbol":symbol, "fundingRate":rate, "markPrice":mark,
        "indexPrice":index, "lastPrice":last, "nextFundingTime":"1700028800000"
    })
}

fn linear_tickers() -> Vec<Value> {
    // Reverse the active instrument ordering; BTC-ALT also has the same display name.
    let alias = ticker("BTC-ALT", "0", "1.2500", "1.2", "1.2400");
    let mut btc = ticker(
        "BTCUSDT",
        "-0.00001250",
        "30000.000",
        "29999.9",
        "30001.2300",
    );
    btc["fundingIntervalHour"] = json!("8");
    vec![
        alias,
        btc,
        ticker("OLDUSDT", "0.01", "10", "10", "10"),
        ticker("PREUSDT", "0.02", "20", "20", "20"),
        ticker("FUTUSDT", "0.03", "30", "30", "30"),
    ]
}

fn envelope(category: &str, rows: Vec<Value>, cursor: &str) -> Value {
    json!({
        "retCode":0, "retMsg":"OK", "time":1700000000123u64,
        "result":{"category":category, "list":rows, "nextPageCursor":cursor}
    })
}

async fn handler(
    State(mock): State<BybitMock>,
    Query(query): Query<BTreeMap<String, String>>,
    request: Request<axum::body::Body>,
) -> Response {
    let category = query.get("category").expect("category is required");
    let offset = match category.as_str() {
        "linear" => 0,
        "inverse" => 2,
        "spot" => 4,
        other => panic!("unexpected category {other}"),
    };
    let is_catalog = match request.uri().path() {
        "/v5/market/instruments-info" => true,
        "/v5/market/tickers" => false,
        path => panic!("unexpected Bybit endpoint {path}"),
    };
    let mut expected = BTreeMap::from([("category".to_string(), category.clone())]);
    if is_catalog && category != "spot" {
        expected.insert("limit".into(), "1000".into());
        if let Some(cursor) = query.get("cursor") {
            assert!(!cursor.is_empty());
            expected.insert("cursor".into(), cursor.clone());
        }
    }
    assert_eq!(
        query, expected,
        "bulk requests must not filter symbols, fields, status, or baseCoin"
    );
    let index = offset + usize::from(!is_catalog);
    mock.counts[index].fetch_add(1, Ordering::SeqCst);
    let gate = mock.gates.read().await.get(&index).cloned();
    if let Some(gate) = gate {
        gate.acquire().await.unwrap().forget();
    }
    let (status, body) = if is_catalog {
        mock.pages
            .read()
            .await
            .get(&(
                category.clone(),
                query.get("cursor").cloned().unwrap_or_default(),
            ))
            .unwrap_or_else(|| panic!("unexpected catalog cursor: {query:?}"))
            .clone()
    } else {
        mock.tickers.read().await.get(category).unwrap().clone()
    };
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

async fn server() -> (BybitMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = BybitMock {
        pages: Arc::new(RwLock::new(BTreeMap::new())),
        tickers: Arc::new(RwLock::new(BTreeMap::new())),
        counts: Arc::new(Default::default()),
        gates: Arc::new(RwLock::new(BTreeMap::new())),
    };
    mock.catalog("linear", linear_catalog()).await;
    mock.catalog("inverse", inverse_catalog()).await;
    mock.catalog(
        "spot",
        vec![json!({
            "symbol":"BTCUSDT", "baseCoin":"BTC", "quoteCoin":"USDT", "status":"Trading"
        })],
    )
    .await;
    mock.ticker(
        "linear",
        StatusCode::OK,
        envelope("linear", linear_tickers(), ""),
    )
    .await;
    mock.ticker(
        "inverse",
        StatusCode::OK,
        envelope(
            "inverse",
            vec![ticker(
                "BTCUSDT",
                "0.00000125",
                "61000.001",
                "61000",
                "61001.000",
            )],
            "",
        ),
    )
    .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(handler).with_state(mock.clone());
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .unwrap();
    });
    (mock, format!("http://{address}"), stop, task)
}

fn id(category: &str, symbol: &str) -> String {
    json!(["bybit", "perp", category, null, symbol]).to_string()
}

fn params(category: &str) -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: json!({"category":category}),
    }
}

fn request(category: &str, ids: Option<Vec<String>>) -> Value {
    json!({"exchange":"bybit", "params":{"category":category}, "marketIds":ids, "fields":FIELDS})
}

fn row<'a>(rows: &'a Value, market_id: &str) -> &'a Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|row| row["marketId"] == market_id)
        .unwrap()
}

fn failure(snapshot: &Value, source: &str, reason: &str) -> bool {
    snapshot["coverage"]["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["source"] == source && failure["reason"] == reason)
}

async fn catalog_http(client: &reqwest::Client, base: &str, category: &str) -> Value {
    let response = client
        .post(format!("{base}/v1/fetchMarkets"))
        .json(&json!({"exchange":"bybit", "params":{"category":category}, "includeInactive":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn bybit_catalog_native_identity_pagination_and_product_scope() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let instruments = linear_catalog();
    mock.page(
        "linear",
        "",
        StatusCode::OK,
        envelope("linear", instruments[..2].to_vec(), CURSOR),
    )
    .await;
    mock.page(
        "linear",
        CURSOR,
        StatusCode::OK,
        envelope("linear", instruments[2..].to_vec(), ""),
    )
    .await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BybitExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let catalog = catalog_http(&client, &base, "linear").await;
    let inverse_catalog = catalog_http(&client, &base, "inverse").await;
    let spot_catalog = catalog_http(&client, &base, "spot").await;
    assert!(spot_catalog["markets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["type"] == "spot"));
    assert!(catalog["markets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["type"] == "future"));
    let linear = super::stats_http(&client, &base, request("linear", None), StatusCode::OK).await;
    let inverse = super::stats_http(&client, &base, request("inverse", None), StatusCode::OK).await;
    let btc_id = id("linear", "BTCUSDT");
    let alias_id = id("linear", "BTC-ALT");
    let inverse_id = id("inverse", "BTCUSDT");
    let btc = row(&linear["markets"], &btc_id);
    let alias = row(&linear["markets"], &alias_id);
    let inverse_btc = row(&inverse["markets"], &inverse_id);
    assert_eq!(btc["symbol"], alias["symbol"]);
    assert_eq!(btc["exchangeMarketId"], inverse_btc["exchangeMarketId"]);
    assert_ne!(btc["marketId"], inverse_btc["marketId"]);
    for (market, catalog, settle, category, contract) in [
        (btc, &catalog, "USDT", "linear", "LinearPerpetual"),
        (alias, &catalog, "USDC", "linear", "LinearPerpetual"),
        (
            inverse_btc,
            &inverse_catalog,
            "BTC",
            "inverse",
            "InversePerpetual",
        ),
    ] {
        let issued = row(&catalog["markets"], market["marketId"].as_str().unwrap());
        assert_eq!(market["settle"], settle);
        assert_eq!(market["settlementAssetId"], settle);
        assert_eq!(market["category"], category);
        assert_eq!(market["contractType"], contract);
        let mut identity = market.clone();
        identity.as_object_mut().unwrap().remove("fields");
        assert_eq!(&identity, issued);
    }
    assert_eq!(btc["fields"]["funding"]["value"]["rate"], "-0.00001250");
    assert_eq!(btc["fields"]["markPrice"]["value"]["amount"], "30000.000");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "29999.9");
    assert_eq!(btc["fields"]["lastPrice"]["value"]["amount"], "30001.2300");
    assert_eq!(alias["fields"]["funding"]["value"]["rate"], "0");
    assert_eq!(alias["fields"]["lastPrice"]["value"]["amount"], "1.2400");
    assert_eq!(
        inverse_btc["fields"]["funding"]["value"]["rate"],
        "0.00000125"
    );
    assert_eq!(
        inverse_btc["fields"]["lastPrice"]["value"]["amount"],
        "61001.000"
    );
    assert_eq!(linear["coverage"]["expectedMarkets"], 2);
    assert_eq!(linear["markets"].as_array().unwrap().len(), 2);
    let inactive = super::stats_http(
        &client,
        &base,
        request(
            "linear",
            Some(vec![id("linear", "OLDUSDT"), id("linear", "PREUSDT")]),
        ),
        StatusCode::OK,
    )
    .await;
    for market in inactive["markets"].as_array().unwrap() {
        assert_eq!(market["active"], false);
        assert_eq!(market["fields"].as_object().unwrap().len(), FIELDS.len());
        for field in IMPLEMENTED {
            assert_eq!(market["fields"][field]["state"], "unavailable");
            assert_eq!(market["fields"][field]["reason"], "inactive-market");
            assert_eq!(market["fields"][field]["value"], Value::Null);
        }
        for field in ["openInterest", "volume24h", "lastSettledFunding"] {
            assert_eq!(market["fields"][field]["state"], "unsupported");
        }
    }
    super::stats_http(
        &client,
        &base,
        request("linear", Some(vec![id("linear", "FUTUSDT")])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.calls(), [2, 1, 1, 1, 1, 0]);
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn bybit_intervals_and_timestamps_reject_ambiguous_units_without_losing_rates() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    // A present invalid ticker interval cannot fall back to valid instrument minutes.
    let cases = [
        ("FALLBACK", Some(json!(480)), None, Some(28_800_000u64)),
        ("FRESH", Some(json!(480)), Some(json!("1")), Some(3_600_000)),
        ("EMPTY", Some(json!(480)), Some(json!("")), None),
        ("NULL", Some(json!(480)), Some(Value::Null), None),
        ("ZERO", Some(json!(480)), Some(json!("0")), None),
        ("FRACTION", Some(json!(480)), Some(json!("1.5")), None),
        ("NUMBER", Some(json!(480)), Some(json!(1)), None),
        (
            "HOUR_OVERFLOW",
            Some(json!(480)),
            Some(json!(u64::MAX.to_string())),
            None,
        ),
        ("MINUTE_OVERFLOW", Some(json!(u64::MAX)), None, None),
        ("MINUTE_STRING", Some(json!("480")), None, None),
        ("NO_INTERVAL", None, None, None),
    ];
    let mut instruments = Vec::new();
    let mut tickers = Vec::new();
    for (symbol, minutes, hours, _) in &cases {
        let mut info = instrument("linear", symbol, symbol, "USDT", "USDT");
        info.as_object_mut().unwrap().remove("fundingInterval");
        if let Some(minutes) = minutes {
            info["fundingInterval"] = minutes.clone();
        }
        let mut quote = ticker(symbol, "-0.00010000", "10.00", "9.99", "10.01");
        if let Some(hours) = hours {
            quote["fundingIntervalHour"] = hours.clone();
        }
        instruments.push(info);
        tickers.push(quote);
    }
    mock.catalog("linear", instruments).await;
    mock.ticker(
        "linear",
        StatusCode::OK,
        envelope("linear", tickers.clone(), ""),
    )
    .await;
    let source = BybitExchange::with_base_url(upstream, 2_000).unwrap();
    let first = source.fetch_market_stats(params("linear")).await.unwrap();
    let rows = serde_json::to_value(&first.rows).unwrap();
    for (symbol, _, _, interval) in &cases {
        let funding = &row(&rows, &id("linear", symbol))["fields"]["funding"];
        assert_eq!(funding["state"], "available", "{symbol}");
        assert_eq!(funding["reason"], Value::Null);
        let value = &funding["value"];
        assert_eq!(value["rate"], "-0.00010000");
        assert_eq!(value["rateIntervalMs"], json!(interval), "{symbol}");
        assert_eq!(value["paymentIntervalMs"], json!(interval), "{symbol}");
        assert_eq!(value["rateUnit"], "decimalFraction");
        assert_eq!(value["kind"], "estimate");
        assert_eq!(value["paymentTimestamp"], Value::Null);
        assert_eq!(value["nextPaymentTimestamp"], 1700028800000u64);
        assert_eq!(funding["exchangeTimestamp"], 1700000000123u64);
        if interval.is_none() {
            assert_eq!(value["equivalents"], Value::Null, "{symbol}");
        }
    }
    let fallback = &row(&rows, &id("linear", "FALLBACK"))["fields"]["funding"]["value"];
    let fresh = &row(&rows, &id("linear", "FRESH"))["fields"]["funding"]["value"];
    let decimal = |value: &Value| {
        value
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap()
    };
    assert_eq!(
        decimal(&fallback["equivalents"]["oneHourPercent"]),
        "-0.00125".parse().unwrap()
    );
    assert_eq!(
        decimal(&fresh["equivalents"]["oneHourPercent"]),
        "-0.01".parse().unwrap()
    );
    assert!(first
        .source_failures
        .iter()
        .any(|failure| failure.source == "bybit:linear:instruments-info"
            && failure.reason == "funding-interval-mismatch"));
    tokio::time::advance(Duration::from_secs(29)).await;
    let cached = source
        .fetch_market_stats(FetchMarketStatsParams {
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(cached.received_at, first.received_at);
    assert_eq!(cached.rows, first.rows);
    assert_eq!(mock.calls(), [1, 1, 0, 0, 0, 0]);

    // Invalid native payment timestamps never become the envelope's observation time.
    for (quote, invalid) in tickers.iter_mut().zip([
        Value::Null,
        json!(0),
        json!("0"),
        json!("-1"),
        json!("1.5"),
        json!("1700028800000 "),
        json!("18446744073709551616"),
        json!(""),
    ]) {
        quote["nextFundingTime"] = invalid;
    }
    mock.ticker("linear", StatusCode::OK, envelope("linear", tickers, ""))
        .await;
    tokio::time::advance(Duration::from_millis(1_001)).await;
    let changed = source.fetch_market_stats(params("linear")).await.unwrap();
    let rows = serde_json::to_value(changed.rows).unwrap();
    for (symbol, _, _, _) in &cases[..8] {
        let funding = &row(&rows, &id("linear", symbol))["fields"]["funding"];
        assert_eq!(funding["state"], "available");
        assert_eq!(funding["value"]["nextPaymentTimestamp"], Value::Null);
    }
    assert_eq!(mock.calls(), [2, 2, 0, 0, 0, 0]);
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn bybit_exact_decimal_contract_clears_only_invalid_present_scalars() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    let invalid_rates = [
        json!(0),
        json!("1e-3"),
        json!(" 0.1"),
        json!("１２"),
        Value::Null,
    ];
    let mut instruments = Vec::new();
    let mut quotes = Vec::new();
    for (index, rate) in invalid_rates.into_iter().enumerate() {
        let symbol = format!("INVALID{index}");
        instruments.push(instrument("linear", &symbol, &symbol, "USDT", "USDT"));
        let mut quote = ticker(&symbol, "0", "1.000000000000000001", "1.2", "1.25");
        quote["fundingRate"] = rate;
        quotes.push(quote);
    }
    for (symbol, rate) in [("ZERO", "0"), ("NEGATIVE", "-0.000000000000000001")] {
        instruments.push(instrument("linear", symbol, symbol, "USDT", "USDT"));
        quotes.push(ticker(symbol, rate, "0.0", "-1", "1e3"));
    }
    let symbol = "MISSING";
    let mut info = instrument("linear", symbol, symbol, "USDT", "USDT");
    info.as_object_mut().unwrap().remove("settleCoin");
    instruments.push(info);
    let mut quote = ticker(symbol, "0.01", "1", "1", "1");
    quote.as_object_mut().unwrap().remove("fundingRate");
    quote["lastPrice"] = json!(1);
    quotes.push(quote);
    instruments.push(instrument(
        "linear",
        "哈基米-USDT",
        "哈基米",
        "USDT",
        "USDT",
    ));
    quotes.push(ticker(
        "哈基米-USDT",
        "0.000100",
        "0.0100",
        "0.0099",
        "0.0101",
    ));
    mock.catalog("linear", instruments).await;
    mock.ticker("linear", StatusCode::OK, envelope("linear", quotes, ""))
        .await;
    let source = BybitExchange::with_base_url(upstream, 2_000).unwrap();
    let snapshot = source.fetch_market_stats(params("linear")).await.unwrap();
    let rows = serde_json::to_value(snapshot.rows).unwrap();
    for index in 0..5 {
        let fields = &row(&rows, &id("linear", &format!("INVALID{index}")))["fields"];
        assert_eq!(fields["funding"]["state"], "unavailable");
        assert_eq!(fields["funding"]["value"], Value::Null);
        assert_eq!(
            fields["markPrice"]["value"]["amount"],
            "1.000000000000000001"
        );
        assert_eq!(fields["lastPrice"]["state"], "available");
    }
    for (symbol, rate) in [("ZERO", "0"), ("NEGATIVE", "-0.000000000000000001")] {
        let fields = &row(&rows, &id("linear", symbol))["fields"];
        assert_eq!(fields["funding"]["value"]["rate"], rate);
        for field in ["markPrice", "indexPrice", "lastPrice"] {
            assert_eq!(fields[field]["state"], "unavailable");
            assert_eq!(fields[field]["value"], Value::Null);
        }
    }
    let unicode = row(&rows, &id("linear", "哈基米-USDT"));
    assert_eq!(unicode["exchangeMarketId"], "哈基米-USDT");
    assert_eq!(unicode["base"], "哈基米");
    assert_eq!(unicode["quote"], "USDT");
    assert_eq!(unicode["fields"]["funding"]["value"]["rate"], "0.000100");
    let catalog = source
        .fetch_markets(FetchMarketsParams {
            params: json!({"category":"linear"}),
            include_inactive: true,
        })
        .await
        .unwrap();
    let catalog = serde_json::to_value(catalog).unwrap();
    let issued = row(&catalog, &id("linear", "哈基米-USDT"));
    assert_eq!(issued["base"], "哈基米");
    assert_eq!(issued["exchangeMarketId"], "哈基米-USDT");
    assert_eq!(mock.calls(), [1, 1, 0, 0, 0, 0]);
    let missing = row(&rows, &id("linear", "MISSING"));
    assert_eq!(missing["settle"], Value::Null);
    assert_eq!(missing["settlementAssetId"], Value::Null);
    for field in ["funding", "lastPrice"] {
        assert_eq!(missing["fields"][field]["value"], Value::Null);
        assert_eq!(missing["fields"][field]["reason"], "invalid-upstream-value");
    }
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn bybit_rest_ws_share_category_acquisition_and_reduce_failure_recovery_and_removal() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BybitExchange::with_base_url(upstream, 2_000).unwrap());
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
    assert_eq!(
        caps["exchanges"]
            .as_array()
            .unwrap()
            .iter()
            .find(|exchange| exchange["exchange"] == "bybit")
            .unwrap()["marketStats"]["state"],
        "supported"
    );
    assert_eq!(mock.calls(), [0; 6]);
    let btc = id("linear", "BTCUSDT");
    let all_request = request("linear", None);
    let selected_request = request("linear", Some(vec![btc.clone()]));
    let mut all_socket = super::stats_socket(&base).await;
    let mut selected_socket = super::stats_socket(&base).await;
    let gate = Arc::new(Semaphore::new(0));
    mock.gates.write().await.insert(0, gate.clone());
    let mut requests = Vec::new();
    for index in 0..8 {
        let (client, base) = (client.clone(), base.clone());
        let mut request = if index % 2 == 0 {
            all_request.clone()
        } else {
            selected_request.clone()
        };
        request["params"] = if index % 2 == 0 {
            Value::Null
        } else {
            json!({})
        };
        requests.push(tokio::spawn(async move {
            super::stats_http(&client, &base, request, StatusCode::OK).await
        }));
    }
    let (catalog_client, catalog_base) = (client.clone(), base.clone());
    let catalog =
        tokio::spawn(async move { catalog_http(&catalog_client, &catalog_base, "linear").await });
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
    mock.gates.write().await.remove(&0);
    gate.add_permits(1);
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    assert_eq!(selected.topic["params"], json!({"category":"linear"}));
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(
        row(&catalog.await.unwrap()["markets"], &btc)["settle"],
        "USDT"
    );
    for (index, request) in requests.into_iter().enumerate() {
        let snapshot = request.await.unwrap();
        if index % 2 == 0 {
            all.assert_matches_snapshot(&snapshot);
        } else {
            selected.assert_matches_snapshot(&snapshot);
        }
    }
    assert_eq!(mock.calls(), [1, 1, 0, 0, 0, 0]);
    let mut duplicate = selected_request.clone();
    duplicate["params"] = json!({});
    super::ws_send(
        &mut selected_socket,
        super::stats_command("subscribe", &duplicate),
    )
    .await;
    assert_eq!(
        super::ws_next(&mut selected_socket).await["type"],
        "alreadySubscribed"
    );

    // The rate is unchanged, but a new payment interval and next payment require a delta.
    let previous_funding = selected.markets[&btc]["fields"]["funding"].clone();
    let mut quotes = linear_tickers();
    quotes[1]["fundingIntervalHour"] = json!("1");
    quotes[1]["nextFundingTime"] = json!("1700032400000");
    mock.ticker(
        "linear",
        StatusCode::OK,
        envelope("linear", quotes.clone(), ""),
    )
    .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let changed = super::ws_delta(&mut selected_socket, &mut selected).await;
    let funding = &selected.markets[&btc]["fields"]["funding"];
    assert_eq!(funding["state"], "available");
    assert_eq!(funding["value"]["rate"], previous_funding["value"]["rate"]);
    assert_eq!(funding["value"]["paymentIntervalMs"], 3_600_000);
    assert_eq!(funding["value"]["nextPaymentTimestamp"], 1700032400000u64);
    assert_ne!(
        funding["value"]["equivalents"],
        previous_funding["value"]["equivalents"]
    );
    assert!(changed["updates"][0]["fields"].get("funding").is_some());
    assert!(all.coverage["sourceFailures"]
        .as_array()
        .unwrap()
        .iter()
        .any(
            |failure| failure["source"] == "bybit:linear:instruments-info"
                && failure["reason"] == "funding-interval-mismatch"
        ));

    // A present malformed scalar clears funding but must not poison valid sibling prices.
    let mut instruments = linear_catalog();
    instruments[0]["fundingInterval"] = json!(60);
    mock.catalog("linear", instruments.clone()).await;
    quotes[1]["fundingRate"] = json!(12);
    quotes[1]["markPrice"] = json!("31000.000");
    mock.ticker(
        "linear",
        StatusCode::OK,
        envelope("linear", quotes.clone(), ""),
    )
    .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let cleared = selected.markets[&btc]["fields"]["funding"].clone();
    let last = selected.markets[&btc]["fields"]["lastPrice"].clone();
    assert_eq!(cleared["state"], "unavailable");
    assert_eq!(cleared["value"], Value::Null);
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["value"]["amount"],
        "31000.000"
    );
    assert!(all.coverage["sourceFailures"]
        .as_array()
        .unwrap()
        .is_empty());
    mock.ticker("linear", StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], true);
    assert_eq!(selected.markets[&btc]["fields"]["funding"], cleared);
    let stale_last = &selected.markets[&btc]["fields"]["lastPrice"];
    assert_eq!(stale_last["state"], "stale");
    for key in ["value", "receivedTimestamp", "exchangeTimestamp"] {
        assert_eq!(stale_last[key], last[key]);
    }
    assert!(failure(
        &json!({"coverage":all.coverage}),
        "bybit:linear:tickers",
        "upstream-failure"
    ));
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;

    // Missing bulk rows retain earlier observations; this is not a scalar clear.
    quotes.remove(1);
    mock.ticker("linear", StatusCode::OK, envelope("linear", quotes, ""))
        .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(selected.markets[&btc]["fields"]["funding"], cleared);
    assert_eq!(
        selected.markets[&btc]["fields"]["lastPrice"]["reason"],
        "missing-upstream-row"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["lastPrice"]["value"],
        last["value"]
    );

    let mut recovered_quotes = linear_tickers();
    recovered_quotes[1]["fundingIntervalHour"] = json!("1");
    recovered_quotes[1]
        .as_object_mut()
        .unwrap()
        .remove("lastPrice");
    mock.ticker(
        "linear",
        StatusCode::OK,
        envelope("linear", recovered_quotes, ""),
    )
    .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["state"],
        "available"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["lastPrice"]["value"],
        Value::Null
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["lastPrice"]["reason"],
        "invalid-upstream-value"
    );

    instruments[0]["status"] = json!("Settling");
    mock.catalog("linear", instruments.clone()).await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let inactive = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(inactive["removedMarketIds"], json!([btc]));
    assert_eq!(selected.markets[&btc]["active"], false);
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["reason"],
        "inactive-market"
    );
    instruments.remove(0);
    mock.catalog("linear", instruments).await;
    super::after_receipt(
        &all.markets.values().next().unwrap()["fields"]["funding"]["receivedTimestamp"],
    )
    .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let removed = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));
    assert!(selected.markets.is_empty());
    super::stats_http(&client, &base, selected_request, StatusCode::BAD_REQUEST).await;
    assert_eq!(mock.calls(), [8, 8, 0, 0, 0, 0]);
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
async fn bybit_invalid_catalog_pages_retain_membership_until_authoritative_recovery() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BybitExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    let all_request = request("linear", None);
    super::ws_send(&mut socket, super::stats_command("subscribe", &all_request)).await;
    let mut view = super::ws_initial(&mut socket).await;
    let mut baseline = view.markets.clone();
    let instruments = linear_catalog();
    let page = envelope("linear", vec![instruments[0].clone()], CURSOR);
    let mut missing_status = instruments[0].clone();
    missing_status.as_object_mut().unwrap().remove("status");
    let mut malformed_prelisting = instruments[0].clone();
    malformed_prelisting["isPreListing"] = json!("false");
    let mut missing_prelisting = instruments[0].clone();
    missing_prelisting
        .as_object_mut()
        .unwrap()
        .remove("isPreListing");
    let mut missing_cursor = envelope("linear", instruments.clone(), "");
    missing_cursor["result"]
        .as_object_mut()
        .unwrap()
        .remove("nextPageCursor");
    let mut missing_contract = instruments[0].clone();
    missing_contract
        .as_object_mut()
        .unwrap()
        .remove("contractType");
    let mut empty_symbol = instruments[0].clone();
    empty_symbol["symbol"] = json!("");
    let invalid_cases = vec![
        // A valid prefix must not become a partial catalog on failure of its last page.
        (
            page.clone(),
            Some((StatusCode::BAD_GATEWAY, json!({"offline":true}))),
        ),
        // Both a repeated cursor and a duplicate symbol across pages invalidate the whole batch.
        (
            page.clone(),
            Some((
                StatusCode::OK,
                envelope("linear", vec![instruments[1].clone()], CURSOR),
            )),
        ),
        (
            page,
            Some((
                StatusCode::OK,
                envelope("linear", vec![instruments[0].clone()], ""),
            )),
        ),
        (envelope("inverse", instruments.clone(), ""), None),
        (envelope("linear", vec![missing_status], ""), None),
        (envelope("linear", vec![missing_prelisting], ""), None),
        (envelope("linear", vec![malformed_prelisting], ""), None),
        (envelope("linear", vec![missing_contract], ""), None),
        (envelope("linear", vec![empty_symbol], ""), None),
        (missing_cursor, None),
        (envelope("linear", vec![], ""), None),
    ];
    for (index, (first, second)) in invalid_cases.into_iter().enumerate() {
        mock.page("linear", "", StatusCode::OK, first).await;
        if let Some((status, second)) = second {
            mock.page("linear", CURSOR, status, second).await;
        }
        tokio::time::advance(Duration::from_millis(30_001)).await;
        let delta = super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(delta["removedMarketIds"], json!([]));
        assert_eq!(
            view.markets.keys().collect::<Vec<_>>(),
            baseline.keys().collect::<Vec<_>>()
        );
        assert_eq!(view.coverage["enumerationComplete"], false);
        assert_eq!(view.coverage["expectedMarkets"], 2);
        for (market_id, original) in &baseline {
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
        let snapshot = super::stats_http(&client, &base, all_request.clone(), StatusCode::OK).await;
        view.assert_matches_snapshot(&snapshot);
        assert!(failure(
            &snapshot,
            "bybit:linear:instruments-info",
            if index == 0 {
                "upstream-failure"
            } else {
                "invalid-upstream-data"
            }
        ));
        let counts = mock.calls();
        super::stats_http(
            &client,
            &base,
            request("linear", Some(vec![id("linear", "UNKNOWN")])),
            StatusCode::BAD_GATEWAY,
        )
        .await;
        assert_eq!(
            mock.calls(),
            counts,
            "failed catalog observations must also share their TTL"
        );
        // Return to fresh observations so the next corruption has an observable transition.
        mock.catalog("linear", instruments.clone()).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(view.coverage["enumerationComplete"], true);
        assert_eq!(
            view.markets[&id("linear", "BTCUSDT")]["fields"]["lastPrice"]["state"],
            "available"
        );
        // Compare the next failure to the most recent observation, not a new receipt as old data.
        for (market_id, original) in &baseline {
            assert_eq!(
                view.markets[market_id]["fields"]["lastPrice"]["value"],
                original["fields"]["lastPrice"]["value"]
            );
        }
        baseline = view.markets.clone();
    }
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
async fn bybit_invalid_topics_are_rejected_before_acquisition_and_cold_catalog_is_not_unknown() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    mock.page(
        "linear",
        "",
        StatusCode::OK,
        json!({"retCode":10001,"retMsg":"offline"}),
    )
    .await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BybitExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    for invalid in [
        json!({"exchange":"bybit","params":{"category":"spot"}}),
        json!({"exchange":"bybit","params":{"category":"option"}}),
        json!({"exchange":"bybit","params":{"category":" Linear "}}),
        json!({"exchange":"bybit","params":{"category":null}}),
        json!({"exchange":"bybit","params":{"category":"linear","symbol":"BTCUSDT"}}),
        json!({"exchange":"bybit","params":[]}),
        request("linear", Some(vec![id("inverse", "BTCUSDT")])),
        json!({"exchange":"bybit","marketIds":["[\"bybit\",\"spot\",\"linear\",null,\"BTCUSDT\"]"]}),
        json!({"exchange":"bybit","marketIds":["[\"bybit\",\"future\",\"linear\",null,\"BTCUSDT\"]"]}),
        json!({"exchange":"bybit","marketIds":["[\"bybit\",\"perp\",null,null,\"BTCUSDT\"]"]}),
        request("linear", Some(vec![])),
        request("linear", Some(vec![id("linear", "BTCUSDT"); 101])),
    ] {
        super::stats_http(&client, &base, invalid.clone(), StatusCode::BAD_REQUEST).await;
        super::ws_error(
            &mut socket,
            super::stats_command("subscribe", &invalid),
            "INVALID_TOPIC",
        )
        .await;
    }
    assert_eq!(mock.calls(), [0; 6]);
    let cold = super::stats_http(&client, &base, request("linear", None), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    let selected = request("linear", Some(vec![id("linear", "BTCUSDT")]));
    super::stats_http(&client, &base, selected.clone(), StatusCode::BAD_GATEWAY).await;
    super::ws_error(
        &mut socket,
        super::stats_command("subscribe", &selected),
        "SUBSCRIBE_FAILED",
    )
    .await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request("linear", None)),
    )
    .await;
    let mut recovering = super::ws_initial(&mut socket).await;
    assert_eq!(recovering.coverage["enumerationComplete"], false);
    let counts = mock.calls();
    assert_eq!(counts[0], 1);
    assert_eq!(&counts[2..], &[0; 4]);
    tokio::time::advance(Duration::from_secs(29)).await;
    super::stats_http(&client, &base, selected, StatusCode::BAD_GATEWAY).await;
    assert_eq!(mock.calls(), counts);
    mock.catalog("linear", linear_catalog()).await;
    tokio::time::advance(Duration::from_millis(1_001)).await;
    super::ws_delta(&mut socket, &mut recovering).await;
    assert_eq!(recovering.coverage["enumerationComplete"], true);
    super::stats_http(
        &client,
        &base,
        request("linear", Some(vec![id("linear", "UNKNOWN")])),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.calls()[0], 2);
    super::ws_unsubscribe(&mut socket, &recovering.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn bybit_catalog_refresh_does_not_retimestamp_cached_ticker_bodies() {
    let manual = super::keep_time_manual();
    let (mock, upstream, stop, task) = server().await;
    let source = Arc::new(BybitExchange::with_base_url(upstream, 60_000).unwrap());
    source
        .fetch_markets(FetchMarketsParams {
            params: json!({"category":"linear"}),
            include_inactive: true,
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(29)).await;
    let first = source.fetch_market_stats(params("linear")).await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    mock.gates.write().await.insert(0, gate.clone());
    tokio::time::advance(Duration::from_millis(1_001)).await;
    let refreshing_source = source.clone();
    let refreshing = tokio::spawn(async move {
        refreshing_source
            .fetch_market_stats(params("linear"))
            .await
            .unwrap()
    });
    super::wait_count(&mock.counts[0], 2).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    mock.gates.write().await.remove(&0);
    gate.add_permits(1);
    let delayed = refreshing.await.unwrap();
    assert_eq!(delayed.received_at, first.received_at);
    assert_eq!(delayed.rows, first.rows);
    assert_eq!(mock.calls(), [2, 1, 0, 0, 0, 0]);
    // Ticker TTL is based on its own body receipt, not the later catalog completion.
    tokio::time::advance(Duration::from_secs(19)).await;
    let fresh = source.fetch_market_stats(params("linear")).await.unwrap();
    assert!(fresh.received_at > first.received_at);
    assert_eq!(mock.calls(), [2, 2, 0, 0, 0, 0]);
    stop.send(()).unwrap();
    task.await.unwrap();
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn bybit_malformed_ticker_batches_never_reassociate_or_clear_known_values() {
    let manual = super::keep_time_manual();
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BybitExchange::with_base_url(upstream, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let mut socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut socket,
        super::stats_command("subscribe", &request("linear", None)),
    )
    .await;
    let mut view = super::ws_initial(&mut socket).await;
    let mut malformed_list = envelope("linear", vec![], "");
    malformed_list["result"]["list"] = json!({"symbol":"BTCUSDT"});
    let mut duplicate = linear_tickers();
    let mut conflicting = duplicate[1].clone();
    conflicting["fundingRate"] = json!("99");
    duplicate.push(conflicting);
    for invalid in [
        malformed_list,
        envelope("linear", duplicate, ""),
        envelope("inverse", linear_tickers(), ""),
    ] {
        let previous = view.markets.clone();
        mock.ticker("linear", StatusCode::OK, invalid).await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        let delta = super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(delta["removedMarketIds"], json!([]));
        assert_eq!(view.coverage["enumerationComplete"], true);
        for (id, market) in &previous {
            for field in IMPLEMENTED {
                let retained = &view.markets[id]["fields"][field];
                assert_eq!(retained["state"], "stale");
                assert_eq!(retained["value"], market["fields"][field]["value"]);
                assert_eq!(
                    retained["receivedTimestamp"],
                    market["fields"][field]["receivedTimestamp"]
                );
            }
        }
        let snapshot =
            super::stats_http(&client, &base, request("linear", None), StatusCode::OK).await;
        assert!(failure(
            &snapshot,
            "bybit:linear:tickers",
            "invalid-upstream-data"
        ));
        view.assert_matches_snapshot(&snapshot);
        mock.ticker(
            "linear",
            StatusCode::OK,
            envelope("linear", linear_tickers(), ""),
        )
        .await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        super::ws_delta(&mut socket, &mut view).await;
        assert_eq!(
            view.markets[&id("linear", "BTCUSDT")]["fields"]["lastPrice"]["state"],
            "available"
        );
    }
    super::ws_unsubscribe(&mut socket, &view.topic).await;
    super::ws_disconnect(socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
    manual.abort();
}
