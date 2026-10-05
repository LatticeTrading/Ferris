//! Bybit stock statistics tests. Bulk `/v5/market/tickers` supplies funding,
//! mark/index, last price, 24h volumes and open interest for one category at a
//! time; `/v5/market/instruments-info` supplies the per-market funding interval.

use axum::{
    extract::State,
    http::{Request, StatusCode},
    response::Response,
    Router,
};
use ferris_market_data_backend::{
    exchanges::{
        ccxt::Venue,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::FetchMarketStatsParams,
};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

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
struct BybitMock {
    // (kind, category, status, baseCoin) -> reply.
    replies: Arc<RwLock<BTreeMap<(String, String, String, String), super::Reply>>>,
    counts: Arc<RwLock<BTreeMap<String, usize>>>,
}

impl BybitMock {
    fn set(
        &self,
        kind: &str,
        category: &str,
        status: &str,
        base_coin: &str,
        http: StatusCode,
        body: Value,
    ) {
        self.replies.write().insert(
            (
                kind.to_string(),
                category.to_string(),
                status.to_string(),
                base_coin.to_string(),
            ),
            super::Reply {
                status: http,
                body,
                barrier: None,
            },
        );
    }

    fn catalog(&self, category: &str, rows: Vec<Value>) {
        self.set(
            "instruments",
            category,
            "",
            "",
            StatusCode::OK,
            envelope(category, rows, ""),
        );
        // Linear/inverse market loading also requests the pre-launch list.
        self.set(
            "instruments",
            category,
            "PreLaunch",
            "",
            StatusCode::OK,
            envelope(category, Vec::new(), ""),
        );
    }

    fn catalog_coin(&self, category: &str, base_coin: &str, rows: Vec<Value>) {
        self.set(
            "instruments",
            category,
            "",
            base_coin,
            StatusCode::OK,
            envelope(category, rows, ""),
        );
    }

    fn tickers(&self, category: &str, base_coin: &str, rows: Vec<Value>) {
        self.set(
            "tickers",
            category,
            "",
            base_coin,
            StatusCode::OK,
            envelope(category, rows, ""),
        );
    }

    fn fail_tickers(&self, category: &str, base_coin: &str, http: StatusCode, body: Value) {
        self.set("tickers", category, "", base_coin, http, body);
    }

    fn calls(&self, kind: &str, category: &str) -> usize {
        self.counts
            .read()
            .get(&format!("{kind}:{category}"))
            .copied()
            .unwrap_or(0)
    }
}

fn instrument(category: &str, symbol: &str, base: &str, quote: &str, settle: &str) -> Value {
    json!({
        "symbol": symbol,
        "contractType": if category == "linear" { "LinearPerpetual" } else { "InversePerpetual" },
        "status": "Trading",
        "baseCoin": base,
        "quoteCoin": quote,
        "settleCoin": settle,
        "isPreListing": false,
        "fundingInterval": 480
    })
}

fn linear_catalog() -> Vec<Value> {
    let mut alias = instrument("linear", "BTC-ALT", "BTC", "USDT", "USDC");
    alias["fundingInterval"] = json!(60);
    let mut old = instrument("linear", "OLDUSDT", "OLD", "USDT", "USDT");
    old["status"] = json!("Settling");
    let mut pre = instrument("linear", "PREUSDT", "PRE", "USDT", "USDT");
    pre["isPreListing"] = json!(true);
    let mut mismatch = instrument("linear", "MISMATCH", "MM", "USDT", "USDT");
    mismatch["fundingInterval"] = json!(480);
    vec![
        instrument("linear", "BTCUSDT", "BTC", "USDT", "USDT"),
        alias,
        old,
        pre,
        mismatch,
    ]
}

fn inverse_catalog() -> Vec<Value> {
    vec![instrument("inverse", "BTCUSDT", "BTC", "USD", "BTC")]
}

fn spot_catalog() -> Vec<Value> {
    vec![json!({
        "symbol": "BTCUSDT",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "status": "Trading"
    })]
}

fn option_catalog() -> Vec<Value> {
    vec![json!({
        "symbol": "BTC-26DEC25-50000-C",
        "status": "Trading",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "optionsType": "Call",
        "deliveryTime": "1766793600000",
        "lotSizeFilter": {"maxOrderQty": "500", "minOrderQty": "0.01", "qtyStep": "0.01"},
        "priceFilter": {"minPrice": "5", "maxPrice": "10000000", "tickSize": "5"}
    })]
}

fn ticker(symbol: &str, rate: &str, mark: &str, index: &str, last: &str) -> Value {
    json!({
        "symbol": symbol,
        "fundingRate": rate,
        "markPrice": mark,
        "indexPrice": index,
        "lastPrice": last,
        "nextFundingTime": "4000000000000"
    })
}

fn linear_tickers() -> Vec<Value> {
    // Reverse order plus a shared display symbol proves identity joins.
    let alias = {
        let mut row = ticker("BTC-ALT", "0", "1.2500", "1.2", "1.2400");
        row["volume24h"] = json!("10.5");
        row["turnover24h"] = json!("13.125");
        row["openInterest"] = json!("1000.0");
        row
    };
    let mut btc = ticker(
        "BTCUSDT",
        "-0.00001250",
        "30000.000",
        "29999.9",
        "30001.2300",
    );
    btc["fundingIntervalHour"] = json!("8");
    btc["volume24h"] = json!("1234.5");
    btc["turnover24h"] = json!("37000000.50");
    btc["openInterest"] = json!("12345.678");
    let mut old = ticker("OLDUSDT", "0.01", "10", "10", "10");
    old["volume24h"] = json!("5");
    old["turnover24h"] = json!("50");
    old["openInterest"] = json!("7");
    let mut pre = ticker("PREUSDT", "0.02", "20", "20", "20");
    pre["volume24h"] = json!("1");
    pre["turnover24h"] = json!("20");
    pre["openInterest"] = json!("2");
    let mut mismatch = ticker("MISMATCH", "-0.00010000", "1.00", "1.00", "1.00");
    mismatch["fundingIntervalHour"] = json!("1");
    mismatch["volume24h"] = json!("1");
    mismatch["turnover24h"] = json!("1");
    mismatch["openInterest"] = json!("1");
    vec![alias, btc, old, pre, mismatch]
}

fn inverse_tickers() -> Vec<Value> {
    let mut btc = ticker("BTCUSDT", "0.00000125", "61000.001", "61000", "61001.000");
    // Inverse `volume24h` is the USD notional and `turnover24h` the base amount.
    btc["volume24h"] = json!("13713832");
    btc["turnover24h"] = json!("115.69");
    btc["openInterest"] = json!("373504107");
    vec![btc]
}

fn spot_tickers() -> Vec<Value> {
    vec![json!({
        "symbol": "BTCUSDT",
        "lastPrice": "30001.2300",
        "volume24h": "1234.5",
        "turnover24h": "37000000.50"
    })]
}

fn option_tickers() -> Vec<Value> {
    vec![json!({
        "symbol": "BTC-26DEC25-50000-C",
        "lastPrice": "1500",
        "markPrice": "1450.5",
        "indexPrice": "60000",
        "volume24h": "0.15",
        "turnover24h": "2482.73",
        "openInterest": "6.3"
    })]
}

fn envelope(category: &str, rows: Vec<Value>, cursor: &str) -> Value {
    json!({
        "retCode": 0,
        "retMsg": "OK",
        "time": 1700000000123u64,
        "result": {"category": category, "list": rows, "nextPageCursor": cursor}
    })
}

fn query(raw: Option<&str>) -> BTreeMap<String, String> {
    raw.unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect()
}

async fn handler(State(mock): State<BybitMock>, request: Request<axum::body::Body>) -> Response {
    let kind = match request.uri().path() {
        "/v5/market/instruments-info" => "instruments",
        "/v5/market/tickers" => "tickers",
        other => panic!("unexpected Bybit route {other}"),
    };
    let params = query(request.uri().query());
    assert!(
        !params.contains_key("symbol") && !params.contains_key("symbols"),
        "bulk acquisition must not filter symbols: {}",
        request.uri()
    );
    let category = params
        .get("category")
        .unwrap_or_else(|| panic!("category is required: {params:?}"))
        .clone();
    let status = params.get("status").cloned().unwrap_or_default();
    // The stock option loader defaults to BTC when no base coin is supplied.
    let base_coin = match params.get("baseCoin").cloned() {
        Some(coin) => coin,
        None if kind == "tickers" && category == "option" => "BTC".to_string(),
        None => String::new(),
    };
    let reply = mock
        .replies
        .read()
        .get(&(
            kind.to_string(),
            category.clone(),
            status.clone(),
            base_coin.clone(),
        ))
        .unwrap_or_else(|| {
            panic!("unexpected {kind} request: category={category} status={status} baseCoin={base_coin}")
        })
        .clone();
    *mock
        .counts
        .write()
        .entry(format!("{kind}:{category}"))
        .or_default() += 1;
    Response::builder()
        .status(reply.status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(reply.body.to_string()))
        .unwrap()
}

async fn server() -> (BybitMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = BybitMock {
        replies: Arc::new(RwLock::new(BTreeMap::new())),
        counts: Arc::new(RwLock::new(BTreeMap::new())),
    };
    mock.catalog("linear", linear_catalog());
    mock.catalog("inverse", inverse_catalog());
    mock.catalog("spot", spot_catalog());
    // The stock option loader groups by base coin; only BTC has markets here.
    mock.catalog_coin("option", "BTC", option_catalog());
    for coin in ["ETH", "SOL", "XRP", "MNT", "DOGE"] {
        mock.catalog_coin("option", coin, Vec::new());
    }
    mock.tickers("linear", "", linear_tickers());
    mock.tickers("inverse", "", inverse_tickers());
    mock.tickers("spot", "", spot_tickers());
    mock.tickers("option", "BTC", option_tickers());
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
        params: json!({"category": category}),
        open_interest_market_ids: Vec::new(),
        include_bulk: true,
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

fn number(value: &Value) -> f64 {
    value.as_f64().expect("numeric metric member")
}

#[tokio::test]
async fn bybit_linear_inverse_identity_volumes_and_open_interest_units() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);

    let linear = source.fetch_market_stats(params("linear")).await.unwrap();
    let rows = serde_json::to_value(&linear.rows).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 5);
    let btc = row(&rows, &id("linear", "BTCUSDT"));
    let alias = row(&rows, &id("linear", "BTC-ALT"));
    // Distinct markets can share a display symbol; identity keeps them apart.
    assert_eq!(btc["symbol"], alias["symbol"]);
    assert_eq!(btc["exchangeMarketId"], "BTCUSDT");
    assert_eq!(btc["category"], "linear");
    assert_eq!(btc["settle"], "USDT");
    assert_eq!(btc["settlementAssetId"], "USDT");
    assert_eq!(alias["settle"], "USDC");
    assert_eq!(alias["settlementAssetId"], "USDC");

    assert_eq!(btc["fields"]["funding"]["value"]["rate"], "-0.00001250");
    assert_eq!(
        btc["fields"]["funding"]["value"]["rateUnit"],
        "decimalFraction"
    );
    assert_eq!(btc["fields"]["funding"]["value"]["kind"], "estimate");
    // The ticker's own hourly interval agrees with the catalog's 8h default.
    assert_eq!(
        btc["fields"]["funding"]["value"]["rateIntervalMs"],
        28_800_000u64
    );
    assert_eq!(
        btc["fields"]["funding"]["value"]["paymentIntervalMs"],
        28_800_000u64
    );
    // Without a ticker interval the catalog's minutes are the only basis.
    assert_eq!(
        alias["fields"]["funding"]["value"]["rateIntervalMs"],
        3_600_000u64
    );
    assert_eq!(alias["fields"]["funding"]["value"]["rate"], "0");
    assert_eq!(btc["fields"]["markPrice"]["value"]["amount"], "30000.000");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "29999.9");
    assert_eq!(btc["fields"]["lastPrice"]["value"]["amount"], "30001.2300");

    // Linear volumes keep their own currency; open interest amount is base.
    let volume = &btc["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 1234.5);
    assert_eq!(number(&volume["quoteVolume"]), 37_000_000.50);
    let interest = &btc["fields"]["openInterest"]["value"];
    assert_eq!(number(&interest["openInterestAmount"]), 12345.678);
    assert_eq!(interest["openInterestValue"], Value::Null);

    // Inverse turnover is in base coins; volume24h is in the USD quote.
    let inverse = source.fetch_market_stats(params("inverse")).await.unwrap();
    let rows = serde_json::to_value(&inverse.rows).unwrap();
    let inverse_btc = row(&rows, &id("inverse", "BTCUSDT"));
    assert_eq!(inverse_btc["exchangeMarketId"], "BTCUSDT");
    assert_eq!(inverse_btc["category"], "inverse");
    assert_eq!(inverse_btc["settle"], "BTC");
    let volume = &inverse_btc["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 115.69);
    assert_eq!(number(&volume["quoteVolume"]), 13_713_832.0);
    // Inverse open interest has no base amount; the raw figure is the USD value.
    let interest = &inverse_btc["fields"]["openInterest"]["value"];
    assert_eq!(interest["openInterestAmount"], Value::Null);
    assert_eq!(number(&interest["openInterestValue"]), 373_504_107.0);

    // One bulk ticker call per category; open interest is never singular.
    assert_eq!(mock.calls("tickers", "linear"), 1);
    assert_eq!(mock.calls("tickers", "inverse"), 1);

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn bybit_funding_interval_disagreement_is_recorded_without_losing_the_rate() {
    let (_mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(params("linear")).await.unwrap();
    assert!(snapshot
        .source_failures
        .iter()
        .any(|failure| failure.source == "bybit:ccxt:fetchTickers"
            && failure.reason == "funding-interval-mismatch"));
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let mismatch = row(&rows, &id("linear", "MISMATCH"));
    assert_eq!(
        mismatch["fields"]["funding"]["value"]["rate"],
        "-0.00010000"
    );
    assert_eq!(mismatch["fields"]["funding"]["state"], "available");
    // The ticker's own interval still wins over the disagreeing catalog value.
    assert_eq!(
        mismatch["fields"]["funding"]["value"]["rateIntervalMs"],
        3_600_000u64
    );

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn bybit_spot_product_reports_only_prices_and_volume() {
    let (_mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(params("spot")).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let spot = row(
        &rows,
        &json!(["bybit", "spot", "spot", null, "BTCUSDT"]).to_string(),
    );
    assert_eq!(spot["symbol"], "BTC/USDT");
    assert_eq!(spot["settle"], Value::Null);
    assert_eq!(spot["settlementAssetId"], Value::Null);
    assert_eq!(spot["category"], "spot");
    assert_eq!(spot["fields"]["lastPrice"]["value"]["amount"], "30001.2300");
    let volume = &spot["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 1234.5);
    assert_eq!(number(&volume["quoteVolume"]), 37_000_000.50);
    for field in [
        "funding",
        "markPrice",
        "indexPrice",
        "openInterest",
        "lastSettledFunding",
    ] {
        assert_eq!(
            spot["fields"][field]["state"], "notApplicable",
            "spot {field}"
        );
        assert_eq!(spot["fields"][field]["value"], Value::Null);
    }

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn bybit_option_product_reports_prices_volume_and_amount_interest() {
    let (_mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(params("option")).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let option = row(
        &rows,
        &json!(["bybit", "option", "option", null, "BTC-26DEC25-50000-C"]).to_string(),
    );
    assert_eq!(option["category"], "option");
    assert_eq!(option["settle"], "USDT");
    assert_eq!(option["fields"]["lastPrice"]["value"]["amount"], "1500");
    assert_eq!(option["fields"]["markPrice"]["value"]["amount"], "1450.5");
    assert_eq!(option["fields"]["indexPrice"]["value"]["amount"], "60000");
    let volume = &option["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 0.15);
    assert_eq!(number(&volume["quoteVolume"]), 2482.73);
    // Options carry no funding schedule, but they do carry an open interest amount.
    assert_eq!(option["fields"]["funding"]["state"], "notApplicable");
    assert_eq!(
        option["fields"]["lastSettledFunding"]["state"],
        "notApplicable"
    );
    let interest = &option["fields"]["openInterest"]["value"];
    assert_eq!(number(&interest["openInterestAmount"]), 6.3);
    assert_eq!(interest["openInterestValue"], Value::Null);

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn bybit_http_ws_share_category_acquisition_and_revise_deltas() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    let btc = id("linear", "BTCUSDT");
    let all_request = request("linear", None);
    let selected_request = request("linear", Some(vec![btc.clone()]));
    let mut all_socket = super::stats_socket(&base).await;
    let mut selected_socket = super::stats_socket(&base).await;
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
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    // The all-markets and selected projections share one category acquisition.
    assert_eq!(mock.calls("tickers", "linear"), 1);
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(all.coverage["expectedMarkets"], 3);
    super::assert_ws_matches_http(&client, &base, &all).await;

    // A revised bulk row produces a revisioned delta for both subscribers.
    let mut rows = linear_tickers();
    rows[1]["openInterest"] = json!("500.5");
    rows[1]["markPrice"] = json!("30500.000");
    mock.tickers("linear", "", rows);
    tokio::time::sleep(Duration::from_secs(31)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let delta = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(delta["revision"], 2);
    assert_eq!(delta["previousRevision"], 1);
    assert_eq!(
        number(&selected.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"]),
        500.5
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["value"]["amount"],
        "30500.000"
    );

    // A failed bulk call keeps the last observation without inventing a removal.
    mock.fail_tickers(
        "linear",
        "",
        StatusCode::BAD_GATEWAY,
        json!({"offline":true}),
    );
    tokio::time::sleep(Duration::from_secs(31)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    let interest = &selected.markets[&btc]["fields"]["openInterest"];
    assert_eq!(interest["state"], "stale");
    assert_eq!(number(&interest["value"]["openInterestAmount"]), 500.5);
    super::assert_ws_matches_http(&client, &base, &selected).await;

    super::ws_unsubscribe(&mut selected_socket, &selected.topic).await;
    super::ws_unsubscribe(&mut all_socket, &all.topic).await;
    super::ws_disconnect(selected_socket).await;
    super::ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn bybit_unknown_selection_is_rejected_from_the_complete_catalog() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Bybit, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    // Invalid topics never reach acquisition.
    for invalid in [
        json!({"exchange":"bybit","params":{"category":"linear","symbol":"BTCUSDT"}}),
        json!({"exchange":"bybit","params":{"category":"futures"}}),
        json!({"exchange":"bybit","params":{"category":null}}),
        json!({"exchange":"bybit","params":[]}),
        request("linear", Some(vec![id("inverse", "BTCUSDT")])),
        request("linear", Some(vec![])),
        request("linear", Some(vec![id("linear", "BTCUSDT"); 101])),
    ] {
        super::stats_http(&client, &base, invalid, StatusCode::BAD_REQUEST).await;
    }
    assert_eq!(mock.calls("tickers", "linear"), 0);

    // A complete catalog turns an unknown identity into a client error, not I/O.
    super::stats_http(&client, &base, request("linear", None), StatusCode::OK).await;
    super::stats_http(
        &client,
        &base,
        request("linear", Some(vec![id("linear", "UNKNOWN")])),
        StatusCode::BAD_REQUEST,
    )
    .await;

    // A cold catalog failure is not an authoritative empty catalog.
    mock.set(
        "instruments",
        "inverse",
        "",
        "",
        StatusCode::OK,
        json!({"retCode":10001,"retMsg":"offline"}),
    );
    let cold = super::stats_http(&client, &base, request("inverse", None), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);

    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}
