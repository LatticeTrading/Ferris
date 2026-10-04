//! Aster stock statistics tests. The swap profile acquires the futures catalog
//! plus `/fapi/v3/ticker/24hr`, `/fapi/v3/premiumIndex` and
//! `/fapi/v3/fundingInfo`; open interest has no stock method at all.

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

// fapi exchangeInfo, sapi exchangeInfo, fapi tickers, sapi tickers,
// fapi premiumIndex, fapi fundingInfo.
const ENDPOINTS: [&str; 6] = [
    "/fapi/v3/exchangeInfo",
    "/api/v3/exchangeInfo",
    "/fapi/v3/ticker/24hr",
    "/api/v3/ticker/24hr",
    "/fapi/v3/premiumIndex",
    "/fapi/v3/fundingInfo",
];

#[derive(Clone)]
struct AsterMock {
    replies: Arc<[RwLock<super::Reply>; 6]>,
    counts: Arc<[AtomicUsize; 6]>,
}

fn reply(status: StatusCode, body: Value) -> super::Reply {
    super::Reply {
        status,
        body,
        barrier: None,
    }
}

impl AsterMock {
    async fn set(&self, endpoint: usize, status: StatusCode, body: Value) {
        *self.replies[endpoint].write().await = reply(status, body);
    }

    async fn catalog(&self, rows: Vec<Value>) {
        self.set(0, StatusCode::OK, json!({"symbols":rows})).await;
    }

    fn calls(&self) -> [usize; 6] {
        self.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst))
    }
}

fn instrument(symbol: &str, base: &str, quote: &str, margin: &str, contract: &str) -> Value {
    json!({
        "symbol": symbol,
        "contractType": contract,
        "status": "TRADING",
        "baseAsset": base,
        "quoteAsset": quote,
        "marginAsset": margin,
        "pair": format!("{base}{quote}")
    })
}

fn swap(symbol: &str, base: &str, quote: &str, margin: &str) -> Value {
    instrument(symbol, base, quote, margin, "PERPETUAL")
}

fn catalog() -> Vec<Value> {
    vec![
        swap("BTCUSDT", "BTC", "USDT", "USDT"),
        swap("ALTUSD1", "ALT", "USD1", "USD1"),
        {
            let mut old = swap("OLDUSDT", "OLD", "USDT", "USDT");
            old["status"] = json!("SETTLING");
            old
        },
        {
            // Missing margin asset: settlement cannot be resolved.
            let mut unresolved = swap("UNKNOWNMARGIN", "RAW", "USDT", "USDT");
            unresolved.as_object_mut().unwrap().remove("marginAsset");
            unresolved
        },
    ]
}

fn ticker(symbol: &str, last: &str, volume: &str, quote_volume: &str) -> Value {
    json!({
        "symbol": symbol,
        "lastPrice": last,
        "volume": volume,
        "quoteVolume": quote_volume,
        "openPrice": "1",
        "priceChange": "0.1",
        "priceChangePercent": "0.279",
        "highPrice": "2",
        "lowPrice": "0.5",
        "closeTime": 1700000000123u64
    })
}

fn tickers() -> Value {
    json!([
        ticker("ALTUSD1", "1.2500", "1500.5", "1875.625"),
        ticker("BTCUSDT", "30000.000", "1234.5", "37000000.50"),
        ticker("OLDUSDT", "10", "5", "50"),
        ticker("UNKNOWNMARGIN", "5.00", "10", "50")
    ])
}

fn premium(symbol: &str, rate: &str, mark: &str, index: &str) -> Value {
    json!({
        "symbol": symbol,
        "lastFundingRate": rate,
        "markPrice": mark,
        "indexPrice": index,
        "nextFundingTime": 1700028800000u64,
        "time": 1700000000123u64
    })
}

fn premium_index() -> Value {
    json!([
        premium("ALTUSD1", "0", "1.2500", "1.200"),
        premium("BTCUSDT", "-0.00001250", "30000.000", "29999.900"),
        premium("OLDUSDT", "0.0100", "10.00", "10.00"),
        premium("UNKNOWNMARGIN", "0.0100", "5.00", "5.01")
    ])
}

fn funding_intervals() -> Value {
    json!([
        {"symbol": "BTCUSDT", "fundingIntervalHours": 8},
        {"symbol": "ALTUSD1", "fundingIntervalHours": 1},
        {"symbol": "OLDUSDT", "fundingIntervalHours": 4}
    ])
}

async fn handler(State(mock): State<AsterMock>, request: Request<axum::body::Body>) -> Response {
    // Bulk acquisition may carry a product selector, never a symbol filter.
    let query = query(request.uri().query());
    assert!(
        !query.contains_key("symbol") && !query.contains_key("symbols"),
        "bulk acquisition must not filter symbols: {}",
        request.uri()
    );
    let index = ENDPOINTS
        .iter()
        .position(|path| *path == request.uri().path())
        .unwrap_or_else(|| panic!("unexpected Aster route {}", request.uri()));
    let reply = mock.replies[index].read().await.clone();
    mock.counts[index].fetch_add(1, Ordering::SeqCst);
    Response::builder()
        .status(reply.status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(reply.body.to_string()))
        .unwrap()
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

async fn server() -> (AsterMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = AsterMock {
        replies: Arc::new([
            RwLock::new(reply(StatusCode::OK, json!({"symbols":catalog()}))),
            RwLock::new(reply(
                StatusCode::OK,
                json!({"symbols":[], "assets":[{"asset":"USDT","marginAvailable":true}]}),
            )),
            RwLock::new(reply(StatusCode::OK, tickers())),
            RwLock::new(reply(StatusCode::OK, json!([]))),
            RwLock::new(reply(StatusCode::OK, premium_index())),
            RwLock::new(reply(StatusCode::OK, funding_intervals())),
        ]),
        counts: Arc::new(Default::default()),
    };
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

fn id(symbol: &str) -> String {
    json!(["aster", "perp", null, null, symbol]).to_string()
}

fn params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: json!({}),
        open_interest_market_ids: Vec::new(),
        include_bulk: true,
    }
}

fn request(ids: Option<Vec<String>>) -> Value {
    json!({"exchange":"aster", "params":{}, "marketIds":ids, "fields":FIELDS})
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
async fn aster_swap_identity_volumes_funding_and_absent_open_interest() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Aster, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(params()).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();

    // Stock Aster lists spot and perpetuals; inactive perpetuals remain visible.
    let btc = row(&rows, &id("BTCUSDT"));
    assert_eq!(btc["symbol"], "BTC/USDT");
    assert_eq!(btc["exchangeMarketId"], "BTCUSDT");
    assert_eq!(btc["settle"], "USDT");
    assert_eq!(btc["settlementAssetId"], "USDT");
    assert_eq!(btc["category"], Value::Null);
    assert_eq!(btc["active"], true);
    let alt = row(&rows, &id("ALTUSD1"));
    assert_eq!(alt["settle"], "USD1");
    if let Some(old) = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["exchangeMarketId"] == "OLDUSDT")
    {
        assert_eq!(old["active"], false);
    }
    // A missing margin asset is a truthful settlement gap, not a guessed currency.
    if let Some(unresolved) = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["exchangeMarketId"] == "UNKNOWNMARGIN")
    {
        assert_eq!(unresolved["settle"], Value::Null);
        assert_eq!(unresolved["settlementAssetId"], Value::Null);
    }

    // Lexical precision is preserved for funding and prices.
    let funding = &btc["fields"]["funding"]["value"];
    assert_eq!(funding["rate"], "-0.00001250");
    assert_eq!(funding["rateUnit"], "decimalFraction");
    assert_eq!(funding["kind"], "estimate");
    assert_eq!(funding["rateIntervalMs"], 28_800_000u64);
    assert_eq!(funding["paymentIntervalMs"], 28_800_000u64);
    assert_eq!(funding["nextPaymentTimestamp"], 1700028800000u64);
    assert_eq!(btc["fields"]["markPrice"]["value"]["amount"], "30000.000");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "29999.900");
    assert_eq!(btc["fields"]["lastPrice"]["value"]["amount"], "30000.000");
    // Per-market intervals are independent.
    assert_eq!(
        alt["fields"]["funding"]["value"]["rateIntervalMs"],
        3_600_000u64
    );

    // volume24h is numeric and keeps both currencies.
    let volume = &btc["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 1234.5);
    assert_eq!(number(&volume["quoteVolume"]), 37_000_000.50);

    // Aster exposes no stock open interest method.
    for (market, reason) in [
        (&btc, "stock-method-not-supported"),
        (&alt, "stock-method-not-supported"),
    ] {
        let interest = &market["fields"]["openInterest"];
        assert_eq!(interest["state"], "unsupported");
        assert_eq!(interest["reason"], reason);
        assert_eq!(interest["value"], Value::Null);
    }

    // Every qualified source was actually exercised.
    let calls = mock.calls();
    assert!(calls[0] >= 1 && calls[2] >= 1 && calls[4] >= 1 && calls[5] >= 1);

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn aster_http_ws_share_acquisition_and_revise_deltas() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Aster, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    let btc = id("BTCUSDT");
    let all_request = request(None);
    let selected_request = request(Some(vec![btc.clone()]));
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
    // One acquisition serves both projections.
    assert_eq!(all.markets[&btc], selected.markets[&btc]);
    assert_eq!(all.coverage["expectedMarkets"], 3);
    let calls = mock.calls();
    assert!(calls[0] >= 1 && calls[2] >= 1 && calls[4] >= 1 && calls[5] >= 1);
    super::assert_ws_matches_http(&client, &base, &all).await;

    // A revised bulk row produces a revisioned delta.
    let mut rows = premium_index();
    rows[1]["markPrice"] = json!("30500.000");
    rows[1]["lastFundingRate"] = json!("-0.00002000");
    mock.set(4, StatusCode::OK, rows).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let delta = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(delta["revision"], 2);
    assert_eq!(delta["previousRevision"], 1);
    assert_eq!(
        selected.markets[&btc]["fields"]["markPrice"]["value"]["amount"],
        "30500.000"
    );
    assert_eq!(
        selected.markets[&btc]["fields"]["funding"]["value"]["rate"],
        "-0.00002000"
    );

    // A failed bulk call keeps the last observation without inventing a removal.
    mock.set(4, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    let mark = &selected.markets[&btc]["fields"]["markPrice"];
    assert_eq!(mark["state"], "stale");
    assert_eq!(mark["value"]["amount"], "30500.000");
    super::assert_ws_matches_http(&client, &base, &selected).await;

    // An authoritative catalog removes a delisted market from the all view.
    let mut catalog = catalog();
    catalog[0]["status"] = json!("SETTLING");
    mock.catalog(catalog).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let removed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));

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
async fn aster_unknown_selection_rejected_from_complete_catalog() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Aster, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    // Malformed topics are rejected before any acquisition.
    for invalid in [
        json!({"exchange":"aster","params":{"dex":""}}),
        json!({"exchange":"aster","marketIds":[]}),
        json!({"exchange":"aster","fields":[]}),
    ] {
        super::stats_http(&client, &base, invalid, StatusCode::BAD_REQUEST).await;
    }
    assert_eq!(mock.calls(), [0; 6]);

    // A complete catalog turns unknown and unqualified selections into client
    // errors rather than upstream requests.
    super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("UNKNOWN")])),
        StatusCode::BAD_REQUEST,
    )
    .await;

    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn aster_cold_catalog_failure_is_not_an_empty_catalog() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    mock.set(0, StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Aster, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    let cold = super::stats_http(&client, &base, request(None), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);

    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}
