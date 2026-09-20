use axum::{
    extract::State,
    http::{Request, StatusCode},
    response::Response,
    Router,
};
use ferris_market_data_backend::{
    exchanges::{
        binance::BinanceExchange,
        traits::{MarketDataExchange, MarketStatsSource},
    },
    models::{
        FetchMarketStatsParams, FetchMarketsParams, MarketStatsFieldName, MarketStatsFieldState,
        MarketStatsValue,
    },
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
    net::TcpListener,
    sync::{oneshot, RwLock, Semaphore},
    task::JoinHandle,
};

#[derive(Clone)]
struct BinanceMock {
    info: Arc<RwLock<(StatusCode, Value)>>,
    premium: Arc<RwLock<(StatusCode, Value)>>,
    funding: Arc<RwLock<(StatusCode, Value)>>,
    counts: Arc<[AtomicUsize; 3]>,
    info_gate: Arc<RwLock<Option<Arc<Semaphore>>>>,
}
fn info() -> Value {
    json!({"symbols":[
     {"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT"},
     {"symbol":"BTCUSDC","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDC","marginAsset":"USDC"},
     {"symbol":"Ω-BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"Ω-BTC","quoteAsset":"USDT","marginAsset":"USDT"},
     {"symbol":"OLDUSDT","contractType":"PERPETUAL","status":"SETTLING","baseAsset":"OLD","quoteAsset":"USDT","marginAsset":"USDT"},
     {"symbol":"FUTUSDT","contractType":"CURRENT_QUARTER","status":"TRADING","baseAsset":"FUT","quoteAsset":"USDT","marginAsset":"USDT"}
    ]})
}
fn premium() -> Value {
    json!([
        {"symbol":"BTCUSDC","markPrice":"30000.000","indexPrice":"29999.9","lastFundingRate":"-0.00001250","nextFundingTime":1700000000000i64,"time":1700000000123i64},
        {"symbol":"ETHUSDT","markPrice":"2000.1","indexPrice":"2000","lastFundingRate":"0","nextFundingTime":0i64,"time":1700000000124i64},
        {"symbol":"Ω-BTCUSDT","markPrice":"1.25","indexPrice":"1.2","lastFundingRate":"0.00000000","nextFundingTime":0i64,"time":1700000000125i64}
    ])
}

fn funding() -> Value {
    json!([
        {"symbol":"BTCUSDC","fundingIntervalHours":8i64},
        {"symbol":"ETHUSDT","fundingIntervalHours":0i64},
        {"symbol":"Ω-BTCUSDT","fundingIntervalHours":9223372036854775807i64}
    ])
}
async fn handler(State(m): State<BinanceMock>, req: Request<axum::body::Body>) -> Response {
    assert!(
        req.uri().query().is_none_or(str::is_empty),
        "unexpected symbol/query fan-out: {}",
        req.uri()
    );
    let (index, slot) = match req.uri().path() {
        "/fapi/v1/exchangeInfo" => (0, &m.info),
        "/fapi/v1/premiumIndex" => (1, &m.premium),
        "/fapi/v1/fundingInfo" => (2, &m.funding),
        path => panic!("unexpected Binance endpoint {path}"),
    };
    m.counts[index].fetch_add(1, Ordering::SeqCst);
    if index == 0 {
        if let Some(gate) = m.info_gate.read().await.clone() {
            gate.acquire().await.unwrap().forget();
        }
    }
    let (status, body) = slot.read().await.clone();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}
async fn server() -> (BinanceMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = BinanceMock {
        info: Arc::new(RwLock::new((StatusCode::OK, info()))),
        premium: Arc::new(RwLock::new((StatusCode::OK, premium()))),
        funding: Arc::new(RwLock::new((StatusCode::OK, funding()))),
        counts: Arc::new(Default::default()),
        info_gate: Arc::new(RwLock::new(None)),
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
fn params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: Value::Null,
    }
}

#[tokio::test]
async fn binance_source_catalog_identity_metadata_and_intervals() {
    let (m, url, stop, task) = server().await;
    let source = Arc::new(BinanceExchange::with_base_url(url, 2_000).unwrap());
    let snap = source.fetch_market_stats(params()).await.unwrap();
    assert_eq!(snap.rows.len(), 4);
    let btc = snap
        .rows
        .iter()
        .find(|r| r.market.symbol == "BTC/USDC")
        .unwrap();
    let identity = btc.market.identity.as_ref().unwrap();
    assert_eq!(
        identity.market_id,
        "[\"binance\",\"perp\",null,null,\"BTCUSDC\"]"
    );
    assert_eq!(identity.settle.as_deref(), Some("USDC"));
    assert_eq!(identity.category, None);
    assert_eq!(identity.dex, None);
    assert_eq!(
        btc.fields[&MarketStatsFieldName::MarkPrice]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Price(p) => Some(p.amount.as_str()),
                _ => None,
            }),
        Some("30000.000")
    );
    assert_eq!(
        btc.fields[&MarketStatsFieldName::IndexPrice]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Price(p) => Some(p.amount.as_str()),
                _ => None,
            }),
        Some("29999.9")
    );
    assert_eq!(
        btc.fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Funding(f) => Some((f.rate.as_str(), f.payment_interval_ms)),
                _ => None,
            }),
        Some(("-0.00001250", Some(28_800_000)))
    );
    let eth = snap
        .rows
        .iter()
        .find(|r| r.market.symbol == "ETH/USDT")
        .unwrap();
    assert_eq!(
        eth.fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Funding(f) => Some((f.rate.as_str(), f.payment_interval_ms)),
                _ => None,
            }),
        Some(("0", None))
    );
    let omega = snap
        .rows
        .iter()
        .find(|r| r.market.symbol.contains("Ω-BTC"))
        .unwrap();
    assert_eq!(
        omega.market.identity.as_ref().unwrap().exchange_market_id,
        "Ω-BTCUSDT"
    );
    assert_eq!(
        omega.market.identity.as_ref().unwrap().market_id,
        "[\"binance\",\"perp\",null,null,\"Ω-BTCUSDT\"]"
    );
    assert_eq!(
        omega.fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Funding(f) => Some(f.payment_interval_ms),
                _ => None,
            }),
        Some(None)
    );
    let active = source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: false,
        })
        .await
        .unwrap();
    assert!(active.iter().any(|x| x.symbol == "FUT/USDT"));
    assert!(!active.iter().any(|x| x.symbol == "OLD/USDT"));
    let all = source
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true,
        })
        .await
        .unwrap();
    assert!(all.iter().any(|x| x.symbol == "OLD/USDT" && !x.active));
    assert!(all
        .iter()
        .any(|market| market.identity.as_ref() == btc.market.identity.as_ref()));
    *m.premium.write().await = (StatusCode::OK, premium()[0].clone());
    tokio::time::pause();
    let manual = super::keep_time_manual();
    tokio::time::advance(Duration::from_secs(30)).await;
    let single = source.fetch_market_stats(params()).await.unwrap();
    let current = single
        .rows
        .iter()
        .find(|row| row.market.symbol == "BTC/USDC")
        .unwrap();
    assert_eq!(
        current.fields[&MarketStatsFieldName::Funding].value,
        btc.fields[&MarketStatsFieldName::Funding].value
    );
    assert!(single.perp_enumeration_complete);
    assert_eq!(
        single
            .rows
            .iter()
            .find(|row| row.market.symbol == "ETH/USDT")
            .unwrap()
            .fields[&MarketStatsFieldName::Funding]
            .reason
            .as_deref(),
        Some("missing-upstream-row")
    );
    tokio::time::resume();
    manual.abort();
    stop.send(()).unwrap();
    task.await.unwrap();
    drop(m);
}

#[tokio::test]
async fn binance_funding_info_outage_retains_native_rate_without_interval() {
    let (m, url, stop, task) = server().await;
    let source = Arc::new(BinanceExchange::with_base_url(url, 2_000).unwrap());
    let baseline = source.fetch_market_stats(params()).await.unwrap();
    *m.funding.write().await = (StatusCode::BAD_GATEWAY, json!({"offline":true}));
    tokio::time::pause();
    let manual = super::keep_time_manual();
    tokio::time::advance(std::time::Duration::from_secs(300)).await;
    let failed = source.fetch_market_stats(params()).await.unwrap();
    let btc = failed
        .rows
        .iter()
        .find(|r| r.market.symbol == "BTC/USDC")
        .unwrap();
    assert_eq!(
        btc.fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Available
    );
    assert_eq!(
        btc.fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .and_then(|v| match v {
                MarketStatsValue::Funding(f) => Some((f.rate.as_str(), f.payment_interval_ms)),
                _ => None,
            }),
        Some(("-0.00001250", None))
    );
    assert!(failed
        .source_failures
        .iter()
        .any(|f| f.source == "binance:fundingInfo"));
    assert_eq!(baseline.rows.len(), failed.rows.len());
    *m.funding.write().await = (
        StatusCode::OK,
        json!([
            {"symbol":"BTCUSDC","fundingIntervalHours":4},
            {"symbol":"BTCUSDC","fundingIntervalHours":0}
        ]),
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    let ambiguous = source.fetch_market_stats(params()).await.unwrap();
    assert!(ambiguous
        .source_failures
        .iter()
        .any(|failure| failure.source == "binance:fundingInfo"
            && failure.reason == "invalid-upstream-data"));
    assert!(
        matches!(&ambiguous.rows.iter().find(|row| row.market.symbol == "BTC/USDC").unwrap()
        .fields[&MarketStatsFieldName::Funding].value, Some(MarketStatsValue::Funding(value))
        if value.rate == "-0.00001250" && value.payment_interval_ms.is_none())
    );
    *m.funding.write().await = (
        StatusCode::OK,
        json!([{ "symbol":"BTCUSDC", "fundingIntervalHours":4 }]),
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    let recovered = source.fetch_market_stats(params()).await.unwrap();
    assert!(
        matches!(&recovered.rows.iter().find(|row| row.market.symbol == "BTC/USDC").unwrap()
        .fields[&MarketStatsFieldName::Funding].value, Some(MarketStatsValue::Funding(value))
        if value.rate == "-0.00001250" && value.payment_interval_ms == Some(14_400_000))
    );
    tokio::time::resume();
    manual.abort();
    stop.send(()).unwrap();
    task.await.unwrap();
    drop(m);
}

#[tokio::test]
async fn binance_http_ws_share_acquisition_and_preserve_failure_transitions() {
    let (m, url, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BinanceExchange::with_base_url(url, 2_000).unwrap());
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
    let capability = &caps["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["exchange"] == "binance")
        .unwrap()["marketStats"];
    assert_eq!(capability["state"], "supported");
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [0, 0, 0]
    );

    let btc = "[\"binance\",\"perp\",null,null,\"BTCUSDC\"]";
    let all_request = json!({"exchange":"binance","fields":["funding","markPrice","openInterest"]});
    let selected_request = json!({"exchange":"binance","marketIds":[btc],"fields":["funding","markPrice","openInterest"]});
    let gate = Arc::new(Semaphore::new(0));
    *m.info_gate.write().await = Some(gate.clone());
    let mut requests = Vec::new();
    for index in 0..12 {
        let (client, base) = (client.clone(), base.clone());
        let request = if index % 2 == 0 {
            all_request.clone()
        } else {
            selected_request.clone()
        };
        requests.push(tokio::spawn(async move {
            super::stats_http(&client, &base, request, StatusCode::OK).await
        }));
    }
    let (catalog_client, catalog_base) = (client.clone(), base.clone());
    let catalog = tokio::spawn(async move {
        let response = catalog_client
            .post(format!("{catalog_base}/v1/fetchMarkets"))
            .json(&json!({"exchange":"binance","includeInactive":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.json::<Value>().await.unwrap()
    });
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
    for count in m.counts.iter() {
        super::wait_count(count, 1).await;
    }
    gate.add_permits(1);
    let mut all = super::ws_initial(&mut all_socket).await;
    let mut selected = super::ws_initial(&mut selected_socket).await;
    *m.info_gate.write().await = None;
    let catalog = catalog.await.unwrap();
    let catalog_btc = catalog["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|market| market["marketId"] == btc)
        .unwrap();
    assert_eq!(catalog_btc["settle"], "USDC");
    assert_eq!(all.markets[btc], selected.markets[btc]);
    assert_eq!(all.coverage["expectedMarkets"], 3);
    assert_eq!(
        all.markets[btc]["fields"]["openInterest"]["state"],
        "unsupported"
    );
    for (index, request) in requests.into_iter().enumerate() {
        let snapshot = request.await.unwrap();
        if index % 2 == 0 {
            all.assert_matches_snapshot(&snapshot);
        } else {
            selected.assert_matches_snapshot(&snapshot);
        }
    }
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [1, 1, 1]
    );
    tokio::time::pause();
    let manual = super::keep_time_manual();

    // Wrong scalar types force the SDK raw-JSON variant; valid sibling prices survive.
    let mut changed = premium();
    changed[0]["lastFundingRate"] = json!(12);
    changed[0]["markPrice"] = json!("31000.000");
    *m.premium.write().await = (StatusCode::OK, changed);
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    let cleared = selected.markets[btc]["fields"]["funding"].clone();
    let last_mark = selected.markets[btc]["fields"]["markPrice"].clone();
    assert_eq!(cleared["state"], "unavailable");
    assert_eq!(cleared["value"], Value::Null);
    assert_eq!(last_mark["value"]["amount"], "31000.000");
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [2, 2, 1]
    );

    *m.premium.write().await = (StatusCode::BAD_GATEWAY, json!({"offline":true}));
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let failed = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(failed["removedMarketIds"], json!([]));
    assert_eq!(all.coverage["enumerationComplete"], true);
    assert_eq!(selected.markets[btc]["fields"]["funding"], cleared);
    let stale_mark = &selected.markets[btc]["fields"]["markPrice"];
    assert_eq!(stale_mark["state"], "stale");
    assert_eq!(stale_mark["value"], last_mark["value"]);
    assert_eq!(
        stale_mark["receivedTimestamp"],
        last_mark["receivedTimestamp"]
    );
    assert_eq!(
        stale_mark["exchangeTimestamp"],
        last_mark["exchangeTimestamp"]
    );
    super::assert_ws_matches_http(&client, &base, &all).await;
    super::assert_ws_matches_http(&client, &base, &selected).await;

    *m.info.write().await = (StatusCode::BAD_GATEWAY, json!({"offline":true}));
    *m.premium.write().await = (StatusCode::OK, premium());
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(all.coverage["enumerationComplete"], false);
    assert_eq!(all.coverage["expectedMarkets"], 3);
    assert_eq!(selected.markets[btc]["fields"]["funding"], cleared);
    super::assert_ws_matches_http(&client, &base, &all).await;

    *m.info.write().await = (StatusCode::OK, info());
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(all.coverage["enumerationComplete"], true);
    assert_eq!(
        selected.markets[btc]["fields"]["funding"]["value"]["rate"],
        "-0.00001250"
    );

    let mut delisted = info();
    delisted["symbols"][1]["status"] = json!("SETTLING");
    *m.info.write().await = (StatusCode::OK, delisted.clone());
    tokio::time::advance(Duration::from_millis(30_001)).await;
    let removed_from_all = super::ws_delta(&mut all_socket, &mut all).await;
    super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed_from_all["removedMarketIds"], json!([btc]));
    assert_eq!(selected.markets[btc]["active"], false);
    assert_eq!(
        selected.markets[btc]["fields"]["funding"]["reason"],
        "inactive-market"
    );
    super::assert_ws_matches_http(&client, &base, &selected).await;

    delisted["symbols"].as_array_mut().unwrap().remove(1);
    *m.info.write().await = (StatusCode::OK, delisted);
    super::after_receipt(
        &all.markets.values().next().unwrap()["fields"]["funding"]["receivedTimestamp"],
    )
    .await;
    tokio::time::advance(Duration::from_millis(30_001)).await;
    super::ws_delta(&mut all_socket, &mut all).await;
    let removed = super::ws_delta(&mut selected_socket, &mut selected).await;
    assert_eq!(removed["removedMarketIds"], json!([btc]));
    assert_eq!(selected.coverage["returnedMarkets"], 0);
    super::stats_http(&client, &base, selected_request, StatusCode::BAD_REQUEST).await;
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [7, 7, 1]
    );
    super::ws_unsubscribe(&mut selected_socket, &selected.topic).await;
    super::ws_unsubscribe(&mut all_socket, &all.topic).await;
    super::ws_disconnect(selected_socket).await;
    super::ws_disconnect(all_socket).await;
    state.shutdown_market_stats().await;
    tokio::time::resume();
    manual.abort();
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_cold_catalog_failure_never_fabricates_unknown_identity() {
    let (m, url, upstream_stop, upstream_task) = server().await;
    *m.info.write().await = (StatusCode::OK, json!({"symbols":"invalid"}));
    let source: Arc<dyn MarketDataExchange> =
        Arc::new(BinanceExchange::with_base_url(url, 2_000).unwrap());
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();
    let btc = "[\"binance\",\"perp\",null,null,\"BTCUSDC\"]";
    for request in [
        json!({"exchange":"binance","params":{"dex":""}}),
        json!({"exchange":"binance","marketIds":["[\"binance\",\"spot\",null,null,\"BTCUSDC\"]"]}),
        json!({"exchange":"binance","marketIds":["[\"hyperliquid\",\"perp\",null,\"\",\"BTC\"]"]}),
        json!({"exchange":"binance","marketIds":[]}),
    ] {
        super::stats_http(&client, &base, request, StatusCode::BAD_REQUEST).await;
    }
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [0, 0, 0]
    );
    let cold = super::stats_http(
        &client,
        &base,
        json!({"exchange":"binance"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert_eq!(
        cold["coverage"]["sourceFailures"][0]["reason"],
        "invalid-upstream-data"
    );
    super::stats_http(
        &client,
        &base,
        json!({"exchange":"binance","marketIds":[btc]}),
        StatusCode::BAD_GATEWAY,
    )
    .await;
    assert_eq!(
        m.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst)),
        [1, 1, 1]
    );
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}
