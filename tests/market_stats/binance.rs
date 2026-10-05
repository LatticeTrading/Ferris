//! Binance stock statistics tests: linear USD-M bulk metrics plus selected
//! singular open interest. Fixtures serve the pinned CCXT 4.5.85 fapi paths.

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

#[derive(Clone)]
struct BinanceMock {
    exchange_info: Arc<RwLock<super::Reply>>,
    tickers: Arc<RwLock<super::Reply>>,
    premium: Arc<RwLock<super::Reply>>,
    funding: Arc<RwLock<super::Reply>>,
    open_interest: Arc<RwLock<BTreeMap<String, super::Reply>>>,
    counts: Arc<[AtomicUsize; 5]>,
    // Every `symbol` query the singular open-interest endpoint received, in order.
    interest_queries: Arc<RwLock<Vec<String>>>,
}

impl BinanceMock {
    async fn set_exchange_info(&self, status: StatusCode, body: Value) {
        *self.exchange_info.write().await = reply(status, body);
    }

    async fn set_tickers(&self, status: StatusCode, body: Value) {
        *self.tickers.write().await = reply(status, body);
    }

    async fn set_interest(&self, symbol: &str, status: StatusCode, body: Value) {
        self.open_interest
            .write()
            .await
            .insert(symbol.to_string(), reply(status, body));
    }

    fn calls(&self) -> [usize; 5] {
        self.counts
            .each_ref()
            .map(|count| count.load(Ordering::SeqCst))
    }

    async fn interest_symbols(&self) -> Vec<String> {
        self.interest_queries.read().await.clone()
    }
}

fn reply(status: StatusCode, body: Value) -> super::Reply {
    super::Reply {
        status,
        body,
        barrier: None,
    }
}

fn instrument(symbol: &str, base: &str, quote: &str, margin: &str, status: &str) -> Value {
    json!({
        "symbol": symbol,
        "contractType": "PERPETUAL",
        "status": status,
        "baseAsset": base,
        "quoteAsset": quote,
        "marginAsset": margin,
        "pair": format!("{base}{quote}"),
        "contractSize": "1",
        "pricePrecision": "2",
        "quantityPrecision": "3"
    })
}

fn exchange_info() -> Value {
    let mut future = instrument("FUTUSDT", "FUT", "USDT", "USDT", "TRADING");
    future["contractType"] = json!("CURRENT_QUARTER");
    future["deliveryDate"] = json!(4_000_000_000_000u64);
    json!({"symbols":[
        instrument("BTCUSDT", "BTC", "USDT", "USDT", "TRADING"),
        instrument("ETHUSDC", "ETH", "USDC", "USDC", "TRADING"),
        instrument("SOLUSDT", "SOL", "USDT", "USDT", "TRADING"),
        instrument("OLDUSDT", "OLD", "USDT", "USDT", "SETTLING"),
        future
    ]})
}

fn ticker(symbol: &str, last: &str, volume: &str, quote_volume: &str, close: u64) -> Value {
    json!({
        "symbol": symbol,
        "lastPrice": last,
        "volume": volume,
        "quoteVolume": quote_volume,
        "weightedAvgPrice": "30000.00",
        "openTime": 1699990000000u64,
        "closeTime": close
    })
}

fn tickers() -> Value {
    json!([
        // A reversed order proves identity joins rather than positional joins.
        ticker("ETHUSDC", "2000.10", "1500.5", "3000000.25", 1700000000100),
        ticker(
            "BTCUSDT",
            "30001.20",
            "1234.5",
            "37000000.50",
            1700000000123
        ),
        ticker("SOLUSDT", "150.25", "9999.9", "1500000.00", 1700000000125),
        ticker("OLDUSDT", "10", "5", "50", 1700000000124)
    ])
}

fn premium(symbol: &str, rate: &str, mark: &str, index: &str, next: u64) -> Value {
    json!({
        "symbol": symbol,
        "lastFundingRate": rate,
        "markPrice": mark,
        "indexPrice": index,
        "nextFundingTime": next,
        "time": 1700000000123u64
    })
}

fn premium_index() -> Value {
    json!([
        premium("ETHUSDC", "0", "2000.100", "2000.000", 1700028800000u64),
        premium(
            "BTCUSDT",
            "-0.00001250",
            "30000.000",
            "29999.900",
            1700028800000u64
        ),
        premium(
            "SOLUSDT",
            "0.00010000",
            "150.250",
            "150.200",
            1700028800000u64
        ),
        premium("OLDUSDT", "0.0100", "10.00", "10.00", 1700028800000u64)
    ])
}

fn funding_intervals() -> Value {
    json!([
        {"symbol": "BTCUSDT", "fundingIntervalHours": 8},
        {"symbol": "ETHUSDC", "fundingIntervalHours": 4},
        {"symbol": "SOLUSDT", "fundingIntervalHours": 8},
        {"symbol": "OLDUSDT", "fundingIntervalHours": 8}
    ])
}

async fn handler(State(mock): State<BinanceMock>, request: Request<axum::body::Body>) -> Response {
    let path = request.uri().path().to_string();
    let (index, reply) = match path.as_str() {
        "/fapi/v1/exchangeInfo" => (0, mock.exchange_info.read().await.clone()),
        "/fapi/v1/ticker/24hr" => {
            assert_no_symbol_filter(&request);
            (1, mock.tickers.read().await.clone())
        }
        "/fapi/v1/premiumIndex" => {
            assert_no_symbol_filter(&request);
            (2, mock.premium.read().await.clone())
        }
        "/fapi/v1/fundingInfo" => {
            assert_no_symbol_filter(&request);
            (3, mock.funding.read().await.clone())
        }
        "/fapi/v1/openInterest" => {
            let symbol = query(request.uri().query())
                .remove("symbol")
                .expect("singular open interest requires a symbol");
            mock.interest_queries.write().await.push(symbol.clone());
            let reply = mock
                .open_interest
                .read()
                .await
                .get(&symbol)
                .cloned()
                .unwrap_or_else(|| panic!("unexpected open-interest symbol {symbol}"));
            (4, reply)
        }
        "/api/v3/exchangeInfo" => (
            0,
            reply(
                StatusCode::OK,
                json!({"symbols":[{
                    "symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING",
                    "isSpotTradingAllowed":true,"baseAssetPrecision":8,"quotePrecision":8
                }]}),
            ),
        ),
        "/api/v3/ticker/24hr" => (
            1,
            reply(
                StatusCode::OK,
                json!([ticker("BTCUSDT", "500.00", "12.5", "6250", 1700000000123)]),
            ),
        ),
        "/eapi/v1/exchangeInfo" => (
            0,
            reply(
                StatusCode::OK,
                json!({"optionSymbols":[{
                    "symbol":"BTC-261226-50000-C", "underlying":"BTCUSDT", "quoteAsset":"USDT",
                    "expiryDate":1798272000000u64, "strikePrice":"50000", "side":"CALL",
                    "unit":0.01, "priceScale":2, "quantityScale":2, "minQty":"0.01"
                }]}),
            ),
        ),
        "/eapi/v1/ticker" => (
            1,
            reply(
                StatusCode::OK,
                json!([{
                    "symbol":"BTC-261226-50000-C", "lastPrice":"1450.00", "volume":"15",
                    "amount":"2482.73", "exercisePrice":"90000", "closeTime":1700000000123u64
                }]),
            ),
        ),
        "/eapi/v1/mark" => (
            2,
            reply(
                StatusCode::OK,
                json!([{
                    "symbol":"BTC-261226-50000-C", "markPrice":"1450.5"
                }]),
            ),
        ),
        "/eapi/v1/index" => (
            2,
            reply(
                StatusCode::OK,
                json!({
                    "indexPrice":"60000.0", "time":1700000000123u64
                }),
            ),
        ),
        "/eapi/v1/openInterest" => (
            4,
            reply(
                StatusCode::OK,
                json!([{
                    "symbol":"BTC-261226-50000-C", "sumOpenInterest":"6.3",
                    "sumOpenInterestUsd":"3780", "timestamp":1700000000123u64
                }]),
            ),
        ),
        other => panic!("unexpected Binance route {other}"),
    };
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

// Bulk acquisition may carry product selectors, but never a symbol fan-out.
fn assert_no_symbol_filter(request: &Request<axum::body::Body>) {
    let params = query(request.uri().query());
    assert!(
        !params.contains_key("symbol") && !params.contains_key("symbols"),
        "bulk acquisition must not filter symbols: {}",
        request.uri()
    );
}

async fn server() -> (BinanceMock, String, oneshot::Sender<()>, JoinHandle<()>) {
    let mock = BinanceMock {
        exchange_info: Arc::new(RwLock::new(reply(StatusCode::OK, exchange_info()))),
        tickers: Arc::new(RwLock::new(reply(StatusCode::OK, tickers()))),
        premium: Arc::new(RwLock::new(reply(StatusCode::OK, premium_index()))),
        funding: Arc::new(RwLock::new(reply(StatusCode::OK, funding_intervals()))),
        open_interest: Arc::new(RwLock::new(BTreeMap::from([
            (
                "BTCUSDT".to_string(),
                reply(
                    StatusCode::OK,
                    json!({"symbol":"BTCUSDT","openInterest":"12345.678","time":1700000000123u64}),
                ),
            ),
            (
                "ETHUSDC".to_string(),
                reply(
                    StatusCode::OK,
                    json!({"symbol":"ETHUSDC","openInterest":"77.5","time":1700000000124u64}),
                ),
            ),
        ]))),
        counts: Arc::new(Default::default()),
        interest_queries: Arc::new(RwLock::new(Vec::new())),
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
    json!(["binance", "perp", null, null, symbol]).to_string()
}

fn request(ids: Option<Vec<String>>, fields: &[&str]) -> Value {
    json!({"exchange":"binance", "params":{}, "marketIds":ids, "fields":fields})
}

fn all_params() -> FetchMarketStatsParams {
    FetchMarketStatsParams {
        params: json!({}),
        open_interest_market_ids: Vec::new(),
        include_bulk: true,
    }
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
async fn binance_linear_source_identity_volumes_and_variable_intervals() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(all_params()).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();

    // A quarter future belongs to the linear catalog but is never treated as a
    // perpetual: funding does not apply to it.
    if let Some(future) = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["exchangeMarketId"] == "FUTUSDT")
    {
        assert_eq!(future["type"], "future");
        assert_eq!(future["fields"]["funding"]["state"], "notApplicable");
    }
    // An inactive USD-M contract may be enumerated but is never active.
    if let Some(old) = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["exchangeMarketId"] == "OLDUSDT")
    {
        assert_eq!(old["active"], false);
    }
    let btc = row(&rows, &id("BTCUSDT"));
    assert_eq!(btc["symbol"], "BTC/USDT");
    assert_eq!(btc["exchangeMarketId"], "BTCUSDT");
    assert_eq!(btc["settle"], "USDT");
    assert_eq!(btc["settlementAssetId"], "USDT");
    assert_eq!(btc["category"], Value::Null);
    assert_eq!(btc["active"], true);

    // Prices and funding keep the upstream lexical precision.
    assert_eq!(btc["fields"]["markPrice"]["value"]["amount"], "30000.000");
    assert_eq!(btc["fields"]["markPrice"]["value"]["baseAsset"], "BTC");
    assert_eq!(btc["fields"]["markPrice"]["value"]["quoteAsset"], "USDT");
    assert_eq!(btc["fields"]["indexPrice"]["value"]["amount"], "29999.900");
    assert_eq!(btc["fields"]["lastPrice"]["value"]["amount"], "30001.20");
    let funding = &btc["fields"]["funding"]["value"];
    assert_eq!(funding["rate"], "-0.00001250");
    assert_eq!(funding["rateUnit"], "decimalFraction");
    assert_eq!(funding["rateIntervalMs"], 28_800_000u64);
    assert_eq!(funding["paymentIntervalMs"], 28_800_000u64);

    // Per-market intervals are independent, and a zero rate stays a real value.
    let eth = row(&rows, &id("ETHUSDC"));
    assert_eq!(eth["fields"]["funding"]["value"]["rate"], "0");
    assert_eq!(
        eth["fields"]["funding"]["value"]["rateIntervalMs"],
        14_400_000u64
    );
    assert_eq!(
        eth["fields"]["funding"]["value"]["equivalents"]["oneHourPercent"],
        "0"
    );

    // volume24h is numeric, quote-denominated per market.
    let volume = &btc["fields"]["volume24h"]["value"];
    assert_eq!(number(&volume["baseVolume"]), 1234.5);
    assert_eq!(number(&volume["quoteVolume"]), 37_000_000.50);
    let eth_volume = &eth["fields"]["volume24h"]["value"];
    assert_eq!(number(&eth_volume["baseVolume"]), 1500.5);
    assert_eq!(number(&eth_volume["quoteVolume"]), 3_000_000.25);

    // No selected open interest demand: the singular endpoint is never touched.
    assert!(mock.interest_symbols().await.is_empty());
    assert_eq!(mock.calls()[4], 0);
    assert!(snapshot.source_failures.is_empty());

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_zero_null_and_invalid_volume_fail_per_field() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    // ETH: a zero base volume with a missing quote volume is one null member, not
    // a failure. SOL: a non-numeric volume clears only volume24h.
    let mut eth = ticker("ETHUSDC", "2000.10", "0", "0", 1700000000100);
    eth.as_object_mut().unwrap().remove("quoteVolume");
    let mut sol = ticker("SOLUSDT", "150.25", "9999.9", "1500000.00", 1700000000125);
    sol["volume"] = json!("not-a-number");
    mock.set_tickers(
        StatusCode::OK,
        json!([
            eth,
            ticker(
                "BTCUSDT",
                "30001.20",
                "1234.5",
                "37000000.50",
                1700000000123
            ),
            sol
        ]),
    )
    .await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let snapshot = source.fetch_market_stats(all_params()).await.unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();

    let eth_volume = &row(&rows, &id("ETHUSDC"))["fields"]["volume24h"];
    assert_eq!(eth_volume["state"], "available");
    assert_eq!(number(&eth_volume["value"]["baseVolume"]), 0.0);
    assert_eq!(eth_volume["value"]["quoteVolume"], Value::Null);
    // Sibling metrics on the same authoritative market are untouched.
    assert_eq!(
        row(&rows, &id("ETHUSDC"))["fields"]["funding"]["value"]["rate"],
        "0"
    );

    let sol_volume = &row(&rows, &id("SOLUSDT"))["fields"]["volume24h"];
    assert_eq!(sol_volume["state"], "unavailable");
    assert_eq!(sol_volume["value"], Value::Null);
    assert_eq!(
        row(&rows, &id("BTCUSDT"))["fields"]["volume24h"]["state"],
        "available"
    );

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_spot_tickers_cannot_publish_futures_values() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let snapshot = source
        .fetch_market_stats(FetchMarketStatsParams {
            params: json!({"category":"spot"}),
            open_interest_market_ids: vec![
                json!(["binance", "spot", null, null, "BTCUSDT"]).to_string()
            ],
            include_bulk: true,
        })
        .await
        .unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let btc = &rows[0]["fields"];
    assert_eq!(btc["lastPrice"]["value"]["amount"], "500.00");
    assert_eq!(
        btc["volume24h"]["value"],
        json!({"baseVolume":12.5,"quoteVolume":6250.0})
    );
    assert_eq!(btc["openInterest"]["state"], "notApplicable");
    assert_eq!(snapshot.source_failures.len(), 0);
    assert_eq!(mock.calls()[4], 0);
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_options_keep_contract_units_and_use_the_underlying_index() {
    let (_mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let snapshot = source
        .fetch_market_stats(FetchMarketStatsParams {
            params: json!({"category":"option"}),
            open_interest_market_ids: vec![json!([
                "binance",
                "option",
                null,
                null,
                "BTC-261226-50000-C"
            ])
            .to_string()],
            include_bulk: true,
        })
        .await
        .unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let option = &rows[0];
    assert_eq!(option["contractSize"], 0.01);
    let fields = &option["fields"];
    assert_eq!(fields["funding"]["state"], "notApplicable");
    assert_eq!(fields["lastPrice"]["value"]["amount"], "1450.00");
    assert_eq!(fields["markPrice"]["value"]["amount"], "1450.5");
    assert_eq!(fields["indexPrice"]["value"]["amount"], "60000.0");
    assert_eq!(
        fields["indexPrice"]["source"],
        "binance:ccxt:eapiPublicGetIndex"
    );
    assert_eq!(
        fields["volume24h"]["value"],
        json!({"baseVolume":null,"quoteVolume":2482.73})
    );
    assert_eq!(
        fields["openInterest"]["value"],
        json!({"openInterestAmount":6.3,"openInterestValue":3780.0})
    );
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_unreadable_open_interest_is_invalid_not_missing() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    for amount in [
        json!("0"),
        json!(null),
        json!("unreadable"),
        json!("NaN"),
        json!("Infinity"),
    ] {
        mock.set_interest(
            "BTCUSDT",
            StatusCode::OK,
            json!({"symbol":"BTCUSDT","openInterest":amount,"time":1700000000123u64}),
        )
        .await;
        let snapshot = source
            .fetch_market_stats(FetchMarketStatsParams {
                params: json!({}),
                open_interest_market_ids: vec![id("BTCUSDT")],
                include_bulk: false,
            })
            .await
            .unwrap();
        let rows = serde_json::to_value(&snapshot.rows).unwrap();
        let field = &row(&rows, &id("BTCUSDT"))["fields"]["openInterest"];
        if amount == "0" {
            assert_eq!(field["state"], "available");
            assert_eq!(
                field["value"],
                json!({"openInterestAmount":0.0,"openInterestValue":null})
            );
        } else if amount.is_null() {
            assert_eq!(field["state"], "unavailable");
            assert_eq!(field["reason"], "missing-upstream-row");
            assert_eq!(field["value"], Value::Null);
        } else {
            assert_eq!(field["state"], "unavailable");
            assert_eq!(
                field["reason"], "invalid-upstream-value",
                "{amount}: {field}"
            );
            assert_eq!(field["value"], Value::Null);
        }
    }
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_selected_open_interest_uses_singular_calls_only() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    // Open interest without a selection is a client error before any acquisition.
    super::stats_http(
        &client,
        &base,
        json!({"exchange":"binance","fields":["openInterest"]}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.calls(), [0, 0, 0, 0, 0]);

    // Default funding demand must never trigger a singular open-interest call.
    let funding_only = super::stats_http(
        &client,
        &base,
        request(None, &["funding", "markPrice", "volume24h"]),
        StatusCode::OK,
    )
    .await;
    assert_eq!(funding_only["coverage"]["expectedMarkets"], 3);
    assert!(mock.interest_symbols().await.is_empty());
    assert_eq!(mock.calls()[4], 0);
    let bulk_after_funding = mock.calls();

    // Selected open interest is a singular per-market observation.
    let selected = request(
        Some(vec![id("BTCUSDT"), id("ETHUSDC")]),
        &["openInterest", "funding"],
    );
    let snapshot = super::stats_http(&client, &base, selected.clone(), StatusCode::OK).await;
    let btc = &row(&snapshot["markets"], &id("BTCUSDT"))["fields"]["openInterest"];
    assert_eq!(btc["state"], "available");
    assert_eq!(number(&btc["value"]["openInterestAmount"]), 12345.678);
    assert_eq!(btc["value"]["openInterestValue"], Value::Null);
    let eth = &row(&snapshot["markets"], &id("ETHUSDC"))["fields"]["openInterest"];
    assert_eq!(number(&eth["value"]["openInterestAmount"]), 77.5);
    let mut requested = mock.interest_symbols().await;
    requested.sort();
    assert_eq!(requested, ["BTCUSDT", "ETHUSDC"]);
    assert_eq!(mock.calls()[4], 2);
    // The shared bulk acquisition is reused, not repeated for the OI viewer.
    assert_eq!(mock.calls()[0..4], bulk_after_funding[0..4]);

    // Repeating the same demand shares the acquisition and the OI observations.
    let bulk_before_repeat = mock.calls();
    super::stats_http(&client, &base, selected, StatusCode::OK).await;
    assert_eq!(mock.calls(), bulk_before_repeat);

    let inactive = super::stats_http(
        &client,
        &base,
        request(Some(vec![id("OLDUSDT")]), &["openInterest"]),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        inactive["markets"][0]["fields"]["openInterest"]["state"],
        "unavailable"
    );
    assert_eq!(
        inactive["markets"][0]["fields"]["openInterest"]["reason"],
        "inactive-market"
    );
    assert_eq!(inactive["coverage"]["sourceFailures"], json!([]));
    assert_eq!(mock.calls(), bulk_before_repeat);

    // An unqualified selection is rejected from the complete catalog without I/O.
    let before = mock.calls();
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("UNKNOWN")]), &["openInterest"]),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(mock.calls(), before);

    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_singular_only_demand_skips_bulk_and_marks_other_fields_not_requested() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source = super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let snapshot = source
        .fetch_market_stats(FetchMarketStatsParams {
            params: json!({}),
            open_interest_market_ids: vec![id("BTCUSDT")],
            include_bulk: false,
        })
        .await
        .unwrap();
    let rows = serde_json::to_value(&snapshot.rows).unwrap();
    let btc = &row(&rows, &id("BTCUSDT"))["fields"];
    // Only the singular open-interest call happens; every bulk metric is deferred.
    assert_eq!(mock.interest_symbols().await, ["BTCUSDT"]);
    assert_eq!(mock.calls()[1], 0);
    assert_eq!(mock.calls()[2], 0);
    assert_eq!(mock.calls()[3], 0);
    assert_eq!(btc["openInterest"]["state"], "available");
    assert_eq!(
        number(&btc["openInterest"]["value"]["openInterestAmount"]),
        12345.678
    );
    for field in [
        "funding",
        "markPrice",
        "indexPrice",
        "lastPrice",
        "volume24h",
    ] {
        assert_eq!(btc[field]["state"], "unavailable", "{field}");
        assert_eq!(btc[field]["reason"], "not-requested", "{field}");
        assert_eq!(btc[field]["value"], Value::Null, "{field}");
    }

    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_http_ws_share_open_interest_and_revise_deltas() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    let btc = id("BTCUSDT");
    let btc_request = request(Some(vec![btc.clone()]), &["openInterest", "funding"]);
    let mut btc_socket = super::stats_socket(&base).await;
    let mut second_socket = super::stats_socket(&base).await;
    super::ws_send(
        &mut btc_socket,
        super::stats_command("subscribe", &btc_request),
    )
    .await;
    super::ws_send(
        &mut second_socket,
        super::stats_command("subscribe", &btc_request),
    )
    .await;
    let mut btc_view = super::ws_initial(&mut btc_socket).await;
    let mut second_view = super::ws_initial(&mut second_socket).await;
    // Two subscribers on the same selection share one acquisition and one
    // singular open-interest observation.
    assert_eq!(mock.interest_symbols().await, ["BTCUSDT"]);
    assert_eq!(mock.calls()[4], 1);
    assert_eq!(btc_view.markets[&btc], second_view.markets[&btc]);
    assert_eq!(
        number(&btc_view.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"]),
        12345.678
    );
    // A snapshot request for the same selection adds no upstream demand.
    let snapshot = super::stats_http(&client, &base, btc_request.clone(), StatusCode::OK).await;
    assert_eq!(mock.interest_symbols().await, ["BTCUSDT"]);
    assert_eq!(mock.calls()[4], 1);
    assert_eq!(
        number(&snapshot["markets"][0]["fields"]["openInterest"]["value"]["openInterestAmount"]),
        12345.678
    );

    // A changed singular observation revises the subscription's topic.
    mock.set_interest(
        "BTCUSDT",
        StatusCode::OK,
        json!({"symbol":"BTCUSDT","openInterest":"999.5","time":1700000000200u64}),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    // Bulk funding can publish before the independently due singular call.
    // Reduce every revision until both clients observe the changed OI receipt.
    tokio::time::timeout(Duration::from_secs(10), async {
        while number(
            &btc_view.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"],
        ) != 999.5
        {
            super::ws_delta(&mut btc_socket, &mut btc_view).await;
        }
        while number(
            &second_view.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"],
        ) != 999.5
        {
            super::ws_delta(&mut second_socket, &mut second_view).await;
        }
    })
    .await
    .expect("singular open interest was not refreshed");
    assert_eq!(
        number(&btc_view.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"]),
        999.5
    );
    assert_eq!(
        number(&second_view.markets[&btc]["fields"]["openInterest"]["value"]["openInterestAmount"]),
        999.5
    );
    super::assert_ws_matches_http(&client, &base, &btc_view).await;

    // A failing singular call keeps the last observation and never invents a removal.
    mock.set_interest("BTCUSDT", StatusCode::BAD_GATEWAY, json!({"offline":true}))
        .await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while btc_view.markets[&btc]["fields"]["openInterest"]["state"] != "stale" {
            let failed = super::ws_delta(&mut btc_socket, &mut btc_view).await;
            assert_eq!(failed["removedMarketIds"], json!([]));
        }
        while second_view.markets[&btc]["fields"]["openInterest"]["state"] != "stale" {
            super::ws_delta(&mut second_socket, &mut second_view).await;
        }
    })
    .await
    .expect("singular open interest failure was not published");
    let interest = &btc_view.markets[&btc]["fields"]["openInterest"];
    assert_eq!(interest["state"], "stale");
    assert_eq!(number(&interest["value"]["openInterestAmount"]), 999.5);

    super::ws_unsubscribe(&mut btc_socket, &btc_view.topic).await;
    super::ws_unsubscribe(&mut second_socket, &second_view.topic).await;
    super::ws_disconnect(btc_socket).await;
    super::ws_disconnect(second_socket).await;
    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}

#[tokio::test]
async fn binance_cold_catalog_failure_never_fabricates_unknown_identity() {
    let (mock, upstream, upstream_stop, upstream_task) = server().await;
    mock.set_exchange_info(StatusCode::OK, json!({"symbols":"invalid"}))
        .await;
    let source: Arc<dyn MarketDataExchange> =
        super::ccxt_stats_exchange(Venue::Binance, &upstream, 2_000);
    let (base, state, stop, task) = super::backend(source).await;
    let client = reqwest::Client::new();

    // Invalid selections are rejected before any acquisition.
    for invalid in [
        json!({"exchange":"binance","params":{"dex":""}}),
        json!({"exchange":"binance","marketIds":["[\"binance\",\"spot\",null,null,\"BTCUSDT\"]"]}),
        json!({"exchange":"binance","marketIds":["[\"hyperliquid\",\"perp\",null,\"\",\"BTC\"]"]}),
        json!({"exchange":"binance","marketIds":[]}),
        json!({"exchange":"binance","fields":[]}),
    ] {
        super::stats_http(&client, &base, invalid, StatusCode::BAD_REQUEST).await;
    }
    assert_eq!(mock.calls(), [0, 0, 0, 0, 0]);

    // A cold catalog failure is not an authoritative empty catalog.
    let cold = super::stats_http(&client, &base, request(None, &["funding"]), StatusCode::OK).await;
    assert_eq!(cold["markets"], json!([]));
    assert_eq!(cold["coverage"]["expectedMarkets"], Value::Null);
    assert_eq!(cold["coverage"]["enumerationComplete"], false);
    assert_eq!(
        cold["coverage"]["sourceFailures"][0]["reason"],
        "invalid-upstream-data"
    );

    // A selected identity cannot be resolved, so it is an upstream failure, not a 400.
    super::stats_http(
        &client,
        &base,
        request(Some(vec![id("BTCUSDT")]), &["funding"]),
        StatusCode::BAD_GATEWAY,
    )
    .await;

    state.shutdown_market_stats().await;
    stop.send(()).unwrap();
    task.await.unwrap();
    upstream_stop.send(()).unwrap();
    upstream_task.await.unwrap();
}
