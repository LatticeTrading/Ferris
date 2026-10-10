//! Stock Hyperliquid REST coverage over a loopback `POST /info` fixture.
//!
//! The fixture is the real stock CCXT Hyperliquid core: markets are acquired
//! through `fetchCurrencies`/`fetchMarkets` (swap + spot + every discovered
//! HIP3 deployer), and trades, candles and books are produced by the genuine
//! stock request builders and parsers. Only the HTTP upstream is local, so a
//! request that reaches a production endpoint fails the test instead.
//!
//! Covered: primary perps, spot, HIP3 deployers (including the eleventh and a
//! non-USDC collateral deployer), the same underlying asset on two deployers,
//! opaque catalog marketIds, client-side window filtering, missing values and
//! the supported depth/limit boundaries.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use ferris_market_data_backend::{
    config::Config,
    exchanges::{
        ccxt::{CcxtExchange, CcxtService, Venue},
        registry::ExchangeRegistry,
    },
    models::UnifiedMarketType,
    realtime::RealtimeService,
    web::{self, AppState},
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

const T: u64 = 1_700_000_000_000;
const HOUR: u64 = 3_600_000;

/// Deployer DEXs in `perpDexs` discovery order. `abc` (non-USDC collateral) and
/// `xyz` are inside the first ten, so their HIP3 coverage is independent of the
/// discovery bound; `ELEVENTH` is the first deployer a ten-deployer bound drops.
const DEPLOYERS: [&str; 13] = [
    "abc", "xyz", "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10",
];

/// The eleventh discovered deployer DEX.
const ELEVENTH: &str = "d8";

fn spot_meta() -> Value {
    json!({
        "tokens": [
            {"index": 0, "name": "USDC", "szDecimals": 8},
            {"index": 1, "name": "USDH", "szDecimals": 8},
            {"index": 2, "name": "PURR", "szDecimals": 0},
            {"index": 3, "name": "HFUN", "szDecimals": 0}
        ],
        "universe": [
            {"index": 0, "name": "PURR/USDC", "tokens": [2, 0]},
            {"index": 1, "name": "@1", "tokens": [3, 0]}
        ]
    })
}

fn ctx(mark: &str) -> Value {
    json!({"funding": "0.0001", "markPx": mark, "oraclePx": mark, "dayNtlVlm": "10", "openInterest": "1"})
}

fn primary_meta() -> Value {
    json!({
        "collateralToken": 0,
        "universe": [
            {"name": "BTC", "szDecimals": 5},
            {"name": "OLD", "szDecimals": 4, "isDelisted": true}
        ]
    })
}

fn dex_meta(dex: &str) -> Value {
    let (collateral, name) = match dex {
        "abc" => (1, "abc:BTC".to_string()),
        "xyz" => (0, "xyz:BTC".to_string()),
        other => (0, format!("{other}:AAA")),
    };
    json!({
        "collateralToken": collateral,
        "universe": [{"name": name, "szDecimals": 2}]
    })
}

fn perp_dexs() -> Value {
    let mut rows = vec![Value::Null];
    rows.extend(DEPLOYERS.iter().map(|dex| json!({"name": dex})));
    Value::Array(rows)
}

fn trade(coin: &str, side: &str, px: Value, sz: Value, time: u64, tid: u64) -> Value {
    json!({"coin": coin, "side": side, "px": px, "sz": sz, "time": time, "tid": tid})
}

fn trades(coin: &str) -> Value {
    match coin {
        "BTC" => json!([
            trade("BTC", "A", json!("60000"), json!("0.01"), T + 1_000, 1),
            trade("BTC", "B", json!("60001"), json!("0.02"), T + 2_000, 2),
            trade("BTC", "A", json!("60002"), json!("0.03"), T + 3_000, 3)
        ]),
        // One row omits the price and one omits the size: neither may be
        // replaced by a fabricated number or an inferred symbol.
        "abc:BTC" => json!([
            trade("abc:BTC", "B", json!("100"), json!("0.5"), T + 1_000, 11),
            trade("abc:BTC", "A", Value::Null, json!("0.25"), T + 2_000, 12),
            trade("abc:BTC", "B", json!("101"), Value::Null, T + 3_000, 13)
        ]),
        "xyz:BTC" => json!([
            trade("xyz:BTC", "A", json!("200"), json!("0.4"), T + 1_000, 21),
            trade("xyz:BTC", "B", json!("201"), json!("0.6"), T + 2_000, 22)
        ]),
        "@1" => json!([trade("@1", "B", json!("5"), json!("7"), T + 1_000, 31)]),
        "PURR/USDC" => json!([trade(
            "PURR/USDC",
            "A",
            json!("0.1"),
            json!("9"),
            T + 1_000,
            41
        )]),
        other => panic!("unexpected recentTrades coin: {other}"),
    }
}

fn candle(open_time: u64, open: f64, volume: Value) -> Value {
    json!({
        "t": open_time, "T": open_time + HOUR - 1,
        "o": open.to_string(), "h": (open + 1.0).to_string(),
        "l": (open - 1.0).to_string(), "c": open.to_string(),
        "v": volume, "n": 1, "i": "1h", "s": "x"
    })
}

fn candles(coin: &str) -> Value {
    match coin {
        "BTC" => json!([
            candle(T, 60000.0, json!("1")),
            candle(T + HOUR, 60001.0, json!("2")),
            candle(T + 2 * HOUR, 60002.0, json!("3"))
        ]),
        // The second candle has no published volume; it must stay null.
        "abc:BTC" => json!([
            candle(T, 100.0, json!("1")),
            candle(T + HOUR, 100.0, Value::Null),
            candle(T + 2 * HOUR, 100.0, json!("3")),
            candle(T + 3 * HOUR, 100.0, json!("4")),
            candle(T + 4 * HOUR, 100.0, json!("5"))
        ]),
        "xyz:BTC" => json!([candle(T, 200.0, json!("9"))]),
        "@1" => json!([
            candle(T, 5.0, json!("1")),
            candle(T + HOUR, 6.0, json!("2"))
        ]),
        other => panic!("unexpected candleSnapshot coin: {other}"),
    }
}

fn book(coin: &str) -> Value {
    let bid = match coin {
        "BTC" => 60000.0,
        "abc:BTC" => 100.0,
        "xyz:BTC" => 200.0,
        "@1" => 5.0,
        other => panic!("unexpected l2Book coin: {other}"),
    };
    json!({
        "coin": coin,
        "time": T,
        "levels": [
            [
                {"px": bid.to_string(), "sz": "1.5", "n": 2},
                {"px": (bid - 1.0).to_string(), "sz": "0.5", "n": 1}
            ],
            [{"px": (bid + 1.0).to_string(), "sz": "1.0", "n": 3}]
        ]
    })
}

#[derive(Clone, Default)]
struct Mock {
    calls: Arc<Mutex<Vec<Value>>>,
}

async fn info(State(mock): State<Mock>, Json(request): Json<Value>) -> Json<Value> {
    mock.calls.lock().await.push(request.clone());
    let kind = request["type"].as_str().unwrap_or_default();
    let body = match kind {
        "spotMeta" => spot_meta(),
        "spotMetaAndAssetCtxs" => json!([spot_meta(), [ctx("0.1"), ctx("0.2")]]),
        "perpDexs" => perp_dexs(),
        "metaAndAssetCtxs" => match request["dex"].as_str() {
            None => json!([primary_meta(), [ctx("60000"), ctx("1")]]),
            Some(dex) => json!([dex_meta(dex), [ctx("100")]]),
        },
        "recentTrades" => trades(request["coin"].as_str().unwrap_or_default()),
        "candleSnapshot" => candles(request["req"]["coin"].as_str().unwrap_or_default()),
        "l2Book" => book(request["coin"].as_str().unwrap_or_default()),
        other => panic!("unexpected Hyperliquid info request `{other}`: {request}"),
    };
    Json(body)
}

struct Harness {
    mock: Mock,
    service: CcxtService,
    base: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Harness {
    async fn start() -> Self {
        let mock = Mock::default();
        let upstream = Router::new()
            .route("/info", post(info))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });

        // Every other venue points at an unroutable loopback port, so a
        // regression that fans out to another venue fails fast.
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            request_timeout_ms: 2_000,
            hyperliquid_base_url: format!("http://{upstream_addr}"),
            extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
            extended_ws_url: "ws://127.0.0.1:1".into(),
            lighter_rest_base_url: "http://127.0.0.1:1".into(),
            lighter_ws_url: "ws://127.0.0.1:1".into(),
            binance_base_url: "http://127.0.0.1:1".into(),
            bybit_base_url: "http://127.0.0.1:1".into(),
            aster_base_url: "http://127.0.0.1:1".into(),
            apex_rest_base_url: "http://127.0.0.1:1/api".into(),
            apex_ws_url: "ws://127.0.0.1:1".into(),
            bitfinex_rest_base_url: "http://127.0.0.1:1".into(),
            bitfinex_ws_url: "ws://127.0.0.1:1".into(),
            kucoin_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_futures_rest_base_url: "http://127.0.0.1:1".into(),
            kucoin_ws_url: "ws://127.0.0.1:1".into(),
            kucoin_futures_ws_url: "ws://127.0.0.1:1".into(),
            nado_gateway_base_url: "http://127.0.0.1:1".into(),
            nado_archive_base_url: "http://127.0.0.1:1".into(),
            nado_ws_url: "ws://127.0.0.1:1".into(),
        };
        let service = CcxtService::start(&config).unwrap();
        let realtime = RealtimeService::new(service.clone());
        let mut registry = ExchangeRegistry::new();
        registry.register(Arc::new(CcxtExchange::new(
            Venue::Hyperliquid,
            service.clone(),
        )));
        let state = AppState::new(Arc::new(registry), realtime);
        let app = Router::new()
            .route("/v1/fetchMarkets", post(web::fetch_markets))
            .route("/v1/fetchTrades", post(web::fetch_trades))
            .route("/v1/fetchOHLCV", post(web::fetch_ohlcv))
            .route("/v1/fetchOrderBook", post(web::fetch_order_book))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            mock,
            service,
            base,
            tasks: vec![upstream_task, task],
        }
    }

    async fn post(&self, endpoint: &str, mut body: Value, expected: StatusCode) -> Value {
        body["exchange"] = json!("hyperliquid");
        let response = reqwest::Client::new()
            .post(format!("{}/v1/{endpoint}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value: Value = response.json().await.unwrap();
        assert_eq!(status, expected, "{endpoint}: {value}");
        value
    }

    async fn calls(&self, kind: &str) -> Vec<Value> {
        self.mock
            .calls
            .lock()
            .await
            .iter()
            .filter(|call| call["type"] == kind)
            .cloned()
            .collect()
    }

    async fn stop(self) {
        self.service.shutdown().await.unwrap();
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// Opaque catalog identity for a Hyperliquid market, exactly as the public
/// catalog publishes it.
fn market_id(product: UnifiedMarketType, dex: Option<&str>, native: &str) -> String {
    ferris_market_data_backend::market_stats::make_market_id(
        "hyperliquid",
        product,
        None,
        dex,
        native,
    )
    .unwrap()
}

fn perp(dex: Option<&str>, native: &str) -> String {
    market_id(UnifiedMarketType::Perp, dex, native)
}

fn spot(index: u64) -> String {
    market_id(UnifiedMarketType::Spot, None, &format!("@{index}"))
}

fn find<'a>(markets: &'a [Value], exchange_market_id: &str) -> &'a Value {
    markets
        .iter()
        .find(|row| row["exchangeMarketId"] == exchange_market_id)
        .unwrap_or_else(|| panic!("market `{exchange_market_id}` is missing from the catalog"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hyperliquid_markets_cover_primary_spot_and_every_discovered_deployer() {
    let h = Harness::start().await;
    let body = h.post("fetchMarkets", json!({}), StatusCode::OK).await;
    let markets = body["markets"].as_array().unwrap();

    // Primary perp keeps its positional native id and the empty primary dex.
    let btc = find(markets, "BTC");
    assert_eq!(btc["marketId"], perp(Some(""), "BTC"));
    assert_eq!(btc["dex"], "");
    assert_eq!(btc["info"]["exchangeSymbol"], "0");
    assert_eq!(btc["settle"], "USDC");
    assert_eq!(btc["symbol"], "BTC/USDC");
    assert_eq!(btc["type"], "perp");

    // Delisted primary markets stay out of the default catalog.
    assert!(markets.iter().all(|row| row["exchangeMarketId"] != "OLD"));
    let all = h
        .post(
            "fetchMarkets",
            json!({"includeInactive": true}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(
        find(all["markets"].as_array().unwrap(), "OLD")["active"],
        false
    );

    // Spot keeps its native pair/index identity and has no deployer.
    let purr = find(markets, "@0");
    assert_eq!(purr["dex"], Value::Null);
    assert_eq!(purr["symbol"], "PURR/USDC");
    let hfun = find(markets, "@1");
    assert_eq!(hfun["symbol"], "HFUN/USDC");

    // Every discovered deployer is loaded, including the eleventh, and each
    // deployer's positional base id is unchanged by the discovery bound.
    for (index, dex) in DEPLOYERS.iter().enumerate() {
        let native = match *dex {
            "abc" => "abc:BTC".to_string(),
            "xyz" => "xyz:BTC".to_string(),
            other => format!("{other}:AAA"),
        };
        let row = find(markets, &native);
        assert_eq!(row["marketId"], perp(Some(*dex), &native), "{dex}");
        assert_eq!(row["dex"], *dex, "{dex}");
        assert_eq!(
            row["info"]["exchangeSymbol"],
            (110_000 + index * 10_000).to_string(),
            "{dex} positional base id"
        );
    }
    assert_eq!(
        find(markets, &format!("{ELEVENTH}:AAA"))["dex"],
        ELEVENTH,
        "the eleventh deployer DEX must be discovered, not truncated"
    );

    // Same underlying asset on two deployers: distinct identities, distinct
    // display symbols, and distinct collateral settlement.
    let xyz = find(markets, "xyz:BTC");
    let abc = find(markets, "abc:BTC");
    assert_eq!(xyz["base"], "XYZ-BTC");
    assert_eq!(xyz["settle"], "USDC");
    assert_eq!(abc["base"], "ABC-BTC");
    assert_eq!(abc["settle"], "USDH");
    assert_ne!(xyz["marketId"], abc["marketId"]);

    // Deployer selection narrows the catalog without re-deriving identity.
    let primary = h
        .post(
            "fetchMarkets",
            json!({"params": {"dex": ""}}),
            StatusCode::OK,
        )
        .await;
    let primary = primary["markets"].as_array().unwrap();
    assert_eq!(primary.len(), 1);
    assert_eq!(primary[0]["exchangeMarketId"], "BTC");
    let selected = h
        .post(
            "fetchMarkets",
            json!({"params": {"dex": "abc"}}),
            StatusCode::OK,
        )
        .await;
    let selected = selected["markets"].as_array().unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0]["marketId"], perp(Some("abc"), "abc:BTC"));

    // Upstream proof: one discovery call, then one metadata call per deployer.
    let discovery = h.calls("perpDexs").await;
    assert_eq!(discovery.len(), 1);
    let loaded: Vec<String> = h
        .calls("metaAndAssetCtxs")
        .await
        .iter()
        .filter_map(|call| call["dex"].as_str().map(str::to_string))
        .collect();
    for dex in DEPLOYERS {
        assert_eq!(
            loaded.iter().filter(|name| *name == dex).count(),
            1,
            "{dex} metadata"
        );
    }

    // Spot scope is spot only; linear scope is swap + hip3 with no spot. Both
    // are consumer-level replacements for pinning the market-type list.
    let spot_only = h
        .post(
            "fetchMarkets",
            json!({"params": {"type": "spot"}}),
            StatusCode::OK,
        )
        .await;
    let spot_only = spot_only["markets"].as_array().unwrap();
    assert!(spot_only.iter().all(|row| row["type"] == "spot"));
    assert!(spot_only.iter().any(|row| row["exchangeMarketId"] == "@1"));
    let linear = h
        .post(
            "fetchMarkets",
            json!({"params": {"type": "perp"}}),
            StatusCode::OK,
        )
        .await;
    let linear = linear["markets"].as_array().unwrap();
    assert!(linear.iter().all(|row| row["type"] == "perp"));
    assert!(linear.iter().any(|row| row["exchangeMarketId"] == "BTC"));
    assert!(linear
        .iter()
        .any(|row| row["marketId"] == perp(Some("xyz"), "xyz:BTC")));
    assert!(linear
        .iter()
        .any(|row| row["marketId"] == perp(Some(ELEVENTH), &format!("{ELEVENTH}:AAA"))));
    // Each hip3-bearing scope discovers and loads every deployer once.
    assert_eq!(h.calls("perpDexs").await.len(), 2);
    let loaded: Vec<String> = h
        .calls("metaAndAssetCtxs")
        .await
        .iter()
        .filter_map(|call| call["dex"].as_str().map(str::to_string))
        .collect();
    for dex in DEPLOYERS {
        assert_eq!(
            loaded.iter().filter(|name| *name == dex).count(),
            2,
            "{dex} metadata per hip3 scope"
        );
    }
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hyperliquid_trades_route_by_native_identity_without_cross_deployer_leaks() {
    let h = Harness::start().await;

    // Primary perp: the public tape is `recentTrades`, keyed by the native name.
    let rows = h
        .post(
            "fetchTrades",
            json!({"symbol": "BTC", "limit": 10}),
            StatusCode::OK,
        )
        .await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"], "3"); // newest first
    assert_eq!(rows[0]["symbol"], "BTC/USDC:USDC");
    assert_eq!(rows[0]["price"], 60002.0);
    assert_eq!(rows[0]["amount"], 0.03);
    assert_eq!(rows[0]["timestamp"], T + 3_000);
    assert_eq!(rows[0]["side"], "sell");
    let tape = h.calls("recentTrades").await;
    assert_eq!(tape.len(), 1);
    assert_eq!(tape[0]["coin"], "BTC");
    assert!(tape[0].get("dex").is_none());

    // A client-side window is applied to the stock tape: `recentTrades` has no
    // upstream window, and out-of-window prints are dropped, never invented.
    let window = h
        .post(
            "fetchTrades",
            json!({"symbol": perp(Some(""), "BTC"), "since": T + 2_000, "params": {"until": T + 2_000}}),
            StatusCode::OK,
        )
        .await;
    let window = window.as_array().unwrap();
    assert_eq!(window.len(), 1);
    assert_eq!(window[0]["id"], "2");
    assert_eq!(h.calls("recentTrades").await.len(), 2);

    // HIP3 with non-USDC collateral: the opaque marketId selects the deployer
    // and the trade resolves through the seeded HIP3 currency metadata.
    let abc = h
        .post(
            "fetchTrades",
            json!({"symbol": perp(Some("abc"), "abc:BTC"), "limit": 10}),
            StatusCode::OK,
        )
        .await;
    let abc = abc.as_array().unwrap();
    assert_eq!(abc.len(), 3);
    for row in abc {
        assert_eq!(row["symbol"], "ABC-BTC/USDH:USDH");
    }
    // Missing price/size stay missing (rows are newest first).
    assert_eq!(abc[0]["price"], 101.0);
    assert_eq!(abc[0]["amount"], Value::Null);
    assert_eq!(abc[1]["price"], Value::Null);
    assert_eq!(abc[1]["amount"], 0.25);
    assert_eq!(abc[2]["price"], 100.0);
    assert_eq!(abc[2]["amount"], 0.5);

    // Same asset, other deployer: its own marketId and its own tape only.
    let xyz = h
        .post(
            "fetchTrades",
            json!({"symbol": perp(Some("xyz"), "xyz:BTC"), "limit": 10}),
            StatusCode::OK,
        )
        .await;
    let xyz = xyz.as_array().unwrap();
    assert_eq!(xyz.len(), 2);
    for row in xyz {
        assert_eq!(row["symbol"], "XYZ-BTC/USDC:USDC");
        assert!(row["price"].as_f64().unwrap() >= 200.0);
    }
    let tape = h.calls("recentTrades").await;
    let coins: Vec<&str> = tape
        .iter()
        .filter_map(|call| call["coin"].as_str())
        .collect();
    assert_eq!(coins, ["BTC", "BTC", "abc:BTC", "xyz:BTC"]);
    assert!(!coins.iter().any(|coin| *coin == "ABC-BTC/USDH:USDH"));

    // Spot keeps the stock market id as its tape key.
    let purr = h
        .post(
            "fetchTrades",
            json!({"symbol": spot(0), "limit": 10}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(purr.as_array().unwrap().len(), 1);
    assert_eq!(purr[0]["symbol"], "PURR/USDC");
    let hfun = h
        .post(
            "fetchTrades",
            json!({"symbol": "HFUN/USDC", "limit": 10}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(hfun[0]["symbol"], "HFUN/USDC");
    let coins: Vec<String> = h
        .calls("recentTrades")
        .await
        .iter()
        .filter_map(|call| call["coin"].as_str().map(str::to_string))
        .collect();
    assert_eq!(coins[4..], ["PURR/USDC", "@1"]);

    // Unsupported depth/limit shapes fail before any upstream call.
    let before = h.mock.calls.lock().await.len();
    h.post(
        "fetchTrades",
        json!({"symbol": "NOPE"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchTrades",
        json!({"symbol": "BTC", "params": {"dex": "xyz"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(h.mock.calls.lock().await.len(), before);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hyperliquid_candles_use_nested_stock_req_and_window_boundaries() {
    let h = Harness::start().await;

    let rows = h
        .post(
            "fetchOHLCV",
            json!({"symbol": "BTC", "timeframe": "1h", "since": T, "limit": 3}),
            StatusCode::OK,
        )
        .await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], json!([T, 60000.0, 60001.0, 59999.0, 60000.0, 1.0]));
    let calls = h.calls("candleSnapshot").await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["req"]["coin"], "BTC");
    assert_eq!(calls[0]["req"]["interval"], "1h");
    assert_eq!(calls[0]["req"]["startTime"], T);
    // No explicit end: the bound is derived from the clamped limit.
    assert_eq!(calls[0]["req"]["endTime"], T + 3 * HOUR);
    assert!(calls[0].get("dex").is_none());
    assert!(calls[0]["req"].get("price").is_none());

    // HIP3: nested `req.coin` is the deployer-prefixed native name, the
    // client-side end time bounds the window, and a missing volume stays null.
    let rows = h
        .post(
            "fetchOHLCV",
            json!({"symbol": perp(Some("abc"), "abc:BTC"), "timeframe": "1h", "since": T, "limit": 5, "params": {"endTime": T + 2 * HOUR}}),
            StatusCode::OK,
        )
        .await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1][5], Value::Null);
    assert_eq!(rows[0][0], T);
    assert_eq!(rows[2][0], T + 2 * HOUR);
    let calls = h.calls("candleSnapshot").await;
    assert_eq!(calls[1]["req"]["coin"], "abc:BTC");
    assert_eq!(calls[1]["req"]["startTime"], T);
    assert_eq!(calls[1]["req"]["endTime"], T + 2 * HOUR);
    let sibling = h.post(
        "fetchOHLCV",
        json!({"symbol":perp(Some("xyz"), "xyz:BTC"),"timeframe":"1h","since":T,"params":{"dex":"xyz"}}),
        StatusCode::OK,
    ).await;
    assert_eq!(sibling, json!([[T, 200.0, 201.0, 199.0, 200.0, 9.0]]));
    assert_eq!(h.calls("candleSnapshot").await[2]["req"]["coin"], "xyz:BTC");

    // Spot candles key on the stock market id.
    let rows = h
        .post(
            "fetchOHLCV",
            json!({"symbol": spot(1), "timeframe": "1h", "since": T, "limit": 2}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(h.calls("candleSnapshot").await[3]["req"]["coin"], "@1");

    // Supported boundary: the limit is clamped to the stock 5000-candle bound.
    h.post(
        "fetchOHLCV",
        json!({"symbol": "BTC", "timeframe": "1h", "since": T, "limit": 5001}),
        StatusCode::OK,
    )
    .await;
    let calls = h.calls("candleSnapshot").await;
    assert_eq!(
        calls[4]["req"]["endTime"].as_u64().unwrap()
            - calls[4]["req"]["startTime"].as_u64().unwrap(),
        5000 * HOUR
    );

    let before = h.mock.calls.lock().await.len();
    for (body, status) in [
        // Hyperliquid publishes no price-source selector for candles.
        (
            json!({"symbol": "BTC", "timeframe": "1h", "params": {"price": "mark"}}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            json!({"symbol": "BTC", "timeframe": "1h", "params": {"price": "index"}}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            json!({"symbol": "BTC", "timeframe": "7m"}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            json!({"symbol": "BTC", "timeframe": "1h", "since": T + 1, "params": {"until": T}}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"symbol": "NOPE", "timeframe": "1h"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        h.post("fetchOHLCV", body, status).await;
    }
    assert_eq!(h.mock.calls.lock().await.len(), before);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hyperliquid_books_route_primary_hip3_and_spot_with_depth_bounds() {
    let h = Harness::start().await;

    let book = h
        .post(
            "fetchOrderBook",
            json!({"symbol": "BTC", "limit": 20}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(book["symbol"], "BTC/USDC:USDC");
    assert_eq!(book["bids"], json!([[60000.0, 1.5], [59999.0, 0.5]]));
    assert_eq!(book["asks"], json!([[60001.0, 1.0]]));
    assert_eq!(book["timestamp"], T);
    assert_eq!(book["nonce"], Value::Null); // stock publishes no book nonce
    let calls = h.calls("l2Book").await;
    assert_eq!(calls[0]["coin"], "BTC");
    assert!(calls[0].get("dex").is_none());

    let book = h
        .post(
            "fetchOrderBook",
            json!({"symbol": perp(Some("abc"), "abc:BTC"), "limit": 1}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(book["symbol"], "ABC-BTC/USDH:USDH");
    assert_eq!(book["bids"], json!([[100.0, 1.5]]));
    assert_eq!(book["asks"], json!([[101.0, 1.0]]));
    assert_eq!(h.calls("l2Book").await[1]["coin"], "abc:BTC");

    // Aggregation is nested the way stock expects: at the request root.
    let book = h
        .post(
            "fetchOrderBook",
            json!({"symbol": perp(Some("xyz"), "xyz:BTC"), "limit": 20, "params": {"nSigFigs": 5, "mantissa": 2}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(book["symbol"], "XYZ-BTC/USDC:USDC");
    let call = h.calls("l2Book").await[2].clone();
    assert_eq!(call["coin"], "xyz:BTC");
    assert_eq!(call["nSigFigs"], 5);
    assert_eq!(call["mantissa"], 2);

    let book = h
        .post(
            "fetchOrderBook",
            json!({"symbol": spot(1), "limit": 20}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(book["symbol"], "HFUN/USDC");
    assert_eq!(h.calls("l2Book").await[3]["coin"], "@1");

    let before = h.mock.calls.lock().await.len();
    h.post(
        "fetchOrderBook",
        json!({"symbol": "BTC", "limit": 21}),
        StatusCode::NOT_IMPLEMENTED,
    )
    .await;
    h.post(
        "fetchOrderBook",
        json!({"symbol": "BTC", "limit": 0}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchOrderBook",
        json!({"symbol": "BTC", "params": {"nSigFigs": 6}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchOrderBook",
        json!({"symbol": "BTC", "params": {"mantissa": 2}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    h.post(
        "fetchOrderBook",
        json!({"symbol": "BTC", "params": {"dex": "xyz"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(h.mock.calls.lock().await.len(), before);
    h.stop().await;
}
