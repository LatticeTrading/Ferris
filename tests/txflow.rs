//! Deterministic TxFlow peers: `/info`, `/ws` and `/api-ws` served locally
//! from mainnet captures (tests/fixtures/txflow, 2026-11-17,
//! x-api-version 2026-08-16). No live network.
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::{
        traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
        txflow::TxflowExchange,
    },
    models::{
        FetchMarketStatsParams, FetchMarketsParams, FetchOhlcvParams, FetchOrderBookParams,
        FetchTickersParams, FetchTradesParams, MarketStatsCapabilities, MarketStatsFieldName,
        MarketStatsFieldState, RealtimeMode,
    },
    realtime::{
        NativeRealtimeOwner, RealtimeChannel, RealtimeSubscription, RealtimeTopic, RealtimeUpdate,
    },
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
    time::timeout,
};

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/tests/fixtures/txflow/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}
fn fixture_lines(name: &str) -> Vec<Value> {
    let path = format!(
        "{}/tests/fixtures/txflow/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[derive(Clone)]
struct ServerState {
    node: mpsc::Sender<WebSocket>,
    api: mpsc::Sender<WebSocket>,
    log: Arc<Mutex<Vec<Value>>>,
    tags_fail: Arc<AtomicBool>,
}
struct Mock {
    api: String,
    ws: String,
    api_ws: String,
    node: mpsc::Receiver<WebSocket>,
    api_peers: mpsc::Receiver<WebSocket>,
    log: Arc<Mutex<Vec<Value>>>,
    tags_fail: Arc<AtomicBool>,
    task: JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn info(
    State(s): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let version = headers
        .get("x-api-version")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    s.log
        .lock()
        .await
        .push(json!({"body": body, "version": version}));
    let ok = |data: Value| Json(json!({"success":true,"code":0,"data":data})).into_response();
    match body["type"].as_str().unwrap_or_default() {
        "perpMeta" => Json(fixture("perpMeta.json")).into_response(),
        "marketTags" if s.tags_fail.load(Ordering::SeqCst) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
        }
        "marketTags" => Json(fixture("marketTags.json")).into_response(),
        "l2Book" if body["coin"] == "1" => Json(fixture("l2Book.json")).into_response(),
        "l2Book" => Json(fixture("l2Book_unknown.json")).into_response(),
        "trades" if body["coin"] == "1" => Json(fixture("trades.json")).into_response(),
        "candleSnapshot" => {
            let req = &body["req"];
            let step = match req["interval"].as_str().unwrap() {
                "1m" => 60_000,
                "1h" => 3_600_000,
                other => panic!("unexpected interval {other}"),
            };
            let (start, end) = (
                req["startTime"].as_u64().unwrap(),
                req["endTime"].as_u64().unwrap(),
            );
            // Upstream answers an oversized window with an empty success.
            if (end - start) / step > 3000 {
                return ok(json!([]));
            }
            let first = start.div_ceil(step) * step;
            let bars: Vec<Value> = (0..)
                .map(|n| first + n * step)
                .take_while(|t| *t < end)
                .map(|t| {
                    json!({"t":t,"T":t+step-1,"o":"100","h":"101.5","l":"99","c":"100.5","v":"2.5","n":3,"s":req["coin"],"i":req["interval"],"priceType":"last"})
                })
                .collect();
            ok(json!(bars))
        }
        _ => (StatusCode::FORBIDDEN, Json(fixture("unknown_type.json"))).into_response(),
    }
}

impl Mock {
    async fn start() -> Self {
        let (node_tx, node) = mpsc::channel(32);
        let (api_tx, api_peers) = mpsc::channel(32);
        let log = Arc::new(Mutex::new(Vec::new()));
        let tags_fail = Arc::new(AtomicBool::new(false));
        let state = ServerState {
            node: node_tx,
            api: api_tx,
            log: log.clone(),
            tags_fail: tags_fail.clone(),
        };
        let app = Router::new()
            .route("/info", post(info))
            .route(
                "/ws",
                get(
                    |ws: WebSocketUpgrade, State(s): State<ServerState>| async move {
                        ws.on_upgrade(move |socket| async move {
                            let _ = s.node.send(socket).await;
                        })
                    },
                ),
            )
            .route(
                "/api-ws",
                get(
                    |ws: WebSocketUpgrade, State(s): State<ServerState>| async move {
                        ws.on_upgrade(move |socket| async move {
                            let _ = s.api.send(socket).await;
                        })
                    },
                ),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            api: format!("http://{addr}"),
            ws: format!("ws://{addr}/ws"),
            api_ws: format!("ws://{addr}/api-ws"),
            node,
            api_peers,
            log,
            tags_fail,
            task,
        }
    }
    fn exchange(&self) -> TxflowExchange {
        TxflowExchange::with_urls(&self.api, &self.ws, &self.api_ws, "mock").unwrap()
    }
    async fn node_peer(&mut self) -> Peer {
        Peer::new(
            timeout(Duration::from_secs(3), self.node.recv())
                .await
                .unwrap()
                .unwrap(),
        )
    }
    async fn api_peer(&mut self) -> Peer {
        Peer::new(
            timeout(Duration::from_secs(3), self.api_peers.recv())
                .await
                .unwrap()
                .unwrap(),
        )
    }
    async fn types(&self) -> Vec<String> {
        self.log
            .lock()
            .await
            .iter()
            .map(|e| e["body"]["type"].as_str().unwrap().to_string())
            .collect()
    }
}

struct Peer {
    socket: WebSocket,
    pings: usize,
}
impl Peer {
    fn new(socket: WebSocket) -> Self {
        Self { socket, pings: 0 }
    }
    /// Next control frame other than a keepalive ping.
    async fn control(&mut self) -> Value {
        loop {
            let frame = timeout(Duration::from_secs(3), self.socket.next())
                .await
                .expect("expected a control frame")
                .unwrap()
                .unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            let v: Value = serde_json::from_str(&text).unwrap();
            if v == json!({"method":"ping"}) {
                self.pings += 1;
                continue;
            }
            return v;
        }
    }
    async fn closed(&mut self) {
        loop {
            match timeout(Duration::from_secs(3), self.socket.next())
                .await
                .expect("socket did not close")
            {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => return,
                Some(Ok(Message::Text(t))) if t.contains("\"ping\"") => {}
                Some(other) => panic!("unexpected frame before close: {other:?}"),
            }
        }
    }
    async fn send(&mut self, v: &Value) {
        self.socket
            .send(Message::Text(v.to_string()))
            .await
            .unwrap();
    }
    async fn quiet(&mut self, ms: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        loop {
            match tokio::time::timeout_at(deadline, self.socket.next()).await {
                Err(_) => return,
                Ok(Some(Ok(Message::Text(t)))) if t.contains("\"ping\"") => self.pings += 1,
                Ok(other) => panic!("unexpected upstream frame: {other:?}"),
            }
        }
    }
}

fn topic(symbol: &str, params: Value) -> RealtimeTopic {
    RealtimeTopic {
        exchange: "txflow".into(),
        symbol: symbol.into(),
        params,
    }
}
async fn sub(
    e: &TxflowExchange,
    c: RealtimeChannel,
    s: &str,
    p: Value,
) -> Result<RealtimeSubscription, ExchangeError> {
    e.prepare_native(c, topic(s, p)).await?.subscribe().await
}
async fn recv(s: &mut RealtimeSubscription) -> RealtimeUpdate {
    timeout(Duration::from_secs(3), s.receiver.recv())
        .await
        .expect("expected a publication")
        .unwrap()
}

#[tokio::test]
async fn catalog_labels_identity_and_version_header() {
    let mock = Mock::start().await;
    let e = mock.exchange();
    let markets = e
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: false,
        })
        .await
        .unwrap();
    let by = |base: &str| markets.iter().find(|m| m.base == base).cloned();
    let btc = by("BTC").unwrap();
    assert_eq!(btc.symbol, "BTC/USDC:USDC");
    assert_eq!(btc.info.category.as_deref(), Some("crypto"));
    assert_eq!(btc.identity.as_ref().unwrap().exchange_market_id, "1");
    assert_eq!(btc.identity.as_ref().unwrap().category, None);
    assert_eq!(btc.identity.as_ref().unwrap().dex, None);
    for (base, label) in [
        ("TSLA", "stock"),
        ("SPY", "index"),
        ("XAU", "commodity"),
        ("CL", "commodity"),
        ("EURUSD", "fx"),
        ("OPENAI", "pre-ipo"),
        ("PEPE", "crypto"),
    ] {
        assert_eq!(
            by(base).unwrap().info.category.as_deref(),
            Some(label),
            "{base}"
        );
    }
    // Negative szDecimals: exact steps survive in native info.
    let pepe = by("PEPE").unwrap();
    assert_eq!(pepe.info.native.as_ref().unwrap()["sizeStep"], "100000");
    assert!(
        by("TON").is_none(),
        "delisted markets are filtered by default"
    );
    let all = e
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: true,
        })
        .await
        .unwrap();
    let ton = all.iter().find(|m| m.base == "TON").unwrap();
    assert!(!ton.active);
    let log = mock.log.lock().await.clone();
    assert!(log.iter().all(|e| e["version"] == "2026-08-16"));

    // marketTags failing on a fresh adapter: labels degrade, identity does not.
    mock.tags_fail.store(true, Ordering::SeqCst);
    let fresh = mock.exchange();
    let degraded = fresh
        .fetch_markets(FetchMarketsParams {
            params: Value::Null,
            include_inactive: false,
        })
        .await
        .unwrap();
    assert_eq!(degraded.len(), markets.len());
    for (a, b) in markets.iter().zip(&degraded) {
        assert_eq!(a.identity, b.identity);
        assert_eq!(b.info.category, None);
    }
}

#[tokio::test]
async fn rest_book_trades_candles_and_spot_rejection() {
    let mock = Mock::start().await;
    let e = mock.exchange();
    let book = |symbol: &str, limit| FetchOrderBookParams {
        symbol: symbol.into(),
        limit,
        params: Value::Null,
    };
    let b = e.fetch_order_book(book("BTC", None)).await.unwrap();
    assert_eq!((b.bids.len(), b.asks.len()), (20, 20));
    assert!(b.bids[0].0 < b.asks[0].0);
    assert_eq!(b.nonce, None);
    assert!(b.timestamp.is_some());
    let b = e.fetch_order_book(book("1", Some(5))).await.unwrap();
    assert_eq!(b.bids.len(), 5);
    let calls = mock.types().await.len();
    for limit in [0, 21] {
        let err = e
            .fetch_order_book(book("BTC", Some(limit)))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ExchangeError::UnsupportedFeature(ref m) if m.contains("20")),
            "{err}"
        );
    }
    // Spot input never reaches l2Book/trades, which would serve spot data.
    for spot in ["BTC/USDC", "10000"] {
        assert!(matches!(
            e.fetch_order_book(book(spot, None)).await,
            Err(ExchangeError::BadSymbol(_))
        ));
        assert!(matches!(
            e.fetch_trades(FetchTradesParams {
                symbol: spot.into(),
                since: None,
                limit: None,
                params: Value::Null
            })
            .await,
            Err(ExchangeError::BadSymbol(_))
        ));
    }
    assert_eq!(mock.types().await.len(), calls, "rejections are local");

    let trades = e
        .fetch_trades(FetchTradesParams {
            symbol: "BTC/USDC:USDC".into(),
            since: None,
            limit: None,
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(trades.len(), 300);
    assert!(trades.windows(2).all(|w| w[0].timestamp <= w[1].timestamp));
    let t = &trades[0];
    assert!(t.id.as_deref().unwrap().parse::<u64>().is_ok());
    assert!(matches!(t.side.as_deref(), Some("buy" | "sell")));
    assert_eq!(t.taker_or_maker.as_deref(), Some("taker"));
    let latest = e
        .fetch_trades(FetchTradesParams {
            symbol: "BTC".into(),
            since: None,
            limit: Some(10),
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(latest.last().unwrap().id, trades.last().unwrap().id);
    let since = trades[100].timestamp.unwrap();
    let after = e
        .fetch_trades(FetchTradesParams {
            symbol: "BTC".into(),
            since: Some(since),
            limit: Some(5),
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(after.len(), 5);
    assert!(after.iter().all(|t| t.timestamp.unwrap() >= since));

    // Paging across the 3000-bar cap: sequential, non-overlapping, deduped.
    mock.log.lock().await.clear();
    let since = 1_700_000_000_000 / 60_000 * 60_000;
    let bars = e
        .fetch_ohlcv(FetchOhlcvParams {
            symbol: "BTC".into(),
            timeframe: Some("1m".into()),
            since: Some(since),
            limit: Some(6000),
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(bars.len(), 6000);
    assert_eq!(bars[0].0, since);
    assert!(bars.windows(2).all(|w| w[1].0 == w[0].0 + 60_000));
    assert_eq!(bars[0].5, Some(2.5));
    let log = mock.log.lock().await.clone();
    assert_eq!(log.len(), 3);
    let mut prev_end = since;
    for entry in &log {
        let req = &entry["body"]["req"];
        assert_eq!(req["coin"], "1");
        let (s, end) = (
            req["startTime"].as_u64().unwrap(),
            req["endTime"].as_u64().unwrap(),
        );
        assert_eq!(s, prev_end);
        assert!((end - s) / 60_000 <= 2500);
        prev_end = end;
    }
    let recent = e
        .fetch_ohlcv(FetchOhlcvParams {
            symbol: "BTC".into(),
            timeframe: Some("1h".into()),
            since: None,
            limit: Some(3),
            params: Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(recent.len(), 3);
    let calls = mock.types().await.len();
    assert!(matches!(
        e.fetch_ohlcv(FetchOhlcvParams {
            symbol: "BTC".into(),
            timeframe: Some("7m".into()),
            since: None,
            limit: None,
            params: Value::Null,
        })
        .await,
        Err(ExchangeError::UnsupportedFeature(_))
    ));
    assert_eq!(
        mock.types().await.len(),
        calls,
        "interval validated locally"
    );
}

#[tokio::test]
async fn tickers_and_stats_share_one_ctx_subscription_without_rest_fanout() {
    let mut mock = Mock::start().await;
    let e = mock.exchange();
    let tickers = {
        let e = e.clone();
        tokio::spawn(async move {
            e.fetch_tickers(FetchTickersParams {
                symbols: None,
                params: Value::Null,
            })
            .await
        })
    };
    let mut peer = mock.node_peer().await;
    assert_eq!(
        peer.control().await,
        json!({"method":"subscribe","type":"allAssetCtxs","subscription":{"type":"allAssetCtxs"}})
    );
    peer.send(&fixture("ws_allAssetCtxs.json")).await;
    let tickers = tickers.await.unwrap().unwrap();
    // 13 fixture markets, of which TON and NG are delisted.
    assert_eq!(tickers.len(), 11);
    let btc = tickers
        .iter()
        .find(|t| t.symbol == "BTC/USDC:USDC")
        .unwrap();
    assert!(btc.last.is_some() && btc.average.is_some());
    assert_eq!(
        (btc.bid, btc.ask),
        (None, None),
        "impactPxs are not bid/ask"
    );
    assert_eq!(
        (btc.base_volume, btc.quote_volume, btc.change),
        (None, None, None)
    );

    let stats = e
        .fetch_market_stats(FetchMarketStatsParams {
            params: Value::Null,
            open_interest_market_ids: vec![],
            include_bulk: true,
        })
        .await
        .unwrap();
    assert_eq!(stats.rows.len(), 11);
    for row in &stats.rows {
        assert_eq!(
            row.fields[&MarketStatsFieldName::Volume24h].state,
            MarketStatsFieldState::Unsupported
        );
        assert_eq!(
            row.fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
        assert!(row.market.info.native.is_none());
    }
    let MarketStatsCapabilities::Supported(caps) = MarketStatsSource::capabilities(&e) else {
        panic!("stats supported")
    };
    assert_eq!(
        caps.fields[&ferris_market_data_backend::models::UnifiedMarketType::Perp]
            [&MarketStatsFieldName::Volume24h]
            .state,
        ferris_market_data_backend::models::CapabilityState::Unsupported
    );
    // No per-market REST anywhere: only the catalog was fetched.
    let types = mock.types().await;
    assert!(
        types.iter().all(|t| t == "perpMeta" || t == "marketTags"),
        "{types:?}"
    );
    // A second reader reuses the same subscription.
    e.fetch_tickers(FetchTickersParams {
        symbols: Some(vec!["TSLA".into()]),
        params: Value::Null,
    })
    .await
    .unwrap();
    peer.quiet(200).await;
    assert_eq!(
        MarketDataExchange::capabilities(&e)
            .realtime
            .marketstats
            .mode,
        Some(RealtimeMode::Polling)
    );
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn book_feed_validates_sequence_and_resyncs_on_gap() {
    let mut mock = Mock::start().await;
    let e = mock.exchange();
    let mut a = sub(&e, RealtimeChannel::OrderBook, "BTC", json!({"levels":5}))
        .await
        .unwrap();
    let mut b = sub(&e, RealtimeChannel::OrderBook, "1", json!({"depth":200}))
        .await
        .unwrap();
    assert!(
        sub(&e, RealtimeChannel::OrderBook, "BTC", json!({"depth":201}))
            .await
            .is_err()
    );
    assert!(matches!(
        sub(&e, RealtimeChannel::OrderBook, "BTC/USDC", json!({})).await,
        Err(ExchangeError::BadSymbol(_))
    ));
    assert!(matches!(
        sub(&e, RealtimeChannel::Trades, "10000", json!({})).await,
        Err(ExchangeError::BadSymbol(_))
    ));
    let mut peer = mock.node_peer().await;
    assert_eq!(
        peer.control().await,
        json!({"method":"subscribe","type":"l2BookDepth","subscription":{"type":"l2BookDepth","coin":"1"}})
    );
    peer.quiet(150).await; // one upstream subscription for both depths
    let frames = fixture_lines("ws_l2BookDepth.jsonl");
    // Replay the live capture: snapshot, deltas, mid-stream snapshots.
    for f in &frames[..12] {
        peer.send(f).await;
    }
    let mut last = None;
    for _ in 0..12 {
        let RealtimeUpdate::OrderBook(book) = recv(&mut b).await else {
            panic!("book")
        };
        assert!(book.bids[0].0 < book.asks[0].0);
        last = book.nonce;
    }
    assert_eq!(last, frames[11]["data"]["seq"].as_u64());
    let RealtimeUpdate::OrderBook(first) = recv(&mut a).await else {
        panic!("book")
    };
    assert_eq!(first.bids.len(), 200, "projection happens downstream");
    while timeout(Duration::from_millis(50), a.receiver.recv())
        .await
        .is_ok()
    {}

    // Gap: prevSeq does not chain -> error, unsubscribe + resubscribe.
    let mut gap = frames[12].clone();
    gap["data"]["type"] = json!("delta");
    gap["data"]["seq"] = json!(999_999_999);
    gap["data"]["prevSeq"] = json!(999_999_998);
    peer.send(&gap).await;
    assert!(matches!(recv(&mut a).await, RealtimeUpdate::Error(_)));
    assert_eq!(peer.control().await["method"], "unsubscribe");
    assert_eq!(peer.control().await["method"], "subscribe");
    // Deltas before the fresh snapshot are ignored, never delivered.
    let mut stale = frames[1].clone();
    stale["data"]["prevSeq"] = json!(1);
    peer.send(&stale).await;
    peer.send(&frames[0]).await;
    let RealtimeUpdate::OrderBook(fresh) = recv(&mut a).await else {
        panic!("fresh snapshot after resync")
    };
    assert_eq!(fresh.nonce, frames[0]["data"]["seq"].as_u64());

    // Partial release unsubscribes; the final release closes the idle socket.
    let t = sub(&e, RealtimeChannel::Trades, "ETH", json!({}))
        .await
        .unwrap();
    assert_eq!(peer.control().await["subscription"]["type"], "trades");
    drop((a, b));
    let unsub = peer.control().await;
    assert_eq!(unsub["method"], "unsubscribe");
    assert_eq!(unsub["type"], "l2BookDepth");
    drop(t);
    peer.closed().await;
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn trades_route_by_instrument_id_on_one_socket_and_resubscribe_on_reconnect() {
    let mut mock = Mock::start().await;
    let e = mock.exchange();
    let mut btc = sub(&e, RealtimeChannel::Trades, "BTC/USDC:USDC", json!({}))
        .await
        .unwrap();
    let mut eth = sub(&e, RealtimeChannel::Trades, "ETH", json!({}))
        .await
        .unwrap();
    let _book = sub(&e, RealtimeChannel::OrderBook, "ETH", json!({}))
        .await
        .unwrap();
    let mut peer = mock.node_peer().await;
    let mut subs = vec![];
    for _ in 0..3 {
        subs.push(peer.control().await);
    }
    assert!(subs.iter().all(|s| s["type"] == s["subscription"]["type"]));
    let frames = fixture_lines("ws_trades.jsonl");
    peer.send(&frames[0]).await;
    let mut eth_trade = frames[1].clone();
    for row in eth_trade["data"].as_array_mut().unwrap() {
        row["instrumentId"] = json!(2);
        row["coin"] = json!("ETH-USDC");
    }
    peer.send(&eth_trade).await;
    let RealtimeUpdate::Trades(t) = recv(&mut btc).await else {
        panic!("trades")
    };
    assert_eq!(t[0].symbol.as_deref(), Some("BTC/USDC:USDC"));
    assert_eq!(
        t[0].id.as_deref(),
        frames[0]["data"][0]["tid"]
            .as_u64()
            .map(|v| v.to_string())
            .as_deref()
    );
    let RealtimeUpdate::Trades(t) = recv(&mut eth).await else {
        panic!("trades")
    };
    assert_eq!(t[0].symbol.as_deref(), Some("ETH/USDC:USDC"));
    assert!(
        timeout(Duration::from_millis(100), btc.receiver.recv())
            .await
            .is_err(),
        "ETH trades must not reach the BTC feed"
    );
    assert!(mock.node.try_recv().is_err(), "one socket per endpoint");

    // Reconnect: every desired feed is re-subscribed on the new socket.
    drop(peer);
    assert!(matches!(recv(&mut btc).await, RealtimeUpdate::Error(_)));
    let mut peer = mock.node_peer().await;
    let mut again = vec![];
    for _ in 0..3 {
        again.push(peer.control().await);
    }
    let key = |v: &Value| v["subscription"].to_string();
    let (mut a, mut b): (Vec<_>, Vec<_>) = (
        subs.iter().map(key).collect(),
        again.iter().map(key).collect(),
    );
    a.sort();
    b.sort();
    assert_eq!(a, b);
    e.shutdown_native().await.unwrap();
    assert!(timeout(Duration::from_secs(1), btc.receiver.recv())
        .await
        .unwrap()
        .is_err());
}

#[tokio::test]
async fn candles_use_api_ws_and_accept_both_echo_forms() {
    let mut mock = Mock::start().await;
    let e = mock.exchange();
    assert!(matches!(
        sub(&e, RealtimeChannel::Ohlcv, "BTC", json!({"timeframe":"7m"})).await,
        Err(ExchangeError::UnsupportedFeature(_))
    ));
    let mut c = sub(&e, RealtimeChannel::Ohlcv, "BTC", json!({"interval":"1m"}))
        .await
        .unwrap();
    let mut peer = mock.api_peer().await;
    let req = peer.control().await;
    assert_eq!(
        req,
        json!({"method":"subscribe","subscription":{"type":"candle","coin":"1","interval":"1m"}})
    );
    let mut bars = 0;
    for f in fixture_lines("ws_api_candle.jsonl") {
        peer.send(&f).await;
        if f["channel"] == "candle" {
            bars += 1;
        }
    }
    let mut window = vec![];
    for _ in 0..bars {
        let RealtimeUpdate::Ohlcv(w) = recv(&mut c).await else {
            panic!("ohlcv")
        };
        window = w.to_vec();
    }
    // Two distinct bars across the rollover; the current one replaced in place.
    assert_eq!(window.len(), 2);
    assert!(window[0].0 < window[1].0);
    assert_eq!(window[1].4, 64638.4);
    drop(c);
    peer.closed().await;
    e.shutdown_native().await.unwrap();
}

/// Mock enforces TxFlow's ~16 s idle close; a quiet feed must survive it.
#[tokio::test]
async fn quiet_socket_survives_idle_close_with_client_pings() {
    let mut mock = Mock::start().await;
    let e = mock.exchange();
    let _t = sub(&e, RealtimeChannel::Trades, "BTC", json!({}))
        .await
        .unwrap();
    let mut peer = mock.node_peer().await;
    peer.control().await;
    let started = tokio::time::Instant::now();
    let mut last_client_frame = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(21) {
        match timeout(Duration::from_secs(1), peer.socket.next()).await {
            Err(_) => {}
            Ok(Some(Ok(Message::Text(t)))) => {
                assert_eq!(
                    serde_json::from_str::<Value>(&t).unwrap(),
                    json!({"method":"ping"})
                );
                peer.pings += 1;
                last_client_frame = tokio::time::Instant::now();
                peer.send(&json!({"method":"PONG"})).await; // must not reach dispatch
            }
            Ok(other) => panic!("unexpected frame: {other:?}"),
        }
        assert!(
            last_client_frame.elapsed() < Duration::from_secs(16),
            "server would have closed the idle socket"
        );
    }
    assert!(peer.pings >= 2);
    assert!(mock.node.try_recv().is_err(), "no reconnect happened");
    e.shutdown_native().await.unwrap();
}
