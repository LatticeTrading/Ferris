//! Deterministic public HTTP + WebSocket peers. Fixtures follow Perpl api-docs
//! websocket.md/types.md (inspected 2026-07-20); no live markets/network required.
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    routing::get,
    Json, Router,
};
use ferris_market_data_backend::{
    exchanges::{perpl::PerplExchange, traits::MarketDataExchange},
    models::{CapabilityState, FetchTradesParams, RealtimeMode},
    realtime::{
        NativeRealtimeOwner, NativeSubscriptionPlan, RealtimeChannel, RealtimeSubscription,
        RealtimeTopic, RealtimeUpdate,
    },
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::timeout,
};

#[derive(Clone)]
struct ServerState {
    chain: u64,
    peers: mpsc::Sender<WebSocket>,
    catalogs: Arc<AtomicUsize>,
}
struct Mock {
    api: String,
    ws: String,
    peers: mpsc::Receiver<WebSocket>,
    task: JoinHandle<()>,
    catalogs: Arc<AtomicUsize>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Mock {
    async fn start(chain: u64) -> Self {
        let (tx, peers) = mpsc::channel(32);
        let catalogs = Arc::new(AtomicUsize::new(0));
        let state = ServerState {
            chain,
            peers: tx,
            catalogs: catalogs.clone(),
        };
        let app=Router::new().route("/v1/pub/context",get(|State(s):State<ServerState>|async move {
            s.catalogs.fetch_add(1,Ordering::SeqCst);
            let mut markets=(1..=32).map(|id|fixture_market(id,&format!("M{id}"),2,3)).collect::<Vec<_>>();
            markets.push(fixture_market(42,"BTC",2,3));markets.push(fixture_market(73,"ETH",3,2));
            Json(json!({"chain":{"chain_id":s.chain},"instances":[{"id":7,"collateral_token_id":9}],"tokens":[{"id":9,"decimals":7}],"markets":markets}))
        })).route("/ws/v1/market-data",get(|ws:WebSocketUpgrade,State(s):State<ServerState>|async move {
            ws.on_upgrade(move |socket|async move {let _=s.peers.send(socket).await;})
        })).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            api: format!("http://{addr}"),
            ws: format!("ws://{addr}/ws/v1/market-data"),
            peers,
            task,
            catalogs,
        }
    }
    fn exchange(&self, chain: u64) -> PerplExchange {
        PerplExchange::with_urls(&self.api, &self.ws, chain).unwrap()
    }
    async fn peer(&mut self) -> Peer {
        Peer {
            socket: timeout(Duration::from_secs(3), self.peers.recv())
                .await
                .unwrap()
                .unwrap(),
            ids: HashMap::new(),
            next: 0,
        }
    }
}
fn fixture_market(id: u64, name: &str, p: u32, s: u32) -> Value {
    json!({"id":id,"perpetual_id":id,"instance_id":7,"name":name,"size_units":name,"funding_interval_sec":1234,"config":{"is_open":true,"price_decimals":p,"size_decimals":s}})
}
struct Peer {
    socket: WebSocket,
    ids: HashMap<String, u64>,
    next: u64,
}
impl Peer {
    async fn request(&mut self) -> Value {
        let frame = timeout(Duration::from_secs(3), self.socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = frame else {
            panic!("expected subscription frame: {frame:?}");
        };
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["mt"], 5);
        v
    }
    async fn send(&mut self, v: Value) {
        self.socket
            .send(Message::Text(v.to_string()))
            .await
            .unwrap();
    }
    async fn ack(&mut self, v: &Value) {
        let mut subs = vec![];
        for entry in v["subs"].as_array().unwrap() {
            let stream = entry["stream"].as_str().unwrap();
            if entry["subscribe"] == true {
                assert!(
                    !self.ids.contains_key(stream),
                    "duplicate upstream subscription: {stream}"
                );
                self.next += 1;
                self.ids.insert(stream.into(), self.next);
                subs.push(json!({"stream":stream,"sid":self.next,"status":{"code":0}}));
            } else {
                assert!(self.ids.remove(stream).is_some());
                subs.push(json!({"stream":stream,"status":{"code":0}}));
            }
        }
        assert!(self.ids.len() <= 16);
        self.send(json!({"mt":6,"subs":subs})).await;
    }
    async fn data(&mut self, stream: &str, mut v: Value) {
        v["sid"] = json!(self.ids[stream]);
        self.send(v).await;
    }
    async fn quiet(&mut self) {
        assert!(
            timeout(Duration::from_millis(80), self.socket.next())
                .await
                .is_err(),
            "unexpected upstream frame"
        );
    }
    async fn closed(&mut self) {
        let result = timeout(Duration::from_secs(3), self.socket.next())
            .await
            .unwrap();
        assert!(
            matches!(result, None | Some(Ok(Message::Close(_))) | Some(Err(_))),
            "socket did not close: {result:?}"
        );
    }
}
async fn plan(
    e: &PerplExchange,
    c: RealtimeChannel,
    s: &str,
    p: Value,
) -> Box<dyn NativeSubscriptionPlan> {
    e.prepare_native(
        c,
        RealtimeTopic {
            exchange: "perpl".into(),
            symbol: s.into(),
            params: p,
        },
    )
    .await
    .unwrap()
}
async fn sub(e: &PerplExchange, c: RealtimeChannel, s: &str, p: Value) -> RealtimeSubscription {
    plan(e, c, s, p).await.subscribe().await.unwrap()
}
async fn recv(s: &mut RealtimeSubscription) -> RealtimeUpdate {
    timeout(Duration::from_secs(3), s.receiver.recv())
        .await
        .unwrap()
        .unwrap()
}
fn book(mt: u64, sn: u64, bid: Value, ask: Value) -> Value {
    json!({"mt":mt,"sn":sn,"at":{"b":sn,"t":123456},"bid":bid,"ask":ask})
}
fn snapshot() -> Value {
    book(
        15,
        100,
        json!([{"p":10000,"s":2000,"o":2},{"p":9900,"s":3000,"o":1}]),
        json!([{"p":10100,"s":4000,"o":1}]),
    )
}
fn trade(mt: u64, log: u64) -> Value {
    json!({"mt":mt,"d":[{"at":{"b":77,"t":123456,"tx":3,"txid":"0xabc","l":log},"p":10025,"s":2500,"sd":2}]})
}
fn candle(t: u64, c: u64) -> Value {
    json!({"t":t,"o":10000,"h":11000,"l":9000,"c":c,"v":"25000000","n":3})
}

#[tokio::test]
async fn resolves_numeric_identity_shares_depths_batches_and_cleans_up_final_lease() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut a = sub(&e, RealtimeChannel::OrderBook, "BTC", json!({"levels":1})).await;
    let mut b = sub(&e, RealtimeChannel::OrderBook, "42", json!({"depth":2})).await;
    let mut t = sub(&e, RealtimeChannel::Trades, "BTC/AUSD:AUSD", json!({})).await;
    assert_ne!(a.key, b.key);
    assert_eq!(a.levels_limit, 1);
    assert_eq!(b.levels_limit, 2);
    let mut peer = mock.peer().await;
    let req = peer.request().await;
    assert_eq!(req["subs"].as_array().unwrap().len(), 2);
    peer.ack(&req).await;
    assert!(peer.ids.contains_key("order-book@42"));
    assert!(peer.ids.contains_key("trades@42"));
    peer.data("order-book@42", snapshot()).await;
    for s in [&mut a, &mut b] {
        let RealtimeUpdate::OrderBook(v) = recv(s).await else {
            panic!()
        };
        assert_eq!(v.bids, vec![(100., 2.), (99., 3.)]);
        assert_eq!(v.nonce, Some(100));
        assert_eq!(v.timestamp, Some(123456));
    }
    // Late subscriber gets the maintained baseline without any network request.
    let mut late = sub(
        &e,
        RealtimeChannel::OrderBook,
        "BTC/AUSD:AUSD",
        json!({"limit":1}),
    )
    .await;
    assert_eq!(a.key, late.key);
    assert!(matches!(
        recv(&mut late).await,
        RealtimeUpdate::OrderBook(_)
    ));
    drop(a);
    drop(late);
    peer.quiet().await;
    peer.data("trades@42", trade(17, 1)).await;
    assert!(matches!(recv(&mut t).await, RealtimeUpdate::Trades(_)));
    drop(b);
    let req = peer.request().await;
    assert_eq!(
        req["subs"],
        json!([{"stream":"order-book@42","subscribe":false}])
    );
    peer.ack(&req).await;
    drop(t);
    peer.closed().await;
    assert_eq!(mock.catalogs.load(Ordering::SeqCst), 1);
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn networks_and_market_ids_timeframes_are_isolated() {
    let mut main = Mock::start(143).await;
    let mut test = Mock::start(10143).await;
    let a = main.exchange(143);
    let b = test.exchange(10143);
    let mut main_sub = sub(&a, RealtimeChannel::Trades, "42", json!({})).await;
    let mut test_sub = sub(&b, RealtimeChannel::Trades, "42", json!({})).await;
    assert_ne!(main_sub.key, test_sub.key);
    let mut eth = sub(&a, RealtimeChannel::Trades, "ETH", json!({})).await;
    let mut one = sub(&a, RealtimeChannel::Ohlcv, "42", json!({"timeframe":"1m"})).await;
    let mut five = sub(&a, RealtimeChannel::Ohlcv, "42", json!({"interval":"5m"})).await;
    assert_ne!(one.key, five.key);
    let mut p = main.peer().await;
    let req = p.request().await;
    p.ack(&req).await;
    let mut q = test.peer().await;
    let req = q.request().await;
    q.ack(&req).await;
    p.data("trades@42", trade(17, 1)).await;
    q.data("trades@42", trade(17, 1)).await;
    p.data("trades@73", trade(17, 1)).await;
    let RealtimeUpdate::Trades(m) = recv(&mut main_sub).await else {
        panic!()
    };
    let RealtimeUpdate::Trades(t) = recv(&mut test_sub).await else {
        panic!()
    };
    let RealtimeUpdate::Trades(x) = recv(&mut eth).await else {
        panic!()
    };
    assert_ne!(m[0].id, t[0].id);
    assert_eq!(m[0].price, Some(100.25));
    assert_eq!(x[0].price, Some(10.025));
    assert_eq!(x[0].amount, Some(25.));
    p.data(
        "candles@42*60",
        json!({"mt":11,"r":60,"d":[candle(60000,10000)]}),
    )
    .await;
    p.data(
        "candles@42*300",
        json!({"mt":11,"r":300,"d":[candle(300000,10500)]}),
    )
    .await;
    let RealtimeUpdate::Ohlcv(v) = recv(&mut one).await else {
        panic!()
    };
    assert_eq!(v[0].0, 60000);
    let RealtimeUpdate::Ohlcv(v) = recv(&mut five).await else {
        panic!()
    };
    assert_eq!(v[0].0, 300000);
    a.shutdown_native().await.unwrap();
    b.shutdown_native().await.unwrap();
    p.closed().await;
    q.closed().await;
}

#[tokio::test]
async fn seventeen_streams_use_two_connections_not_seventeen() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut plans = vec![];
    for id in 1..=17 {
        plans.push(plan(&e, RealtimeChannel::Trades, &id.to_string(), json!({})).await);
    }
    let mut subs = vec![];
    for plan in plans {
        subs.push(plan.subscribe().await.unwrap());
    }
    let mut p = mock.peer().await;
    let mut q = mock.peer().await;
    let a = p.request().await;
    let b = q.request().await;
    let mut counts = vec![
        a["subs"].as_array().unwrap().len(),
        b["subs"].as_array().unwrap().len(),
    ];
    counts.sort();
    assert_eq!(counts, vec![1, 16]);
    p.ack(&a).await;
    q.ack(&b).await;
    assert!(timeout(Duration::from_millis(80), mock.peers.recv())
        .await
        .is_err());
    drop(subs);
    p.closed().await;
    q.closed().await;
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn trade_snapshot_updates_identity_validation_and_bounded_queue() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut s = sub(&e, RealtimeChannel::Trades, "42", json!({})).await;
    let mut p = mock.peer().await;
    let req = p.request().await;
    p.ack(&req).await;
    p.data("trades@42", trade(17, 1)).await;
    let RealtimeUpdate::Trades(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v[0].price, Some(100.25));
    assert_eq!(v[0].amount, Some(2.5));
    assert_eq!(v[0].side.as_deref(), Some("sell"));
    assert_eq!(v[0].timestamp, Some(123456));
    assert_eq!(v[0].info["native"]["at"]["l"], 1);
    p.data("trades@42", trade(18, 2)).await;
    let RealtimeUpdate::Trades(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v[0].id.as_deref(), Some("[143,42,\"0xabc\",2]"));
    p.data("trades@42", trade(18, 2)).await;
    let RealtimeUpdate::Trades(v) = recv(&mut s).await else {
        panic!()
    };
    assert!(v.is_empty());
    // Use a second receiver as a barrier: all publications reach the fast
    // consumer while the slow consumer's bounded broadcast queue drops frames.
    let mut fast = sub(&e, RealtimeChannel::Trades, "42", json!({})).await;
    for i in 3..=160 {
        p.data("trades@42", trade(18, i)).await;
        recv(&mut fast).await;
    }
    assert!(matches!(
        s.receiver.recv().await,
        Err(broadcast::error::RecvError::Lagged(_))
    ));
    e.shutdown_native().await.unwrap();
    p.closed().await;
    while s.receiver.recv().await.is_ok() {}
    while fast.receiver.recv().await.is_ok() {}
    assert!(matches!(
        fast.receiver.recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
    assert!(plan(&e, RealtimeChannel::Trades, "42", json!({}))
        .await
        .subscribe()
        .await
        .is_err());
}

#[tokio::test]
async fn maintained_book_delta_delete_malformed_recovery_and_noncontiguous_sequence() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut s = sub(&e, RealtimeChannel::OrderBook, "42", json!({})).await;
    let mut p = mock.peer().await;
    let req = p.request().await;
    p.ack(&req).await;
    p.data("order-book@42", snapshot()).await;
    recv(&mut s).await;
    p.data(
        "order-book@42",
        book(16, 150, json!([{"p":10000,"s":5000,"o":3}]), json!([])),
    )
    .await;
    let RealtimeUpdate::OrderBook(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v.bids, vec![(100., 5.), (99., 3.)]);
    assert_eq!(v.nonce, Some(150));
    p.data(
        "order-book@42",
        book(16, 151, json!([{"p":10000,"s":900,"o":0}]), json!([])),
    )
    .await;
    let RealtimeUpdate::OrderBook(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v.bids, vec![(99., 3.)]);
    // A valid first row followed by malformed data must never leak partial state.
    p.data(
        "order-book@42",
        book(
            16,
            152,
            json!([{"p":9900,"s":5000,"o":1},{"p":9800,"s":-1,"o":1}]),
            json!([]),
        ),
    )
    .await;
    assert!(matches!(recv(&mut s).await, RealtimeUpdate::Error(_)));
    let req = p.request().await;
    assert_eq!(req["subs"][0]["subscribe"], false);
    let old_sid = p.ids["order-book@42"];
    p.data("order-book@42", book(16, 153, json!([]), json!([])))
        .await;
    p.ack(&req).await;
    let req = p.request().await;
    assert_eq!(req["subs"][0]["subscribe"], true);
    p.ack(&req).await;
    let mut late = book(16, 154, json!([]), json!([]));
    late["sid"] = json!(old_sid);
    p.send(late).await;
    p.data("order-book@42", snapshot()).await;
    let RealtimeUpdate::OrderBook(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v.bids, vec![(100., 2.), (99., 3.)]);
    // Regressing provenance is unsafe, unlike a normal sequence jump.
    p.data("order-book@42", book(16, 99, json!([]), json!([])))
        .await;
    assert!(matches!(recv(&mut s).await, RealtimeUpdate::Error(_)));
    let req = p.request().await;
    p.ack(&req).await;
    let req = p.request().await;
    p.ack(&req).await;
    p.data("order-book@42", snapshot()).await;
    assert!(matches!(recv(&mut s).await, RealtimeUpdate::OrderBook(_)));
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn candles_overlap_replace_mutable_row_validate_and_scale_collateral_from_catalog() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut s = sub(&e, RealtimeChannel::Ohlcv, "42", json!({"timeframe":"1m"})).await;
    let mut p = mock.peer().await;
    let req = p.request().await;
    p.ack(&req).await;
    p.data(
        "candles@42*60",
        json!({"mt":11,"r":60,"d":[candle(60000,10000),candle(120000,10000)]}),
    )
    .await;
    let RealtimeUpdate::Ohlcv(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v[0], (60000, 100., 110., 90., 100., Some(2.5)));
    p.data(
        "candles@42*60",
        json!({"mt":12,"r":60,"d":[candle(120000,10500),candle(180000,10100)]}),
    )
    .await;
    let RealtimeUpdate::Ohlcv(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v.len(), 3);
    assert_eq!(v[1].4, 105.);
    assert_eq!(v[2].0, 180000);
    p.data(
        "candles@42*60",
        json!({"mt":12,"r":60,"d":[candle(180000,10900)]}),
    )
    .await;
    let RealtimeUpdate::Ohlcv(v) = recv(&mut s).await else {
        panic!()
    };
    assert_eq!(v.len(), 3);
    assert_eq!(v[2].4, 109.);
    p.data(
        "candles@42*60",
        json!({"mt":12,"r":60,"d":[candle(180000,12000)]}),
    )
    .await;
    assert!(matches!(recv(&mut s).await, RealtimeUpdate::Error(_)));
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn socket_failure_invalidates_queued_books_and_resubscribes_once_for_shared_clients() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut a = sub(&e, RealtimeChannel::OrderBook, "42", json!({})).await;
    let mut b = sub(&e, RealtimeChannel::OrderBook, "BTC", json!({})).await;
    let mut p = mock.peer().await;
    let req = p.request().await;
    p.ack(&req).await;
    p.data("order-book@42", snapshot()).await;
    recv(&mut a).await; // b deliberately lags
                        // Abrupt drop simulates a network failure rather than a clean close.
    drop(p);
    assert!(matches!(recv(&mut a).await, RealtimeUpdate::Error(_)));
    assert!(matches!(recv(&mut b).await, RealtimeUpdate::Error(_))); // not stale snapshot
    let mut p = mock.peer().await;
    let req = p.request().await;
    assert_eq!(req["subs"].as_array().unwrap().len(), 1);
    p.ack(&req).await;
    p.data("order-book@42", snapshot()).await;
    assert!(matches!(recv(&mut a).await, RealtimeUpdate::OrderBook(_)));
    assert!(matches!(recv(&mut b).await, RealtimeUpdate::OrderBook(_)));
    e.shutdown_native().await.unwrap();
    p.closed().await;
    assert!(matches!(
        a.receiver.recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
}

#[tokio::test]
async fn per_entry_subscription_errors_do_not_kill_other_streams() {
    let mut mock = Mock::start(143).await;
    let e = mock.exchange(143);
    let mut a = sub(&e, RealtimeChannel::Trades, "42", json!({})).await;
    let mut b = sub(&e, RealtimeChannel::Trades, "73", json!({})).await;
    let mut p = mock.peer().await;
    let req = p.request().await;
    assert_eq!(req["subs"].as_array().unwrap().len(), 2);
    p.ids.insert("trades@42".into(), 1);
    p.send(json!({"mt":6,"subs":[{"stream":"trades@42","sid":1},{"stream":"trades@73","status":{"code":404,"error":"unknown stream"}}]})).await;
    assert!(matches!(recv(&mut b).await,RealtimeUpdate::Error(e) if e.contains("404")));
    p.data("trades@42", trade(17, 1)).await;
    assert!(matches!(recv(&mut a).await, RealtimeUpdate::Trades(_)));
    p.quiet().await;
    e.shutdown_native().await.unwrap();
}

#[tokio::test]
async fn validation_and_snapshot_capability_contract() {
    let mock = Mock::start(143).await;
    let e = mock.exchange(143);
    for (c, p) in [
        (RealtimeChannel::OrderBook, json!({"levels":101})),
        (RealtimeChannel::OrderBook, json!({"levels":0})),
        (RealtimeChannel::OrderBook, json!({"levels":"5"})),
        (RealtimeChannel::OrderBook, json!({"levels":5,"depth":2})),
        (RealtimeChannel::Ohlcv, json!({"timeframe":"1s"})),
        (RealtimeChannel::Ohlcv, json!({"timeframe":60})),
        (RealtimeChannel::Trades, json!({"levels":1})),
    ] {
        assert!(e
            .prepare_native(
                c,
                RealtimeTopic {
                    exchange: "perpl".into(),
                    symbol: "42".into(),
                    params: p
                }
            )
            .await
            .is_err());
    }
    assert!(e
        .fetch_trades(FetchTradesParams {
            symbol: "42".into(),
            since: None,
            limit: None,
            params: json!({})
        })
        .await
        .is_err());
    assert!(e.market_stats_source().is_some());
    let caps = MarketDataExchange::capabilities(&e);
    assert_eq!(caps.fetch_trades.state, CapabilityState::Unsupported);
    assert_eq!(caps.realtime.marketstats.mode, Some(RealtimeMode::Polling));
    let wrong = mock.exchange(10143);
    assert!(wrong
        .prepare_native(
            RealtimeChannel::Trades,
            RealtimeTopic {
                exchange: "perpl".into(),
                symbol: "42".into(),
                params: json!({})
            }
        )
        .await
        .is_err());
}
