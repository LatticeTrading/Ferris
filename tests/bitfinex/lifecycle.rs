use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_shared_feeds_retirement_fences_and_fresh_snapshots() {
    let h = Harness::start().await;
    let mut t = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let mut b = h
        .subscribe(RealtimeChannel::OrderBook, json!({"depth":1}))
        .await;
    let b2 = h
        .subscribe(RealtimeChannel::OrderBook, json!({"depth":50}))
        .await;
    let mut c = h
        .subscribe(RealtimeChannel::Ohlcv, json!({"timeframe":"1D"}))
        .await;
    let (conn, tc) = h.command("subscribe", Some(TRADES), 1).await;
    let (_, bc) = h.command("subscribe", Some(BOOK), 1).await;
    let (_, cc) = h.command("subscribe", Some(CANDLES), 1).await;
    assert_eq!(bc["len"], 100);
    h.inject(trade(&tc["chanId"], 1));
    let RealtimeUpdate::Trades(rows) = next(&mut t).await else {
        panic!()
    };
    assert_eq!(rows[0].amount, Some(0.02));
    assert_eq!(rows[0].side.as_deref(), Some("sell"));
    h.inject(book(&bc["chanId"], 2.0));
    let RealtimeUpdate::OrderBook(row) = next(&mut b).await else {
        panic!()
    };
    assert_eq!(row.bids, [(60000.0, 2.0)]);
    assert_eq!(row.timestamp, None);
    h.inject(json!([
        cc["chanId"],
        [[TIME, 60000, 60001, 60002, 59999, 3]]
    ]));
    let RealtimeUpdate::Ohlcv(rows) = next(&mut c).await else {
        panic!()
    };
    assert_eq!(
        rows[0],
        (TIME, 60000.0, 60002.0, 59999.0, 60001.0, Some(3.0))
    );
    drop(b2);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!h
        .mock
        .commands
        .borrow()
        .iter()
        .any(|(_, v)| v["event"] == "unsubscribe"));
    h.mock.ack_mode.store(1, Ordering::SeqCst);
    drop(b);
    let (_, unsub) = h.command("unsubscribe", None, 1).await;
    assert_eq!(unsub["chanId"], bc["chanId"]);
    let mut b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    h.inject(json!({"event":"unsubscribed","status":"OK","chanId":999}));
    h.inject(book(&bc["chanId"], 99.0));
    quiet(&mut b).await;
    h.inject(trade(&tc["chanId"], 2));
    assert!(matches!(next(&mut t).await, RealtimeUpdate::Trades(_)));
    h.inject(json!({"event":"unsubscribed","status":"OK","chanId":bc["chanId"]}));
    let (same, new) = h.command("subscribe", Some(BOOK), 2).await;
    assert_eq!(same, conn);
    assert_ne!(new["chanId"], bc["chanId"]);
    h.inject(json!({"event":"unsubscribed","status":"OK","chanId":bc["chanId"]})); // duplicate ACK
    h.inject(book(&bc["chanId"], 99.0)); // late old data after re-add
    h.inject(json!([new["chanId"], [60000, 1, 88]])); // pre-snapshot delta
    quiet(&mut b).await;
    h.inject(book(&new["chanId"], 4.0));
    let RealtimeUpdate::OrderBook(row) = next(&mut b).await else {
        panic!()
    };
    assert_eq!(row.bids, [(60000.0, 4.0)]);
    h.inject(json!([new["chanId"], [60000, 0, 1]]));
    let RealtimeUpdate::OrderBook(row) = next(&mut b).await else {
        panic!()
    };
    assert!(row.bids.is_empty());
    h.mock.ack_mode.store(0, Ordering::SeqCst);
    drop(c);
    let (_, u) = h.command("unsubscribe", None, 2).await;
    assert_eq!(u["chanId"], cc["chanId"]);
    drop(t);
    let (_, u) = h.command("unsubscribe", None, 3).await;
    assert_eq!(u["chanId"], tc["chanId"]);
    assert_eq!(&*h.mock.connections.borrow(), &[conn]);
    drop(b);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsubscribe_waits_for_delayed_channel_assignment_and_shutdown_releases() {
    let h = Harness::start().await;
    let t = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    h.command("subscribe", Some(TRADES), 1).await;
    h.mock.subscribe_hold.store(1, Ordering::SeqCst);
    let b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let (_, request) = h.command("subscribe", Some(BOOK), 1).await;
    drop(b);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!h
        .mock
        .commands
        .borrow()
        .iter()
        .any(|(_, v)| v["event"] == "unsubscribe"));
    // A delayed subscribe ACK supplies the ID needed for queued retirement.
    let mut ack = request.clone();
    ack["event"] = json!("subscribed");
    h.inject(ack);
    h.mock.ack_mode.store(1, Ordering::SeqCst);
    let (_, u) = h.command("unsubscribe", None, 1).await;
    assert_eq!(u["chanId"], request["chanId"]);
    timeout(Duration::from_secs(2), h.stop()).await.unwrap();
    drop(t);
}

async fn recovery(mode: usize) {
    let h = Harness::start().await;
    let mut t = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let (first, _) = h.command("subscribe", Some(TRADES), 1).await;
    h.command("subscribe", Some(BOOK), 1).await;
    h.mock.ack_mode.store(mode, Ordering::SeqCst);
    drop(b);
    h.command("unsubscribe", None, 1).await;
    let (second, tc) = timeout(Duration::from_secs(15), async {
        let mut rx = h.mock.commands.subscribe();
        let rows = rx
            .wait_for(|rows| {
                rows.iter()
                    .any(|(id, v)| *id > first && v["event"] == "subscribe" && topic(v) == TRADES)
            })
            .await
            .unwrap();
        rows.iter()
            .rev()
            .find(|(_, v)| v["event"] == "subscribe" && topic(v) == TRADES)
            .unwrap()
            .clone()
    })
    .await
    .unwrap();
    assert!(second > first);
    let RealtimeUpdate::Error(error) = next(&mut t).await else {
        panic!("continuity error missing")
    };
    assert!(error.contains(if mode == 1 { "timed out" } else { "rejected" }));
    h.inject(trade(&tc["chanId"], 5));
    assert!(matches!(next(&mut t).await, RealtimeUpdate::Trades(_)));
    drop(t);
    h.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_ack_recovers() {
    recovery(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_ack_recovers() {
    recovery(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_checksum_mismatch_reconnects_and_requires_new_snapshot() {
    let h = Harness::start().await;
    let mut b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let (first, bc) = h.command("subscribe", Some(BOOK), 1).await;
    h.inject(book(&bc["chanId"], 2.0));
    assert!(matches!(next(&mut b).await, RealtimeUpdate::OrderBook(_)));
    // Official CRC32 ordering: "60000:2:60001:-5".
    h.inject(json!([bc["chanId"], "cs", 2124903168]));
    quiet(&mut b).await;
    h.inject(json!([bc["chanId"], "cs", 123]));
    assert!(matches!(next(&mut b).await, RealtimeUpdate::Error(_)));
    let (second, bc) = h.command("subscribe", Some(BOOK), 2).await;
    assert!(second > first);
    h.inject(json!([bc["chanId"], [60000, 1, 9]]));
    quiet(&mut b).await;
    h.inject(book(&bc["chanId"], 3.0));
    assert!(matches!(next(&mut b).await, RealtimeUpdate::OrderBook(_)));
    drop(b);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reused_server_id_and_transport_failure_reconnect_with_fresh_books() {
    let h = Harness::start().await;
    h.mock.reuse.store(1, Ordering::SeqCst);
    let mut t = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let (first, _) = h.command("subscribe", Some(TRADES), 1).await;
    h.command("subscribe", Some(BOOK), 1).await;
    drop(b);
    h.command("unsubscribe", None, 1).await;
    let mut b = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let (second, bc) = h.command("subscribe", Some(BOOK), 3).await;
    assert!(second > first);
    let RealtimeUpdate::Error(error) = next(&mut t).await else {
        panic!()
    };
    assert!(error.contains("reused"));
    // Receive the reconnect error before sending fresh data.
    assert!(matches!(next(&mut b).await, RealtimeUpdate::Error(_)));
    h.inject(book(&bc["chanId"], 7.0));
    assert!(matches!(next(&mut b).await, RealtimeUpdate::OrderBook(_)));
    h.inject(json!("close"));
    let (third, _) = h.command("subscribe", Some(BOOK), 4).await;
    assert!(third > second);
    assert!(matches!(next(&mut t).await, RealtimeUpdate::Error(_)));
    drop((b, t));
    h.stop().await;
}
