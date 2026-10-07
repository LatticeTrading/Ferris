use super::*;

async fn quiet(sub: &mut RealtimeSubscription) {
    assert!(
        timeout(Duration::from_millis(120), sub.receiver.recv())
            .await
            .is_err(),
        "retired/pre-snapshot data leaked to new viewer"
    );
}
fn ack(request: Value, success: bool) -> Value {
    json!({"request":request,"success":success,"ret_msg":""})
}
fn trade(id: &str) -> Value {
    json!({"topic":TRADES,"data":[{"i":id,"p":"60000","S":"Buy","v":"0.02","s":"BTCUSDT","T":TIME}]})
}
async fn warm_trade(h: &Harness, trades: &mut RealtimeSubscription, id: &str) {
    h.inject(trade(id));
    let RealtimeUpdate::Trades(rows) = timeout(WINDOW, trades.receiver.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("warm stream interrupted");
    };
    assert_eq!(rows[0].id.as_deref(), Some(id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readd_waits_for_ack_and_new_snapshot_without_restarting_warm_feed() {
    let h = Harness::start().await;
    let mut trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let mut book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let first = h.connected(None, &[TRADES, BOOK]).await;
    h.inject(book_frame(true, "1"));
    next(&mut book).await;
    h.mock.ack_mode.store(1, Ordering::SeqCst);
    drop(book);
    let request = h.command("unsubscribe", BOOK, 1).await;
    let mut book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    // Both unmatched ACKs and old valid-looking snapshots must be ignored.
    h.inject(ack(
        json!({"op":"unsubscribe","args":["orderBook25.H.BTCUSDT"]}),
        true,
    ));
    h.inject(book_frame(true, "99"));
    quiet(&mut book).await;
    assert_eq!(
        h.mock
            .commands
            .borrow()
            .iter()
            .filter(|(_, cmd)| cmd["op"] == "subscribe" && cmd["args"] == json!([BOOK]))
            .count(),
        1
    );
    warm_trade(&h, &mut trades, "still-warm").await;
    h.inject(ack(request.clone(), true));
    h.command("subscribe", BOOK, 2).await;
    // A duplicate unsubscribe ACK after re-add must not clear the new cache.
    h.inject(ack(request, true));
    h.inject(book_frame(false, "98"));
    quiet(&mut book).await;
    h.inject(book_frame(true, "2"));
    let RealtimeUpdate::OrderBook(row) = timeout(WINDOW, book.receiver.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("new snapshot");
    };
    assert_eq!(row.bids, [(60000.0, 2.0)]);
    warm_trade(&h, &mut trades, "still-same-connection").await;
    assert_eq!(h.connected(None, &[TRADES, BOOK]).await.id, first.id);
    drop((book, trades));
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_unsubscribe_falls_back_to_clean_connection() {
    let h = Harness::start().await;
    let mut trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let first = h.connected(None, &[TRADES, BOOK]).await;
    h.mock.ack_mode.store(2, Ordering::SeqCst);
    drop(book);
    h.command("unsubscribe", BOOK, 1).await;
    let second = h.connected(Some(first.id), &[TRADES]).await;
    assert_ne!(second.uri, first.uri);
    let RealtimeUpdate::Error(message) = timeout(WINDOW, trades.receiver.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("reconnect must signal lost continuity");
    };
    assert!(message.contains("unsubscribe rejected"));
    h.inject(trade("recovered"));
    let RealtimeUpdate::Trades(rows) = next(&mut trades).await else {
        panic!();
    };
    assert_eq!(rows[0].id.as_deref(), Some("recovered"));
    drop(trades);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_ack_times_out_instead_of_leaking_or_hanging_readd() {
    let h = Harness::start().await;
    let mut trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let first = h.connected(None, &[TRADES, BOOK]).await;
    h.mock.ack_mode.store(1, Ordering::SeqCst);
    drop(book);
    h.command("unsubscribe", BOOK, 1).await;
    let mut book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    warm_trade(&h, &mut trades, "while-ack-pending").await;
    // Production deadline is ten seconds; no wall-clock change or test-only
    // production configuration. Wait separately with a generous outer bound.
    let mut connections = h.mock.connections.subscribe();
    timeout(
        Duration::from_secs(15),
        connections
            .wait_for(|rows| rows.len() == 1 && rows[0].id > first.id && rows[0].topics.len() == 2),
    )
    .await
    .unwrap()
    .unwrap();
    let RealtimeUpdate::Error(message) = timeout(WINDOW, trades.receiver.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("timeout must signal lost continuity");
    };
    assert!(message.contains("acknowledgement timed out"));
    warm_trade(&h, &mut trades, "after-timeout-reconnect").await;
    h.inject(book_frame(false, "99"));
    h.inject(book_frame(true, "3"));
    let RealtimeUpdate::OrderBook(row) = next(&mut book).await else {
        panic!();
    };
    assert_eq!(row.bids, [(60000.0, 3.0)]);
    drop((book, trades));
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_pending_ack_does_not_wait_for_timeout() {
    let h = Harness::start().await;
    let trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    let book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    h.connected(None, &[TRADES, BOOK]).await;
    h.mock.ack_mode.store(1, Ordering::SeqCst);
    drop(book);
    h.command("unsubscribe", BOOK, 1).await;
    timeout(Duration::from_secs(2), h.stop()).await.unwrap();
    drop(trades);
}
