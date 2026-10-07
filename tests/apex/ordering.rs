use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshots_before_subscribe_ack_seed_initial_and_readded_books() {
    let h = Harness::start().await;
    let trades = h.subscribe(RealtimeChannel::Trades, json!({})).await;
    h.connected(None, &[TRADES]).await;
    h.mock.before_ack.lock().await.insert(
        BOOK.into(),
        vec![book_frame(true, "1"), book_frame(false, "2")],
    );
    let mut book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let first = h.connected(None, &[TRADES, BOOK]).await;
    let RealtimeUpdate::OrderBook(row) = next(&mut book).await else {
        panic!("pre-ACK snapshot must seed the book");
    };
    assert!(row.bids[0].1 == 1.0 || row.bids[0].1 == 2.0);
    drop(book);
    h.command("unsubscribe", BOOK, 1).await;
    h.connected(None, &[TRADES]).await;
    h.mock.before_ack.lock().await.insert(
        BOOK.into(),
        vec![book_frame(false, "99"), book_frame(true, "3")],
    );
    let mut book = h.subscribe(RealtimeChannel::OrderBook, json!({})).await;
    let RealtimeUpdate::OrderBook(row) = next(&mut book).await else {
        panic!("fresh pre-ACK snapshot after re-add");
    };
    assert_eq!(row.bids, [(60000.0, 3.0)]);
    assert_eq!(h.connected(None, &[TRADES, BOOK]).await.id, first.id);
    drop((book, trades));
    h.stop().await;
}
