use super::*;

#[tokio::test]
async fn reconnect_error_survives_epoch_change_but_stale_data_does_not() {
    let (sender, receiver) = broadcast::channel(8);
    let state = Arc::new(DeliveryState::default());
    let mut receiver = RealtimeReceiver::new(receiver, Arc::clone(&state), Arc::new(Notify::new()));
    sender
        .send(Publication::at(0, RealtimeUpdate::Trades(Arc::new(vec![]))))
        .ok()
        .unwrap();
    sender
        .send(Publication::at(
            1,
            RealtimeUpdate::Error("continuity lost".into()),
        ))
        .ok()
        .unwrap();
    state.epoch.store(2, Ordering::Release);
    sender
        .send(Publication::at(2, RealtimeUpdate::Ohlcv(Arc::new(vec![]))))
        .ok()
        .unwrap();
    assert!(
        matches!(receiver.recv().await.unwrap(), RealtimeUpdate::Error(message) if &*message=="continuity lost")
    );
    assert!(matches!(
        receiver.recv().await.unwrap(),
        RealtimeUpdate::Ohlcv(_)
    ));
    // A newly joining viewer is not sent historical errors.
    let mut newcomer = RealtimeReceiver::join(
        sender.subscribe(),
        Arc::clone(&state),
        Arc::new(Notify::new()),
    )
    .unwrap();
    sender
        .send(Publication::at(2, RealtimeUpdate::Trades(Arc::new(vec![]))))
        .ok()
        .unwrap();
    assert!(matches!(
        newcomer.recv().await.unwrap(),
        RealtimeUpdate::Trades(_)
    ));
}

fn book(levels: usize) -> RealtimeUpdate {
    RealtimeUpdate::OrderBook(Arc::new(CcxtOrderBook {
        bids: (0..levels).map(|i| (100.0 - i as f64, 1.0)).collect(),
        asks: (0..levels).map(|i| (101.0 + i as f64, 1.0)).collect(),
        ..Default::default()
    }))
}

fn bid_levels(update: RealtimeUpdate) -> usize {
    match update {
        RealtimeUpdate::OrderBook(book) => book.bids.len(),
        _ => panic!("expected an order book"),
    }
}

#[tokio::test]
async fn book_viewers_bound_conversion_depth_and_never_see_a_shallower_book() {
    let (sender, receiver) = broadcast::channel(8);
    let state = Arc::new(DeliveryState::default());
    let released = Arc::new(Notify::new());
    // Without registered demand the owner must publish the whole book.
    assert_eq!(state.depth.required(), usize::MAX);

    let mut shallow = RealtimeReceiver::new(receiver, Arc::clone(&state), Arc::clone(&released))
        .with_book_depth(5);
    assert_eq!(state.depth.required(), 5);
    let mut deep = RealtimeReceiver::join(
        sender.subscribe(),
        Arc::clone(&state),
        Arc::clone(&released),
    )
    .unwrap()
    .with_book_depth(20);
    assert_eq!(state.depth.required(), 20);

    // Converted before the deeper demand was visible to the owner, then a
    // book that serves it, then a source that simply holds fewer levels.
    for (levels, book_depth) in [(5, 5), (20, 20), (12, usize::MAX)] {
        sender
            .send(Publication {
                book_depth,
                ..Publication::new(book(levels))
            })
            .ok()
            .unwrap();
    }
    assert_eq!(bid_levels(shallow.recv().await.unwrap()), 5);
    assert_eq!(bid_levels(shallow.recv().await.unwrap()), 20);
    assert_eq!(bid_levels(shallow.recv().await.unwrap()), 12);
    assert_eq!(
        bid_levels(deep.recv().await.unwrap()),
        20,
        "a book truncated below the viewer's depth must be skipped, not delivered"
    );
    assert_eq!(bid_levels(deep.recv().await.unwrap()), 12);

    // Demand follows the current viewers in both directions.
    drop(deep);
    assert_eq!(state.depth.required(), 5);
    drop(shallow);
    assert_eq!(state.depth.required(), usize::MAX);
}

#[test]
fn encoded_payloads_are_serialized_once_per_view() {
    use crate::orderbook::BookAmountMode::{Base, Native};
    let encoded = EncodedPayloads::default();
    let calls = std::cell::Cell::new(0);
    let encode = |key, text: &str| {
        encoded.get_or_encode(key, || {
            calls.set(calls.get() + 1);
            Some(text.to_string())
        })
    };
    let first = encode((20, Native), "a").unwrap();
    let again = encode((20, Native), "ignored").unwrap();
    assert!(Arc::ptr_eq(&first, &again));
    assert_eq!(&*again, "a");
    // Display depth and amount unit are distinct views of one publication.
    assert_eq!(&*encode((5, Native), "b").unwrap(), "b");
    assert_eq!(&*encode((20, Base), "c").unwrap(), "c");
    assert_eq!(calls.get(), 3);

    // Past the bound a view is still encoded, just not retained.
    for depth in 100..100 + ENCODED_VARIANTS {
        encode((depth, Native), "d").unwrap();
    }
    assert_eq!(&*encode((999, Native), "e").unwrap(), "e");
    assert_eq!(&*encode((999, Native), "f").unwrap(), "f");
    assert_eq!(&*encode((20, Native), "ignored").unwrap(), "a");

    // An encoding failure is shared instead of retried per viewer.
    let failing = EncodedPayloads::default();
    assert!(failing.get_or_encode((1, Native), || None).is_none());
    assert!(failing
        .get_or_encode((1, Native), || Some("late".into()))
        .is_none());
}
