use super::*;

#[tokio::test]
async fn reconnect_error_survives_epoch_change_but_stale_data_does_not() {
    let (sender, receiver) = broadcast::channel(8);
    let state = Arc::new(DeliveryState::default());
    let mut receiver = RealtimeReceiver::new(receiver, Arc::clone(&state), Arc::new(Notify::new()));
    sender
        .send(Publication {
            epoch: 0,
            update: RealtimeUpdate::Trades(Arc::new(vec![])),
        })
        .ok()
        .unwrap();
    sender
        .send(Publication {
            epoch: 1,
            update: RealtimeUpdate::Error("continuity lost".into()),
        })
        .ok()
        .unwrap();
    state.epoch.store(2, Ordering::Release);
    sender
        .send(Publication {
            epoch: 2,
            update: RealtimeUpdate::Ohlcv(Arc::new(vec![])),
        })
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
        .send(Publication {
            epoch: 2,
            update: RealtimeUpdate::Trades(Arc::new(vec![])),
        })
        .ok()
        .unwrap();
    assert!(matches!(
        newcomer.recv().await.unwrap(),
        RealtimeUpdate::Trades(_)
    ));
}
