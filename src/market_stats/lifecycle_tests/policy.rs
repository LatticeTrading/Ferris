//! Paused-clock regression coverage of policy-driven source lifecycle.
use super::*;
use crate::{
    exchanges::ccxt::{statistics_profile, Venue},
    realtime::{
        DeliveryState, Publication, RealtimeReceiver, RealtimeSubscription, RealtimeTopic,
        RealtimeUpdate, StatisticsRowUpdate, StatisticsUpdate,
    },
};
use tokio::sync::{broadcast, Notify};

pub(super) type LiveCall = oneshot::Sender<Result<Option<RealtimeSubscription>, ExchangeError>>;

fn fixture_for(
    venue: Venue,
) -> (
    Arc<MarketStatsCoordinator>,
    SourceController,
    mpsc::UnboundedReceiver<LiveCall>,
) {
    let (coordinator, mut controller) = fixture();
    drop(coordinator);
    let (live, receiver) = mpsc::unbounded_channel();
    let exchange = Arc::get_mut(&mut controller.exchange).unwrap();
    exchange.venue = venue.public_id();
    exchange.live = Some(live);
    let mut registry = ExchangeRegistry::new();
    registry.register(controller.exchange.clone());
    (
        Arc::new(MarketStatsCoordinator::new(Arc::new(registry))),
        controller,
        receiver,
    )
}

fn id(venue: &str, native: &str) -> String {
    make_market_id(
        venue,
        UnifiedMarketType::Perp,
        (venue == "bybit").then_some("linear"),
        (venue == "hyperliquid").then_some(""),
        native,
    )
    .unwrap()
}

fn request_for(
    venue: Venue,
    selected: Option<&[&str]>,
    fields: &[MarketStatsFieldName],
) -> FetchMarketStatsRequest {
    FetchMarketStatsRequest {
        exchange: venue.public_id().into(),
        params: json!({}),
        fields: Some(fields.to_vec()),
        market_ids: selected.map(|ids| {
            ids.iter()
                .map(|native| id(venue.public_id(), native))
                .collect()
        }),
    }
}

pub(super) fn rows_for(
    venue: &str,
    receipt: u64,
    params: &FetchMarketStatsParams,
) -> Vec<MarketStatsRow> {
    let mut rows = source_rows(receipt);
    let policy = Venue::from_public_id(venue).and_then(statistics_profile::live_statistics);
    for (index, row) in rows.iter_mut().enumerate() {
        row.market.exchange = venue.into();
        let native = if venue == "lighterxyz" {
            if index == 0 {
                "0"
            } else {
                "1"
            }
        } else {
            row.market.base.as_str()
        };
        let market_id = id(venue, native);
        row.market.identity.as_mut().unwrap().market_id = market_id.clone();
        for (name, field) in &mut row.fields {
            if policy.is_some_and(|policy| policy.failure_fields.contains(name))
                || (!params.include_bulk
                    && !(*name == MarketStatsFieldName::OpenInterest
                        && params.open_interest_market_ids.contains(&market_id)))
            {
                *field = MarketStatsField {
                    state: MarketStatsFieldState::Unavailable,
                    value: None,
                    reason: Some("not-requested".into()),
                    source: None,
                    exchange_timestamp: None,
                    received_timestamp: None,
                };
            }
        }
    }
    rows
}

fn live_subscription() -> (
    RealtimeSubscription,
    broadcast::Sender<Publication>,
    Arc<DeliveryState>,
) {
    let (sender, receiver) = broadcast::channel(16);
    let state = Arc::new(DeliveryState::default());
    (
        RealtimeSubscription {
            key: "fixture".into(),
            levels_limit: 0,
            topic: RealtimeTopic {
                exchange: "lighterxyz".into(),
                symbol: "*".into(),
                params: json!({}),
            },
            receiver: RealtimeReceiver::new(receiver, state.clone(), Arc::new(Notify::new())),
        },
        sender,
        state,
    )
}

fn publish(sender: &broadcast::Sender<Publication>, update: RealtimeUpdate) {
    assert!(sender.send(Publication { epoch: 0, update }).is_ok());
}

fn patch(name: MarketStatsFieldName, at: Instant) -> RealtimeUpdate {
    let policy = statistics_profile::live_statistics(Venue::Lighter).unwrap();
    let mut field = source_rows(123)[0].fields[&name].clone();
    field.source = Some(policy.source.into());
    RealtimeUpdate::Statistics(Arc::new(StatisticsUpdate {
        rows: vec![StatisticsRowUpdate {
            market_id: id("lighterxyz", "0"),
            fields: BTreeMap::from([(name, field)]),
            received_at: at,
        }],
    }))
}

#[tokio::test(start_paused = true)]
async fn disabled_live_paths_and_bulk_oi_do_not_create_demand_calls() {
    for venue in Venue::ALL.into_iter().filter(|v| *v != Venue::Lighter) {
        let (coordinator, mut source, mut live) = fixture_for(venue);
        let mut subscribe = Box::pin(coordinator.subscribe(request_for(
            venue,
            Some(&["BTC"]),
            &[MarketStatsFieldName::Funding],
        )));
        assert!(poll!(subscribe.as_mut()).is_pending());
        let call = source.next_call().await;
        assert!(call.params.open_interest_market_ids.is_empty());
        call.succeed(Instant::now());
        let sub = ready(subscribe).await.unwrap();
        assert!(live.try_recv().is_err());
        if venue != Venue::Binance {
            let snapshot = ready(coordinator.snapshot(request_for(
                venue,
                Some(&["BTC"]),
                &[MarketStatsFieldName::OpenInterest],
            )))
            .await
            .unwrap();
            assert_eq!(snapshot.markets.len(), 1);
            source.expect_no_call().await;
        }
        coordinator.unsubscribe_by_key(&sub.key).await;
        ready(coordinator.shutdown()).await;
    }
}

#[tokio::test(start_paused = true)]
async fn selected_oi_union_demand_only_deadlines_and_cancellation() {
    let (coordinator, mut source, _live) = fixture_for(Venue::Binance);
    let origin = Instant::now();
    let req = |ids: Option<&[&str]>, field| request_for(Venue::Binance, ids, &[field]);
    let mut bulk = Box::pin(coordinator.subscribe(req(None, MarketStatsFieldName::Funding)));
    assert!(poll!(bulk.as_mut()).is_pending());
    source.next_call().await.succeed(origin);
    let bulk = ready(bulk).await.unwrap();
    advance_to(origin + Duration::from_secs(5)).await;
    let mut selected = Box::pin(async {
        tokio::join!(
            coordinator.subscribe(req(Some(&["BTC"]), MarketStatsFieldName::OpenInterest)),
            coordinator.subscribe(req(
                Some(&["BTC", "ETH"]),
                MarketStatsFieldName::OpenInterest
            ))
        )
    });
    assert!(poll!(selected.as_mut()).is_pending());
    let call = source.next_call().await;
    assert!(!call.params.include_bulk);
    assert_eq!(
        call.params.open_interest_market_ids,
        vec![id("binance", "BTC"), id("binance", "ETH")]
    );
    call.succeed(Instant::now());
    let (first, second) = ready(selected).await;
    let first = first.unwrap();
    let second = second.unwrap();
    coordinator.unsubscribe_by_key(&first.key).await;
    coordinator.unsubscribe_by_key(&second.key).await;
    advance_to(origin + POLL).await;
    let call = source.next_call().await;
    assert!(
        call.params.include_bulk,
        "demand-only pass moved bulk deadline"
    );
    assert!(
        call.params.open_interest_market_ids.is_empty(),
        "released selection retained OI demand"
    );
    call.succeed(Instant::now());
    source.expect_no_call().await;
    coordinator.unsubscribe_by_key(&bulk.key).await;
    source.expect_no_call().await;
    advance_to(origin + Duration::from_secs(40)).await;
    let mut cancelled =
        Box::pin(coordinator.subscribe(req(Some(&["BTC"]), MarketStatsFieldName::OpenInterest)));
    assert!(poll!(cancelled.as_mut()).is_pending());
    let mut call = source.next_call().await;
    assert!(
        call.params.include_bulk,
        "idle worker must bootstrap on restart"
    );
    assert_eq!(
        call.params.open_interest_market_ids,
        vec![id("binance", "BTC")]
    );
    drop(cancelled);
    call.expect_cancelled().await;
    ready(coordinator.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn live_bootstrap_failure_retry_sparse_recovery_and_release() {
    let (coordinator, mut source, mut live) = fixture_for(Venue::Lighter);
    let origin = Instant::now();
    let mut subscribe = Box::pin(coordinator.subscribe(request_for(
        Venue::Lighter,
        None,
        &[MarketStatsFieldName::Funding],
    )));
    assert!(poll!(subscribe.as_mut()).is_pending());
    source.next_call().await.succeed(origin);
    let setup = ready(live.recv()).await.unwrap();
    assert!(
        poll!(subscribe.as_mut()).is_pending(),
        "bulk alone completed live bootstrap"
    );
    setup
        .send(Err(ExchangeError::UpstreamRequest("setup outage".into())))
        .ok()
        .unwrap();
    let mut sub = ready(subscribe).await.unwrap();
    let policy = statistics_profile::live_statistics(Venue::Lighter).unwrap();
    assert_eq!(
        sub.receiver.borrow().source_failures[0].source,
        policy.source
    );
    advance_to(origin + Duration::from_secs(29)).await;
    assert!(live.try_recv().is_err());
    advance_to(origin + POLL).await;
    let setup = ready(live.recv()).await.unwrap();
    source.next_call().await.succeed(Instant::now());
    let (subscription, sender, delivery) = live_subscription();
    setup.send(Ok(Some(subscription))).ok().unwrap();
    let observed = Instant::now();
    publish(&sender, patch(MarketStatsFieldName::Funding, observed));
    source.expect_no_call().await;
    let fresh = sub.receiver.borrow_and_update().clone();
    assert!(fresh.source_failures.is_empty());
    assert_eq!(
        fresh.rows[0].fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Available
    );
    let receipt = fresh.field_received_at[&id("lighterxyz", "0")][&MarketStatsFieldName::Funding];
    publish(&sender, RealtimeUpdate::Error("live outage".into()));
    ready(sub.receiver.changed()).await.unwrap();
    let failed = sub.receiver.borrow_and_update().clone();
    assert_eq!(failed.source_failures[0].source, policy.source);
    assert_eq!(
        failed.rows[0].fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Stale
    );
    assert_eq!(
        failed.field_received_at[&id("lighterxyz", "0")][&MarketStatsFieldName::Funding],
        receipt
    );
    publish(
        &sender,
        patch(MarketStatsFieldName::MarkPrice, Instant::now()),
    );
    ready(sub.receiver.changed()).await.unwrap();
    let sparse = sub.receiver.borrow_and_update().clone();
    assert!(sparse.source_failures.is_empty());
    assert_eq!(
        sparse.rows[0].fields[&MarketStatsFieldName::Funding],
        failed.rows[0].fields[&MarketStatsFieldName::Funding]
    );
    drop(sender);
    ready(sub.receiver.changed()).await.unwrap();
    assert!(!sub.receiver.borrow().source_failures.is_empty());
    assert_eq!(delivery.viewers.load(Ordering::SeqCst), 0);
    advance_to(origin + Duration::from_secs(59)).await;
    assert!(live.try_recv().is_err());
    advance_to(origin + Duration::from_secs(61)).await;
    let retry = ready(live.recv()).await.unwrap();
    source.next_call().await.succeed(Instant::now());
    // Cancelling the last lease while re-setup is pending drops that future too.
    coordinator.unsubscribe_by_key(&sub.key).await;
    ready(coordinator.shutdown()).await;
    assert!(retry.is_closed());
}

#[tokio::test(start_paused = true)]
async fn silent_live_bootstrap_times_out_without_dropping_subscription() {
    let (coordinator, mut source, mut live) = fixture_for(Venue::Lighter);
    let origin = Instant::now();
    let mut subscribe = Box::pin(coordinator.subscribe(request_for(
        Venue::Lighter,
        None,
        &[MarketStatsFieldName::Funding],
    )));
    assert!(poll!(subscribe.as_mut()).is_pending());
    source.next_call().await.succeed(origin);
    let (subscription, _sender, delivery) = live_subscription();
    ready(live.recv())
        .await
        .unwrap()
        .send(Ok(Some(subscription)))
        .ok()
        .unwrap();
    source.expect_no_call().await;
    advance_to(origin + Duration::from_secs(9)).await;
    assert!(poll!(subscribe.as_mut()).is_pending());
    advance_to(origin + Duration::from_secs(11)).await;
    let sub = ready(subscribe).await.unwrap();
    assert!(sub.receiver.borrow().source_failures[0]
        .message
        .contains("timed out"));
    assert_eq!(delivery.viewers.load(Ordering::SeqCst), 1);
    coordinator.unsubscribe_by_key(&sub.key).await;
    ready(coordinator.shutdown()).await;
    assert_eq!(delivery.viewers.load(Ordering::SeqCst), 0);
}
