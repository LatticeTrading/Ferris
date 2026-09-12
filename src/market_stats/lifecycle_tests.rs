use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{future::join_all, poll};
use serde_json::json;
use tokio::{
    sync::{mpsc, oneshot},
    time::{advance, timeout, Instant},
};

use super::{
    make_market_id, project_snapshot, MarketStatsCoordinator, MarketStatsSourceSnapshot,
    MarketStatsSubscription,
};
use crate::{
    errors::ApiError,
    exchanges::{
        registry::ExchangeRegistry,
        traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    },
    models::{
        CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTrade, FeatureCapability,
        FetchMarketStatsParams, FetchMarketStatsRequest, FetchMarketsParams, FetchOhlcvParams,
        FetchOrderBookParams, FetchTradesParams, FundingKind, FundingValue, MarketIdentity,
        MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSnapshot,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability, PriceValue,
        UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType,
    },
};

const POLL: Duration = Duration::from_secs(30);
const SOURCE: &str = "hyperliquid:primary:metaAndAssetCtxs";
const WALL_ORIGIN: u64 = 1_700_000_000_000;

// The controller supplies the primary receipt independently of completion. This models
// a primary response arriving before the concurrently acquired spot metadata.
struct SourceCall {
    completion: oneshot::Sender<Result<Instant, ExchangeError>>,
    cancelled: oneshot::Receiver<()>,
}

impl SourceCall {
    fn succeed(self, received_at: Instant) {
        self.completion.send(Ok(received_at)).unwrap();
    }

    fn fail(self) {
        self.completion
            .send(Err(ExchangeError::UpstreamRequest("fixture outage".into())))
            .unwrap();
    }

    async fn expect_cancelled(&mut self) {
        ready(&mut self.cancelled).await.unwrap();
        assert!(self.completion.is_closed());
    }
}

struct FakeExchange {
    started: mpsc::UnboundedSender<SourceCall>,
    calls: AtomicUsize,
    active: AtomicUsize,
    origin: Instant,
}

struct InFlight<'a> {
    active: &'a AtomicUsize,
    cancelled: Option<oneshot::Sender<()>>,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Some(cancelled) = self.cancelled.take() {
            let _ = cancelled.send(());
        }
    }
}

#[async_trait]
impl MarketStatsSource for FakeExchange {
    fn capabilities(&self) -> MarketStatsCapabilities {
        let fields = source_rows(WALL_ORIGIN)[0]
            .fields
            .iter()
            .map(|(&name, field)| {
                let state = match field.state {
                    MarketStatsFieldState::Unsupported => CapabilityState::Unsupported,
                    MarketStatsFieldState::NotApplicable => CapabilityState::NotApplicable,
                    _ => CapabilityState::Supported,
                };
                (
                    name,
                    FeatureCapability {
                        state,
                        reason: field.reason.clone(),
                    },
                )
            })
            .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: "hyperliquid".into(),
                params: json!({"dex": ""}),
            },
            all_markets: MarketStatsAllMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                active_only: true,
            },
            selected_markets: MarketStatsSelectedMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                limit: 100,
            },
            fields: BTreeMap::from([(UnifiedMarketType::Perp, fields)]),
            upstream_mode: "sharedPolling".into(),
            poll_interval_ms: 30_000,
            stale_after_ms: 90_000,
            ws: MarketStatsWsCapability {
                snapshot: true,
                delta: true,
                max_subscriptions_per_connection: 16,
            },
            funding_kinds: vec![FundingKind::CurrentUnclassified],
            rate_interval_ms: None,
            payment_interval_ms: Some(3_600_000),
            limitations: vec!["rate-basis-unverified".into()],
        })
    }

    async fn fetch_market_stats(
        &self,
        _params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            self.active.fetch_add(1, Ordering::SeqCst),
            0,
            "overlapping acquisition"
        );
        let (completion, result) = oneshot::channel();
        let (cancelled, cancellation) = oneshot::channel();
        let mut in_flight = InFlight {
            active: &self.active,
            cancelled: Some(cancelled),
        };
        self.started
            .send(SourceCall {
                completion,
                cancelled: cancellation,
            })
            .unwrap_or_else(|_| panic!("source controller was dropped"));
        let result = result
            .await
            .expect("source call was neither completed nor cancelled");
        in_flight.cancelled = None;
        result.map(|received_at| MarketStatsSourceSnapshot {
            rows: source_rows(
                WALL_ORIGIN + received_at.duration_since(self.origin).as_millis() as u64,
            ),
            perp_catalog_known: true,
            perp_enumeration_complete: true,
            spot_enumeration_complete: true,
            contexts_valid: true,
            received_at: Some(received_at),
            next_poll_at: received_at + POLL,
            source_failures: Vec::new(),
        })
    }
}

#[async_trait]
impl MarketDataExchange for FakeExchange {
    fn id(&self) -> &'static str {
        "hyperliquid"
    }

    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        Some(self)
    }

    async fn fetch_trades(&self, _: FetchTradesParams) -> Result<Vec<CcxtTrade>, ExchangeError> {
        panic!("statistics must not acquire trades")
    }

    async fn fetch_ohlcv(&self, _: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        panic!("statistics must not acquire candles")
    }

    async fn fetch_order_book(
        &self,
        _: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        panic!("statistics must not acquire an order book")
    }

    async fn fetch_markets(
        &self,
        _: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        panic!("statistics identity must come from the bulk source snapshot")
    }
}

struct SourceController {
    exchange: Arc<FakeExchange>,
    started: mpsc::UnboundedReceiver<SourceCall>,
}

impl SourceController {
    async fn next_call(&mut self) -> SourceCall {
        ready(self.started.recv())
            .await
            .expect("source event channel closed")
    }

    async fn expect_no_call(&mut self) {
        // With paused time this drives runnable tasks and a bounded virtual deadline;
        // it neither sleeps on the wall clock nor silently jumps to another poll.
        assert!(
            timeout(Duration::from_millis(1), self.started.recv())
                .await
                .is_err(),
            "an acquisition started without demand or before its receipt deadline"
        );
    }

    fn calls(&self) -> usize {
        self.exchange.calls.load(Ordering::SeqCst)
    }

    fn active(&self) -> usize {
        self.exchange.active.load(Ordering::SeqCst)
    }
}

fn fixture() -> (Arc<MarketStatsCoordinator>, SourceController) {
    let (started, receiver) = mpsc::unbounded_channel();
    let exchange = Arc::new(FakeExchange {
        started,
        calls: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        origin: Instant::now(),
    });
    let mut registry = ExchangeRegistry::new();
    registry.register(exchange.clone());
    (
        Arc::new(MarketStatsCoordinator::new(Arc::new(registry))),
        SourceController {
            exchange,
            started: receiver,
        },
    )
}

fn market_id(native: &str) -> String {
    make_market_id(
        "hyperliquid",
        UnifiedMarketType::Perp,
        None,
        Some(""),
        native,
    )
    .unwrap()
}

fn request(selected: Option<&str>, fields: &[MarketStatsFieldName]) -> FetchMarketStatsRequest {
    FetchMarketStatsRequest {
        exchange: "hyperliquid".into(),
        market_ids: selected.map(|native| vec![market_id(native)]),
        fields: Some(fields.to_vec()),
        params: json!({"dex": ""}),
    }
}

fn funding_request(selected: Option<&str>) -> FetchMarketStatsRequest {
    request(selected, &[MarketStatsFieldName::Funding])
}

fn source_rows(receipt: u64) -> Vec<MarketStatsRow> {
    ["BTC", "ETH"]
        .into_iter()
        .map(|native| {
            let mut fields = BTreeMap::new();
            fields.insert(
                MarketStatsFieldName::Funding,
                MarketStatsField {
                    state: MarketStatsFieldState::Available,
                    value: Some(MarketStatsValue::Funding(FundingValue {
                        rate: "-0.0000125".into(),
                        kind: FundingKind::CurrentUnclassified,
                        rate_interval_ms: None,
                        payment_interval_ms: Some(3_600_000),
                        payment_timestamp: None,
                        next_payment_timestamp: None,
                    })),
                    reason: Some("rate-basis-unverified".into()),
                    exchange_timestamp: None,
                    received_timestamp: Some(receipt),
                    source: Some(SOURCE.into()),
                },
            );
            for (name, amount) in [
                (MarketStatsFieldName::MarkPrice, "123.45"),
                (MarketStatsFieldName::IndexPrice, "123.40"),
            ] {
                fields.insert(
                    name,
                    MarketStatsField {
                        state: MarketStatsFieldState::Available,
                        value: Some(MarketStatsValue::Price(PriceValue {
                            amount: amount.into(),
                            base_asset: native.into(),
                            quote_asset: "USDT".into(),
                        })),
                        reason: None,
                        exchange_timestamp: None,
                        received_timestamp: Some(receipt),
                        source: Some(SOURCE.into()),
                    },
                );
            }
            for (name, reason) in [
                (
                    MarketStatsFieldName::LastSettledFunding,
                    "adapter-not-implemented",
                ),
                (MarketStatsFieldName::LastPrice, "adapter-not-implemented"),
                (MarketStatsFieldName::OpenInterest, "units-unverified"),
                (MarketStatsFieldName::Volume24h, "units-unverified"),
            ] {
                fields.insert(
                    name,
                    MarketStatsField {
                        state: MarketStatsFieldState::Unsupported,
                        value: None,
                        reason: Some(reason.into()),
                        exchange_timestamp: None,
                        received_timestamp: None,
                        source: None,
                    },
                );
            }
            MarketStatsRow {
                market: UnifiedMarket {
                    exchange: "hyperliquid".into(),
                    symbol: format!("{native}/USDC:USDC"),
                    base: native.into(),
                    quote: "USDC".into(),
                    market_type: UnifiedMarketType::Perp,
                    active: true,
                    min_order_size: Some(0.001),
                    tick_size: Some(0.01),
                    contract_size: None,
                    info: UnifiedMarketInfo {
                        category: None,
                        raw_symbol: Some(native.into()),
                        exchange_symbol: Some(native.into()),
                    },
                    identity: Some(MarketIdentity {
                        market_id: market_id(native),
                        exchange_market_id: native.into(),
                        category: None,
                        dex: Some(String::new()),
                        contract_type: None,
                        settle: Some("USDC".into()),
                        settlement_asset_id: Some("fixture-usdc-token".into()),
                    }),
                },
                fields,
            }
        })
        .collect()
}

async fn ready<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_millis(1), future)
        .await
        .unwrap_or_else(|_| panic!("expected an event at the current virtual instant"))
}

async fn advance_to(deadline: Instant) {
    advance(deadline.saturating_duration_since(Instant::now())).await;
}

fn view(subscription: &mut MarketStatsSubscription) -> MarketStatsSnapshot {
    project_snapshot(
        &subscription.topic,
        &subscription.receiver.borrow_and_update(),
    )
}

fn funding(snapshot: &MarketStatsSnapshot) -> &MarketStatsField {
    &snapshot
        .markets
        .iter()
        .find(|row| row.market.identity.as_ref().unwrap().exchange_market_id == "BTC")
        .expect("BTC was lost from a known catalog")
        .fields[&MarketStatsFieldName::Funding]
}

async fn subscribe_initial(
    coordinator: &MarketStatsCoordinator,
    source: &mut SourceController,
) -> MarketStatsSubscription {
    let mut subscribe = Box::pin(coordinator.subscribe(funding_request(None)));
    assert!(poll!(subscribe.as_mut()).is_pending());
    source.next_call().await.succeed(Instant::now());
    ready(subscribe).await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn market_stats_concurrent_http_and_subscriptions_share_one_bootstrap() {
    let (coordinator, mut source) = fixture();
    let requests = (0..20).map(|index| {
        coordinator.snapshot(funding_request(if index % 2 == 0 {
            None
        } else {
            Some("BTC")
        }))
    });
    let mut demand = Box::pin(async {
        tokio::join!(
            join_all(requests),
            coordinator.subscribe(funding_request(None)),
            coordinator.subscribe(request(Some("BTC"), &[MarketStatsFieldName::MarkPrice])),
        )
    });
    assert!(poll!(demand.as_mut()).is_pending());
    let first = source.next_call().await;
    assert!(poll!(demand.as_mut()).is_pending());
    assert_eq!(source.calls(), 1);
    first.succeed(Instant::now());
    let (snapshots, all, selected) = ready(demand).await;
    let mut all = all.unwrap();
    let mut selected = selected.unwrap();
    let all_view = view(&mut all);
    for (index, snapshot) in snapshots.into_iter().enumerate() {
        let snapshot = snapshot.unwrap();
        assert_eq!(
            snapshot.coverage.expected_markets,
            Some(if index % 2 == 0 { 2 } else { 1 })
        );
        assert!(snapshot.coverage.enumeration_complete);
        assert_eq!(funding(&snapshot), funding(&all_view));
    }
    let selected_view = view(&mut selected);
    assert_eq!(selected_view.markets.len(), 1);
    assert_eq!(selected_view.markets[0].market, all_view.markets[0].market);
    assert_eq!(
        selected_view.markets[0].fields[&MarketStatsFieldName::MarkPrice].received_timestamp,
        funding(&all_view).received_timestamp,
    );
    assert_eq!(source.calls(), 1);
    coordinator.unsubscribe_by_key(&all.key).await;
    coordinator.unsubscribe_by_key(&selected.key).await;
    ready(coordinator.shutdown()).await;
    assert_eq!(source.active(), 0);
}

#[tokio::test(start_paused = true)]
async fn market_stats_duplicate_projection_keeps_polling_until_both_leases_release() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let mut demand = Box::pin(async {
        tokio::join!(
            coordinator.subscribe(funding_request(Some("BTC"))),
            coordinator.subscribe(funding_request(Some("BTC"))),
        )
    });
    assert!(poll!(demand.as_mut()).is_pending());
    source.next_call().await.succeed(origin);
    let (first, second) = ready(demand).await;
    let first = first.unwrap();
    let mut second = second.unwrap();
    assert_eq!(first.key, second.key);
    let initial = view(&mut second);
    coordinator.unsubscribe_by_key(&first.key).await;
    drop(first);

    advance_to(origin + POLL).await;
    source.next_call().await.succeed(Instant::now());
    ready(second.receiver.changed()).await.unwrap();
    let refreshed = view(&mut second);
    assert_eq!(funding(&refreshed).value, funding(&initial).value);
    assert_eq!(
        funding(&refreshed).received_timestamp,
        Some(WALL_ORIGIN + 30_000)
    );
    assert_eq!(source.calls(), 2);

    coordinator.unsubscribe_by_key(&second.key).await;
    advance_to(origin + Duration::from_secs(120)).await;
    source.expect_no_call().await;
    assert_eq!(source.calls(), 2);
    ready(coordinator.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn market_stats_refresh_uses_primary_receipt_deadline_and_never_refreshes_on_read() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let mut subscribe = Box::pin(coordinator.subscribe(funding_request(None)));
    assert!(poll!(subscribe.as_mut()).is_pending());
    let first = source.next_call().await;
    advance_to(origin + Duration::from_secs(5)).await;
    first.succeed(origin);
    let mut subscription = ready(subscribe).await.unwrap();
    let initial = view(&mut subscription);
    assert_eq!(funding(&initial).received_timestamp, Some(WALL_ORIGIN));

    advance_to(origin + Duration::from_secs(29)).await;
    let cached = ready(coordinator.snapshot(funding_request(Some("BTC"))))
        .await
        .unwrap();
    assert_eq!(funding(&cached), funding(&initial));
    assert!(!subscription.receiver.has_changed().unwrap());
    source.expect_no_call().await;

    advance_to(origin + POLL).await;
    let refresh = source.next_call().await;
    assert_eq!(
        source.calls(),
        2,
        "the second observation must not wait 60 seconds"
    );
    let pending = ready(coordinator.snapshot(funding_request(Some("BTC"))))
        .await
        .unwrap();
    assert_eq!(funding(&pending), funding(&initial));
    refresh.succeed(Instant::now());
    ready(subscription.receiver.changed()).await.unwrap();
    let refreshed = view(&mut subscription);
    assert_eq!(funding(&refreshed).value, funding(&initial).value);
    assert_eq!(
        funding(&refreshed).received_timestamp,
        Some(WALL_ORIGIN + 30_000)
    );

    advance_to(origin + Duration::from_secs(59)).await;
    source.expect_no_call().await;
    advance_to(origin + Duration::from_secs(60)).await;
    let mut third = source.next_call().await;
    assert_eq!(source.calls(), 3);
    ready(coordinator.shutdown()).await;
    third.expect_cancelled().await;
}

#[tokio::test(start_paused = true)]
async fn market_stats_failed_selected_subscription_releases_bootstrap_without_http_lease() {
    // Both failure exits matter: upstream failure cannot prove identity, while a
    // healthy complete catalog can reject an unknown ID after provisional demand.
    for upstream_failure in [true, false] {
        let (coordinator, mut source) = fixture();
        let origin = Instant::now();
        let mut subscribe = Box::pin(coordinator.subscribe(funding_request(Some("UNKNOWN"))));
        assert!(poll!(subscribe.as_mut()).is_pending());
        let call = source.next_call().await;
        if upstream_failure {
            call.fail();
        } else {
            call.succeed(origin);
        }
        let result = ready(subscribe).await;
        if upstream_failure {
            assert!(matches!(
                result,
                Err(ApiError::Exchange(ExchangeError::UpstreamRequest(_)))
            ));
        } else {
            assert!(matches!(result, Err(ApiError::Validation(_))));
        }
        advance_to(origin + Duration::from_secs(31)).await;
        source.expect_no_call().await;
        assert_eq!(
            source.calls(),
            1,
            "a rejected subscription retained background demand"
        );
        assert_eq!(source.active(), 0);
        ready(coordinator.shutdown()).await;
    }
}

#[tokio::test(start_paused = true)]
async fn market_stats_http_lease_expires_at_90_seconds_and_new_demand_reacquires() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let mut snapshot = Box::pin(coordinator.snapshot(funding_request(None)));
    assert!(poll!(snapshot.as_mut()).is_pending());
    source.next_call().await.succeed(origin);
    let initial = ready(snapshot).await.unwrap();
    assert_eq!(funding(&initial).received_timestamp, Some(WALL_ORIGIN));

    for seconds in [30, 60] {
        advance_to(origin + Duration::from_secs(seconds)).await;
        source.next_call().await.succeed(Instant::now());
        source.expect_no_call().await;
    }
    advance_to(origin + Duration::from_secs(90)).await;
    source.expect_no_call().await;
    advance_to(origin + Duration::from_secs(200)).await;
    source.expect_no_call().await;
    assert_eq!(source.calls(), 3);

    let resumed_at = Instant::now();
    let mut renewed = Box::pin(coordinator.snapshot(funding_request(Some("BTC"))));
    assert!(
        poll!(renewed.as_mut()).is_pending(),
        "idle state claimed freshness before acquisition"
    );
    source.next_call().await.succeed(resumed_at);
    let refreshed = ready(renewed).await.unwrap();
    assert_eq!(funding(&refreshed).state, MarketStatsFieldState::Available);
    assert_eq!(
        funding(&refreshed).received_timestamp,
        Some(WALL_ORIGIN + resumed_at.duration_since(origin).as_millis() as u64),
    );
    assert_eq!(source.calls(), 4);
    ready(coordinator.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn market_stats_last_subscriber_cancels_pending_source_before_shared_restart() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let subscription = subscribe_initial(&coordinator, &mut source).await;
    advance_to(origin + POLL).await;
    let mut pending = source.next_call().await;
    assert_eq!(source.active(), 1);
    coordinator.unsubscribe_by_key(&subscription.key).await;
    pending.expect_cancelled().await;
    assert_eq!(source.active(), 0);

    // Keeping the old receiver alive is not demand. A new generation must not
    // inherit the cancelled fetch or publish its eventual result.
    let mut restart = Box::pin(async {
        tokio::join!(
            coordinator.subscribe(funding_request(None)),
            coordinator.subscribe(funding_request(Some("BTC"))),
        )
    });
    assert!(poll!(restart.as_mut()).is_pending());
    let fresh = source.next_call().await;
    assert_eq!(source.calls(), 3);
    assert_eq!(source.active(), 1);
    fresh.succeed(Instant::now());
    let (all, selected) = ready(restart).await;
    let mut all = all.unwrap();
    let mut selected = selected.unwrap();
    assert_eq!(funding(&view(&mut all)), funding(&view(&mut selected)));
    assert_eq!(
        funding(&view(&mut all)).received_timestamp,
        Some(WALL_ORIGIN + 30_000)
    );
    assert_eq!(source.calls(), 3);
    coordinator.unsubscribe_by_key(&all.key).await;
    coordinator.unsubscribe_by_key(&selected.key).await;
    ready(coordinator.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn market_stats_shutdown_waits_for_pending_source_cancellation() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let _subscription = subscribe_initial(&coordinator, &mut source).await;
    advance_to(origin + POLL).await;
    let mut pending = source.next_call().await;
    ready(coordinator.shutdown()).await;
    assert_eq!(
        source.active(),
        0,
        "shutdown returned while the source future was alive"
    );
    pending.expect_cancelled().await;
    advance_to(origin + Duration::from_secs(180)).await;
    source.expect_no_call().await;
    assert_eq!(source.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn market_stats_shutdown_cancels_cold_bootstrap_and_wakes_its_waiter() {
    let (coordinator, mut source) = fixture();
    let mut subscribe = Box::pin(coordinator.subscribe(funding_request(None)));
    assert!(poll!(subscribe.as_mut()).is_pending());
    let mut pending = source.next_call().await;
    ready(coordinator.shutdown()).await;
    pending.expect_cancelled().await;
    assert_eq!(source.active(), 0);
    assert!(ready(subscribe).await.is_err());
    assert_eq!(source.calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn market_stats_pending_acquisition_publishes_stale_at_90_seconds_before_resolution() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let mut subscription = subscribe_initial(&coordinator, &mut source).await;
    let initial = view(&mut subscription);
    advance_to(origin + POLL).await;
    let pending = source.next_call().await;

    advance_to(origin + Duration::from_secs(89)).await;
    assert_eq!(
        funding(&view(&mut subscription)).state,
        MarketStatsFieldState::Available
    );
    advance_to(origin + Duration::from_secs(90)).await;
    ready(subscription.receiver.changed()).await.unwrap();
    let stale = view(&mut subscription);
    assert_eq!(funding(&stale).state, MarketStatsFieldState::Stale);
    assert_eq!(funding(&stale).reason.as_deref(), Some("stale-threshold"));
    assert_eq!(funding(&stale).value, funding(&initial).value);
    assert_eq!(
        funding(&stale).received_timestamp,
        funding(&initial).received_timestamp
    );
    assert_eq!(
        source.active(),
        1,
        "expiry waited for acquisition completion"
    );
    assert_eq!(
        source.calls(),
        2,
        "slow acquisition caused overlapping/catch-up polls"
    );

    advance_to(origin + Duration::from_secs(91)).await;
    pending.succeed(Instant::now());
    ready(subscription.receiver.changed()).await.unwrap();
    let recovered = view(&mut subscription);
    assert_eq!(funding(&recovered).state, MarketStatsFieldState::Available);
    assert_eq!(funding(&recovered).value, funding(&initial).value);
    assert_eq!(
        funding(&recovered).received_timestamp,
        Some(WALL_ORIGIN + 91_000)
    );
    assert_eq!(
        funding(&recovered).reason.as_deref(),
        Some("rate-basis-unverified")
    );
    ready(coordinator.shutdown()).await;
}

#[tokio::test(start_paused = true)]
async fn market_stats_failed_poll_publishes_original_receipt_then_recovers_at_next_deadline() {
    let (coordinator, mut source) = fixture();
    let origin = Instant::now();
    let mut subscription = subscribe_initial(&coordinator, &mut source).await;
    let initial = view(&mut subscription);
    advance_to(origin + POLL).await;
    source.next_call().await.fail();
    ready(subscription.receiver.changed()).await.unwrap();
    let failed = view(&mut subscription);
    assert_eq!(funding(&failed).state, MarketStatsFieldState::Stale);
    assert_eq!(funding(&failed).reason.as_deref(), Some("upstream-failure"));
    assert_eq!(funding(&failed).value, funding(&initial).value);
    assert_eq!(
        funding(&failed).received_timestamp,
        funding(&initial).received_timestamp
    );
    assert!(!failed.coverage.enumeration_complete);
    assert_eq!(
        failed
            .markets
            .iter()
            .map(|row| &row.market)
            .collect::<Vec<_>>(),
        initial
            .markets
            .iter()
            .map(|row| &row.market)
            .collect::<Vec<_>>(),
        "a source failure must not remove known catalog membership",
    );

    advance_to(origin + Duration::from_secs(60)).await;
    source.next_call().await.succeed(Instant::now());
    ready(subscription.receiver.changed()).await.unwrap();
    let recovered = view(&mut subscription);
    assert_eq!(funding(&recovered).state, MarketStatsFieldState::Available);
    assert_eq!(
        funding(&recovered).received_timestamp,
        Some(WALL_ORIGIN + 60_000)
    );
    assert!(recovered.coverage.enumeration_complete);
    assert!(recovered.coverage.source_failures.is_empty());
    assert_eq!(source.calls(), 3);
    ready(coordinator.shutdown()).await;
}
