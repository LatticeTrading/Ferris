use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use serde_json::Value;
use tokio::{
    sync::{watch, Mutex, Notify},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    errors::ApiError,
    exchanges::{
        ccxt::{statistics_profile, Venue},
        registry::ExchangeRegistry,
        traits::{ExchangeError, MarketDataExchange},
    },
    models::{
        FetchMarketStatsParams, FetchMarketStatsRequest, MarketStatsField, MarketStatsFieldName,
        MarketStatsRow, MarketStatsSnapshot, MarketStatsSourceFailure, MarketStatsTopic,
        UnifiedMarketType,
    },
    realtime::{RealtimeReceiver, RealtimeSubscription, RealtimeUpdate, StatisticsUpdate},
};

mod projection;
pub use projection::project_snapshot;
use projection::{expire_snapshot, merge_outcome, normalize_topic, validate_selection};

#[cfg(test)]
mod lifecycle_tests;

pub fn make_market_id(
    exchange: &str,
    market_type: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    exchange_market_id: &str,
) -> Result<String, ExchangeError> {
    serde_json::to_string(&(exchange, market_type, category, dex, exchange_market_id)).map_err(
        |err| ExchangeError::Internal(format!("failed to serialize market identity: {err}")),
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketStatsSourceSnapshot {
    pub rows: Vec<MarketStatsRow>,
    pub catalog_known: bool,
    pub complete_catalogs: BTreeSet<UnifiedMarketType>,
    pub contexts_valid: bool,
    // Primary receipt time for age calculations; wall-clock receipts live on each field.
    pub received_at: Option<Instant>,
    pub field_received_at: HashMap<String, BTreeMap<MarketStatsFieldName, Instant>>,
    pub next_poll_at: Instant,
    pub source_failures: Vec<MarketStatsSourceFailure>,
}

#[derive(Clone)]
pub struct MarketStatsCoordinator {
    inner: Arc<CoordinatorInner>,
}

struct CoordinatorInner {
    registry: Arc<ExchangeRegistry>,
    state: Mutex<CoordinatorState>,
    shutdown: watch::Sender<bool>,
}

#[derive(Default)]
struct CoordinatorState {
    sources: HashMap<String, SourceState>,
    projections: HashMap<String, ProjectionLease>,
    closed: bool,
}

struct ProjectionLease {
    acquisition_key: String,
    subscribers: usize,
    interest_ids: Vec<String>,
}

struct SourceState {
    latest: watch::Sender<Arc<MarketStatsSourceSnapshot>>,
    wake: Arc<Notify>,
    worker: Option<JoinHandle<()>>,
    epoch: u64,
    initialized: bool,
    pending: usize,
    subscribers: usize,
    http_until: Option<Instant>,
    interest: HashMap<String, InterestDemand>,
}

impl SourceState {
    fn has_demand(&self, now: Instant) -> bool {
        self.pending > 0 || self.subscribers > 0 || self.http_until.is_some_and(|until| until > now)
    }
}

#[derive(Default)]
struct InterestDemand {
    pending: usize,
    subscribers: usize,
    http_until: Option<Instant>,
    last_attempt: Option<Instant>,
    in_flight: bool,
}

impl InterestDemand {
    fn has_demand(&self, now: Instant) -> bool {
        self.pending > 0 || self.subscribers > 0 || self.http_until.is_some_and(|until| until > now)
    }

    fn due(&self, now: Instant) -> bool {
        self.has_demand(now)
            && !self.in_flight
            && self
                .last_attempt
                .is_none_or(|at| now.saturating_duration_since(at) >= Duration::from_secs(30))
    }
}

pub struct MarketStatsSubscription {
    pub topic: MarketStatsTopic,
    pub key: String,
    pub receiver: watch::Receiver<Arc<MarketStatsSourceSnapshot>>,
}

// A cancelled HTTP/bootstrap future must release provisional demand, too.
struct PendingDemand {
    inner: Arc<CoordinatorInner>,
    acquisition_key: String,
    armed: bool,
    interest_ids: Vec<String>,
}

impl Drop for PendingDemand {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let key = self.acquisition_key.clone();
        let interest_ids = std::mem::take(&mut self.interest_ids);
        tokio::spawn(async move {
            let mut state = inner.state.lock().await;
            if let Some(source) = state.sources.get_mut(&key) {
                source.pending = source.pending.saturating_sub(1);
                for id in interest_ids {
                    if let Some(demand) = source.interest.get_mut(&id) {
                        demand.pending = demand.pending.saturating_sub(1);
                    }
                }
                source.wake.notify_one();
            }
        });
    }
}

pub fn projection_key(topic: &MarketStatsTopic) -> Result<String, ApiError> {
    serde_json::to_string(topic)
        .map_err(|error| ApiError::Exchange(ExchangeError::Internal(error.to_string())))
}

impl MarketStatsCoordinator {
    pub fn new(registry: Arc<ExchangeRegistry>) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            inner: Arc::new(CoordinatorInner {
                registry,
                state: Mutex::new(CoordinatorState::default()),
                shutdown,
            }),
        }
    }

    pub fn normalize_request(
        &self,
        mut request: FetchMarketStatsRequest,
    ) -> Result<MarketStatsTopic, ApiError> {
        request.exchange = request.exchange.trim().to_ascii_lowercase();
        let exchange = self
            .inner
            .registry
            .get(&request.exchange)
            .ok_or_else(|| ApiError::UnsupportedExchange(request.exchange.clone()))?;
        if exchange.market_stats_source().is_none() {
            return Err(ApiError::UnsupportedFeature(format!(
                "market statistics are not implemented for exchange '{}'",
                request.exchange
            )));
        }
        normalize_topic(request)
    }

    pub async fn snapshot(
        &self,
        request: FetchMarketStatsRequest,
    ) -> Result<MarketStatsSnapshot, ApiError> {
        let topic = self.normalize_request(request)?;
        let (_, latest) = self.acquire(&topic, false).await?;
        Ok(project_snapshot(&topic, &latest))
    }

    pub async fn subscribe(
        &self,
        request: FetchMarketStatsRequest,
    ) -> Result<MarketStatsSubscription, ApiError> {
        let topic = self.normalize_request(request)?;
        let key = projection_key(&topic)?;
        let (receiver, _) = self.acquire(&topic, true).await?;
        Ok(MarketStatsSubscription {
            topic,
            key,
            receiver,
        })
    }

    async fn acquire(
        &self,
        topic: &MarketStatsTopic,
        persistent: bool,
    ) -> Result<
        (
            watch::Receiver<Arc<MarketStatsSourceSnapshot>>,
            Arc<MarketStatsSourceSnapshot>,
        ),
        ApiError,
    > {
        let venue = Venue::from_public_id(&topic.exchange)
            .ok_or_else(|| ApiError::UnsupportedExchange(topic.exchange.clone()))?;
        let params = statistics_profile::acquisition_params(venue, &topic.params);
        let acquisition_key = serde_json::to_string(&(&topic.exchange, &params))
            .map_err(|error| ApiError::Exchange(ExchangeError::Internal(error.to_string())))?;
        let interest_ids = if statistics_profile::selected_open_interest(venue).is_some()
            && topic.fields.contains(&MarketStatsFieldName::OpenInterest)
        {
            topic
                .market_ids
                .clone()
                .expect("selected-only interest was validated")
        } else {
            Vec::new()
        };
        let projection_key = projection_key(topic)?;
        let exchange = self
            .inner
            .registry
            .get(&topic.exchange)
            .ok_or_else(|| ApiError::UnsupportedExchange(topic.exchange.clone()))?;
        let mut shutdown = self.inner.shutdown.subscribe();
        let mut receiver = {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Err(coordinator_closed());
            }
            let source = state
                .sources
                .entry(acquisition_key.clone())
                .or_insert_with(|| {
                    let initial = MarketStatsSourceSnapshot {
                        rows: Vec::new(),
                        catalog_known: false,
                        complete_catalogs: BTreeSet::new(),
                        contexts_valid: false,
                        received_at: None,
                        field_received_at: HashMap::new(),
                        next_poll_at: Instant::now(),
                        source_failures: Vec::new(),
                    };
                    let (latest, _) = watch::channel(Arc::new(initial));
                    SourceState {
                        latest,
                        wake: Arc::new(Notify::new()),
                        worker: None,
                        epoch: 0,
                        initialized: false,
                        pending: 0,
                        subscribers: 0,
                        http_until: None,
                        interest: HashMap::new(),
                    }
                });
            if !source.has_demand(Instant::now()) {
                if let Some(worker) = source.worker.take() {
                    worker.abort();
                    // Cancellation completes under the same ordering fence before restart.
                    let _ = worker.await;
                }
                for demand in source.interest.values_mut() {
                    demand.in_flight = false;
                }
            }
            source.pending += 1;
            for id in &interest_ids {
                let demand = source.interest.entry(id.clone()).or_default();
                if !demand.has_demand(Instant::now())
                    && demand
                        .last_attempt
                        .is_some_and(|at| at.elapsed() >= Duration::from_secs(30))
                {
                    demand.last_attempt = None;
                }
                demand.pending += 1;
            }
            if source.worker.is_none() {
                source.epoch += 1;
                source.initialized = false;
                let inner = self.inner.clone();
                let key = acquisition_key.clone();
                let params = params.clone();
                let epoch = source.epoch;
                let wake = source.wake.clone();
                source.worker = Some(tokio::spawn(run_source(
                    inner, key, epoch, exchange, params, wake,
                )));
            }
            source.wake.notify_one();
            source.latest.subscribe()
        };
        let mut demand = PendingDemand {
            inner: self.inner.clone(),
            acquisition_key: acquisition_key.clone(),
            armed: true,
            interest_ids: interest_ids.clone(),
        };
        loop {
            let mut state = self.inner.state.lock().await;
            if state.closed {
                return Err(coordinator_closed());
            }
            let Some(source) = state.sources.get_mut(&acquisition_key) else {
                return Err(coordinator_closed());
            };
            if source.initialized {
                let expired = expire_snapshot(&source.latest.borrow(), Instant::now());
                if let Some(expired) = expired {
                    source.latest.send_replace(Arc::new(expired));
                }
                let latest = source.latest.borrow().clone();
                let valid = validate_selection(topic, &latest);
                if valid.is_ok()
                    && interest_ids.iter().any(|id| {
                        source
                            .interest
                            .get(id)
                            .is_none_or(|demand| demand.last_attempt.is_none())
                    })
                {
                    drop(state);
                    tokio::select! {
                        result = receiver.changed() => { result.map_err(|_| coordinator_closed())?; },
                        _ = shutdown.changed() => { return Err(coordinator_closed()); },
                    }
                    continue;
                }
                for id in &interest_ids {
                    let demand = source
                        .interest
                        .get_mut(id)
                        .expect("pending interest demand");
                    demand.pending -= 1;
                    if valid.is_ok() {
                        if persistent {
                            demand.subscribers += 1;
                        } else {
                            demand.http_until = Some(Instant::now() + Duration::from_secs(90));
                        }
                    }
                }
                source.pending -= 1;
                demand.armed = false;
                if valid.is_ok() {
                    if persistent {
                        source.subscribers += 1;
                    } else {
                        source.http_until = Some(Instant::now() + Duration::from_secs(90));
                    }
                }
                source.wake.notify_one();
                valid?;
                if persistent {
                    let lease = state.projections.entry(projection_key).or_insert_with(|| {
                        ProjectionLease {
                            acquisition_key,
                            subscribers: 0,
                            interest_ids,
                        }
                    });
                    lease.subscribers += 1;
                }
                return Ok((receiver, latest));
            }
            drop(state);
            tokio::select! {
                result = receiver.changed() => { result.map_err(|_| coordinator_closed())?; },
                _ = shutdown.changed() => { return Err(coordinator_closed()); },
            }
        }
    }

    pub async fn unsubscribe_by_key(&self, key: &str) {
        let mut state = self.inner.state.lock().await;
        let Some(lease) = state.projections.get_mut(key) else {
            return;
        };
        let acquisition_key = lease.acquisition_key.clone();
        let interest_ids = lease.interest_ids.clone();
        lease.subscribers -= 1;
        if lease.subscribers == 0 {
            state.projections.remove(key);
        }
        if let Some(source) = state.sources.get_mut(&acquisition_key) {
            source.subscribers = source.subscribers.saturating_sub(1);
            for id in interest_ids {
                if let Some(demand) = source.interest.get_mut(&id) {
                    demand.subscribers = demand.subscribers.saturating_sub(1);
                }
            }
            source.wake.notify_one();
        }
    }

    pub async fn shutdown(&self) {
        let workers = {
            let mut state = self.inner.state.lock().await;
            state.closed = true;
            self.inner.shutdown.send_replace(true);
            state.projections.clear();
            state
                .sources
                .values_mut()
                .filter_map(|source| source.worker.take())
                .collect::<Vec<_>>()
        };
        for worker in workers {
            let _ = worker.await;
        }
    }
}

fn coordinator_closed() -> ApiError {
    ApiError::Exchange(ExchangeError::Internal(
        "market statistics coordinator is shut down".to_string(),
    ))
}

type SourceFuture =
    Pin<Box<dyn Future<Output = Result<MarketStatsSourceSnapshot, ExchangeError>> + Send>>;

fn acquire_source(
    exchange: Arc<dyn MarketDataExchange>,
    params: FetchMarketStatsParams,
) -> SourceFuture {
    Box::pin(async move {
        let source = exchange
            .market_stats_source()
            .ok_or_else(|| ExchangeError::Internal("statistics source disappeared".to_string()))?;
        source.fetch_market_stats(params).await
    })
}

type LiveFuture =
    Pin<Box<dyn Future<Output = Result<Option<RealtimeSubscription>, ExchangeError>> + Send>>;

fn acquire_live(exchange: Arc<dyn MarketDataExchange>) -> LiveFuture {
    Box::pin(async move {
        exchange
            .market_stats_source()
            .ok_or_else(|| ExchangeError::Internal("statistics source disappeared".into()))?
            .subscribe_market_stats()
            .await
    })
}

struct SourceCall {
    future: SourceFuture,
    interest_ids: Vec<String>,
    include_bulk: bool,
}

type LiveFields = HashMap<String, BTreeMap<MarketStatsFieldName, (MarketStatsField, Instant)>>;

fn collect_live(pending: &mut LiveFields, update: &StatisticsUpdate) {
    for row in &update.rows {
        let fields = pending.entry(row.market_id.clone()).or_default();
        for (name, field) in &row.fields {
            fields.insert(*name, (field.clone(), row.received_at));
        }
    }
}

fn apply_live(snapshot: &mut MarketStatsSourceSnapshot, pending: &mut LiveFields) {
    for row in &mut snapshot.rows {
        let Some(identity) = &row.market.identity else {
            continue;
        };
        let Some(fields) = pending.remove(&identity.market_id) else {
            continue;
        };
        if !row.market.active {
            continue;
        }
        let receipts = snapshot
            .field_received_at
            .entry(identity.market_id.clone())
            .or_default();
        for (name, (field, received_at)) in fields {
            if receipts.get(&name).is_some_and(|at| *at >= received_at) {
                continue;
            }
            row.fields.insert(name, field);
            receipts.insert(name, received_at);
        }
    }
}

fn fail_live(
    snapshot: &mut MarketStatsSourceSnapshot,
    policy: &statistics_profile::LiveStatisticsPolicy,
    message: &str,
) {
    snapshot
        .source_failures
        .retain(|failure| failure.source != policy.source);
    snapshot.source_failures.push(MarketStatsSourceFailure {
        source: policy.source.into(),
        reason: "upstream-failure".into(),
        message: message.into(),
    });
    for row in &mut snapshot.rows {
        if !policy.failure_products.contains(&row.market.market_type) || !row.market.active {
            continue;
        }
        for (name, field) in &mut row.fields {
            if field.source.as_deref() == Some(policy.source)
                || policy.failure_fields.contains(name)
            {
                projection::fail_field(field, "upstream-failure");
            }
        }
    }
}

async fn run_source(
    inner: Arc<CoordinatorInner>,
    key: String,
    epoch: u64,
    exchange: Arc<dyn MarketDataExchange>,
    params: Value,
    wake: Arc<Notify>,
) {
    let mut shutdown = inner.shutdown.subscribe();
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let live_policy =
        Venue::from_public_id(exchange.id()).and_then(statistics_profile::live_statistics);
    let maintained = live_policy.is_some();
    let mut live_setup = maintained.then(|| acquire_live(exchange.clone()));
    let mut live_receiver: Option<RealtimeReceiver> = None;
    let mut live_ready = !maintained;
    let mut live_retry = Instant::now();
    let mut live_bootstrap = Instant::now() + Duration::from_secs(10);
    let mut live_error: Option<String> = None;
    let mut live_pending = LiveFields::new();
    let mut live_changed = false;
    let mut bulk_ready = false;
    let mut in_flight: Option<SourceCall> = None;
    let mut outcome: Option<(
        Result<MarketStatsSourceSnapshot, ExchangeError>,
        Vec<String>,
        bool,
    )> = None;
    loop {
        let deadline = {
            let mut state = inner.state.lock().await;
            let closed = state.closed;
            let Some(source) = state.sources.get_mut(&key) else {
                return;
            };
            if source.epoch != epoch {
                return;
            }
            let now = Instant::now();
            if closed || !source.has_demand(now) {
                drop(in_flight.take());
                drop(live_receiver.take());
                drop(live_setup.take());
                for demand in source.interest.values_mut() {
                    demand.in_flight = false;
                }
                source.worker = None;
                return;
            }
            if maintained && !live_ready && now >= live_bootstrap {
                live_error =
                    Some("timed out waiting for the initial stock statistics observation".into());
                live_ready = true;
                live_changed = true;
            }
            if let Some((result, interest_ids, include_bulk)) = outcome.take() {
                let previous = source.latest.borrow().clone();
                let mut fresh = merge_outcome(&previous, result, exchange.id(), &params);
                if !include_bulk {
                    fresh.next_poll_at = previous.next_poll_at;
                }
                for id in interest_ids {
                    if let Some(demand) = source.interest.get_mut(&id) {
                        demand.in_flight = false;
                        demand.last_attempt = Some(now);
                    }
                }
                apply_live(&mut fresh, &mut live_pending);
                if let (Some(policy), Some(error)) = (live_policy, &live_error) {
                    fail_live(&mut fresh, policy, error);
                }
                bulk_ready |= include_bulk;
                source.latest.send_replace(Arc::new(fresh));
            }
            if live_changed {
                let previous = source.latest.borrow().clone();
                let mut fresh = (*previous).clone();
                apply_live(&mut fresh, &mut live_pending);
                if fresh.catalog_known {
                    live_pending.clear();
                }
                if let Some(policy) = live_policy {
                    if let Some(error) = &live_error {
                        fail_live(&mut fresh, policy, error);
                    } else {
                        fresh
                            .source_failures
                            .retain(|failure| failure.source != policy.source);
                    }
                }
                source.latest.send_replace(Arc::new(fresh));
                live_changed = false;
            }
            let initialized = bulk_ready && live_ready;
            if initialized && !source.initialized {
                source.initialized = true;
                source.latest.send_modify(|_| {});
            }
            let expired = expire_snapshot(&source.latest.borrow(), now);
            if let Some(expired) = expired {
                source.latest.send_replace(Arc::new(expired));
            }
            source.interest.retain(|_, demand| {
                demand.has_demand(now)
                    || demand.in_flight
                    || demand.last_attempt.is_some_and(|at| {
                        now.saturating_duration_since(at) < Duration::from_secs(90)
                    })
            });
            let bulk_deadline = source.latest.borrow().next_poll_at;
            if in_flight.is_none() {
                let include_bulk = !bulk_ready || now >= bulk_deadline;
                let mut interest_ids: Vec<_> = source
                    .interest
                    .iter_mut()
                    .filter_map(|(id, demand)| {
                        if demand.due(now) {
                            demand.in_flight = true;
                            Some(id.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                interest_ids.sort_unstable();
                if include_bulk || !interest_ids.is_empty() {
                    in_flight = Some(SourceCall {
                        future: acquire_source(
                            exchange.clone(),
                            FetchMarketStatsParams {
                                params: params.clone(),
                                open_interest_market_ids: interest_ids.clone(),
                                include_bulk,
                            },
                        ),
                        interest_ids,
                        include_bulk,
                    });
                }
            }
            if maintained && live_setup.is_none() && live_receiver.is_none() && now >= live_retry {
                live_setup = Some(acquire_live(exchange.clone()));
            }
            bulk_deadline
        };
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = wake.notified() => {},
            _ = timer.tick() => {},
            result = async { match in_flight.as_mut() { Some(call) => call.future.as_mut().await, None => std::future::pending().await } } => {
                let call = in_flight.take().expect("completed source call");
                outcome = Some((result, call.interest_ids, call.include_bulk));
            },
            result = async { match live_setup.as_mut() { Some(future) => future.await, None => std::future::pending().await } } => {
                live_setup = None;
                match result {
                    Ok(Some(subscription)) => {
                        live_receiver = Some(subscription.receiver);
                        live_bootstrap = Instant::now() + Duration::from_secs(10);
                    }
                    Ok(None) => { live_ready = true; }
                    Err(error) => {
                        live_error = Some(error.to_string());
                        live_ready = true;
                        live_changed = true;
                        live_retry = Instant::now() + Duration::from_secs(30);
                    }
                }
            },
            update = async { match live_receiver.as_mut() { Some(receiver) => receiver.recv().await, None => std::future::pending().await } } => {
                match update {
                    Ok(RealtimeUpdate::Statistics(update)) => {
                        collect_live(&mut live_pending, &update);
                        live_ready = true;
                        live_error = None;
                        live_changed = true;
                    }
                    Ok(RealtimeUpdate::Error(error)) => {
                        live_ready = true;
                        live_error = Some(error.to_string());
                        live_changed = true;
                    }
                    Err(error) => {
                        if matches!(error, tokio::sync::broadcast::error::RecvError::Closed) {
                            live_receiver = None;
                            live_retry = Instant::now() + Duration::from_secs(30);
                        }
                        live_ready = true;
                        live_error = Some(error.to_string());
                        live_changed = true;
                    }
                    _ => {},
                }
            },
            _ = tokio::time::sleep_until(deadline), if in_flight.is_none() => {},
        }
    }
}

#[cfg(test)]
mod tests;
