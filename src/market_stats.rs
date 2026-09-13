use std::{
    collections::{BTreeMap, HashMap},
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
        registry::ExchangeRegistry,
        traits::{ExchangeError, MarketDataExchange},
    },
    models::{
        FetchMarketStatsParams, FetchMarketStatsRequest, MarketStatsFieldName, MarketStatsRow,
        MarketStatsSnapshot, MarketStatsSourceFailure, MarketStatsTopic, UnifiedMarketType,
    },
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
    pub perp_catalog_known: bool,
    pub perp_enumeration_complete: bool,
    pub spot_enumeration_complete: bool,
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
}

impl SourceState {
    fn has_demand(&self, now: Instant) -> bool {
        self.pending > 0 || self.subscribers > 0 || self.http_until.is_some_and(|until| until > now)
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
}

impl Drop for PendingDemand {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let inner = self.inner.clone();
        let key = self.acquisition_key.clone();
        tokio::spawn(async move {
            let mut state = inner.state.lock().await;
            if let Some(source) = state.sources.get_mut(&key) {
                source.pending = source.pending.saturating_sub(1);
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
        let acquisition_key = serde_json::to_string(&(&topic.exchange, &topic.params))
            .map_err(|error| ApiError::Exchange(ExchangeError::Internal(error.to_string())))?;
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
                        perp_catalog_known: false,
                        perp_enumeration_complete: false,
                        spot_enumeration_complete: false,
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
                    }
                });
            if !source.has_demand(Instant::now()) {
                if let Some(worker) = source.worker.take() {
                    worker.abort();
                    // Cancellation completes under the same ordering fence before restart.
                    let _ = worker.await;
                }
            }
            source.pending += 1;
            if source.worker.is_none() {
                source.epoch += 1;
                source.initialized = false;
                let inner = self.inner.clone();
                let key = acquisition_key.clone();
                let params = topic.params.clone();
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
        lease.subscribers -= 1;
        if lease.subscribers == 0 {
            state.projections.remove(key);
        }
        if let Some(source) = state.sources.get_mut(&acquisition_key) {
            source.subscribers = source.subscribers.saturating_sub(1);
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

fn acquire_source(exchange: Arc<dyn MarketDataExchange>, params: Value) -> SourceFuture {
    Box::pin(async move {
        let source = exchange
            .market_stats_source()
            .ok_or_else(|| ExchangeError::Internal("statistics source disappeared".to_string()))?;
        source
            .fetch_market_stats(FetchMarketStatsParams { params })
            .await
    })
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
    let mut in_flight = Some(acquire_source(exchange.clone(), params.clone()));
    let mut outcome = None;
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
            if closed || !source.has_demand(Instant::now()) {
                // Drop the in-flight future/gate before allowing a replacement worker.
                drop(in_flight.take());
                source.worker = None;
                return;
            }
            if let Some(result) = outcome.take() {
                let previous = source.latest.borrow().clone();
                let fresh = merge_outcome(&previous, result, exchange.id());
                source.latest.send_replace(Arc::new(fresh));
                source.initialized = true;
            }
            let expired = expire_snapshot(&source.latest.borrow(), Instant::now());
            if let Some(expired) = expired {
                source.latest.send_replace(Arc::new(expired));
            }
            let deadline = source.latest.borrow().next_poll_at;
            deadline
        };
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = wake.notified() => {},
            _ = timer.tick() => {},
            result = async { match in_flight.as_mut() { Some(future) => future.await, None => std::future::pending().await } } => {
                in_flight = None;
                outcome = Some(result);
            },
            _ = tokio::time::sleep_until(deadline), if in_flight.is_none() => {
                in_flight = Some(acquire_source(exchange.clone(), params.clone()));
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn market_stats_ids_have_exact_catalog_encoding() {
        assert_eq!(
            make_market_id(
                "hyperliquid",
                UnifiedMarketType::Perp,
                None,
                Some(""),
                "BTC"
            )
            .unwrap(),
            r#"["hyperliquid","perp",null,"","BTC"]"#,
        );
        assert_eq!(
            make_market_id("hyperliquid", UnifiedMarketType::Spot, None, None, "0").unwrap(),
            r#"["hyperliquid","spot",null,null,"0"]"#,
        );
    }

    #[test]
    fn market_stats_ids_preserve_native_names_and_namespace_boundaries() {
        let identities = [
            (
                "hyperliquid",
                UnifiedMarketType::Perp,
                None,
                Some(""),
                "A:B",
            ),
            ("hyperliquid", UnifiedMarketType::Perp, None, Some("A"), "B"),
            (
                "hyperliquid",
                UnifiedMarketType::Perp,
                Some(""),
                None,
                "A:B",
            ),
            ("hyperliquid", UnifiedMarketType::Perp, None, None, "A:B"),
            ("hyperliquid", UnifiedMarketType::Spot, None, None, "A:B"),
            ("other", UnifiedMarketType::Perp, None, Some(""), "A:B"),
            ("hyperliquid", UnifiedMarketType::Perp, None, Some(""), "AB"),
            (
                "hyperliquid",
                UnifiedMarketType::Perp,
                None,
                Some(""),
                "a:b",
            ),
            (
                "hyperliquid",
                UnifiedMarketType::Perp,
                None,
                Some(""),
                "A\"\\/雪",
            ),
        ];
        let mut ids = HashSet::new();
        for (exchange, market_type, category, dex, native_id) in identities {
            let id = make_market_id(exchange, market_type, category, dex, native_id).unwrap();
            let decoded: (
                String,
                UnifiedMarketType,
                Option<String>,
                Option<String>,
                String,
            ) = serde_json::from_str(&id).unwrap();
            assert_eq!(
                decoded,
                (
                    exchange.to_string(),
                    market_type,
                    category.map(str::to_string),
                    dex.map(str::to_string),
                    native_id.to_string(),
                ),
            );
            assert!(ids.insert(id), "distinct native identities collided");
        }
    }
}
