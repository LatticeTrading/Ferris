use std::{
    collections::HashMap,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::FutureExt;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    config::Config,
    exchanges::traits::ExchangeError,
    market_stats::MarketStatsSourceSnapshot,
    models::FetchMarketStatsParams,
    realtime::{RealtimeChannel, RealtimeSubscription, RealtimeTopic},
};

use super::{
    catalog::Catalog,
    live::{CatalogWatch, LiveHub},
    rest::{fetch_snapshot, SnapshotRequest, SnapshotResponse},
    statistics::fetch_statistics,
    statistics_profile,
    stream::{live_scope, prepare_lighter_statistics, prepare_live, PreparedTopic},
    venue::{exchange_error, CatalogScope, Provider, ProviderConfig, Venue},
};

const COMMAND_CAPACITY: usize = 32;
const CATALOG_TTL: Duration = Duration::from_secs(30);

/// A successful source observation. Cache hits share this allocation and retain
/// its original receipt timestamp and generation.
pub struct CatalogSnapshot {
    pub venue: Venue,
    pub scope: CatalogScope,
    pub catalog: Catalog,
    pub timestamp: u64,
    pub generation: u64,
}

/// Send + Sync handles only. All stock cores, Values, and conversion work stay
/// inside dedicated current-thread runtimes, never on the Axum worker pool.
#[derive(Clone)]
pub struct CcxtService {
    inner: Arc<ServiceInner>,
}

struct ServiceInner {
    owners: HashMap<Venue, Owner>,
    live: LiveHub,
}

struct Owner {
    commands: mpsc::Sender<Command>,
    stop: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

enum Command {
    Catalog {
        scope: CatalogScope,
        // Refresh only if this observation is still current. Concurrent refresh
        // requests based on the same generation share a single upstream load.
        observed_generation: Option<u64>,
        reply: oneshot::Sender<Result<Arc<CatalogSnapshot>, ExchangeError>>,
    },
    Snapshot {
        scope: CatalogScope,
        request: SnapshotRequest,
        reply: oneshot::Sender<Result<SnapshotResponse, ExchangeError>>,
    },
    PrepareLive {
        scope: CatalogScope,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
        reply: oneshot::Sender<Result<PreparedTopic, ExchangeError>>,
    },
    Statistics {
        scope: CatalogScope,
        params: FetchMarketStatsParams,
        reply: oneshot::Sender<Result<MarketStatsSourceSnapshot, ExchangeError>>,
    },
    PrepareStatistics {
        reply: oneshot::Sender<Result<PreparedTopic, ExchangeError>>,
    },
}

#[derive(Default)]
struct CatalogState {
    provider: Option<Provider>,
    snapshot: Option<Arc<CatalogSnapshot>>,
    loaded_at: Option<Instant>,
    watch: Option<Arc<CatalogWatch>>,
}

impl CatalogState {
    fn is_fresh(&self) -> bool {
        self.loaded_at
            .is_some_and(|loaded_at| loaded_at.elapsed() < CATALOG_TTL)
    }
}

impl CcxtService {
    pub fn start(config: &Config) -> Result<Self, ExchangeError> {
        // Validate every configuration before starting any threads.
        let configs = Venue::ALL
            .into_iter()
            .map(|venue| ProviderConfig::new(venue, config).map(|config| (venue, config)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut owners = HashMap::with_capacity(Venue::ALL.len());
        for (venue, config) in configs {
            owners.insert(venue, Owner::start(venue, config)?);
        }
        Ok(Self {
            inner: Arc::new(ServiceInner {
                owners,
                live: LiveHub::default(),
            }),
        })
    }

    pub async fn catalog(
        &self,
        venue: Venue,
        scope: CatalogScope,
    ) -> Result<Arc<CatalogSnapshot>, ExchangeError> {
        self.request(venue, scope, None).await
    }

    /// Reload metadata, sharing refreshes based on the same prior observation.
    /// Failure returns an error and never overwrites the last valid snapshot.
    pub async fn refresh_catalog(
        &self,
        previous: &CatalogSnapshot,
    ) -> Result<Arc<CatalogSnapshot>, ExchangeError> {
        self.request(previous.venue, previous.scope, Some(previous.generation))
            .await
    }

    async fn request(
        &self,
        venue: Venue,
        scope: CatalogScope,
        observed_generation: Option<u64>,
    ) -> Result<Arc<CatalogSnapshot>, ExchangeError> {
        let scope = venue.scope(scope)?;
        let owner = &self.inner.owners[&venue];
        let (reply, response) = oneshot::channel();
        owner
            .commands
            .send(Command::Catalog {
                scope,
                observed_generation,
                reply,
            })
            .await
            .map_err(|_| owner_stopped(venue))?;
        response.await.map_err(|_| owner_stopped(venue))?
    }

    pub(super) async fn snapshot(
        &self,
        venue: Venue,
        request: SnapshotRequest,
    ) -> Result<SnapshotResponse, ExchangeError> {
        let scope = request.scope(venue)?;
        let (reply, response) = oneshot::channel();
        self.inner.owners[&venue]
            .commands
            .send(Command::Snapshot {
                scope,
                request,
                reply,
            })
            .await
            .map_err(|_| owner_stopped(venue))?;
        response.await.map_err(|_| owner_stopped(venue))?
    }

    pub(crate) async fn prepare_live(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    ) -> Result<PreparedTopic, ExchangeError> {
        let topic = RealtimeTopic::from_client_request(
            Some(topic.exchange),
            Some(topic.symbol),
            topic.params,
        )
        .map_err(ExchangeError::BadSymbol)?;
        let venue = Venue::from_public_id(&topic.exchange).ok_or_else(|| {
            ExchangeError::UnsupportedFeature(format!("unsupported exchange `{}`", topic.exchange))
        })?;
        let scope = live_scope(venue, channel, &topic.params)?;
        let (reply, response) = oneshot::channel();
        self.inner.owners[&venue]
            .commands
            .send(Command::PrepareLive {
                scope,
                channel,
                topic,
                reply,
            })
            .await
            .map_err(|_| owner_stopped(venue))?;
        response.await.map_err(|_| owner_stopped(venue))?
    }

    pub(crate) async fn subscribe_live(
        &self,
        prepared: PreparedTopic,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        self.inner.live.subscribe(prepared).await
    }

    pub(super) async fn statistics(
        &self,
        venue: Venue,
        mut params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        params.params = statistics_profile::normalize_params(venue, &params.params)?;
        let scope = statistics_profile::scope(venue, &params.params)?;
        let (reply, response) = oneshot::channel();
        self.inner.owners[&venue]
            .commands
            .send(Command::Statistics {
                scope,
                params,
                reply,
            })
            .await
            .map_err(|_| owner_stopped(venue))?;
        response.await.map_err(|_| owner_stopped(venue))?
    }

    pub(super) async fn subscribe_lighter_statistics(
        &self,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        let (reply, response) = oneshot::channel();
        self.inner.owners[&Venue::Lighter]
            .commands
            .send(Command::PrepareStatistics { reply })
            .await
            .map_err(|_| owner_stopped(Venue::Lighter))?;
        let prepared = response
            .await
            .map_err(|_| owner_stopped(Venue::Lighter))??;
        self.inner.live.subscribe(prepared).await
    }

    pub(crate) async fn shutdown_live(&self) -> Result<(), ExchangeError> {
        self.inner.live.shutdown().await
    }

    /// Cancel pending acquisition, reject queued commands, and join every owner.
    /// A dropped requester does not cancel a shared load; service shutdown does.
    pub async fn shutdown(&self) -> Result<(), ExchangeError> {
        let live_result = self.shutdown_live().await;
        for owner in self.inner.owners.values() {
            owner.stop.send_replace(true);
        }
        for owner in self.inner.owners.values() {
            let mut finished = owner.finished.clone();
            // Sender closure also releases waiters if an owner panics.
            let _ = finished.wait_for(|finished| *finished).await;
        }
        let mut joins = Vec::with_capacity(self.inner.owners.len());
        for (venue, owner) in &self.inner.owners {
            if let Some(join) = owner.thread.lock().take() {
                joins.push((*venue, join));
            }
        }
        let owner_result = tokio::task::spawn_blocking(move || {
            let mut error = None;
            for (venue, join) in joins {
                if join.join().is_err() {
                    error = Some(ExchangeError::Internal(format!(
                        "{} CCXT owner panicked",
                        venue.public_id()
                    )));
                }
            }
            error.map_or(Ok(()), Err)
        })
        .await
        .map_err(|error| ExchangeError::Internal(format!("CCXT shutdown failed: {error}")))?;
        live_result.and(owner_result)
    }
}

impl Owner {
    fn start(venue: Venue, config: ProviderConfig) -> Result<Self, ExchangeError> {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (stop, stopping) = watch::channel(false);
        let (finished_tx, finished) = watch::channel(false);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| ExchangeError::Internal(format!("CCXT runtime failed: {error}")))?;
        let thread = thread::Builder::new()
            .name(format!("ccxt-{}-catalog", venue.public_id()))
            .spawn(move || {
                runtime.block_on(run_owner(venue, config, receiver, stopping));
                drop(runtime);
                finished_tx.send_replace(true);
            })
            .map_err(|error| ExchangeError::Internal(format!("CCXT owner failed: {error}")))?;
        Ok(Self {
            commands,
            stop,
            finished,
            thread: Mutex::new(Some(thread)),
        })
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(thread) = self.thread.get_mut().take() {
            // The owner select cancels network waits. Explicit shutdown joins
            // off the application executor; Drop still must not orphan a thread.
            let _ = thread.join();
        }
    }
}

async fn run_owner(
    venue: Venue,
    config: ProviderConfig,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<bool>,
) {
    // REST/catalog owners never call watch methods or touch the process-global
    // WS URL registry. Live owners must own an actual URL across ALL channels;
    // creating another instance/thread at that URL would not isolate transport.
    let mut catalogs: HashMap<CatalogScope, CatalogState> = HashMap::new();
    loop {
        let command = tokio::select! {
            biased;
            _ = stop.changed() => break,
            command = commands.recv() => match command {
                Some(command) => command,
                None => break,
            },
        };
        match command {
            Command::Catalog {
                scope,
                observed_generation,
                reply,
            } => {
                let state = catalogs.entry(scope).or_default();
                if let Some(snapshot) = &state.snapshot {
                    if observed_generation.map_or_else(
                        || state.is_fresh(),
                        |observed| observed < snapshot.generation,
                    ) {
                        let _ = reply.send(Ok(Arc::clone(snapshot)));
                        continue;
                    }
                }
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = load_catalog(venue, scope, &config, state) => result,
                };
                let _ = reply.send(result);
            }
            Command::Snapshot {
                scope,
                request,
                reply,
            } => {
                let state = catalogs.entry(scope).or_default();
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = run_snapshot(venue, scope, &config, state, request) => result,
                };
                let _ = reply.send(result);
            }
            Command::PrepareLive {
                scope,
                channel,
                topic,
                reply,
            } => {
                let state = catalogs.entry(scope).or_default();
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = std::panic::AssertUnwindSafe(async {
                        // Attaching a viewer never triggers a metadata refresh;
                        // catalog/snapshot requests own refresh policy separately.
                        let snapshot = if state.provider.is_none() {
                            load_catalog(venue, scope, &config, state).await?
                        } else {
                            Arc::clone(state.snapshot.as_ref().expect("loaded catalog"))
                        };
                        prepare_live(venue, channel, topic, snapshot, state.provider.as_ref().expect("loaded provider"), &config).await
                    }).catch_unwind() => result.unwrap_or_else(|panic| Err(ExchangeError::UpstreamRequest(panic_message(panic)))),
                };
                let _ = reply.send(result);
            }
            Command::Statistics {
                scope,
                params,
                reply,
            } => {
                let state = catalogs.entry(scope).or_default();
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = run_statistics(venue, scope, &config, state, params) => result,
                };
                let _ = reply.send(result);
            }
            Command::PrepareStatistics { reply } => {
                let state = catalogs.entry(CatalogScope::Default).or_default();
                let result = tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    result = std::panic::AssertUnwindSafe(async {
                        let snapshot = if state.provider.is_none() {
                            load_catalog(venue, CatalogScope::Default, &config, state).await?
                        } else {
                            Arc::clone(state.snapshot.as_ref().expect("loaded catalog"))
                        };
                        let watch = state.watch.get_or_insert_with(|| {
                            Arc::new(CatalogWatch::new(Arc::clone(&snapshot)))
                        }).clone();
                        prepare_lighter_statistics(snapshot, &config, watch).await
                    }).catch_unwind() => result.unwrap_or_else(|panic| Err(ExchangeError::UpstreamRequest(panic_message(panic)))),
                };
                let _ = reply.send(result);
            }
        }
    }
}

async fn run_snapshot(
    venue: Venue,
    scope: CatalogScope,
    config: &ProviderConfig,
    state: &mut CatalogState,
    mut request: SnapshotRequest,
) -> Result<SnapshotResponse, ExchangeError> {
    let result = std::panic::AssertUnwindSafe(async {
        let new_provider = state.provider.is_none();
        let provider = state
            .provider
            .get_or_insert_with(|| Provider::new(venue, scope, config));
        if let Err(error) = request.prepare(venue, scope, provider) {
            if new_provider {
                state.provider = None;
            }
            return Err(error);
        }
        let snapshot = if new_provider || !state.is_fresh() {
            load_catalog(venue, scope, config, state).await?
        } else {
            Arc::clone(state.snapshot.as_ref().expect("loaded catalog"))
        };
        fetch_snapshot(
            venue,
            state.provider.as_mut().expect("loaded provider"),
            &snapshot.catalog,
            request,
        )
        .await
    })
    .catch_unwind()
    .await;
    let result = result.unwrap_or_else(|panic| {
        Err(ExchangeError::UpstreamRequest(format!(
            "{} CCXT snapshot panicked: {}",
            venue.public_id(),
            panic_message(panic)
        )))
    });
    if matches!(
        &result,
        Err(ExchangeError::UpstreamRequest(_)
            | ExchangeError::UpstreamData(_)
            | ExchangeError::Internal(_))
    ) {
        state.provider = None;
    }
    result
}

async fn run_statistics(
    venue: Venue,
    scope: CatalogScope,
    config: &ProviderConfig,
    state: &mut CatalogState,
    params: FetchMarketStatsParams,
) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
    let result = std::panic::AssertUnwindSafe(async {
        let snapshot = if state.provider.is_none() || !state.is_fresh() {
            load_catalog(venue, scope, config, state).await?
        } else {
            Arc::clone(state.snapshot.as_ref().expect("loaded catalog"))
        };
        fetch_statistics(
            venue,
            state.provider.as_mut().expect("loaded provider"),
            &snapshot,
            &params,
        )
        .await
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|panic| {
        Err(ExchangeError::UpstreamRequest(format!(
            "{} CCXT statistics panicked: {}",
            venue.public_id(),
            panic_message(panic)
        )))
    });
    if result.is_err() {
        state.provider = None;
    }
    result
}

async fn load_catalog(
    venue: Venue,
    scope: CatalogScope,
    config: &ProviderConfig,
    state: &mut CatalogState,
) -> Result<Arc<CatalogSnapshot>, ExchangeError> {
    let reload = state.snapshot.is_some();
    let provider = state
        .provider
        .get_or_insert_with(|| Provider::new(venue, scope, config));
    let result = match std::panic::AssertUnwindSafe(provider.load_markets(reload))
        .catch_unwind()
        .await
    {
        Ok(loaded) => loaded
            .map_err(|error| exchange_error(venue, error))
            .and_then(|markets| {
                if markets.is_empty() {
                    return Err(ExchangeError::UpstreamData(format!(
                        "{} returned an empty catalog for {scope:?}",
                        venue.public_id()
                    )));
                }
                Catalog::from_markets(venue, markets, provider.precision_mode()?)
            }),
        Err(panic) => Err(ExchangeError::UpstreamRequest(format!(
            "{} CCXT provider panicked while loading {scope:?}: {}",
            venue.public_id(),
            panic_message(panic)
        ))),
    };
    let catalog = match result {
        Ok(catalog) => catalog,
        Err(error) => {
            // CCXT translates panics into errors; a failed call may leave its
            // dispatch stack/cache partial. Discard that core, not the good DTO.
            state.provider = None;
            return Err(error);
        }
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ExchangeError::Internal(format!("invalid receipt clock: {error}")))?
        .as_millis() as u64;
    let generation = state.snapshot.as_ref().map_or(1, |old| old.generation + 1);
    let snapshot = Arc::new(CatalogSnapshot {
        venue,
        scope,
        catalog,
        timestamp,
        generation,
    });
    state.snapshot = Some(Arc::clone(&snapshot));
    if let Some(watch) = &state.watch {
        watch.publish(Arc::clone(&snapshot));
    }
    state.loaded_at = Some(Instant::now());
    Ok(snapshot)
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn owner_stopped(venue: Venue) -> ExchangeError {
    ExchangeError::Internal(format!(
        "{} CCXT catalog owner is stopped",
        venue.public_id()
    ))
}
