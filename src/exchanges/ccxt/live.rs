//! One current-thread runtime per actual stock URL; no stock Value crosses it.

use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc, LazyLock},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, oneshot, watch, Notify};

use crate::{
    exchanges::traits::ExchangeError,
    models::UnifiedMarketType,
    realtime::{
        DeliveryState, Publication, RealtimeChannel, RealtimeReceiver, RealtimeSubscription,
        RealtimeUpdate, StatisticsRowUpdate, StatisticsUpdate,
    },
};

use super::{
    convert::{convert_book, convert_candles, convert_trades},
    owner::CatalogSnapshot,
    stream::{LiveChannel, LiveProvider, LiveSpec, PreparedTopic},
    venue::Venue,
};

const BROADCAST_CAPACITY: usize = 512;
const MAX_OWNERS: usize = 128;
const MAX_SHARED_FEEDS: usize = 200;
const UNSUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_RECONNECT: Duration = Duration::from_millis(500);
const MAX_RECONNECT: Duration = Duration::from_secs(15);

/// Shared, owned catalog cell for maintained statistics feeds. The coordinator
/// publishes a fresh snapshot after a REST catalog reload; the live feed merges
/// newly listed markets without restarting the shared URL. Owned Ferris rows
/// only — never a stock `Value` or a second book copy.
#[derive(Clone)]
pub(crate) struct CatalogWatch {
    cell: Arc<RwLock<Arc<CatalogSnapshot>>>,
}

impl CatalogWatch {
    pub(crate) fn new(snapshot: Arc<CatalogSnapshot>) -> Self {
        Self {
            cell: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub(crate) fn publish(&self, snapshot: Arc<CatalogSnapshot>) {
        *self.cell.write() = snapshot;
    }

    pub(crate) fn load(&self) -> Arc<CatalogSnapshot> {
        Arc::clone(&self.cell.read())
    }
}

// The stock registry is process-global, including across CcxtService instances.
static URL_OWNERS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Default)]
pub(super) struct LiveHub {
    state: Mutex<HubState>,
}

#[derive(Default)]
struct HubState {
    owners: HashMap<String, Arc<UrlOwner>>,
    stopped: bool,
}

struct UrlOwner {
    control: Arc<Mutex<Control>>,
    changed: Arc<Notify>,
    stop: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Control {
    accepting: bool,
    feeds: HashMap<String, Arc<Feed>>,
}

struct Feed {
    spec: Arc<LiveSpec>,
    sender: broadcast::Sender<Publication>,
    delivery: Arc<DeliveryState>,
}

impl LiveHub {
    pub(super) async fn subscribe(
        &self,
        prepared: PreparedTopic,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        let PreparedTopic {
            spec,
            topic,
            levels,
            client_key,
        } = prepared;
        loop {
            let waiting = {
                let mut hub = self.state.lock();
                if hub.stopped {
                    return Err(stopped());
                }
                hub.owners.retain(|_, owner| !*owner.finished.borrow());
                // A fixed Binance feed retains its allocated slot across viewers.
                for owner in hub.owners.values() {
                    if let Some(receiver) = owner.attach_existing(&spec.key) {
                        return Ok(RealtimeSubscription {
                            key: client_key,
                            topic,
                            levels_limit: levels,
                            receiver,
                        });
                    }
                }
                let url = if let Some(slots) = spec.slots {
                    (0..slots)
                        .map(|slot| format!("{}/{slot}", spec.url))
                        .find(|url| !hub.owners.contains_key(url))
                        .ok_or_else(|| capacity("all stock Binance URL slots are in use"))?
                } else {
                    spec.url.clone()
                };
                if let Some(owner) = hub.owners.get(&url) {
                    if let Some(receiver) = owner.attach(Arc::clone(&spec))? {
                        return Ok(RealtimeSubscription {
                            key: client_key,
                            topic,
                            levels_limit: levels,
                            receiver,
                        });
                    }
                    Some(owner.finished.clone())
                } else {
                    if hub.owners.len() >= MAX_OWNERS {
                        return Err(capacity("live URL owner limit reached"));
                    }
                    let slot = spec
                        .slots
                        .and_then(|_| url.rsplit('/').next()?.parse().ok());
                    let (owner, receiver) = UrlOwner::start(url.clone(), slot, Arc::clone(&spec))?;
                    hub.owners.insert(url, Arc::new(owner));
                    return Ok(RealtimeSubscription {
                        key: client_key,
                        topic,
                        levels_limit: levels,
                        receiver,
                    });
                }
            };
            if let Some(mut finished) = waiting {
                let _ = finished.wait_for(|done| *done).await;
            }
        }
    }

    pub(super) async fn shutdown(&self) -> Result<(), ExchangeError> {
        let owners = {
            let mut hub = self.state.lock();
            hub.stopped = true;
            hub.owners.values().cloned().collect::<Vec<_>>()
        };
        for owner in &owners {
            owner.stop.send_replace(true);
        }
        for owner in &owners {
            let mut finished = owner.finished.clone();
            let _ = finished.wait_for(|done| *done).await;
        }
        self.state.lock().owners.clear();
        let mut result = Ok(());
        for owner in owners {
            let task = owner.task.lock().take();
            if let Some(task) = task {
                if let Err(error) = task.await {
                    result = Err(ExchangeError::Internal(format!(
                        "CCXT live supervisor failed: {error}"
                    )));
                }
            }
        }
        result
    }
}

impl UrlOwner {
    fn start(
        url: String,
        slot: Option<usize>,
        spec: Arc<LiveSpec>,
    ) -> Result<(Self, RealtimeReceiver), ExchangeError> {
        let reservation = UrlReservation::acquire(&url)?;
        let changed = Arc::new(Notify::new());
        let (feed, receiver) = new_feed(Arc::clone(&spec), Arc::clone(&changed));
        let control = Arc::new(Mutex::new(Control {
            accepting: true,
            feeds: HashMap::from([(spec.key.clone(), feed)]),
        }));
        let (stop, stopping) = watch::channel(false);
        let (finished_tx, finished) = watch::channel(false);
        let worker_control = Arc::clone(&control);
        let worker_changed = Arc::clone(&changed);
        let worker_stop = stop.clone();
        let task = tokio::spawn(async move {
            let _completion = OwnerCompletion {
                control: Arc::clone(&worker_control),
                reservation: Some(reservation),
                finished: finished_tx,
            };
            run_url(
                &url,
                slot,
                worker_control,
                worker_changed,
                worker_stop,
                stopping,
            )
            .await;
        });
        Ok((
            Self {
                control,
                changed,
                stop,
                finished,
                task: Mutex::new(Some(task)),
            },
            receiver,
        ))
    }

    fn attach_existing(&self, key: &str) -> Option<RealtimeReceiver> {
        let control = self.control.lock();
        if !control.accepting {
            return None;
        }
        let feed = control.feeds.get(key)?;
        RealtimeReceiver::join(
            feed.sender.subscribe(),
            Arc::clone(&feed.delivery),
            Arc::clone(&self.changed),
        )
    }

    fn attach(&self, spec: Arc<LiveSpec>) -> Result<Option<RealtimeReceiver>, ExchangeError> {
        let mut control = self.control.lock();
        if !control.accepting {
            return Ok(None);
        }
        control
            .feeds
            .retain(|_, feed| feed.delivery.viewers.load(Ordering::Acquire) != 0);
        if control.feeds.len() >= MAX_SHARED_FEEDS {
            return Err(capacity("shared live URL subscription limit reached"));
        }
        if control
            .feeds
            .values()
            .any(|feed| feed.spec.venue != spec.venue || feed.spec.hash == spec.hash)
        {
            return Err(ExchangeError::UnsupportedFeature(
                "stock CCXT cannot maintain these conflicting acquisitions at the same URL".into(),
            ));
        }
        let (feed, receiver) = new_feed(spec, Arc::clone(&self.changed));
        control.feeds.insert(feed.spec.key.clone(), feed);
        self.changed.notify_one();
        Ok(Some(receiver))
    }
}

impl Drop for UrlOwner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

struct OwnerCompletion {
    control: Arc<Mutex<Control>>,
    reservation: Option<UrlReservation>,
    finished: watch::Sender<bool>,
}

impl Drop for OwnerCompletion {
    fn drop(&mut self) {
        let mut control = self.control.lock();
        control.accepting = false;
        control.feeds.clear();
        drop(control);
        self.reservation.take();
        self.finished.send_replace(true);
    }
}

struct SessionThread {
    thread: Option<thread::JoinHandle<()>>,
    stop: watch::Sender<bool>,
}

impl Drop for SessionThread {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.stop.send_replace(true);
            let _ = thread.join();
        }
    }
}

fn new_feed(spec: Arc<LiveSpec>, changed: Arc<Notify>) -> (Arc<Feed>, RealtimeReceiver) {
    let (sender, receiver) = broadcast::channel(BROADCAST_CAPACITY);
    let delivery = Arc::new(DeliveryState::default());
    let receiver = RealtimeReceiver::new(receiver, Arc::clone(&delivery), changed);
    (
        Arc::new(Feed {
            spec,
            sender,
            delivery,
        }),
        receiver,
    )
}

struct UrlReservation(String);

impl UrlReservation {
    fn acquire(url: &str) -> Result<Self, ExchangeError> {
        if !URL_OWNERS.lock().insert(url.to_string()) {
            return Err(ExchangeError::Internal(
                "another CCXT service already owns this stock URL".into(),
            ));
        }
        Ok(Self(url.to_string()))
    }
}

impl Drop for UrlReservation {
    fn drop(&mut self) {
        ccxt_pro::pro::ws_client::drop_client(&self.0);
        URL_OWNERS.lock().remove(&self.0);
    }
}

struct ActiveFeed {
    feed: Arc<Feed>,
    epoch: u64,
    ready: bool,
    retained: Option<ccxt::Value>,
    /// Statistics only: last delivered stock ticker cache value per symbol. The
    /// held `Arc` keeps pointer identity meaningful (no ABA) and lets unchanged
    /// rows keep their prior receipts.
    ticker_marks: HashMap<String, ccxt::Value>,
    /// Statistics only: catalog generation and the native-id index it was built
    /// with, so newly listed markets resolve without restarting the URL.
    catalog: Option<Arc<CatalogSnapshot>>,
    market_index: HashMap<String, usize>,
}

impl ActiveFeed {
    fn new(feed: Arc<Feed>) -> Self {
        let epoch = feed.delivery.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        Self {
            feed,
            epoch,
            ready: false,
            retained: None,
            ticker_marks: HashMap::new(),
            catalog: None,
            market_index: HashMap::new(),
        }
    }

    fn publish(&self, update: RealtimeUpdate) {
        let _ = self.feed.sender.send(Publication {
            epoch: self.epoch,
            update,
        });
    }

    fn invalidate(&mut self) {
        self.ready = false;
        self.epoch = self.feed.delivery.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        // A new epoch/reconnect means the stock cache is cleared and rebuilt:
        // stale pointer marks would otherwise suppress the fresh baseline.
        self.ticker_marks.clear();
        self.catalog = None;
        self.market_index.clear();
    }
}

struct RetiringFeed {
    spec: Arc<LiveSpec>,
    deadline: tokio::time::Instant,
}

enum SessionExit {
    Stop,
    Reconnect(ExchangeError),
}

fn desired_feeds(control: &Mutex<Control>) -> Vec<Arc<Feed>> {
    let mut control = control.lock();
    control
        .feeds
        .retain(|_, feed| feed.delivery.viewers.load(Ordering::Acquire) != 0);
    if control.feeds.is_empty() {
        control.accepting = false;
    }
    control.feeds.values().cloned().collect()
}

async fn run_url(
    url: &str,
    slot: Option<usize>,
    control: Arc<Mutex<Control>>,
    changed: Arc<Notify>,
    stopping: watch::Sender<bool>,
    mut stop: watch::Receiver<bool>,
) {
    let mut delay = INITIAL_RECONNECT;
    loop {
        let desired = desired_feeds(&control);
        let Some(first) = desired.first() else {
            break;
        };
        if *stop.borrow() {
            break;
        }
        let worker_url = url.to_string();
        let worker_control = Arc::clone(&control);
        let worker_changed = Arc::clone(&changed);
        let mut worker_stop = stop.clone();
        let (reply, response) = oneshot::channel();
        let started = std::time::Instant::now();
        // Every reconnect gets a new OS thread as well as a new runtime: the
        // stock deferred coroutine queue is thread-local and not cancellable.
        let thread = thread::Builder::new()
            .name(format!("ccxt-{}-live", first.spec.venue.public_id()))
            // Stock Binance's dev-profile dynamic dispatcher uses ~1.2 MiB
            // per nested poll; registration/seed dispatch exceeds 2 MiB.
            .stack_size(if first.spec.venue == Venue::Binance {
                8 * 1024 * 1024
            } else {
                2 * 1024 * 1024
            })
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => {
                            let exit = runtime.block_on(run_session(
                                &worker_url,
                                slot,
                                &worker_control,
                                &worker_changed,
                                &mut worker_stop,
                            ));
                            // Drop stock reader/writer/keepalive tasks before a
                            // replacement can use the same registry URL.
                            drop(runtime);
                            exit
                        }
                        Err(error) => {
                            SessionExit::Reconnect(ExchangeError::Internal(error.to_string()))
                        }
                    }
                }));
                ccxt_pro::pro::ws_client::drop_client(&worker_url);
                let exit = result.unwrap_or_else(|panic| {
                    SessionExit::Reconnect(super::stream::panic_error(panic))
                });
                let _ = reply.send(exit);
            });
        let thread = match thread {
            Ok(thread) => thread,
            Err(error) => {
                for feed in desired {
                    publish_error(&feed, error.to_string());
                }
                break;
            }
        };
        let mut session = SessionThread {
            thread: Some(thread),
            stop: stopping.clone(),
        };
        let exit = response.await.unwrap_or(SessionExit::Stop);
        // The reply is the thread's final action. Joining also runs its TLS
        // destructors; cancellation of this supervisor joins via SessionThread.
        if let Some(thread) = session.thread.take() {
            let _ = thread.join();
        }
        match exit {
            SessionExit::Stop => break,
            SessionExit::Reconnect(error) => {
                for feed in desired_feeds(&control) {
                    publish_error(&feed, error.to_string());
                }
                if started.elapsed() > Duration::from_secs(30) {
                    delay = INITIAL_RECONNECT;
                }
                tokio::select! {
                    _ = stop.changed() => {},
                    _ = changed.notified() => {},
                    _ = tokio::time::sleep(delay) => {},
                }
                delay = (delay * 2).min(MAX_RECONNECT);
            }
        }
    }
}

fn publish_error(feed: &Feed, message: String) {
    let epoch = feed.delivery.epoch.fetch_add(1, Ordering::AcqRel) + 1;
    let _ = feed.sender.send(Publication {
        epoch,
        update: RealtimeUpdate::Error(message.into()),
    });
}

async fn run_session(
    url: &str,
    slot: Option<usize>,
    control: &Mutex<Control>,
    changed: &Notify,
    stop: &mut watch::Receiver<bool>,
) -> SessionExit {
    let desired = desired_feeds(control);
    let Some(first) = desired.first() else {
        return SessionExit::Stop;
    };
    let mut provider = LiveProvider::new(&first.spec, slot);
    let mut active: HashMap<String, ActiveFeed> = HashMap::new();
    let mut retiring: HashMap<String, RetiringFeed> = HashMap::new();
    let mut cache_routes: HashMap<i64, String> = HashMap::new();
    let mut hashes = Vec::new();
    let mut reconcile = true;
    let exit = loop {
        if *stop.borrow() {
            break SessionExit::Stop;
        }
        if reconcile {
            let desired = desired_feeds(control);
            if desired.is_empty() {
                break SessionExit::Stop;
            }
            let desired_keys: HashSet<_> =
                desired.iter().map(|feed| feed.spec.key.as_str()).collect();
            let removed: Vec<_> = active
                .iter()
                .filter(|(key, current)| {
                    !desired_keys.contains(key.as_str())
                        || desired
                            .iter()
                            .any(|feed| feed.spec.key == **key && !Arc::ptr_eq(feed, &current.feed))
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in removed {
                let mut old = active.remove(&key).expect("active key");
                old.invalidate();
                provider.clear_retained(old.retained.take());
                provider.clear_feed(&old.feed.spec);
                provider.enqueue(&old.feed.spec, true);
                retiring.insert(
                    key,
                    RetiringFeed {
                        spec: Arc::clone(&old.feed.spec),
                        deadline: tokio::time::Instant::now() + UNSUBSCRIBE_TIMEOUT,
                    },
                );
            }
            for feed in desired {
                if let Some(current) = active.get_mut(&feed.spec.key) {
                    current.epoch = feed.delivery.epoch.load(Ordering::Acquire);
                    continue;
                }
                if let Some(retired) = retiring.remove(&feed.spec.key) {
                    provider.expire_unsubscribe(url, &retired.spec);
                }
                provider.add_market(&feed.spec);
                provider.enqueue(&feed.spec, false);
                active.insert(feed.spec.key.clone(), ActiveFeed::new(feed));
            }
            cache_routes.retain(|_, key| active.contains_key(key));
            rebuild_hashes(&active, &retiring, &mut hashes);
            reconcile = false;
        }
        let deadline = retiring.values().map(|feed| feed.deadline).min();
        let result = tokio::select! {
            biased;
            _ = stop.changed() => break SessionExit::Stop,
            _ = changed.notified() => { reconcile = true; continue; },
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let expired: Vec<_> = retiring.iter().filter(|(_, feed)| feed.deadline <= tokio::time::Instant::now()).map(|(key, _)| key.clone()).collect();
                for key in expired {
                    let retired = retiring.remove(&key).expect("expired retirement");
                    provider.expire_unsubscribe(url, &retired.spec);
                    provider.clear_feed(&retired.spec);
                }
                rebuild_hashes(&active, &retiring, &mut hashes);
                continue;
            },
            result = provider.next(url, &hashes) => result,
        };
        let result = match result {
            Ok(result) => result,
            Err(ExchangeError::UpstreamRequest(message))
                if message.contains("[UnsubscribeError]") =>
            {
                ccxt::Value::Bool(true)
            }
            Err(error) => break SessionExit::Reconnect(error),
        };
        if result.as_bool() == Some(true) {
            // Some stock venues key acknowledgements only by symbol. A late ack
            // can remove a re-added feed: observe stock ownership and resubscribe,
            // invalidating its epoch without stopping another maintained hash.
            if let Some(client) = ccxt_pro::pro::ws_client::get_client(url) {
                for current in active.values_mut() {
                    if !client.is_subscribed(&current.feed.spec.hash) {
                        current.invalidate();
                        provider.clear_retained(current.retained.take());
                        provider.clear_feed(&current.feed.spec);
                        provider.expire_unsubscribe(url, &current.feed.spec);
                        provider.enqueue(&current.feed.spec, false);
                    }
                }
                let completed: Vec<_> = retiring
                    .iter()
                    .filter(|(_, retired)| {
                        !client.is_subscribed(&retired.spec.hash)
                            && !client.is_subscribed(&retired.spec.unsubscribe_hash())
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in completed {
                    let retired = retiring.remove(&key).expect("completed retirement");
                    provider.expire_unsubscribe(url, &retired.spec);
                    provider.clear_feed(&retired.spec);
                }
                rebuild_hashes(&active, &retiring, &mut hashes);
            }
            continue;
        }
        let raw = result
            .as_array()
            .filter(|rows| {
                rows.len() == 3 && rows[0].as_str().is_some() && rows[1].as_str().is_some()
            })
            .map(|rows| rows[2].clone())
            .unwrap_or(result);
        let cache_id = ccxt::value::get_value_k(&raw, "__cache_id").as_i64();
        let statistics_key = active
            .iter()
            .find(|(_, current)| {
                current.feed.spec.channel == LiveChannel::Statistics && lighter_ticker_frame(&raw)
            })
            .map(|(key, _)| key.clone());
        let key = if let Some(id) = cache_id {
            if let Some(key) = cache_routes.get(&id) {
                Some(key.clone())
            } else {
                let key = active
                    .iter()
                    .find(|(_, current)| {
                        ccxt::value::get_value_k(&provider.cache(&current.feed.spec), "__cache_id")
                            .as_i64()
                            == Some(id)
                    })
                    .map(|(key, _)| key.clone());
                if let Some(key) = &key {
                    cache_routes.insert(id, key.clone());
                }
                key
            }
        } else if statistics_key.is_some() {
            statistics_key
        } else {
            let symbol = ccxt::value::get_value_k(&raw, "symbol");
            active
                .iter()
                .find(|(_, current)| {
                    current.feed.spec.channel == LiveChannel::Client(RealtimeChannel::OrderBook)
                        && symbol.as_str() == Some(current.feed.spec.symbol.as_str())
                })
                .map(|(key, _)| key.clone())
        };
        let Some(current) = key.as_ref().and_then(|key| active.get_mut(key)) else {
            // Data already settled for a retired hash must not acquire a new
            // viewer's generation. Clear its stock payload while control finishes.
            for retired in retiring.values() {
                provider.clear_feed(&retired.spec);
            }
            continue;
        };
        let spec = Arc::clone(&current.feed.spec);
        if spec.channel != LiveChannel::Statistics {
            current.retained = Some(raw.clone());
        }
        let update = match spec.channel {
            LiveChannel::Client(RealtimeChannel::OrderBook) => {
                convert_book(raw, &spec.symbol).map(|book| {
                    // Binance also resolves its REST seed, before a WS bridge. This
                    // observable gate excludes that seed; it does NOT claim to fix
                    // the user's accepted upstream post-bridge continuity defect.
                    if spec.venue == Venue::Binance && book.timestamp.is_none() {
                        return None;
                    }
                    Some(RealtimeUpdate::OrderBook(Arc::new(book)))
                })
            }
            LiveChannel::Client(RealtimeChannel::Trades) => provider
                .incremental(&spec, raw)
                .and_then(|rows| convert_trades(rows, &spec.symbol))
                .map(|rows| (!rows.is_empty()).then(|| RealtimeUpdate::Trades(Arc::new(rows)))),
            LiveChannel::Client(RealtimeChannel::Ohlcv) => provider
                .incremental(&spec, raw)
                .and_then(convert_candles)
                .map(|rows| (!rows.is_empty()).then(|| RealtimeUpdate::Ohlcv(Arc::new(rows)))),
            LiveChannel::Statistics => {
                refresh_statistics_catalog(&mut provider, current, &spec);
                statistics_update(&provider, current, &spec).map(|rows| {
                    (!rows.is_empty())
                        .then(|| RealtimeUpdate::Statistics(Arc::new(StatisticsUpdate { rows })))
                })
            }
        };
        match update {
            Ok(Some(update)) => {
                current.ready = true;
                current.publish(update);
            }
            Ok(None) => {}
            Err(error) => break SessionExit::Reconnect(error),
        }
    };
    for current in active.values_mut() {
        current.invalidate();
        provider.clear_retained(current.retained.take());
        provider.clear_feed(&current.feed.spec);
    }
    for retired in retiring.values() {
        provider.clear_feed(&retired.spec);
    }
    exit
}

/// True when a settled frame is a Lighter `market_stats` ticker rather than a
/// book/trade payload sharing the same URL. Books carry `bids`/`asks`; trades
/// carry trade keys, not market-statistics keys.
fn lighter_ticker_frame(raw: &ccxt::Value) -> bool {
    let Some(fields) = raw.as_map() else {
        return false;
    };
    if fields.contains_key("bids") || fields.contains_key("asks") {
        return false;
    }
    let info = ccxt::value::get_value_k(raw, "info");
    let Some(info) = info.as_map() else {
        return false;
    };
    info.contains_key("market_id")
        && [
            "mark_price",
            "index_price",
            "open_interest",
            "last_trade_price",
            "current_funding_rate",
            "daily_base_token_volume",
        ]
        .iter()
        .any(|key| info.contains_key(*key))
}

fn scalar_key(value: &ccxt::Value) -> Option<String> {
    match value {
        ccxt::Value::Str(value) => Some(value.to_string()),
        ccxt::Value::Int(value) => Some(value.to_string()),
        ccxt::Value::Float(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Cache identity: unchanged symbols keep the same inner `Arc`, so a pointer
/// comparison (holding both allocations, no ABA, no unsafe) proves the row did
/// not change and its prior receipts stay untouched.
fn same_cache(previous: &ccxt::Value, current: &ccxt::Value) -> bool {
    match (previous, current) {
        (ccxt::Value::Dict(previous), ccxt::Value::Dict(current)) => Arc::ptr_eq(previous, current),
        _ => previous == current,
    }
}

/// Merge catalog rows the live core has not seen yet and rebuild the native-id
/// index when the shared cell's generation advances.
fn refresh_statistics_catalog(
    provider: &mut LiveProvider,
    current: &mut ActiveFeed,
    spec: &LiveSpec,
) {
    let Some(watch) = &spec.catalog else {
        return;
    };
    let snapshot = watch.load();
    if current.catalog.as_ref().map(|old| old.generation) == Some(snapshot.generation) {
        return;
    }
    provider.add_catalog(snapshot.catalog.entries());
    let mut index = HashMap::new();
    for (position, entry) in snapshot.catalog.entries().iter().enumerate() {
        if let Some(identity) = &entry.market.identity {
            index.insert(identity.exchange_market_id.clone(), position);
        }
    }
    current.market_index = index;
    current.catalog = Some(snapshot);
}

/// Changed rows in the stock aggregate cache. A ticker whose held cache value is
/// pointer-identical to the last delivered one is unchanged: it keeps its prior
/// receipts and is not returned, however many frames arrive. A finite frame that
/// changed several rows returns all of them, so no later frame is required.
fn changed_tickers<'a>(
    marks: &HashMap<String, ccxt::Value>,
    tickers: &'a ccxt::Value,
) -> Vec<(String, &'a ccxt::Value)> {
    let Some(tickers) = tickers.as_map() else {
        return Vec::new();
    };
    tickers
        .iter()
        .filter(|(_, ticker)| lighter_ticker_frame(ticker))
        .filter(|(symbol, ticker)| {
            !marks
                .get(*symbol)
                .is_some_and(|previous| same_cache(previous, ticker))
        })
        .map(|(symbol, ticker)| (symbol.clone(), ticker))
        .collect()
}

/// Every changed ticker in the stock aggregate cache, as sparse field patches.
/// A finite multi-row frame is delivered in full even when no later frame
/// arrives; unchanged rows are skipped by cache identity.
fn statistics_update(
    provider: &LiveProvider,
    current: &mut ActiveFeed,
    spec: &LiveSpec,
) -> Result<Vec<StatisticsRowUpdate>, ExchangeError> {
    let Some(catalog) = current.catalog.clone() else {
        return Ok(Vec::new());
    };
    let entries = catalog.catalog.entries();
    let received_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ExchangeError::Internal(format!("invalid receipt clock: {error}")))?
        .as_millis() as u64;
    let received_at = tokio::time::Instant::now();
    let tickers = provider.cache(spec);
    let mut rows = Vec::new();
    for (symbol, ticker) in changed_tickers(&current.ticker_marks, &tickers) {
        let info = ccxt::value::get_value_k(ticker, "info");
        let Some(market_id) = scalar_key(&ccxt::value::get_value_k(&info, "market_id")) else {
            continue;
        };
        let Some(position) = current.market_index.get(&market_id) else {
            continue;
        };
        let entry = &entries[*position];
        // `market_stats/all` is swap-only; a spot market id can never be a
        // statistics row even though the catalog holds both products.
        if entry.market.market_type != UnifiedMarketType::Perp {
            continue;
        }
        let fields =
            match super::statistics::lighter_ticker_patch(ticker, &entry.raw, received_timestamp) {
                Ok(fields) => fields,
                // A single unreadable row is an absent observation, not a reason to
                // tear down the shared URL and every book/trade feed on it.
                Err(_) => continue,
            };
        current.ticker_marks.insert(symbol.clone(), ticker.clone());
        if fields.is_empty() {
            continue;
        }
        let market_id = entry
            .market
            .identity
            .as_ref()
            .expect("catalog rows carry identity")
            .market_id
            .clone();
        rows.push(StatisticsRowUpdate {
            market_id,
            fields,
            received_at,
        });
    }
    Ok(rows)
}

fn rebuild_hashes(
    active: &HashMap<String, ActiveFeed>,
    retiring: &HashMap<String, RetiringFeed>,
    hashes: &mut Vec<String>,
) {
    hashes.clear();
    for current in active.values() {
        hashes.push(current.feed.spec.hash.clone());
        hashes.push(current.feed.spec.unsubscribe_hash());
    }
    for retired in retiring.values() {
        hashes.push(retired.spec.hash.clone());
        hashes.push(retired.spec.unsubscribe_hash());
    }
    hashes.sort_unstable();
    hashes.dedup();
}

fn capacity(message: &str) -> ExchangeError {
    ExchangeError::UpstreamRequest(message.into())
}

fn stopped() -> ExchangeError {
    ExchangeError::Internal("CCXT live service is stopped".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn value(input: serde_json::Value) -> ccxt::Value {
        ccxt::Value::from_json(&input)
    }

    #[test]
    fn statistics_frame_is_distinguished_from_book_and_trade() {
        let ticker = value(json!({
            "symbol": "ETH/USDC:USDC",
            "info": {
                "market_id": 0,
                "mark_price": "3013.91",
                "index_price": "3015.56",
                "last_trade_price": "3013.13",
                "current_funding_rate": "0.0012",
                "funding_rate": "0.0012",
                "daily_base_token_volume": 643235.2763,
            }
        }));
        let book = value(json!({
            "symbol": "ETH/USDC:USDC",
            "bids": [[3013.0, 1.0]],
            "asks": [[3014.0, 1.0]],
        }));
        let trade = value(json!({
            "symbol": "ETH/USDC:USDC",
            "info": {
                "market_id": 0,
                "trade_id": 526801155,
                "size": "0.0346",
                "price": "3028.85",
            }
        }));
        let ack = ccxt::Value::Bool(true);
        let book_with_info = value(json!({
            "symbol": "ETH/USDC:USDC",
            "bids": [[3013.0, 1.0]],
            "asks": [[3014.0, 1.0]],
            "info": {"market_id": 0, "mark_price": "3013.91"},
        }));
        assert!(lighter_ticker_frame(&ticker));
        assert!(!lighter_ticker_frame(&book));
        assert!(!lighter_ticker_frame(&book_with_info));
        assert!(!lighter_ticker_frame(&trade));
        assert!(!lighter_ticker_frame(&ack));
    }

    #[test]
    fn unchanged_rows_keep_their_receipts_across_repeated_frames() {
        let first = value(json!({
            "symbol": "ETH/USDC:USDC",
            "info": {"market_id": 0, "mark_price": "3013.91"},
        }));
        let second = value(json!({
            "symbol": "SOL/USDC:USDC",
            "info": {"market_id": 1, "mark_price": "1.15954"},
        }));
        let book = value(json!({"symbol": "ETH/USDC:USDC", "bids": [], "asks": []}));
        let mut tickers = value(json!({}));
        ccxt::set_value(
            &mut tickers,
            &ccxt::Value::from("ETH/USDC:USDC"),
            first.clone(),
        );
        ccxt::set_value(
            &mut tickers,
            &ccxt::Value::from("SOL/USDC:USDC"),
            second.clone(),
        );
        ccxt::set_value(&mut tickers, &ccxt::Value::from("BTC/USDC:USDC"), book);

        // A finite multi-row frame delivers every changed row at once.
        let changed = changed_tickers(&HashMap::<String, ccxt::Value>::new(), &tickers);
        assert_eq!(changed.len(), 2);

        // The held cache values make unchanged rows pointer-identical, so a
        // later identical frame has nothing new to publish.
        let marks: HashMap<String, ccxt::Value> = changed
            .iter()
            .map(|(symbol, ticker)| (symbol.clone(), (*ticker).clone()))
            .collect();
        assert!(changed_tickers(&marks, &tickers).is_empty());

        // Replacing one symbol's cache value (stock re-parse) re-delivers only it.
        let mut replaced = tickers.clone();
        ccxt::set_value(
            &mut replaced,
            &ccxt::Value::from("ETH/USDC:USDC"),
            value(json!({
                "symbol": "ETH/USDC:USDC",
                "info": {"market_id": 0, "mark_price": "3014.00"},
            })),
        );
        let changed = changed_tickers(&marks, &replaced);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, "ETH/USDC:USDC");
    }

    #[test]
    fn scalar_keys_cover_numeric_and_string_market_ids() {
        assert_eq!(scalar_key(&ccxt::Value::Int(7)), Some("7".to_string()));
        assert_eq!(scalar_key(&ccxt::Value::from("7")), Some("7".to_string()));
        assert_eq!(scalar_key(&ccxt::Value::Null), None);
        assert_eq!(scalar_key(&ccxt::Value::Bool(true)), None);
    }
}
