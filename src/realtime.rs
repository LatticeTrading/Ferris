//! Ferris live demand and owned delivery. Stock Pro acquisition lives in CCXT owners.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::{broadcast, Notify};

use crate::{
    exchanges::{
        ccxt::{CcxtService, PreparedTopic},
        traits::ExchangeError,
    },
    models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade, MarketStatsField, MarketStatsFieldName},
    orderbook::BookAmountMode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RealtimeChannel {
    Trades,
    OrderBook,
    Ohlcv,
}

impl RealtimeChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::OrderBook => "orderbook",
            Self::Ohlcv => "ohlcv",
        }
    }

    pub fn from_client_value(value: &str) -> Option<Self> {
        match value {
            "trades" => Some(Self::Trades),
            "orderbook" | "order_book" => Some(Self::OrderBook),
            "ohlcv" | "candles" | "kline" | "klines" => Some(Self::Ohlcv),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeTopic {
    pub exchange: String,
    pub symbol: String,
    pub params: Value,
}

impl RealtimeTopic {
    pub fn from_client_request(
        exchange: Option<String>,
        symbol: Option<String>,
        params: Value,
    ) -> Result<Self, String> {
        let exchange = exchange.unwrap_or_else(|| "hyperliquid".to_string());
        let exchange = exchange.trim().to_ascii_lowercase();
        if exchange.is_empty() {
            return Err("`exchange` cannot be empty".to_string());
        }
        let symbol = symbol.unwrap_or_default().trim().to_string();
        if symbol.is_empty() {
            return Err("`symbol` cannot be empty".to_string());
        }
        let params = if params.is_null() {
            Value::Object(Map::new())
        } else if params.is_object() {
            params
        } else {
            return Err("`params` must be an object or null".to_string());
        };
        // serde_json's default Map is ordered, including nested objects.
        Ok(Self {
            exchange,
            symbol,
            params,
        })
    }
}

pub(crate) mod native;

/// Validated, owned preparation. Dropping a plan owns no upstream demand.
#[async_trait]
pub trait NativeSubscriptionPlan: Send {
    fn client_key(&self) -> &str;
    async fn subscribe(self: Box<Self>) -> Result<RealtimeSubscription, ExchangeError>;
}

/// Native realtime ownership boundary.  Implementations own protocol parsing,
/// connection packing, reconnection, and feed state; the service only routes
/// prepared plans and exposes the common lease/delivery contract.
#[async_trait]
pub trait NativeRealtimeOwner: Send + Sync {
    fn exchange_id(&self) -> &'static str;

    async fn prepare_native(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    ) -> Result<Box<dyn NativeSubscriptionPlan>, ExchangeError>;

    async fn shutdown_native(&self) -> Result<(), ExchangeError>;
}

pub(crate) enum PreparedRealtimeTopic {
    Ccxt(PreparedTopic),
    Native(Box<dyn NativeSubscriptionPlan>),
}

impl PreparedRealtimeTopic {
    pub(crate) fn client_key(&self) -> &str {
        match self {
            Self::Ccxt(prepared) => &prepared.client_key,
            Self::Native(plan) => plan.client_key(),
        }
    }
}

#[derive(Clone)]
pub struct RealtimeService {
    service: CcxtService,
    native: std::collections::HashMap<String, Arc<dyn NativeRealtimeOwner>>,
}

impl RealtimeService {
    pub fn new(service: CcxtService) -> Self {
        Self {
            service,
            native: std::collections::HashMap::new(),
        }
    }

    pub fn with_native(mut self, owner: Arc<dyn NativeRealtimeOwner>) -> Self {
        self.native.insert(owner.exchange_id().to_string(), owner);
        self
    }

    pub async fn subscribe(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        let prepared = self.prepare(channel, topic).await?;
        self.subscribe_prepared(prepared).await
    }

    pub(crate) async fn prepare(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    ) -> Result<PreparedRealtimeTopic, ExchangeError> {
        if let Some(owner) = self.native.get(&topic.exchange) {
            return owner
                .prepare_native(channel, topic)
                .await
                .map(PreparedRealtimeTopic::Native);
        }
        self.service
            .prepare_live(channel, topic)
            .await
            .map(PreparedRealtimeTopic::Ccxt)
    }

    pub(crate) async fn subscribe_prepared(
        &self,
        prepared: PreparedRealtimeTopic,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        match prepared {
            PreparedRealtimeTopic::Ccxt(prepared) => self.service.subscribe_live(prepared).await,
            PreparedRealtimeTopic::Native(plan) => plan.subscribe().await,
        }
    }

    pub async fn shutdown(&self) -> Result<(), ExchangeError> {
        let owners = self.native.values().cloned().collect::<Vec<_>>();
        let mut result = Ok(());
        for owner in owners {
            if let Err(error) = owner.shutdown_native().await {
                result = Err(error);
            }
        }
        result.and(self.service.shutdown_live().await)
    }
}

#[derive(Clone)]
pub enum RealtimeUpdate {
    Trades(Arc<Vec<CcxtTrade>>),
    OrderBook(Arc<CcxtOrderBook>),
    Ohlcv(Arc<Vec<CcxtOhlcv>>),
    /// Sparse per-row field patches from a shared statistics feed. Only fields
    /// the source actually observed this frame are present; absent fields keep
    /// their prior state and receipt. Identity is the canonical Ferris market id.
    Statistics(Arc<StatisticsUpdate>),
    Error(Arc<str>),
}

/// One source frame's worth of changed statistics rows.
#[derive(Debug, Clone)]
pub struct StatisticsUpdate {
    pub rows: Vec<StatisticsRowUpdate>,
}

/// One changed statistics row, keyed by canonical Ferris market identity.
#[derive(Debug, Clone)]
pub struct StatisticsRowUpdate {
    pub market_id: String,
    pub fields: BTreeMap<MarketStatsFieldName, MarketStatsField>,
    pub received_at: tokio::time::Instant,
}

pub struct RealtimeSubscription {
    pub key: String,
    pub topic: RealtimeTopic,
    pub levels_limit: usize,
    pub receiver: RealtimeReceiver,
}

/// Only owned publications cross runtimes. The receiver itself is the demand
/// lease, including when a subscribe request or a WS forwarder is cancelled.
pub struct RealtimeReceiver {
    receiver: broadcast::Receiver<Publication>,
    state: Arc<DeliveryState>,
    released: Arc<Notify>,
    initial: Option<Publication>,
    book_amount_mode: BookAmountMode,
    /// Display depth registered as demand on the shared feed; 0 when none.
    book_depth: usize,
}

impl RealtimeReceiver {
    pub(crate) fn new(
        receiver: broadcast::Receiver<Publication>,
        state: Arc<DeliveryState>,
        released: Arc<Notify>,
    ) -> Self {
        state.viewers.fetch_add(1, Ordering::AcqRel);
        Self {
            receiver,
            state,
            released,
            initial: None,
            book_amount_mode: BookAmountMode::Native,
            book_depth: 0,
        }
    }

    pub(crate) fn join(
        receiver: broadcast::Receiver<Publication>,
        state: Arc<DeliveryState>,
        released: Arc<Notify>,
    ) -> Option<Self> {
        // Demand must not resurrect a generation after its last viewer left.
        state
            .viewers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |viewers| {
                (viewers != 0).then_some(viewers + 1)
            })
            .ok()?;
        Some(Self {
            receiver,
            state,
            released,
            initial: None,
            book_amount_mode: BookAmountMode::Native,
            book_depth: 0,
        })
    }

    pub(crate) fn with_book_amount_mode(mut self, mode: BookAmountMode) -> Self {
        self.book_amount_mode = mode;
        self
    }

    pub(crate) fn with_initial(mut self, initial: Option<Publication>) -> Self {
        self.initial = initial;
        self
    }

    /// Register this viewer's display depth as demand on the shared feed. The
    /// owner then converts only as deep as the deepest current viewer, and this
    /// receiver never observes a book truncated below its own depth.
    pub(crate) fn with_book_depth(mut self, levels: usize) -> Self {
        if levels != 0 && self.book_depth == 0 {
            self.state.depth.register(levels);
            self.book_depth = levels;
        }
        self
    }

    pub(crate) fn book_amount_mode(&self) -> BookAmountMode {
        self.book_amount_mode
    }

    /// Whether another receiver currently shares this feed, i.e. whether an
    /// encoded payload can be reused by anyone else.
    pub(crate) fn is_shared(&self) -> bool {
        self.state.viewers.load(Ordering::Relaxed) > 1
    }

    pub async fn recv(&mut self) -> Result<RealtimeUpdate, broadcast::error::RecvError> {
        self.recv_shared().await.map(|(update, _)| update)
    }

    /// Like [`Self::recv`], plus the publication's shared encoding cache so
    /// that every viewer of one publication can reuse a single serialization.
    pub(crate) async fn recv_shared(
        &mut self,
    ) -> Result<(RealtimeUpdate, Arc<EncodedPayloads>), broadcast::error::RecvError> {
        loop {
            let publication = match self.initial.take() {
                Some(publication) => publication,
                None => self.receiver.recv().await?,
            };
            // Continuity errors concern this receiver's existing lease even
            // when a replacement session has already advanced the data epoch.
            // Only stale market data is suppressible; otherwise a slow reader
            // can silently miss the reconnect boundary before fresh updates.
            if matches!(&publication.update, RealtimeUpdate::Error(_))
                || publication.epoch == self.state.epoch.load(Ordering::Acquire)
            {
                let update = match publication.update {
                    RealtimeUpdate::OrderBook(book) => {
                        // Converted before this viewer's deeper demand was
                        // visible to the owner: wait for the next publication
                        // rather than deliver a too-shallow book.
                        if publication.book_depth < self.book_depth {
                            continue;
                        }
                        match book.amount_view(self.book_amount_mode) {
                            Ok(view) => RealtimeUpdate::OrderBook(view),
                            Err(error) => RealtimeUpdate::Error(error.to_string().into()),
                        }
                    }
                    other => other,
                };
                return Ok((update, publication.encoded));
            }
        }
    }
}

impl Drop for RealtimeReceiver {
    fn drop(&mut self) {
        if self.book_depth != 0 {
            self.state.depth.release(self.book_depth);
        }
        if self.state.viewers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.epoch.fetch_add(1, Ordering::AcqRel);
            self.released.notify_one();
        }
    }
}

#[derive(Default)]
pub(crate) struct DeliveryState {
    pub viewers: AtomicUsize,
    pub epoch: AtomicU64,
    pub(crate) depth: DepthDemand,
}

/// Display depths requested by a feed's current book viewers. The acquisition
/// owner reads only the cached maximum, once per update.
#[derive(Default)]
pub(crate) struct DepthDemand {
    levels: parking_lot::Mutex<BTreeMap<usize, usize>>,
    deepest: AtomicUsize,
}

impl DepthDemand {
    fn register(&self, levels: usize) {
        let mut all = self.levels.lock();
        *all.entry(levels).or_insert(0) += 1;
        self.deepest.store(
            all.keys().next_back().copied().unwrap_or(0),
            Ordering::Release,
        );
    }

    fn release(&self, levels: usize) {
        let mut all = self.levels.lock();
        if let Some(viewers) = all.get_mut(&levels) {
            *viewers -= 1;
            if *viewers == 0 {
                all.remove(&levels);
            }
        }
        self.deepest.store(
            all.keys().next_back().copied().unwrap_or(0),
            Ordering::Release,
        );
    }

    /// Depth the owner must publish to satisfy every current viewer. Without
    /// registered demand the whole book is required.
    pub(crate) fn required(&self) -> usize {
        match self.deepest.load(Ordering::Acquire) {
            0 => usize::MAX,
            levels => levels,
        }
    }
}

/// Maximum distinct payload variants (display depth x amount unit) cached per
/// publication. Further variants are encoded per viewer, as before.
const ENCODED_VARIANTS: usize = 8;

type EncodedSlot = Arc<OnceLock<Option<Arc<str>>>>;

/// Serialized `data` payloads of one publication, shared by all its viewers so
/// fan-out costs one serialization per distinct view instead of one per client.
#[derive(Default)]
pub(crate) struct EncodedPayloads {
    slots: parking_lot::Mutex<Vec<((usize, BookAmountMode), EncodedSlot)>>,
}

impl EncodedPayloads {
    /// The cached payload for `key`, encoding it at most once. `None` when the
    /// encoder fails. Concurrent viewers of the same variant wait for the
    /// first encoder; different variants encode in parallel.
    pub(crate) fn get_or_encode(
        &self,
        key: (usize, BookAmountMode),
        encode: impl FnOnce() -> Option<String>,
    ) -> Option<Arc<str>> {
        let slot = {
            let mut slots = self.slots.lock();
            match slots.iter().find(|(existing, _)| *existing == key) {
                Some((_, slot)) => Some(Arc::clone(slot)),
                None if slots.len() < ENCODED_VARIANTS => {
                    let slot = EncodedSlot::default();
                    slots.push((key, Arc::clone(&slot)));
                    Some(slot)
                }
                None => None,
            }
        };
        match slot {
            Some(slot) => slot.get_or_init(|| encode().map(Arc::from)).clone(),
            None => encode().map(Arc::from),
        }
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct Publication {
    pub(crate) epoch: u64,
    pub(crate) update: RealtimeUpdate,
    /// Deepest display depth a book publication can serve. `usize::MAX` unless
    /// the owner converted fewer levels than the source book holds.
    pub(crate) book_depth: usize,
    pub(crate) encoded: Arc<EncodedPayloads>,
}

impl Publication {
    pub(crate) fn new(update: RealtimeUpdate) -> Self {
        Self::at(0, update)
    }

    pub(crate) fn at(epoch: u64, update: RealtimeUpdate) -> Self {
        Self {
            epoch,
            update,
            book_depth: usize::MAX,
            encoded: Arc::default(),
        }
    }
}
