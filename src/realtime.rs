//! Ferris live demand and owned delivery. Stock Pro acquisition lives in CCXT owners.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
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

    pub async fn recv(&mut self) -> Result<RealtimeUpdate, broadcast::error::RecvError> {
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
                return Ok(match publication.update {
                    RealtimeUpdate::OrderBook(book) => {
                        match book.amount_view(self.book_amount_mode) {
                            Ok(view) => RealtimeUpdate::OrderBook(view),
                            Err(error) => RealtimeUpdate::Error(error.to_string().into()),
                        }
                    }
                    other => other,
                });
            }
        }
    }
}

impl Drop for RealtimeReceiver {
    fn drop(&mut self) {
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
}

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct Publication {
    pub(crate) epoch: u64,
    pub(crate) update: RealtimeUpdate,
}

impl Publication {
    pub(crate) fn new(update: RealtimeUpdate) -> Self {
        Self { epoch: 0, update }
    }
}
