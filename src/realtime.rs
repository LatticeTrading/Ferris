//! Ferris live demand and owned delivery. Stock Pro acquisition lives in CCXT owners.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::{broadcast, Notify};

use crate::{
    exchanges::{
        ccxt::{CcxtService, PreparedTopic},
        traits::ExchangeError,
    },
    models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade, MarketStatsField, MarketStatsFieldName},
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

#[derive(Clone)]
pub struct RealtimeService {
    service: CcxtService,
}

impl RealtimeService {
    pub fn new(service: CcxtService) -> Self {
        Self { service }
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
    ) -> Result<PreparedTopic, ExchangeError> {
        self.service.prepare_live(channel, topic).await
    }

    pub(crate) async fn subscribe_prepared(
        &self,
        prepared: PreparedTopic,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        self.service.subscribe_live(prepared).await
    }

    pub async fn shutdown(&self) -> Result<(), ExchangeError> {
        self.service.shutdown_live().await
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
        })
    }

    pub async fn recv(&mut self) -> Result<RealtimeUpdate, broadcast::error::RecvError> {
        loop {
            let publication = self.receiver.recv().await?;
            // Continuity errors concern this receiver's existing lease even
            // when a replacement session has already advanced the data epoch.
            // Only stale market data is suppressible; otherwise a slow reader
            // can silently miss the reconnect boundary before fresh updates.
            if matches!(&publication.update, RealtimeUpdate::Error(_))
                || publication.epoch == self.state.epoch.load(Ordering::Acquire)
            {
                return Ok(publication.update);
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
    pub epoch: u64,
    pub update: RealtimeUpdate,
}
