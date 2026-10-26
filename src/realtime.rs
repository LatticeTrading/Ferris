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
        phoenix::PhoenixExchange,
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
        if exchange == "phoenix" {
            let allowed = ["timeframe", "interval", "levels", "depth", "limit"];
            if let Some(object) = params.as_object() {
                if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
                    return Err(format!("unsupported Phoenix realtime parameter `{key}`"));
                }
            }
        }
        // serde_json's default Map is ordered, including nested objects.
        Ok(Self {
            exchange,
            symbol,
            params,
        })
    }
}

pub(crate) enum PreparedRealtimeTopic {
    Ccxt(PreparedTopic),
    Phoenix {
        channel: RealtimeChannel,
        topic: RealtimeTopic,
        levels: usize,
        client_key: String,
    },
}

impl PreparedRealtimeTopic {
    pub(crate) fn client_key(&self) -> &str {
        match self {
            Self::Ccxt(prepared) => &prepared.client_key,
            Self::Phoenix { client_key, .. } => client_key,
        }
    }
}

#[derive(Clone)]
pub struct RealtimeService {
    service: CcxtService,
    phoenix: Option<Arc<PhoenixExchange>>,
}

impl RealtimeService {
    pub fn new(service: CcxtService) -> Self {
        Self {
            service,
            phoenix: None,
        }
    }

    pub fn with_phoenix(mut self, phoenix: Arc<PhoenixExchange>) -> Self {
        self.phoenix = Some(phoenix);
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
        if topic.exchange == "phoenix" {
            let levels = topic
                .params
                .get("levels")
                .or_else(|| topic.params.get("depth"))
                .or_else(|| topic.params.get("limit"))
                .and_then(Value::as_u64)
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ExchangeError::BadSymbol("Phoenix depth is too large".into()))?
                .unwrap_or(usize::MAX);
            if levels == 0 {
                return Err(ExchangeError::BadSymbol(
                    "Phoenix depth must be positive".into(),
                ));
            }
            let timeframe = (channel == RealtimeChannel::Ohlcv).then(|| {
                topic
                    .params
                    .get("timeframe")
                    .or_else(|| topic.params.get("interval"))
                    .and_then(Value::as_str)
                    .unwrap_or("1m")
                    .to_string()
            });
            if let Some(timeframe) = &timeframe {
                if !matches!(
                    timeframe.as_str(),
                    "1s" | "5s" | "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d"
                ) {
                    return Err(ExchangeError::UnsupportedFeature(format!(
                        "unsupported Phoenix timeframe `{timeframe}`"
                    )));
                }
            }
            let client_key = serde_json::to_string(&(
                topic.exchange.clone(),
                topic.symbol.clone(),
                channel.as_str(),
                timeframe,
                levels,
            ))
            .map_err(|e| ExchangeError::Internal(e.to_string()))?;
            return Ok(PreparedRealtimeTopic::Phoenix {
                channel,
                topic,
                levels,
                client_key,
            });
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
            PreparedRealtimeTopic::Phoenix {
                channel,
                topic,
                levels,
                client_key,
            } => {
                let phoenix = self.phoenix.as_ref().ok_or_else(|| {
                    ExchangeError::Internal("Phoenix realtime adapter is not configured".into())
                })?;
                phoenix
                    .subscribe_live(channel, topic, levels, client_key)
                    .await
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), ExchangeError> {
        if let Some(phoenix) = &self.phoenix {
            phoenix.shutdown_live().await?;
        }
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
    pub(crate) epoch: u64,
    pub(crate) update: RealtimeUpdate,
}

impl Publication {
    pub(crate) fn new(update: RealtimeUpdate) -> Self {
        Self { epoch: 0, update }
    }
}
