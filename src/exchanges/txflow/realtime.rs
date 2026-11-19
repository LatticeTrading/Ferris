//! TxFlow realtime: one multiplexed socket per endpoint (`/ws` node streams,
//! `/api-ws` candles), behind the exchange-neutral shared hub.
//!
//! Neither endpoint acknowledges subscriptions usefully (`/ws` never does;
//! `/api-ws` ACKs even invalid intervals), so everything is validated locally
//! and each feed is routed by the instrument id carried in its frames.
use super::{
    wire::{self, BookState, CandleState, CtxStore},
    Market, TxflowExchange,
};
use crate::{
    exchanges::traits::ExchangeError,
    orderbook::BookAmountMode,
    realtime::{
        native::{Feed, Protocol, Session, SharedHub},
        NativeRealtimeOwner, NativeSubscriptionPlan, RealtimeChannel, RealtimeSubscription,
        RealtimeTopic, RealtimeUpdate,
    },
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::time::Instant;

pub(super) const MAX_BOOK_LEVELS: u64 = 200;
/// Unknown upstream frame-rate threshold: pace control frames, never burst.
const PACE: Duration = Duration::from_millis(200);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Endpoint {
    Node,
    Api,
}

pub(super) enum Kind {
    Trades(Arc<Market>),
    Book(Arc<Market>),
    Candle(Arc<Market>, &'static str),
    AssetCtxs(Arc<CtxStore>),
}
pub(super) struct Spec {
    /// Wire subscription identity on one socket.
    pub stream: String,
    pub kind: Kind,
}
impl Spec {
    fn subscription(&self) -> Value {
        match &self.kind {
            Kind::Trades(m) => json!({"type":"trades","coin":m.id.to_string()}),
            Kind::Book(m) => json!({"type":"l2BookDepth","coin":m.id.to_string()}),
            // The numeric id keeps the `s` echo stable at first; later frames
            // were observed to switch to the base name, handled in routing.
            Kind::Candle(m, i) => json!({"type":"candle","coin":m.id.to_string(),"interval":i}),
            Kind::AssetCtxs(_) => json!({"type":"allAssetCtxs"}),
        }
    }
}

pub(super) const INTERVALS: [&str; 11] = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d",
];
pub(super) fn interval(tf: &str) -> Result<(&'static str, u64), ExchangeError> {
    let ms = match tf {
        "1m" => 60_000,
        "3m" => 180_000,
        "5m" => 300_000,
        "15m" => 900_000,
        "30m" => 1_800_000,
        "1h" => 3_600_000,
        "2h" => 7_200_000,
        "4h" => 14_400_000,
        "8h" => 28_800_000,
        "12h" => 43_200_000,
        "1d" => 86_400_000,
        _ => {
            return Err(ExchangeError::UnsupportedFeature(format!(
                "unsupported TxFlow timeframe `{tf}`; supported: {}",
                INTERVALS.join(", ")
            )))
        }
    };
    let name = INTERVALS.iter().find(|i| **i == tf).expect("listed");
    Ok((name, ms))
}

pub(super) struct TxProtocol {
    pub endpoint: Endpoint,
}
impl Protocol for TxProtocol {
    type Spec = Spec;
    type Session = TxSession;
    fn capacity(&self) -> usize {
        // TxFlow documents one multiplexed socket per endpoint and no
        // per-connection subscription cap for `/ws` / `/api-ws`.
        500
    }
    fn max_connections(&self) -> usize {
        // 10 sockets per IP per endpoint, shared with every process on the host.
        2
    }
    fn keepalive(&self) -> Option<(Duration, String)> {
        // ~16 s without a client frame closes the socket (1006).
        Some((
            Duration::from_secs(10),
            json!({"method":"ping"}).to_string(),
        ))
    }
    fn session(&self) -> TxSession {
        TxSession {
            endpoint: self.endpoint,
            entries: HashMap::new(),
            last_frame: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// Desired; subscribe frame not sent yet.
    Pending,
    Live,
    /// Needs unsubscribe then a fresh subscribe (book resync).
    Resync,
    /// No longer desired; unsubscribe frame not sent yet.
    Leaving,
}
struct Entry {
    feed: Arc<Feed<Spec>>,
    phase: Phase,
    book: BookState,
    candles: CandleState,
    awaiting_since: Option<Instant>,
    resyncs: u32,
}
pub(super) struct TxSession {
    endpoint: Endpoint,
    entries: HashMap<String, Entry>,
    last_frame: Option<Instant>,
}
impl TxSession {
    fn frame(&self, spec: &Spec, subscribe: bool) -> String {
        wire::subscribe_frame(
            self.endpoint == Endpoint::Node,
            subscribe,
            spec.subscription(),
        )
    }
    fn live(&mut self, stream: &str) -> Option<&mut Entry> {
        self.entries
            .get_mut(stream)
            .filter(|e| e.phase == Phase::Live)
    }
}
impl Session<Spec> for TxSession {
    fn reconcile(&mut self, desired: Vec<Arc<Feed<Spec>>>) -> Result<Option<String>, String> {
        let now = Instant::now();
        let mut wanted = std::collections::HashSet::new();
        for feed in desired {
            wanted.insert(feed.spec.stream.clone());
            match self.entries.get_mut(&feed.spec.stream) {
                Some(e) => {
                    if !Arc::ptr_eq(&e.feed, &feed) {
                        e.feed = feed; // same wire stream, new lease generation
                    }
                    if e.phase == Phase::Leaving {
                        e.phase = Phase::Live; // still subscribed upstream
                    }
                }
                None => {
                    self.entries.insert(
                        feed.spec.stream.clone(),
                        Entry {
                            feed,
                            phase: Phase::Pending,
                            book: BookState::default(),
                            candles: CandleState::default(),
                            awaiting_since: None,
                            resyncs: 0,
                        },
                    );
                }
            }
        }
        self.entries.retain(|stream, e| {
            if wanted.contains(stream) {
                return true;
            }
            match e.phase {
                Phase::Pending => false,
                _ => {
                    e.phase = Phase::Leaving;
                    true
                }
            }
        });
        for e in self.entries.values_mut() {
            if e.phase == Phase::Live
                && e.awaiting_since
                    .is_some_and(|t| now.duration_since(t) >= SNAPSHOT_TIMEOUT)
            {
                if e.resyncs >= 1 {
                    return Err("TxFlow book snapshot timed out after resubscribe".into());
                }
                e.feed
                    .invalidate("TxFlow book snapshot timed out; resubscribing");
                e.book.reset();
                e.phase = Phase::Resync;
                e.resyncs += 1;
            }
        }
        if self
            .last_frame
            .is_some_and(|t| now.duration_since(t) < PACE)
        {
            return Ok(None);
        }
        let pick = |phase: Phase| {
            let mut keys = self
                .entries
                .iter()
                .filter(|(_, e)| e.phase == phase)
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>();
            keys.sort();
            keys.into_iter().next()
        };
        let frame = if let Some(stream) = pick(Phase::Leaving) {
            let e = self.entries.remove(&stream).expect("picked");
            self.frame(&e.feed.spec, false)
        } else if let Some(stream) = pick(Phase::Resync) {
            let e = self.entries.get_mut(&stream).expect("picked");
            e.phase = Phase::Pending;
            let spec = &self.entries[&stream].feed.spec;
            self.frame(spec, false)
        } else if let Some(stream) = pick(Phase::Pending) {
            let e = self.entries.get_mut(&stream).expect("picked");
            e.phase = Phase::Live;
            if matches!(e.feed.spec.kind, Kind::Book(_)) {
                e.awaiting_since = Some(now);
            }
            let spec = &self.entries[&stream].feed.spec;
            self.frame(spec, true)
        } else {
            return Ok(None);
        };
        self.last_frame = Some(now);
        Ok(Some(frame))
    }

    fn message(&mut self, text: &str) -> Result<(), String> {
        let v: Value =
            serde_json::from_str(text).map_err(|e| format!("invalid TxFlow JSON: {e}"))?;
        // Keepalive replies never reach channel dispatch.
        if v.get("method").and_then(Value::as_str) == Some("PONG") {
            return Ok(());
        }
        let channel = v.get("channel").and_then(Value::as_str);
        if v.get("type").and_then(Value::as_str) == Some("error") || channel == Some("error") {
            // RISK_RATE_LIMITED drops an unknown control frame (`/ws` has no
            // ACK to tell which). Rebuild the session from desired state
            // after the hub's backoff instead of guessing.
            return Err(format!("TxFlow websocket error: {v}"));
        }
        let data = v.get("data").unwrap_or(&Value::Null);
        match (self.endpoint, channel) {
            (_, Some("pong" | "subscriptionResponse")) => Ok(()),
            (Endpoint::Node, Some("trades")) => {
                let rows = data
                    .as_array()
                    .ok_or("TxFlow trades frame is not an array")?;
                let mut by_id: HashMap<u64, Vec<&Value>> = HashMap::new();
                for row in rows {
                    let id = wire::id_of(row.get("instrumentId"))
                        .ok_or("TxFlow trade has no instrumentId")?;
                    by_id.entry(id).or_default().push(row);
                }
                for (id, rows) in by_id {
                    let Some(e) = self.live(&format!("trades:{id}")) else {
                        continue; // late data for a retired stream is not demand
                    };
                    let Kind::Trades(m) = &e.feed.spec.kind else {
                        continue;
                    };
                    match rows
                        .into_iter()
                        .map(|r| wire::trade(m, r))
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(trades) => e
                            .feed
                            .publish_update(RealtimeUpdate::Trades(Arc::new(trades))),
                        Err(error) => e.feed.invalidate(&error),
                    }
                }
                Ok(())
            }
            (Endpoint::Node, Some("l2BookDepth")) => {
                let id = wire::id_of(data.get("id")).ok_or("TxFlow book frame has no id")?;
                let Some(e) = self.live(&format!("book:{id}")) else {
                    return Ok(());
                };
                let Kind::Book(m) = &e.feed.spec.kind else {
                    return Ok(());
                };
                let m = m.clone();
                match e.book.apply(&m, data) {
                    Ok(Some(book)) => {
                        e.awaiting_since = None;
                        e.resyncs = 0;
                        e.feed.publish(RealtimeUpdate::OrderBook(Arc::new(book)));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        // Never emit a book reconstructed across a gap.
                        e.feed.invalidate(&error);
                        e.phase = Phase::Resync;
                    }
                }
                Ok(())
            }
            (Endpoint::Node, Some("allAssetCtxs")) => {
                if let Some(e) = self.live("ctxs") {
                    if let Kind::AssetCtxs(store) = &e.feed.spec.kind {
                        if let Err(error) = store.merge(data) {
                            store.fail(&error);
                            e.feed.invalidate(&error);
                        }
                    }
                }
                Ok(())
            }
            (Endpoint::Api, Some("candle")) => {
                let (s, i, bar) = wire::candle(data)?;
                // `s` echoes the subscribed id first, then the base name.
                let key = self
                    .entries
                    .iter()
                    .filter(|(_, e)| e.phase == Phase::Live)
                    .find(|(_, e)| match &e.feed.spec.kind {
                        Kind::Candle(m, iv) => *iv == i && m.matches_echo(&s),
                        _ => false,
                    })
                    .map(|(k, _)| k.clone());
                if let Some(e) = key.and_then(|k| self.entries.get_mut(&k)) {
                    if let Some(window) = e.candles.apply(bar) {
                        e.feed.publish(RealtimeUpdate::Ohlcv(Arc::new(window)));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

struct Plan {
    hub: Arc<SharedHub<TxProtocol>>,
    spec: Spec,
    topic: RealtimeTopic,
    feed_key: String,
    client_key: String,
    levels: usize,
    amount_mode: BookAmountMode,
}
#[async_trait]
impl NativeSubscriptionPlan for Plan {
    fn client_key(&self) -> &str {
        &self.client_key
    }
    async fn subscribe(self: Box<Self>) -> Result<RealtimeSubscription, ExchangeError> {
        let receiver = self.hub.subscribe(self.feed_key, self.spec)?;
        Ok(RealtimeSubscription {
            key: self.client_key,
            topic: self.topic,
            levels_limit: self.levels,
            receiver: receiver.with_book_amount_mode(self.amount_mode),
        })
    }
}

#[async_trait]
impl NativeRealtimeOwner for TxflowExchange {
    fn exchange_id(&self) -> &'static str {
        "txflow"
    }
    async fn prepare_native(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
    ) -> Result<Box<dyn NativeSubscriptionPlan>, ExchangeError> {
        let mut topic = RealtimeTopic::from_client_request(
            Some(topic.exchange),
            Some(topic.symbol),
            topic.params,
        )
        .map_err(ExchangeError::BadSymbol)?;
        if topic.exchange != "txflow" {
            return Err(ExchangeError::BadSymbol("wrong native owner".into()));
        }
        let allowed: &[&str] = match channel {
            RealtimeChannel::Trades => &[],
            RealtimeChannel::OrderBook => &["levels", "depth", "limit", "amountUnit"],
            RealtimeChannel::Ohlcv => &["timeframe", "interval"],
        };
        let amount_mode = BookAmountMode::parse(&topic.params)?;
        let params = topic.params.as_object().expect("normalized params");
        if let Some(key) = params.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(ExchangeError::BadSymbol(format!(
                "unsupported TxFlow realtime parameter `{key}`"
            )));
        }
        let mut value: Option<&Value> = None;
        for key in allowed.iter().filter(|key| **key != "amountUnit") {
            if let Some(v) = params.get(*key) {
                if value.is_some_and(|old| old != v) {
                    return Err(ExchangeError::BadSymbol(
                        "conflicting TxFlow parameter aliases".into(),
                    ));
                }
                value = Some(v);
            }
        }
        let levels = if channel == RealtimeChannel::OrderBook {
            let n = value
                .map(|v| {
                    v.as_u64().ok_or_else(|| {
                        ExchangeError::BadSymbol("TxFlow depth must be an integer".into())
                    })
                })
                .transpose()?
                .unwrap_or(MAX_BOOK_LEVELS);
            if !(1..=MAX_BOOK_LEVELS).contains(&n) {
                return Err(ExchangeError::BadSymbol(format!(
                    "TxFlow realtime depth must be 1–{MAX_BOOK_LEVELS}"
                )));
            }
            n as usize
        } else {
            usize::MAX
        };
        let timeframe = if channel == RealtimeChannel::Ohlcv {
            let tf = value
                .map(|v| {
                    v.as_str().ok_or_else(|| {
                        ExchangeError::BadSymbol("TxFlow timeframe must be a string".into())
                    })
                })
                .transpose()?
                .unwrap_or("1m");
            Some(interval(tf)?.0)
        } else {
            None
        };
        let market = self.resolve(&topic.symbol).await?;
        if channel == RealtimeChannel::OrderBook {
            super::book_policy(&market).validate_mode(amount_mode)?;
        }
        if market.delisted {
            return Err(ExchangeError::BadSymbol("TxFlow market is delisted".into()));
        }
        let (hub, stream, kind) = match channel {
            RealtimeChannel::Trades => (
                &self.node,
                format!("trades:{}", market.id),
                Kind::Trades(market.clone()),
            ),
            RealtimeChannel::OrderBook => (
                &self.node,
                format!("book:{}", market.id),
                Kind::Book(market.clone()),
            ),
            RealtimeChannel::Ohlcv => {
                let tf = timeframe.expect("ohlcv timeframe");
                (
                    &self.api_ws,
                    format!("candle:{}:{tf}", market.id),
                    Kind::Candle(market.clone(), tf),
                )
            }
        };
        let feed_key =
            json!([self.network_key, market.id, channel.as_str(), timeframe]).to_string();
        let client_key = json!([feed_key, levels, amount_mode]).to_string();
        topic.symbol = market.symbol();
        topic.params = match channel {
            RealtimeChannel::OrderBook => json!({"levels":levels}),
            RealtimeChannel::Ohlcv => json!({"timeframe":timeframe}),
            _ => json!({}),
        };
        if channel == RealtimeChannel::OrderBook && amount_mode == BookAmountMode::Base {
            topic.params["amountUnit"] = json!("base");
        }
        Ok(Box::new(Plan {
            hub: hub.clone(),
            spec: Spec { stream, kind },
            topic,
            feed_key,
            client_key,
            levels,
            amount_mode,
        }))
    }
    async fn shutdown_native(&self) -> Result<(), ExchangeError> {
        if let Some(keeper) = self.ctx_keeper.lock().await.take() {
            keeper.abort();
            let _ = keeper.await;
        }
        self.node.shutdown().await;
        self.api_ws.shutdown().await;
        Ok(())
    }
}

pub(super) fn ctx_spec(store: Arc<CtxStore>) -> Spec {
    Spec {
        stream: "ctxs".into(),
        kind: Kind::AssetCtxs(store),
    }
}
