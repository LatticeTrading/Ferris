//! Optional native subscription control around the stock multi-hash driver.
//! No second reader, transport, or market-data parser. Venue protocols describe
//! control frames and data-topic identity; this layer owns their lifecycle.
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    time::Duration,
};

use ccxt::{
    exchange::{DerivedExchange, Exchange, ExchangeRuntime},
    exchange_generated::ExchangeBase,
    Value,
};

use super::{LiveChannel, LiveSpec};
use crate::exchanges::traits::ExchangeError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::exchanges::ccxt) enum UnsubscribeMode {
    Stock,
    Native,
    Reconnect,
}

pub(in crate::exchanges::ccxt) fn stock_unsubscribe(_channel: LiveChannel) -> UnsubscribeMode {
    UnsubscribeMode::Stock
}

pub(in crate::exchanges::ccxt) enum LiveEvent {
    Data(Value),
    Unsubscribed(String),
}

pub(in crate::exchanges::ccxt) struct Subscription {
    pub topic: String,
    pub hash: String,
    pub snapshot_required: bool,
}

pub(in crate::exchanges::ccxt) enum Incoming {
    Other,
    /// Server-assigned channel IDs fence subscription generations (Bitfinex).
    Subscribed {
        topic: String,
        id: String,
        cleanup: Vec<String>,
    },
    Retired {
        id: String,
        accepted: bool,
    },
    RoutedData {
        id: String,
        snapshot: bool,
    },
    Failure(String),
    Ack {
        topics: Vec<String>,
        unsubscribe: bool,
        accepted: bool,
    },
    Data {
        topic: String,
        snapshot: bool,
    },
}

/// Topic-correlated control protocol. Qualify before reuse: unsubscribe ACK
/// must fence the old generation's data; re-add is serialized behind that ACK.
/// Subscribe ACK need not precede data (Apex sends its snapshot first). Without
/// request IDs a delayed duplicate across an entire later retirement cannot be
/// distinguished; protocols lacking ordered unsubscribe semantics must use
/// Reconnect (or a future request-ID-aware adapter), not this implementation.
pub(in crate::exchanges::ccxt) trait Protocol: Send {
    type Core: ExchangeBase;
    const CHANNEL_IDS: bool = false;
    /// Optional application heartbeat for ports whose transport only sends
    /// protocol-level Ping. No extra socket or reader is introduced.
    const HEARTBEAT: Option<Duration> = None;
    fn ping(_core: &mut Self::Core) -> Value {
        panic!("heartbeat has no stock ping dispatcher");
    }
    fn pong(_message: &Value) -> Option<String> {
        None
    }
    fn subscription(spec: &LiveSpec, timeframes: &Value) -> Result<Subscription, ExchangeError>;
    fn unsubscribe(topic: &str) -> Value;
    fn incoming(message: &Value) -> Incoming;
    /// Optional override for the incremental-book snapshot replay index. The
    /// default keeps the stock behavior unchanged.
    fn get_cache_index(_core: &Self::Core, _orderbook: &Value, _deltas: &Value) -> Option<Value> {
        None
    }

    /// Opt in only when stock's snapshot driver cannot dispatch `handle_deltas`.
    /// Selection, reset, cache clearing and resolution remain in stock ws_run.
    const REPLAY_BOOK_DELTAS: bool = false;

    /// Dispatch one cached delta to the stock parser. A protocol opting in must
    /// supply a verified dispatch arm; a missing implementation fails loudly.
    fn replay_book_delta(_core: &mut Self::Core, _orderbook: Value, _delta: Value) {
        panic!("buffered book replay has no stock delta dispatcher");
    }
}

struct Held {
    subscription: Subscription,
    retiring: bool,
    unsubscribe_sent: bool,
    awaiting_subscribe: bool,
    awaiting_snapshot: bool,
    channel_id: Option<String>,
}

const CONTROL_HASH: &str = "ferris:native-control";
const UNSUBSCRIBE: &str = "ferris_native_unsubscribe";
const HEARTBEAT: &str = "ferris_application_heartbeat";

#[cfg(test)]
mod tests;

/// Used ONLY as the outer ws_run driver. Every dynamic method, including all
/// stock watches/parsers, delegates to the original core except handle_message,
/// optional snapshot hooks, and queued control/heartbeat sends. Nested stock
/// ws_run calls use the same-URL branch and never become competing readers.
pub(in crate::exchanges::ccxt) struct Controlled<C, P> {
    pub core: C,
    held: HashMap<String, Held>,
    routes: HashMap<String, (String, Vec<String>)>,
    seen_ids: HashSet<String>,
    events: VecDeque<Result<LiveEvent, ExchangeError>>,
    url: String,
    pending_ping: Option<String>,
    protocol: std::marker::PhantomData<P>,
}

impl<C: ExchangeBase, P: Protocol<Core = C>> Controlled<C, P> {
    pub fn new(core: C) -> Self {
        Self {
            core,
            held: HashMap::new(),
            routes: HashMap::new(),
            seen_ids: HashSet::new(),
            events: VecDeque::new(),
            url: String::new(),
            pending_ping: None,
            protocol: std::marker::PhantomData,
        }
    }

    pub fn bind_url(&mut self, url: &str) {
        self.url = url.to_string();
        self.pending_ping = None;
        self.schedule_heartbeat();
    }

    pub fn subscribe(&mut self, spec: &LiveSpec) -> Result<(), ExchangeError> {
        let subscription = P::subscription(spec, &self.core.timeframes)?;
        if self.held.contains_key(&subscription.topic) {
            return Err(ExchangeError::Internal(
                "native topic is still owned or retiring".into(),
            ));
        }
        self.held.insert(
            subscription.topic.clone(),
            Held {
                awaiting_snapshot: subscription.snapshot_required,
                subscription,
                retiring: false,
                unsubscribe_sent: false,
                awaiting_subscribe: true,
                channel_id: None,
            },
        );
        Ok(())
    }

    pub fn unsubscribe(&mut self, spec: &LiveSpec) -> Result<(), ExchangeError> {
        let topic = self
            .held
            .iter_mut()
            .find(|(_, held)| held.subscription.hash == spec.hash)
            .ok_or_else(|| {
                ExchangeError::Internal("native unsubscribe has no owned topic".into())
            })?;
        topic.1.retiring = true;
        // Queue on the stock driver, AFTER any already-queued watch. Sending
        // directly here would race a subscription not yet sent or connected.
        ccxt::exchange_stubs::enqueue_spawn(UNSUBSCRIBE, vec![Value::from(topic.0.as_str())]);
        Ok(())
    }

    pub async fn next(&mut self, url: &str, hashes: &[String]) -> Result<LiveEvent, ExchangeError> {
        let mut hashes = hashes.to_vec();
        hashes.push(CONTROL_HASH.to_string());
        loop {
            if let Some(event) = self.events.pop_front() {
                return event;
            }
            let raw = self
                .ws_run(
                    url.to_string(),
                    hashes.clone(),
                    Value::Null,
                    vec![],
                    Value::Null,
                )
                .await;
            if raw.as_str() != Some(CONTROL_HASH) {
                return Ok(LiveEvent::Data(raw));
            }
        }
    }

    fn schedule_heartbeat(&self) {
        if let Some(period) = P::HEARTBEAT {
            // Use the stock drive-loop timer queue, not a select that could
            // cancel a snapshot fetch halfway through draining stock work.
            ccxt::exchange_stubs::enqueue_spawn_after(HEARTBEAT, vec![], period.as_millis() as i64);
        }
    }

    fn send_heartbeat(&mut self) -> Result<(), ExchangeError> {
        if self.pending_ping.is_some() {
            return Err(ExchangeError::UpstreamRequest(
                "application heartbeat timed out".into(),
            ));
        }
        let ping = P::ping(&mut self.core);
        let raw_id = ccxt::value::get_value_k(&ping, "id");
        let id = raw_id
            .as_str()
            .map(str::to_owned)
            .or_else(|| raw_id.as_i64().map(|id| id.to_string()))
            .ok_or_else(|| ExchangeError::Internal("stock ping has no request id".into()))?;
        let client = ccxt_pro::pro::ws_client::get_client(&self.url);
        if !client.is_some_and(|client| client.send_text(ping.to_json().to_string())) {
            return Err(ExchangeError::UpstreamRequest(
                "application heartbeat send failed".into(),
            ));
        }
        self.pending_ping = Some(id);
        self.schedule_heartbeat();
        Ok(())
    }

    fn intercept(&mut self, message: &Value) -> bool {
        if let Some(id) = P::pong(message) {
            if self.pending_ping.as_deref() == Some(&id) {
                self.pending_ping = None;
            }
        }
        match P::incoming(message) {
            Incoming::Other => false,
            Incoming::Failure(message) => {
                self.fail(message);
                true
            }
            Incoming::Subscribed { topic, id, cleanup } => {
                let Some(held) = self.held.get_mut(&topic) else {
                    self.fail("unmatched native subscribe acknowledgment".into());
                    return true;
                };
                if held.channel_id.as_deref() == Some(&id) {
                    return true; // duplicate; never rewrite stock routing
                }
                if held.channel_id.is_some()
                    || self.seen_ids.contains(&id)
                    || self.seen_ids.len() >= 4096
                {
                    self.fail("reused or excessive native channel IDs".into());
                    return true;
                }
                held.channel_id = Some(id.clone());
                held.awaiting_subscribe = false;
                self.seen_ids.insert(id.clone());
                self.routes.insert(id, (topic.clone(), cleanup));
                if held.retiring {
                    ccxt::exchange_stubs::enqueue_spawn(UNSUBSCRIBE, vec![Value::from(topic)]);
                }
                false // stock must record chanId -> subscription for its parsers
            }
            Incoming::Retired { id, accepted } => {
                let Some((topic, _)) = self.routes.get(&id) else {
                    return true;
                };
                let topic = topic.clone();
                let Some(held) = self.held.get(&topic) else {
                    return true;
                };
                if !held.retiring || !held.unsubscribe_sent {
                    return true;
                }
                if accepted {
                    let (_, cleanup) = self.routes.remove(&id).expect("owned channel");
                    // Stock stores reverse lookup aliases containing the channel
                    // ID as a scalar. Remove only this generation's aliases.
                    if let Some(client) = ccxt_pro::pro::ws_client::get_client(&self.url) {
                        if let Some(subs) = client.subscriptions_value().as_map() {
                            for (key, value) in subs {
                                if value.as_str() == Some(&id) {
                                    ccxt_pro::pro::ws_client::value_subs_remove(&self.url, key);
                                }
                            }
                        }
                    }
                    for key in std::iter::once(id).chain(cleanup) {
                        ccxt_pro::pro::ws_client::value_subs_remove(&self.url, &key);
                    }
                }
                self.ack(vec![topic], true, accepted);
                true
            }
            Incoming::RoutedData { id, snapshot } => {
                if let Some((topic, _)) = self.routes.get(&id) {
                    let topic = topic.clone();
                    self.data(&topic, snapshot)
                } else {
                    if !self.seen_ids.contains(&id) {
                        // Without the subscribe ACK there is no safe way to route
                        // a snapshot. Do not silently lose it and accept deltas.
                        self.fail("native data preceded channel assignment".into());
                    }
                    true
                }
            }
            Incoming::Data { topic, snapshot } => self.data(&topic, snapshot),
            Incoming::Ack {
                topics,
                unsubscribe,
                accepted,
            } => {
                self.ack(topics, unsubscribe, accepted);
                true
            }
        }
    }

    fn data(&mut self, topic: &str, snapshot: bool) -> bool {
        let Some(held) = self.held.get_mut(topic) else {
            return true;
        };
        if held.retiring || (held.awaiting_snapshot && !snapshot) {
            return true;
        }
        held.awaiting_snapshot = false;
        false
    }

    fn fail(&mut self, message: String) {
        self.events
            .push_back(Err(ExchangeError::UpstreamRequest(message)));
        self.wake();
    }

    fn wake(&self) {
        if let Some(client) = ccxt_pro::pro::ws_client::get_client(&self.url) {
            client.resolve(CONTROL_HASH, Value::from(CONTROL_HASH));
        }
    }

    fn ack(&mut self, topics: Vec<String>, unsubscribe: bool, accepted: bool) {
        for topic in topics {
            let Some(held) = self.held.get_mut(&topic) else {
                continue;
            };
            if unsubscribe {
                if !held.retiring || !held.unsubscribe_sent {
                    continue;
                }
                if accepted {
                    let held = self.held.remove(&topic).expect("retiring topic");
                    self.events
                        .push_back(Ok(LiveEvent::Unsubscribed(held.subscription.hash)));
                } else {
                    self.events
                        .push_back(Err(ExchangeError::UpstreamRequest(format!(
                            "native unsubscribe rejected for {topic}"
                        ))));
                }
            } else if held.awaiting_subscribe {
                if accepted {
                    // Initial snapshots may precede this ACK. Retirement,
                    // not subscription acknowledgment, fences generations.
                    held.awaiting_subscribe = false;
                } else {
                    self.events
                        .push_back(Err(ExchangeError::UpstreamRequest(format!(
                            "native subscribe rejected for {topic}"
                        ))));
                }
            }
        }
        if !self.events.is_empty() {
            self.wake();
        }
    }
}

impl<C: ExchangeBase, P: Protocol<Core = C>> Deref for Controlled<C, P> {
    type Target = Exchange;
    fn deref(&self) -> &Exchange {
        &self.core
    }
}
impl<C: ExchangeBase, P: Protocol<Core = C>> DerefMut for Controlled<C, P> {
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.core
    }
}
// No parser runs on this wrapper: call_dynamic forwards to the original core.
impl<C: ExchangeBase, P: Protocol<Core = C>> DerivedExchange for Controlled<C, P> {}
impl<C: ExchangeBase, P: Protocol<Core = C>> ExchangeBase for Controlled<C, P> {
    fn call_dynamic<'a>(
        &'a mut self,
        method: &'a str,
        args: Vec<Value>,
    ) -> Pin<Box<dyn Future<Output = Value> + Send + 'a>> {
        Box::pin(async move {
            if method == HEARTBEAT && P::HEARTBEAT.is_some() {
                if let Err(error) = self.send_heartbeat() {
                    self.fail(error.to_string());
                }
                return Value::Null;
            }
            if method == UNSUBSCRIBE {
                let topic = args[0].as_str().expect("internal native topic");
                let mut identity = topic.to_string();
                if let Some(held) = self.held.get_mut(topic) {
                    if held.unsubscribe_sent {
                        return Value::Null;
                    }
                    if P::CHANNEL_IDS {
                        let Some(id) = &held.channel_id else {
                            return Value::Null;
                        };
                        identity = id.clone();
                    }
                    held.unsubscribe_sent = true;
                }
                let client = ccxt_pro::pro::ws_client::get_client(&self.url);
                if !client.is_some_and(|client| {
                    client.send_text(P::unsubscribe(&identity).to_json().to_string())
                }) {
                    self.events.push_back(Err(ExchangeError::UpstreamRequest(
                        "native unsubscribe send failed".into(),
                    )));
                    if let Some(client) = ccxt_pro::pro::ws_client::get_client(&self.url) {
                        client.resolve(CONTROL_HASH, Value::from(CONTROL_HASH));
                    }
                }
                return Value::Null;
            }
            if method == "handle_message"
                && args.get(1).is_some_and(|message| self.intercept(message))
            {
                return Value::Null;
            }
            if method == "get_cache_index" {
                if let (Some(orderbook), Some(deltas)) = (args.first(), args.get(1)) {
                    if let Some(value) = P::get_cache_index(&self.core, orderbook, deltas) {
                        return value;
                    }
                }
            }
            if method == "handle_deltas" && P::REPLAY_BOOK_DELTAS {
                let (Some(orderbook), Some(deltas)) = (args.first(), args.get(1)) else {
                    return Value::Null;
                };
                let Some(deltas) = deltas.as_array() else {
                    return Value::Null;
                };
                if orderbook.as_map().is_none() {
                    return Value::Null;
                }
                // Match stock handle_book_deltas: clone the same Value handle
                // for each delta, in order. Never serialize/rebuild the book:
                // __book_id and book-side handles carry shared mutation state.
                for delta in deltas {
                    P::replay_book_delta(&mut self.core, orderbook.clone(), delta.clone());
                }
                return Value::Null;
            }
            self.core.call_dynamic(method, args).await
        })
    }
}
