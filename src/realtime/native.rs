//! Exchange-neutral shared native WebSocket ownership. Protocols own wire
//! subscriptions and state; this layer owns leases, bounded delivery, socket
//! tasks, cancellation, reconnect/backoff and final-demand cleanup.
use super::{DeliveryState, Publication, RealtimeReceiver, RealtimeUpdate};
use crate::exchanges::traits::ExchangeError;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Notify},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};

const QUEUE: usize = 128;
const MAX_CONNECTIONS: usize = 32;
const TICK: Duration = Duration::from_millis(25);

pub(crate) trait Protocol: Send + Sync + 'static {
    type Spec: Send + Sync + 'static;
    type Session: Session<Self::Spec>;
    fn capacity(&self) -> usize;
    fn session(&self) -> Self::Session;
}
pub(crate) trait Session<S>: Send {
    /// Reconcile desired leases, including native unsubscribe ACKs and rate
    /// limits. At most one batched control frame per tick.
    fn reconcile(&mut self, desired: Vec<Arc<Feed<S>>>) -> Result<Option<String>, String>;
    fn message(&mut self, text: &str) -> Result<(), String>;
}

pub(crate) struct Feed<S> {
    pub key: String,
    pub spec: S,
    sender: broadcast::Sender<Publication>,
    delivery: Arc<DeliveryState>,
    // Atomic with receiver attachment, so newcomers see a complete baseline
    // without replaying it to existing viewers or racing the next publication.
    latest: Mutex<Option<Publication>>,
}
impl<S> Feed<S> {
    pub fn publish(&self, update: RealtimeUpdate) {
        let mut latest = self.latest.lock();
        let publication = Publication {
            epoch: self.delivery.epoch.load(Ordering::Acquire),
            update,
        };
        *latest = Some(publication.clone());
        let _ = self.sender.send(publication);
    }
    /// Publish an incremental frame without making it a join-time baseline.
    /// Native trade updates use this so a later viewer receives the latest
    /// snapshot, not a duplicate of an arbitrary update.
    pub fn publish_update(&self, update: RealtimeUpdate) {
        let publication = Publication {
            epoch: self.delivery.epoch.load(Ordering::Acquire),
            update,
        };
        let _ = self.sender.send(publication);
    }
    pub fn invalidate(&self, error: &str) {
        let mut latest = self.latest.lock();
        let epoch = self.delivery.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let publication = Publication {
            epoch,
            update: RealtimeUpdate::Error(error.into()),
        };
        *latest = Some(publication.clone());
        let _ = self.sender.send(publication);
    }
    fn attach(&self, changed: Arc<Notify>) -> Option<RealtimeReceiver> {
        let latest = self.latest.lock();
        let initial = latest
            .clone()
            .filter(|publication| !matches!(publication.update, RealtimeUpdate::Error(_)));
        RealtimeReceiver::join(self.sender.subscribe(), self.delivery.clone(), changed)
            .map(|r| r.with_initial(initial))
    }
    fn demanded(&self) -> bool {
        self.delivery.viewers.load(Ordering::Acquire) != 0
    }
}

pub(crate) struct SharedHub<P: Protocol> {
    protocol: Arc<P>,
    url: String,
    state: Mutex<HubState<P::Spec>>,
}
struct HubState<S> {
    stopped: bool,
    owners: Vec<Owner<S>>,
}
struct Control<S> {
    accepting: bool,
    feeds: HashMap<String, Arc<Feed<S>>>,
}
struct Owner<S> {
    control: Arc<Mutex<Control<S>>>,
    changed: Arc<Notify>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl<S> Drop for Owner<S> {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl<P: Protocol> SharedHub<P> {
    pub fn new(url: String, protocol: P) -> Self {
        Self {
            protocol: Arc::new(protocol),
            url,
            state: Mutex::new(HubState {
                stopped: false,
                owners: vec![],
            }),
        }
    }
    /// Synchronous attachment under the ownership fence: cancellation cannot
    /// create demand without returning a receiver which releases it on drop.
    pub fn subscribe(&self, key: String, spec: P::Spec) -> Result<RealtimeReceiver, ExchangeError> {
        let mut hub = self.state.lock();
        if hub.stopped {
            return Err(ExchangeError::Internal(
                "native realtime owner is stopped".into(),
            ));
        }
        hub.owners
            .retain(|o| o.task.as_ref().is_some_and(|t| !t.is_finished()));
        for owner in &hub.owners {
            let control = owner.control.lock();
            if control.accepting {
                if let Some(receiver) = control
                    .feeds
                    .get(&key)
                    .and_then(|f| f.attach(owner.changed.clone()))
                {
                    return Ok(receiver);
                }
            }
        }
        for owner in &hub.owners {
            let mut control = owner.control.lock();
            if !control.accepting {
                continue;
            }
            control.feeds.retain(|_, f| f.demanded());
            if control.feeds.len() < self.protocol.capacity() {
                let (feed, receiver) = new_feed(key.clone(), spec, owner.changed.clone());
                control.feeds.insert(key, feed);
                owner.changed.notify_one();
                return Ok(receiver);
            }
        }
        if hub.owners.len() >= MAX_CONNECTIONS {
            return Err(ExchangeError::UpstreamRequest(
                "native realtime connection capacity reached".into(),
            ));
        }
        let changed = Arc::new(Notify::new());
        let (feed, receiver) = new_feed(key.clone(), spec, changed.clone());
        let control = Arc::new(Mutex::new(Control {
            accepting: true,
            feeds: HashMap::from([(key, feed)]),
        }));
        let (stop, stopping) = watch::channel(false);
        let worker_control = control.clone();
        let worker_changed = changed.clone();
        let protocol = self.protocol.clone();
        let url = self.url.clone();
        let task = tokio::spawn(async move {
            run(
                url,
                protocol,
                worker_control.clone(),
                worker_changed,
                stopping,
            )
            .await;
            let mut control = worker_control.lock();
            control.accepting = false;
            control.feeds.clear(); // close receivers, including shutdown with active leases
        });
        hub.owners.push(Owner {
            control,
            changed,
            stop,
            task: Some(task),
        });
        Ok(receiver)
    }
    pub async fn shutdown(&self) {
        let owners = {
            let mut hub = self.state.lock();
            hub.stopped = true;
            for o in &hub.owners {
                o.stop.send_replace(true);
            }
            std::mem::take(&mut hub.owners)
        };
        for mut owner in owners {
            if let Some(task) = owner.task.take() {
                let _ = task.await;
            }
        }
    }
}
fn new_feed<S>(key: String, spec: S, changed: Arc<Notify>) -> (Arc<Feed<S>>, RealtimeReceiver) {
    let (sender, receiver) = broadcast::channel(QUEUE);
    let delivery = Arc::new(DeliveryState::default());
    let receiver = RealtimeReceiver::new(receiver, delivery.clone(), changed);
    (
        Arc::new(Feed {
            key,
            spec,
            sender,
            delivery,
            latest: Mutex::new(None),
        }),
        receiver,
    )
}
fn desired<S>(control: &Mutex<Control<S>>) -> Vec<Arc<Feed<S>>> {
    let mut control = control.lock();
    control.feeds.retain(|_, f| f.demanded());
    if control.feeds.is_empty() {
        control.accepting = false;
    }
    control.feeds.values().cloned().collect()
}
async fn run<P: Protocol>(
    url: String,
    protocol: Arc<P>,
    control: Arc<Mutex<Control<P::Spec>>>,
    changed: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(250);
    loop {
        if *stop.borrow() || desired(&control).is_empty() {
            return;
        }
        // Briefly coalesce concurrent demand, not one subscription request per client.
        tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(TICK) => {} }
        let started = tokio::time::Instant::now();
        let result = tokio::select! {
            _ = stop.changed() => return,
            result = session(&url, protocol.session(), &control, &changed) => result,
        };
        let Err(error) = result else {
            return;
        };
        for feed in desired(&control) {
            feed.invalidate(&error);
        }
        if started.elapsed() >= Duration::from_secs(30) {
            backoff = Duration::from_millis(250);
        }
        let deadline = tokio::time::Instant::now() + backoff;
        loop {
            if *stop.borrow() || desired(&control).is_empty() {
                return;
            }
            tokio::select! {
                _ = stop.changed() => return,
                _ = changed.notified() => {},
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}
async fn session<S: Send + Sync, T: Session<S>>(
    url: &str,
    mut protocol: T,
    control: &Mutex<Control<S>>,
    changed: &Notify,
) -> Result<(), String> {
    let config = WebSocketConfig {
        max_message_size: Some(4 * 1024 * 1024),
        max_frame_size: Some(4 * 1024 * 1024),
        ..Default::default()
    };
    let connecting = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_with_config(url, Some(config), false),
    );
    tokio::pin!(connecting);
    let (mut socket, _) = loop {
        tokio::select! {
            _ = changed.notified() => { if desired(control).is_empty() { return Ok(()); } },
            result = &mut connecting => break result.map_err(|_| "native websocket connection timeout".to_string())?.map_err(|e| e.to_string())?,
        }
    };
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = changed.notified() => {
                if desired(control).is_empty() {
                    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
                    return Ok(());
                }
            },
            _ = tick.tick() => {
                let feeds = desired(control);
                if feeds.is_empty() { return Ok(()); }
                if let Some(text) = protocol.reconcile(feeds)? {
                    tokio::time::timeout(Duration::from_secs(5), socket.send(Message::Text(text))).await
                        .map_err(|_| "native websocket write timeout".to_string())?.map_err(|e| e.to_string())?;
                }
            },
            message = socket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => protocol.message(&text)?,
                    Some(Ok(Message::Ping(payload))) => {
                        tokio::time::timeout(
                            Duration::from_secs(5),
                            socket.send(Message::Pong(payload)),
                        )
                        .await
                        .map_err(|_| "pong timeout".to_string())?
                        .map_err(|e| e.to_string())?;
                    },
                    Some(Ok(Message::Close(_))) | None => return Err("native websocket closed; resynchronizing".into()),
                    Some(Err(e)) => return Err(e.to_string()),
                    Some(Ok(Message::Binary(_))) => return Err("unexpected native binary frame".into()),
                    _ => {},
                }
            }
        }
    }
}
