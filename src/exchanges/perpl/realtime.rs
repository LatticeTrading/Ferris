use super::{
    wire::{self, Spec, State},
    PerplExchange,
};
use crate::{
    exchanges::traits::ExchangeError,
    realtime::{
        native::{Feed, Protocol, Session, SharedHub},
        NativeRealtimeOwner, NativeSubscriptionPlan, RealtimeChannel, RealtimeSubscription,
        RealtimeTopic,
    },
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

struct Plan {
    hub: Arc<SharedHub<PerplProtocol>>,
    spec: Spec,
    topic: RealtimeTopic,
    feed_key: String,
    client_key: String,
    levels: usize,
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
            receiver,
        })
    }
}
pub(super) fn resolution(tf: &str) -> Result<u64, ExchangeError> {
    Ok(match tf {
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "1h" => 3600,
        "2h" => 7200,
        "4h" => 14400,
        "8h" => 28800,
        "12h" => 43200,
        "1d" => 86400,
        _ => {
            return Err(ExchangeError::UnsupportedFeature(format!(
                "unsupported Perpl timeframe `{tf}`"
            )))
        }
    })
}
#[async_trait]
impl NativeRealtimeOwner for PerplExchange {
    fn exchange_id(&self) -> &'static str {
        "perpl"
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
        if topic.exchange != "perpl" {
            return Err(ExchangeError::BadSymbol("wrong native owner".into()));
        }
        let allowed: &[&str] = match channel {
            RealtimeChannel::Trades => &[],
            RealtimeChannel::OrderBook => &["levels", "depth", "limit"],
            RealtimeChannel::Ohlcv => &["timeframe", "interval"],
        };
        let params = topic.params.as_object().expect("normalized params");
        if let Some(key) = params.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(ExchangeError::BadSymbol(format!(
                "unsupported Perpl realtime parameter `{key}`"
            )));
        }
        let mut value: Option<&Value> = None;
        for key in allowed {
            if let Some(v) = params.get(*key) {
                if value.is_some_and(|old| old != v) {
                    return Err(ExchangeError::BadSymbol(
                        "conflicting Perpl parameter aliases".into(),
                    ));
                }
                value = Some(v);
            }
        }
        let levels = if channel == RealtimeChannel::OrderBook {
            let n = value
                .map(|v| {
                    v.as_u64().ok_or_else(|| {
                        ExchangeError::BadSymbol("Perpl depth must be an integer".into())
                    })
                })
                .transpose()?
                .unwrap_or(100);
            if !(1..=100).contains(&n) {
                return Err(ExchangeError::BadSymbol("Perpl depth must be 1–100".into()));
            }
            n as usize
        } else {
            usize::MAX
        };
        let timeframe = if channel == RealtimeChannel::Ohlcv {
            Some(
                value
                    .map(|v| {
                        v.as_str().ok_or_else(|| {
                            ExchangeError::BadSymbol("Perpl timeframe must be a string".into())
                        })
                    })
                    .transpose()?
                    .unwrap_or("1m")
                    .to_string(),
            )
        } else {
            None
        };
        let resolution = timeframe.as_deref().map(resolution).transpose()?;
        let market = self.resolve(&topic.symbol).await?;
        if !market.config.is_open {
            return Err(ExchangeError::BadSymbol("Perpl market is inactive".into()));
        }
        let stream = match channel {
            RealtimeChannel::Trades => format!("trades@{}", market.id),
            RealtimeChannel::OrderBook => format!("order-book@{}", market.id),
            RealtimeChannel::Ohlcv => format!("candles@{}*{}", market.id, resolution.unwrap()),
        };
        let feed_key =
            json!([self.network_key, market.id, channel.as_str(), resolution]).to_string();
        let client_key = json!([feed_key, levels]).to_string();
        topic.symbol = format!("{}/AUSD:AUSD", market.name);
        topic.params = match channel {
            RealtimeChannel::OrderBook => json!({"levels":levels}),
            RealtimeChannel::Ohlcv => json!({"timeframe":timeframe}),
            _ => json!({}),
        };
        Ok(Box::new(Plan {
            hub: self.live.clone(),
            spec: Spec {
                market,
                chain: self.chain_id,
                stream,
                channel,
                resolution,
            },
            topic,
            feed_key,
            client_key,
            levels,
        }))
    }
    async fn shutdown_native(&self) -> Result<(), ExchangeError> {
        self.live.shutdown().await;
        Ok(())
    }
}
pub(super) struct PerplProtocol;
impl Protocol for PerplProtocol {
    type Spec = Spec;
    type Session = PerplSession;
    fn capacity(&self) -> usize {
        16
    }
    fn session(&self) -> PerplSession {
        PerplSession::default()
    }
}
#[derive(Default)]
struct Budget(VecDeque<Instant>);
impl Budget {
    fn take(&mut self, now: Instant) -> bool {
        while self
            .0
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            self.0.pop_front();
        }
        if self.0.len() >= 10 {
            false
        } else {
            self.0.push_back(now);
            true
        }
    }
}
#[derive(Clone, Copy)]
enum Phase {
    Subscribing(Instant),
    Active,
    Retire,
    Unsubscribing(Instant),
    Failed,
}
struct Active {
    feed: Arc<Feed<Spec>>,
    sid: Option<u64>,
    phase: Phase,
    state: State,
    snapshot_due: Option<Instant>,
}
#[derive(Default)]
pub(super) struct PerplSession {
    active: HashMap<String, Active>,
    budget: Budget,
}
impl Session<Spec> for PerplSession {
    fn reconcile(&mut self, desired: Vec<Arc<Feed<Spec>>>) -> Result<Option<String>, String> {
        let now = Instant::now();
        for active in self.active.values_mut() {
            if matches!(active.phase,Phase::Subscribing(t)|Phase::Unsubscribing(t) if now.duration_since(t)>Duration::from_secs(10))
            {
                return Err("Perpl subscription acknowledgement timed out".into());
            }
            let wanted = desired.iter().any(|f| Arc::ptr_eq(f, &active.feed));
            if matches!(active.phase, Phase::Active)
                && (!wanted || active.snapshot_due.is_some_and(|t| now >= t))
            {
                if wanted {
                    active
                        .feed
                        .invalidate("Perpl snapshot timed out; resubscribing");
                }
                active.phase = Phase::Retire;
                active.state = State::default();
            }
        }
        self.active.retain(|_, a| {
            !matches!(a.phase, Phase::Failed) || desired.iter().any(|f| Arc::ptr_eq(f, &a.feed))
        });
        let mut remove = self
            .active
            .iter()
            .filter(|(_, a)| matches!(a.phase, Phase::Retire))
            .map(|(s, _)| s.clone())
            .collect::<Vec<_>>();
        remove.sort();
        let mut add = desired
            .into_iter()
            .filter(|f| !self.active.contains_key(&f.spec.stream))
            .collect::<Vec<_>>();
        add.sort_by(|a, b| a.key.cmp(&b.key));
        // Retiring and pending entries continue to occupy slots until ACK. No
        // reliance on unspecified ordering of mixed unsubscribe/subscribe batches.
        let slots = 16usize.saturating_sub(self.active.len());
        add.truncate(slots);
        if remove.is_empty() && add.is_empty() {
            return Ok(None);
        }
        if !self.budget.take(now) {
            return Ok(None);
        }
        let mut subs = vec![];
        for stream in remove {
            self.active.get_mut(&stream).unwrap().phase = Phase::Unsubscribing(now);
            subs.push(json!({"stream":stream,"subscribe":false}));
        }
        for feed in add {
            let stream = feed.spec.stream.clone();
            subs.push(json!({"stream":stream,"subscribe":true}));
            self.active.insert(
                stream,
                Active {
                    feed,
                    sid: None,
                    phase: Phase::Subscribing(now),
                    state: State::default(),
                    snapshot_due: None,
                },
            );
        }
        Ok(Some(json!({"mt":5,"subs":subs}).to_string()))
    }
    fn message(&mut self, text: &str) -> Result<(), String> {
        let v: Value =
            serde_json::from_str(text).map_err(|e| format!("invalid Perpl JSON: {e}"))?;
        let mt = wire::uint(&v, "mt")?;
        if mt == 6 {
            let subs = v
                .get("subs")
                .and_then(Value::as_array)
                .ok_or("missing Perpl subscription results")?;
            for entry in subs {
                let stream = entry
                    .get("stream")
                    .and_then(Value::as_str)
                    .ok_or("missing Perpl response stream")?;
                let code = entry
                    .get("status")
                    .map(|s| wire::uint(s, "code"))
                    .transpose()?
                    .unwrap_or(0);
                let Some(a) = self.active.get_mut(stream) else {
                    return Err("unmatched Perpl subscription response".into());
                };
                if matches!(a.phase, Phase::Unsubscribing(_)) {
                    if code != 0 {
                        return Err(format!("Perpl unsubscribe failed: {entry}"));
                    }
                    self.active.remove(stream);
                    continue;
                }
                if !matches!(a.phase, Phase::Subscribing(_)) {
                    return Err("unexpected Perpl subscription response".into());
                }
                if code != 0 {
                    a.feed
                        .invalidate(&format!("Perpl subscription failed ({code}): {entry}"));
                    // The entry failure is not a connection failure. Keep the
                    // healthy session alive, but release this failed stream so
                    // a fresh downstream lease can retry it explicitly.
                    a.phase = Phase::Failed;
                    continue;
                }
                let sid = wire::uint(entry, "sid")?;
                a.sid = Some(sid);
                a.phase = Phase::Active;
                a.snapshot_due = Some(Instant::now() + Duration::from_secs(10));
                if self.active.values().filter(|a| a.sid == Some(sid)).count() != 1 {
                    return Err("duplicate Perpl sid".into());
                }
            }
            return Ok(());
        }
        if matches!(mt, 2 | 100) {
            return Ok(());
        }
        if mt == 3 {
            return Err(format!("Perpl request status: {v}"));
        }
        let sid = wire::uint(&v, "sid")?;
        let Some(a) = self.active.values_mut().find(|a| a.sid == Some(sid)) else {
            // Late data for an acknowledged retired subscription is not demand.
            return Ok(());
        };
        if !matches!(a.phase, Phase::Active) {
            return Ok(());
        }
        match a.state.apply(&a.feed.spec, &v) {
            Ok(Some(update)) => {
                a.snapshot_due = None;
                if matches!(a.feed.spec.channel, RealtimeChannel::Trades)
                    && matches!(wire::uint(&v, "mt"), Ok(18))
                {
                    a.feed.publish_update(update);
                } else {
                    a.feed.publish(update);
                }
            }
            Ok(None) => {}
            Err(error) => {
                a.feed.invalidate(&error);
                a.phase = Phase::Retire;
                a.state = State::default(); // No delta can revive an invalid book.
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn rolling_budget_is_ten_requests_not_ten_streams() {
        let mut b = Budget::default();
        let start = Instant::now();
        for _ in 0..10 {
            assert!(b.take(start));
        }
        assert!(!b.take(start + Duration::from_secs(59)));
        assert!(b.take(start + Duration::from_secs(60)));
    }
}
