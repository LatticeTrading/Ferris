use std::{
    cmp::Ordering as CmpOrdering,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::extract::ws::Message;
use serde::Serialize;
use tokio::{
    sync::{mpsc::Sender, watch, Notify},
    task::JoinHandle,
    time::{sleep_until, Instant},
};

use crate::{
    market_stats::{project_snapshot, MarketStatsSourceSnapshot},
    models::{
        MarketStatsCoverage, MarketStatsField, MarketStatsFieldName, MarketStatsRow,
        MarketStatsScope, MarketStatsSnapshot, MarketStatsTopic, UnifiedMarket,
    },
};

const MIN_EMISSION_INTERVAL: Duration = Duration::from_secs(1);
static PROCESS_GENERATION: OnceLock<u128> = OnceLock::new();
static SUBSCRIPTION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_generation() -> String {
    let process = PROCESS_GENERATION.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    });
    let subscription = SUBSCRIPTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{process}-{subscription}")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotMessage<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    mode: &'static str,
    topic: &'a MarketStatsTopic,
    generation: &'a str,
    revision: u64,
    #[serde(flatten)]
    snapshot: &'a MarketStatsSnapshot,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaMessage<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    mode: &'static str,
    topic: &'a MarketStatsTopic,
    generation: &'a str,
    previous_revision: u64,
    revision: u64,
    timestamp: u64,
    scope: &'a MarketStatsScope,
    #[serde(flatten)]
    changes: ProjectionChanges<'a>,
    coverage: &'a MarketStatsCoverage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectionChanges<'a> {
    updates: Vec<RowUpdate<'a>>,
    removed_market_ids: Vec<&'a str>,
}

#[derive(Serialize)]
struct RowUpdate<'a> {
    #[serde(flatten)]
    market: &'a UnifiedMarket,
    fields: BTreeMap<MarketStatsFieldName, &'a MarketStatsField>,
}

fn market_id(row: &MarketStatsRow) -> &str {
    // project_snapshot emits only identified rows, sorted by this opaque ID.
    &row.market
        .identity
        .as_ref()
        .expect("projected market identity")
        .market_id
}

fn row_update<'a>(
    previous: Option<&MarketStatsRow>,
    current: &'a MarketStatsRow,
) -> Option<RowUpdate<'a>> {
    let full = previous.is_none_or(|row| row.market != current.market);
    let fields: BTreeMap<_, _> = current
        .fields
        .iter()
        .filter_map(|(name, field)| {
            (full || previous.and_then(|row| row.fields.get(name)) != Some(field))
                .then_some((*name, field))
        })
        .collect();
    (full || !fields.is_empty()).then_some(RowUpdate {
        market: &current.market,
        fields,
    })
}

fn projection_changes<'a>(
    previous: &'a MarketStatsSnapshot,
    current: &'a MarketStatsSnapshot,
) -> Option<ProjectionChanges<'a>> {
    let mut updates = Vec::new();
    let mut removed_market_ids = Vec::new();
    let mut old_rows = previous.markets.iter().peekable();
    let mut new_rows = current.markets.iter().peekable();
    // Merge already-sorted projections without a second identity index or cloned rows.
    while let (Some(old), Some(new)) = (old_rows.peek().copied(), new_rows.peek().copied()) {
        match market_id(old).cmp(market_id(new)) {
            CmpOrdering::Less => {
                removed_market_ids.push(market_id(old));
                old_rows.next();
            }
            CmpOrdering::Greater => {
                updates.push(row_update(None, new).expect("new row update"));
                new_rows.next();
            }
            CmpOrdering::Equal => {
                if let Some(update) = row_update(Some(old), new) {
                    updates.push(update);
                }
                old_rows.next();
                new_rows.next();
            }
        }
    }
    removed_market_ids.extend(old_rows.map(market_id));
    updates.extend(new_rows.map(|row| row_update(None, row).expect("new row update")));
    if updates.is_empty()
        && removed_market_ids.is_empty()
        && previous.coverage == current.coverage
        && previous.scope == current.scope
    {
        None
    } else {
        Some(ProjectionChanges {
            updates,
            removed_market_ids,
        })
    }
}

fn request_close(close_signal: &Notify, force_close: &AtomicBool) {
    force_close.store(true, Ordering::Release);
    close_signal.notify_one();
}

pub(super) fn spawn_market_stats_forwarder(
    topic: MarketStatsTopic,
    mut receiver: watch::Receiver<Arc<MarketStatsSourceSnapshot>>,
    outgoing_sender: Sender<Message>,
    close_signal: Arc<Notify>,
    force_close: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let generation = next_generation();
    tokio::spawn(async move {
        // The socket owner has enqueued its acknowledgement before spawning us.
        let mut previous = project_snapshot(&topic, &receiver.borrow_and_update());
        let mut revision = 1;
        if !super::send_ws_json(
            &outgoing_sender,
            &SnapshotMessage {
                message_type: "marketstats",
                mode: "snapshot",
                topic: &topic,
                generation: &generation,
                revision,
                snapshot: &previous,
            },
        ) {
            request_close(&close_signal, &force_close);
            return;
        }
        let mut next_emission = Instant::now() + MIN_EMISSION_INTERVAL;

        while receiver.changed().await.is_ok() {
            // Watch holds complete states. Wait first, then borrow the latest one: a
            // sparse delta is never discarded to coalesce a newer observation.
            sleep_until(next_emission).await;
            let current = project_snapshot(&topic, &receiver.borrow_and_update());
            let Some(changes) = projection_changes(&previous, &current) else {
                continue;
            };
            let next_revision = revision + 1;
            if !super::send_ws_json(
                &outgoing_sender,
                &DeltaMessage {
                    message_type: "marketstats",
                    mode: "delta",
                    topic: &topic,
                    generation: &generation,
                    previous_revision: revision,
                    revision: next_revision,
                    timestamp: current.timestamp,
                    scope: &current.scope,
                    changes,
                    coverage: &current.coverage,
                },
            ) {
                request_close(&close_signal, &force_close);
                return;
            }
            previous = current;
            revision = next_revision;
            next_emission = Instant::now() + MIN_EMISSION_INTERVAL;
        }
        // Lease ownership stays with the socket subscription map on every exit.
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{json, Value};
    use tokio::{
        sync::mpsc,
        task::yield_now,
        time::{advance, timeout},
    };

    use super::*;
    use crate::{
        market_stats::make_market_id,
        models::{
            FundingKind, FundingValue, MarketIdentity, MarketStatsFieldState,
            MarketStatsSourceFailure, MarketStatsValue, PriceValue, UnifiedMarketInfo,
            UnifiedMarketType,
        },
    };

    fn row(name: &str) -> MarketStatsRow {
        MarketStatsRow {
            market: UnifiedMarket {
                exchange: "hyperliquid".into(),
                symbol: format!("{name}/USDC:USDC"),
                base: name.into(),
                quote: "USDC".into(),
                market_type: UnifiedMarketType::Perp,
                active: true,
                min_order_size: None,
                tick_size: None,
                contract_size: None,
                info: UnifiedMarketInfo::default(),
                identity: Some(MarketIdentity {
                    market_id: make_market_id(
                        "hyperliquid",
                        UnifiedMarketType::Perp,
                        None,
                        Some(""),
                        name,
                    )
                    .unwrap(),
                    exchange_market_id: name.into(),
                    category: None,
                    dex: Some(String::new()),
                    contract_type: None,
                    settle: Some("USDC".into()),
                    settlement_asset_id: Some("usdc-token".into()),
                }),
            },
            fields: BTreeMap::from([
                (
                    MarketStatsFieldName::Funding,
                    MarketStatsField {
                        state: MarketStatsFieldState::Available,
                        value: Some(MarketStatsValue::Funding(FundingValue::new(
                            "0.0001".into(),
                            crate::models::FundingRateUnit::DecimalFraction,
                            FundingKind::CurrentUnclassified,
                            Some(3_600_000),
                            Some(3_600_000),
                            None,
                            None,
                        ))),
                        reason: None,
                        exchange_timestamp: None,
                        received_timestamp: Some(100),
                        source: Some("hyperliquid:primary:metaAndAssetCtxs".into()),
                    },
                ),
                (
                    MarketStatsFieldName::MarkPrice,
                    MarketStatsField {
                        state: MarketStatsFieldState::Available,
                        value: Some(MarketStatsValue::Price(PriceValue {
                            amount: "100".into(),
                            base_asset: name.into(),
                            quote_asset: "USDT".into(),
                        })),
                        reason: None,
                        exchange_timestamp: None,
                        received_timestamp: Some(100),
                        source: Some("hyperliquid:primary:metaAndAssetCtxs".into()),
                    },
                ),
            ]),
        }
    }

    fn source(rows: Vec<MarketStatsRow>) -> MarketStatsSourceSnapshot {
        MarketStatsSourceSnapshot {
            rows,
            perp_catalog_known: true,
            perp_enumeration_complete: true,
            spot_enumeration_complete: true,
            contexts_valid: true,
            received_at: Some(Instant::now()),
            field_received_at: Default::default(),
            next_poll_at: Instant::now() + Duration::from_secs(30),
            source_failures: Vec::new(),
        }
    }

    fn topic() -> MarketStatsTopic {
        MarketStatsTopic {
            exchange: "hyperliquid".into(),
            params: json!({"dex": ""}),
            market_ids: None,
            fields: vec![
                MarketStatsFieldName::Funding,
                MarketStatsFieldName::MarkPrice,
            ],
        }
    }

    struct Stream {
        source: watch::Sender<Arc<MarketStatsSourceSnapshot>>,
        outgoing: mpsc::Receiver<Message>,
        closed: Arc<Notify>,
        force_close: Arc<AtomicBool>,
        task: JoinHandle<()>,
    }

    impl Stream {
        fn new(
            topic: MarketStatsTopic,
            initial: MarketStatsSourceSnapshot,
            capacity: usize,
        ) -> Self {
            let (source, receiver) = watch::channel(Arc::new(initial));
            let (sender, outgoing) = mpsc::channel(capacity);
            let closed = Arc::new(Notify::new());
            let force_close = Arc::new(AtomicBool::new(false));
            let task = spawn_market_stats_forwarder(
                topic,
                receiver,
                sender,
                closed.clone(),
                force_close.clone(),
            );
            Self {
                source,
                outgoing,
                closed,
                force_close,
                task,
            }
        }

        fn publish(&self, latest: &MarketStatsSourceSnapshot) {
            self.source.send_replace(Arc::new(latest.clone()));
        }

        async fn receive(&mut self) -> Value {
            let message = timeout(Duration::from_secs(2), self.outgoing.recv())
                .await
                .expect("statistics message deadline")
                .expect("statistics stream closed");
            let Message::Text(text) = message else {
                panic!("expected JSON text")
            };
            serde_json::from_str(&text).unwrap()
        }

        async fn stop(self) {
            self.task.abort();
            let _ = self.task.await;
        }
    }

    // A consumer of JSON envelopes, not the implementation's diff helper. Each
    // field object replaces its prior object atomically; missing fields survive.
    #[derive(Default)]
    struct Consumer {
        generation: String,
        revision: u64,
        rows: BTreeMap<String, MarketStatsRow>,
        scope: Option<MarketStatsScope>,
        coverage: Option<MarketStatsCoverage>,
    }

    impl Consumer {
        fn apply(&mut self, message: &Value) -> bool {
            assert_eq!(message["type"], "marketstats");
            if message["mode"] == "snapshot" {
                assert_eq!(message["revision"], 1);
                let snapshot: MarketStatsSnapshot =
                    serde_json::from_value(message.clone()).unwrap();
                self.generation = message["generation"].as_str().unwrap().to_owned();
                self.revision = 1;
                self.rows = snapshot
                    .markets
                    .into_iter()
                    .map(|row| (market_id(&row).to_owned(), row))
                    .collect();
                self.scope = Some(snapshot.scope);
                self.coverage = Some(snapshot.coverage);
            } else {
                assert_eq!(message["mode"], "delta");
                if message["generation"] != self.generation
                    || message["previousRevision"] != self.revision
                    || message["revision"] != self.revision + 1
                {
                    return false;
                }
                for id in message["removedMarketIds"].as_array().unwrap() {
                    self.rows.remove(id.as_str().unwrap());
                }
                for update in message["updates"].as_array().unwrap() {
                    let mut row: MarketStatsRow = serde_json::from_value(update.clone()).unwrap();
                    let id = market_id(&row).to_owned();
                    if let Some(mut previous) = self.rows.remove(&id) {
                        previous.fields.extend(row.fields);
                        row.fields = previous.fields;
                    }
                    self.rows.insert(id, row);
                }
                self.revision += 1;
                self.scope = Some(serde_json::from_value(message["scope"].clone()).unwrap());
                self.coverage = Some(serde_json::from_value(message["coverage"].clone()).unwrap());
            }
            true
        }

        fn assert_matches(&self, topic: &MarketStatsTopic, source: &MarketStatsSourceSnapshot) {
            let expected = project_snapshot(topic, source);
            assert_eq!(
                self.rows.values().cloned().collect::<Vec<_>>(),
                expected.markets
            );
            assert_eq!(self.scope.as_ref(), Some(&expected.scope));
            assert_eq!(self.coverage.as_ref(), Some(&expected.coverage));
        }
    }

    fn assert_keys(value: &Value, expected: &[&str]) {
        let actual: BTreeSet<_> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(actual, expected.iter().copied().collect());
    }

    async fn advance_and_run(duration: Duration) {
        yield_now().await;
        advance(duration).await;
        yield_now().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_coalesces_complete_states_without_losing_fields_or_removals() {
        let topic = topic();
        let mut latest = source(vec![row("BTC"), row("ETH"), row("XRP"), row("ZEC")]);
        let mut stream = Stream::new(topic.clone(), latest.clone(), 8);
        let mut consumer = Consumer::default();
        let initial = stream.receive().await;
        assert!(consumer.apply(&initial));
        consumer.assert_matches(&topic, &latest);
        assert_keys(
            &initial,
            &[
                "coverage",
                "generation",
                "markets",
                "mode",
                "revision",
                "scope",
                "timestamp",
                "topic",
                "type",
            ],
        );

        // Funding clears in one state, another row's mark changes in the next,
        // then two removals and a new row arrive before the one-second boundary.
        let funding = latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap();
        funding.state = MarketStatsFieldState::Unavailable;
        funding.value = None;
        funding.reason = Some("invalid-upstream-value".into());
        funding.received_timestamp = Some(200);
        stream.publish(&latest);
        yield_now().await;
        advance_and_run(Duration::from_millis(200)).await;
        latest.rows[1]
            .fields
            .get_mut(&MarketStatsFieldName::MarkPrice)
            .unwrap()
            .value = Some(MarketStatsValue::Price(PriceValue {
            amount: "220".into(),
            base_asset: "ETH".into(),
            quote_asset: "USDT".into(),
        }));
        stream.publish(&latest);
        yield_now().await;
        advance_and_run(Duration::from_millis(200)).await;
        let removed = vec![
            market_id(&latest.rows[2]).to_owned(),
            market_id(&latest.rows[3]).to_owned(),
        ];
        latest.rows.truncate(2);
        latest.rows.push(row("ADA"));
        stream.publish(&latest);
        advance_and_run(Duration::from_millis(599)).await;
        assert!(matches!(
            stream.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        advance_and_run(Duration::from_millis(1)).await;
        let delta = stream.receive().await;
        assert_eq!(delta["previousRevision"], 1);
        assert_eq!(delta["revision"], 2);
        assert_eq!(delta["removedMarketIds"], json!(removed));
        assert_keys(
            &delta,
            &[
                "coverage",
                "generation",
                "mode",
                "previousRevision",
                "removedMarketIds",
                "revision",
                "scope",
                "timestamp",
                "topic",
                "type",
                "updates",
            ],
        );
        let btc_update = delta["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["exchangeMarketId"] == "BTC")
            .unwrap();
        assert_keys(&btc_update["fields"], &["funding"]);
        assert!(btc_update["fields"]["funding"]["value"].is_null());
        assert!(consumer.apply(&delta));
        consumer.assert_matches(&topic, &latest);

        // A later failure does not remove membership or restore the cleared rate.
        latest.perp_enumeration_complete = false;
        latest.source_failures.push(MarketStatsSourceFailure {
            source: "hyperliquid:primary:metaAndAssetCtxs".into(),
            reason: "upstream-failure".into(),
            message: "offline".into(),
        });
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let failure = stream.receive().await;
        assert_eq!(failure["removedMarketIds"], json!([]));
        assert_eq!(failure["updates"], json!([]));
        assert!(consumer.apply(&failure));
        consumer.assert_matches(&topic, &latest);
        stream.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_observes_receipts_and_coverage_but_not_capture_time() {
        let topic = topic();
        let mut latest = source(vec![row("BTC")]);
        let mut stream = Stream::new(topic.clone(), latest.clone(), 8);
        let mut consumer = Consumer::default();
        assert!(consumer.apply(&stream.receive().await));
        let mut same = project_snapshot(&topic, &latest);
        let mut recaptured = same.clone();
        recaptured.timestamp = same.timestamp + 10;
        assert!(projection_changes(&same, &recaptured).is_none());
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        assert!(matches!(
            stream.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap()
            .received_timestamp = Some(101);
        stream.publish(&latest);
        let receipt = stream.receive().await;
        assert_eq!(
            receipt["updates"][0]["fields"]["funding"]["value"]["rate"],
            "0.0001"
        );
        assert_eq!(
            receipt["updates"][0]["fields"]["funding"]["receivedTimestamp"],
            101
        );
        assert!(receipt["updates"][0]["fields"].get("markPrice").is_none());
        assert!(consumer.apply(&receipt));
        consumer.assert_matches(&topic, &latest);

        latest.source_failures.push(MarketStatsSourceFailure {
            source: "hyperliquid:spotMeta".into(),
            reason: "settlement-unresolved".into(),
            message: "unknown collateral token".into(),
        });
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let coverage = stream.receive().await;
        assert_eq!(coverage["updates"], json!([]));
        assert_eq!(coverage["removedMarketIds"], json!([]));
        assert!(consumer.apply(&coverage));
        consumer.assert_matches(&topic, &latest);

        latest.source_failures.clear();
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        assert!(consumer.apply(&stream.receive().await));
        consumer.assert_matches(&topic, &latest);
        // Even an otherwise-empty scope transition is observable, unlike capture time.
        same.scope.params = json!({"dex": "other"});
        assert!(projection_changes(&same, &recaptured).is_some());
        stream.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_identity_and_active_changes_replace_all_requested_fields() {
        let mut latest = source(vec![row("BTC")]);
        let mut selected = topic();
        selected.market_ids = Some(vec![market_id(&latest.rows[0]).to_owned()]);
        let mut stream = Stream::new(selected.clone(), latest.clone(), 8);
        let mut consumer = Consumer::default();
        assert!(consumer.apply(&stream.receive().await));

        latest.rows[0].market.identity.as_mut().unwrap().settle = None;
        latest.rows[0].market.tick_size = Some(0.1);
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let identity = stream.receive().await;
        assert_eq!(
            identity["updates"][0]["fields"],
            serde_json::to_value(&latest.rows[0].fields).unwrap()
        );
        assert!(identity["updates"][0]["settle"].is_null());
        assert!(consumer.apply(&identity));
        consumer.assert_matches(&selected, &latest);

        latest.rows[0].market.active = false;
        latest.rows[0].market.tick_size = None;
        for field in latest.rows[0].fields.values_mut() {
            field.state = MarketStatsFieldState::Unavailable;
            field.value = None;
            field.reason = Some("inactive-market".into());
        }
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let inactive = stream.receive().await;
        assert_eq!(inactive["removedMarketIds"], json!([]));
        assert_eq!(
            inactive["updates"][0]["fields"],
            serde_json::to_value(&latest.rows[0].fields).unwrap()
        );
        assert!(consumer.apply(&inactive));
        consumer.assert_matches(&selected, &latest);

        latest.rows.clear();
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let absent = stream.receive().await;
        assert_eq!(absent["removedMarketIds"], json!(selected.market_ids));
        assert!(consumer.apply(&absent));
        consumer.assert_matches(&selected, &latest);
        stream.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_resubscribe_replaces_generation_and_rejects_gapped_deltas() {
        let topic = topic();
        let mut latest = source(vec![row("BTC")]);
        let mut first = Stream::new(topic.clone(), latest.clone(), 8);
        let mut consumer = Consumer::default();
        let first_snapshot = first.receive().await;
        assert!(consumer.apply(&first_snapshot));
        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap()
            .received_timestamp = Some(200);
        first.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let delta = first.receive().await;
        let mut gap = delta.clone();
        gap["previousRevision"] = json!(2);
        gap["revision"] = json!(3);
        assert!(!consumer.apply(&gap));
        assert_eq!(consumer.revision, 1);
        assert!(consumer.apply(&delta));
        consumer.assert_matches(&topic, &latest);
        first.stop().await;

        let mut replacement = Stream::new(topic.clone(), latest.clone(), 8);
        let replacement_snapshot = replacement.receive().await;
        assert_ne!(
            first_snapshot["generation"],
            replacement_snapshot["generation"]
        );
        assert_eq!(replacement_snapshot["revision"], 1);
        assert!(consumer.apply(&replacement_snapshot));
        assert!(!consumer.apply(&delta));
        consumer.assert_matches(&topic, &latest);
        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::MarkPrice)
            .unwrap()
            .received_timestamp = Some(300);
        replacement.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        let replacement_delta = replacement.receive().await;
        assert_eq!(replacement_delta["previousRevision"], 1);
        assert_eq!(replacement_delta["revision"], 2);
        assert!(consumer.apply(&replacement_delta));
        consumer.assert_matches(&topic, &latest);
        replacement.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_initial_snapshot_uses_latest_unseen_source_state() {
        let topic = topic();
        let mut latest = source(vec![row("BTC")]);
        let mut stream = Stream::new(topic.clone(), latest.clone(), 8);
        // No await: these complete states replace the watch value before startup.
        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap()
            .received_timestamp = Some(200);
        stream.publish(&latest);
        latest.rows.push(row("ETH"));
        stream.publish(&latest);
        let snapshot = stream.receive().await;
        let mut consumer = Consumer::default();
        assert_eq!(snapshot["mode"], "snapshot");
        assert!(consumer.apply(&snapshot));
        consumer.assert_matches(&topic, &latest);
        advance_and_run(Duration::from_secs(1)).await;
        assert!(matches!(
            stream.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        stream.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_full_delta_queue_forces_close_without_continuing_chain() {
        let mut latest = source(vec![row("BTC")]);
        let mut stream = Stream::new(topic(), latest.clone(), 1);
        let snapshot = stream.receive().await;
        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap()
            .received_timestamp = Some(200);
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        // Leave revision 2 queued; the next delta cannot be enqueued.
        latest.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::MarkPrice)
            .unwrap()
            .received_timestamp = Some(300);
        stream.publish(&latest);
        advance_and_run(Duration::from_secs(1)).await;
        timeout(Duration::from_secs(1), stream.closed.notified())
            .await
            .expect("close notification");
        assert!(stream.force_close.load(Ordering::Acquire));
        let delta = stream.receive().await;
        let mut consumer = Consumer::default();
        assert!(consumer.apply(&snapshot));
        assert!(consumer.apply(&delta));
        assert_eq!(consumer.revision, 2);
        stream.task.await.unwrap();
        assert!(matches!(
            stream.outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        // Reconnect recovers every change through a replacement full projection.
        let mut replacement = Stream::new(topic(), latest.clone(), 1);
        let fresh = replacement.receive().await;
        assert_ne!(snapshot["generation"], fresh["generation"]);
        assert!(consumer.apply(&fresh));
        consumer.assert_matches(&topic(), &latest);
        replacement.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn market_stats_stream_full_initial_queue_forces_close_before_any_delta() {
        let (_source, receiver) = watch::channel(Arc::new(source(vec![row("BTC")])));
        let (sender, mut outgoing) = mpsc::channel(1);
        sender
            .try_send(Message::Text("acknowledgement".into()))
            .unwrap();
        let closed = Arc::new(Notify::new());
        let force_close = Arc::new(AtomicBool::new(false));
        let task = spawn_market_stats_forwarder(
            topic(),
            receiver,
            sender,
            closed.clone(),
            force_close.clone(),
        );
        timeout(Duration::from_secs(1), closed.notified())
            .await
            .expect("close notification");
        assert!(force_close.load(Ordering::Acquire));
        task.await.unwrap();
        assert!(
            matches!(outgoing.recv().await, Some(Message::Text(text)) if text == "acknowledgement")
        );
        assert!(outgoing.recv().await.is_none());
    }
}
