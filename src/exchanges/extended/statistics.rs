use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};

use super::ExtendedExchange;
use crate::{
    exchanges::traits::{ExchangeError, MarketStatsSource},
    market_stats::MarketStatsSourceSnapshot,
    models::{
        CapabilityState, FeatureCapability, FetchMarketStatsParams, FundingKind, FundingRateUnit,
        FundingValue, MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSourceFailure,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability, PriceValue,
        UnifiedMarket, UnifiedMarketType,
    },
};

const SOURCE: &str = "extended:info/markets";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const FUNDING_INTERVAL_MS: u64 = 3_600_000;
const FIELDS: [MarketStatsFieldName; 7] = [
    MarketStatsFieldName::Funding,
    MarketStatsFieldName::LastSettledFunding,
    MarketStatsFieldName::MarkPrice,
    MarketStatsFieldName::IndexPrice,
    MarketStatsFieldName::LastPrice,
    MarketStatsFieldName::Volume24h,
    MarketStatsFieldName::OpenInterest,
];

#[derive(Default)]
pub(super) struct ExtendedCache {
    observation: RwLock<Option<Arc<ExtendedObservation>>>,
    acquisition: Mutex<()>,
}

pub(super) struct ExtendedObservation {
    pub(super) result: Result<MarketStatsSourceSnapshot, ExchangeError>,
    next_poll_at: Instant,
}

#[derive(Clone, Copy)]
struct ExtendedReceipt {
    received_at: Instant,
    received_timestamp: u64,
}

impl ExtendedReceipt {
    fn now() -> Self {
        Self {
            received_at: Instant::now(),
            received_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

impl ExtendedExchange {
    pub(super) async fn cached_markets(&self) -> Arc<ExtendedObservation> {
        {
            let cached = self.statistics_cache.observation.read().await;
            if let Some(observation) = cached
                .as_ref()
                .filter(|observation| observation.next_poll_at > Instant::now())
            {
                return observation.clone();
            }
        }
        let _guard = self.statistics_cache.acquisition.lock().await;
        {
            let cached = self.statistics_cache.observation.read().await;
            if let Some(observation) = cached
                .as_ref()
                .filter(|observation| observation.next_poll_at > Instant::now())
            {
                return observation.clone();
            }
        }
        // Capture both clocks after HTTP/JSON completion, before validating or mapping.
        // Only the acquisition gate spans this await; cache readers never hold it.
        let response = self.get_response("/info/markets", &[]).await;
        let receipt = ExtendedReceipt::now();
        let observation = Arc::new(ExtendedObservation {
            result: response.and_then(|response| normalize_markets(response, receipt)),
            next_poll_at: receipt.received_at + POLL_INTERVAL,
        });
        *self.statistics_cache.observation.write().await = Some(observation.clone());
        observation
    }
}

#[async_trait]
impl MarketStatsSource for ExtendedExchange {
    fn capabilities(&self) -> MarketStatsCapabilities {
        let fields = FIELDS
            .into_iter()
            .map(|name| {
                let (state, reason) = field_support(name);
                (
                    name,
                    FeatureCapability {
                        state,
                        reason: reason.map(str::to_string),
                    },
                )
            })
            .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: "extended".to_string(),
                params: json!({}),
            },
            all_markets: MarketStatsAllMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                active_only: true,
            },
            selected_markets: MarketStatsSelectedMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                limit: 100,
            },
            fields: BTreeMap::from([(UnifiedMarketType::Perp, fields)]),
            upstream_mode: "sharedPolling".to_string(),
            poll_interval_ms: 30_000,
            stale_after_ms: 90_000,
            ws: MarketStatsWsCapability {
                snapshot: true,
                delta: true,
                max_subscriptions_per_connection: 16,
            },
            funding_kinds: vec![FundingKind::Estimate],
            rate_interval_ms: Some(FUNDING_INTERVAL_MS),
            payment_interval_ms: Some(FUNDING_INTERVAL_MS),
            limitations: vec![
                "perpetual-only".to_string(),
                "rate-unit-decimal-fraction".to_string(),
                "receipt-time-freshness".to_string(),
            ],
        })
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        if !params.params.is_null()
            && !params
                .params
                .as_object()
                .is_some_and(|params| params.is_empty())
        {
            return Err(ExchangeError::BadSymbol(
                "Extended statistics params must be null or an empty object".to_string(),
            ));
        }
        let observation = self.cached_markets().await;
        match &observation.result {
            Ok(snapshot) => Ok(snapshot.clone()),
            Err(error) => {
                // The coordinator alone retains normalized membership and values. A cached
                // failure keeps its original deadline rather than restarting each consumer.
                let mut snapshot = empty_snapshot(observation.next_poll_at);
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: SOURCE.to_string(),
                    reason: match error {
                        ExchangeError::UpstreamData(_) => "invalid-upstream-data",
                        _ => "upstream-failure",
                    }
                    .to_string(),
                    message: error.to_string(),
                });
                Ok(snapshot)
            }
        }
    }
}

fn empty_snapshot(next_poll_at: Instant) -> MarketStatsSourceSnapshot {
    MarketStatsSourceSnapshot {
        rows: Vec::new(),
        perp_catalog_known: false,
        perp_enumeration_complete: false,
        spot_enumeration_complete: true,
        contexts_valid: false,
        received_at: None,
        field_received_at: Default::default(),
        next_poll_at,
        source_failures: Vec::new(),
    }
}

fn normalize_markets(
    response: Value,
    receipt: ExtendedReceipt,
) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
    if response.get("status").and_then(Value::as_str) != Some("OK") {
        return Err(invalid_data("markets response has invalid status"));
    }
    let rows = response
        .get("data")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| invalid_data("markets response requires a nonempty data array"))?;
    let mut names = HashSet::with_capacity(rows.len());
    let mut snapshot = empty_snapshot(receipt.received_at + POLL_INTERVAL);
    snapshot.rows.reserve(rows.len());
    for raw in rows {
        if !raw.is_object() {
            return Err(invalid_data("markets response contains a nonobject row"));
        }
        let native = required_string(raw, "name")?;
        if !names.insert(native) {
            return Err(invalid_data(format!("duplicate market name {native}")));
        }
        let Some(market) = super::map_market(raw, true)? else {
            continue;
        };
        let identity = market.identity.as_ref().unwrap();
        if identity.settlement_asset_id.is_none() {
            snapshot.source_failures.push(MarketStatsSourceFailure {
                source: SOURCE.to_string(),
                reason: "settlement-unresolved".to_string(),
                message: format!("Extended {native} has no valid l2Config.collateralId"),
            });
        }
        let stats = raw.get("marketStats").filter(|stats| stats.is_object());
        let fields = market_fields(&market, stats, receipt);
        if market.active {
            if stats.is_some() {
                snapshot.field_received_at.insert(
                    identity.market_id.clone(),
                    FIELDS
                        .into_iter()
                        .filter(|name| field_support(*name).0 == CapabilityState::Supported)
                        .map(|name| (name, receipt.received_at))
                        .collect(),
                );
            } else {
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: SOURCE.to_string(),
                    reason: "context-mismatch".to_string(),
                    message: format!("Extended {native} has no marketStats object"),
                });
            }
        }
        snapshot.rows.push(MarketStatsRow { market, fields });
    }
    snapshot.rows.sort_unstable_by(|left, right| {
        left.market
            .identity
            .as_ref()
            .unwrap()
            .market_id
            .cmp(&right.market.identity.as_ref().unwrap().market_id)
    });
    snapshot.perp_catalog_known = true;
    snapshot.perp_enumeration_complete = true;
    // Missing row contexts are field-local failures; healthy siblings keep their receipts.
    snapshot.contexts_valid = true;
    snapshot.received_at = Some(receipt.received_at);
    Ok(snapshot)
}

pub(super) fn required_string<'a>(row: &'a Value, key: &str) -> Result<&'a str, ExchangeError> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .ok_or_else(|| invalid_data(format!("market has invalid {key}")))
}

fn field_support(field: MarketStatsFieldName) -> (CapabilityState, Option<&'static str>) {
    use MarketStatsFieldName::*;
    match field {
        Funding | MarkPrice | IndexPrice | LastPrice => (CapabilityState::Supported, None),
        LastSettledFunding | Volume24h | OpenInterest => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
}

fn market_fields(
    market: &UnifiedMarket,
    stats: Option<&Value>,
    receipt: ExtendedReceipt,
) -> BTreeMap<MarketStatsFieldName, MarketStatsField> {
    FIELDS
        .into_iter()
        .map(|name| {
            let (support, reason) = field_support(name);
            let field = if support == CapabilityState::Unsupported {
                fixed_field(MarketStatsFieldState::Unsupported, reason)
            } else if !market.active {
                fixed_field(MarketStatsFieldState::Unavailable, Some("inactive-market"))
            } else if let Some(stats) = stats {
                observed_field(market, name, stats, receipt)
            } else {
                let mut field =
                    fixed_field(MarketStatsFieldState::Unavailable, Some("context-mismatch"));
                field.source = Some(SOURCE.to_string());
                field
            };
            (name, field)
        })
        .collect()
}

fn observed_field(
    market: &UnifiedMarket,
    name: MarketStatsFieldName,
    stats: &Value,
    receipt: ExtendedReceipt,
) -> MarketStatsField {
    let mut field = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: None,
        received_timestamp: Some(receipt.received_timestamp),
        source: Some(SOURCE.to_string()),
    };
    let key = match name {
        MarketStatsFieldName::Funding => "fundingRate",
        MarketStatsFieldName::MarkPrice => "markPrice",
        MarketStatsFieldName::IndexPrice => "indexPrice",
        MarketStatsFieldName::LastPrice => "lastPrice",
        _ => unreachable!("only implemented fields are observed"),
    };
    let Some(raw) = stats
        .get(key)
        .and_then(Value::as_str)
        .and_then(decimal_string)
    else {
        return field;
    };
    field.value = Some(if name == MarketStatsFieldName::Funding {
        MarketStatsValue::Funding(FundingValue::new(
            raw.to_string(),
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            Some(FUNDING_INTERVAL_MS),
            Some(FUNDING_INTERVAL_MS),
            None,
            None,
        ))
    } else {
        if raw.starts_with('-') || !raw.bytes().any(|byte| matches!(byte, b'1'..=b'9')) {
            return field;
        }
        MarketStatsValue::Price(PriceValue {
            amount: raw.to_string(),
            base_asset: market.base.clone(),
            quote_asset: market.quote.clone(),
        })
    });
    field.state = MarketStatsFieldState::Available;
    field.reason = None;
    field
}

fn fixed_field(state: MarketStatsFieldState, reason: Option<&str>) -> MarketStatsField {
    MarketStatsField {
        state,
        value: None,
        reason: reason.map(str::to_string),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}

// Preserve native precision and spelling; accept the same ASCII grammar as prior adapters.
fn decimal_string(value: &str) -> Option<&str> {
    let digits = value.strip_prefix('-').unwrap_or(value).as_bytes();
    let integer_end = digits
        .iter()
        .position(|byte| *byte == b'.')
        .unwrap_or(digits.len());
    if integer_end == 0 || !digits[..integer_end].iter().all(u8::is_ascii_digit) {
        return None;
    }
    if integer_end < digits.len() {
        let fraction = &digits[integer_end + 1..];
        if fraction.is_empty() || !fraction.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }
    Some(value)
}

pub(super) fn invalid_data(message: impl Into<String>) -> ExchangeError {
    ExchangeError::UpstreamData(format!("Extended {}", message.into()))
}
