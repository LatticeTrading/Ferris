use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};

use super::AsterExchange;
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

const INFO_SOURCE: &str = "aster:exchangeInfo";
const MARK_SOURCE: &str = "aster:premiumIndex";
const FUNDING_INFO_SOURCE: &str = "aster:fundingInfo";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
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
pub(super) struct AsterStatisticsCache {
    exchange_info: AsterCache<Vec<UnifiedMarket>>,
    premium_index: AsterCache<HashMap<String, PremiumValues>>,
    funding_info: AsterCache<HashMap<String, Option<u64>>>,
}

struct AsterCache<T> {
    observation: RwLock<Option<Arc<AsterObservation<T>>>>,
    acquisition: Mutex<()>,
}

impl<T> Default for AsterCache<T> {
    fn default() -> Self {
        Self {
            observation: RwLock::new(None),
            acquisition: Mutex::new(()),
        }
    }
}

#[derive(Clone, Copy)]
struct AsterReceipt {
    received_at: Instant,
    received_timestamp: u64,
}

impl AsterReceipt {
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

pub(super) struct AsterObservation<T> {
    pub(super) result: Result<T, ExchangeError>,
    receipt: AsterReceipt,
    next_poll_at: Instant,
}

impl<T> AsterObservation<T> {
    fn new(result: Result<T, ExchangeError>, receipt: AsterReceipt) -> Self {
        Self {
            result,
            receipt,
            next_poll_at: receipt.received_at + POLL_INTERVAL,
        }
    }
}

// Each endpoint has its own gate. Both successes and failures retain their original
// receipt deadline; no reader/writer cache lock crosses a network await.
impl<T> AsterCache<T> {
    async fn get<F>(&self, fetch: F) -> Arc<AsterObservation<T>>
    where
        F: Future<Output = AsterObservation<T>>,
    {
        {
            let cached = self.observation.read().await;
            if let Some(value) = cached
                .as_ref()
                .filter(|value| value.next_poll_at > Instant::now())
            {
                return value.clone();
            }
        }
        let _guard = self.acquisition.lock().await;
        {
            let cached = self.observation.read().await;
            if let Some(value) = cached
                .as_ref()
                .filter(|value| value.next_poll_at > Instant::now())
            {
                return value.clone();
            }
        }
        let observation = Arc::new(fetch.await);
        *self.observation.write().await = Some(observation.clone());
        observation
    }
}

struct PremiumValues {
    funding: Option<String>,
    mark: Option<String>,
    index: Option<String>,
    next_funding: Option<u64>,
    exchange_timestamp: Option<u64>,
}

impl AsterExchange {
    pub(super) async fn cached_exchange_info(&self) -> Arc<AsterObservation<Vec<UnifiedMarket>>> {
        self.statistics_cache
            .exchange_info
            .get(async {
                let response = self.get("/fapi/v3/exchangeInfo", &[]).await;
                let receipt = AsterReceipt::now();
                AsterObservation::new(response.and_then(|value| catalog_markets(&value)), receipt)
            })
            .await
    }

    async fn cached_premium_index(&self) -> Arc<AsterObservation<HashMap<String, PremiumValues>>> {
        self.statistics_cache
            .premium_index
            .get(async {
                let response = self.get("/fapi/v3/premiumIndex", &[]).await;
                let receipt = AsterReceipt::now();
                AsterObservation::new(response.and_then(|value| premium_values(&value)), receipt)
            })
            .await
    }

    async fn cached_funding_info(&self) -> Arc<AsterObservation<HashMap<String, Option<u64>>>> {
        self.statistics_cache
            .funding_info
            .get(async {
                let response = self.get("/fapi/v3/fundingInfo", &[]).await;
                let receipt = AsterReceipt::now();
                AsterObservation::new(
                    response.and_then(|value| funding_intervals(&value)),
                    receipt,
                )
            })
            .await
    }
}

#[async_trait]
impl MarketStatsSource for AsterExchange {
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
                exchange: "aster".to_string(),
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
            rate_interval_ms: None,
            payment_interval_ms: None,
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
                "Aster statistics params must be null or an empty object".to_string(),
            ));
        }
        // Native bulk acquisition never depends on downstream selection or fields.
        let (info, marks, funding_info) = tokio::join!(
            self.cached_exchange_info(),
            self.cached_premium_index(),
            self.cached_funding_info(),
        );
        let mut source_failures = Vec::new();
        if let Err(error) = &info.result {
            source_failures.push(failure(INFO_SOURCE, error));
        }
        if let Err(error) = &marks.result {
            source_failures.push(failure(MARK_SOURCE, error));
        }
        if let Err(error) = &funding_info.result {
            source_failures.push(failure(FUNDING_INFO_SOURCE, error));
        }
        let mut snapshot = MarketStatsSourceSnapshot {
            rows: Vec::new(),
            perp_catalog_known: info.result.is_ok(),
            perp_enumeration_complete: info.result.is_ok(),
            spot_enumeration_complete: true,
            contexts_valid: marks.result.is_ok(),
            received_at: marks.result.is_ok().then_some(marks.receipt.received_at),
            field_received_at: Default::default(),
            next_poll_at: info
                .next_poll_at
                .min(marks.next_poll_at)
                .min(funding_info.next_poll_at),
            source_failures,
        };
        let Ok(markets) = &info.result else {
            // Only the coordinator retains normalized membership after catalog failures.
            snapshot.received_at = None;
            snapshot.contexts_valid = false;
            return Ok(snapshot);
        };
        snapshot.rows.reserve(markets.len());
        let mark_failure = marks.result.as_ref().err().map(failure_reason);
        for market in markets {
            let identity = market
                .identity
                .as_ref()
                .expect("identified Aster perpetual");
            let native = &identity.exchange_market_id;
            let mark = marks.result.as_ref().ok().and_then(|rows| rows.get(native));
            let interval = funding_info
                .result
                .as_ref()
                .ok()
                .and_then(|rows| rows.get(native))
                .copied()
                .flatten();
            if identity.settle.is_none() {
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: INFO_SOURCE.to_string(),
                    reason: "settlement-unresolved".to_string(),
                    message: format!("Aster {native} has no valid marginAsset"),
                });
            }
            if market.active && interval.is_none() && funding_info.result.is_ok() {
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: FUNDING_INFO_SOURCE.to_string(),
                    reason: "funding-interval-unavailable".to_string(),
                    message: format!("Aster {native} has no valid fundingIntervalHours"),
                });
            }
            let fields = market_fields(market, mark, interval, mark_failure, marks.receipt);
            // Missing rows do not advance any field clock. Explicit scalar clears are
            // observations, but an absent row remains available only through retention.
            if market.active && mark.is_some() {
                snapshot.field_received_at.insert(
                    identity.market_id.clone(),
                    FIELDS
                        .into_iter()
                        .filter(|name| field_support(*name).0 == CapabilityState::Supported)
                        .map(|name| (name, marks.receipt.received_at))
                        .collect(),
                );
            }
            snapshot.rows.push(MarketStatsRow {
                market: market.clone(),
                fields,
            });
        }
        Ok(snapshot)
    }
}

fn catalog_markets(response: &Value) -> Result<Vec<UnifiedMarket>, ExchangeError> {
    let rows = response
        .get("symbols")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| invalid_data("exchangeInfo requires a nonempty symbols array"))?;
    let mut symbols = HashSet::with_capacity(rows.len());
    let mut markets = Vec::with_capacity(rows.len());
    for raw in rows {
        let native = required_string(raw, "symbol")?;
        if !symbols.insert(native) {
            return Err(invalid_data(format!(
                "duplicate instrument symbol {native}"
            )));
        }
        if let Some(market) = super::map_market(raw)? {
            markets.push(market);
        }
    }
    markets.sort_unstable_by(|left, right| {
        left.identity
            .as_ref()
            .unwrap()
            .market_id
            .cmp(&right.identity.as_ref().unwrap().market_id)
    });
    Ok(markets)
}

fn premium_values(response: &Value) -> Result<HashMap<String, PremiumValues>, ExchangeError> {
    let rows = response
        .as_array()
        .ok_or_else(|| invalid_data("premiumIndex response must be an array"))?;
    let mut marks = HashMap::with_capacity(rows.len());
    for raw in rows {
        let native = required_string(raw, "symbol")?;
        let values = PremiumValues {
            funding: raw
                .get("lastFundingRate")
                .and_then(Value::as_str)
                .and_then(decimal_string)
                .map(str::to_string),
            mark: raw
                .get("markPrice")
                .and_then(Value::as_str)
                .and_then(positive_decimal_string)
                .map(str::to_string),
            index: raw
                .get("indexPrice")
                .and_then(Value::as_str)
                .and_then(positive_decimal_string)
                .map(str::to_string),
            next_funding: raw.get("nextFundingTime").and_then(positive_integer),
            exchange_timestamp: raw.get("time").and_then(positive_integer),
        };
        if marks.insert(native.to_string(), values).is_some() {
            return Err(invalid_data(format!(
                "duplicate premiumIndex symbol {native}"
            )));
        }
    }
    Ok(marks)
}

fn funding_intervals(response: &Value) -> Result<HashMap<String, Option<u64>>, ExchangeError> {
    let rows = response
        .as_array()
        .ok_or_else(|| invalid_data("fundingInfo response must be an array"))?;
    let mut intervals = HashMap::with_capacity(rows.len());
    for raw in rows {
        let native = required_string(raw, "symbol")?;
        let interval = raw
            .get("fundingIntervalHours")
            .and_then(positive_integer)
            .and_then(|hours| hours.checked_mul(3_600_000));
        if intervals.insert(native.to_string(), interval).is_some() {
            return Err(invalid_data(format!(
                "duplicate fundingInfo symbol {native}"
            )));
        }
    }
    Ok(intervals)
}

pub(super) fn required_string<'a>(row: &'a Value, key: &str) -> Result<&'a str, ExchangeError> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .ok_or_else(|| invalid_data(format!("market row has invalid {key}")))
}

fn positive_integer(value: &Value) -> Option<u64> {
    value.as_u64().filter(|value| *value > 0)
}

fn field_support(field: MarketStatsFieldName) -> (CapabilityState, Option<&'static str>) {
    use MarketStatsFieldName::*;
    match field {
        Funding | MarkPrice | IndexPrice => (CapabilityState::Supported, None),
        LastSettledFunding | LastPrice | Volume24h | OpenInterest => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
}

fn market_fields(
    market: &UnifiedMarket,
    mark: Option<&PremiumValues>,
    interval: Option<u64>,
    mark_failure: Option<&str>,
    receipt: AsterReceipt,
) -> BTreeMap<MarketStatsFieldName, MarketStatsField> {
    FIELDS
        .into_iter()
        .map(|name| {
            let (support, reason) = field_support(name);
            let field = if support == CapabilityState::Unsupported {
                fixed_field(MarketStatsFieldState::Unsupported, reason)
            } else if !market.active {
                fixed_field(MarketStatsFieldState::Unavailable, Some("inactive-market"))
            } else if let Some(mark) = mark {
                observed_field(market, name, mark, interval, receipt)
            } else {
                let mut field = fixed_field(
                    MarketStatsFieldState::Unavailable,
                    Some(mark_failure.unwrap_or("missing-upstream-row")),
                );
                field.source = Some(MARK_SOURCE.to_string());
                field
            };
            (name, field)
        })
        .collect()
}

fn observed_field(
    market: &UnifiedMarket,
    name: MarketStatsFieldName,
    mark: &PremiumValues,
    interval: Option<u64>,
    receipt: AsterReceipt,
) -> MarketStatsField {
    let mut field = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: mark.exchange_timestamp,
        received_timestamp: Some(receipt.received_timestamp),
        source: Some(MARK_SOURCE.to_string()),
    };
    let Some(raw) = (match name {
        MarketStatsFieldName::Funding => mark.funding.as_ref(),
        MarketStatsFieldName::MarkPrice => mark.mark.as_ref(),
        MarketStatsFieldName::IndexPrice => mark.index.as_ref(),
        _ => unreachable!("only implemented fields are observed"),
    }) else {
        return field;
    };
    field.value = Some(if name == MarketStatsFieldName::Funding {
        MarketStatsValue::Funding(FundingValue::new(
            raw.clone(),
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            interval,
            interval,
            None,
            mark.next_funding,
        ))
    } else {
        MarketStatsValue::Price(PriceValue {
            amount: raw.clone(),
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

// Validate exact ASCII decimals without rounding or changing native spelling.
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

fn positive_decimal_string(value: &str) -> Option<&str> {
    decimal_string(value).filter(|value| {
        !value.starts_with('-') && value.bytes().any(|byte| matches!(byte, b'1'..=b'9'))
    })
}

pub(super) fn invalid_data(message: impl Into<String>) -> ExchangeError {
    ExchangeError::UpstreamData(format!("Aster {}", message.into()))
}

fn failure_reason(error: &ExchangeError) -> &'static str {
    match error {
        ExchangeError::UpstreamData(_) => "invalid-upstream-data",
        _ => "upstream-failure",
    }
}

fn failure(source: &str, error: &ExchangeError) -> MarketStatsSourceFailure {
    MarketStatsSourceFailure {
        source: source.to_string(),
        reason: failure_reason(error).to_string(),
        message: error.to_string(),
    }
}
