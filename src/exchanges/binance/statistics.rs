use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use binance_sdk::derivatives_trading_usds_futures::rest_api::{
    ExchangeInformationResponse, GetFundingRateInfoResponseInner, MarkPriceParams,
    MarkPriceResponse,
};
use serde_json::{json, Value};
use tokio::time::Instant;

use super::{BinanceCache, BinanceExchange, BinanceObservation};
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

const INFO_SOURCE: &str = "binance:exchangeInfo";
const MARK_SOURCE: &str = "binance:premiumIndex";
const FUNDING_INFO_SOURCE: &str = "binance:fundingInfo";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const FUNDING_INFO_INTERVAL: Duration = Duration::from_secs(300);
const FIELDS: [MarketStatsFieldName; 7] = [
    MarketStatsFieldName::Funding,
    MarketStatsFieldName::LastSettledFunding,
    MarketStatsFieldName::MarkPrice,
    MarketStatsFieldName::IndexPrice,
    MarketStatsFieldName::LastPrice,
    MarketStatsFieldName::Volume24h,
    MarketStatsFieldName::OpenInterest,
];

#[async_trait]
impl MarketStatsSource for BinanceExchange {
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
                exchange: "binance".to_string(),
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
            funding_kinds: vec![FundingKind::CurrentUnclassified],
            rate_interval_ms: None,
            payment_interval_ms: None,
            limitations: vec![
                "usd-m-perpetual-only".to_string(),
                "rate-unit-decimal-fraction".to_string(),
                "funding-info-exceptions-only".to_string(),
                "receipt-time-freshness".to_string(),
            ],
        })
    }

    async fn fetch_market_stats(
        &self,
        _params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        // Selection and field projection never enter acquisition. Catalog REST calls share
        // the same observation; only normalized data is copied into the source snapshot.
        let (info, marks, funding_info) = tokio::join!(
            self.cached_exchange_info(),
            self.cached_mark_prices(),
            self.cached_funding_info(),
        );
        let mut failures = Vec::new();
        if let Err(error) = &info.result {
            failures.push(failure(INFO_SOURCE, error));
        }
        if let Err(error) = &marks.result {
            failures.push(failure(MARK_SOURCE, error));
        }
        if let Err(error) = &funding_info.result {
            failures.push(failure(FUNDING_INFO_SOURCE, error));
        }
        let mut snapshot = MarketStatsSourceSnapshot {
            rows: Vec::new(),
            perp_catalog_known: info.result.is_ok(),
            perp_enumeration_complete: info.result.is_ok(),
            spot_enumeration_complete: true,
            contexts_valid: marks.result.is_ok(),
            received_at: marks.result.is_ok().then_some(marks.received_at),
            field_received_at: Default::default(),
            next_poll_at: info.next_poll_at.max(marks.next_poll_at),
            source_failures: failures,
        };
        let Ok(markets) = &info.result else {
            // Never rehydrate an older raw catalog after explicit removal/invalidation.
            // The coordinator retains the last normalized membership as incomplete.
            snapshot.received_at = None;
            snapshot.contexts_valid = false;
            return Ok(snapshot);
        };
        let mark_failure = marks.result.as_ref().err().map(failure_reason);
        for market in markets.iter().filter(|market| market.identity.is_some()) {
            let native = &market
                .identity
                .as_ref()
                .expect("identified Binance perpetual")
                .exchange_market_id;
            let mark = marks.result.as_ref().ok().and_then(|rows| rows.get(native));
            let interval = funding_info
                .result
                .as_ref()
                .ok()
                .and_then(|rows| rows.get(native))
                .copied();
            if market
                .identity
                .as_ref()
                .is_some_and(|identity| identity.settle.is_none())
            {
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: INFO_SOURCE.to_string(),
                    reason: "settlement-unresolved".to_string(),
                    message: format!("Binance {native} has no marginAsset"),
                });
            }
            snapshot.rows.push(MarketStatsRow {
                fields: market_fields(
                    market,
                    mark,
                    interval,
                    mark_failure,
                    marks.received_timestamp,
                ),
                market: market.clone(),
            });
        }
        snapshot.rows.sort_unstable_by(|left, right| {
            left.market
                .identity
                .as_ref()
                .unwrap()
                .market_id
                .cmp(&right.market.identity.as_ref().unwrap().market_id)
        });
        Ok(snapshot)
    }
}

// Same double-checked, per-source acquisition gate as Hyperliquid/MarketsCache. Failures
// are observations too: a burst of viewers cannot bypass the next receipt-based deadline.
impl<T> BinanceCache<T> {
    async fn get<F>(&self, interval: Duration, fetch: F) -> Arc<BinanceObservation<T>>
    where
        F: Future<Output = Result<T, ExchangeError>>,
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
        let result = fetch.await;
        let received_at = Instant::now();
        let next_poll_at = received_at
            + if result.is_ok() {
                interval
            } else {
                POLL_INTERVAL
            };
        let observation = Arc::new(BinanceObservation {
            result: result.map(Arc::new),
            received_at,
            received_timestamp: now_millis(),
            next_poll_at,
        });
        *self.observation.write().await = Some(observation.clone());
        observation
    }
}

impl BinanceExchange {
    pub(super) async fn cached_exchange_info(&self) -> Arc<BinanceObservation<Vec<UnifiedMarket>>> {
        self.exchange_info_cache
            .get(POLL_INTERVAL, async {
                let response = self
                    .client
                    .exchange_information()
                    .await
                    .map_err(super::map_anyhow_error)?;
                let data = response
                    .data()
                    .await
                    .map_err(|error| ExchangeError::UpstreamData(error.to_string()))?;
                catalog_markets(data)
            })
            .await
    }

    async fn cached_mark_prices(&self) -> Arc<BinanceObservation<HashMap<String, MarkValues>>> {
        self.mark_price_cache
            .get(POLL_INTERVAL, async {
                let request = MarkPriceParams::builder().build().map_err(|error| {
                    ExchangeError::Internal(format!(
                        "failed to build Binance mark-price request: {error}"
                    ))
                })?;
                let response = self
                    .client
                    .mark_price(request)
                    .await
                    .map_err(super::map_anyhow_error)?;
                let data = response
                    .data()
                    .await
                    .map_err(|error| ExchangeError::UpstreamData(error.to_string()))?;
                mark_prices(data)
            })
            .await
    }

    async fn cached_funding_info(&self) -> Arc<BinanceObservation<HashMap<String, u64>>> {
        self.funding_info_cache
            .get(FUNDING_INFO_INTERVAL, async {
                let response = self
                    .client
                    .get_funding_rate_info()
                    .await
                    .map_err(super::map_anyhow_error)?;
                let data = response
                    .data()
                    .await
                    .map_err(|error| ExchangeError::UpstreamData(error.to_string()))?;
                funding_intervals(data)
            })
            .await
    }
}

fn catalog_markets(info: ExchangeInformationResponse) -> Result<Vec<UnifiedMarket>, ExchangeError> {
    let symbols = info
        .symbols
        .filter(|symbols| !symbols.is_empty())
        .ok_or_else(|| {
            ExchangeError::UpstreamData(
                "Binance exchangeInfo must contain nonempty symbols".to_string(),
            )
        })?;
    let mut seen = HashSet::with_capacity(symbols.len());
    let mut markets = Vec::with_capacity(symbols.len());
    for symbol in &symbols {
        let native = symbol
            .symbol
            .as_deref()
            .filter(|s| !s.is_empty() && s.trim() == *s);
        let Some(native) = native else {
            return Err(ExchangeError::UpstreamData(
                "Binance exchangeInfo has an invalid symbol".to_string(),
            ));
        };
        if !seen.insert(native)
            || [
                symbol.contract_type.as_deref(),
                symbol.status.as_deref(),
                symbol.base_asset.as_deref(),
                symbol.quote_asset.as_deref(),
            ]
            .into_iter()
            .any(|value| value.is_none_or(|value| value.is_empty() || value.trim() != value))
        {
            return Err(ExchangeError::UpstreamData(format!(
                "Binance exchangeInfo has duplicate or incomplete metadata for {native}"
            )));
        }
        if let Some(market) = super::map_exchange_information_symbol(symbol, true)? {
            markets.push(market);
        }
    }
    Ok(markets)
}

pub(super) struct MarkValues {
    mark: Option<String>,
    index: Option<String>,
    funding: Option<String>,
    next_funding: Option<u64>,
    exchange_timestamp: Option<u64>,
}

fn mark_prices(response: MarkPriceResponse) -> Result<HashMap<String, MarkValues>, ExchangeError> {
    let mut rows = HashMap::new();
    match response {
        MarkPriceResponse::MarkPriceResponse1(value) => {
            insert_mark(
                &mut rows,
                value.symbol,
                MarkValues {
                    mark: value.mark_price,
                    index: value.index_price,
                    funding: value.last_funding_rate,
                    next_funding: positive_timestamp(value.next_funding_time),
                    exchange_timestamp: positive_timestamp(value.time),
                },
            )?;
        }
        MarkPriceResponse::MarkPriceResponse2(values) => {
            rows.reserve(values.len());
            for value in values {
                insert_mark(
                    &mut rows,
                    value.symbol,
                    MarkValues {
                        mark: value.mark_price,
                        index: value.index_price,
                        funding: value.last_funding_rate,
                        next_funding: positive_timestamp(value.next_funding_time),
                        exchange_timestamp: positive_timestamp(value.time),
                    },
                )?;
            }
        }
        MarkPriceResponse::Other(value) => {
            let values = match value {
                Value::Array(values) => values,
                Value::Object(_) => vec![value],
                _ => {
                    return Err(ExchangeError::UpstreamData(
                        "Binance premiumIndex must be an object or array".to_string(),
                    ))
                }
            };
            rows.reserve(values.len());
            for value in values {
                let Value::Object(mut value) = value else {
                    return Err(ExchangeError::UpstreamData(
                        "Binance premiumIndex contains a nonobject row".to_string(),
                    ));
                };
                let symbol = take_string(&mut value, "symbol");
                let mark = MarkValues {
                    mark: take_string(&mut value, "markPrice"),
                    index: take_string(&mut value, "indexPrice"),
                    funding: take_string(&mut value, "lastFundingRate"),
                    next_funding: positive_timestamp(
                        value.get("nextFundingTime").and_then(Value::as_i64),
                    ),
                    exchange_timestamp: positive_timestamp(
                        value.get("time").and_then(Value::as_i64),
                    ),
                };
                insert_mark(&mut rows, symbol, mark)?;
            }
        }
    }
    Ok(rows)
}

fn insert_mark(
    rows: &mut HashMap<String, MarkValues>,
    symbol: Option<String>,
    mark: MarkValues,
) -> Result<(), ExchangeError> {
    let symbol = symbol
        .filter(|symbol| !symbol.is_empty() && symbol.trim() == symbol)
        .ok_or_else(|| {
            ExchangeError::UpstreamData(
                "Binance premiumIndex contains a missing or invalid symbol".to_string(),
            )
        })?;
    if rows.insert(symbol, mark).is_some() {
        return Err(ExchangeError::UpstreamData(
            "Binance premiumIndex contains duplicate symbols".to_string(),
        ));
    }
    Ok(())
}

fn take_string(value: &mut serde_json::Map<String, Value>, key: &str) -> Option<String> {
    match value.remove(key) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn positive_timestamp(value: Option<i64>) -> Option<u64> {
    value.filter(|value| *value > 0).map(|value| value as u64)
}

fn funding_intervals(
    values: Vec<GetFundingRateInfoResponseInner>,
) -> Result<HashMap<String, u64>, ExchangeError> {
    let mut intervals = HashMap::with_capacity(values.len());
    for value in values {
        let symbol = value
            .symbol
            .filter(|symbol| !symbol.is_empty() && symbol.trim() == symbol)
            .ok_or_else(|| {
                ExchangeError::UpstreamData(
                    "Binance fundingInfo contains an invalid symbol".to_string(),
                )
            })?;
        // Reserve zero for unknown intervals until duplicate identity checks finish.
        // An invalid later row must not silently leave an earlier schedule in place.
        let interval = value
            .funding_interval_hours
            .and_then(|hours| u64::try_from(hours).ok())
            .and_then(|hours| hours.checked_mul(3_600_000))
            .unwrap_or(0);
        if intervals.insert(symbol, interval).is_some() {
            return Err(ExchangeError::UpstreamData(
                "Binance fundingInfo contains duplicate symbols".to_string(),
            ));
        }
    }
    intervals.retain(|_, interval| *interval > 0);
    Ok(intervals)
}

fn field_support(field: MarketStatsFieldName) -> (CapabilityState, Option<&'static str>) {
    use MarketStatsFieldName::*;
    match field {
        Funding | MarkPrice | IndexPrice => (CapabilityState::Supported, None),
        Volume24h | OpenInterest => (CapabilityState::Unsupported, Some("units-unverified")),
        _ => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
}

fn market_fields(
    market: &UnifiedMarket,
    mark: Option<&MarkValues>,
    interval: Option<u64>,
    mark_failure: Option<&str>,
    received_timestamp: u64,
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
                observed_field(market, name, mark, interval, received_timestamp)
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
    mark: &MarkValues,
    interval: Option<u64>,
    received_timestamp: u64,
) -> MarketStatsField {
    let mut field = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: mark.exchange_timestamp,
        received_timestamp: Some(received_timestamp),
        source: Some(MARK_SOURCE.to_string()),
    };
    let Some(raw) = (match name {
        MarketStatsFieldName::Funding => mark.funding.as_deref(),
        MarketStatsFieldName::MarkPrice => mark.mark.as_deref(),
        MarketStatsFieldName::IndexPrice => mark.index.as_deref(),
        _ => unreachable!("only supported fields are observed"),
    })
    .and_then(decimal_string) else {
        return field;
    };
    field.value = Some(if name == MarketStatsFieldName::Funding {
        MarketStatsValue::Funding(FundingValue::new(
            raw.to_string(),
            FundingRateUnit::DecimalFraction,
            FundingKind::CurrentUnclassified,
            interval,
            interval,
            None,
            mark.next_funding,
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
    field.reason = field_support(name).1.map(str::to_string);
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

// Validate lexically, without floating-point conversion or changing native precision.
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

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
