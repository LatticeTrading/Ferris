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

use super::{BybitCategory, BybitExchange, DEFAULT_FETCH_MARKETS_LIMIT};
use crate::{
    exchanges::traits::{ExchangeError, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityState, FeatureCapability, FetchMarketStatsParams, FundingKind, FundingRateUnit,
        FundingValue, MarketIdentity, MarketStatsAllMarketsCapability, MarketStatsCapabilities,
        MarketStatsField, MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow,
        MarketStatsScope, MarketStatsSelectedMarketsCapability, MarketStatsSourceFailure,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability, PriceValue,
        UnifiedMarket, UnifiedMarketType,
    },
};

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
pub(super) struct BybitCategoryCache {
    instruments: BybitCache<BybitCatalog>,
    tickers: BybitCache<HashMap<String, TickerValues>>,
}

struct BybitCache<T> {
    observation: RwLock<Option<Arc<BybitObservation<T>>>>,
    acquisition: Mutex<()>,
}

impl<T> Default for BybitCache<T> {
    fn default() -> Self {
        Self {
            observation: RwLock::new(None),
            acquisition: Mutex::new(()),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct BybitReceipt {
    received_at: Instant,
    received_timestamp: u64,
}

impl BybitReceipt {
    pub(super) fn now() -> Self {
        Self {
            received_at: Instant::now(),
            received_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

pub(super) struct BybitResponse {
    pub(super) result: Result<Value, ExchangeError>,
    pub(super) receipt: BybitReceipt,
    pub(super) exchange_timestamp: Option<u64>,
}

pub(super) struct BybitObservation<T> {
    pub(super) result: Result<Arc<T>, ExchangeError>,
    receipt: BybitReceipt,
    exchange_timestamp: Option<u64>,
    next_poll_at: Instant,
}

impl<T> BybitObservation<T> {
    fn new(
        result: Result<T, ExchangeError>,
        receipt: BybitReceipt,
        exchange_timestamp: Option<u64>,
    ) -> Self {
        Self {
            result: result.map(Arc::new),
            receipt,
            exchange_timestamp,
            next_poll_at: receipt.received_at + POLL_INTERVAL,
        }
    }
}

// Successes and failures are both receipt-based observations. A per-endpoint gate
// coalesces catalog and statistics consumers without serializing the two categories.
impl<T> BybitCache<T> {
    async fn get<F>(&self, fetch: F) -> Arc<BybitObservation<T>>
    where
        F: Future<Output = BybitObservation<T>>,
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

pub(super) struct BybitCatalog {
    pub(super) markets: Vec<UnifiedMarket>,
    funding_intervals: HashMap<String, u64>,
}

struct TickerValues {
    funding: Option<String>,
    mark: Option<String>,
    index: Option<String>,
    last: Option<String>,
    next_funding: Option<u64>,
    // None means absent, Some(None) is an explicit invalid interval and must not
    // fall back to catalog metadata.
    funding_interval: Option<Option<u64>>,
}

impl BybitCategory {
    fn statistics_cache_index(self) -> usize {
        match self {
            Self::Linear => 0,
            Self::Inverse => 1,
            _ => unreachable!("only perpetual categories have statistics caches"),
        }
    }

    fn instruments_source(self) -> &'static str {
        match self {
            Self::Linear => "bybit:linear:instruments-info",
            Self::Inverse => "bybit:inverse:instruments-info",
            _ => unreachable!("only perpetual categories have statistics sources"),
        }
    }

    fn tickers_source(self) -> &'static str {
        match self {
            Self::Linear => "bybit:linear:tickers",
            Self::Inverse => "bybit:inverse:tickers",
            _ => unreachable!("only perpetual categories have statistics sources"),
        }
    }
}

impl BybitExchange {
    pub(super) async fn cached_instruments(
        &self,
        category: BybitCategory,
    ) -> Arc<BybitObservation<BybitCatalog>> {
        self.statistics_cache[category.statistics_cache_index()]
            .instruments
            .get(self.fetch_instruments(category))
            .await
    }

    async fn fetch_instruments(&self, category: BybitCategory) -> BybitObservation<BybitCatalog> {
        let mut receipt = BybitReceipt::now();
        let mut exchange_timestamp = None;
        let result = async {
            let mut catalog = BybitCatalog {
                markets: Vec::new(),
                funding_intervals: HashMap::new(),
            };
            let mut symbols = HashSet::new();
            let mut cursors = HashSet::new();
            let mut cursor = None;
            loop {
                let mut query = vec![
                    ("category", category.as_str().to_string()),
                    ("limit", DEFAULT_FETCH_MARKETS_LIMIT.to_string()),
                ];
                if let Some(cursor) = cursor.take() {
                    query.push(("cursor", cursor));
                }
                let response = self
                    .get_public_market_response("/v5/market/instruments-info", &query)
                    .await;
                receipt = response.receipt;
                exchange_timestamp = response.exchange_timestamp;
                let result = response.result?;
                for raw in category_rows(&result, category)? {
                    let native = required_string(raw, "symbol")?;
                    if !symbols.insert(native.to_string()) {
                        return Err(invalid_data(format!(
                            "duplicate instrument symbol {native}"
                        )));
                    }
                    let (market, interval) = catalog_market(raw, category)?;
                    if let Some(interval) = interval {
                        catalog
                            .funding_intervals
                            .insert(native.to_string(), interval);
                    }
                    catalog.markets.push(market);
                }
                let next = match result.get("nextPageCursor") {
                    Some(Value::String(cursor)) => cursor.as_str(),
                    _ => return Err(invalid_data("invalid instruments nextPageCursor")),
                };
                if next.is_empty() {
                    break;
                }
                if !cursors.insert(next.to_string()) {
                    return Err(invalid_data("instruments pagination cursor cycle"));
                }
                cursor = Some(next.to_string());
            }
            if catalog.markets.is_empty() {
                return Err(invalid_data("empty instruments catalog"));
            }
            Ok(catalog)
        }
        .await;
        BybitObservation::new(result, receipt, exchange_timestamp)
    }

    async fn cached_tickers(
        &self,
        category: BybitCategory,
    ) -> Arc<BybitObservation<HashMap<String, TickerValues>>> {
        self.statistics_cache[category.statistics_cache_index()]
            .tickers
            .get(async {
                let response = self
                    .get_public_market_response(
                        "/v5/market/tickers",
                        &[("category", category.as_str().to_string())],
                    )
                    .await;
                let result = response
                    .result
                    .and_then(|result| ticker_values(&result, category));
                BybitObservation::new(result, response.receipt, response.exchange_timestamp)
            })
            .await
    }
}

#[async_trait]
impl MarketStatsSource for BybitExchange {
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
                exchange: "bybit".to_string(),
                params: json!({"category": "linear"}),
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
                "linear-and-inverse-perpetual-only".to_string(),
                "pre-market-excluded".to_string(),
                "rate-unit-decimal-fraction".to_string(),
                "receipt-time-freshness".to_string(),
            ],
        })
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        let category = statistics_category(&params.params)?;
        // Native bulk endpoints are independent of the downstream selection and fields.
        let (instruments, tickers) = tokio::join!(
            self.cached_instruments(category),
            self.cached_tickers(category),
        );
        let mut source_failures = Vec::new();
        if let Err(error) = &instruments.result {
            source_failures.push(failure(category.instruments_source(), error));
        }
        if let Err(error) = &tickers.result {
            source_failures.push(failure(category.tickers_source(), error));
        }
        let mut snapshot = MarketStatsSourceSnapshot {
            rows: Vec::new(),
            perp_catalog_known: instruments.result.is_ok(),
            perp_enumeration_complete: instruments.result.is_ok(),
            spot_enumeration_complete: true,
            contexts_valid: tickers.result.is_ok(),
            received_at: tickers
                .result
                .is_ok()
                .then_some(tickers.receipt.received_at),
            field_received_at: Default::default(),
            next_poll_at: instruments.next_poll_at.min(tickers.next_poll_at),
            source_failures,
        };
        let Ok(catalog) = &instruments.result else {
            // Keep no raw fallback: only the coordinator retains normalized membership.
            snapshot.received_at = None;
            snapshot.contexts_valid = false;
            return Ok(snapshot);
        };
        let ticker_failure = tickers.result.as_ref().err().map(failure_reason);
        for market in &catalog.markets {
            let Some(identity) = &market.identity else {
                continue;
            };
            let native = &identity.exchange_market_id;
            let ticker = tickers
                .result
                .as_ref()
                .ok()
                .and_then(|rows| rows.get(native));
            let instrument_interval = catalog.funding_intervals.get(native).copied();
            let interval = ticker
                .and_then(|ticker| ticker.funding_interval)
                .unwrap_or(instrument_interval);
            if let Some(ticker_interval) =
                ticker.and_then(|ticker| ticker.funding_interval.flatten())
            {
                if instrument_interval.is_some_and(|interval| interval != ticker_interval) {
                    snapshot.source_failures.push(MarketStatsSourceFailure {
                        source: category.instruments_source().to_string(),
                        reason: "funding-interval-mismatch".to_string(),
                        message: format!("Bybit {native} ticker funding interval differs from instrument metadata"),
                    });
                }
            }
            if identity.settle.is_none() {
                snapshot.source_failures.push(MarketStatsSourceFailure {
                    source: category.instruments_source().to_string(),
                    reason: "settlement-unresolved".to_string(),
                    message: format!("Bybit {native} has no valid settleCoin"),
                });
            }
            let fields = market_fields(
                market,
                ticker,
                interval,
                ticker_failure,
                category.tickers_source(),
                &tickers,
            );
            // Missing rows have no new receipt. This also preserves each old row's
            // original age when other rows in a bulk response continue to advance.
            if ticker.is_some() && market.active {
                snapshot.field_received_at.insert(
                    identity.market_id.clone(),
                    FIELDS
                        .into_iter()
                        .filter(|name| field_support(*name).0 == CapabilityState::Supported)
                        .map(|name| (name, tickers.receipt.received_at))
                        .collect(),
                );
            }
            snapshot.rows.push(MarketStatsRow {
                market: market.clone(),
                fields,
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

fn statistics_category(params: &Value) -> Result<BybitCategory, ExchangeError> {
    match params {
        Value::Null => Ok(BybitCategory::Linear),
        Value::Object(params) if params.is_empty() => Ok(BybitCategory::Linear),
        Value::Object(params) if params.len() == 1 => {
            match params.get("category").and_then(Value::as_str) {
                Some("linear") => Ok(BybitCategory::Linear),
                Some("inverse") => Ok(BybitCategory::Inverse),
                _ => Err(ExchangeError::BadSymbol(
                    "Bybit statistics category must be linear or inverse".to_string(),
                )),
            }
        }
        _ => Err(ExchangeError::BadSymbol(
            "Bybit statistics params accept only category".to_string(),
        )),
    }
}

fn category_rows(result: &Value, category: BybitCategory) -> Result<&Vec<Value>, ExchangeError> {
    if result.get("category").and_then(Value::as_str) != Some(category.as_str()) {
        return Err(invalid_data("response category does not match request"));
    }
    result
        .get("list")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_data("response missing result.list array"))
}

fn required_string<'a>(row: &'a Value, key: &str) -> Result<&'a str, ExchangeError> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.trim() == *value)
        .ok_or_else(|| invalid_data(format!("instrument or ticker has invalid {key}")))
}

fn catalog_market(
    raw: &Value,
    category: BybitCategory,
) -> Result<(UnifiedMarket, Option<u64>), ExchangeError> {
    let native = required_string(raw, "symbol")?;
    let contract_type = required_string(raw, "contractType")?;
    let status = required_string(raw, "status")?;
    required_string(raw, "baseCoin")?;
    required_string(raw, "quoteCoin")?;
    let is_perpetual = matches!(
        (category, contract_type),
        (BybitCategory::Linear, "LinearPerpetual") | (BybitCategory::Inverse, "InversePerpetual")
    );
    let mut market = super::map_market_row(raw, category, true)
        .map_err(|error| invalid_data(format!("unable to map instrument {native}: {error}")))?
        .ok_or_else(|| invalid_data(format!("unable to map instrument {native}")))?;
    // Restore exact native IDs; display normalization never participates in identity.
    market.info.raw_symbol = Some(native.to_string());
    market.info.exchange_symbol = Some(native.to_string());
    if !is_perpetual {
        return Ok((market, None));
    }
    let prelisting = raw
        .get("isPreListing")
        .and_then(Value::as_bool)
        .ok_or_else(|| invalid_data(format!("perpetual {native} has invalid isPreListing")))?;
    market.active = status == "Trading" && !prelisting;
    let settle = raw
        .get("settleCoin")
        .and_then(Value::as_str)
        .filter(|asset| !asset.is_empty() && asset.trim() == *asset);
    market.identity = Some(MarketIdentity {
        market_id: make_market_id(
            "bybit",
            UnifiedMarketType::Perp,
            Some(category.as_str()),
            None,
            native,
        )?,
        exchange_market_id: native.to_string(),
        category: Some(category.as_str().to_string()),
        dex: None,
        contract_type: Some(contract_type.to_string()),
        settle: settle.map(str::to_string),
        settlement_asset_id: settle.map(str::to_string),
    });
    let interval = raw
        .get("fundingInterval")
        .and_then(Value::as_u64)
        .filter(|minutes| *minutes > 0)
        .and_then(|minutes| minutes.checked_mul(60_000));
    Ok((market, interval))
}

fn ticker_values(
    result: &Value,
    category: BybitCategory,
) -> Result<HashMap<String, TickerValues>, ExchangeError> {
    let rows = category_rows(result, category)?;
    let mut tickers = HashMap::with_capacity(rows.len());
    for raw in rows {
        let native = required_string(raw, "symbol")?;
        let values = TickerValues {
            funding: string_field(raw, "fundingRate"),
            mark: string_field(raw, "markPrice"),
            index: string_field(raw, "indexPrice"),
            last: string_field(raw, "lastPrice"),
            next_funding: raw.get("nextFundingTime").and_then(positive_integer_string),
            funding_interval: raw.get("fundingIntervalHour").map(|value| {
                positive_integer_string(value).and_then(|hours| hours.checked_mul(3_600_000))
            }),
        };
        if tickers.insert(native.to_string(), values).is_some() {
            return Err(invalid_data(format!("duplicate ticker symbol {native}")));
        }
    }
    Ok(tickers)
}

fn string_field(row: &Value, key: &str) -> Option<String> {
    row.get(key).and_then(Value::as_str).map(str::to_string)
}

fn positive_integer_string(value: &Value) -> Option<u64> {
    let value = value.as_str()?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<u64>().ok().filter(|value| *value > 0)
}

fn field_support(field: MarketStatsFieldName) -> (CapabilityState, Option<&'static str>) {
    use MarketStatsFieldName::*;
    match field {
        Funding | MarkPrice | IndexPrice | LastPrice => (CapabilityState::Supported, None),
        Volume24h | OpenInterest => (CapabilityState::Unsupported, Some("units-unverified")),
        LastSettledFunding => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
}

fn market_fields(
    market: &UnifiedMarket,
    ticker: Option<&TickerValues>,
    interval: Option<u64>,
    ticker_failure: Option<&str>,
    source: &str,
    observation: &BybitObservation<HashMap<String, TickerValues>>,
) -> BTreeMap<MarketStatsFieldName, MarketStatsField> {
    FIELDS
        .into_iter()
        .map(|name| {
            let (support, reason) = field_support(name);
            let field = if support == CapabilityState::Unsupported {
                fixed_field(MarketStatsFieldState::Unsupported, reason)
            } else if !market.active {
                fixed_field(MarketStatsFieldState::Unavailable, Some("inactive-market"))
            } else if let Some(ticker) = ticker {
                observed_field(market, name, ticker, interval, source, observation)
            } else {
                let mut field = fixed_field(
                    MarketStatsFieldState::Unavailable,
                    Some(ticker_failure.unwrap_or("missing-upstream-row")),
                );
                field.source = Some(source.to_string());
                field
            };
            (name, field)
        })
        .collect()
}

fn observed_field(
    market: &UnifiedMarket,
    name: MarketStatsFieldName,
    ticker: &TickerValues,
    interval: Option<u64>,
    source: &str,
    observation: &BybitObservation<HashMap<String, TickerValues>>,
) -> MarketStatsField {
    let mut field = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: observation.exchange_timestamp,
        received_timestamp: Some(observation.receipt.received_timestamp),
        source: Some(source.to_string()),
    };
    let Some(raw) = (match name {
        MarketStatsFieldName::Funding => ticker.funding.as_deref(),
        MarketStatsFieldName::MarkPrice => ticker.mark.as_deref(),
        MarketStatsFieldName::IndexPrice => ticker.index.as_deref(),
        MarketStatsFieldName::LastPrice => ticker.last.as_deref(),
        _ => unreachable!("only implemented fields are observed"),
    })
    .and_then(decimal_string) else {
        return field;
    };
    field.value = Some(if name == MarketStatsFieldName::Funding {
        MarketStatsValue::Funding(FundingValue::new(
            raw.to_string(),
            FundingRateUnit::DecimalFraction,
            FundingKind::Estimate,
            interval,
            interval,
            None,
            ticker.next_funding,
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

// Match the existing adapters' ASCII lexical validation, not a floating-point parse.
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

fn invalid_data(message: impl Into<String>) -> ExchangeError {
    ExchangeError::UpstreamData(format!("Bybit {}", message.into()))
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
