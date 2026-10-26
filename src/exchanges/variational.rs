//! Native snapshot adapter for Variational's public metadata API.
//! Semantics: https://docs.variational.io/technical-documentation/api
//! No trades, synthetic candles, or indicative quotes disguised as an L2 book.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    sync::Mutex,
    time::{sleep, Instant},
};
use tracing::warn;

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityReason, CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTrade, DataCapability,
        ExchangeDataCapabilities, FeatureCapability, FetchMarketStatsParams, FetchMarketsParams,
        FetchOhlcvParams, FetchOrderBookParams, FetchTradesParams, FundingKind, FundingRateUnit,
        FundingValue, MarketScope, MarketStatsAllMarketsCapability, MarketStatsCapabilities,
        MarketStatsField, MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow,
        MarketStatsScope, MarketStatsSelectedMarketsCapability, MarketStatsSourceFailure,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability,
        OpenInterestValue, PriceValue, RealtimeCapabilities, RealtimeCapability, RealtimeMode,
        UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType, Volume24hValue,
    },
};

const ENDPOINT: &str =
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats";
const SOURCE: &str = "variational:metadata/v2/stats";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_RETRIES: usize = 1;
// Ferris equivalents use a 365-day year. Preserve the native annualized rate
// rather than misinterpreting the swap's ACT/360 payment rate as a daily rate.
const YEAR_MS: u64 = 365 * 86_400_000;

struct CachedObservation {
    expires_at: Instant,
    result: Result<Arc<Observation>, ExchangeError>,
    last_good: Option<Arc<Observation>>,
}

type ObservationCache = Option<CachedObservation>;

#[derive(Clone)]
pub struct VariationalExchange {
    client: reqwest::Client,
    endpoint: String,
    cache: Arc<Mutex<ObservationCache>>,
}

#[derive(Clone)]
struct Observation {
    snapshot: Snapshot,
    received_at: Instant,
    wall: u64,
    next_poll_at: Instant,
    refresh_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpstreamErrorBody {
    error: Option<String>,
    message: Option<String>,
    retry_after_seconds: Option<u64>,
}

impl VariationalExchange {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(
            std::env::var("VARIATIONAL_METADATA_URL").unwrap_or_else(|_| ENDPOINT.to_string()),
        )
    }

    /// Accepts a full metadata endpoint URL, including its path.
    pub fn with_base_url(endpoint: impl Into<String>) -> Result<Self, ExchangeError> {
        let client = reqwest::Client::builder()
            .user_agent("ferris-market-data-backend/variational")
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|error| ExchangeError::Internal(error.to_string()))?;
        Ok(Self {
            client,
            endpoint: endpoint.into(),
            cache: Arc::new(Mutex::new(None)),
        })
    }

    async fn observation(&self) -> Result<Arc<Observation>, ExchangeError> {
        // Share requests and the original receipt across market-list and stats
        // callers. Holding the mutex across the fetch keeps this single-flight,
        // including the bounded retry below. A failed refresh must not erase a
        // usable catalog: retain the last good observation and expose it as
        // stale until the next refresh window.
        let mut cache = self.cache.lock().await;
        let now = Instant::now();
        if let Some(entry) = cache.as_ref() {
            if now < entry.expires_at {
                return entry.result.clone();
            }
        }

        let fetched = self.fetch_observation().await.map(Arc::new);
        let now = Instant::now();
        let entry = match fetched {
            Ok(observation) => CachedObservation {
                expires_at: observation.next_poll_at,
                result: Ok(observation.clone()),
                last_good: Some(observation),
            },
            Err(error) => {
                // Retain data only for transport/HTTP failures. A malformed or
                // semantically invalid successful response must still fail closed
                // rather than silently serving it as a stale refresh.
                let last_good = matches!(&error, ExchangeError::UpstreamRequest(_))
                    .then(|| cache.as_ref().and_then(|entry| entry.last_good.clone()))
                    .flatten();
                if let Some(last_good) = last_good {
                    let stale = Arc::new(Observation {
                        snapshot: last_good.snapshot.clone(),
                        received_at: last_good.received_at,
                        wall: last_good.wall,
                        next_poll_at: now + POLL_INTERVAL,
                        refresh_error: Some(error.to_string()),
                    });
                    CachedObservation {
                        expires_at: now + POLL_INTERVAL,
                        result: Ok(stale),
                        last_good: Some(last_good),
                    }
                } else {
                    CachedObservation {
                        expires_at: now + POLL_INTERVAL,
                        result: Err(error),
                        last_good: None,
                    }
                }
            }
        };
        let result = entry.result.clone();
        *cache = Some(entry);
        result
    }

    async fn fetch_observation(&self) -> Result<Observation, ExchangeError> {
        let mut retries = 0;
        loop {
            // Do not retry transport failures here: reqwest's 20-second timeout
            // is per attempt, and retrying one could exceed the endpoint budget.
            // HTTP 429/5xx responses are fast and carry the upstream's retry hint.
            let response = self
                .client
                .get(&self.endpoint)
                .send()
                .await
                .map_err(|error| ExchangeError::UpstreamRequest(error.to_string()))?;

            if !response.status().is_success() {
                let status = response.status();
                let cf_ray = response
                    .headers()
                    .get("cf-ray")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let body = response.bytes().await.unwrap_or_default();
                let details = serde_json::from_slice::<UpstreamErrorBody>(&body).ok();
                let retry_after = details
                    .as_ref()
                    .and_then(|body| body.retry_after_seconds)
                    .map(|seconds| {
                        Duration::from_secs(seconds.clamp(1, MAX_RETRY_DELAY.as_secs()))
                    });
                let delay = retry_after.unwrap_or(RETRY_DELAY);
                let retryable = status.as_u16() == 429 || status.is_server_error();
                let detail = details
                    .as_ref()
                    .and_then(|body| body.message.as_deref().or(body.error.as_deref()))
                    .unwrap_or("no upstream error detail");
                warn!(
                    status = status.as_u16(),
                    cf_ray = cf_ray.as_deref().unwrap_or("unknown"),
                    retry_after_seconds = retry_after.map(|value| value.as_secs()),
                    error = detail,
                    retry = retryable && retries < MAX_RETRIES,
                    "Variational upstream request rejected"
                );
                if retryable && retries < MAX_RETRIES {
                    retries += 1;
                    sleep(delay).await;
                    continue;
                }
                let ray = cf_ray
                    .map(|value| format!(", cf-ray {value}"))
                    .unwrap_or_default();
                return Err(ExchangeError::UpstreamRequest(format!(
                    "Variational returned HTTP {status}{ray}: {detail}"
                )));
            }

            let snapshot: Snapshot = response.json().await.map_err(|error| {
                ExchangeError::UpstreamData(format!("invalid Variational metadata: {error}"))
            })?;
            let mut ids = HashSet::new();
            for listing in &snapshot.listings {
                market_from_listing(listing)?;
                if !ids.insert(&listing.ticker) {
                    return Err(ExchangeError::UpstreamData(
                        "duplicate Variational ticker".into(),
                    ));
                }
            }
            let received_at = Instant::now();
            return Ok(Observation {
                snapshot,
                received_at,
                wall: now_millis(),
                next_poll_at: received_at + POLL_INTERVAL,
                refresh_error: None,
            });
        }
    }
}

#[async_trait]
impl MarketDataExchange for VariationalExchange {
    fn id(&self) -> &'static str {
        "variational"
    }

    fn capabilities(&self) -> ExchangeDataCapabilities {
        let products = vec![UnifiedMarketType::Perp];
        ExchangeDataCapabilities {
            fetch_markets: DataCapability::supported(products.clone(), MarketScope::Catalog),
            fetch_tickers: DataCapability::unsupported(
                CapabilityReason::VenueDataUnavailable,
                "Variational does not expose public tickers",
            ),
            fetch_trades: DataCapability::unsupported(
                CapabilityReason::VenueDataUnavailable,
                "Variational does not expose public trade history",
            ),
            fetch_ohlcv: DataCapability::unsupported(
                CapabilityReason::VenueDataUnavailable,
                "Variational does not expose public candles",
            ),
            fetch_order_book: DataCapability::unsupported(
                CapabilityReason::IndicativeQuotesOnly,
                "Variational exposes indicative quote tiers, not an L2 order book",
            ),
            fetch_market_stats: DataCapability::supported(
                products.clone(),
                MarketScope::AllMarkets,
            ),
            realtime: RealtimeCapabilities {
                trades: RealtimeCapability::unsupported(
                    CapabilityReason::VenueDataUnavailable,
                    "Variational does not expose public realtime trades",
                ),
                orderbook: RealtimeCapability::unsupported(
                    CapabilityReason::IndicativeQuotesOnly,
                    "Variational does not expose a realtime L2 order book",
                ),
                ohlcv: RealtimeCapability::unsupported(
                    CapabilityReason::VenueDataUnavailable,
                    "Variational does not expose public realtime candles",
                ),
                marketstats: RealtimeCapability::supported(
                    products,
                    MarketScope::AllMarkets,
                    RealtimeMode::Polling,
                    Some(POLL_INTERVAL.as_millis() as u64),
                ),
            },
        }
    }

    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        Some(self)
    }

    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        validate_params(&params.params)?;
        let observation = self.observation().await?;
        let mut markets = observation
            .snapshot
            .listings
            .iter()
            .map(market_from_listing)
            .collect::<Result<Vec<_>, _>>()?;
        markets.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));
        Ok(markets)
    }

    async fn fetch_trades(&self, _: FetchTradesParams) -> Result<Vec<CcxtTrade>, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "public trades are not available from Variational".into(),
        ))
    }
    async fn fetch_ohlcv(&self, _: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "public OHLCV data is not available from Variational".into(),
        ))
    }
    async fn fetch_order_book(
        &self,
        _: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "Variational publishes indicative quote tiers, not an order book".into(),
        ))
    }
}

#[async_trait]
impl MarketStatsSource for VariationalExchange {
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        validate_params(input)?;
        Ok(serde_json::json!({}))
    }
    fn catalog_source(&self) -> &'static str {
        SOURCE
    }

    fn capabilities(&self) -> MarketStatsCapabilities {
        use MarketStatsFieldName::*;
        let fields = [
            Funding,
            LastSettledFunding,
            MarkPrice,
            IndexPrice,
            LastPrice,
            Volume24h,
            OpenInterest,
        ]
        .into_iter()
        .map(|name| {
            let reason = match name {
                IndexPrice => Some("index-price-not-published"),
                LastPrice => Some("last-price-not-published"),
                _ => None,
            };
            (
                name,
                FeatureCapability {
                    state: if reason.is_some() {
                        CapabilityState::Unsupported
                    } else {
                        CapabilityState::Supported
                    },
                    reason: reason.map(str::to_owned),
                },
            )
        })
        .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: self.id().into(),
                params: serde_json::json!({}),
            },
            all_markets: MarketStatsAllMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                active_only: true,
            },
            selected_markets: MarketStatsSelectedMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                limit: 100,
            },
            fields: [(UnifiedMarketType::Perp, fields)].into_iter().collect(),
            upstream_mode: "sharedPolling".into(),
            poll_interval_ms: 30_000,
            stale_after_ms: 90_000,
            // This describes Ferris downstream WS, not an upstream socket.
            ws: MarketStatsWsCapability {
                snapshot: true,
                delta: true,
                max_subscriptions_per_connection: 16,
            },
            funding_kinds: vec![FundingKind::CurrentUnclassified, FundingKind::Settled],
            rate_interval_ms: None,
            payment_interval_ms: None,
            limitations: [
                "receipt-time-freshness",
                "no-public-trades-ohlcv-or-orderbook",
                "open-interest-value-in-usdc-trader-long-plus-short-excludes-olp",
                "volume-quote-only-in-usdc",
                "swap-funding-long-side-only",
                "swap-funding-native-annualized-rate-365-day-equivalents-not-act360-payment",
                "last-settled-funding-swaps-only",
                "catalog-covers-published-listings-only",
                "listed-markets-assumed-active-no-trading-status",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        })
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        validate_params(&params.params)?;
        let observation = self.observation().await?;
        let mut rows = Vec::new();
        let mut field_received_at = HashMap::new();
        for listing in &observation.snapshot.listings {
            let market = market_from_listing(listing)?;
            let id = market
                .identity
                .as_ref()
                .expect("native identity")
                .market_id
                .clone();
            let mut fields: BTreeMap<_, _> = [
                (MarketStatsFieldName::Funding, funding_field(listing, false)),
                (
                    MarketStatsFieldName::LastSettledFunding,
                    funding_field(listing, true),
                ),
                (MarketStatsFieldName::MarkPrice, price_field(listing)),
                (
                    MarketStatsFieldName::IndexPrice,
                    unsupported("index-price-not-published"),
                ),
                (
                    MarketStatsFieldName::LastPrice,
                    unsupported("last-price-not-published"),
                ),
                (MarketStatsFieldName::Volume24h, volume_field(listing)),
                (
                    MarketStatsFieldName::OpenInterest,
                    open_interest_field(listing),
                ),
            ]
            .into_iter()
            .collect();
            let mut receipts = BTreeMap::new();
            for (name, field) in &mut fields {
                if field.state != MarketStatsFieldState::Unsupported {
                    field.received_timestamp = Some(observation.wall);
                    receipts.insert(*name, observation.received_at);
                }
            }
            field_received_at.insert(id, receipts);
            rows.push(MarketStatsRow { market, fields });
        }
        rows.sort_unstable_by(|a, b| a.market.symbol.cmp(&b.market.symbol));
        let source_failures = observation
            .refresh_error
            .as_ref()
            .map(|message| {
                // The catalog and values are still useful, but this is a stale
                // snapshot. Keep original receipt times so age remains honest.
                for row in &mut rows {
                    for field in row.fields.values_mut() {
                        if field.state == MarketStatsFieldState::Available {
                            field.state = MarketStatsFieldState::Stale;
                            field.reason = Some("upstream-failure".into());
                        }
                    }
                }
                MarketStatsSourceFailure {
                    source: SOURCE.into(),
                    reason: "upstream-failure".into(),
                    message: message.clone(),
                }
            })
            .into_iter()
            .collect();
        Ok(MarketStatsSourceSnapshot {
            rows,
            catalog_known: true,
            // num_markets may exceed listings: coverage is the published catalog,
            // not a claim to enumerate unpublished venue markets.
            complete_catalogs: [UnifiedMarketType::Perp].into_iter().collect(),
            contexts_valid: true,
            received_at: Some(observation.received_at),
            field_received_at,
            next_poll_at: observation.next_poll_at,
            source_failures,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Snapshot {
    listings: Vec<Listing>,
}

#[derive(Debug, Clone, Deserialize)]
struct Listing {
    ticker: String,
    contract_type: String,
    // Use Values for scalar observations: one absent/malformed statistic should
    // not prevent the screener from displaying healthy fields or the catalog.
    #[serde(default)]
    mark_price: Value,
    #[serde(default)]
    volume_24h: Value,
    #[serde(default)]
    open_interest: Value,
    #[serde(default)]
    funding: Value,
}

fn validate_params(params: &Value) -> Result<(), ExchangeError> {
    if params.is_null() {
        return Ok(());
    }
    let Some(object) = params.as_object() else {
        return Err(ExchangeError::BadSymbol(
            "Variational params must be an object or null".into(),
        ));
    };
    if object.is_empty() {
        return Ok(());
    }
    if object.len() == 1
        && object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "perp" | "perpetual" | "swap"
                )
            })
    {
        return Ok(());
    }
    Err(ExchangeError::BadSymbol(
        "Variational accepts only type: perp, perpetual, or swap".into(),
    ))
}

fn market_from_listing(listing: &Listing) -> Result<UnifiedMarket, ExchangeError> {
    let suffix = match listing.contract_type.as_str() {
        "perpetual" => "-PERP",
        "swap" => "-SWAP",
        _ => {
            return Err(ExchangeError::UpstreamData(
                "unknown Variational contract type".into(),
            ))
        }
    };
    let ticker = &listing.ticker;
    let base = ticker
        .strip_suffix(suffix)
        .filter(|base| !base.is_empty() && base.trim() == *base)
        .ok_or_else(|| {
            ExchangeError::UpstreamData("invalid Variational ticker/contract type".into())
        })?;
    Ok(UnifiedMarket {
        exchange: "variational".into(),
        symbol: ticker.clone(),
        base: base.into(),
        quote: "USDC".into(),
        market_type: UnifiedMarketType::Perp,
        active: true,
        min_order_size: None,
        tick_size: None,
        contract_size: None,
        info: UnifiedMarketInfo {
            category: Some(listing.contract_type.clone()),
            raw_symbol: Some(ticker.clone()),
            exchange_symbol: Some(ticker.clone()),
            ..Default::default()
        },
        identity: Some(crate::models::MarketIdentity {
            market_id: make_market_id("variational", UnifiedMarketType::Perp, None, None, ticker)?,
            exchange_market_id: ticker.clone(),
            category: None,
            dex: None,
            contract_type: Some(listing.contract_type.clone()),
            settle: Some("USDC".into()),
            settlement_asset_id: Some("USDC".into()),
        }),
    })
}

fn field(
    state: MarketStatsFieldState,
    value: Option<MarketStatsValue>,
    reason: Option<&str>,
) -> MarketStatsField {
    MarketStatsField {
        state,
        value,
        reason: reason.map(str::to_owned),
        exchange_timestamp: None,
        received_timestamp: None,
        source: Some(SOURCE.into()),
    }
}
fn available(value: MarketStatsValue) -> MarketStatsField {
    field(MarketStatsFieldState::Available, Some(value), None)
}
fn unsupported(reason: &str) -> MarketStatsField {
    field(MarketStatsFieldState::Unsupported, None, Some(reason))
}
fn missing() -> MarketStatsField {
    field(
        MarketStatsFieldState::Unavailable,
        None,
        Some("missing-upstream-value"),
    )
}
fn invalid() -> MarketStatsField {
    field(
        MarketStatsFieldState::Unavailable,
        None,
        Some("invalid-upstream-value"),
    )
}
fn number(value: &Value) -> Option<f64> {
    value
        .as_str()?
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
}
fn price_field(listing: &Listing) -> MarketStatsField {
    if listing.mark_price.is_null() {
        return missing();
    }
    if !number(&listing.mark_price).is_some_and(|v| v > 0.0) {
        return invalid();
    }
    available(MarketStatsValue::Price(PriceValue {
        amount: listing.mark_price.as_str().unwrap().into(),
        base_asset: listing.ticker.rsplit_once('-').unwrap().0.into(),
        quote_asset: "USDC".into(),
    }))
}
fn volume_field(listing: &Listing) -> MarketStatsField {
    if listing.volume_24h.is_null() {
        return missing();
    }
    match number(&listing.volume_24h).filter(|v| *v >= 0.0) {
        Some(value) => available(MarketStatsValue::Volume24h(Volume24hValue {
            base_volume: None,
            quote_volume: Some(value),
        })),
        None => invalid(),
    }
}
fn open_interest_field(listing: &Listing) -> MarketStatsField {
    let long = &listing.open_interest["long_open_interest"];
    let short = &listing.open_interest["short_open_interest"];
    if long.is_null() || short.is_null() {
        return missing();
    }
    match (number(long), number(short)) {
        (Some(long), Some(short)) if long >= 0.0 && short >= 0.0 && (long + short).is_finite() => {
            // API docs: each side is trader notional in USDC, not base coins.
            // Do not double it to include OLP or multiply it by mark price.
            available(MarketStatsValue::OpenInterest(OpenInterestValue {
                open_interest_amount: None,
                open_interest_value: Some(long + short),
            }))
        }
        _ => invalid(),
    }
}
fn funding_field(listing: &Listing, settled: bool) -> MarketStatsField {
    let swap = listing.contract_type == "swap";
    if settled && !swap {
        return unsupported("last-settlement-not-provided");
    }
    let funding = &listing.funding;
    if funding.is_null() {
        return missing();
    }
    let rate_object = if settled || !swap {
        &funding["last"]
    } else if funding["next"].is_object() {
        &funding["next"]
    } else {
        // Between swap funding publication and the next schedule, retain the
        // most recently applied rate rather than reporting a fake zero.
        &funding["last"]
    };
    let rate = &rate_object[if swap {
        "annualized_long_rate"
    } else {
        "interval_rate"
    }];
    if rate.is_null() {
        return missing();
    }
    if number(rate).is_none() {
        return invalid();
    }
    let Some(payment_interval) = funding["interval_seconds"]
        .as_u64()
        .filter(|v| *v > 0)
        .and_then(|v| v.checked_mul(1000))
    else {
        return invalid();
    };
    let next_payment = match &funding["next_timestamp"] {
        Value::Null => None,
        value => match value
            .as_u64()
            .filter(|v| *v > 0)
            .and_then(|v| v.checked_mul(1000))
        {
            Some(value) => Some(value),
            None => return invalid(),
        },
    };
    available(MarketStatsValue::Funding(FundingValue::new(
        rate.as_str().unwrap().into(),
        FundingRateUnit::DecimalFraction,
        if settled {
            FundingKind::Settled
        } else {
            FundingKind::CurrentUnclassified
        },
        Some(if swap { YEAR_MS } else { payment_interval }),
        Some(payment_interval),
        None,
        if settled { None } else { next_payment },
    )))
}
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
