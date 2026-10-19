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
use tokio::{sync::Mutex, time::Instant};

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTrade, FeatureCapability,
        FetchMarketStatsParams, FetchMarketsParams, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams, FundingKind, FundingRateUnit, FundingValue,
        MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSupportedCapabilities, MarketStatsValue,
        MarketStatsWsCapability, OpenInterestValue, PriceValue, UnifiedMarket, UnifiedMarketInfo,
        UnifiedMarketType, Volume24hValue,
    },
};

const ENDPOINT: &str =
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/v2/stats";
const SOURCE: &str = "variational:metadata/v2/stats";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
// Ferris equivalents use a 365-day year. Preserve the native annualized rate
// rather than misinterpreting the swap's ACT/360 payment rate as a daily rate.
const YEAR_MS: u64 = 365 * 86_400_000;

type CachedObservation = Option<(Instant, Result<Arc<Observation>, ExchangeError>)>;

#[derive(Clone)]
pub struct VariationalExchange {
    client: reqwest::Client,
    endpoint: String,
    cache: Arc<Mutex<CachedObservation>>,
}

struct Observation {
    snapshot: Snapshot,
    received_at: Instant,
    wall: u64,
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
        // Share both requests and the original receipt across market-list and
        // stats callers. Cache failures too so clients cannot hammer a 429.
        // Holding the async mutex makes concurrent cold requests single-flight;
        // cancelling the request drops the lock and does not leave a worker.
        let mut cache = self.cache.lock().await;
        if let Some((expires, result)) = cache.as_ref() {
            if Instant::now() < *expires {
                return result.clone();
            }
        }
        let result = self.fetch_observation().await.map(Arc::new);
        let received_at = result
            .as_ref()
            .map_or_else(|_| Instant::now(), |value| value.received_at);
        *cache = Some((received_at + POLL_INTERVAL, result.clone()));
        result
    }

    async fn fetch_observation(&self) -> Result<Observation, ExchangeError> {
        let response = self
            .client
            .get(&self.endpoint)
            .send()
            .await
            .map_err(|error| ExchangeError::UpstreamRequest(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ExchangeError::UpstreamRequest(format!(
                "Variational returned HTTP {}",
                response.status()
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
        Ok(Observation {
            snapshot,
            received_at: Instant::now(),
            wall: now_millis(),
        })
    }
}

#[async_trait]
impl MarketDataExchange for VariationalExchange {
    fn id(&self) -> &'static str {
        "variational"
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
        Ok(MarketStatsSourceSnapshot {
            rows,
            catalog_known: true,
            // num_markets may exceed listings: coverage is the published catalog,
            // not a claim to enumerate unpublished venue markets.
            complete_catalogs: [UnifiedMarketType::Perp].into_iter().collect(),
            contexts_valid: true,
            received_at: Some(observation.received_at),
            field_received_at,
            next_poll_at: observation.received_at + POLL_INTERVAL,
            source_failures: Vec::new(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    listings: Vec<Listing>,
}

#[derive(Debug, Deserialize)]
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
