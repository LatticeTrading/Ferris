//! Native public market-data adapter for Perpl.
//! Uses Perpl's bulk ticker endpoint for all-market observations; never fans out
//! per-market ticker requests. Public REST history does not document trades.
use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityReason, CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTicker, CcxtTrade,
        DataCapability, ExchangeDataCapabilities, FeatureCapability, FetchMarketStatsParams,
        FetchMarketsParams, FetchOhlcvParams, FetchOrderBookParams, FetchTickersParams,
        FetchTradesParams, FundingKind, FundingRateUnit, FundingValue, MarketScope,
        MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSupportedCapabilities, MarketStatsValue,
        MarketStatsWsCapability, OpenInterestValue, PriceValue, RealtimeCapabilities,
        RealtimeCapability, RealtimeMode, UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType,
        Volume24hValue,
    },
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::time::Instant;
const API: &str = "https://app.perpl.xyz/api";
const SOURCE: &str = "perpl:v1/market-data/ticker";
const POLL: Duration = Duration::from_secs(30);
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1024;
#[derive(Clone)]
pub struct PerplExchange {
    client: reqwest::Client,
    api: String,
    cache: Arc<Mutex<Option<(Instant, Arc<Vec<Market>>)>>>,
    stats_cache: Arc<Mutex<Option<(Instant, Arc<(Instant, Vec<MarketStatsRow>)>)>>>,
    chain_id: u64,
}
#[derive(Debug, Clone, Deserialize)]
struct Market {
    id: u64,
    perpetual_id: u64,
    name: String,
    #[allow(dead_code)]
    size_units: String,
    funding_interval_sec: u64,
    config: Config,
    #[serde(default)]
    funding: Option<Funding>,
    #[serde(default)]
    #[allow(dead_code)]
    state: Option<State>,
}
#[derive(Debug, Clone, Deserialize)]
struct Config {
    is_open: bool,
    price_decimals: u32,
    size_decimals: u32,
    #[serde(default)]
    #[allow(dead_code)]
    min_posting_amount: Option<String>,
}
#[derive(Debug, Clone, Deserialize)]
struct Funding {
    rate: i64,
    #[serde(default)]
    at: Option<At>,
    #[serde(default)]
    feb: Option<u64>,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
struct At {
    #[serde(default)]
    b: Option<u64>,
    #[serde(default)]
    t: Option<u64>,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
struct State {
    #[serde(default)]
    orl: Option<i64>,
    #[serde(default)]
    mrk: Option<i64>,
    #[serde(default)]
    lst: Option<i64>,
    #[serde(default)]
    mid: Option<i64>,
    #[serde(default)]
    bid: Option<i64>,
    #[serde(default)]
    ask: Option<i64>,
    #[serde(default)]
    dv: Option<i64>,
    #[serde(default)]
    dva: Option<String>,
    #[serde(default)]
    oi: Option<i64>,
    #[serde(default)]
    at: Option<At>,
}
#[derive(Deserialize)]
struct Context {
    markets: Vec<Market>,
    #[serde(default)]
    chain: Option<Chain>,
}
#[derive(Deserialize)]
struct Chain {
    chain_id: u64,
}
#[derive(Deserialize)]
struct TickerEnvelope {
    #[serde(default)]
    d: HashMap<String, State>,
}
#[derive(Deserialize)]
struct Book {
    #[serde(default)]
    sn: Option<u64>,
    #[serde(default)]
    at: Option<At>,
    #[serde(default)]
    bid: Vec<Level>,
    #[serde(default)]
    ask: Vec<Level>,
}
#[derive(Deserialize)]
struct Level {
    p: i64,
    s: i64,
}
#[derive(Deserialize)]
struct CandleEnvelope {
    #[serde(default)]
    d: Vec<Candle>,
}
#[derive(Deserialize)]
struct Candle {
    t: u64,
    o: i64,
    c: i64,
    h: i64,
    l: i64,
    #[serde(default)]
    v: Option<String>,
}
impl PerplExchange {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(std::env::var("PERPL_API_URL").unwrap_or_else(|_| API.into()))
    }
    pub fn with_base_url(url: impl Into<String>) -> Result<Self, ExchangeError> {
        let api = url.into().trim_end_matches('/').to_string();
        if api.is_empty() {
            return Err(ExchangeError::Internal(
                "Perpl API URL must not be empty".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .user_agent("ferris-market-data-backend/perpl")
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| ExchangeError::Internal(e.to_string()))?;
        Ok(Self {
            client,
            api,
            cache: Arc::new(Mutex::new(None)),
            stats_cache: Arc::new(Mutex::new(None)),
            chain_id: std::env::var("PERPL_CHAIN_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(143),
        })
    }
    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ExchangeError> {
        let r = self
            .client
            .get(format!("{}{}", self.api, path))
            .send()
            .await
            .map_err(|e| ExchangeError::UpstreamRequest(e.to_string()))?;
        let status = r.status();
        if !status.is_success() {
            return Err(ExchangeError::UpstreamRequest(format!(
                "Perpl returned HTTP {status} for {path}"
            )));
        }
        r.json()
            .await
            .map_err(|e| ExchangeError::UpstreamData(format!("invalid Perpl response: {e}")))
    }
    async fn markets(&self) -> Result<Arc<Vec<Market>>, ExchangeError> {
        let mut c = self.cache.lock().await;
        if let Some((until, v)) = c.as_ref() {
            if Instant::now() < *until {
                return Ok(v.clone());
            }
        }
        let context: Context = self.get("/v1/pub/context").await?;
        if context
            .chain
            .as_ref()
            .is_some_and(|x| x.chain_id != self.chain_id)
        {
            return Err(ExchangeError::UpstreamData(
                "Perpl context chain ID does not match configured PERPL_CHAIN_ID".into(),
            ));
        }
        let mut ids = HashSet::new();
        for m in &context.markets {
            if !ids.insert(m.id)
                || m.id != m.perpetual_id
                || m.name.trim().is_empty()
                || m.config.price_decimals > 18
                || m.config.size_decimals > 18
            {
                return Err(ExchangeError::UpstreamData(
                    "invalid or duplicate Perpl market metadata".into(),
                ));
            }
        }
        let v = Arc::new(context.markets);
        *c = Some((Instant::now() + POLL, v.clone()));
        Ok(v)
    }
    async fn resolve(&self, s: &str) -> Result<Market, ExchangeError> {
        let input = s.trim();
        let name = alias_name(input);
        self.markets()
            .await?
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(name) || m.id.to_string() == input)
            .cloned()
            .ok_or_else(|| ExchangeError::BadSymbol(format!("unknown Perpl market `{s}`")))
    }
    async fn ticker(&self) -> Result<TickerEnvelope, ExchangeError> {
        self.get("/v1/market-data/ticker").await
    }
}
fn scale(n: i64, d: u32) -> Option<f64> {
    let v = n as f64 / 10_f64.powi(d as i32);
    v.is_finite().then_some(v)
}
fn market(m: &Market) -> Result<UnifiedMarket, ExchangeError> {
    let id = m.id.to_string();
    Ok(UnifiedMarket {
        exchange: "perpl".into(),
        symbol: format!("{}/AUSD:AUSD", m.name),
        base: m.name.clone(),
        quote: "AUSD".into(),
        market_type: UnifiedMarketType::Perp,
        active: m.config.is_open,
        min_order_size: None,
        tick_size: Some(1.0 / 10_f64.powi(m.config.price_decimals as i32)),
        contract_size: Some(1.0),
        info: UnifiedMarketInfo {
            category: Some("isolated-perpetual".into()),
            raw_symbol: Some(m.name.clone()),
            exchange_symbol: Some(id.clone()),
            ..Default::default()
        },
        identity: Some(crate::models::MarketIdentity {
            market_id: make_market_id("perpl", UnifiedMarketType::Perp, None, None, &id)?,
            exchange_market_id: id,
            category: None,
            dex: None,
            contract_type: Some("perpetual".into()),
            settle: Some("AUSD".into()),
            settlement_asset_id: Some("AUSD".into()),
        }),
    })
}
fn validate_params(v: &Value) -> Result<(), ExchangeError> {
    if v.is_null() || v.as_object().is_some_and(|o| o.is_empty()) {
        Ok(())
    } else {
        Err(ExchangeError::UnsupportedFeature(
            "Perpl accepts no request params".into(),
        ))
    }
}
fn unsupported(reason: &str) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unsupported,
        value: None,
        reason: Some(reason.into()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: Some(SOURCE.into()),
    }
}
fn unavailable(reason: &str) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(reason.into()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: Some(SOURCE.into()),
    }
}
fn price(n: Option<i64>, m: &Market, wall: u64, at: Option<&At>) -> MarketStatsField {
    match n.and_then(|v| scale(v, m.config.price_decimals)) {
        Some(v) if v > 0.0 => MarketStatsField {
            state: MarketStatsFieldState::Available,
            value: Some(MarketStatsValue::Price(PriceValue {
                amount: v.to_string(),
                base_asset: m.name.clone(),
                quote_asset: "AUSD".into(),
            })),
            reason: None,
            exchange_timestamp: at.and_then(|x| x.t),
            received_timestamp: Some(wall),
            source: Some(SOURCE.into()),
        },
        _ => unavailable("missing-or-invalid-value"),
    }
}
#[async_trait]
impl MarketDataExchange for PerplExchange {
    fn id(&self) -> &'static str {
        "perpl"
    }
    fn capabilities(&self) -> ExchangeDataCapabilities {
        let p = vec![UnifiedMarketType::Perp];
        ExchangeDataCapabilities {
            fetch_markets: DataCapability::supported(p.clone(), MarketScope::Catalog),
            fetch_tickers: DataCapability::supported(p.clone(), MarketScope::AllMarkets),
            fetch_trades: DataCapability::unsupported(
                CapabilityReason::VenueDataUnavailable,
                "Perpl documents no public REST trade-history endpoint",
            ),
            fetch_ohlcv: DataCapability::supported(p.clone(), MarketScope::MarketSpecific),
            fetch_order_book: DataCapability::supported(p.clone(), MarketScope::MarketSpecific),
            fetch_market_stats: DataCapability::supported(p.clone(), MarketScope::AllMarkets),
            realtime: RealtimeCapabilities {
                trades: RealtimeCapability::unsupported(
                    CapabilityReason::AdapterNotImplemented,
                    "Perpl realtime requires a native shared-stream owner",
                ),
                orderbook: RealtimeCapability::unsupported(
                    CapabilityReason::AdapterNotImplemented,
                    "Perpl realtime requires a native shared-stream owner",
                ),
                ohlcv: RealtimeCapability::unsupported(
                    CapabilityReason::AdapterNotImplemented,
                    "Perpl realtime requires a native shared-stream owner",
                ),
                marketstats: RealtimeCapability::supported(
                    p,
                    MarketScope::AllMarkets,
                    RealtimeMode::Polling,
                    Some(30_000),
                ),
            },
        }
    }
    async fn fetch_markets(
        &self,
        p: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        validate_params(&p.params)?;
        let mut v = self
            .markets()
            .await?
            .iter()
            .filter(|m| p.include_inactive || m.config.is_open)
            .map(market)
            .collect::<Result<Vec<_>, _>>()?;
        v.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        Ok(v)
    }
    async fn fetch_tickers(&self, p: FetchTickersParams) -> Result<Vec<CcxtTicker>, ExchangeError> {
        validate_params(&p.params)?;
        let ms = self.markets().await?;
        let requested = p.symbols;
        let states = self.ticker().await?;
        let selected = if let Some(symbols) = requested {
            let mut out = Vec::new();
            for s in symbols {
                out.push(self.resolve(&s).await?)
            }
            out
        } else {
            ms.iter().filter(|m| m.config.is_open).cloned().collect()
        };
        Ok(selected
            .into_iter()
            .filter_map(|m| {
                let s = states.d.get(&m.id.to_string())?;
                let price = s.mid.and_then(|v| scale(v, m.config.price_decimals));
                Some(CcxtTicker {
                    info: json!({"marketId":m.id,"state":s}),
                    symbol: format!("{}/AUSD:AUSD", m.name),
                    timestamp: s.at.as_ref().and_then(|a| a.t),
                    datetime: None,
                    high: None,
                    low: None,
                    bid: s.bid.and_then(|v| scale(v, m.config.price_decimals)),
                    bid_volume: None,
                    ask: s.ask.and_then(|v| scale(v, m.config.price_decimals)),
                    ask_volume: None,
                    vwap: None,
                    open: None,
                    close: None,
                    last: s.lst.and_then(|v| scale(v, m.config.price_decimals)),
                    previous_close: None,
                    change: None,
                    percentage: None,
                    average: price,
                    base_volume: s.dv.and_then(|v| scale(v, m.config.size_decimals)),
                    quote_volume: s
                        .dva
                        .as_ref()
                        .and_then(|v| v.parse::<f64>().ok())
                        .map(|v| v / 1_000_000.0),
                })
            })
            .collect())
    }
    async fn fetch_trades(&self, _: FetchTradesParams) -> Result<Vec<CcxtTrade>, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "Perpl has no documented public REST trade history".into(),
        ))
    }
    async fn fetch_ohlcv(&self, p: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        validate_params(&p.params)?;
        let m = self.resolve(&p.symbol).await?;
        let tf = p.timeframe.unwrap_or_else(|| "1m".into());
        let sec = match tf.as_str() {
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
        };
        let limit = p.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 {
            return Err(ExchangeError::BadSymbol(
                "OHLCV limit must be positive".into(),
            ));
        }
        let interval_ms = sec * 1000;
        let end = p.since.unwrap_or_else(now_millis);
        let start = p
            .since
            .unwrap_or_else(|| end.saturating_sub((limit as u64) * interval_ms));
        let from = start / interval_ms * interval_ms;
        let mut out = Vec::new();
        // Perpl caps a candle response at 1024 rows. Page sequentially rather
        // than silently truncating a caller's request or fanning requests out
        // concurrently against a rate-limited public endpoint.
        let mut page_from = from;
        let final_to = from.saturating_add((limit as u64) * interval_ms);
        while page_from < final_to {
            let page_to = page_from
                .saturating_add(MAX_LIMIT as u64 * interval_ms)
                .min(final_to);
            let path = format!(
                "/v1/market-data/{}/candles/{}/{page_from}-{page_to}",
                m.id, sec
            );
            let e: CandleEnvelope = self.get(&path).await?;
            for c in e.d {
                let Some(o) = scale(c.o, m.config.price_decimals) else {
                    continue;
                };
                let Some(close) = scale(c.c, m.config.price_decimals) else {
                    continue;
                };
                let Some(h) = scale(c.h, m.config.price_decimals) else {
                    continue;
                };
                let Some(l) = scale(c.l, m.config.price_decimals) else {
                    continue;
                };
                let volume =
                    c.v.and_then(|v| v.parse::<f64>().ok())
                        .map(|v| v / 1_000_000.0)
                        .filter(|v| v.is_finite() && *v >= 0.0);
                if l <= h && o >= l && o <= h && close >= l && close <= h {
                    out.push((c.t, o, h, l, close, volume));
                }
            }
            if page_to <= page_from {
                break;
            }
            page_from = page_to;
        }
        out.sort_by_key(|r| r.0);
        out.dedup_by_key(|r| r.0);
        out.retain(|r| p.since.is_none_or(|s| r.0 >= s));
        out.truncate(limit);
        Ok(out)
    }
    async fn fetch_order_book(
        &self,
        p: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        validate_params(&p.params)?;
        let m = self.resolve(&p.symbol).await?;
        let depth = p.limit.unwrap_or(100);
        if !(1..=100).contains(&depth) {
            return Err(ExchangeError::BadSymbol(
                "Perpl order book limit must be between 1 and 100".into(),
            ));
        }
        let b: Book = self
            .get(&format!("/v1/market-data/{}/book?levels={depth}", m.id))
            .await?;
        let convert = |ls: Vec<Level>| {
            ls.into_iter()
                .map(|x| {
                    Ok((
                        scale(x.p, m.config.price_decimals)
                            .filter(|v| *v > 0.0)
                            .ok_or_else(|| {
                                ExchangeError::UpstreamData("invalid Perpl book price".into())
                            })?,
                        scale(x.s, m.config.size_decimals)
                            .filter(|v| *v >= 0.0)
                            .ok_or_else(|| {
                                ExchangeError::UpstreamData("invalid Perpl book size".into())
                            })?,
                    ))
                })
                .collect::<Result<Vec<_>, ExchangeError>>()
        };
        Ok(CcxtOrderBook {
            asks: convert(b.ask)?,
            bids: convert(b.bid)?,
            datetime: None,
            timestamp: b.at.and_then(|x| x.t),
            nonce: b.sn,
            symbol: Some(format!("{}/AUSD:AUSD", m.name)),
        })
    }
}
#[async_trait]
impl MarketStatsSource for PerplExchange {
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        validate_params(input)?;
        Ok(json!({}))
    }
    fn catalog_source(&self) -> &'static str {
        "perpl:v1/pub/context"
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
        .map(|n| {
            (
                n,
                FeatureCapability {
                    state: if matches!(
                        n,
                        Funding | MarkPrice | IndexPrice | LastPrice | Volume24h | OpenInterest
                    ) {
                        CapabilityState::Supported
                    } else {
                        CapabilityState::Unsupported
                    },
                    reason: if matches!(
                        n,
                        Funding | MarkPrice | IndexPrice | LastPrice | Volume24h | OpenInterest
                    ) {
                        None
                    } else {
                        Some("volume-units-not-qualified".into())
                    },
                },
            )
        })
        .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: "perpl".into(),
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
            fields: [(UnifiedMarketType::Perp, fields)].into_iter().collect(),
            upstream_mode: "sharedPolling".into(),
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
                "ticker-is-acquired-once-for-all-markets".into(),
                "volume-is-quote-ausd-and-base-size".into(),
            ],
        })
    }
    async fn fetch_market_stats(
        &self,
        _: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        let ms = self.markets().await?;
        let mut cache = self.stats_cache.lock().await;
        let now = Instant::now();
        let (rows, received) = if let Some((until, r)) = cache.as_ref() {
            if now < *until {
                (r.clone(), r.0)
            } else {
                let t = self.ticker().await?;
                let received = Instant::now();
                let wall = now_millis();
                let r = Arc::new((received, stats_rows(&ms, &t.d, wall)));
                *cache = Some((received + POLL, r.clone()));
                (r, received)
            }
        } else {
            let t = self.ticker().await?;
            let received = Instant::now();
            let wall = now_millis();
            let r = Arc::new((received, stats_rows(&ms, &t.d, wall)));
            *cache = Some((received + POLL, r.clone()));
            (r, received)
        };
        let mut times = HashMap::new();
        for row in rows.1.iter() {
            let id = row.market.identity.as_ref().unwrap().market_id.clone();
            times.insert(
                id,
                row.fields
                    .iter()
                    .filter(|(_, f)| f.state == MarketStatsFieldState::Available)
                    .map(|(f, _)| (*f, received))
                    .collect(),
            );
        }
        Ok(MarketStatsSourceSnapshot {
            rows: rows.1.clone(),
            catalog_known: true,
            complete_catalogs: [UnifiedMarketType::Perp].into_iter().collect(),
            contexts_valid: true,
            received_at: Some(received),
            field_received_at: times,
            next_poll_at: received + POLL,
            source_failures: vec![],
        })
    }
}
fn volume(state: Option<&State>, market: &Market) -> MarketStatsField {
    let base = state
        .and_then(|s| s.dv)
        .and_then(|v| scale(v, market.config.size_decimals))
        .filter(|v| v.is_finite() && *v >= 0.0);
    let quote = state
        .and_then(|s| s.dva.as_deref())
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v / 1_000_000.0)
        .filter(|v| v.is_finite() && *v >= 0.0);
    if base.is_none() && quote.is_none() {
        return unavailable("missing-or-invalid-volume");
    }
    MarketStatsField {
        state: MarketStatsFieldState::Available,
        value: Some(MarketStatsValue::Volume24h(Volume24hValue {
            base_volume: base,
            quote_volume: quote,
        })),
        reason: None,
        exchange_timestamp: state.and_then(|s| s.at.as_ref()).and_then(|a| a.t),
        received_timestamp: Some(now_millis()),
        source: Some(SOURCE.into()),
    }
}
fn alias_name(input: &str) -> &str {
    input
        .strip_suffix(":AUSD:AUSD")
        .unwrap_or(input)
        .split('/')
        .next()
        .unwrap_or(input)
}
fn stats_rows(ms: &[Market], data: &HashMap<String, State>, wall: u64) -> Vec<MarketStatsRow> {
    ms.iter()
        .filter(|m| m.config.is_open)
        .filter_map(|m| {
            let s = data.get(&m.id.to_string());
            let mkt = market(m).ok()?;
            let mut fields = BTreeMap::new();
            let funding = match m
                .funding
                .as_ref()
                .and_then(|f| Some((f.rate, f.at.as_ref(), f.feb)))
            {
                Some((r, at, _)) => {
                    let f = MarketStatsField {
                        state: MarketStatsFieldState::Available,
                        value: Some(MarketStatsValue::Funding(FundingValue::new(
                            format!("{r}e-6"),
                            FundingRateUnit::DecimalFraction,
                            FundingKind::CurrentUnclassified,
                            None,
                            m.funding_interval_sec.checked_mul(1000),
                            None,
                            None,
                        ))),
                        reason: None,
                        exchange_timestamp: at.and_then(|x| x.t),
                        received_timestamp: Some(wall),
                        source: Some(SOURCE.into()),
                    };
                    f
                }
                None => unavailable("missing-funding"),
            };
            fields.insert(MarketStatsFieldName::Funding, funding);
            fields.insert(
                MarketStatsFieldName::LastSettledFunding,
                unsupported("settled-funding-not-classified"),
            );
            fields.insert(
                MarketStatsFieldName::MarkPrice,
                price(
                    s.and_then(|x| x.mrk),
                    m,
                    wall,
                    s.and_then(|x| x.at.as_ref()),
                ),
            );
            fields.insert(
                MarketStatsFieldName::IndexPrice,
                price(
                    s.and_then(|x| x.orl),
                    m,
                    wall,
                    s.and_then(|x| x.at.as_ref()),
                ),
            );
            fields.insert(
                MarketStatsFieldName::LastPrice,
                price(
                    s.and_then(|x| x.lst),
                    m,
                    wall,
                    s.and_then(|x| x.at.as_ref()),
                ),
            );
            fields.insert(MarketStatsFieldName::Volume24h, volume(s, m));
            let oi = s
                .and_then(|x| x.oi)
                .and_then(|v| scale(v, m.config.size_decimals));
            fields.insert(
                MarketStatsFieldName::OpenInterest,
                match oi {
                    Some(v) => MarketStatsField {
                        state: MarketStatsFieldState::Available,
                        value: Some(MarketStatsValue::OpenInterest(OpenInterestValue {
                            open_interest_amount: Some(v),
                            open_interest_value: None,
                        })),
                        reason: None,
                        exchange_timestamp: s.and_then(|x| x.at.as_ref()).and_then(|x| x.t),
                        received_timestamp: Some(wall),
                        source: Some(SOURCE.into()),
                    },
                    None => unavailable("missing-open-interest"),
                },
            );
            Some(MarketStatsRow {
                market: mkt,
                fields,
            })
        })
        .collect()
}
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scale_decimals() {
        assert_eq!(scale(796504, 1), Some(79650.4));
        assert_eq!(scale(861741, 5), Some(8.61741));
    }
    #[test]
    fn resolver_aliases_share_one_name_parser() {
        assert_eq!(alias_name("BTC/AUSD:AUSD"), "BTC");
        assert_eq!(alias_name("BTC"), "BTC");
        assert_eq!(alias_name("1"), "1");
    }
    #[test]
    fn valid_candles_sort_and_deduplicate() {
        let mut rows: Vec<CcxtOhlcv> = vec![
            (2_u64, 2.0, 2.0, 2.0, 2.0, None),
            (1_u64, 1.0, 1.0, 1.0, 1.0, None),
            (1_u64, 1.0, 1.0, 1.0, 1.0, None),
        ];
        rows.sort_by_key(|row| row.0);
        rows.dedup_by_key(|row| row.0);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].0 < rows[1].0);
    }
}
