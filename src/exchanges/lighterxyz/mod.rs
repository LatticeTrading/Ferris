use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchMarketsParams, FetchOhlcvParams,
        FetchOrderBookParams, FetchTradesParams, UnifiedMarket, UnifiedMarketInfo,
        UnifiedMarketType,
    },
    ws_shared::{normalize_lighter_timestamp_ms, parse_f64_lossy, parse_u64_lossy},
};

pub const DEFAULT_LIGHTER_MARKETS_URL: &str = "https://explorer.elliot.ai/api/markets";
pub const DEFAULT_LIGHTER_REST_BASE_URL: &str = "https://mainnet.zklighter.elliot.ai";
pub const DEFAULT_LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const DEFAULT_LIGHTER_MARKET_CATALOG_REFRESH_MS: u64 = 90_000;

const DEFAULT_FETCH_ORDER_BOOK_LIMIT: usize = 100;
const MAX_FETCH_ORDER_BOOK_LIMIT: usize = 250;
const DEFAULT_FETCH_TRADES_LIMIT: usize = 100;
const MAX_FETCH_TRADES_LIMIT: usize = 100;
const DEFAULT_FETCH_OHLCV_LIMIT: usize = 200;
const MAX_FETCH_OHLCV_LIMIT: usize = 500;
const LIGHTER_HTTP_USER_AGENT: &str =
    "Mozilla/5.0 (compatible; FerrisMarketDataBackend/0.1; +https://ferris.local)";

pub struct LighterExchange {
    http_client: reqwest::Client,
    rest_base_url: String,
    catalog_service: Arc<LighterMarketCatalogService>,
}

impl LighterExchange {
    pub fn new(
        rest_base_url: String,
        timeout_ms: u64,
        catalog_service: Arc<LighterMarketCatalogService>,
    ) -> Result<Self, ExchangeError> {
        let http_client = build_lighter_http_client(timeout_ms).map_err(|err| {
            ExchangeError::Internal(format!("failed to build Lighter REST client: {err}"))
        })?;

        Ok(Self {
            http_client,
            rest_base_url: rest_base_url.trim_end_matches('/').to_string(),
            catalog_service,
        })
    }

    async fn get_public(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Value, ExchangeError> {
        let endpoint = format!("{}{}", self.rest_base_url, path);
        let response = self
            .http_client
            .get(&endpoint)
            .query(query)
            .send()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(err.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(err.to_string()))?;

        if status != StatusCode::OK {
            return Err(ExchangeError::UpstreamRequest(format!(
                "lighterxyz status={status} body={} query={query:?}",
                truncate(&body, 240),
            )));
        }

        serde_json::from_str::<Value>(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse Lighter response: {err}; body={}",
                truncate(&body, 240)
            ))
        })
    }
}

#[async_trait]
impl MarketDataExchange for LighterExchange {
    fn id(&self) -> &'static str {
        "lighterxyz"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let market = self
            .catalog_service
            .resolve_market(&params.symbol, &params.params)
            .await?;

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_TRADES_LIMIT)
            .clamp(1, MAX_FETCH_TRADES_LIMIT);

        let query = vec![
            ("market_id", market.market_id.to_string()),
            ("limit", requested_limit.to_string()),
        ];

        let response = self.get_public("/api/v1/recentTrades", &query).await?;
        ensure_lighter_code_ok(&response, "recent trades")?;

        let rows = response
            .get("trades")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ExchangeError::UpstreamData(
                    "lighterxyz recent trades response missing `trades`".to_string(),
                )
            })?;

        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            match map_trade_row(row, &market.symbol) {
                Ok(trade) => mapped.push(trade),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map lighterxyz trade row, skipping")
                }
            }
        }

        mapped.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));

        if let Some(since) = params.since {
            mapped.retain(|trade| trade.timestamp.unwrap_or_default() >= since);
        }

        if mapped.len() > requested_limit {
            mapped.truncate(requested_limit);
        }

        Ok(mapped)
    }

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        let market = self
            .catalog_service
            .resolve_market(&params.symbol, &params.params)
            .await?;

        let timeframe = params
            .timeframe
            .unwrap_or_else(|| "1m".to_string())
            .trim()
            .to_string();
        let interval_ms = interval_to_millis(&timeframe).ok_or_else(|| {
            ExchangeError::BadSymbol(format!("unsupported timeframe `{timeframe}`"))
        })?;

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_OHLCV_LIMIT)
            .clamp(1, MAX_FETCH_OHLCV_LIMIT);

        let now = now_millis();
        let until = extract_u64_field(&params.params, "until")
            .or_else(|| extract_u64_field(&params.params, "endTimestamp"))
            .or_else(|| extract_u64_field(&params.params, "end_timestamp"));

        let start_timestamp = params.since.unwrap_or_else(|| {
            now.saturating_sub(interval_ms.saturating_mul(requested_limit as u64))
        });
        let mut end_timestamp = until.unwrap_or(now);
        if params.since.is_some() && until.is_none() {
            end_timestamp =
                start_timestamp.saturating_add(interval_ms.saturating_mul(requested_limit as u64));
        }
        if end_timestamp <= start_timestamp {
            end_timestamp = start_timestamp.saturating_add(interval_ms);
        }

        let mut query = vec![
            ("market_id", market.market_id.to_string()),
            ("resolution", timeframe.clone()),
            ("start_timestamp", start_timestamp.to_string()),
            ("end_timestamp", end_timestamp.to_string()),
            ("count_back", requested_limit.to_string()),
        ];

        if let Some(set_timestamp_to_end) =
            extract_bool_field(&params.params, "set_timestamp_to_end")
                .or_else(|| extract_bool_field(&params.params, "setTimestampToEnd"))
        {
            query.push(("set_timestamp_to_end", set_timestamp_to_end.to_string()));
        }

        let response = self.get_public("/api/v1/candles", &query).await?;
        ensure_lighter_code_ok(&response, "candles")?;

        let rows = response.get("c").and_then(Value::as_array).ok_or_else(|| {
            ExchangeError::UpstreamData("lighterxyz candles response missing `c`".to_string())
        })?;

        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            match map_candle_row(row) {
                Ok(candle) => mapped.push(candle),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map lighterxyz candle row, skipping")
                }
            }
        }

        mapped.sort_by(|left, right| left.0.cmp(&right.0));

        if let Some(since) = params.since {
            mapped.retain(|candle| candle.0 >= since);
        }

        if mapped.len() > requested_limit {
            if params.since.is_some() {
                mapped.truncate(requested_limit);
            } else {
                mapped = mapped.split_off(mapped.len() - requested_limit);
            }
        }

        Ok(mapped)
    }

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        let market = self
            .catalog_service
            .resolve_market(&params.symbol, &params.params)
            .await?;

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_ORDER_BOOK_LIMIT)
            .clamp(1, MAX_FETCH_ORDER_BOOK_LIMIT);

        let query = vec![
            ("market_id", market.market_id.to_string()),
            ("limit", requested_limit.to_string()),
        ];

        let response = self.get_public("/api/v1/orderBookOrders", &query).await?;
        let code = response
            .get("code")
            .and_then(parse_u64_lossy)
            .unwrap_or_default();
        if !matches!(code, 0 | 200) {
            let message = response
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown upstream error");
            return Err(ExchangeError::UpstreamRequest(format!(
                "lighterxyz orderbook returned code {code}: {message}"
            )));
        }

        let mut bids = parse_rest_orderbook_side(response.get("bids"));
        let mut asks = parse_rest_orderbook_side(response.get("asks"));
        bids.sort_by(|left, right| right.0.total_cmp(&left.0));
        asks.sort_by(|left, right| left.0.total_cmp(&right.0));

        if bids.len() > requested_limit {
            bids.truncate(requested_limit);
        }
        if asks.len() > requested_limit {
            asks.truncate(requested_limit);
        }

        let timestamp = max_transaction_time(response.get("bids"))
            .max(max_transaction_time(response.get("asks")));
        let datetime = timestamp.and_then(iso8601_millis);

        Ok(CcxtOrderBook {
            asks,
            bids,
            datetime,
            timestamp,
            nonce: None,
            symbol: Some(market.symbol),
        })
    }

    async fn fetch_markets(
        &self,
        _params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        let catalog = self.catalog_service.get_catalog().await?;
        Ok(catalog
            .markets
            .iter()
            .cloned()
            .map(|market| market.into_unified_market())
            .collect())
    }
}

#[derive(Clone)]
pub struct LighterMarketCatalogService {
    http_client: reqwest::Client,
    markets_url: String,
    refresh_interval: Duration,
    state: Arc<LighterMarketCatalogState>,
}

struct LighterMarketCatalogState {
    snapshot: RwLock<Option<CachedLighterMarketCatalog>>,
    refresh_lock: Mutex<()>,
}

#[derive(Clone)]
struct CachedLighterMarketCatalog {
    catalog: LighterMarketCatalog,
    fetched_at: Instant,
}

impl LighterMarketCatalogService {
    pub fn new(
        timeout_ms: u64,
        markets_url: String,
        refresh_interval_ms: u64,
    ) -> Result<Self, ExchangeError> {
        let http_client = build_lighter_http_client(timeout_ms).map_err(|err| {
            ExchangeError::Internal(format!("failed to build Lighter markets client: {err}"))
        })?;

        Ok(Self {
            http_client,
            markets_url,
            refresh_interval: Duration::from_millis(refresh_interval_ms.max(1)),
            state: Arc::new(LighterMarketCatalogState {
                snapshot: RwLock::new(None),
                refresh_lock: Mutex::new(()),
            }),
        })
    }

    pub async fn get_catalog(&self) -> Result<LighterMarketCatalog, ExchangeError> {
        if let Some(snapshot) = self.cached_snapshot_if_fresh().await {
            return Ok(snapshot.catalog);
        }

        let _refresh_guard = self.state.refresh_lock.lock().await;

        if let Some(snapshot) = self.cached_snapshot_if_fresh().await {
            return Ok(snapshot.catalog);
        }

        let stale_snapshot = self.cached_snapshot().await;
        match self.fetch_catalog().await {
            Ok(catalog) => {
                let snapshot = CachedLighterMarketCatalog {
                    catalog: catalog.clone(),
                    fetched_at: Instant::now(),
                };
                *self.state.snapshot.write().await = Some(snapshot);
                Ok(catalog)
            }
            Err(err) => {
                if let Some(snapshot) = stale_snapshot {
                    tracing::warn!(error = %err, "failed to refresh Lighter market catalog, using stale snapshot");
                    Ok(snapshot.catalog)
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn resolve_market(
        &self,
        symbol: &str,
        params: &Value,
    ) -> Result<LighterMarket, ExchangeError> {
        let catalog = self.get_catalog().await?;

        if let Some(market_id) = extract_market_id(params) {
            return catalog.market_by_id(market_id).ok_or_else(|| {
                ExchangeError::BadSymbol(format!("unknown lighterxyz market id `{market_id}`"))
            });
        }

        catalog.resolve_symbol(symbol)
    }

    async fn cached_snapshot_if_fresh(&self) -> Option<CachedLighterMarketCatalog> {
        let snapshot = self.cached_snapshot().await?;
        (snapshot.fetched_at.elapsed() < self.refresh_interval).then_some(snapshot)
    }

    async fn cached_snapshot(&self) -> Option<CachedLighterMarketCatalog> {
        self.state.snapshot.read().await.clone()
    }

    async fn fetch_catalog(&self) -> Result<LighterMarketCatalog, ExchangeError> {
        let response = self
            .http_client
            .get(&self.markets_url)
            .send()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(err.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(err.to_string()))?;

        if status != StatusCode::OK {
            return Err(ExchangeError::UpstreamRequest(format!(
                "lighterxyz markets status={status} body={}",
                truncate(&body, 240),
            )));
        }

        let rows: Vec<LighterCatalogRow> = serde_json::from_str(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse Lighter markets payload: {err}; body={}",
                truncate(&body, 240)
            ))
        })?;

        LighterMarketCatalog::from_rows(rows)
    }
}

#[derive(Debug, Clone)]
pub struct LighterMarketCatalog {
    pub markets: Vec<LighterMarket>,
    canonical_symbol_to_market_id: HashMap<String, u64>,
    raw_symbol_to_market_id: HashMap<String, u64>,
    market_id_to_market: HashMap<u64, LighterMarket>,
}

impl LighterMarketCatalog {
    fn from_rows(rows: Vec<LighterCatalogRow>) -> Result<Self, ExchangeError> {
        let mut markets = Vec::with_capacity(rows.len());
        let mut canonical_symbol_to_market_id = HashMap::with_capacity(rows.len());
        let mut raw_symbol_to_market_id = HashMap::with_capacity(rows.len());
        let mut market_id_to_market = HashMap::with_capacity(rows.len());

        for row in rows {
            let market = LighterMarket::from_row(row)?;
            canonical_symbol_to_market_id.insert(market.symbol.clone(), market.market_id);
            raw_symbol_to_market_id.insert(
                normalize_lighter_lookup_symbol(&market.raw_symbol)?,
                market.market_id,
            );
            market_id_to_market.insert(market.market_id, market.clone());
            markets.push(market);
        }

        markets.sort_by(|left, right| left.symbol.cmp(&right.symbol));

        Ok(Self {
            markets,
            canonical_symbol_to_market_id,
            raw_symbol_to_market_id,
            market_id_to_market,
        })
    }

    pub fn resolve_symbol(&self, symbol: &str) -> Result<LighterMarket, ExchangeError> {
        let normalized = normalize_lighter_lookup_symbol(symbol)?;

        if let Some(market_id) = self.canonical_symbol_to_market_id.get(&normalized) {
            return self.market_by_id(*market_id).ok_or_else(|| {
                ExchangeError::UpstreamData(format!(
                    "lighterxyz catalog missing market for symbol `{normalized}`"
                ))
            });
        }

        if let Some(market_id) = self.raw_symbol_to_market_id.get(&normalized) {
            return self.market_by_id(*market_id).ok_or_else(|| {
                ExchangeError::UpstreamData(format!(
                    "lighterxyz catalog missing market for raw symbol `{normalized}`"
                ))
            });
        }

        Err(ExchangeError::BadSymbol(format!(
            "unknown lighterxyz symbol `{symbol}`"
        )))
    }

    pub fn market_by_id(&self, market_id: u64) -> Option<LighterMarket> {
        self.market_id_to_market.get(&market_id).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct LighterMarket {
    pub market_id: u64,
    pub raw_symbol: String,
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub market_type: UnifiedMarketType,
}

impl LighterMarket {
    fn from_row(row: LighterCatalogRow) -> Result<Self, ExchangeError> {
        let raw_symbol = row.symbol.trim().to_string();
        if raw_symbol.is_empty() {
            return Err(ExchangeError::UpstreamData(
                "lighterxyz market catalog row missing symbol".to_string(),
            ));
        }

        if raw_symbol.contains('/') {
            let (base, quote) = split_spot_symbol(&raw_symbol)?;
            return Ok(Self {
                market_id: row.market_index,
                raw_symbol,
                symbol: format!("{base}/{quote}"),
                base,
                quote,
                market_type: UnifiedMarketType::Spot,
            });
        }

        let base = sanitize_asset(&raw_symbol)?;
        Ok(Self {
            market_id: row.market_index,
            raw_symbol,
            symbol: format!("{base}/USD"),
            base,
            quote: "USD".to_string(),
            market_type: UnifiedMarketType::Perp,
        })
    }

    fn into_unified_market(self) -> UnifiedMarket {
        UnifiedMarket {
            identity: None,
            exchange: "lighterxyz".to_string(),
            symbol: self.symbol,
            base: self.base,
            quote: self.quote,
            market_type: self.market_type,
            active: true,
            min_order_size: None,
            tick_size: None,
            contract_size: None,
            info: UnifiedMarketInfo {
                category: None,
                raw_symbol: Some(self.raw_symbol),
                exchange_symbol: Some(self.market_id.to_string()),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct LighterCatalogRow {
    symbol: String,
    market_index: u64,
}

fn parse_rest_orderbook_side(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(price) = row.get("price").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(size) = row.get("remaining_base_amount").and_then(parse_f64_lossy) else {
            continue;
        };
        parsed.push((price, size));
    }

    parsed
}

fn build_lighter_http_client(timeout_ms: u64) -> Result<reqwest::Client, reqwest::Error> {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/plain, */*"),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(LIGHTER_HTTP_USER_AGENT),
    );

    reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .default_headers(headers)
        .build()
}

fn map_trade_row(raw: &Value, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = raw
        .get("transaction_time")
        .and_then(parse_u64_lossy)
        .map(normalize_lighter_timestamp_ms)
        .or_else(|| {
            raw.get("timestamp")
                .and_then(parse_u64_lossy)
                .map(normalize_lighter_timestamp_ms)
        })
        .ok_or_else(|| {
            ExchangeError::UpstreamData(
                "lighterxyz trade missing timestamp fields `transaction_time`/`timestamp`"
                    .to_string(),
            )
        })?;

    let price = raw.get("price").and_then(parse_f64_lossy).ok_or_else(|| {
        ExchangeError::UpstreamData("lighterxyz trade missing `price`".to_string())
    })?;
    let amount = raw.get("size").and_then(parse_f64_lossy).ok_or_else(|| {
        ExchangeError::UpstreamData("lighterxyz trade missing `size`".to_string())
    })?;

    let datetime = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid timestamp `{timestamp}`")))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    let side = raw
        .get("is_maker_ask")
        .and_then(Value::as_bool)
        .map(|is_maker_ask| {
            if is_maker_ask {
                "buy".to_string()
            } else {
                "sell".to_string()
            }
        });
    let cost = raw
        .get("usd_amount")
        .and_then(parse_f64_lossy)
        .or(Some(price * amount));
    let trade_type = raw
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    Ok(CcxtTrade {
        info: raw.clone(),
        amount: Some(amount),
        datetime: Some(datetime),
        id: raw.get("trade_id").and_then(stringify_json_value),
        order: None,
        price: Some(price),
        timestamp: Some(timestamp),
        trade_type,
        side,
        symbol: Some(symbol.to_string()),
        taker_or_maker: None,
        cost,
        fee: None,
    })
}

fn map_candle_row(raw: &Value) -> Result<CcxtOhlcv, ExchangeError> {
    let timestamp = raw
        .get("t")
        .and_then(parse_u64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("lighterxyz candle missing `t`".to_string()))?;
    let open = raw
        .get("o")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("lighterxyz candle missing `o`".to_string()))?;
    let high = raw
        .get("h")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("lighterxyz candle missing `h`".to_string()))?;
    let low = raw
        .get("l")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("lighterxyz candle missing `l`".to_string()))?;
    let close = raw
        .get("c")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("lighterxyz candle missing `c`".to_string()))?;
    let volume = raw.get("v").and_then(parse_f64_lossy).unwrap_or(0.0);

    Ok((timestamp, open, high, low, close, volume))
}

fn max_transaction_time(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_array).and_then(|rows| {
        rows.iter()
            .filter_map(|row| row.get("transaction_time").and_then(parse_u64_lossy))
            .max()
    })
}

pub fn normalize_lighter_lookup_symbol(symbol: &str) -> Result<String, ExchangeError> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeError::BadSymbol(
            "symbol cannot be empty".to_string(),
        ));
    }

    let core = trimmed.split(':').next().unwrap_or_default().trim();
    if core.is_empty() {
        return Err(ExchangeError::BadSymbol(format!(
            "invalid lighterxyz symbol `{symbol}`"
        )));
    }

    if core.contains('/') {
        let (base, quote) = split_spot_symbol(core)?;
        return Ok(format!("{base}/{quote}"));
    }

    sanitize_asset(core)
}

fn split_spot_symbol(symbol: &str) -> Result<(String, String), ExchangeError> {
    let mut parts = symbol.split('/');
    let base = sanitize_asset(parts.next().unwrap_or_default())?;
    let quote = sanitize_asset(parts.next().unwrap_or_default())?;

    if parts.next().is_some() {
        return Err(ExchangeError::BadSymbol(format!(
            "invalid lighterxyz spot symbol `{symbol}`"
        )));
    }

    Ok((base, quote))
}

fn sanitize_asset(value: &str) -> Result<String, ExchangeError> {
    let normalized = value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();

    if normalized.is_empty() {
        return Err(ExchangeError::BadSymbol(
            "lighterxyz asset must be non-empty".to_string(),
        ));
    }

    Ok(normalized)
}

fn extract_market_id(params: &Value) -> Option<u64> {
    params
        .get("market_id")
        .or_else(|| params.get("marketId"))
        .and_then(parse_u64_lossy)
}

fn extract_u64_field(params: &Value, key: &str) -> Option<u64> {
    params.get(key).and_then(parse_u64_lossy)
}

fn extract_bool_field(params: &Value, key: &str) -> Option<bool> {
    params.get(key).and_then(|value| match value {
        Value::Bool(boolean) => Some(*boolean),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "y" | "on" => Some(true),
            "false" | "0" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    })
}

fn interval_to_millis(timeframe: &str) -> Option<u64> {
    match timeframe {
        "1m" => Some(60_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "30m" => Some(1_800_000),
        "1h" => Some(3_600_000),
        "4h" => Some(14_400_000),
        "12h" => Some(43_200_000),
        "1d" => Some(86_400_000),
        "1w" => Some(604_800_000),
        _ => None,
    }
}

fn ensure_lighter_code_ok(response: &Value, endpoint_label: &str) -> Result<(), ExchangeError> {
    let code = response
        .get("code")
        .and_then(parse_u64_lossy)
        .unwrap_or_default();
    if matches!(code, 0 | 200) {
        return Ok(());
    }

    let message = response
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown upstream error");
    Err(ExchangeError::UpstreamRequest(format!(
        "lighterxyz {endpoint_label} returned code {code}: {message}"
    )))
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn stringify_json_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for ch in value.chars().take(max_chars) {
        output.push(ch);
    }
    if value.chars().count() > max_chars {
        output.push_str("...");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_maps_perp_and_spot_symbols() {
        let catalog = LighterMarketCatalog::from_rows(vec![
            LighterCatalogRow {
                symbol: "APEX".to_string(),
                market_index: 86,
            },
            LighterCatalogRow {
                symbol: "LINK/USDC".to_string(),
                market_index: 2050,
            },
        ])
        .expect("catalog should build");

        let apex = catalog
            .resolve_symbol("APEX/USD")
            .expect("perp canonical symbol should resolve");
        assert_eq!(apex.market_id, 86);
        assert_eq!(apex.raw_symbol, "APEX");
        assert_eq!(apex.symbol, "APEX/USD");

        let apex_raw = catalog
            .resolve_symbol("APEX")
            .expect("perp raw symbol should resolve");
        assert_eq!(apex_raw.market_id, 86);

        let link = catalog
            .resolve_symbol("LINK/USDC")
            .expect("spot symbol should resolve");
        assert_eq!(link.market_id, 2050);
        assert_eq!(link.symbol, "LINK/USDC");

        let by_id = catalog
            .market_by_id(2050)
            .expect("market id should resolve");
        assert_eq!(by_id.symbol, "LINK/USDC");
    }

    #[test]
    fn lighter_market_into_unified_market_uses_expected_mapping() {
        let market = LighterMarket::from_row(LighterCatalogRow {
            symbol: "APEX".to_string(),
            market_index: 86,
        })
        .expect("market should map");

        let unified = market.into_unified_market();
        assert_eq!(unified.exchange, "lighterxyz");
        assert_eq!(unified.symbol, "APEX/USD");
        assert_eq!(unified.base, "APEX");
        assert_eq!(unified.quote, "USD");
        assert!(matches!(unified.market_type, UnifiedMarketType::Perp));
        assert_eq!(unified.info.raw_symbol.as_deref(), Some("APEX"));
        assert_eq!(unified.info.exchange_symbol.as_deref(), Some("86"));
    }

    #[test]
    fn rest_orderbook_side_uses_remaining_base_amount() {
        let rows = parse_rest_orderbook_side(Some(&serde_json::json!([
            {
                "price": "3327.46",
                "remaining_base_amount": "29.0915",
                "transaction_time": 1766434222583u64
            }
        ])));

        assert_eq!(rows, vec![(3327.46, 29.0915)]);
    }

    #[test]
    fn maps_recent_trade_to_ccxt_shape() {
        let raw = serde_json::json!({
            "trade_id": 14035051,
            "type": "trade",
            "price": "3335.65",
            "size": "0.1187",
            "usd_amount": "13.67",
            "is_maker_ask": false,
            "timestamp": 1722339648u64,
            "transaction_time": 1722339648123u64,
        });

        let mapped = map_trade_row(&raw, "ETH/USD").expect("trade should map");
        assert_eq!(mapped.id.as_deref(), Some("14035051"));
        assert_eq!(mapped.trade_type.as_deref(), Some("trade"));
        assert_eq!(mapped.side.as_deref(), Some("sell"));
        assert_eq!(mapped.symbol.as_deref(), Some("ETH/USD"));
        assert_eq!(mapped.price, Some(3335.65));
        assert_eq!(mapped.amount, Some(0.1187));
        assert_eq!(mapped.cost, Some(13.67));
        assert_eq!(mapped.timestamp, Some(1_722_339_648_123));
    }

    #[test]
    fn maps_recent_trade_microsecond_transaction_time_to_millis() {
        let raw = serde_json::json!({
            "trade_id": 1,
            "type": "trade",
            "price": "100",
            "size": "2",
            "usd_amount": "200",
            "is_maker_ask": true,
            "transaction_time": 1773455182331874u64,
        });

        let mapped = map_trade_row(&raw, "BTC/USD").expect("trade should map");
        assert_eq!(mapped.timestamp, Some(1_773_455_182_331));
        assert_eq!(mapped.side.as_deref(), Some("buy"));
    }

    #[test]
    fn maps_candle_row_and_defaults_missing_volume_to_zero() {
        let raw = serde_json::json!({
            "t": 1700000060000u64,
            "o": 100.0,
            "h": 101.5,
            "l": 99.8,
            "c": 101.2,
        });

        let mapped = map_candle_row(&raw).expect("candle should map");
        assert_eq!(mapped, (1_700_000_060_000, 100.0, 101.5, 99.8, 101.2, 0.0));
    }

    #[test]
    fn recognizes_supported_lighter_timeframes() {
        assert_eq!(interval_to_millis("1m"), Some(60_000));
        assert_eq!(interval_to_millis("1h"), Some(3_600_000));
        assert_eq!(interval_to_millis("1w"), Some(604_800_000));
        assert_eq!(interval_to_millis("3m"), None);
    }
}
