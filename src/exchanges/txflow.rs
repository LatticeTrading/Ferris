//! Native public market-data adapter for TxFlow perpetuals.
//!
//! TxFlow's API is Hyperliquid-*shaped* but not compatible: numeric instrument
//! ids in requests, an `{success, code, data}` envelope selected by
//! `x-api-version`, percent funding, a mandatory client keepalive, and REST
//! and WebSocket trade ids from different id spaces. It therefore never goes
//! through the CCXT Hyperliquid path.
//!
//! Tickers and market stats come from one shared `/ws allAssetCtxs`
//! subscription; there is no per-market REST fan-out anywhere. Spot (live
//! upstream, ids >= 10000) and outcome markets are out of scope.
use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTicker, CcxtTrade, DataCapability,
        ExchangeDataCapabilities, FeatureCapability, FetchMarketStatsParams, FetchMarketsParams,
        FetchOhlcvParams, FetchOrderBookParams, FetchTickersParams, FetchTradesParams, FundingKind,
        FundingRateUnit, FundingValue, MarketIdentity, MarketScope,
        MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSupportedCapabilities, MarketStatsValue,
        MarketStatsWsCapability, OpenInterestValue, PriceValue, RealtimeCapabilities,
        RealtimeCapability, RealtimeMode, UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType,
    },
    realtime::{native::SharedHub, RealtimeUpdate},
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
mod realtime;
mod wire;

const API: &str = "https://rpc.txflow.com";
const WS: &str = "wss://rpc.txflow.com/ws";
const API_WS: &str = "wss://rpc.txflow.com/api-ws";
/// Response shapes depend on this header (without it `marketTicker` returns a
/// bare payload). A bump is a contract change, not a config knob.
pub(crate) const API_VERSION: &str = "2026-08-16";
const SOURCE: &str = "txflow:ws/allAssetCtxs";
const CATALOG_SOURCE: &str = "txflow:info/perpMeta";
const CATALOG_TTL: Duration = Duration::from_secs(300);
/// Stats polling reads only the local WS cache, never upstream.
const STATS_POLL: Duration = Duration::from_secs(5);
const CTX_FRESH: Duration = Duration::from_secs(30);
const CTX_WAIT: Duration = Duration::from_secs(10);
const CTX_IDLE: Duration = Duration::from_secs(300);
const REST_BOOK_LEVELS: usize = 20;
const REST_TRADES: usize = 300;
const DEFAULT_LIMIT: usize = 100;
const MAX_OHLCV_LIMIT: usize = 10_000;
/// Upstream silently returns `[]` above 3000 bars; stay well below.
const CANDLE_PAGE: u64 = 2500;
const TRADE_ID_NOTE: &str = "TxFlow REST and WebSocket trade ids are different id spaces; the same fill has different ids. Merge sources by content (time, price, size, side), not id";

#[derive(Clone)]
pub struct TxflowExchange {
    client: reqwest::Client,
    api: String,
    network: String,
    network_key: String,
    catalog: Arc<Mutex<CatalogCache>>,
    /// Last-good `marketTags` labels; a tags failure never fails the catalog.
    labels: Arc<parking_lot::Mutex<LabelCache>>,
    ctxs: Arc<wire::CtxStore>,
    ctx_keeper: Arc<Mutex<Option<JoinHandle<()>>>>,
    node: Arc<SharedHub<realtime::TxProtocol>>,
    api_ws: Arc<SharedHub<realtime::TxProtocol>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Market {
    pub id: u64,
    pub name: String,
    pub base: String,
    pub quote: String,
    pub tick: Decimal,
    pub step: Decimal,
    pub delisted: bool,
    pub halted: bool,
    pub category: Option<&'static str>,
    pub tags: Vec<String>,
    pub raw: Value,
}
impl Market {
    pub fn symbol(&self) -> String {
        format!("{}/{}:{}", self.base, self.quote, self.quote)
    }
    /// `/api-ws candle` echoes `s` as the subscribed id, then the base name.
    pub fn matches_echo(&self, s: &str) -> bool {
        s.parse::<u64>().ok() == Some(self.id)
            || s.eq_ignore_ascii_case(&self.base)
            || s.eq_ignore_ascii_case(&self.name)
    }
}

struct Catalog {
    markets: Vec<Arc<Market>>,
    by_id: HashMap<u64, Arc<Market>>,
    /// Upper-cased alias -> market; `None` when the alias is ambiguous.
    by_alias: HashMap<String, Option<Arc<Market>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaRow {
    index: u64,
    name: String,
    base_currency: String,
    quote_currency: String,
    currency_type: String,
    price_tick: String,
    base_precision: String,
    sz_decimals: i32,
    #[serde(default)]
    delisted: bool,
    #[serde(default)]
    halt_trading: bool,
}

type Labels = HashMap<u64, (Option<&'static str>, Vec<String>)>;
type CatalogCache = Option<(Instant, Arc<Catalog>)>;
/// `(marketTags version, labels)`.
type LabelCache = Option<(String, Arc<Labels>)>;

impl TxflowExchange {
    pub fn new() -> Result<Self, ExchangeError> {
        let api = std::env::var("TXFLOW_API_URL").unwrap_or_else(|_| API.into());
        let ws = std::env::var("TXFLOW_WS_URL").ok();
        let api_ws = std::env::var("TXFLOW_API_WS_URL").ok();
        let network = std::env::var("TXFLOW_NETWORK").ok();
        let network = match network {
            Some(n) => n,
            None => network_of(&api)
                .ok_or_else(|| {
                    ExchangeError::Internal("set TXFLOW_NETWORK for a custom TXFLOW_API_URL".into())
                })?
                .into(),
        };
        let (dws, dapi_ws) = default_ws(&network);
        Self::with_urls(
            api,
            ws.unwrap_or_else(|| dws.into()),
            api_ws.unwrap_or_else(|| dapi_ws.into()),
            &network,
        )
    }
    /// Independent network-scoped adapter (catalog, caches, sockets); also
    /// used with mock servers.
    pub fn with_urls(
        api: impl Into<String>,
        ws: impl Into<String>,
        api_ws: impl Into<String>,
        network: &str,
    ) -> Result<Self, ExchangeError> {
        let api = api.into().trim_end_matches('/').to_string();
        let (ws, api_ws) = (ws.into(), api_ws.into());
        if network.trim().is_empty() {
            return Err(ExchangeError::Internal("TxFlow network must be set".into()));
        }
        for url in [&ws, &api_ws] {
            let parsed =
                reqwest::Url::parse(url).map_err(|e| ExchangeError::Internal(e.to_string()))?;
            if !matches!(parsed.scheme(), "ws" | "wss") {
                return Err(ExchangeError::Internal(
                    "TxFlow WS URLs must use ws/wss".into(),
                ));
            }
        }
        if api.is_empty() {
            return Err(ExchangeError::Internal(
                "TxFlow API URL must not be empty".into(),
            ));
        }
        // Mixing testnet sockets with mainnet REST (or vice versa) would
        // silently pair one network's ids with another's data.
        for url in [&api, &ws, &api_ws] {
            if network_of(url).is_some_and(|n| n != network) {
                return Err(ExchangeError::Internal(format!(
                    "TxFlow URL `{url}` does not belong to network `{network}`"
                )));
            }
        }
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-api-version",
            reqwest::header::HeaderValue::from_static(API_VERSION),
        );
        let client = reqwest::Client::builder()
            .user_agent("ferris-market-data-backend/txflow")
            .default_headers(headers)
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| ExchangeError::Internal(e.to_string()))?;
        Ok(Self {
            client,
            network_key: json!([network, &api, &ws, &api_ws]).to_string(),
            network: network.into(),
            api,
            catalog: Arc::new(Mutex::new(None)),
            labels: Arc::new(parking_lot::Mutex::new(None)),
            ctxs: Arc::new(wire::CtxStore::default()),
            ctx_keeper: Arc::new(Mutex::new(None)),
            node: Arc::new(SharedHub::new(
                ws,
                realtime::TxProtocol {
                    endpoint: realtime::Endpoint::Node,
                },
            )),
            api_ws: Arc::new(SharedHub::new(
                api_ws,
                realtime::TxProtocol {
                    endpoint: realtime::Endpoint::Api,
                },
            )),
        })
    }

    /// `POST /info` with envelope checking.
    async fn info<T: for<'de> Deserialize<'de>>(&self, body: Value) -> Result<T, ExchangeError> {
        let kind = body["type"].as_str().unwrap_or("info").to_string();
        let response = self
            .client
            .post(format!("{}/info", self.api))
            .json(&body)
            .send()
            .await
            .map_err(|e| ExchangeError::UpstreamRequest(e.to_string()))?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map(|v| format!("; retry after {v}s"))
                .unwrap_or_default();
            return Err(ExchangeError::UpstreamRequest(format!(
                "TxFlow rate limited `{kind}`{retry}"
            )));
        }
        let text = response
            .text()
            .await
            .map_err(|e| ExchangeError::UpstreamRequest(e.to_string()))?;
        // Error envelopes also arrive with HTTP 403 (disabled/not allowed).
        let envelope: Value = serde_json::from_str(&text).map_err(|_| {
            ExchangeError::UpstreamRequest(format!(
                "TxFlow returned HTTP {status} for `{kind}`: {}",
                text.chars().take(200).collect::<String>()
            ))
        })?;
        match envelope.get("success").and_then(Value::as_bool) {
            Some(true) => serde_json::from_value(envelope.get("data").cloned().unwrap_or_default())
                .map_err(|e| ExchangeError::UpstreamData(format!("invalid TxFlow `{kind}`: {e}"))),
            Some(false) => {
                let code = envelope.get("code").and_then(Value::as_i64).unwrap_or(-1);
                let message = envelope
                    .get("message")
                    .or_else(|| envelope.get("response"))
                    .and_then(Value::as_str)
                    .unwrap_or("no message");
                Err(match code {
                    10000 => {
                        ExchangeError::UnsupportedFeature(format!("TxFlow `{kind}`: {message}"))
                    }
                    21001 => ExchangeError::UpstreamData(format!("TxFlow `{kind}`: {message}")),
                    _ => ExchangeError::UpstreamRequest(format!(
                        "TxFlow `{kind}` failed ({code}): {message}"
                    )),
                })
            }
            None => Err(ExchangeError::UpstreamData(format!(
                "TxFlow `{kind}` response has no envelope (HTTP {status})"
            ))),
        }
    }

    async fn catalog(&self) -> Result<Arc<Catalog>, ExchangeError> {
        // Held across the fetch: single-flight refresh.
        let mut cache = self.catalog.lock().await;
        if let Some((until, c)) = cache.as_ref() {
            if Instant::now() < *until {
                return Ok(c.clone());
            }
        }
        let meta: Value = match self
            .info(json!({"type":"perpMeta","includeDelisted":true}))
            .await
        {
            Ok(meta) => meta,
            // Retain last-good data rather than failing every caller.
            Err(error) => match cache.as_ref() {
                Some((_, c)) => {
                    tracing::warn!(%error, "TxFlow perpMeta refresh failed; serving last-good catalog");
                    return Ok(c.clone());
                }
                None => return Err(error),
            },
        };
        let labels = self.labels().await;
        let catalog = Arc::new(build_catalog(&meta, &labels)?);
        if let Some((_, old)) = cache.as_ref() {
            let gone = old
                .by_id
                .keys()
                .filter(|id| !catalog.by_id.contains_key(id))
                .count();
            if gone > 0 {
                tracing::warn!(gone, "TxFlow instruments disappeared from perpMeta");
            }
        }
        *cache = Some((Instant::now() + CATALOG_TTL, catalog.clone()));
        Ok(catalog)
    }

    async fn labels(&self) -> Arc<Labels> {
        match self.info::<Value>(json!({"type":"marketTags"})).await {
            Ok(tags) => {
                let version = tags["version"].as_str().unwrap_or_default().to_string();
                if let Some((v, l)) = self.labels.lock().as_ref() {
                    if !version.is_empty() && *v == version {
                        return l.clone();
                    }
                }
                let labels = Arc::new(parse_labels(&tags));
                *self.labels.lock() = Some((version, labels.clone()));
                labels
            }
            Err(error) => {
                tracing::warn!(%error, "TxFlow marketTags failed; keeping last-good labels");
                self.labels
                    .lock()
                    .as_ref()
                    .map(|(_, l)| l.clone())
                    .unwrap_or_default()
            }
        }
    }

    async fn resolve(&self, input: &str) -> Result<Arc<Market>, ExchangeError> {
        resolve_in(&*self.catalog().await?, input)
    }

    /// Fresh `allAssetCtxs` rows from the shared subscription, starting it on
    /// first use. The lease is released after `CTX_IDLE` without readers.
    async fn asset_ctxs(&self) -> Result<wire::CtxRows, ExchangeError> {
        self.ctxs.touch();
        {
            let mut keeper = self.ctx_keeper.lock().await;
            if keeper.as_ref().is_none_or(JoinHandle::is_finished) {
                let key = json!([self.network_key, "allAssetCtxs"]).to_string();
                let receiver = self
                    .node
                    .subscribe(key, realtime::ctx_spec(self.ctxs.clone()))?;
                let store = self.ctxs.clone();
                *keeper = Some(tokio::spawn(async move {
                    let mut receiver = receiver;
                    loop {
                        tokio::select! {
                            update = receiver.recv() => match update {
                                Ok(RealtimeUpdate::Error(error)) => store.fail(&error),
                                Ok(_) => {}
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                            },
                            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                                if store.idle_for().is_none_or(|idle| idle >= CTX_IDLE) {
                                    return; // drops the lease; the socket closes if idle
                                }
                            }
                        }
                    }
                }));
            }
        }
        let deadline = Instant::now() + CTX_WAIT;
        loop {
            let notified = self.ctxs.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.ctxs.read(CTX_FRESH) {
                Ok(rows) => return Ok(rows),
                Err(error) if Instant::now() >= deadline => {
                    return Err(ExchangeError::UpstreamRequest(format!(
                        "timed out waiting for TxFlow allAssetCtxs{}",
                        error.map(|e| format!(": {e}")).unwrap_or_default()
                    )))
                }
                Err(_) => {
                    let _ = tokio::time::timeout_at(deadline, notified).await;
                }
            }
        }
    }
}

fn network_of(url: &str) -> Option<&'static str> {
    let host = reqwest::Url::parse(url)
        .ok()?
        .host_str()?
        .to_ascii_lowercase();
    if host == "txflow-testnet.xyz" || host.ends_with(".txflow-testnet.xyz") {
        Some("testnet")
    } else if host == "txflow.com" || host.ends_with(".txflow.com") {
        Some("mainnet")
    } else {
        None
    }
}
fn default_ws(network: &str) -> (&'static str, &'static str) {
    if network == "testnet" {
        (
            "wss://rpc.txflow-testnet.xyz/ws",
            "wss://rpc.txflow-testnet.xyz/api-ws",
        )
    } else {
        (WS, API_WS)
    }
}

/// Asset-class display label (D3). The mapping table is the contract: new
/// upstream tag names map to `None` until added deliberately.
fn tag_label(name: &str) -> Option<(&'static str, bool)> {
    Some(match name {
        "Stocks" => ("stock", true),
        "Indices" => ("index", true),
        "Commodities" => ("commodity", true),
        "FX" => ("fx", true),
        "Pre-IPO" => ("pre-ipo", true),
        "Layer 1" | "Layer 2" | "DeFi" | "AI" | "Meme" | "RWA" | "Others" => ("crypto", false),
        _ => return None,
    })
}
fn parse_labels(tags: &Value) -> Labels {
    let names: HashMap<u64, String> = tags["l2"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| Some((t["id"].as_u64()?, t["name"].as_str()?.to_string())))
        .collect();
    let mut unknown = HashSet::new();
    let mut out = Labels::new();
    for row in tags["instrumentTags"].as_array().into_iter().flatten() {
        let Some(id) = wire::id_of(row.get("instrumentId")) else {
            continue;
        };
        let tag_names: Vec<String> = row["tagIds"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| names.get(&t.as_u64()?).cloned())
            .collect();
        let mut tradfi = HashSet::new();
        let mut crypto = false;
        let mut unmapped = false;
        for name in &tag_names {
            match tag_label(name) {
                Some((label, true)) => {
                    tradfi.insert(label);
                }
                Some((_, false)) => crypto = true,
                None => {
                    unmapped = true;
                    unknown.insert(name.clone());
                }
            }
        }
        // TradFi wins over crypto; two different TradFi classes, or any
        // unmapped tag without a TradFi class, is ambiguous.
        let label = match tradfi.len() {
            1 => tradfi.into_iter().next(),
            0 if crypto && !unmapped => Some("crypto"),
            _ => None,
        };
        out.insert(id, (label, tag_names));
    }
    if !unknown.is_empty() {
        tracing::warn!(
            ?unknown,
            "unmapped TxFlow marketTags names; label left unset"
        );
    }
    out
}

fn build_catalog(meta: &Value, labels: &Labels) -> Result<Catalog, ExchangeError> {
    let rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeError::UpstreamData("TxFlow perpMeta has no universe".into()))?;
    let mut markets = Vec::with_capacity(rows.len());
    let mut by_id = HashMap::new();
    for raw in rows {
        let row: MetaRow = match serde_json::from_value(raw.clone()) {
            Ok(row) => row,
            Err(error) => {
                tracing::warn!(%error, "skipping malformed TxFlow perpMeta row");
                continue;
            }
        };
        // Perps only (D8): `/` names are spot pairs.
        if row.name.contains('/') || !row.name.contains('-') {
            continue;
        }
        if row.currency_type != "usdc"
            || row.quote_currency != "USDC"
            || row.base_currency.trim().is_empty()
        {
            tracing::warn!(name = %row.name, "skipping TxFlow market with unknown quote/currency type");
            continue;
        }
        let tick = Decimal::from_str(&row.price_tick)
            .ok()
            .filter(|d| *d > Decimal::ZERO);
        // `basePrecision` is the size step (e.g. "100" for szDecimals -2);
        // prefer it over deriving one from szDecimals.
        let step = Decimal::from_str(&row.base_precision)
            .ok()
            .filter(|d| *d > Decimal::ZERO)
            .or_else(|| {
                (row.sz_decimals.unsigned_abs() <= 18).then(|| {
                    if row.sz_decimals >= 0 {
                        Decimal::new(1, row.sz_decimals as u32)
                    } else {
                        Decimal::from(10u64.pow(row.sz_decimals.unsigned_abs()))
                    }
                })
            });
        let (Some(tick), Some(step)) = (tick, step) else {
            tracing::warn!(name = %row.name, "skipping TxFlow market with invalid tick/step");
            continue;
        };
        let (category, tags) = labels.get(&row.index).cloned().unwrap_or_default();
        let market = Arc::new(Market {
            id: row.index,
            name: row.name,
            base: row.base_currency,
            quote: row.quote_currency,
            tick,
            step,
            delisted: row.delisted,
            halted: row.halt_trading,
            category,
            tags,
            raw: raw.clone(),
        });
        if by_id.insert(market.id, market.clone()).is_some() {
            return Err(ExchangeError::UpstreamData(format!(
                "duplicate TxFlow instrument index {}",
                market.id
            )));
        }
        markets.push(market);
    }
    let mut by_alias: HashMap<String, Option<Arc<Market>>> = HashMap::new();
    for m in &markets {
        let mut aliases = HashSet::new();
        aliases.insert(m.base.to_ascii_uppercase());
        aliases.insert(m.name.to_ascii_uppercase());
        aliases.insert(m.symbol().to_ascii_uppercase());
        for alias in aliases {
            by_alias
                .entry(alias)
                .and_modify(|e| {
                    if e.as_ref().is_some_and(|x| x.id != m.id) {
                        *e = None
                    }
                })
                .or_insert_with(|| Some(m.clone()));
        }
    }
    Ok(Catalog {
        markets,
        by_id,
        by_alias,
    })
}

/// One parser for REST and realtime: `BASE/USDC:USDC`, `BASE`, `BASE-USDC`,
/// or the numeric id. Spot-shaped input (`BASE/USDC`) and spot ids never
/// resolve to a perp.
fn resolve_in(catalog: &Catalog, input: &str) -> Result<Arc<Market>, ExchangeError> {
    let s = input.trim();
    let upper = s.to_ascii_uppercase();
    if s.contains('/') && !s.contains(':') {
        return Err(ExchangeError::BadSymbol(format!(
            "`{s}` is a spot symbol; TxFlow spot markets are not supported (use BASE/USDC:USDC for perpetuals)"
        )));
    }
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        let id: u64 = s
            .parse()
            .map_err(|_| ExchangeError::BadSymbol(format!("invalid TxFlow market id `{s}`")))?;
        if catalog.by_alias.contains_key(&upper) {
            return Err(ExchangeError::BadSymbol(format!(
                "ambiguous TxFlow symbol `{s}` (both an id and a name)"
            )));
        }
        return catalog.by_id.get(&id).cloned().ok_or_else(|| {
            ExchangeError::BadSymbol(if id >= 10_000 {
                format!("TxFlow id `{s}` is not a perpetual (spot ids are not supported)")
            } else {
                format!("unknown TxFlow market `{s}`")
            })
        });
    }
    match catalog.by_alias.get(&upper) {
        Some(Some(m)) => Ok(m.clone()),
        Some(None) => Err(ExchangeError::BadSymbol(format!(
            "ambiguous TxFlow symbol `{s}`; use the numeric instrument id"
        ))),
        None => Err(ExchangeError::BadSymbol(format!(
            "unknown TxFlow market `{s}`"
        ))),
    }
}

fn unified(m: &Market, with_native: bool) -> Result<UnifiedMarket, ExchangeError> {
    let id = m.id.to_string();
    Ok(UnifiedMarket {
        exchange: "txflow".into(),
        symbol: m.symbol(),
        base: m.base.clone(),
        quote: m.quote.clone(),
        market_type: UnifiedMarketType::Perp,
        // D5: halted-but-listed stays active; only delisting deactivates.
        active: !m.delisted,
        // TxFlow documents no perp minimum order size; the step is not one.
        min_order_size: None,
        tick_size: wire::float(m.tick),
        contract_size: Some(1.0),
        info: UnifiedMarketInfo {
            category: m.category.map(Into::into),
            raw_symbol: Some(m.name.clone()),
            exchange_symbol: Some(id.clone()),
            native: with_native.then(|| {
                json!({
                    "perpMeta": m.raw,
                    "haltTrading": m.halted,
                    "sizeStep": m.step.to_string(),
                    "priceTick": m.tick.to_string(),
                    "marketTags": m.tags,
                })
            }),
            ..Default::default()
        },
        identity: Some(MarketIdentity {
            market_id: make_market_id("txflow", UnifiedMarketType::Perp, None, None, &id)?,
            exchange_market_id: id,
            category: None,
            dex: None,
            contract_type: Some("perpetual".into()),
            settle: Some(m.quote.clone()),
            settlement_asset_id: Some(m.quote.clone()),
        }),
    })
}

pub(crate) fn book_policy(m: &Market) -> crate::orderbook::BookPolicy {
    // Sizes are base units; contract size 1.
    crate::orderbook::BookPolicy::base(&m.base, "txflow:perpMeta/basePrecision")
}

fn validate_params(v: &Value) -> Result<(), ExchangeError> {
    if v.is_null() || v.as_object().is_some_and(|o| o.is_empty()) {
        Ok(())
    } else {
        Err(ExchangeError::UnsupportedFeature(
            "TxFlow accepts no request params".into(),
        ))
    }
}

fn field(
    state: MarketStatsFieldState,
    value: Option<MarketStatsValue>,
    reason: Option<&str>,
    ctx: Option<&wire::Ctx>,
) -> MarketStatsField {
    MarketStatsField {
        state,
        value,
        reason: reason.map(Into::into),
        exchange_timestamp: ctx.and_then(|c| c.block_time),
        received_timestamp: ctx.map(|c| c.received_wall),
        source: Some(SOURCE.into()),
    }
}
fn unsupported(reason: &str) -> MarketStatsField {
    field(MarketStatsFieldState::Unsupported, None, Some(reason), None)
}
fn unavailable(reason: &str, ctx: Option<&wire::Ctx>) -> MarketStatsField {
    let mut f = field(MarketStatsFieldState::Unavailable, None, Some(reason), None);
    f.received_timestamp = ctx.map(|c| c.received_wall);
    f
}
fn price(m: &Market, v: Option<Decimal>, ctx: Option<&wire::Ctx>) -> MarketStatsField {
    match v {
        Some(v) => field(
            MarketStatsFieldState::Available,
            Some(MarketStatsValue::Price(PriceValue {
                amount: v.normalize().to_string(),
                base_asset: m.base.clone(),
                quote_asset: m.quote.clone(),
            })),
            None,
            ctx,
        ),
        None => unavailable("missing-or-invalid-value", ctx),
    }
}

fn stats_row(m: &Market, ctx: Option<&wire::Ctx>) -> Result<MarketStatsRow, ExchangeError> {
    use MarketStatsFieldName::*;
    let mut fields = BTreeMap::new();
    // Percent per settlement interval; the interval is per market, taken from
    // the published funding schedule rather than hard-coded.
    let interval = ctx
        .and_then(|c| c.next_funding.zip(c.prev_funding))
        .and_then(|(n, p)| n.checked_sub(p))
        .filter(|i| *i > 0);
    fields.insert(
        Funding,
        match ctx.and_then(|c| c.funding) {
            Some(rate) => field(
                MarketStatsFieldState::Available,
                Some(MarketStatsValue::Funding(FundingValue::new(
                    rate.normalize().to_string(),
                    FundingRateUnit::Percent,
                    FundingKind::CurrentUnclassified,
                    interval,
                    interval,
                    ctx.and_then(|c| c.prev_funding),
                    ctx.and_then(|c| c.next_funding),
                ))),
                None,
                ctx,
            ),
            None => unavailable("missing-funding", ctx),
        },
    );
    fields.insert(
        LastSettledFunding,
        unsupported("txflow-publishes-no-public-market-funding-history"),
    );
    fields.insert(MarkPrice, price(m, ctx.and_then(|c| c.mark), ctx));
    // Oracle = aggregated external price, the funding anchor.
    fields.insert(IndexPrice, price(m, ctx.and_then(|c| c.oracle), ctx));
    fields.insert(LastPrice, price(m, ctx.and_then(|c| c.last), ctx));
    fields.insert(
        Volume24h,
        unsupported("txflow-24h-volume-is-per-market-only-no-bulk-source"),
    );
    fields.insert(
        OpenInterest,
        match ctx.and_then(|c| c.open_interest).and_then(wire::float) {
            // Base units. Quote value is not published; not derived here.
            Some(oi) => field(
                MarketStatsFieldState::Available,
                Some(MarketStatsValue::OpenInterest(OpenInterestValue {
                    open_interest_amount: Some(oi),
                    open_interest_value: None,
                })),
                None,
                ctx,
            ),
            None => unavailable("missing-open-interest", ctx),
        },
    );
    Ok(MarketStatsRow {
        market: unified(m, false)?,
        fields,
    })
}

fn ticker(m: &Market, c: &wire::Ctx) -> CcxtTicker {
    CcxtTicker {
        info: json!({
            "instrumentId": m.id,
            "ctx": c.raw,
            "source": SOURCE,
            // impactPxs are impact prices, deliberately not bid/ask.
            "note": "bid/ask unavailable: allAssetCtxs carries impact prices, not top of book; 24h volume/change are per-market only upstream",
        }),
        symbol: m.symbol(),
        timestamp: c.block_time,
        datetime: c.block_time.and_then(wire::datetime),
        high: None,
        low: None,
        bid: None,
        bid_volume: None,
        ask: None,
        ask_volume: None,
        vwap: None,
        open: None,
        close: None,
        last: c.last.and_then(wire::float),
        previous_close: None,
        change: None,
        percentage: None,
        average: c.mid.and_then(wire::float),
        base_volume: None,
        quote_volume: None,
    }
}

#[async_trait]
impl MarketDataExchange for TxflowExchange {
    fn id(&self) -> &'static str {
        "txflow"
    }
    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        Some(self)
    }
    fn capabilities(&self) -> ExchangeDataCapabilities {
        let p = vec![UnifiedMarketType::Perp];
        let with = |mut c: DataCapability, msg: &str| {
            c.message = Some(msg.into());
            c
        };
        let rt = |mut c: RealtimeCapability, msg: &str| {
            c.message = Some(msg.into());
            c
        };
        ExchangeDataCapabilities {
            fetch_markets: with(
                DataCapability::supported(p.clone(), MarketScope::Catalog),
                "perpetuals only; spot and outcome markets are not supported. info.category labels asset class (stock, index, commodity, fx, pre-ipo, crypto)",
            ),
            fetch_tickers: with(
                DataCapability::supported(p.clone(), MarketScope::AllMarkets),
                "shared allAssetCtxs stream: last, mid (average) and raw ctx; no bid/ask, 24h volume, or 24h change",
            ),
            fetch_trades: with(
                DataCapability::supported(p.clone(), MarketScope::MarketSpecific),
                &format!("latest 300 trades; no history. {TRADE_ID_NOTE}"),
            ),
            fetch_ohlcv: with(
                DataCapability::supported(p.clone(), MarketScope::MarketSpecific),
                "11 intervals: 1m 3m 5m 15m 30m 1h 2h 4h 8h 12h 1d; base volume",
            ),
            fetch_order_book: with(
                DataCapability::supported(p.clone(), MarketScope::MarketSpecific),
                "REST books are capped at 20 levels per side (realtime up to 200)",
            ),
            fetch_market_stats: with(
                DataCapability::supported(p.clone(), MarketScope::AllMarkets),
                "mark, oracle index, last, open interest (base), current funding (percent); no settled funding history or 24h volume",
            ),
            realtime: RealtimeCapabilities {
                trades: rt(
                    RealtimeCapability::supported(
                        p.clone(),
                        MarketScope::MarketSpecific,
                        RealtimeMode::Websocket,
                        None,
                    ),
                    &format!("live trades only, no snapshot. {TRADE_ID_NOTE}"),
                ),
                orderbook: rt(
                    RealtimeCapability::supported(
                        p.clone(),
                        MarketScope::MarketSpecific,
                        RealtimeMode::Websocket,
                        None,
                    ),
                    "l2BookDepth snapshot + sequenced deltas, up to 200 levels per side",
                ),
                ohlcv: rt(
                    RealtimeCapability::supported(
                        p.clone(),
                        MarketScope::MarketSpecific,
                        RealtimeMode::Websocket,
                        None,
                    ),
                    "bars observed since subscription (current bar replaced in place); use fetchOHLCV for history",
                ),
                marketstats: rt(
                    RealtimeCapability::supported(
                        p,
                        MarketScope::AllMarkets,
                        RealtimeMode::Polling,
                        Some(STATS_POLL.as_millis() as u64),
                    ),
                    "polls a local cache fed by one shared allAssetCtxs websocket subscription",
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
            .catalog()
            .await?
            .markets
            .iter()
            .filter(|m| p.include_inactive || !m.delisted)
            .map(|m| unified(m, true))
            .collect::<Result<Vec<_>, _>>()?;
        v.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        Ok(v)
    }
    async fn fetch_tickers(&self, p: FetchTickersParams) -> Result<Vec<CcxtTicker>, ExchangeError> {
        validate_params(&p.params)?;
        let catalog = self.catalog().await?;
        let selected = match p.symbols {
            Some(symbols) => symbols
                .iter()
                .map(|s| resolve_in(&catalog, s))
                .collect::<Result<Vec<_>, _>>()?,
            None => catalog
                .markets
                .iter()
                .filter(|m| !m.delisted)
                .cloned()
                .collect(),
        };
        let (ctxs, _) = self.asset_ctxs().await?;
        Ok(selected
            .iter()
            .filter_map(|m| Some(ticker(m, ctxs.get(&m.id)?)))
            .collect())
    }
    async fn fetch_trades(&self, p: FetchTradesParams) -> Result<Vec<CcxtTrade>, ExchangeError> {
        validate_params(&p.params)?;
        let limit = p.limit.unwrap_or(REST_TRADES);
        if limit == 0 {
            return Err(ExchangeError::BadSymbol(
                "trade limit must be positive".into(),
            ));
        }
        let m = self.resolve(&p.symbol).await?;
        let rows: Vec<Value> = self
            .info(json!({"type":"trades","coin":m.id.to_string()}))
            .await?;
        let mut trades = rows
            .iter()
            .map(|r| wire::trade(&m, r))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExchangeError::UpstreamData)?;
        // Upstream is newest-first; Ferris snapshots are oldest-first.
        trades.sort_by_key(|t| t.timestamp);
        // Within the latest-300 window only; an older `since` is not backfilled.
        if let Some(since) = p.since {
            trades.retain(|t| t.timestamp.is_some_and(|ts| ts >= since));
            trades.truncate(limit);
        } else if trades.len() > limit {
            trades.drain(..trades.len() - limit);
        }
        Ok(trades)
    }
    async fn fetch_ohlcv(&self, p: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        validate_params(&p.params)?;
        let tf = p.timeframe.unwrap_or_else(|| "1m".into());
        let (tf, interval_ms) = realtime::interval(&tf)?;
        let limit = p.limit.unwrap_or(DEFAULT_LIMIT);
        if limit == 0 || limit > MAX_OHLCV_LIMIT {
            return Err(ExchangeError::BadSymbol(format!(
                "OHLCV limit must be 1–{MAX_OHLCV_LIMIT}"
            )));
        }
        let m = self.resolve(&p.symbol).await?;
        let span = (limit as u64).saturating_mul(interval_ms);
        // Never an unbounded window: [since, since + limit bars) or the last
        // `limit` bars including the current one.
        let (from, to) = match p.since {
            Some(since) => {
                let from = since / interval_ms * interval_ms;
                (from, from.saturating_add(span))
            }
            None => {
                let to = (now_millis() / interval_ms + 1) * interval_ms;
                (to.saturating_sub(span), to)
            }
        };
        let mut out: Vec<CcxtOhlcv> = Vec::new();
        let mut page_from = from;
        while page_from < to {
            let page_to = page_from.saturating_add(CANDLE_PAGE * interval_ms).min(to);
            let rows: Vec<Value> = self
                .info(json!({"type":"candleSnapshot","req":{
                    "coin": m.id.to_string(), "interval": tf,
                    "startTime": page_from, "endTime": page_to,
                }}))
                .await?;
            for row in rows {
                let (_, i, bar) = wire::candle(&row).map_err(ExchangeError::UpstreamData)?;
                if !i.is_empty() && i != tf {
                    return Err(ExchangeError::UpstreamData(
                        "TxFlow candle interval mismatch".into(),
                    ));
                }
                out.push(bar);
            }
            page_from = page_to;
        }
        out.sort_by_key(|r| r.0);
        out.dedup_by_key(|r| r.0);
        out.retain(|r| r.0 >= from && r.0 < to && p.since.is_none_or(|s| r.0 + interval_ms > s));
        if p.since.is_some() {
            out.truncate(limit);
        } else if out.len() > limit {
            out.drain(..out.len() - limit);
        }
        Ok(out)
    }
    async fn fetch_order_book(
        &self,
        p: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        let mode = crate::orderbook::BookAmountMode::parse(&p.params)?;
        validate_params(&crate::orderbook::BookAmountMode::without_selector(
            &p.params,
        ))?;
        // D6: reject, never clamp.
        let depth = p.limit.unwrap_or(REST_BOOK_LEVELS);
        if !(1..=REST_BOOK_LEVELS).contains(&depth) {
            return Err(ExchangeError::UnsupportedFeature(format!(
                "TxFlow REST order books are capped at {REST_BOOK_LEVELS} levels per side (limit 1–{REST_BOOK_LEVELS}); use the realtime orderbook for up to 200"
            )));
        }
        let m = self.resolve(&p.symbol).await?;
        let policy = book_policy(&m);
        policy.validate_mode(mode)?;
        let data: Value = self
            .info(json!({"type":"l2Book","coin":m.id.to_string()}))
            .await?;
        let (bids, asks) = wire::book_sides(&data, false).map_err(ExchangeError::UpstreamData)?;
        if bids
            .first()
            .zip(asks.first())
            .is_some_and(|((b, _), (a, _))| b >= a)
        {
            return Err(ExchangeError::UpstreamData("crossed TxFlow book".into()));
        }
        wire::to_book(
            &m,
            bids.into_iter().take(depth),
            asks.into_iter().take(depth),
            wire::id_of(data.get("time")),
            None,
        )
        .map_err(ExchangeError::UpstreamData)?
        .into_amount_mode(mode)
    }
}

#[async_trait]
impl MarketStatsSource for TxflowExchange {
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        validate_params(input)?;
        Ok(json!({}))
    }
    fn catalog_source(&self) -> &'static str {
        CATALOG_SOURCE
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
            let reason = match n {
                LastSettledFunding => Some("txflow-publishes-no-public-market-funding-history"),
                Volume24h => Some("txflow-24h-volume-is-per-market-only-no-bulk-source"),
                _ => None,
            };
            (
                n,
                FeatureCapability {
                    state: if reason.is_some() {
                        CapabilityState::Unsupported
                    } else {
                        CapabilityState::Supported
                    },
                    reason: reason.map(Into::into),
                },
            )
        })
        .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: "txflow".into(),
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
            upstream_mode: "sharedWebSocketCache".into(),
            poll_interval_ms: STATS_POLL.as_millis() as u64,
            stale_after_ms: 90_000,
            // Sparse native WS deltas are not wired into the coordinator for
            // native sources; the coordinator polls the local cache.
            ws: MarketStatsWsCapability {
                snapshot: false,
                delta: false,
                max_subscriptions_per_connection: 0,
            },
            funding_kinds: vec![FundingKind::CurrentUnclassified],
            rate_interval_ms: None,
            payment_interval_ms: None,
            limitations: vec![
                "one-shared-allAssetCtxs-subscription-for-all-markets".into(),
                "funding-is-percent-per-market-settlement-interval".into(),
                "open-interest-is-base-units".into(),
                "index-price-is-oracle-price".into(),
            ],
        })
    }
    async fn fetch_market_stats(
        &self,
        p: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        validate_params(&p.params)?;
        let catalog = self.catalog().await?;
        let (ctxs, received) = self.asset_ctxs().await?;
        let mut rows = Vec::new();
        let mut times = HashMap::new();
        for m in catalog.markets.iter().filter(|m| !m.delisted) {
            let ctx = ctxs.get(&m.id).map(Arc::as_ref);
            let row = stats_row(m, ctx)?;
            let id = row
                .market
                .identity
                .as_ref()
                .expect("TxFlow markets have identity")
                .market_id
                .clone();
            times.insert(
                id,
                row.fields
                    .iter()
                    .filter(|(_, f)| f.state == MarketStatsFieldState::Available)
                    .map(|(n, _)| (*n, ctx.map_or(received, |c| c.received)))
                    .collect(),
            );
            rows.push(row);
        }
        rows.sort_unstable_by(|a, b| a.market.symbol.cmp(&b.market.symbol));
        Ok(MarketStatsSourceSnapshot {
            rows,
            catalog_known: true,
            complete_catalogs: [UnifiedMarketType::Perp].into_iter().collect(),
            contexts_valid: true,
            received_at: Some(received),
            field_received_at: times,
            next_poll_at: Instant::now() + STATS_POLL,
            source_failures: vec![],
        })
    }
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl TxflowExchange {
    /// Network this adapter instance is bound to (`mainnet`, `testnet`, or a
    /// custom name for mock upstreams).
    pub fn network(&self) -> &str {
        &self.network
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn btc() -> Market {
        Market {
            id: 1,
            name: "BTC-USDC".into(),
            base: "BTC".into(),
            quote: "USDC".into(),
            tick: Decimal::from_str("0.1").unwrap(),
            step: Decimal::from_str("0.0001").unwrap(),
            delisted: false,
            halted: false,
            category: Some("crypto"),
            tags: vec![],
            raw: json!({}),
        }
    }

    fn meta() -> Value {
        json!({"universe":[
            {"name":"BTC-USDC","currencyType":"usdc","baseCurrency":"BTC","quoteCurrency":"USDC","basePrecision":"0.0001","szDecimals":4,"index":1,"priceTick":"0.1","haltTrading":false,"delisted":false,"useNotionalMargin":true},
            {"name":"PEPE-USDC","currencyType":"usdc","baseCurrency":"PEPE","quoteCurrency":"USDC","basePrecision":"100000","szDecimals":-5,"index":40,"priceTick":"0.0000001","haltTrading":false,"delisted":false},
            {"name":"1000BONK-USDC","currencyType":"usdc","baseCurrency":"1000BONK","quoteCurrency":"USDC","basePrecision":"100","szDecimals":-2,"index":35,"priceTick":"0.000001","haltTrading":false,"delisted":false},
            {"name":"TON-USDC","currencyType":"usdc","baseCurrency":"TON","quoteCurrency":"USDC","basePrecision":"0.1","szDecimals":1,"index":31,"priceTick":"0.001","haltTrading":true,"delisted":true},
            {"name":"TSLA-USDC","currencyType":"usdc","baseCurrency":"TSLA","quoteCurrency":"USDC","basePrecision":"0.01","szDecimals":2,"index":59,"priceTick":"0.01","haltTrading":true,"delisted":false},
            {"name":"EURUSD-USDC","currencyType":"usdc","baseCurrency":"EURUSD","quoteCurrency":"USDC","basePrecision":"1","szDecimals":0,"index":240,"priceTick":"0.0001","haltTrading":false,"delisted":false},
            {"name":"FOO-USDT","currencyType":"usdt","baseCurrency":"FOO","quoteCurrency":"USDT","basePrecision":"1","szDecimals":0,"index":77,"priceTick":"0.1"}
        ]})
    }
    fn tags() -> Value {
        json!({"version":"v1","l2":[
            {"id":1,"name":"Stocks"},{"id":4,"name":"Layer 1"},{"id":9,"name":"Meme"},{"id":67,"name":"FX"},{"id":100,"name":"Indices"},{"id":500,"name":"Brand New"}
        ],"instrumentTags":[
            {"instrumentId":1,"tagIds":[4]},
            {"instrumentId":40,"tagIds":[4,9]},
            {"instrumentId":59,"tagIds":[1,4]},
            {"instrumentId":240,"tagIds":[67,100]},
            {"instrumentId":35,"tagIds":[500]}
        ]})
    }

    #[test]
    fn catalog_parses_exact_ticks_steps_and_skips_unknown_quotes() {
        let c = build_catalog(&meta(), &Labels::new()).unwrap();
        assert!(
            !c.by_id.contains_key(&77),
            "unknown currencyType is rejected"
        );
        let pepe = &c.by_id[&40];
        assert_eq!(pepe.step, Decimal::from(100_000));
        assert_eq!(pepe.tick.to_string(), "0.0000001");
        assert_eq!(c.by_id[&35].step, Decimal::from(100));
        let u = unified(pepe, true).unwrap();
        assert_eq!(u.tick_size, Some(1e-7));
        assert_eq!(u.symbol, "PEPE/USDC:USDC");
        assert_eq!(u.min_order_size, None);
    }

    #[test]
    fn duplicate_index_is_rejected() {
        let mut m = meta();
        let dup = m["universe"][0].clone();
        m["universe"].as_array_mut().unwrap().push(dup);
        assert!(build_catalog(&m, &Labels::new()).is_err());
    }

    #[test]
    fn resolver_aliases_and_spot_rejection() {
        let c = build_catalog(&meta(), &Labels::new()).unwrap();
        for alias in ["BTC/USDC:USDC", "BTC", "btc", "BTC-USDC", "1", " 1 "] {
            assert_eq!(resolve_in(&c, alias).unwrap().id, 1, "{alias}");
        }
        assert_eq!(resolve_in(&c, "1000BONK").unwrap().id, 35);
        let spot = resolve_in(&c, "BTC/USDC").unwrap_err();
        assert!(
            matches!(spot, ExchangeError::BadSymbol(ref m) if m.contains("spot")),
            "{spot}"
        );
        let spot_id = resolve_in(&c, "10000").unwrap_err();
        assert!(matches!(spot_id, ExchangeError::BadSymbol(ref m) if m.contains("spot")));
        assert!(resolve_in(&c, "NOPE").is_err());
    }

    #[test]
    fn halted_listed_is_active_and_delisted_is_not() {
        let c = build_catalog(&meta(), &Labels::new()).unwrap();
        let tsla = unified(&c.by_id[&59], true).unwrap();
        assert!(tsla.active);
        assert_eq!(tsla.info.native.as_ref().unwrap()["haltTrading"], true);
        assert!(!unified(&c.by_id[&31], true).unwrap().active);
    }

    #[test]
    fn asset_class_labels_never_change_identity() {
        let labels = parse_labels(&tags());
        assert_eq!(labels[&1].0, Some("crypto"));
        assert_eq!(labels[&40].0, Some("crypto"));
        assert_eq!(labels[&59].0, Some("stock"), "TradFi wins over crypto");
        assert_eq!(labels[&240].0, None, "two TradFi classes are ambiguous");
        assert_eq!(labels[&35].0, None, "unknown tag names map to None");
        let with = build_catalog(&meta(), &labels).unwrap();
        let without = build_catalog(&meta(), &Labels::new()).unwrap();
        for (id, m) in &with.by_id {
            let a = unified(m, true).unwrap();
            let b = unified(&without.by_id[id], true).unwrap();
            assert_eq!(a.identity, b.identity);
            assert_eq!(a.symbol, b.symbol);
        }
        assert_eq!(
            unified(&with.by_id[&59], true)
                .unwrap()
                .info
                .category
                .as_deref(),
            Some("stock")
        );
        assert_eq!(
            unified(&without.by_id[&59], true).unwrap().info.category,
            None
        );
    }

    #[test]
    fn stats_rows_publish_percent_funding_and_unsupported_volume() {
        let c = wire::ctx(
            &json!({"funding":"0.00125000","openInterest":"239.2232","oraclePx":"64687.8","markPx":"64652.3","midPx":"64660.4","impactPxs":["64660.3","64660.5"],"lastPrice":"","nextFundingTime":1794880800000_u64,"prevFundingTime":1794877200000_u64,"blockTime":1794877962378_u64}),
            Instant::now(),
            1,
        )
        .unwrap();
        let row = stats_row(&btc(), Some(&c)).unwrap();
        let f = &row.fields[&MarketStatsFieldName::Funding];
        let Some(MarketStatsValue::Funding(v)) = &f.value else {
            panic!("funding")
        };
        assert_eq!(v.rate, "0.00125");
        assert_eq!(v.rate_unit, FundingRateUnit::Percent);
        assert_eq!(v.rate_interval_ms, Some(3_600_000));
        assert_eq!(v.next_payment_timestamp, Some(1794880800000));
        assert_eq!(v.payment_timestamp, Some(1794877200000));
        assert_eq!(
            Decimal::from_str(&v.equivalents.as_ref().unwrap().eight_hour_percent).unwrap(),
            Decimal::from_str("0.01").unwrap()
        );
        assert_eq!(
            row.fields[&MarketStatsFieldName::Volume24h].state,
            MarketStatsFieldState::Unsupported
        );
        assert_eq!(
            row.fields[&MarketStatsFieldName::LastSettledFunding].state,
            MarketStatsFieldState::Unsupported
        );
        assert_eq!(
            row.fields[&MarketStatsFieldName::LastPrice].state,
            MarketStatsFieldState::Unavailable,
            "empty lastPrice is unavailable, never zero"
        );
        let Some(MarketStatsValue::OpenInterest(oi)) =
            &row.fields[&MarketStatsFieldName::OpenInterest].value
        else {
            panic!("oi")
        };
        assert_eq!(oi.open_interest_amount, Some(239.2232));
        // A market with no ctx row is unavailable, not zero.
        let row = stats_row(&btc(), None).unwrap();
        assert_eq!(
            row.fields[&MarketStatsFieldName::MarkPrice].state,
            MarketStatsFieldState::Unavailable
        );
    }

    #[test]
    fn network_detection_and_mixing_is_rejected() {
        assert_eq!(network_of("https://rpc.txflow.com"), Some("mainnet"));
        assert_eq!(
            network_of("wss://rpc.txflow-testnet.xyz/ws"),
            Some("testnet")
        );
        assert_eq!(network_of("http://127.0.0.1:1"), None);
        assert!(TxflowExchange::with_urls(
            "https://rpc.txflow.com",
            "wss://rpc.txflow-testnet.xyz/ws",
            "wss://rpc.txflow.com/api-ws",
            "mainnet"
        )
        .is_err());
        assert!(TxflowExchange::with_urls(API, WS, API_WS, "mainnet").is_ok());
    }
}
