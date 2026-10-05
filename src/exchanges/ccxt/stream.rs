//! Stock Pro dispatch and request policy. CCXT owns parsing and book maintenance.

use std::sync::Arc;

use ccxt::{exchange::ExchangeRuntime, exchange_generated::ExchangeBase, Value};
use futures_util::FutureExt;
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::traits::ExchangeError,
    models::UnifiedMarketType,
    realtime::{RealtimeChannel, RealtimeTopic},
};

use super::{
    catalog::{catalog_scope, CatalogMarket},
    live::CatalogWatch,
    owner::CatalogSnapshot,
    venue::{CatalogScope, Provider, ProviderConfig, Venue},
};

/// Internal feed identity. Public client channels keep their own enum so the
/// statistics aggregate can share a URL without becoming a subscribable channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveChannel {
    Client(RealtimeChannel),
    Statistics,
}

impl LiveChannel {
    fn client(self) -> Option<RealtimeChannel> {
        match self {
            Self::Client(channel) => Some(channel),
            Self::Statistics => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct LiveSpec {
    pub key: String,
    pub venue: Venue,
    pub scope: CatalogScope,
    pub channel: LiveChannel,
    pub symbol: String,
    pub timeframe: Option<String>,
    pub depth: Option<usize>,
    pub params: JsonValue,
    pub market: JsonValue,
    pub url: String,
    pub slots: Option<usize>,
    pub hash: String,
    /// Statistics only: shared owned catalog cell so newly listed markets resolve
    /// without restarting the URL. Never a stock `Value`; no book copy.
    pub catalog: Option<Arc<CatalogWatch>>,
    config: ProviderConfig,
    candle_cache_key: Option<String>,
}

impl LiveSpec {
    fn candle_key(&self) -> &str {
        self.candle_cache_key
            .as_deref()
            .or(self.timeframe.as_deref())
            .expect("candle timeframe")
    }

    pub(super) fn unsubscribe_hash(&self) -> String {
        if self.venue == Venue::Bybit && self.channel == LiveChannel::Client(RealtimeChannel::Ohlcv)
        {
            return format!("unsubscribe::{}", self.hash);
        }
        let prefix = if matches!(self.venue, Venue::Binance | Venue::Aster) {
            match self.channel {
                LiveChannel::Client(RealtimeChannel::Trades) => "trade",
                LiveChannel::Client(RealtimeChannel::OrderBook) => "orderbook",
                LiveChannel::Client(RealtimeChannel::Ohlcv) => "ohlcv",
                LiveChannel::Statistics => return format!("unsubscribe:{}", self.hash),
            }
        } else {
            return format!("unsubscribe:{}", self.hash);
        };
        match self.timeframe.as_deref() {
            Some(timeframe) => format!("unsubscribe:{prefix}:{}:{timeframe}", self.symbol),
            None => format!("unsubscribe:{prefix}:{}", self.symbol),
        }
    }
}

pub(crate) struct PreparedTopic {
    pub spec: Arc<LiveSpec>,
    pub topic: RealtimeTopic,
    pub levels: usize,
    pub client_key: String,
}

pub(super) fn live_scope(
    venue: Venue,
    channel: RealtimeChannel,
    params: &JsonValue,
) -> Result<CatalogScope, ExchangeError> {
    use RealtimeChannel::*;
    if venue == Venue::Lighter && channel == Ohlcv {
        return Err(unsupported(venue, "stock CCXT Pro has no candle watcher"));
    }
    let map = params
        .as_object()
        .ok_or_else(|| ExchangeError::BadSymbol("`params` must be an object or null".into()))?;
    for key in map.keys().map(String::as_str) {
        let allowed = match key {
            "type" | "category" | "subType" | "settle" => true,
            "dex" => venue == Venue::Hyperliquid,
            "coin" => venue != Venue::Lighter,
            "marketId" | "market_id" => venue == Venue::Lighter,
            "levels" | "depth" | "limit" => channel == OrderBook,
            "nSigFigs" | "mantissa" => channel == OrderBook && venue == Venue::Hyperliquid,
            "rpi" => channel == OrderBook && venue == Venue::Binance,
            "name" => channel == Trades && venue == Venue::Binance,
            "timeframe" | "interval" => channel == Ohlcv,
            "price" => channel == Ohlcv && matches!(venue, Venue::Binance | Venue::Extended),
            "candleType" => channel == Ohlcv && venue == Venue::Extended,
            _ => false,
        };
        if !allowed {
            return Err(unsupported(
                venue,
                &format!("unsupported live parameter `{key}`"),
            ));
        }
    }
    let scope = catalog_scope(venue, params)?;
    Ok(if venue == Venue::Bybit && scope == CatalogScope::Default {
        CatalogScope::Linear
    } else {
        scope
    })
}

pub(super) async fn prepare_live(
    venue: Venue,
    channel: RealtimeChannel,
    mut topic: RealtimeTopic,
    catalog: Arc<CatalogSnapshot>,
    provider: &Provider,
    config: &ProviderConfig,
) -> Result<PreparedTopic, ExchangeError> {
    use RealtimeChannel::*;
    let market = catalog.catalog.resolve(&topic.symbol, &topic.params)?;
    let option = market.raw["option"].as_bool() == Some(true);
    let contract = market.raw["contract"].as_bool() == Some(true);
    let mut params = json!({});
    let mut timeframe = None;
    let mut depth = None;
    let mut levels = usize::MAX;
    match channel {
        OrderBook => {
            let (default, maximum, acquisition) = match venue {
                Venue::Binance if !contract => (20, 5_000, Some(5_000)),
                Venue::Binance => (20, 1_000, Some(1_000)),
                Venue::Bybit if option => (100, 100, Some(100)),
                Venue::Bybit => (1_000, 1_000, Some(1_000)),
                Venue::Hyperliquid => (20, 20, None),
                Venue::Aster => (20, 20, Some(20)),
                Venue::Lighter => (20, usize::MAX, None),
                Venue::Extended => (20, 1_000, Some(1_000)),
            };
            levels = integer(&topic.params, &["levels", "depth", "limit"])?
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ExchangeError::BadSymbol("book depth is too large".into()))?
                .unwrap_or(default);
            if levels == 0 {
                return Err(ExchangeError::BadSymbol(
                    "book depth must be positive".into(),
                ));
            }
            if levels > maximum {
                return Err(unsupported(
                    venue,
                    &format!("live book depth exceeds {maximum}"),
                ));
            }
            depth = acquisition;
            if venue == Venue::Binance {
                if let Some(rpi) = topic.params.get("rpi").filter(|v| !v.is_null()) {
                    let rpi = rpi
                        .as_bool()
                        .ok_or_else(|| ExchangeError::BadSymbol("rpi must be a boolean".into()))?;
                    if rpi && market.linear != Some(true) {
                        return Err(unsupported(venue, "RPI books require a linear contract"));
                    }
                    if rpi {
                        params["rpi"] = json!(true);
                    }
                }
            }
            if venue == Venue::Hyperliquid {
                let figures = integer(&topic.params, &["nSigFigs"])?;
                let mantissa = integer(&topic.params, &["mantissa"])?;
                if figures.is_some_and(|n| !(2..=5).contains(&n)) {
                    return Err(ExchangeError::BadSymbol(
                        "nSigFigs must be between 2 and 5".into(),
                    ));
                }
                if mantissa.is_some_and(|m| figures != Some(5) || !matches!(m, 1 | 2 | 5)) {
                    return Err(ExchangeError::BadSymbol(
                        "mantissa requires nSigFigs=5 and 1, 2, or 5".into(),
                    ));
                }
                // The stock watcher merges params at the request root. Its
                // subscription object is the supported way to set aggregation.
                if figures.is_some() {
                    params["subscription"] = json!({
                        "type": "l2Book",
                        "coin": if market.raw["swap"] == true { &market.raw["baseName"] } else { &market.raw["id"] },
                        "nSigFigs": figures,
                        "mantissa": mantissa,
                    });
                }
            }
        }
        Trades => {
            if let Some(name) = string(&topic.params, &["name"])? {
                if !matches!(name, "trade" | "aggTrade") {
                    return Err(unsupported(venue, "trade name must be trade or aggTrade"));
                }
                if name != "trade" {
                    params["name"] = json!(name);
                }
            }
        }
        Ohlcv => {
            if option && venue == Venue::Bybit {
                return Err(unsupported(venue, "Bybit option candles are not supported"));
            }
            let input = string(&topic.params, &["timeframe", "interval"])?.unwrap_or("1m");
            let normalized = if provider.has_timeframe(input) {
                input.to_string()
            } else {
                provider.timeframe_alias(input).ok_or_else(|| {
                    unsupported(venue, &format!("unsupported candle timeframe `{input}`"))
                })?
            };
            timeframe = Some(normalized);
            if let Some(price) = string(&topic.params, &["price"])? {
                if !contract || !matches!(price, "mark" | "index") {
                    return Err(unsupported(
                        venue,
                        "live price candles require a contract and mark or index",
                    ));
                }
                if venue == Venue::Binance {
                    params["channel"] = json!(if price == "mark" {
                        "markPriceKline"
                    } else {
                        "indexPriceKline"
                    });
                } else {
                    params["candleType"] = json!(if price == "mark" {
                        "mark-prices"
                    } else {
                        "index-prices"
                    });
                }
            }
            if let Some(kind) = string(&topic.params, &["candleType"])? {
                if !matches!(kind, "trades" | "mark-prices" | "index-prices") {
                    return Err(unsupported(venue, "unsupported candleType"));
                }
                params["candleType"] = json!(kind);
            }
            if params["candleType"] == "trades" {
                params.as_object_mut().unwrap().remove("candleType");
            }
        }
    }
    let symbol = market.ccxt_symbol.clone();
    let candle_cache_key = match (venue, timeframe.as_deref(), params["candleType"].as_str()) {
        (Venue::Extended, Some(timeframe), Some(kind)) => Some(format!("{timeframe}:{kind}")),
        _ => None,
    };
    let mut spec = LiveSpec {
        key: serde_json::to_string(&(
            venue.public_id(),
            &market.market.identity,
            channel.as_str(),
            &timeframe,
            &params,
        ))
        .map_err(|e| ExchangeError::Internal(e.to_string()))?,
        venue,
        scope: catalog.scope,
        channel: LiveChannel::Client(channel),
        symbol: symbol.clone(),
        timeframe,
        depth,
        params,
        market: market.raw.clone(),
        url: String::new(),
        hash: String::new(),
        slots: None,
        catalog: None,
        config: config.clone(),
        candle_cache_key,
    };
    let mut pro = LiveProvider::new(&spec, None);
    if let LiveProvider::Binance(core) = &pro {
        spec.slots = field(&core.options, &["streamLimits", binance_kind(&spec)])
            .as_i64()
            .and_then(|slots| usize::try_from(slots).ok());
        if !spec.slots.is_some_and(|slots| slots > 0) {
            return Err(ExchangeError::Internal(
                "invalid stock Binance stream allocator".into(),
            ));
        }
    }
    spec.url = pro.url(&spec).await?;
    spec.hash = message_hash(&spec);
    topic.symbol = symbol;
    let client_key = serde_json::to_string(&(&spec.key, (channel == OrderBook).then_some(levels)))
        .map_err(|e| ExchangeError::Internal(e.to_string()))?;
    Ok(PreparedTopic {
        spec: Arc::new(spec),
        topic,
        levels,
        client_key,
    })
}

/// One aggregate `market_stats/all` subscription for Lighter. The core is seeded
/// from the shared catalog cell (never `load_markets`/network here), and the feed
/// shares the venue's existing websocket URL with books/trades.
pub(super) async fn prepare_lighter_statistics(
    catalog: Arc<CatalogSnapshot>,
    config: &ProviderConfig,
    watch: Arc<CatalogWatch>,
) -> Result<PreparedTopic, ExchangeError> {
    if catalog.venue != Venue::Lighter {
        return Err(unsupported(
            catalog.venue,
            "statistics aggregate is Lighter-only",
        ));
    }
    let snapshot = watch.load();
    let representative = snapshot
        .catalog
        .entries()
        .iter()
        .find(|entry| entry.market.market_type == UnifiedMarketType::Perp)
        .ok_or_else(|| {
            ExchangeError::UpstreamData("Lighter catalog has no perpetual market".into())
        })?;
    let key = format!("statistics:{}", Venue::Lighter.public_id());
    let mut spec = LiveSpec {
        key: key.clone(),
        venue: Venue::Lighter,
        scope: catalog.scope,
        channel: LiveChannel::Statistics,
        symbol: "*".to_string(),
        timeframe: None,
        depth: None,
        params: json!({}),
        market: representative.raw.clone(),
        url: String::new(),
        hash: String::new(),
        slots: None,
        catalog: Some(Arc::clone(&watch)),
        config: config.clone(),
        candle_cache_key: None,
    };
    let mut pro = LiveProvider::new(&spec, None);
    spec.url = pro.url(&spec).await?;
    spec.hash = message_hash(&spec);
    Ok(PreparedTopic {
        spec: Arc::new(spec),
        topic: RealtimeTopic {
            exchange: Venue::Lighter.public_id().to_string(),
            symbol: "*".to_string(),
            params: json!({}),
        },
        levels: 0,
        client_key: key,
    })
}

fn string<'a>(input: &'a JsonValue, keys: &[&str]) -> Result<Option<&'a str>, ExchangeError> {
    for key in keys {
        match input.get(key) {
            None | Some(JsonValue::Null) => continue,
            Some(JsonValue::String(value)) if !value.trim().is_empty() => {
                return Ok(Some(value.trim()))
            }
            _ => {
                return Err(ExchangeError::BadSymbol(format!(
                    "{key} must be a nonempty string"
                )))
            }
        }
    }
    Ok(None)
}

fn integer(input: &JsonValue, keys: &[&str]) -> Result<Option<u64>, ExchangeError> {
    for key in keys {
        if let Some(value) = input.get(key).filter(|value| !value.is_null()) {
            return value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("{key} must be a nonnegative integer"))
                });
        }
    }
    Ok(None)
}

fn unsupported(venue: Venue, message: &str) -> ExchangeError {
    ExchangeError::UnsupportedFeature(format!("{}: {message}", venue.public_id()))
}

fn message_hash(spec: &LiveSpec) -> String {
    use RealtimeChannel::*;
    if spec.channel == LiveChannel::Statistics {
        // Stock `watch_tickers(null)` resolves this aggregate hash.
        return "tickers".to_string();
    }
    let channel = spec.channel.client().expect("client channel");
    match (spec.venue, channel) {
        (Venue::Hyperliquid, Ohlcv) => format!(
            "candles:{}:{}",
            spec.timeframe.as_deref().unwrap(),
            spec.symbol
        ),
        (Venue::Bybit | Venue::Binance, Ohlcv) => format!(
            "ohlcv::{}::{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
        (Venue::Aster, Ohlcv) => format!(
            "ohlcv:{}:{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
        (Venue::Extended, Ohlcv) => format!(
            "ohlcv:{}:{}:{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap(),
            spec.params["candleType"].as_str().unwrap_or("trades")
        ),
        (Venue::Binance | Venue::Lighter, OrderBook) => format!("orderbook::{}", spec.symbol),
        (_, OrderBook) => format!("orderbook:{}", spec.symbol),
        (Venue::Binance | Venue::Lighter | Venue::Aster, Trades) => {
            format!("trade::{}", spec.symbol)
        }
        (Venue::Extended, Trades) => format!("trades:{}", spec.symbol),
        (_, Trades) => format!("trade:{}", spec.symbol),
        (Venue::Lighter, Ohlcv) => unreachable!("rejected before metadata acquisition"),
    }
}

// Concrete cores are deliberately not typed wrappers: ExchangeRuntime's
// multi-hash driver must own ALL channels at an actual URL, not await them in turn.
pub(super) enum LiveProvider {
    Binance(ccxt_pro::pro::binance::BinanceCore),
    Bybit(ccxt_pro::pro::bybit::BybitCore),
    Hyperliquid(ccxt_pro::pro::hyperliquid::HyperliquidCore),
    Lighter(ccxt_pro::pro::lighter::LighterCore),
    Aster(ccxt_pro::pro::aster::AsterCore),
    Extended(ccxt_pro::pro::extended::ExtendedCore),
}

macro_rules! dispatch {
    ($provider:expr, $core:ident => $body:expr) => {
        match $provider {
            LiveProvider::Binance($core) => $body,
            LiveProvider::Bybit($core) => $body,
            LiveProvider::Hyperliquid($core) => $body,
            LiveProvider::Lighter($core) => $body,
            LiveProvider::Aster($core) => $body,
            LiveProvider::Extended($core) => $body,
        }
    };
}

impl LiveProvider {
    pub(super) fn new(spec: &LiveSpec, slot: Option<usize>) -> Self {
        let mut config = spec.config.for_scope(spec.venue, spec.scope);
        let markets = match &spec.catalog {
            // Statistics resolves rows for every loaded market: seed them all so
            // the stock parser names each row instead of building a synthetic one.
            Some(watch) => watch
                .load()
                .catalog
                .entries()
                .iter()
                .map(|entry| Value::from_json(&entry.raw))
                .collect(),
            None => vec![Value::from_json(&spec.market)],
        };
        ccxt::set_value(&mut config, &Value::from("markets"), Value::from(markets));
        if let Some(slot) = slot {
            let mut options = ccxt::value::get_value_k(&config, "options");
            ccxt::set_value(
                &mut options,
                &Value::from("streamIndex"),
                Value::Int(slot as i64 - 1),
            );
            ccxt::set_value(&mut config, &Value::from("options"), options);
        }
        let config = Some(config);
        match spec.venue {
            Venue::Binance => Self::Binance(ccxt_pro::pro::binance::BinanceCore::new(config)),
            Venue::Bybit => Self::Bybit(ccxt_pro::pro::bybit::BybitCore::new(config)),
            Venue::Hyperliquid => {
                Self::Hyperliquid(ccxt_pro::pro::hyperliquid::HyperliquidCore::new(config))
            }
            Venue::Lighter => Self::Lighter(ccxt_pro::pro::lighter::LighterCore::new(config)),
            Venue::Aster => Self::Aster(ccxt_pro::pro::aster::AsterCore::new(config)),
            Venue::Extended => {
                let mut core = ccxt_pro::pro::extended::ExtendedCore::new(config);
                core.parent.exchange.userAgent = Value::from("Ferris/1.0");
                Self::Extended(core)
            }
        }
    }

    async fn url(&mut self, spec: &LiveSpec) -> Result<String, ExchangeError> {
        let url = match self {
            Self::Binance(core) => {
                let kind = binance_kind(spec);
                let name = match spec.channel {
                    LiveChannel::Client(RealtimeChannel::OrderBook) => {
                        if spec.params["rpi"] == true {
                            "rpiDepth"
                        } else {
                            "depth"
                        }
                    }
                    LiveChannel::Client(RealtimeChannel::Trades) => {
                        spec.params["name"].as_str().unwrap_or("trade")
                    }
                    LiveChannel::Client(RealtimeChannel::Ohlcv) => {
                        spec.params["channel"].as_str().unwrap_or("kline")
                    }
                    LiveChannel::Statistics => unreachable!("statistics is Lighter-only"),
                };
                core.get_ws_url(
                    Value::from(kind),
                    core.get_future_ws_category(Value::from(name)),
                )
            }
            Self::Bybit(core) => {
                core.get_url_by_market_type(&[
                    Value::from(spec.symbol.as_str()),
                    Value::Bool(false),
                    Value::from("watchOrderBook"),
                    Value::from_json(&spec.params),
                ])
                .await
            }
            Self::Hyperliquid(core) => field(&core.urls, &["api", "ws", "public"]),
            Self::Lighter(core) => field(&core.urls, &["api", "ws"]),
            Self::Aster(core) => field(
                &core.urls,
                &[
                    "api",
                    "ws",
                    "public",
                    spec.market["type"].as_str().unwrap_or("swap"),
                ],
            ),
            Self::Extended(core) => {
                let base = field(&core.urls, &["api", "ws"]);
                let base = base.as_str().ok_or_else(|| {
                    ExchangeError::UpstreamData("missing stock websocket URL".into())
                })?;
                let id = spec.market["id"]
                    .as_str()
                    .ok_or_else(|| ExchangeError::UpstreamData("missing stock market id".into()))?;
                Value::from(match spec.channel {
                    LiveChannel::Client(RealtimeChannel::OrderBook) => {
                        format!("{base}/orderbooks/{id}")
                    }
                    LiveChannel::Client(RealtimeChannel::Trades) => {
                        format!("{base}/publicTrades/{id}")
                    }
                    LiveChannel::Client(RealtimeChannel::Ohlcv) => {
                        let kind = spec.params["candleType"].as_str().unwrap_or("trades");
                        let timeframe = spec.timeframe.as_deref().unwrap();
                        let interval = ccxt::value::get_value_k(&core.timeframes, timeframe);
                        format!(
                            "{base}/candles/{id}/{kind}?interval={}",
                            interval.as_str().unwrap_or(timeframe)
                        )
                    }
                    LiveChannel::Statistics => unreachable!("statistics is Lighter-only"),
                })
            }
        };
        url.as_str()
            .filter(|s| s.starts_with("ws://") || s.starts_with("wss://"))
            .map(str::to_string)
            .ok_or_else(|| ExchangeError::UpstreamData("invalid stock websocket URL".into()))
    }

    pub(super) fn add_market(&mut self, spec: &LiveSpec) {
        if let Some(watch) = &spec.catalog {
            self.add_catalog(watch.load().catalog.entries());
            return;
        }
        dispatch!(self, core => {
            if ccxt::value::get_value_k(&core.markets, &spec.symbol).is_null() {
                let mut markets = core.markets.clone();
                ccxt::set_value(&mut markets, &Value::from(spec.symbol.as_str()), Value::from_json(&spec.market));
                core.set_markets(markets, &[]);
            }
        });
    }

    /// Merge catalog rows the live core has not seen yet. The statistics feed
    /// calls this when the shared catalog cell advances, so a newly listed
    /// market is parseable without restarting the shared URL.
    pub(super) fn add_catalog(&mut self, entries: &[CatalogMarket]) {
        dispatch!(self, core => {
            let mut markets = core.markets.clone();
            let mut changed = false;
            for entry in entries {
                if ccxt::value::get_value_k(&markets, &entry.ccxt_symbol).is_null() {
                    ccxt::set_value(
                        &mut markets,
                        &Value::from(entry.ccxt_symbol.as_str()),
                        Value::from_json(&entry.raw),
                    );
                    changed = true;
                }
            }
            if changed {
                core.set_markets(markets, &[]);
            }
        });
    }

    pub(super) fn enqueue(&self, spec: &LiveSpec, unwatch: bool) {
        let symbol = Value::from(spec.symbol.as_str());
        let params = Value::from_json(&spec.params);
        if spec.channel == LiveChannel::Statistics {
            // One aggregate subscription: `watch_tickers(null, {})` on
            // `market_stats/all`; `un_watch_tickers` mirrors it.
            let method = if unwatch {
                "un_watch_tickers"
            } else {
                "watch_tickers"
            };
            ccxt::exchange_stubs::enqueue_spawn(method, vec![Value::Null, params]);
            return;
        }
        let limit = spec
            .depth
            .map(|depth| Value::Int(depth as i64))
            .unwrap_or_default();
        let timeframe = Value::from(spec.timeframe.as_deref().unwrap_or("1m"));
        let (method, args) = match (spec.channel.client().expect("client channel"), unwatch) {
            (RealtimeChannel::Trades, false) => (
                "watch_trades",
                vec![symbol, Value::Null, Value::Null, params],
            ),
            (RealtimeChannel::OrderBook, false) => {
                ("watch_order_book", vec![symbol, limit, params])
            }
            (RealtimeChannel::Ohlcv, false) => (
                "watch_ohlcv",
                vec![symbol, timeframe, Value::Null, Value::Null, params],
            ),
            (RealtimeChannel::Trades, true) => ("un_watch_trades", vec![symbol, params]),
            (RealtimeChannel::OrderBook, true) => {
                let mut params = params;
                if matches!(spec.venue, Venue::Bybit | Venue::Aster) {
                    ccxt::set_value(&mut params, &Value::from("limit"), limit);
                }
                ("un_watch_order_book", vec![symbol, params])
            }
            (RealtimeChannel::Ohlcv, true) => ("un_watch_ohlcv", vec![symbol, timeframe, params]),
        };
        // Native watches invoked inside the single ws_run loop register their
        // stock frames and return via its same-URL nested branch. They must not
        // be awaited sequentially on quiet per-channel futures.
        ccxt::exchange_stubs::enqueue_spawn(method, args);
    }

    pub(super) async fn next(
        &mut self,
        url: &str,
        hashes: &[String],
    ) -> Result<Value, ExchangeError> {
        std::panic::AssertUnwindSafe(async {
            dispatch!(self, core => core.ws_run(url.to_string(), hashes.to_vec(), Value::Null, Vec::new(), Value::Null).await)
        }).catch_unwind().await.map_err(panic_error)
    }

    pub(super) fn cache(&self, spec: &LiveSpec) -> Value {
        if spec.channel == LiveChannel::Statistics {
            // The aggregate's observable cache is the stock unified tickers map.
            return dispatch!(self, core => core.tickers.clone());
        }
        dispatch!(self, core => match spec.channel.client().expect("client channel") {
            RealtimeChannel::OrderBook => ccxt::value::get_value_k(&core.orderbooks, &spec.symbol),
            RealtimeChannel::Trades => ccxt::value::get_value_k(&core.trades, &spec.symbol),
            RealtimeChannel::Ohlcv => field(&core.ohlcvs, &[&spec.symbol, spec.candle_key()]),
        })
    }

    pub(super) fn incremental(
        &self,
        spec: &LiveSpec,
        mut cache: Value,
    ) -> Result<Value, ExchangeError> {
        if spec.channel == LiveChannel::Statistics {
            // Tickers are a live keyed map, not an append-only cache: the
            // statistics path compares per-symbol cache pointers itself.
            return Ok(cache);
        }
        let limit = cache.get_limit(Value::from(spec.symbol.as_str()), Value::Null);
        if limit
            .as_i64()
            .zip(ccxt::runtime::get_array_length(&cache).as_i64())
            .is_some_and(|(count, size)| count > size)
        {
            return Err(ExchangeError::UpstreamData(
                "stock incremental cache overflow".into(),
            ));
        }
        Ok(dispatch!(self, core => core.filter_by_since_limit(cache, &[
            Value::Null, limit,
            if spec.channel == LiveChannel::Client(RealtimeChannel::Trades) { Value::from("timestamp") } else { Value::Int(0) },
            Value::Bool(true),
        ])))
    }

    pub(super) fn clear_feed(&mut self, spec: &LiveSpec) {
        if spec.channel == LiveChannel::Statistics {
            // `Value::clear` is a no-op on a plain map; replace it outright.
            dispatch!(self, core => { core.tickers = Value::from_json(&json!({})); });
            return;
        }
        let mut cache = self.cache(spec);
        if spec.channel == LiveChannel::Client(RealtimeChannel::OrderBook) {
            cache.reset(Value::from_json(&json!({"bids": [], "asks": []})));
            ccxt::set_value(
                &mut cache,
                &Value::from("cache"),
                Value::from(Vec::<Value>::new()),
            );
        } else {
            cache.clear();
        }
        dispatch!(self, core => {
            let map = match spec.channel.client().expect("client channel") {
                RealtimeChannel::OrderBook => &mut core.orderbooks,
                RealtimeChannel::Trades => &mut core.trades,
                RealtimeChannel::Ohlcv => &mut core.ohlcvs,
            };
            if let Value::Dict(map) = map {
                if spec.channel == LiveChannel::Client(RealtimeChannel::Ohlcv) {
                    if let Some(Value::Dict(timeframes)) = Arc::make_mut(map).get_mut(&spec.symbol) {
                        Arc::make_mut(timeframes).shift_remove(spec.candle_key());
                    }
                } else {
                    Arc::make_mut(map).shift_remove(&spec.symbol);
                }
            }
        });
    }

    pub(super) fn clear_retained(&self, cache: Option<Value>) {
        if let Some(mut cache) = cache {
            if ccxt::value::get_value_k(&cache, "__book_id")
                .as_i64()
                .is_some()
            {
                cache.reset(Value::from_json(&json!({"bids": [], "asks": []})));
                ccxt::set_value(
                    &mut cache,
                    &Value::from("cache"),
                    Value::from(Vec::<Value>::new()),
                );
            } else {
                cache.clear();
            }
        }
    }

    pub(super) fn expire_unsubscribe(&self, url: &str, spec: &LiveSpec) {
        let unsubscribe = spec.unsubscribe_hash();
        dispatch!(self, core => core.clean_unsubscription(
            ccxt_pro::pro::ws_client::client_value(url), Value::from(spec.hash.as_str()),
            Value::from(unsubscribe.as_str()), &[],
        ));
        if let Some(client) = ccxt_pro::pro::ws_client::get_client(url) {
            // Only this retired feed's settlements: never drain the shared
            // source queue or discard an unrelated warm hash.
            let hashes = [spec.hash.clone(), unsubscribe];
            while client.take_settled(&hashes).is_some() {}
        }
    }
}

fn field(value: &Value, path: &[&str]) -> Value {
    path.iter().fold(value.clone(), |value, key| {
        ccxt::value::get_value_k(&value, key)
    })
}

fn binance_kind(spec: &LiveSpec) -> &'static str {
    if spec.market["option"] == true {
        if spec.channel == LiveChannel::Client(RealtimeChannel::Ohlcv) {
            "optionMarket"
        } else {
            "option"
        }
    } else if spec.market["contract"] == true {
        if spec.market["linear"] == true {
            "future"
        } else {
            "delivery"
        }
    } else {
        "spot"
    }
}

pub(super) fn panic_error(panic: Box<dyn std::any::Any + Send>) -> ExchangeError {
    let message = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown CCXT panic".into());
    ExchangeError::UpstreamRequest(message)
}
