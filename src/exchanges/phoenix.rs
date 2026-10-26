//! Native Phoenix Eternal public market-data adapter.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    sync::{broadcast, Mutex, Notify},
    task::JoinHandle,
    time::Instant,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource},
    market_stats::{make_market_id, MarketStatsSourceSnapshot},
    models::{
        CapabilityState, CcxtOhlcv, CcxtOrderBook, CcxtTicker, CcxtTrade, DataCapability,
        ExchangeDataCapabilities, FeatureCapability, FetchMarketStatsParams, FetchMarketsParams,
        FetchOhlcvParams, FetchOrderBookParams, FetchTickersParams, FetchTradesParams, FundingKind,
        MarketScope, MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSupportedCapabilities, MarketStatsValue,
        MarketStatsWsCapability, OpenInterestValue, RealtimeCapabilities, RealtimeCapability,
        RealtimeMode, UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType,
    },
    realtime::{
        DeliveryState, Publication, RealtimeChannel, RealtimeReceiver, RealtimeSubscription,
        RealtimeTopic, RealtimeUpdate,
    },
};

const API: &str = "https://perp-api.phoenix.trade";
const WS: &str = "wss://perp-api.phoenix.trade/v1/ws";
const SOURCE: &str = "phoenix:v1/market/stats";
const POLL_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_LIMIT: usize = 100;
const MAX_TRADE_LIMIT: usize = 10_000;
const MAX_CANDLE_LIMIT: usize = 2_500;

#[derive(Clone)]
pub struct PhoenixExchange {
    client: reqwest::Client,
    api: String,
    ws: String,
    cache: Arc<Mutex<Option<(Instant, Arc<Vec<PhoenixMarket>>)>>>,
    stats_cache: Arc<Mutex<Option<(Instant, Arc<Vec<PhoenixStatsRow>>)>>>,
    live: PhoenixLiveHub,
}
#[derive(Clone)]
struct PhoenixLiveHub {
    inner: Arc<Mutex<LiveState>>,
}

#[derive(Default)]
struct LiveState {
    feeds: HashMap<String, Arc<PhoenixFeed>>,
    stopped: bool,
}
struct PhoenixFeed {
    sender: broadcast::Sender<Publication>,
    delivery: Arc<DeliveryState>,
    released: Arc<Notify>,
    worker: Mutex<Option<JoinHandle<()>>>,
    channel: RealtimeChannel,
    symbol: String,
    timeframe: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PhoenixMarket {
    symbol: String,
    #[allow(dead_code)]
    asset_id: u64,
    market_status: String,
    tick_size: u64,
    base_lots_decimals: i32,
    #[allow(dead_code)]
    maker_fee: f64,
    #[allow(dead_code)]
    taker_fee: f64,
    #[serde(default)]
    stats_snapshot: Option<PhoenixMarketStatsSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PhoenixMarketStatsSnapshot {
    #[serde(default)]
    open_interest_base_lots: Option<String>,
}

struct PhoenixStatsRow {
    symbol: String,
    timestamp: Option<u64>,
    open_interest: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FillPage {
    data: Vec<PhoenixFill>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PhoenixFill {
    market_symbol: String,
    base_qty: String,
    quote_qty: String,
    price: String,
    timestamp: String,
    #[serde(default)]
    transaction_signature: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Candle {
    time: u64,
    low: f64,
    high: f64,
    open: f64,
    close: f64,
    #[serde(default)]
    volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PhoenixBook {
    slot: u64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

impl PhoenixExchange {
    pub fn new() -> Result<Self, ExchangeError> {
        let api = std::env::var("PHOENIX_API_URL").unwrap_or_else(|_| API.into());
        let ws = std::env::var("PHOENIX_WS_URL").unwrap_or_else(|_| WS.into());
        Self::with_urls(api, ws)
    }
    pub fn with_base_url(base: impl Into<String>) -> Result<Self, ExchangeError> {
        Self::with_urls(base, WS)
    }
    pub fn with_urls(api: impl Into<String>, ws: impl Into<String>) -> Result<Self, ExchangeError> {
        let api = api.into().trim_end_matches('/').to_string();
        let ws = ws.into();
        if api.is_empty() || ws.trim().is_empty() {
            return Err(ExchangeError::Internal(
                "Phoenix API and websocket URLs must not be empty".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .user_agent("ferris-market-data-backend/phoenix")
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| ExchangeError::Internal(e.to_string()))?;
        Ok(Self {
            client,
            api,
            ws,
            cache: Arc::new(Mutex::new(None)),
            stats_cache: Arc::new(Mutex::new(None)),
            live: PhoenixLiveHub {
                inner: Arc::new(Mutex::new(LiveState {
                    feeds: HashMap::new(),
                    stopped: false,
                })),
            },
        })
    }
    async fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ExchangeError> {
        let response = self
            .client
            .get(format!("{}{}", self.api, path))
            .query(query)
            .send()
            .await
            .map_err(|e| ExchangeError::UpstreamRequest(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ExchangeError::UpstreamRequest(format!(
                "Phoenix returned HTTP {status}"
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|e| ExchangeError::UpstreamRequest(e.to_string()))?;
        serde_json::from_str(&body).map_err(|e| {
            ExchangeError::UpstreamData(format!(
                "invalid Phoenix response: {e}; body prefix: {}",
                body.chars().take(256).collect::<String>()
            ))
        })
    }
    async fn markets(&self) -> Result<Arc<Vec<PhoenixMarket>>, ExchangeError> {
        let mut cache = self.cache.lock().await;
        if let Some((until, markets)) = cache.as_ref() {
            if Instant::now() < *until {
                return Ok(Arc::clone(markets));
            }
        }
        let markets = self
            .get_json::<Vec<PhoenixMarket>>("/v1/view/exchange/markets", &[])
            .await?;
        let mut symbols = HashSet::new();
        for market in &markets {
            if market.symbol.trim().is_empty() || !symbols.insert(market.symbol.clone()) {
                return Err(ExchangeError::UpstreamData(
                    "invalid or duplicate Phoenix market symbol".into(),
                ));
            }
        }
        let markets = Arc::new(markets);
        *cache = Some((Instant::now() + POLL_INTERVAL, Arc::clone(&markets)));
        Ok(markets)
    }
    async fn stats_snapshot(&self) -> Result<Arc<Vec<PhoenixStatsRow>>, ExchangeError> {
        {
            let cache = self.stats_cache.lock().await;
            if let Some((until, rows)) = cache.as_ref() {
                if Instant::now() < *until {
                    return Ok(Arc::clone(rows));
                }
            }
        }
        let markets = self.markets().await?;
        let rows = markets
            .iter()
            .filter(|market| active(&market.market_status))
            .map(|market| {
                let open_interest = market
                    .stats_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.open_interest_base_lots.as_deref())
                    .and_then(|value| value.parse::<f64>().ok())
                    .filter(|value| value.is_finite() && *value >= 0.0);
                PhoenixStatsRow {
                    symbol: market.symbol.clone(),
                    timestamp: None,
                    open_interest,
                }
            })
            .collect::<Vec<_>>();
        let rows = Arc::new(rows);
        *self.stats_cache.lock().await = Some((Instant::now() + POLL_INTERVAL, Arc::clone(&rows)));
        Ok(rows)
    }

    async fn resolve(&self, input: &str) -> Result<PhoenixMarket, ExchangeError> {
        let raw = input.trim();
        let base = raw
            .strip_suffix("/USD:USD")
            .or_else(|| raw.strip_suffix("/USD"))
            .unwrap_or(raw);
        let base = base.strip_suffix("-PERP").unwrap_or(base);
        let key = base.to_ascii_lowercase();
        self.markets()
            .await?
            .iter()
            .find(|m| m.symbol.to_ascii_lowercase() == key)
            .cloned()
            .ok_or_else(|| ExchangeError::BadSymbol(format!("unknown Phoenix market `{input}`")))
    }
    pub(crate) async fn subscribe_live(
        &self,
        channel: RealtimeChannel,
        topic: RealtimeTopic,
        levels: usize,
        client_key: String,
    ) -> Result<RealtimeSubscription, ExchangeError> {
        let symbol = self.resolve(&topic.symbol).await?.symbol;
        let timeframe = (channel == RealtimeChannel::Ohlcv).then(|| {
            topic
                .params
                .get("timeframe")
                .or_else(|| topic.params.get("interval"))
                .and_then(Value::as_str)
                .unwrap_or("1m")
                .to_string()
        });
        let key = client_key.clone();
        let mut live = self.live.inner.lock().await;
        if live.stopped {
            return Err(ExchangeError::Internal(
                "Phoenix realtime adapter is stopped".into(),
            ));
        }
        if let Some(feed) = live.feeds.get(&key) {
            if feed
                .delivery
                .viewers
                .load(std::sync::atomic::Ordering::Acquire)
                != 0
            {
                let receiver = RealtimeReceiver::join(
                    feed.sender.subscribe(),
                    Arc::clone(&feed.delivery),
                    Arc::clone(&feed.released),
                )
                .ok_or_else(|| ExchangeError::Internal("Phoenix live feed stopped".into()))?;
                return Ok(RealtimeSubscription {
                    key,
                    topic,
                    levels_limit: levels,
                    receiver,
                });
            }
        }
        let (sender, receiver) = broadcast::channel(512);
        let delivery = Arc::new(DeliveryState::default());
        let released = Arc::new(Notify::new());
        let receiver =
            RealtimeReceiver::new(receiver, Arc::clone(&delivery), Arc::clone(&released));
        let feed = Arc::new(PhoenixFeed {
            sender: sender.clone(),
            delivery: Arc::clone(&delivery),
            released: Arc::clone(&released),
            worker: Mutex::new(None),
            channel,
            symbol,
            timeframe,
        });
        let worker_feed = Arc::clone(&feed);
        let ws = self.ws.clone();
        let worker = tokio::spawn(async move {
            run_live_feed(ws, worker_feed).await;
        });
        *feed.worker.lock().await = Some(worker);
        live.feeds.insert(key.clone(), feed);
        Ok(RealtimeSubscription {
            key,
            topic,
            levels_limit: levels,
            receiver,
        })
    }
    pub(crate) async fn shutdown_live(&self) -> Result<(), ExchangeError> {
        let feeds = {
            let mut live = self.live.inner.lock().await;
            live.stopped = true;
            live.feeds.drain().map(|(_, feed)| feed).collect::<Vec<_>>()
        };
        for feed in feeds {
            if let Some(worker) = feed.worker.lock().await.take() {
                worker.abort();
                let _ = worker.await;
            }
        }
        Ok(())
    }
}
async fn run_live_feed(ws: String, feed: Arc<PhoenixFeed>) {
    loop {
        if feed
            .delivery
            .viewers
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return;
        }
        let connection = tokio::select! {
            _ = feed.released.notified() => return,
            result = connect_async(&ws) => result,
        };
        let Ok((mut socket, _)) = connection else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let subscription = match feed.channel {
            RealtimeChannel::Trades => json!({"channel":"trades", "symbol":feed.symbol}),
            RealtimeChannel::OrderBook => json!({"channel":"orderbook", "symbol":feed.symbol}),
            RealtimeChannel::Ohlcv => {
                json!({"channel":"candles", "symbol":feed.symbol, "timeframe":feed.timeframe.as_deref().unwrap_or("1m")})
            }
        };
        if socket
            .send(Message::Text(
                json!({"type":"subscribe", "subscription":subscription}).to_string(),
            ))
            .await
            .is_err()
        {
            continue;
        }
        loop {
            let message = tokio::select! {
                _ = feed.released.notified() => return,
                message = socket.next() => message,
            };
            let Some(Ok(message)) = message else {
                break;
            };
            let Message::Text(text) = message else {
                continue;
            };
            match parse_live_message(feed.channel, &text) {
                Ok(Some(update)) => {
                    let _ = feed.sender.send(Publication::new(update));
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = feed
                        .sender
                        .send(Publication::new(RealtimeUpdate::Error(error.into())));
                }
            }
        }
        if feed
            .delivery
            .viewers
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
        {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn parse_live_message(
    channel: RealtimeChannel,
    text: &str,
) -> Result<Option<RealtimeUpdate>, String> {
    let value: Value = serde_json::from_str(text)
        .map_err(|error| format!("invalid Phoenix websocket JSON: {error}"))?;
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "subscriptionConfirmed" | "subscriptionStatus"))
    {
        return Ok(None);
    }
    if value.get("type").and_then(Value::as_str) == Some("subscriptionError") {
        return Err(value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Phoenix websocket subscription error")
            .into());
    }
    if value.get("channel").and_then(Value::as_str) == Some("error") {
        return Err(value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Phoenix websocket error")
            .into());
    }
    match channel {
        RealtimeChannel::Trades => {
            let events = value
                .get("trades")
                .or_else(|| value.get("data"))
                .and_then(Value::as_array)
                .ok_or_else(|| "Phoenix trades message has no trades array".to_string())?;
            let mut trades = Vec::with_capacity(events.len());
            for event in events {
                trades.push(ws_trade(event)?);
            }
            Ok(Some(RealtimeUpdate::Trades(Arc::new(trades))))
        }
        RealtimeChannel::Ohlcv => {
            let candle = value
                .get("candle")
                .or_else(|| value.get("data"))
                .unwrap_or(&value);
            Ok(Some(RealtimeUpdate::Ohlcv(Arc::new(vec![ws_candle(
                candle,
            )?]))))
        }
        RealtimeChannel::OrderBook => {
            let book = value
                .get("orderbook")
                .or_else(|| value.get("data"))
                .ok_or_else(|| "Phoenix orderbook message has no orderbook".to_string())?;
            let bids = levels(book.get("bids"))?;
            let asks = levels(book.get("asks"))?;
            Ok(Some(RealtimeUpdate::OrderBook(Arc::new(CcxtOrderBook {
                asks,
                bids,
                datetime: None,
                timestamp: None,
                nonce: book.get("slot").and_then(Value::as_u64),
                symbol: value
                    .get("symbol")
                    .or_else(|| book.get("symbol"))
                    .and_then(Value::as_str)
                    .map(|s| format!("{s}/USD")),
            }))))
        }
    }
}

fn levels(value: Option<&Value>) -> Result<Vec<(f64, f64)>, String> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| "Phoenix orderbook levels are missing".to_string())?
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| "invalid Phoenix orderbook level".to_string())?;
            let price = values
                .first()
                .and_then(Value::as_f64)
                .ok_or_else(|| "invalid Phoenix orderbook price".to_string())?;
            let amount = values
                .get(1)
                .and_then(Value::as_f64)
                .ok_or_else(|| "invalid Phoenix orderbook amount".to_string())?;
            if !(price.is_finite() && price > 0.0 && amount.is_finite() && amount >= 0.0) {
                return Err("invalid Phoenix orderbook number".into());
            }
            Ok((price, amount))
        })
        .collect()
}

fn validate_levels(
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), ExchangeError> {
    let valid = |levels: Vec<(f64, f64)>| {
        levels
            .into_iter()
            .map(|(price, amount)| {
                if !(price.is_finite() && price > 0.0 && amount.is_finite() && amount >= 0.0) {
                    return Err(ExchangeError::UpstreamData(
                        "invalid Phoenix orderbook level".into(),
                    ));
                }
                Ok((price, amount))
            })
            .collect::<Result<Vec<_>, _>>()
    };
    Ok((valid(bids)?, valid(asks)?))
}

fn ws_trade(value: &Value) -> Result<CcxtTrade, String> {
    let amount = value
        .get("baseAmount")
        .and_then(Value::as_f64)
        .ok_or_else(|| "Phoenix trade has no base amount".to_string())?;
    let quote = value
        .get("quoteAmount")
        .and_then(Value::as_f64)
        .ok_or_else(|| "Phoenix trade has no quote amount".to_string())?;
    if !amount.is_finite() || !quote.is_finite() || amount == 0.0 || quote < 0.0 {
        return Err("invalid Phoenix trade quantity".into());
    }
    let timestamp = value
        .get("timestamp")
        .and_then(|v| {
            v.as_str()
                .and_then(|raw| raw.parse::<u64>().ok())
                .or_else(|| v.as_u64())
        })
        .and_then(|seconds| seconds.checked_mul(1000));
    let price = Some((quote / amount.abs()).abs()).filter(|v| v.is_finite() && *v > 0.0);
    Ok(CcxtTrade {
        info: value.clone(),
        amount: Some(amount.abs()),
        datetime: timestamp
            .and_then(|t| chrono::DateTime::from_timestamp_millis(t as i64))
            .map(|t| t.to_rfc3339()),
        id: json_string(value.get("tradeSequenceNumber")),
        order: None,
        price,
        timestamp,
        trade_type: Some("market".into()),
        side: Some(
            match value.get("side").and_then(Value::as_str) {
                Some("ask") | Some("sell") => "sell",
                _ => "buy",
            }
            .into(),
        ),
        symbol: value
            .get("symbol")
            .and_then(Value::as_str)
            .map(|s| format!("{s}/USD")),
        taker_or_maker: None,
        cost: Some(quote.abs()),
        fee: None,
    })
}
fn json_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_str()
            .map(str::to_owned)
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn ws_candle(value: &Value) -> Result<CcxtOhlcv, String> {
    let time = value
        .get("time")
        .and_then(Value::as_u64)
        .ok_or_else(|| "Phoenix candle has no time".to_string())?
        .checked_mul(1000)
        .ok_or_else(|| "Phoenix candle time overflow".to_string())?;
    let number = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite())
            .ok_or_else(|| format!("Phoenix candle has invalid {name}"))
    };
    let open = number("open")?;
    let high = number("high")?;
    let low = number("low")?;
    let close = number("close")?;
    if low > high || open < low || open > high || close < low || close > high {
        return Err("invalid Phoenix candle range".into());
    }
    let volume = value
        .get("volume")
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite() && *v >= 0.0);
    Ok((time, open, high, low, close, volume))
}

#[async_trait]
impl MarketDataExchange for PhoenixExchange {
    fn id(&self) -> &'static str {
        "phoenix"
    }
    fn capabilities(&self) -> ExchangeDataCapabilities {
        capabilities()
    }
    async fn fetch_tickers(
        &self,
        params: FetchTickersParams,
    ) -> Result<Vec<CcxtTicker>, ExchangeError> {
        validate_params(&params.params)?;
        let requested = params.symbols.unwrap_or_default();
        let markets = self.markets().await?;
        let selected = if requested.is_empty() {
            markets
                .iter()
                .filter(|m| active(&m.market_status))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            let mut selected = Vec::with_capacity(requested.len());
            for symbol in requested {
                selected.push(self.resolve(&symbol).await?);
            }
            selected
        };
        let mut result = Vec::with_capacity(selected.len());
        for market in selected {
            let snapshot = self
                .get_json::<Value>(&format!("/v1/view/market/{}", market.symbol), &[])
                .await?;
            let mark = snapshot
                .get("market")
                .and_then(|value| value.get("markPrice"))
                .and_then(|value| value.get("price"))
                .and_then(Value::as_f64)
                .filter(|v| v.is_finite() && *v > 0.0);
            let index = snapshot
                .get("market")
                .and_then(|value| value.get("spotPrice"))
                .and_then(|value| value.get("price"))
                .and_then(Value::as_f64)
                .filter(|v| v.is_finite() && *v > 0.0);
            let timestamp = now_millis();
            result.push(CcxtTicker {
                info: json!({
                    "marketSymbol": market.symbol,
                    "markPrice": mark,
                    "spotPrice": index,
                    "openInterest": serde_json::Value::Null
                }),
                symbol: format!("{}/USD", market.symbol),
                timestamp: Some(timestamp),
                datetime: chrono::DateTime::from_timestamp_millis(timestamp as i64)
                    .map(|t| t.to_rfc3339()),
                high: None,
                low: None,
                bid: None,
                bid_volume: None,
                ask: None,
                ask_volume: None,
                vwap: None,
                open: None,
                close: None,
                last: None,
                previous_close: None,
                change: None,
                percentage: None,
                average: None,
                base_volume: None,
                quote_volume: None,
            });
        }
        Ok(result)
    }
    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        Some(self)
    }
    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        validate_params(&params.params)?;
        let mut result = self
            .markets()
            .await?
            .iter()
            .filter(|m| params.include_inactive || active(&m.market_status))
            .map(market_to_unified)
            .collect::<Result<Vec<_>, _>>()?;
        result.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));
        Ok(result)
    }
    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        validate_params(&params.params)?;
        let market = self.resolve(&params.symbol).await?;
        let limit = params.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_TRADE_LIMIT);
        if limit == 0 {
            return Err(ExchangeError::BadSymbol(
                "trade limit must be positive".into(),
            ));
        }
        let mut query = vec![("limit", limit.to_string())];
        if let Some(since) = params.since {
            query.push(("startTime", since.to_string()));
        }
        if let Some(cursor) = params.params.get("cursor").and_then(Value::as_str) {
            query.push(("cursor", cursor.to_owned()));
        }
        let page = self
            .get_json::<FillPage>(&format!("/v1/trades/{}/fills", market.symbol), &query)
            .await?;
        let mut rows = page
            .data
            .into_iter()
            .map(fill_to_trade)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(since) = params.since {
            rows.retain(|r| r.timestamp.is_some_and(|t| t >= since));
        }
        rows.sort_unstable_by_key(|r| std::cmp::Reverse(r.timestamp));
        rows.truncate(limit);
        Ok(rows)
    }
    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        validate_params(&params.params)?;
        let market = self.resolve(&params.symbol).await?;
        let timeframe = params
            .timeframe
            .or_else(|| {
                params
                    .params
                    .get("timeframe")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "1m".into());
        validate_timeframe(&timeframe)?;
        let limit = params.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_CANDLE_LIMIT);
        if limit == 0 {
            return Err(ExchangeError::BadSymbol(
                "OHLCV limit must be positive".into(),
            ));
        }
        let mut query = vec![("timeframe", timeframe), ("limit", limit.to_string())];
        if let Some(since) = params.since {
            query.push(("startTime", since.to_string()));
        }
        let mut rows = self
            .get_json::<Vec<Candle>>(&format!("/v1/candles/{}", market.symbol), &query)
            .await?
            .into_iter()
            .map(candle_to_ohlcv)
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_unstable_by_key(|row| row.0);
        if let Some(since) = params.since {
            rows.retain(|row| row.0 >= since);
        }
        rows.truncate(limit);
        Ok(rows)
    }
    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        validate_params(&params.params)?;
        let market = self.resolve(&params.symbol).await?;
        let book = self
            .get_json::<PhoenixBook>(&format!("/v1/view/orderbook/{}", market.symbol), &[])
            .await?;
        let (bids, asks) = validate_levels(book.bids, book.asks)?;
        let limit = params.limit.unwrap_or(usize::MAX);
        Ok(CcxtOrderBook {
            asks: asks.into_iter().take(limit).collect(),
            bids: bids.into_iter().take(limit).collect(),
            datetime: None,
            timestamp: None,
            nonce: Some(book.slot),
            symbol: Some(format!("{}/USD", market.symbol)),
        })
    }
}

#[async_trait]
impl MarketStatsSource for PhoenixExchange {
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        validate_params(input)?;
        Ok(json!({}))
    }

    fn catalog_source(&self) -> &'static str {
        SOURCE
    }

    fn capabilities(&self) -> MarketStatsCapabilities {
        stats_capabilities()
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        validate_params(&params.params)?;
        let received_at = Instant::now();
        let wall = now_millis();
        let rows = self.stats_snapshot().await?;
        let markets = self.markets().await?;
        let mut result = Vec::with_capacity(rows.len());
        let mut field_received_at = HashMap::new();
        for row in rows.iter() {
            let market = markets
                .iter()
                .find(|market| market.symbol == row.symbol)
                .ok_or_else(|| {
                    ExchangeError::UpstreamData(format!(
                        "Phoenix market {} disappeared",
                        row.symbol
                    ))
                })?;
            let unified = market_to_unified(market)?;
            let market_id = unified
                .identity
                .as_ref()
                .expect("Phoenix markets always have native identity")
                .market_id
                .clone();
            let fields: BTreeMap<_, _> = [
                (
                    MarketStatsFieldName::Funding,
                    unsupported("phoenix-funding-not-published"),
                ),
                (
                    MarketStatsFieldName::LastSettledFunding,
                    unsupported("phoenix-settled-funding-not-published"),
                ),
                (
                    MarketStatsFieldName::MarkPrice,
                    unsupported("phoenix-mark-price-unavailable-in-bulk-stats"),
                ),
                (
                    MarketStatsFieldName::IndexPrice,
                    unsupported("phoenix-index-price-unavailable-in-bulk-stats"),
                ),
                (
                    MarketStatsFieldName::LastPrice,
                    unsupported("phoenix-last-price-not-published"),
                ),
                (
                    MarketStatsFieldName::Volume24h,
                    unsupported("phoenix-24h-volume-not-published"),
                ),
                (
                    MarketStatsFieldName::OpenInterest,
                    oi_field(row.open_interest, row.timestamp, wall),
                ),
            ]
            .into_iter()
            .collect();
            let mut receipts = BTreeMap::new();
            for (field, value) in &fields {
                if value.state != MarketStatsFieldState::Unsupported {
                    receipts.insert(*field, received_at);
                }
            }
            field_received_at.insert(market_id, receipts);
            result.push(MarketStatsRow {
                market: unified,
                fields,
            });
        }
        result.sort_unstable_by(|a, b| a.market.symbol.cmp(&b.market.symbol));
        Ok(MarketStatsSourceSnapshot {
            rows: result,
            catalog_known: true,
            complete_catalogs: [UnifiedMarketType::Perp].into_iter().collect(),
            contexts_valid: true,
            received_at: Some(received_at),
            field_received_at,
            next_poll_at: received_at + POLL_INTERVAL,
            source_failures: Vec::new(),
        })
    }
}

fn capabilities() -> ExchangeDataCapabilities {
    let products = vec![UnifiedMarketType::Perp];
    ExchangeDataCapabilities {
        fetch_markets: DataCapability::supported(products.clone(), MarketScope::Catalog),
        fetch_tickers: DataCapability::supported(products.clone(), MarketScope::AllMarkets),
        fetch_trades: DataCapability::supported(products.clone(), MarketScope::MarketSpecific),
        fetch_ohlcv: DataCapability::supported(products.clone(), MarketScope::MarketSpecific),
        fetch_order_book: DataCapability::supported(products.clone(), MarketScope::MarketSpecific),
        fetch_market_stats: DataCapability::supported(products.clone(), MarketScope::AllMarkets),
        realtime: RealtimeCapabilities {
            trades: RealtimeCapability::supported(
                products.clone(),
                MarketScope::MarketSpecific,
                RealtimeMode::Websocket,
                None,
            ),
            orderbook: RealtimeCapability::supported(
                products.clone(),
                MarketScope::MarketSpecific,
                RealtimeMode::Websocket,
                None,
            ),
            ohlcv: RealtimeCapability::supported(
                products.clone(),
                MarketScope::MarketSpecific,
                RealtimeMode::Websocket,
                None,
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
fn stats_capabilities() -> MarketStatsCapabilities {
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
        (
            name,
            FeatureCapability {
                state: match name {
                    OpenInterest => CapabilityState::Supported,
                    _ => CapabilityState::Unsupported,
                },
                reason: match name {
                    OpenInterest => None,
                    MarkPrice => Some("phoenix-mark-price-not-in-bulk-stats".into()),
                    IndexPrice => Some("phoenix-index-price-not-in-bulk-stats".into()),
                    Funding => Some("phoenix-stats-does-not-publish-funding-rate".into()),
                    LastSettledFunding => {
                        Some("phoenix-stats-does-not-publish-settled-funding".into())
                    }
                    LastPrice => Some("last-price-requires-trade-observation".into()),
                    Volume24h => Some("phoenix-stats-does-not-publish-24h-volume".into()),
                },
            },
        )
    })
    .collect();
    MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
        scope: MarketStatsScope {
            exchange: "phoenix".into(),
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
            "stats-use-exchange-market-snapshot-open-interest-only".into(),
            "last-price-volume-funding-mark-index-unavailable".into(),
        ],
    })
}
fn validate_params(value: &Value) -> Result<(), ExchangeError> {
    let Some(map) = value.as_object() else {
        if value.is_null() {
            return Ok(());
        }
        return Err(ExchangeError::BadSymbol(
            "Phoenix params must be an object or null".into(),
        ));
    };
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "timeframe" | "interval" | "cursor" | "levels" | "depth" | "limit"
        ) {
            return Err(ExchangeError::UnsupportedFeature(format!(
                "Phoenix parameter `{key}` is not supported"
            )));
        }
    }
    Ok(())
}
fn validate_timeframe(value: &str) -> Result<(), ExchangeError> {
    if matches!(
        value,
        "1s" | "5s" | "1m" | "5m" | "15m" | "30m" | "1h" | "4h" | "1d"
    ) {
        Ok(())
    } else {
        Err(ExchangeError::UnsupportedFeature(format!(
            "unsupported Phoenix timeframe `{value}`"
        )))
    }
}
fn active(status: &str) -> bool {
    matches!(status, "active" | "postOnly")
}
fn market_to_unified(m: &PhoenixMarket) -> Result<UnifiedMarket, ExchangeError> {
    let symbol = format!("{}/USD", m.symbol);
    let market_id = make_market_id("phoenix", UnifiedMarketType::Perp, None, None, &m.symbol)?;
    if m.tick_size == 0 {
        return Err(ExchangeError::UpstreamData(format!(
            "Phoenix market {} has zero tick size",
            m.symbol
        )));
    }
    let base_factor = 10_f64.powi(m.base_lots_decimals);
    let tick_size = m.tick_size as f64 * base_factor / 1_000_000.0;
    let min_order_size = 1.0 / base_factor;
    if !(tick_size.is_finite()
        && tick_size > 0.0
        && min_order_size.is_finite()
        && min_order_size > 0.0)
    {
        return Err(ExchangeError::UpstreamData(format!(
            "Phoenix market {} has invalid precision",
            m.symbol
        )));
    }
    Ok(UnifiedMarket {
        exchange: "phoenix".into(),
        symbol,
        base: m.symbol.clone(),
        quote: "USD".into(),
        market_type: UnifiedMarketType::Perp,
        active: active(&m.market_status),
        min_order_size: Some(min_order_size),
        tick_size: Some(tick_size),
        contract_size: Some(1.0),
        info: UnifiedMarketInfo {
            category: Some(m.market_status.clone()),
            raw_symbol: Some(m.symbol.clone()),
            exchange_symbol: Some(m.symbol.clone()),
            ..Default::default()
        },
        identity: Some(crate::models::MarketIdentity {
            market_id,
            exchange_market_id: m.symbol.clone(),
            category: None,
            dex: None,
            contract_type: Some("perpetual".into()),
            settle: Some("USD".into()),
            settlement_asset_id: Some("USD".into()),
        }),
    })
}
fn fill_to_trade(fill: PhoenixFill) -> Result<CcxtTrade, ExchangeError> {
    let base = fill
        .base_qty
        .parse::<f64>()
        .map_err(|_| ExchangeError::UpstreamData("invalid Phoenix fill base quantity".into()))?;
    let quote = fill
        .quote_qty
        .parse::<f64>()
        .map_err(|_| ExchangeError::UpstreamData("invalid Phoenix fill quote quantity".into()))?;
    let price = fill
        .price
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v > 0.0);
    let timestamp = chrono::DateTime::parse_from_rfc3339(&fill.timestamp)
        .ok()
        .and_then(|d| u64::try_from(d.timestamp_millis()).ok());
    if !base.is_finite() || !quote.is_finite() || base == 0.0 || quote == 0.0 {
        return Err(ExchangeError::UpstreamData(
            "invalid Phoenix fill quantity".into(),
        ));
    }
    Ok(CcxtTrade {
        info: json!(fill),
        amount: Some(base.abs()),
        datetime: timestamp.map(|t| {
            chrono::DateTime::from_timestamp_millis(t as i64)
                .unwrap()
                .to_rfc3339()
        }),
        id: fill.transaction_signature,
        order: None,
        price,
        timestamp,
        trade_type: Some("market".into()),
        side: Some(if base >= 0.0 { "buy" } else { "sell" }.into()),
        symbol: Some(format!("{}/USD", fill.market_symbol)),
        taker_or_maker: None,
        cost: Some(quote.abs()),
        fee: None,
    })
}
fn candle_to_ohlcv(candle: Candle) -> Result<CcxtOhlcv, ExchangeError> {
    for value in [candle.low, candle.high, candle.open, candle.close] {
        if !value.is_finite() {
            return Err(ExchangeError::UpstreamData(
                "non-finite Phoenix candle".into(),
            ));
        }
    }
    if candle.low > candle.high
        || candle.open < candle.low
        || candle.open > candle.high
        || candle.close < candle.low
        || candle.close > candle.high
    {
        return Err(ExchangeError::UpstreamData(
            "invalid Phoenix candle range".into(),
        ));
    }
    if candle.volume.is_some_and(|v| !v.is_finite() || v < 0.0) {
        return Err(ExchangeError::UpstreamData(
            "invalid Phoenix candle volume".into(),
        ));
    }
    Ok((
        candle.time,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
    ))
}
fn oi_field(value: Option<f64>, exchange_timestamp: Option<u64>, wall: u64) -> MarketStatsField {
    let value = value.filter(|v| v.is_finite() && *v >= 0.0);
    MarketStatsField {
        state: if value.is_some() {
            MarketStatsFieldState::Available
        } else {
            MarketStatsFieldState::Unavailable
        },
        value: value.map(|v| {
            MarketStatsValue::OpenInterest(OpenInterestValue {
                open_interest_amount: Some(v),
                open_interest_value: None,
            })
        }),
        reason: value
            .is_none()
            .then_some("missing-or-invalid-open-interest".into()),
        exchange_timestamp: value.map(|_| exchange_timestamp).flatten(),
        received_timestamp: value.map(|_| wall),
        source: Some(SOURCE.into()),
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
    fn signed_fill_maps_to_ccxt_trade() {
        let trade = fill_to_trade(PhoenixFill {
            market_symbol: "SOL".into(),
            base_qty: "-2.5".into(),
            quote_qty: "250.0".into(),
            price: "100.0".into(),
            timestamp: "2026-01-01T00:00:00Z".into(),
            transaction_signature: Some("sig".into()),
        })
        .unwrap();
        assert_eq!(trade.amount, Some(2.5));
        assert_eq!(trade.price, Some(100.0));
        assert_eq!(trade.cost, Some(250.0));
        assert_eq!(trade.side.as_deref(), Some("sell"));
        assert_eq!(trade.symbol.as_deref(), Some("SOL/USD"));
        assert_eq!(trade.id.as_deref(), Some("sig"));
    }

    #[test]
    fn websocket_trade_and_control_messages_parse() {
        assert!(parse_live_message(
            RealtimeChannel::Trades,
            r#"{"type":"subscriptionConfirmed"}"#,
        )
        .unwrap()
        .is_none());

        let message = json!({
            "channel": "trades",
            "symbol": "SOL",
            "trades": [{
                "timestamp": "1767225600",
                "symbol": "SOL",
                "baseAmount": 2.0,
                "quoteAmount": 200.0,
                "tradeSequenceNumber": "7",
                "side": "bid"
            }]
        });
        let Some(RealtimeUpdate::Trades(rows)) =
            parse_live_message(RealtimeChannel::Trades, &message.to_string()).unwrap()
        else {
            panic!("expected Phoenix trades update");
        };
        assert_eq!(rows[0].price, Some(100.0));
        assert_eq!(rows[0].side.as_deref(), Some("buy"));
        assert_eq!(rows[0].timestamp, Some(1_767_225_600_000));
    }

    #[test]
    fn invalid_candle_range_is_rejected() {
        let result = candle_to_ohlcv(Candle {
            time: 1,
            low: 101.0,
            high: 100.0,
            open: 100.0,
            close: 100.0,
            volume: None,
        });
        assert!(matches!(result, Err(ExchangeError::UpstreamData(_))));
    }

    #[test]
    fn websocket_candle_accepts_second_timeframe() {
        let message = json!({
            "channel": "candle",
            "symbol": "SOL",
            "candle": {
                "time": 1,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1.0
            }
        });
        assert!(matches!(
            parse_live_message(RealtimeChannel::Ohlcv, &message.to_string()),
            Ok(Some(RealtimeUpdate::Ohlcv(_)))
        ));
    }
}

#[cfg(test)]
mod stats_tests {
    use super::*;

    #[test]
    fn snapshot_stats_exclude_unavailable_bulk_prices() {
        let capabilities = stats_capabilities();
        let MarketStatsCapabilities::Supported(capabilities) = capabilities else {
            panic!("Phoenix stats must be supported");
        };
        assert_eq!(capabilities.upstream_mode, "sharedPolling");
        assert!(capabilities.fields[&UnifiedMarketType::Perp]
            .contains_key(&MarketStatsFieldName::OpenInterest));
        assert!(capabilities
            .limitations
            .iter()
            .any(|value| value.contains("mark-index")));
    }
}
