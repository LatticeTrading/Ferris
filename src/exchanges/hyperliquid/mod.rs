use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams,
    },
};

const HYPERLIQUID_RECENT_TRADES_UPSTREAM_MAX: usize = 10;
const DEFAULT_FETCH_TRADES_LIMIT: usize = 100;
const DEFAULT_FETCH_OHLCV_LIMIT: usize = 200;
const MAX_FETCH_OHLCV_LIMIT: usize = 5_000;
const MAX_FETCH_ORDER_BOOK_LEVELS: usize = 20;

const INITIAL_RECONNECT_DELAY_MS: u64 = 500;
const MAX_RECONNECT_DELAY_MS: u64 = 15_000;

type HyperliquidWsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct HyperliquidExchange {
    http_client: reqwest::Client,
    base_url: String,
    catalog: RwLock<Option<MarketCatalog>>,
    trade_cache: TradeCache,
    collector: HyperliquidCollectorHandle,
}

impl HyperliquidExchange {
    pub fn new(
        base_url: String,
        timeout_ms: u64,
        trade_cache_capacity_per_coin: usize,
        trade_cache_retention_ms: u64,
        trade_collector_enabled: bool,
    ) -> Result<Self, ExchangeError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build reqwest client: {err}"))
            })?;

        let trade_cache = TradeCache::new(trade_cache_capacity_per_coin, trade_cache_retention_ms);
        let collector = start_trade_collector(
            base_url.clone(),
            trade_cache.clone(),
            trade_collector_enabled,
        );

        Ok(Self {
            http_client,
            base_url,
            catalog: RwLock::new(None),
            trade_cache,
            collector,
        })
    }

    async fn post_info(&self, payload: Value) -> Result<Value, ExchangeError> {
        let url = format!("{}/info", self.base_url);
        let response = self
            .http_client
            .post(url)
            .json(&payload)
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
                "status={status} body={} payload={}",
                truncate(&body, 240),
                payload
            )));
        }

        serde_json::from_str::<Value>(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!("{err}; body={}", truncate(&body, 240)))
        })
    }

    async fn get_catalog(&self) -> Result<MarketCatalog, ExchangeError> {
        if let Some(cached) = self.catalog.read().await.clone() {
            return Ok(cached);
        }

        let mut write_guard = self.catalog.write().await;
        if let Some(cached) = write_guard.clone() {
            return Ok(cached);
        }

        let fresh = self.build_catalog().await?;
        *write_guard = Some(fresh.clone());
        Ok(fresh)
    }

    async fn build_catalog(&self) -> Result<MarketCatalog, ExchangeError> {
        let mut catalog = MarketCatalog::default();

        let perps = self
            .post_info(json!({ "type": "metaAndAssetCtxs" }))
            .await?;
        if let Some(universe) = perps
            .get(0)
            .and_then(|item| item.get("universe"))
            .and_then(Value::as_array)
        {
            for item in universe {
                if let Some(coin) = item.get("name").and_then(Value::as_str) {
                    catalog.insert(format!("{coin}/USDC:USDC"), coin);
                    catalog.insert(format!("{coin}/USDC"), coin);
                    catalog.insert(coin, coin);
                }
            }
        }

        let spot_meta = self.post_info(json!({ "type": "spotMeta" })).await?;
        let token_map = parse_token_map(&spot_meta)?;
        let token_aliases = build_token_aliases(&spot_meta);

        if let Some(universe) = spot_meta.get("universe").and_then(Value::as_array) {
            for pair in universe {
                let Some(index) = pair.get("index").and_then(Value::as_u64) else {
                    continue;
                };

                let pair_name = pair.get("name").and_then(Value::as_str).unwrap_or_default();

                let Some(token_indexes) = pair.get("tokens").and_then(Value::as_array) else {
                    continue;
                };
                if token_indexes.len() != 2 {
                    continue;
                }

                let Some(base_idx) = token_indexes[0].as_u64() else {
                    continue;
                };
                let Some(quote_idx) = token_indexes[1].as_u64() else {
                    continue;
                };

                let Some(base) = token_map.get(&base_idx) else {
                    continue;
                };
                let Some(quote) = token_map.get(&quote_idx) else {
                    continue;
                };

                let coin = if pair_name == "PURR/USDC" {
                    "PURR/USDC".to_string()
                } else {
                    format!("@{index}")
                };

                let canonical_symbol = format!("{base}/{quote}");
                catalog.insert(&coin, &coin);
                catalog.insert(&canonical_symbol, &coin);

                if !pair_name.is_empty() {
                    catalog.insert(pair_name, &coin);
                }

                let alias_base = token_aliases.get(base).unwrap_or(base);
                let alias_quote = token_aliases.get(quote).unwrap_or(quote);
                let alias_symbol = format!("{alias_base}/{alias_quote}");
                catalog.insert(alias_symbol, &coin);
            }
        }

        Ok(catalog)
    }

    fn resolve_coin_and_symbol(
        &self,
        symbol: &str,
        extra_params: &Value,
        catalog: &MarketCatalog,
    ) -> Result<(String, String, Option<String>), ExchangeError> {
        if let Some(coin) = extra_params
            .get("coin")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let resolved_symbol = catalog
                .symbol_for_coin(coin)
                .unwrap_or_else(|| symbol.to_string());
            let dex = extra_params
                .get("dex")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string);

            return Ok((coin.to_string(), resolved_symbol, dex));
        }

        let normalized_symbol = symbol.trim();
        if normalized_symbol.is_empty() {
            return Err(ExchangeError::BadSymbol(
                "symbol cannot be empty".to_string(),
            ));
        }

        let coin = catalog
            .coin_for_symbol(normalized_symbol)
            .or_else(|| heuristic_coin_from_symbol(normalized_symbol))
            .unwrap_or_else(|| normalized_symbol.to_string());
        let resolved_symbol = normalized_symbol.to_string();
        let dex = extra_params
            .get("dex")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);

        Ok((coin, resolved_symbol, dex))
    }
}

#[async_trait]
impl MarketDataExchange for HyperliquidExchange {
    fn id(&self) -> &'static str {
        "hyperliquid"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let catalog = match self.get_catalog().await {
            Ok(catalog) => catalog,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "unable to load Hyperliquid market catalog, falling back to raw symbol"
                );
                MarketCatalog::default()
            }
        };

        let (coin, resolved_symbol, dex) =
            self.resolve_coin_and_symbol(&params.symbol, &params.params, &catalog)?;

        self.collector.subscribe(coin.clone());

        let mut payload = json!({
            "type": "recentTrades",
            "coin": coin,
        });

        if let Some(dex) = dex {
            payload["dex"] = Value::String(dex);
        }

        let response = self.post_info(payload).await?;
        let upstream_trades = response.as_array().ok_or_else(|| {
            ExchangeError::UpstreamData("expected an array for recentTrades".to_string())
        })?;

        let upstream_limit = params
            .limit
            .unwrap_or(HYPERLIQUID_RECENT_TRADES_UPSTREAM_MAX)
            .min(HYPERLIQUID_RECENT_TRADES_UPSTREAM_MAX);

        self.trade_cache.insert_batch(&coin, upstream_trades).await;

        let raw_trades = self.trade_cache.get_trades(&coin).await;
        let mut mapped = map_trades_lossy(raw_trades, &resolved_symbol);

        mapped.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));

        if let Some(since) = params.since {
            mapped.retain(|trade| trade.timestamp.unwrap_or_default() >= since);
        }

        let limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_TRADES_LIMIT)
            .min(self.trade_cache.capacity_per_coin())
            .max(upstream_limit);

        if mapped.len() > limit {
            mapped.truncate(limit);
        }

        Ok(mapped)
    }

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        let catalog = self.get_catalog().await.unwrap_or_default();
        let (coin, _resolved_symbol, _dex) =
            self.resolve_coin_and_symbol(&params.symbol, &params.params, &catalog)?;

        let timeframe = params
            .timeframe
            .unwrap_or_else(|| "1m".to_string())
            .trim()
            .to_string();
        let interval_ms = interval_to_millis(&timeframe).ok_or_else(|| {
            ExchangeError::BadSymbol(format!("unsupported timeframe `{timeframe}`"))
        })?;

        let limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_OHLCV_LIMIT)
            .min(MAX_FETCH_OHLCV_LIMIT)
            .max(1);

        let now = now_millis();
        let until = extract_u64_field(&params.params, "until");

        let start_time = params
            .since
            .unwrap_or_else(|| now.saturating_sub(interval_ms.saturating_mul(limit as u64)));

        let mut end_time = until.unwrap_or(now);
        if params.since.is_some() && until.is_none() {
            end_time = start_time.saturating_add(interval_ms.saturating_mul(limit as u64));
        }
        if end_time <= start_time {
            end_time = start_time.saturating_add(interval_ms);
        }

        let payload = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": timeframe,
                "startTime": start_time,
                "endTime": end_time,
            }
        });

        let response = self.post_info(payload).await?;
        let candles = response
            .as_array()
            .ok_or_else(|| ExchangeError::UpstreamData("expected candle array".to_string()))?;

        let mut mapped = Vec::with_capacity(candles.len());
        for candle in candles {
            match map_candle(candle.clone()) {
                Ok(candle) => mapped.push(candle),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map candle, skipping");
                }
            }
        }

        mapped.sort_by(|left, right| left.0.cmp(&right.0));

        if let Some(since) = params.since {
            mapped.retain(|candle| candle.0 >= since);
        }

        if mapped.len() > limit {
            if params.since.is_some() {
                mapped.truncate(limit);
            } else {
                mapped = mapped.split_off(mapped.len() - limit);
            }
        }

        Ok(mapped)
    }

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        let catalog = self.get_catalog().await.unwrap_or_default();
        let (coin, resolved_symbol, _dex) =
            self.resolve_coin_and_symbol(&params.symbol, &params.params, &catalog)?;

        let mut payload = json!({
            "type": "l2Book",
            "coin": coin,
        });

        if let Some(n_sig_figs) = extract_i64_field(&params.params, "nSigFigs") {
            payload["nSigFigs"] = Value::Number(n_sig_figs.into());
        }
        if let Some(mantissa) = extract_i64_field(&params.params, "mantissa") {
            payload["mantissa"] = Value::Number(mantissa.into());
        }

        let response = self.post_info(payload).await?;

        let timestamp = extract_u64_from_json_value(response.get("time")).unwrap_or_default();
        let datetime = iso8601_millis(timestamp);

        let levels = response
            .get("levels")
            .and_then(Value::as_array)
            .ok_or_else(|| ExchangeError::UpstreamData("missing levels".to_string()))?;

        if levels.len() < 2 {
            return Err(ExchangeError::UpstreamData(
                "order book levels should contain bids and asks".to_string(),
            ));
        }

        let bids_raw = levels
            .first()
            .and_then(Value::as_array)
            .ok_or_else(|| ExchangeError::UpstreamData("missing bids array".to_string()))?;
        let asks_raw = levels
            .get(1)
            .and_then(Value::as_array)
            .ok_or_else(|| ExchangeError::UpstreamData("missing asks array".to_string()))?;

        let mut bids = map_order_book_side_lossy(bids_raw);
        let mut asks = map_order_book_side_lossy(asks_raw);

        bids.sort_by(|left, right| right.0.total_cmp(&left.0));
        asks.sort_by(|left, right| left.0.total_cmp(&right.0));

        let limit = params.limit.unwrap_or(MAX_FETCH_ORDER_BOOK_LEVELS).max(1);
        let final_limit = limit.min(MAX_FETCH_ORDER_BOOK_LEVELS);

        if bids.len() > final_limit {
            bids.truncate(final_limit);
        }
        if asks.len() > final_limit {
            asks.truncate(final_limit);
        }

        Ok(CcxtOrderBook {
            asks,
            bids,
            datetime,
            timestamp: Some(timestamp),
            nonce: None,
            symbol: Some(resolved_symbol),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct MarketCatalog {
    symbol_to_coin: HashMap<String, String>,
    coin_to_symbol: HashMap<String, String>,
}

impl MarketCatalog {
    fn insert(&mut self, symbol: impl AsRef<str>, coin: impl AsRef<str>) {
        let symbol = symbol.as_ref().trim();
        let coin = coin.as_ref().trim();

        if symbol.is_empty() || coin.is_empty() {
            return;
        }

        self.symbol_to_coin
            .insert(normalize_lookup_key(symbol), coin.to_string());

        self.coin_to_symbol
            .entry(normalize_lookup_key(coin))
            .or_insert_with(|| symbol.to_string());
    }

    fn coin_for_symbol(&self, symbol: &str) -> Option<String> {
        self.symbol_to_coin
            .get(&normalize_lookup_key(symbol))
            .cloned()
    }

    fn symbol_for_coin(&self, coin: &str) -> Option<String> {
        self.coin_to_symbol
            .get(&normalize_lookup_key(coin))
            .cloned()
    }
}

#[derive(Clone)]
struct TradeCache {
    inner: Arc<RwLock<HashMap<String, CoinTradeBuffer>>>,
    capacity_per_coin: usize,
    retention_ms: u64,
}

impl TradeCache {
    fn new(capacity_per_coin: usize, retention_ms: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            capacity_per_coin: capacity_per_coin.max(10),
            retention_ms,
        }
    }

    fn capacity_per_coin(&self) -> usize {
        self.capacity_per_coin
    }

    async fn insert_batch(&self, coin: &str, trades: &[Value]) {
        if trades.is_empty() {
            return;
        }

        let coin_key = normalize_lookup_key(coin);
        let mut write_guard = self.inner.write().await;
        let buffer = write_guard.entry(coin_key).or_default();
        for trade in trades {
            buffer.trades.push_back(trade.clone());
        }

        compact_trade_buffer(
            &mut buffer.trades,
            self.capacity_per_coin,
            self.retention_ms,
            now_millis(),
        );
    }

    async fn get_trades(&self, coin: &str) -> Vec<Value> {
        let coin_key = normalize_lookup_key(coin);
        let read_guard = self.inner.read().await;
        read_guard
            .get(&coin_key)
            .map(|buffer| buffer.trades.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct CoinTradeBuffer {
    trades: VecDeque<Value>,
}

fn compact_trade_buffer(
    raw_trades: &mut VecDeque<Value>,
    capacity_per_coin: usize,
    retention_ms: u64,
    now_ms: u64,
) {
    if raw_trades.is_empty() {
        return;
    }

    let cutoff = now_ms.saturating_sub(retention_ms);
    let mut sorted: Vec<(u64, Option<String>, Value)> = raw_trades
        .drain(..)
        .map(|trade| {
            (
                extract_u64_from_json_value(trade.get("time")).unwrap_or_default(),
                trade_identity(&trade),
                trade,
            )
        })
        .collect();

    sorted.sort_by(|left, right| right.0.cmp(&left.0));

    let mut deduped_ids = HashSet::new();
    let mut compacted = VecDeque::with_capacity(capacity_per_coin);

    for (timestamp, identity, trade) in sorted {
        if retention_ms > 0 && timestamp > 0 && timestamp < cutoff {
            continue;
        }

        if let Some(identity) = identity {
            if !deduped_ids.insert(identity) {
                continue;
            }
        }

        compacted.push_back(trade);
        if compacted.len() >= capacity_per_coin {
            break;
        }
    }

    *raw_trades = compacted;
}

fn trade_identity(trade: &Value) -> Option<String> {
    if let Some(tid) = trade.get("tid") {
        if let Some(value) = tid.as_u64() {
            return Some(format!("tid:{value}"));
        }
        if let Some(value) = tid.as_str() {
            return Some(format!("tid:{value}"));
        }
    }

    if let Some(hash) = trade.get("hash").and_then(Value::as_str) {
        if let Some(time) = extract_u64_from_json_value(trade.get("time")) {
            return Some(format!("hash:{hash}:time:{time}"));
        }
    }

    None
}

#[derive(Clone)]
enum HyperliquidCollectorHandle {
    Disabled,
    Enabled(UnboundedSender<CollectorCommand>),
}

impl HyperliquidCollectorHandle {
    fn subscribe(&self, coin: String) {
        match self {
            Self::Disabled => {}
            Self::Enabled(sender) => {
                let _ = sender.send(CollectorCommand::Subscribe { coin });
            }
        }
    }
}

enum CollectorCommand {
    Subscribe { coin: String },
}

fn start_trade_collector(
    base_url: String,
    trade_cache: TradeCache,
    enabled: bool,
) -> HyperliquidCollectorHandle {
    if !enabled {
        tracing::info!("Hyperliquid trade collector disabled");
        return HyperliquidCollectorHandle::Disabled;
    }

    let (sender, receiver) = unbounded_channel();
    let ws_url = to_hyperliquid_ws_url(&base_url);

    tokio::spawn(run_trade_collector_loop(ws_url, trade_cache, receiver));

    HyperliquidCollectorHandle::Enabled(sender)
}

async fn run_trade_collector_loop(
    ws_url: String,
    trade_cache: TradeCache,
    mut receiver: UnboundedReceiver<CollectorCommand>,
) {
    let mut subscriptions = HashSet::<String>::new();
    let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

    loop {
        while let Ok(command) = receiver.try_recv() {
            apply_collector_command(command, &mut subscriptions);
        }

        if receiver.is_closed() {
            return;
        }

        if subscriptions.is_empty() {
            match receiver.recv().await {
                Some(command) => {
                    apply_collector_command(command, &mut subscriptions);
                    continue;
                }
                None => return,
            }
        }

        match connect_async(&ws_url).await {
            Ok((mut stream, _response)) => {
                reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

                tracing::info!(
                    subscriptions = subscriptions.len(),
                    "connected Hyperliquid trade collector"
                );

                for coin in &subscriptions {
                    if let Err(err) = send_trade_subscription(&mut stream, coin).await {
                        tracing::warn!(coin = %coin, error = %err, "failed to subscribe to coin");
                    }
                }

                let should_reconnect = run_collector_connection(
                    &mut stream,
                    &trade_cache,
                    &mut receiver,
                    &mut subscriptions,
                )
                .await;

                if !should_reconnect {
                    return;
                }

                tracing::warn!("trade collector disconnected, reconnecting");
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to connect trade collector websocket");
            }
        }

        sleep(reconnect_delay).await;
        reconnect_delay = next_reconnect_delay(reconnect_delay);
    }
}

async fn run_collector_connection(
    stream: &mut HyperliquidWsStream,
    trade_cache: &TradeCache,
    receiver: &mut UnboundedReceiver<CollectorCommand>,
    subscriptions: &mut HashSet<String>,
) -> bool {
    loop {
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else {
                    return false;
                };

                match command {
                    CollectorCommand::Subscribe { coin } => {
                        let coin_key = coin.trim();
                        if coin_key.is_empty() {
                            continue;
                        }

                        if subscriptions.insert(coin_key.to_string()) {
                            if let Err(err) = send_trade_subscription(stream, coin_key).await {
                                tracing::warn!(coin = %coin_key, error = %err, "failed to send trade subscription");
                                return true;
                            }
                        }
                    }
                }
            }
            message = stream.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        process_trade_ws_payload(text.as_ref(), trade_cache).await;
                    }
                    Some(Ok(Message::Binary(binary))) => {
                        match String::from_utf8(binary.to_vec()) {
                            Ok(text) => process_trade_ws_payload(&text, trade_cache).await,
                            Err(err) => tracing::warn!(error = %err, "unable to decode binary websocket payload"),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(err) = stream.send(Message::Pong(payload)).await {
                            tracing::warn!(error = %err, "failed to send pong");
                            return true;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        return true;
                    }
                    Some(Err(err)) => {
                        tracing::warn!(error = %err, "trade collector websocket read error");
                        return true;
                    }
                    None => {
                        return true;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn send_trade_subscription(
    stream: &mut HyperliquidWsStream,
    coin: &str,
) -> Result<(), String> {
    let payload = json!({
        "method": "subscribe",
        "subscription": {
            "type": "trades",
            "coin": coin,
        }
    });

    stream
        .send(Message::Text(payload.to_string().into()))
        .await
        .map_err(|err| err.to_string())
}

async fn process_trade_ws_payload(payload: &str, trade_cache: &TradeCache) {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return;
    };

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if channel != "trades" {
        return;
    }

    let Some(data) = value.get("data").and_then(Value::as_array) else {
        return;
    };

    let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
    for trade in data {
        let Some(coin) = trade.get("coin").and_then(Value::as_str) else {
            continue;
        };
        grouped
            .entry(coin.to_string())
            .or_default()
            .push(trade.clone());
    }

    for (coin, trades) in grouped {
        trade_cache.insert_batch(&coin, &trades).await;
    }
}

fn apply_collector_command(command: CollectorCommand, subscriptions: &mut HashSet<String>) {
    match command {
        CollectorCommand::Subscribe { coin } => {
            let coin = coin.trim();
            if !coin.is_empty() {
                subscriptions.insert(coin.to_string());
            }
        }
    }
}

fn next_reconnect_delay(current: Duration) -> Duration {
    let doubled = current.as_millis().saturating_mul(2);
    let bounded = doubled.min(MAX_RECONNECT_DELAY_MS as u128) as u64;
    Duration::from_millis(bounded)
}

fn to_hyperliquid_ws_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://") {
        return format!("wss://{rest}/ws");
    }
    if let Some(rest) = trimmed.strip_prefix("http://") {
        return format!("ws://{rest}/ws");
    }
    format!("wss://{trimmed}/ws")
}

fn map_trades_lossy(raw_trades: Vec<Value>, symbol: &str) -> Vec<CcxtTrade> {
    let mut mapped = Vec::with_capacity(raw_trades.len());
    for raw in raw_trades {
        match map_trade(raw, symbol) {
            Ok(trade) => mapped.push(trade),
            Err(err) => tracing::warn!(error = %err, "unable to map trade from cache"),
        }
    }
    mapped
}

fn map_trade(raw: Value, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = value_u64(&raw, "time")?;
    let id = value_id(&raw, "tid")?;
    let price = value_f64(&raw, "px")?;
    let amount = value_f64(&raw, "sz")?;
    let side = match value_str(&raw, "side")? {
        "B" => "buy".to_string(),
        "A" => "sell".to_string(),
        other => {
            return Err(ExchangeError::UpstreamData(format!(
                "unexpected trade side value `{other}`"
            )));
        }
    };

    let datetime = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid timestamp: {timestamp}")))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    Ok(CcxtTrade {
        info: raw,
        amount: Some(amount),
        datetime: Some(datetime),
        id: Some(id),
        order: None,
        price: Some(price),
        timestamp: Some(timestamp),
        trade_type: None,
        side: Some(side),
        symbol: Some(symbol.to_string()),
        taker_or_maker: None,
        cost: Some(price * amount),
        fee: None,
    })
}

fn map_order_book_side_lossy(levels: &[Value]) -> Vec<(f64, f64)> {
    let mut output = Vec::with_capacity(levels.len());
    for level in levels {
        let Ok(price) = value_f64(level, "px") else {
            continue;
        };
        let Ok(size) = value_f64(level, "sz") else {
            continue;
        };
        output.push((price, size));
    }
    output
}

fn map_candle(raw: Value) -> Result<CcxtOhlcv, ExchangeError> {
    let timestamp = value_u64(&raw, "t")?;
    let open = value_f64(&raw, "o")?;
    let high = value_f64(&raw, "h")?;
    let low = value_f64(&raw, "l")?;
    let close = value_f64(&raw, "c")?;
    let volume = value_f64(&raw, "v")?;

    Ok((timestamp, open, high, low, close, volume))
}

fn parse_token_map(value: &Value) -> Result<HashMap<u64, String>, ExchangeError> {
    let mut out = HashMap::new();

    let Some(tokens) = value.get("tokens").and_then(Value::as_array) else {
        return Ok(out);
    };

    for token in tokens {
        let Some(index) = token.get("index").and_then(Value::as_u64) else {
            continue;
        };
        let Some(name) = token.get("name").and_then(Value::as_str) else {
            continue;
        };
        out.insert(index, name.to_string());
    }

    if out.is_empty() {
        return Err(ExchangeError::UpstreamData(
            "spotMeta did not return token mappings".to_string(),
        ));
    }

    Ok(out)
}

fn build_token_aliases(value: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(tokens) = value.get("tokens").and_then(Value::as_array) else {
        return out;
    };

    for token in tokens {
        let Some(name) = token.get("name").and_then(Value::as_str) else {
            continue;
        };

        if let Some(alias) = known_spot_alias(name) {
            out.insert(name.to_string(), alias.to_string());
            continue;
        }

        let full_name = token
            .get("fullName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if name.starts_with('U') && full_name.starts_with("Unit ") && name.len() > 1 {
            out.insert(name.to_string(), name[1..].to_string());
        }
    }

    out
}

fn known_spot_alias(name: &str) -> Option<&'static str> {
    match name {
        "UDZ" => Some("2Z"),
        "UBONK" => Some("BONK"),
        "UBTC" => Some("BTC"),
        "UETH" => Some("ETH"),
        "UFART" => Some("FARTCOIN"),
        "HPENGU" => Some("PENGU"),
        "UPUMP" => Some("PUMP"),
        "USOL" => Some("SOL"),
        "UUUSPX" => Some("SPX"),
        "USDT0" => Some("USDT"),
        "XAUT0" => Some("XAUT"),
        "UXPL" => Some("XPL"),
        _ => None,
    }
}

fn value_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, ExchangeError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("missing or invalid `{key}` field")))
}

fn value_u64(value: &Value, key: &str) -> Result<u64, ExchangeError> {
    extract_u64_from_json_value(value.get(key))
        .ok_or_else(|| ExchangeError::UpstreamData(format!("missing or invalid `{key}` field")))
}

fn value_id(value: &Value, key: &str) -> Result<String, ExchangeError> {
    let Some(raw) = value.get(key) else {
        return Err(ExchangeError::UpstreamData(format!(
            "missing or invalid `{key}` field"
        )));
    };

    if let Some(text) = raw.as_str() {
        return Ok(text.to_string());
    }

    if let Some(num) = raw.as_u64() {
        return Ok(num.to_string());
    }

    Err(ExchangeError::UpstreamData(format!(
        "missing or invalid `{key}` field"
    )))
}

fn value_f64(value: &Value, key: &str) -> Result<f64, ExchangeError> {
    let Some(raw) = value.get(key) else {
        return Err(ExchangeError::UpstreamData(format!(
            "missing or invalid `{key}` field"
        )));
    };

    if let Some(numeric) = raw.as_f64() {
        return Ok(numeric);
    }

    if let Some(text) = raw.as_str() {
        return text.parse::<f64>().map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse `{key}` as number from value `{text}`: {err}"
            ))
        });
    }

    Err(ExchangeError::UpstreamData(format!(
        "missing or invalid `{key}` field"
    )))
}

fn extract_u64_field(params: &Value, field: &str) -> Option<u64> {
    extract_u64_from_json_value(params.get(field))
}

fn extract_i64_field(params: &Value, field: &str) -> Option<i64> {
    params.get(field).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
    })
}

fn extract_u64_from_json_value(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    value
        .as_u64()
        .or_else(|| {
            value
                .as_i64()
                .and_then(|value| (value >= 0).then_some(value as u64))
        })
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
}

fn interval_to_millis(interval: &str) -> Option<u64> {
    match interval {
        "1m" => Some(60_000),
        "3m" => Some(180_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "30m" => Some(1_800_000),
        "1h" => Some(3_600_000),
        "2h" => Some(7_200_000),
        "4h" => Some(14_400_000),
        "8h" => Some(28_800_000),
        "12h" => Some(43_200_000),
        "1d" => Some(86_400_000),
        "3d" => Some(259_200_000),
        "1w" => Some(604_800_000),
        "1M" => Some(2_592_000_000),
        _ => None,
    }
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn normalize_lookup_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn heuristic_coin_from_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with('@') {
        return Some(trimmed.to_string());
    }

    if !trimmed.contains('/') {
        return Some(trimmed.to_string());
    }

    let mut symbol_parts = trimmed.split('/');
    let base = symbol_parts.next().unwrap_or_default().trim();
    let quote_part = symbol_parts.next().unwrap_or_default().trim();
    if base.is_empty() || quote_part.is_empty() {
        return None;
    }

    let quote = quote_part
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();

    if quote == "USDC" {
        return Some(base.to_string());
    }

    None
}

fn now_millis() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn truncate(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }

    let shortened: String = value.chars().take(limit).collect();
    format!("{shortened}...")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        build_token_aliases, heuristic_coin_from_symbol, interval_to_millis, map_trade,
        trade_identity, TradeCache,
    };

    #[test]
    fn maps_hyperliquid_trade_to_ccxt_shape() {
        let raw = json!({
            "coin": "BTC",
            "side": "B",
            "px": "68008.0",
            "sz": "0.0231",
            "time": 1771354910961u64,
            "hash": "0xabc",
            "tid": 909337828325218u64,
            "users": ["0x1", "0x2"]
        });

        let trade = map_trade(raw, "BTC/USDC:USDC").expect("trade should map");
        assert_eq!(trade.side.as_deref(), Some("buy"));
        assert_eq!(trade.symbol.as_deref(), Some("BTC/USDC:USDC"));
        assert_eq!(trade.id.as_deref(), Some("909337828325218"));
        assert_eq!(trade.price, Some(68008.0));
        assert_eq!(trade.amount, Some(0.0231));
    }

    #[test]
    fn builds_aliases_from_known_mapping_and_unit_prefix() {
        let value = json!({
            "tokens": [
                {"name": "UBTC", "index": 1, "fullName": "Unit Bitcoin"},
                {"name": "UABC", "index": 2, "fullName": "Unit ABC"},
                {"name": "HYPE", "index": 3, "fullName": "Hyperliquid"}
            ]
        });

        let aliases = build_token_aliases(&value);
        assert_eq!(aliases.get("UBTC").map(String::as_str), Some("BTC"));
        assert_eq!(aliases.get("UABC").map(String::as_str), Some("ABC"));
        assert!(aliases.get("HYPE").is_none());
    }

    #[test]
    fn resolves_common_perp_symbol_without_catalog() {
        assert_eq!(
            heuristic_coin_from_symbol("BTC/USDC:USDC"),
            Some("BTC".to_string())
        );
        assert_eq!(
            heuristic_coin_from_symbol("ETH/USDC"),
            Some("ETH".to_string())
        );
        assert_eq!(heuristic_coin_from_symbol("@107"), Some("@107".to_string()));
    }

    #[test]
    fn recognizes_supported_timeframes() {
        assert_eq!(interval_to_millis("1m"), Some(60_000));
        assert_eq!(interval_to_millis("1M"), Some(2_592_000_000));
        assert_eq!(interval_to_millis("invalid"), None);
    }

    #[tokio::test]
    async fn trade_cache_keeps_unique_trade_ids() {
        let cache = TradeCache::new(100, 86_400_000);
        let now = super::now_millis();
        let first = json!({"tid": 1, "time": now, "px": "1", "sz": "1", "side": "B"});
        let duplicate = json!({"tid": 1, "time": now, "px": "1", "sz": "1", "side": "B"});

        cache.insert_batch("BTC", &[first, duplicate]).await;
        let trades = cache.get_trades("BTC").await;
        assert_eq!(trades.len(), 1);
        assert_eq!(trade_identity(&trades[0]).as_deref(), Some("tid:1"));
    }
}
