use anyhow::{bail, Context};
use futures_util::SinkExt;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    cli::Config,
    constants::{
        DEFAULT_BINANCE_FUTURES_WS_URL, DEFAULT_BYBIT_LINEAR_WS_URL, DEFAULT_HYPERLIQUID_WS_URL,
    },
    view::{iso8601_millis, OrderBookSnapshot, TradeRow},
};

pub(crate) fn ensure_hyperliquid_ws(config: &Config) -> anyhow::Result<()> {
    if config.exchange != "hyperliquid" {
        bail!(
            "ws transport is currently supported only for exchange=hyperliquid (got `{}`)",
            config.exchange
        );
    }

    Ok(())
}

pub(crate) fn resolved_ws_base_url(config: &Config) -> String {
    let configured = config.ws_url.trim();
    if configured.is_empty() {
        return DEFAULT_HYPERLIQUID_WS_URL.to_string();
    }

    if configured != DEFAULT_HYPERLIQUID_WS_URL {
        return configured.to_string();
    }

    match config.exchange.as_str() {
        "binance" => DEFAULT_BINANCE_FUTURES_WS_URL.to_string(),
        "bybit" => DEFAULT_BYBIT_LINEAR_WS_URL.to_string(),
        _ => DEFAULT_HYPERLIQUID_WS_URL.to_string(),
    }
}

pub(crate) fn build_binance_trade_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    let symbol = resolve_binance_ws_symbol(config)?;
    let stream_name = format!("{}@trade", symbol.to_ascii_lowercase());
    Ok(format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        stream_name
    ))
}

pub(crate) fn build_bybit_trade_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    if base_url.trim().is_empty() {
        bail!("invalid Bybit websocket endpoint");
    }

    Ok(base_url.trim_end_matches('/').to_string())
}

pub(crate) fn build_bybit_orderbook_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    build_bybit_trade_ws_endpoint(config)
}

pub(crate) fn build_binance_orderbook_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    let symbol = resolve_binance_ws_symbol(config)?;
    let depth_levels = to_binance_ws_depth_levels(config.orderbook_levels);
    let stream_name = format!(
        "{}@depth{}@100ms",
        symbol.to_ascii_lowercase(),
        depth_levels
    );
    Ok(format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        stream_name
    ))
}

pub(crate) fn to_binance_ws_depth_levels(levels: usize) -> usize {
    if levels <= 5 {
        5
    } else if levels <= 10 {
        10
    } else {
        20
    }
}

pub(crate) fn to_bybit_ws_depth_levels(levels: usize) -> usize {
    if levels <= 1 {
        1
    } else if levels <= 50 {
        50
    } else if levels <= 200 {
        200
    } else {
        1_000
    }
}

pub(crate) fn resolve_binance_ws_symbol(config: &Config) -> anyhow::Result<String> {
    if let Some(coin) = config.coin.as_deref() {
        let base = coin
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();

        if base.is_empty() {
            bail!("`--coin` cannot be empty for exchange=binance");
        }

        return Ok(format!("{base}USDT"));
    }

    let core = config
        .symbol
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();

    let market_symbol = if core.contains('/') {
        let mut parts = core.split('/');
        let base = parts.next().unwrap_or_default().trim();
        let quote = parts.next().unwrap_or_default().trim();
        if base.is_empty() || quote.is_empty() || parts.next().is_some() {
            bail!("invalid Binance symbol `{}`", config.symbol);
        }
        format!("{base}{quote}")
    } else {
        core.chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
    };

    if market_symbol.len() < 6 {
        bail!("invalid Binance symbol `{}`", config.symbol);
    }

    Ok(market_symbol)
}

pub(crate) fn resolve_bybit_ws_symbol(config: &Config) -> anyhow::Result<String> {
    if let Some(coin) = config.coin.as_deref() {
        let asset = coin
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();

        if asset.is_empty() {
            bail!("`--coin` cannot be empty for exchange=bybit");
        }

        if has_bybit_quote_suffix(&asset) {
            return Ok(asset);
        }

        return Ok(format!("{asset}USDT"));
    }

    let core = config
        .symbol
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();

    let market_symbol = if core.contains('/') {
        let mut parts = core.split('/');
        let base = parts.next().unwrap_or_default().trim();
        let quote = parts.next().unwrap_or_default().trim();
        if base.is_empty() || quote.is_empty() || parts.next().is_some() {
            bail!("invalid Bybit symbol `{}`", config.symbol);
        }
        format!("{base}{quote}")
    } else {
        core.chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
    };

    if market_symbol.len() < 6 {
        bail!("invalid Bybit symbol `{}`", config.symbol);
    }

    Ok(market_symbol)
}

pub(crate) fn has_bybit_quote_suffix(symbol: &str) -> bool {
    ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"]
        .iter()
        .any(|quote| symbol.len() > quote.len() && symbol.ends_with(quote))
}

pub(crate) fn resolve_hyperliquid_coin(config: &Config) -> String {
    if let Some(coin) = config.coin.as_deref() {
        return coin.trim().to_string();
    }

    infer_hyperliquid_coin_from_symbol(&config.symbol)
        .unwrap_or_else(|| config.symbol.trim().to_string())
}

pub(crate) fn infer_hyperliquid_coin_from_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with('@') {
        return Some(trimmed.to_string());
    }

    trimmed
        .split('/')
        .next()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
}

pub(crate) type HyperliquidWsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub(crate) async fn send_hyperliquid_subscription(
    stream: &mut HyperliquidWsStream,
    subscription: Value,
) -> anyhow::Result<()> {
    let payload = json!({
        "method": "subscribe",
        "subscription": subscription,
    });

    stream
        .send(Message::Text(payload.to_string().into()))
        .await
        .context("failed to send websocket subscription")
}

pub(crate) async fn send_bybit_trade_subscription(
    stream: &mut HyperliquidWsStream,
    symbol: &str,
) -> anyhow::Result<()> {
    let topic = format!("publicTrade.{symbol}");
    send_bybit_subscription(stream, &topic, "trade").await
}

pub(crate) async fn send_bybit_orderbook_subscription(
    stream: &mut HyperliquidWsStream,
    symbol: &str,
    depth: usize,
) -> anyhow::Result<()> {
    let topic = format!("orderbook.{depth}.{symbol}");
    send_bybit_subscription(stream, &topic, "orderbook").await
}

pub(crate) async fn send_bybit_subscription(
    stream: &mut HyperliquidWsStream,
    topic: &str,
    channel: &str,
) -> anyhow::Result<()> {
    let payload = json!({
        "op": "subscribe",
        "args": [topic],
    });

    stream
        .send(Message::Text(payload.to_string().into()))
        .await
        .with_context(|| format!("failed to send Bybit {channel} websocket subscription"))
}

pub(crate) fn ws_message_text(message: Message) -> anyhow::Result<Option<String>> {
    match message {
        Message::Text(text) => Ok(Some(text.to_string())),
        Message::Binary(bytes) => {
            let text = String::from_utf8(bytes.to_vec())
                .context("unable to decode websocket binary payload")?;
            Ok(Some(text))
        }
        Message::Pong(_) | Message::Frame(_) => Ok(None),
        Message::Ping(_) | Message::Close(_) => Ok(None),
    }
}

pub(crate) fn parse_hyperliquid_orderbook_message(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<OrderBookSnapshot> {
    let value = serde_json::from_str::<Value>(payload).ok()?;
    if value.get("channel").and_then(Value::as_str)? != "l2Book" {
        return None;
    }

    let data = value.get("data")?;
    let timestamp = parse_u64_lossy(data.get("time")?);
    let datetime = timestamp.and_then(iso8601_millis);

    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }

    let bids = parse_hyperliquid_levels(levels.first()?, levels_limit, true);
    let asks = parse_hyperliquid_levels(levels.get(1)?, levels_limit, false);

    Some(OrderBookSnapshot {
        asks,
        bids,
        datetime,
        timestamp,
        symbol: Some(symbol.to_string()),
    })
}

pub(crate) fn parse_hyperliquid_levels(
    levels: &Value,
    limit: usize,
    descending: bool,
) -> Vec<(f64, f64)> {
    let Some(levels) = levels.as_array() else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let Some(px) = level.get("px").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(sz) = level.get("sz").and_then(parse_f64_lossy) else {
            continue;
        };
        parsed.push((px, sz));
    }

    if descending {
        parsed.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        parsed.sort_by(|left, right| left.0.total_cmp(&right.0));
    }

    if parsed.len() > limit {
        parsed.truncate(limit);
    }

    parsed
}

pub(crate) fn parse_hyperliquid_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    if value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        != "trades"
    {
        return Vec::new();
    }

    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row.get("time").and_then(parse_u64_lossy);
        let price = row.get("px").and_then(parse_f64_lossy);
        let amount = row.get("sz").and_then(parse_f64_lossy);

        let side = row
            .get("side")
            .and_then(Value::as_str)
            .map(|value| match value {
                "B" | "buy" | "BUY" => "buy".to_string(),
                "A" | "sell" | "SELL" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        let id = row.get("tid").and_then(stringify_json_value);

        parsed.push(TradeRow {
            id,
            timestamp,
            datetime: timestamp.and_then(iso8601_millis),
            side,
            price,
            amount,
            cost: match (price, amount) {
                (Some(price), Some(amount)) => Some(price * amount),
                _ => None,
            },
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

pub(crate) fn parse_binance_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let data = value.get("data").unwrap_or(&value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or_default();
    if event != "trade" && event != "aggTrade" {
        return Vec::new();
    }

    let timestamp = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy);
    let price = data.get("p").and_then(parse_f64_lossy);
    let amount = data.get("q").and_then(parse_f64_lossy);
    let id = data
        .get("t")
        .or_else(|| data.get("a"))
        .and_then(stringify_json_value);

    let side = data
        .get("m")
        .and_then(Value::as_bool)
        .map(|is_buyer_maker| {
            if is_buyer_maker {
                "sell".to_string()
            } else {
                "buy".to_string()
            }
        });

    let cost = match (price, amount) {
        (Some(price), Some(amount)) => Some(price * amount),
        _ => None,
    };

    vec![TradeRow {
        id,
        timestamp,
        datetime: timestamp.and_then(iso8601_millis),
        side,
        price,
        amount,
        cost,
    }]
}

pub(crate) fn parse_bybit_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !topic.starts_with("publicTrade.") {
        return Vec::new();
    }

    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row.get("T").and_then(parse_u64_lossy);
        let price = row.get("p").and_then(parse_f64_lossy);
        let amount = row.get("v").and_then(parse_f64_lossy);
        let id = row.get("i").and_then(stringify_json_value);
        let side = row
            .get("S")
            .and_then(Value::as_str)
            .map(|value| match value {
                "Buy" | "BUY" | "buy" => "buy".to_string(),
                "Sell" | "SELL" | "sell" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        let cost = match (price, amount) {
            (Some(price), Some(amount)) => Some(price * amount),
            _ => None,
        };

        parsed.push(TradeRow {
            id,
            timestamp,
            datetime: timestamp.and_then(iso8601_millis),
            side,
            price,
            amount,
            cost,
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BybitOrderBookEventType {
    Snapshot,
    Delta,
}

#[derive(Debug)]
pub(crate) struct BybitOrderBookEvent {
    pub(crate) event_type: BybitOrderBookEventType,
    pub(crate) bids: Vec<(f64, f64)>,
    pub(crate) asks: Vec<(f64, f64)>,
    pub(crate) timestamp: Option<u64>,
}

pub(crate) fn parse_bybit_orderbook_message(payload: &str) -> Option<BybitOrderBookEvent> {
    let value = serde_json::from_str::<Value>(payload).ok()?;

    let topic = value.get("topic")?.as_str()?;
    if !topic.starts_with("orderbook.") {
        return None;
    }

    let event_type = match value.get("type").and_then(Value::as_str)? {
        "snapshot" => BybitOrderBookEventType::Snapshot,
        "delta" => BybitOrderBookEventType::Delta,
        _ => return None,
    };

    let data = value.get("data")?;
    let bids = parse_bybit_orderbook_side(data.get("b"));
    let asks = parse_bybit_orderbook_side(data.get("a"));

    let timestamp = value
        .get("ts")
        .and_then(parse_u64_lossy)
        .or_else(|| data.get("cts").and_then(parse_u64_lossy));

    Some(BybitOrderBookEvent {
        event_type,
        bids,
        asks,
        timestamp,
    })
}

pub(crate) fn parse_bybit_orderbook_side(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut output = Vec::with_capacity(levels.len());
    for row in levels {
        let Some(row) = row.as_array() else {
            continue;
        };
        if row.len() < 2 {
            continue;
        }

        let Some(price) = row.first().and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(size) = row.get(1).and_then(parse_f64_lossy) else {
            continue;
        };

        output.push((price, size));
    }

    output
}

pub(crate) fn apply_bybit_orderbook_event(
    state: &mut Option<OrderBookSnapshot>,
    event: BybitOrderBookEvent,
    levels_limit: usize,
    default_symbol: &str,
) -> bool {
    let symbol = default_symbol.to_string();
    let datetime = event.timestamp.and_then(iso8601_millis);

    match event.event_type {
        BybitOrderBookEventType::Snapshot => {
            let mut bids = event.bids;
            let mut asks = event.asks;

            normalize_bybit_book_side(&mut bids, true, levels_limit);
            normalize_bybit_book_side(&mut asks, false, levels_limit);

            *state = Some(OrderBookSnapshot {
                asks,
                bids,
                datetime,
                timestamp: event.timestamp,
                symbol: Some(symbol),
            });
            true
        }
        BybitOrderBookEventType::Delta => {
            let Some(book) = state.as_mut() else {
                return false;
            };

            apply_bybit_level_updates(&mut book.bids, &event.bids, true);
            apply_bybit_level_updates(&mut book.asks, &event.asks, false);

            normalize_bybit_book_side(&mut book.bids, true, levels_limit);
            normalize_bybit_book_side(&mut book.asks, false, levels_limit);

            book.timestamp = event.timestamp;
            book.datetime = datetime;
            book.symbol = Some(symbol);
            true
        }
    }
}

pub(crate) fn apply_bybit_level_updates(
    levels: &mut Vec<(f64, f64)>,
    updates: &[(f64, f64)],
    descending: bool,
) {
    for (price, size) in updates {
        let existing_index = levels
            .iter()
            .position(|(existing_price, _)| bybit_price_eq(*existing_price, *price));

        if *size <= 0.0 {
            if let Some(index) = existing_index {
                levels.remove(index);
            }
            continue;
        }

        if let Some(index) = existing_index {
            levels[index].1 = *size;
        } else {
            levels.push((*price, *size));
        }
    }

    sort_bybit_book_side(levels, descending);
}

pub(crate) fn normalize_bybit_book_side(
    levels: &mut Vec<(f64, f64)>,
    descending: bool,
    levels_limit: usize,
) {
    levels.retain(|(_, size)| *size > 0.0);
    sort_bybit_book_side(levels, descending);
    if levels.len() > levels_limit {
        levels.truncate(levels_limit);
    }
}

pub(crate) fn sort_bybit_book_side(levels: &mut [(f64, f64)], descending: bool) {
    if descending {
        levels.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        levels.sort_by(|left, right| left.0.total_cmp(&right.0));
    }
}

pub(crate) fn bybit_price_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

pub(crate) fn parse_binance_orderbook_message(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<OrderBookSnapshot> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return None;
    };

    let data = value.get("data").unwrap_or(&value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or_default();
    if event != "depthUpdate" {
        return None;
    }

    let timestamp = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy);
    let datetime = timestamp.and_then(iso8601_millis);

    let mut bids = parse_binance_orderbook_side(data.get("b"), levels_limit);
    let mut asks = parse_binance_orderbook_side(data.get("a"), levels_limit);

    bids.sort_by(|left, right| right.0.total_cmp(&left.0));
    asks.sort_by(|left, right| left.0.total_cmp(&right.0));

    if bids.len() > levels_limit {
        bids.truncate(levels_limit);
    }
    if asks.len() > levels_limit {
        asks.truncate(levels_limit);
    }

    Some(OrderBookSnapshot {
        asks,
        bids,
        datetime,
        timestamp,
        symbol: Some(symbol.to_string()),
    })
}

pub(crate) fn parse_binance_orderbook_side(value: Option<&Value>, limit: usize) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut output = Vec::with_capacity(levels.len().min(limit));

    for row in levels.iter().take(limit) {
        let Some(row) = row.as_array() else {
            continue;
        };
        if row.len() < 2 {
            continue;
        }

        let Some(price) = row.first().and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(size) = row.get(1).and_then(parse_f64_lossy) else {
            continue;
        };
        output.push((price, size));
    }

    output
}

pub(crate) fn parse_u64_lossy(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

pub(crate) fn parse_f64_lossy(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

pub(crate) fn stringify_json_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
