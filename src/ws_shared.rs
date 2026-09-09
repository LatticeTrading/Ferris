use serde_json::Value;

pub const BINANCE_QUOTES: &[&str] = &["USDT", "USDC", "BUSD", "FDUSD", "USDS"];
pub const ASTER_QUOTES: &[&str] = &["USDT", "USD1", "U"];
pub const ASTER_WS_BASE_URL: &str = "wss://fstream.asterdex.com/ws";
pub const BYBIT_QUOTES: &[&str] = &["USDT", "USDC", "USD", "BTC", "ETH", "EUR"];
pub const EXTENDED_QUOTES: &[&str] = &["USD"];
pub const EXTENDED_WS_BASE_URL: &str =
    "wss://api.starknet.extended.exchange/stream.extended.exchange/v1";
pub const EXTENDED_USER_AGENT: &str = "FerrisMarketDataBackend/0.1";

#[derive(Debug, Clone)]
pub struct WsTrade {
    pub info: Value,
    pub id: Option<String>,
    pub timestamp: Option<u64>,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub amount: Option<f64>,
    pub cost: Option<f64>,
}

pub fn parse_extended_trades(payload: &str) -> Vec<WsTrade> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };
    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        match parse_extended_trade(row) {
            Some(trade) => parsed.push(trade),
            None => tracing::warn!("unable to map Extended trade row, skipping"),
        }
    }
    parsed.sort_by_key(|trade| trade.timestamp);
    parsed
}

pub fn parse_extended_trade(row: &Value) -> Option<WsTrade> {
    let timestamp = row.get("T").and_then(parse_u64_lossy)?;
    let price = row
        .get("p")
        .and_then(parse_f64_lossy)
        .filter(|v| v.is_finite() && *v > 0.0)?;
    let amount = row
        .get("q")
        .and_then(parse_f64_lossy)
        .filter(|v| v.is_finite() && *v >= 0.0)?;
    let side = match row.get("S")?.as_str()? {
        "BUY" => "buy",
        "SELL" => "sell",
        _ => return None,
    };
    let cost = price * amount;
    if !cost.is_finite() {
        return None;
    }
    Some(WsTrade {
        info: row.clone(),
        id: row.get("i").and_then(stringify_json_value),
        timestamp: Some(timestamp),
        side: Some(side.to_string()),
        price: Some(price),
        amount: Some(amount),
        cost: Some(cost),
    })
}

pub fn resolve_extended_ws_symbol(
    symbol: &str,
    coin_override: Option<&str>,
) -> Result<String, String> {
    let raw = coin_override.unwrap_or(symbol).trim().to_ascii_uppercase();
    let core = if let Some((core, settlement)) = raw.split_once(':') {
        if settlement != "USD" {
            return Err(format!("invalid Extended settlement in `{raw}`"));
        }
        core
    } else {
        raw.as_str()
    };
    let base = core
        .strip_suffix("/USD")
        .or_else(|| core.strip_suffix("-USD"))
        .or_else(|| coin_override.map(|_| core))
        .ok_or_else(|| {
            format!("invalid Extended symbol `{raw}`; expected BASE-USD or BASE/USD:USD")
        })?;
    if base.is_empty() || !base.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err(format!("invalid Extended base asset `{base}`"));
    }
    Ok(format!("{base}-{}", EXTENDED_QUOTES[0]))
}

pub fn extended_public_symbol(_symbol: &str, market: &str) -> String {
    let base = market.strip_suffix("-USD").unwrap_or(market);
    format!("{base}/USD:USD")
}

pub fn extended_interval(timeframe: &str) -> Result<&'static str, String> {
    match timeframe.trim() {
        "1m" => Ok("PT1M"),
        "5m" => Ok("PT5M"),
        "15m" => Ok("PT15M"),
        "30m" => Ok("PT30M"),
        "1h" => Ok("PT1H"),
        "2h" => Ok("PT2H"),
        "4h" => Ok("PT4H"),
        "8h" => Ok("PT8H"),
        "12h" => Ok("PT12H"),
        "1d" => Ok("PT24H"),
        "1w" => Ok("P7D"),
        "1M" => Ok("P30D"),
        _ => Err(format!("unsupported Extended timeframe `{timeframe}`")),
    }
}

pub fn extended_candle_type(params: &Value) -> Result<&str, String> {
    match params.get("candleType") {
        None => Ok("trades"),
        Some(Value::String(value))
            if matches!(value.as_str(), "trades" | "mark-prices" | "index-prices") =>
        {
            Ok(value)
        }
        _ => Err("Extended candleType must be trades, mark-prices, or index-prices".to_string()),
    }
}

pub fn parse_extended_candle(row: &Value) -> Option<crate::models::CcxtOhlcv> {
    let number = |key| {
        row.get(key)
            .and_then(parse_f64_lossy)
            .filter(|v| v.is_finite())
    };
    let volume = match row.get("v") {
        None => 0.0,
        Some(_) => number("v")?,
    };
    Some((
        row.get("T").and_then(parse_u64_lossy)?,
        number("o")?,
        number("h")?,
        number("l")?,
        number("c")?,
        volume,
    ))
}

pub fn parse_extended_candles(payload: &str) -> Vec<crate::models::CcxtOhlcv> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };
    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut candles: Vec<_> = rows.iter().filter_map(parse_extended_candle).collect();
    candles.sort_by_key(|candle| candle.0);
    candles
}

pub fn extended_ws_request(
    endpoint: &str,
) -> Result<
    tokio_tungstenite::tungstenite::handshake::client::Request,
    tokio_tungstenite::tungstenite::Error,
> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};
    let mut request = endpoint.into_client_request()?;
    request
        .headers_mut()
        .insert("User-Agent", HeaderValue::from_static(EXTENDED_USER_AGENT));
    Ok(request)
}

#[derive(Default)]
pub struct ExtendedOrderBookState {
    bids: std::collections::BTreeMap<crate::binance_orderbook::BinancePrice, i128>,
    asks: std::collections::BTreeMap<crate::binance_orderbook::BinancePrice, i128>,
    seq: Option<u64>,
}

impl ExtendedOrderBookState {
    pub fn apply(
        &mut self,
        payload: &str,
        symbol: &str,
        limit: usize,
    ) -> Result<Option<crate::models::CcxtOrderBook>, String> {
        let result = self.apply_message(payload, symbol, limit);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn apply_message(
        &mut self,
        payload: &str,
        symbol: &str,
        limit: usize,
    ) -> Result<Option<crate::models::CcxtOrderBook>, String> {
        let value: Value =
            serde_json::from_str(payload).map_err(|err| format!("invalid Extended book: {err}"))?;
        let snapshot = match value.get("type").and_then(Value::as_str) {
            Some("SNAPSHOT") => true,
            Some("DELTA") => false,
            _ => return Ok(None),
        };
        let seq = value
            .get("seq")
            .and_then(parse_u64_lossy)
            .ok_or("Extended book missing seq")?;
        if let Some(previous) = self.seq {
            if previous.checked_add(1) != Some(seq) {
                return Err(format!("Extended book sequence gap: {previous} -> {seq}"));
            }
        } else if !snapshot {
            return Err("Extended book delta before snapshot".to_string());
        }
        let data = value.get("data").ok_or("Extended book missing data")?;
        let market = data
            .get("m")
            .and_then(Value::as_str)
            .ok_or("Extended book missing market")?;
        if resolve_extended_ws_symbol(symbol, None)? != market {
            return Err("Extended book market mismatch".to_string());
        }
        if snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (key, side) in [("b", &mut self.bids), ("a", &mut self.asks)] {
            let Some(rows) = data.get(key) else {
                if snapshot {
                    return Err(format!("Extended snapshot missing {key}"));
                }
                continue;
            };
            for row in rows
                .as_array()
                .ok_or("Extended book side must be an array")?
            {
                let price = crate::binance_orderbook::BinancePrice::parse(
                    row.get("p")
                        .and_then(Value::as_str)
                        .ok_or("Extended book missing price")?,
                )?;
                let absolute = row.get("c");
                let quantity = extended_quantity(
                    absolute
                        .or_else(|| row.get("q"))
                        .and_then(Value::as_str)
                        .ok_or("Extended book missing quantity")?,
                )?;
                let quantity = if snapshot || absolute.is_some() {
                    quantity
                } else {
                    side.get(&price)
                        .copied()
                        .unwrap_or(0)
                        .checked_add(quantity)
                        .ok_or("Extended book quantity overflow")?
                };
                if quantity < 0 {
                    return Err("Extended book negative resulting quantity".to_string());
                }
                if quantity == 0 {
                    side.remove(&price);
                } else {
                    side.insert(price, quantity);
                }
            }
        }
        self.seq = Some(seq);
        let timestamp = value.get("ts").and_then(parse_u64_lossy);
        Ok(Some(crate::models::CcxtOrderBook {
            bids: self
                .bids
                .iter()
                .rev()
                .take(limit)
                .map(|(p, q)| (p.as_f64(), *q as f64 / 1e18))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(limit)
                .map(|(p, q)| (p.as_f64(), *q as f64 / 1e18))
                .collect(),
            timestamp,
            datetime: timestamp
                .and_then(|ts| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(i64::try_from(ts).ok()?)
                })
                .map(|ts| ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            nonce: Some(seq),
            symbol: Some(symbol.to_string()),
        }))
    }
}

fn extended_quantity(raw: &str) -> Result<i128, String> {
    // ponytail: exact fixed-point up to 18 decimals; use a decimal crate if upstream exceeds this.
    let negative = raw.starts_with('-');
    let unsigned = raw.strip_prefix('-').unwrap_or(raw);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let fraction = fraction.trim_end_matches('0');
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 18
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("invalid Extended quantity `{raw}`"));
    }
    let whole = whole
        .parse::<i128>()
        .ok()
        .and_then(|n| n.checked_mul(1_000_000_000_000_000_000));
    let fraction = if fraction.is_empty() {
        Some(0)
    } else {
        fraction
            .parse::<i128>()
            .ok()
            .and_then(|n| n.checked_mul(10i128.pow(18 - fraction.len() as u32)))
    };
    let quantity = whole
        .zip(fraction)
        .and_then(|(a, b)| a.checked_add(b))
        .ok_or_else(|| format!("Extended quantity out of range `{raw}`"))?;
    Ok(if negative { -quantity } else { quantity })
}

pub fn parse_hyperliquid_trades(payload: &str) -> Vec<WsTrade> {
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
        let Some(timestamp) = row.get("time").and_then(parse_u64_lossy) else {
            continue;
        };
        let Some(price) = row.get("px").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(amount) = row.get("sz").and_then(parse_f64_lossy) else {
            continue;
        };

        let side = row
            .get("side")
            .and_then(Value::as_str)
            .map(|value| match value {
                "B" | "BUY" | "buy" => "buy".to_string(),
                "A" | "SELL" | "sell" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        parsed.push(WsTrade {
            info: row.clone(),
            id: row.get("tid").and_then(stringify_json_value),
            timestamp: Some(timestamp),
            side,
            price: Some(price),
            amount: Some(amount),
            cost: Some(price * amount),
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

pub fn parse_binance_trades(payload: &str) -> Vec<WsTrade> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let data = value.get("data").unwrap_or(&value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or_default();
    if event != "trade" && event != "aggTrade" {
        return Vec::new();
    }

    let Some(timestamp) = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy)
    else {
        return Vec::new();
    };
    let Some(price) = data.get("p").and_then(parse_f64_lossy) else {
        return Vec::new();
    };
    let Some(amount) = data.get("q").and_then(parse_f64_lossy) else {
        return Vec::new();
    };

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

    vec![WsTrade {
        info: data.clone(),
        id: data
            .get("t")
            .or_else(|| data.get("a"))
            .and_then(stringify_json_value),
        timestamp: Some(timestamp),
        side,
        price: Some(price),
        amount: Some(amount),
        cost: Some(price * amount),
    }]
}

pub fn parse_bybit_trades(payload: &str) -> Vec<WsTrade> {
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
        let Some(timestamp) = row.get("T").and_then(parse_u64_lossy) else {
            continue;
        };
        let Some(price) = row.get("p").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(amount) = row.get("v").and_then(parse_f64_lossy) else {
            continue;
        };

        let side = row
            .get("S")
            .and_then(Value::as_str)
            .map(|value| match value {
                "Buy" | "BUY" | "buy" => "buy".to_string(),
                "Sell" | "SELL" | "sell" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        parsed.push(WsTrade {
            info: row.clone(),
            id: row.get("i").and_then(stringify_json_value),
            timestamp: Some(timestamp),
            side,
            price: Some(price),
            amount: Some(amount),
            cost: Some(price * amount),
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

pub fn parse_lighter_trades(payload: &str) -> Vec<WsTrade> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    if value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        != "update/trade"
    {
        return Vec::new();
    }

    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !channel.starts_with("trade:") {
        return Vec::new();
    }

    let trades_value = value.get("trades").unwrap_or(&Value::Null);
    let rows = match trades_value {
        Value::Array(rows) => rows.clone(),
        Value::Object(_) => vec![trades_value.clone()],
        _ => return Vec::new(),
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(price) = row.get("price").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(amount) = row.get("size").and_then(parse_f64_lossy) else {
            continue;
        };
        let timestamp = row
            .get("timestamp")
            .and_then(parse_u64_lossy)
            .map(normalize_lighter_timestamp_ms);
        let side = row
            .get("is_maker_ask")
            .and_then(Value::as_bool)
            .map(|is_maker_ask| {
                if is_maker_ask {
                    "buy".to_string()
                } else {
                    "sell".to_string()
                }
            });
        let cost = row
            .get("usd_amount")
            .and_then(parse_f64_lossy)
            .or(Some(price * amount));

        parsed.push(WsTrade {
            info: row.clone(),
            id: row.get("trade_id").and_then(stringify_json_value),
            timestamp,
            side,
            price: Some(price),
            amount: Some(amount),
            cost,
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

pub fn infer_hyperliquid_coin_from_symbol(symbol: &str) -> Option<String> {
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
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn resolve_binance_ws_symbol(
    symbol: &str,
    coin_override: Option<&str>,
) -> Result<String, String> {
    if let Some(coin) = coin_override {
        let asset = sanitize_asset(coin, "Binance")?;
        return Ok(format!("{asset}USDT"));
    }

    normalize_market_symbol(symbol, "Binance")
}

pub fn resolve_aster_ws_symbol(
    symbol: &str,
    coin_override: Option<&str>,
) -> Result<String, String> {
    if let Some(coin) = coin_override {
        let asset = sanitize_asset(coin, "Aster")?;
        if split_market_symbol(&asset, ASTER_QUOTES).is_some() {
            return Ok(asset);
        }
        return Ok(format!("{asset}USDT"));
    }
    let market = if symbol.contains('/') {
        normalize_market_symbol(symbol, "Aster")?
    } else {
        sanitize_asset(symbol.split(':').next().unwrap_or_default(), "Aster")?
    };
    if split_market_symbol(&market, ASTER_QUOTES).is_none() {
        return Err(format!("invalid Aster symbol `{symbol}`"));
    }
    Ok(market)
}

pub fn resolve_bybit_ws_symbol(
    symbol: &str,
    coin_override: Option<&str>,
) -> Result<String, String> {
    if let Some(coin) = coin_override {
        let asset = sanitize_asset(coin, "Bybit")?;
        if has_bybit_quote_suffix(&asset) {
            return Ok(asset);
        }
        return Ok(format!("{asset}USDT"));
    }
    normalize_market_symbol(symbol, "Bybit")
}

pub fn resolve_public_symbol(
    original_symbol: &str,
    market_symbol: &str,
    quotes: &[&str],
) -> String {
    let trimmed = original_symbol.trim();
    if trimmed.contains('/') {
        return trimmed.to_string();
    }

    if let Some((base, quote)) = split_market_symbol(market_symbol, quotes) {
        return format!("{base}/{quote}:{quote}");
    }

    trimmed.to_string()
}

pub fn has_bybit_quote_suffix(symbol: &str) -> bool {
    BYBIT_QUOTES
        .iter()
        .any(|quote| symbol.len() > quote.len() && symbol.ends_with(quote))
}

pub fn parse_u64_lossy(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_i64()
                .and_then(|numeric| (numeric >= 0).then_some(numeric as u64))
        })
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
}

pub fn parse_f64_lossy(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|numeric| numeric as f64))
        .or_else(|| value.as_u64().map(|numeric| numeric as f64))
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}

pub fn stringify_json_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn normalize_lighter_timestamp_ms(timestamp: u64) -> u64 {
    if timestamp < 1_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else if timestamp >= 1_000_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

fn normalize_market_symbol(symbol: &str, exchange_name: &str) -> Result<String, String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err("`symbol` cannot be empty".to_string());
    }

    let core = trimmed.split(':').next().unwrap_or_default().trim();
    if core.is_empty() {
        return Err(format!("invalid {exchange_name} symbol `{symbol}`"));
    }

    if core.contains('/') {
        let mut parts = core.split('/');
        let base = sanitize_asset(parts.next().unwrap_or_default(), exchange_name)?;
        let quote = sanitize_asset(parts.next().unwrap_or_default(), exchange_name)?;

        if parts.next().is_some() {
            return Err(format!("invalid {exchange_name} symbol `{symbol}`"));
        }

        return Ok(format!("{base}{quote}"));
    }

    let collapsed = core
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();

    if collapsed.len() < 6 {
        return Err(format!("invalid {exchange_name} symbol `{symbol}`"));
    }

    Ok(collapsed)
}

fn sanitize_asset(value: &str, exchange_name: &str) -> Result<String, String> {
    let normalized = value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();

    if normalized.is_empty() {
        return Err(format!(
            "invalid {exchange_name} symbol: base/quote assets must be non-empty"
        ));
    }

    Ok(normalized)
}

fn split_market_symbol(market_symbol: &str, quotes: &[&str]) -> Option<(String, String)> {
    for quote in quotes {
        if market_symbol.len() <= quote.len() || !market_symbol.ends_with(quote) {
            continue;
        }

        let base = market_symbol[..market_symbol.len() - quote.len()].to_string();
        if !base.is_empty() {
            return Some((base, (*quote).to_string()));
        }
    }

    None
}

#[cfg(test)]
mod extended_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extended_symbols_timeframes_and_candles() {
        for symbol in ["btc-usd", "BTC/USD", "BTC/USD:USD"] {
            let market = resolve_extended_ws_symbol(symbol, None).unwrap();
            assert_eq!(market, "BTC-USD");
            assert_eq!(extended_public_symbol(symbol, &market), "BTC/USD:USD");
        }
        for coin in ["ETH", "eth-usd", "ETH/USD:USD"] {
            assert_eq!(
                resolve_extended_ws_symbol("BTC/USD", Some(coin)).unwrap(),
                "ETH-USD"
            );
        }
        for symbol in [
            "",
            "BTC/USDT",
            "BTC/USD:USDC",
            "BTC/USD/ETH",
            "BTC-USD?depth=1",
        ] {
            assert!(resolve_extended_ws_symbol(symbol, None).is_err());
        }
        for (timeframe, interval) in [
            ("1m", "PT1M"),
            ("5m", "PT5M"),
            ("15m", "PT15M"),
            ("30m", "PT30M"),
            ("1h", "PT1H"),
            ("2h", "PT2H"),
            ("4h", "PT4H"),
            ("8h", "PT8H"),
            ("12h", "PT12H"),
            ("1d", "PT24H"),
            ("1w", "P7D"),
            ("1M", "P30D"),
        ] {
            assert_eq!(extended_interval(timeframe).unwrap(), interval);
        }
        assert!(extended_interval("3m").is_err());
        assert!(extended_candle_type(&json!({"candleType":"funding"})).is_err());
        let candles = parse_extended_candles(&json!({"data":[{"T":2,"o":"1","h":"3","l":"0.5","c":"2","v":"4"},{"T":1,"o":"1","h":"2","l":"0.5","c":"1.5"}]}).to_string());
        assert_eq!(
            candles,
            vec![(1, 1.0, 2.0, 0.5, 1.5, 0.0), (2, 1.0, 3.0, 0.5, 2.0, 4.0)]
        );
        assert!(
            parse_extended_candle(&json!({"T":1,"o":1,"h":2,"l":0.5,"c":1,"v":"NaN"})).is_none()
        );
        let trades = parse_extended_trades(&json!({"ts":3,"seq":1,"data":[{"T":2,"i":"b","S":"SELL","p":"3","q":"2"},{"T":1,"i":1,"S":"BUY","p":"2","q":"4"},{"T":1,"p":"bad"}]}).to_string());
        assert_eq!(
            trades
                .iter()
                .map(|t| (t.timestamp, t.side.as_deref(), t.cost))
                .collect::<Vec<_>>(),
            vec![
                (Some(1), Some("buy"), Some(8.0)),
                (Some(2), Some("sell"), Some(6.0))
            ]
        );
    }

    #[test]
    fn extended_book_sync_preserves_depth_and_exact_delta_removal() {
        let event = |kind, seq, bids: Value, asks: Value| {
            json!({"type":kind,"seq":seq,"ts":1000,"data":{"m":"BTC-USD","b":bids,"a":asks}})
                .to_string()
        };
        let mut state = ExtendedOrderBookState::default();
        let snapshot = event(
            "SNAPSHOT",
            1,
            json!([{"p":"100","q":"0.1"},{"p":"99","q":"2"}]),
            json!([{"p":"102","q":"9","c":"3"}]),
        );
        let book = state.apply(&snapshot, "BTC/USD:USD", 1).unwrap().unwrap();
        assert_eq!(book.bids, vec![(100.0, 0.1)]);
        assert_eq!(book.asks, vec![(102.0, 3.0)]);
        state
            .apply(
                &event("DELTA", 2, json!([{"p":"100.0","q":"0.2"}]), json!([])),
                "BTC/USD:USD",
                1,
            )
            .unwrap();
        let book = state
            .apply(
                &event(
                    "DELTA",
                    3,
                    json!([{"p":"100","q":"-0.3"}]),
                    json!([{"p":"102","q":"5","c":"0"}]),
                ),
                "BTC/USD:USD",
                1,
            )
            .unwrap()
            .unwrap();
        assert_eq!(book.bids, vec![(99.0, 2.0)]);
        assert!(book.asks.is_empty());
        assert_eq!(book.nonce, Some(3));
        let book = state
            .apply(
                &event("SNAPSHOT", 4, json!([]), json!([{"p":"101","q":"1"}])),
                "BTC/USD:USD",
                2,
            )
            .unwrap()
            .unwrap();
        assert!(book.bids.is_empty());
        assert_eq!(book.asks, vec![(101.0, 1.0)]);
        assert!(state
            .apply(&event("DELTA", 6, json!([]), json!([])), "BTC/USD:USD", 1)
            .is_err());
        assert!(state
            .apply(&event("DELTA", 7, json!([]), json!([])), "BTC/USD:USD", 1)
            .is_err());
        assert!(state.apply(&snapshot, "BTC/USD:USD", 1).unwrap().is_some());
        assert!(state
            .apply(
                &event("DELTA", 2, json!([{"p":"100","q":"-1"}]), json!([])),
                "BTC/USD:USD",
                1
            )
            .is_err());
    }
}
