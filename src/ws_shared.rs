use serde_json::Value;

pub const BINANCE_QUOTES: &[&str] = &["USDT", "USDC", "BUSD", "FDUSD", "USDS"];
pub const BYBIT_QUOTES: &[&str] = &["USDT", "USDC", "USD", "BTC", "ETH", "EUR"];

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
