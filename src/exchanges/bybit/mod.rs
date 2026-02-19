use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams,
    },
};

const DEFAULT_BYBIT_BASE_URL: &str = "https://api.bybit.com";

const DEFAULT_FETCH_TRADES_LIMIT: usize = 100;
const MAX_FETCH_TRADES_LIMIT: usize = 1_000;
const MAX_FETCH_TRADES_LIMIT_SPOT: usize = 60;

const DEFAULT_FETCH_OHLCV_LIMIT: usize = 200;
const MAX_FETCH_OHLCV_LIMIT: usize = 1_000;

const DEFAULT_FETCH_ORDER_BOOK_LEVELS: usize = 100;

pub struct BybitExchange {
    http_client: reqwest::Client,
    base_url: String,
}

impl BybitExchange {
    pub fn new(timeout_ms: u64) -> Result<Self, ExchangeError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build Bybit REST client: {err}"))
            })?;

        Ok(Self {
            http_client,
            base_url: DEFAULT_BYBIT_BASE_URL.to_string(),
        })
    }

    async fn get_public_market_result(
        &self,
        endpoint_path: &str,
        query: &[(&str, String)],
    ) -> Result<Value, ExchangeError> {
        let endpoint = format!("{}{}", self.base_url, endpoint_path);

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
                "bybit status={status} body={} query={:?}",
                truncate(&body, 240),
                query,
            )));
        }

        let envelope: BybitEnvelope = serde_json::from_str(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse Bybit response: {err}; body={}",
                truncate(&body, 240)
            ))
        })?;

        if envelope.ret_code != 0 {
            return Err(map_bybit_api_error(envelope.ret_code, &envelope.ret_msg));
        }

        envelope.result.ok_or_else(|| {
            ExchangeError::UpstreamData("Bybit response missing `result`".to_string())
        })
    }
}

#[async_trait]
impl MarketDataExchange for BybitExchange {
    fn id(&self) -> &'static str {
        "bybit"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let category = parse_category_from_params(&params.params)?;
        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let resolved_symbol = resolve_public_symbol(&params.symbol, &market_symbol);

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_TRADES_LIMIT)
            .clamp(1, max_trade_limit_for_category(category));

        let query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", market_symbol.clone()),
            ("limit", requested_limit.to_string()),
        ];

        let result = self
            .get_public_market_result("/v5/market/recent-trade", &query)
            .await?;

        let rows = result
            .get("list")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ExchangeError::UpstreamData(
                    "Bybit recent trades response missing `result.list`".to_string(),
                )
            })?;

        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            match map_trade_row(row, &resolved_symbol) {
                Ok(trade) => mapped.push(trade),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map Bybit trade, skipping");
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
        let category = parse_category_from_params(&params.params)?;
        if matches!(category, BybitCategory::Option) {
            return Err(ExchangeError::BadSymbol(
                "Bybit kline does not support category `option`".to_string(),
            ));
        }

        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let timeframe = params
            .timeframe
            .unwrap_or_else(|| "1m".to_string())
            .trim()
            .to_string();

        let interval = map_timeframe_to_bybit_interval(&timeframe).ok_or_else(|| {
            ExchangeError::BadSymbol(format!("unsupported timeframe `{timeframe}`"))
        })?;

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_OHLCV_LIMIT)
            .clamp(1, MAX_FETCH_OHLCV_LIMIT);

        let mut query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", market_symbol),
            ("interval", interval.to_string()),
            ("limit", requested_limit.to_string()),
        ];

        if let Some(start) = params.since {
            query.push(("start", start.to_string()));
        }
        if let Some(end) = extract_u64_field(&params.params, "until")
            .or_else(|| extract_u64_field(&params.params, "endTime"))
        {
            query.push(("end", end.to_string()));
        }

        let result = self
            .get_public_market_result("/v5/market/kline", &query)
            .await?;

        let rows = result
            .get("list")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ExchangeError::UpstreamData(
                    "Bybit kline response missing `result.list`".to_string(),
                )
            })?;

        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            match map_kline_row(row) {
                Ok(candle) => mapped.push(candle),
                Err(err) => tracing::warn!(error = %err, "unable to map Bybit kline row, skipping"),
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
        let category = parse_category_from_params(&params.params)?;
        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let resolved_symbol = resolve_public_symbol(&params.symbol, &market_symbol);

        let requested_levels = params
            .limit
            .unwrap_or(DEFAULT_FETCH_ORDER_BOOK_LEVELS)
            .clamp(1, max_order_book_limit_for_category(category));

        let query = vec![
            ("category", category.as_str().to_string()),
            ("symbol", market_symbol),
            ("limit", requested_levels.to_string()),
        ];

        let result = self
            .get_public_market_result("/v5/market/orderbook", &query)
            .await?;

        let mut bids = map_order_book_levels(result.get("b"), "bids")?;
        let mut asks = map_order_book_levels(result.get("a"), "asks")?;

        bids.sort_by(|left, right| right.0.total_cmp(&left.0));
        asks.sort_by(|left, right| left.0.total_cmp(&right.0));

        if bids.len() > requested_levels {
            bids.truncate(requested_levels);
        }
        if asks.len() > requested_levels {
            asks.truncate(requested_levels);
        }

        let timestamp = result.get("ts").and_then(parse_u64_lossy);
        let datetime = timestamp.and_then(iso8601_millis);
        let nonce = result.get("u").and_then(parse_u64_lossy);

        Ok(CcxtOrderBook {
            asks,
            bids,
            datetime,
            timestamp,
            nonce,
            symbol: Some(resolved_symbol),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BybitCategory {
    Spot,
    Linear,
    Inverse,
    Option,
}

impl BybitCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
            Self::Option => "option",
        }
    }
}

fn max_trade_limit_for_category(category: BybitCategory) -> usize {
    match category {
        BybitCategory::Spot => MAX_FETCH_TRADES_LIMIT_SPOT,
        _ => MAX_FETCH_TRADES_LIMIT,
    }
}

fn max_order_book_limit_for_category(category: BybitCategory) -> usize {
    match category {
        BybitCategory::Spot => 200,
        BybitCategory::Linear | BybitCategory::Inverse => 500,
        BybitCategory::Option => 25,
    }
}

fn parse_category_from_params(params: &Value) -> Result<BybitCategory, ExchangeError> {
    let Some(raw) = params.get("category").and_then(Value::as_str) else {
        return Ok(BybitCategory::Linear);
    };

    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(BybitCategory::Linear);
    }

    match normalized.as_str() {
        "spot" => Ok(BybitCategory::Spot),
        "linear" => Ok(BybitCategory::Linear),
        "inverse" => Ok(BybitCategory::Inverse),
        "option" | "options" => Ok(BybitCategory::Option),
        other => Err(ExchangeError::BadSymbol(format!(
            "unsupported Bybit category `{other}`"
        ))),
    }
}

fn map_trade_row(raw: &Value, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = raw
        .get("time")
        .and_then(parse_u64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit trade missing `time`".to_string()))?;

    let price = raw
        .get("price")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit trade missing `price`".to_string()))?;

    let amount = raw
        .get("size")
        .and_then(parse_f64_lossy)
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit trade missing `size`".to_string()))?;

    let datetime = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid timestamp `{timestamp}`")))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    let side = raw
        .get("side")
        .and_then(Value::as_str)
        .map(|value| match value {
            "Buy" | "BUY" | "buy" => "buy".to_string(),
            "Sell" | "SELL" | "sell" => "sell".to_string(),
            other => other.to_ascii_lowercase(),
        });

    let id = raw.get("execId").and_then(stringify_json_value);
    let cost = price * amount;

    Ok(CcxtTrade {
        info: raw.clone(),
        amount: Some(amount),
        datetime: Some(datetime),
        id,
        order: None,
        price: Some(price),
        timestamp: Some(timestamp),
        trade_type: None,
        side,
        symbol: Some(symbol.to_string()),
        taker_or_maker: None,
        cost: Some(cost),
        fee: None,
    })
}

fn map_kline_row(raw: &Value) -> Result<CcxtOhlcv, ExchangeError> {
    let row = raw.as_array().ok_or_else(|| {
        ExchangeError::UpstreamData("Bybit kline row is not an array".to_string())
    })?;

    if row.len() < 6 {
        return Err(ExchangeError::UpstreamData(format!(
            "Bybit kline row expected at least 6 fields, got {}",
            row.len()
        )));
    }

    let timestamp = parse_u64_lossy(&row[0]).ok_or_else(|| {
        ExchangeError::UpstreamData("Bybit kline missing `startTime`".to_string())
    })?;
    let open = parse_f64_lossy(&row[1])
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit kline missing `open`".to_string()))?;
    let high = parse_f64_lossy(&row[2])
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit kline missing `high`".to_string()))?;
    let low = parse_f64_lossy(&row[3])
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit kline missing `low`".to_string()))?;
    let close = parse_f64_lossy(&row[4])
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit kline missing `close`".to_string()))?;
    let volume = parse_f64_lossy(&row[5])
        .ok_or_else(|| ExchangeError::UpstreamData("Bybit kline missing `volume`".to_string()))?;

    Ok((timestamp, open, high, low, close, volume))
}

fn map_order_book_levels(
    levels: Option<&Value>,
    side: &str,
) -> Result<Vec<(f64, f64)>, ExchangeError> {
    let Some(levels) = levels.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    let mut output = Vec::with_capacity(levels.len());
    for level in levels {
        let Some(row) = level.as_array() else {
            return Err(ExchangeError::UpstreamData(format!(
                "Bybit {side} level is not an array"
            )));
        };
        if row.len() < 2 {
            continue;
        }

        let price = parse_f64_lossy(&row[0]).ok_or_else(|| {
            ExchangeError::UpstreamData(format!("failed to parse Bybit {side} price"))
        })?;
        let amount = parse_f64_lossy(&row[1]).ok_or_else(|| {
            ExchangeError::UpstreamData(format!("failed to parse Bybit {side} amount"))
        })?;

        output.push((price, amount));
    }

    Ok(output)
}

fn parse_u64_lossy(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_f64_lossy(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn stringify_json_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn extract_u64_field(params: &Value, field: &str) -> Option<u64> {
    params.get(field).and_then(parse_u64_lossy)
}

fn map_timeframe_to_bybit_interval(timeframe: &str) -> Option<&'static str> {
    match timeframe {
        "1m" => Some("1"),
        "3m" => Some("3"),
        "5m" => Some("5"),
        "15m" => Some("15"),
        "30m" => Some("30"),
        "1h" => Some("60"),
        "2h" => Some("120"),
        "4h" => Some("240"),
        "6h" => Some("360"),
        "12h" => Some("720"),
        "1d" => Some("D"),
        "1w" => Some("W"),
        "1M" => Some("M"),
        _ => None,
    }
}

fn normalize_market_symbol(symbol: &str) -> Result<String, ExchangeError> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeError::BadSymbol(
            "symbol cannot be empty".to_string(),
        ));
    }

    let core = trimmed.split(':').next().unwrap_or_default().trim();
    if core.is_empty() {
        return Err(ExchangeError::BadSymbol(format!(
            "invalid Bybit symbol `{symbol}`"
        )));
    }

    if core.contains('/') {
        let mut parts = core.split('/');
        let base = sanitize_asset(parts.next().unwrap_or_default())?;
        let quote = sanitize_asset(parts.next().unwrap_or_default())?;

        if parts.next().is_some() {
            return Err(ExchangeError::BadSymbol(format!(
                "invalid Bybit symbol `{symbol}`"
            )));
        }

        return Ok(format!("{base}{quote}"));
    }

    let collapsed = core
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();

    if collapsed.len() < 6 {
        return Err(ExchangeError::BadSymbol(format!(
            "invalid Bybit symbol `{symbol}`"
        )));
    }

    Ok(collapsed)
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
            "symbol must contain base and quote assets".to_string(),
        ));
    }

    Ok(normalized)
}

fn resolve_public_symbol(original_symbol: &str, market_symbol: &str) -> String {
    let trimmed = original_symbol.trim();
    if trimmed.contains('/') {
        return trimmed.to_string();
    }

    if let Some((base, quote)) = split_market_symbol(market_symbol) {
        return format!("{base}/{quote}:{quote}");
    }

    trimmed.to_string()
}

fn split_market_symbol(market_symbol: &str) -> Option<(String, String)> {
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"] {
        if market_symbol.len() <= quote.len() || !market_symbol.ends_with(quote) {
            continue;
        }

        let base = market_symbol[..market_symbol.len() - quote.len()].to_string();
        if !base.is_empty() {
            return Some((base, quote.to_string()));
        }
    }

    None
}

fn map_bybit_api_error(code: i32, message: &str) -> ExchangeError {
    if is_bad_symbol_error(code, message) {
        return ExchangeError::BadSymbol(format!(
            "bybit rejected request with code={code}: {message}"
        ));
    }

    ExchangeError::UpstreamRequest(format!(
        "bybit rejected request with code={code}: {message}"
    ))
}

fn is_bad_symbol_error(code: i32, message: &str) -> bool {
    if matches!(code, 10001 | 10021 | 10022 | 100401) {
        return true;
    }

    let normalized_message = message.to_ascii_lowercase();
    normalized_message.contains("symbol") || normalized_message.contains("category")
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    value.chars().take(max_chars).collect::<String>() + "..."
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        map_bybit_api_error, map_kline_row, map_order_book_levels, map_timeframe_to_bybit_interval,
        map_trade_row, max_order_book_limit_for_category, normalize_market_symbol,
        parse_category_from_params, resolve_public_symbol, BybitCategory,
    };
    use crate::exchanges::traits::ExchangeError;

    #[test]
    fn normalizes_ccxt_and_exchange_symbols() {
        assert_eq!(
            normalize_market_symbol("BTC/USDT:USDT").expect("symbol should normalize"),
            "BTCUSDT"
        );
        assert_eq!(
            normalize_market_symbol("btcusdt").expect("symbol should normalize"),
            "BTCUSDT"
        );
        assert!(normalize_market_symbol("BTC").is_err());
    }

    #[test]
    fn resolves_public_symbol_for_raw_bybit_input() {
        assert_eq!(resolve_public_symbol("BTCUSDT", "BTCUSDT"), "BTC/USDT:USDT");
        assert_eq!(
            resolve_public_symbol("BTC/USDT:USDT", "BTCUSDT"),
            "BTC/USDT:USDT"
        );
    }

    #[test]
    fn parses_category_from_params_with_default_linear() {
        assert_eq!(
            parse_category_from_params(&json!({})).unwrap(),
            BybitCategory::Linear
        );
        assert_eq!(
            parse_category_from_params(&json!({"category": "spot"})).unwrap(),
            BybitCategory::Spot
        );
        assert_eq!(
            parse_category_from_params(&json!({"category": "options"})).unwrap(),
            BybitCategory::Option
        );
        assert!(parse_category_from_params(&json!({"category": "invalid"})).is_err());
    }

    #[test]
    fn maps_bybit_trade_to_ccxt_shape() {
        let raw = json!({
            "time": "1700000000000",
            "symbol": "BTCUSDT",
            "price": "100.25",
            "size": "0.5",
            "side": "Buy",
            "execId": "abc123",
            "isBlockTrade": false,
            "isRPITrade": false,
        });

        let mapped = map_trade_row(&raw, "BTC/USDT:USDT").expect("trade should map");
        assert_eq!(mapped.id.as_deref(), Some("abc123"));
        assert_eq!(mapped.side.as_deref(), Some("buy"));
        assert_eq!(mapped.symbol.as_deref(), Some("BTC/USDT:USDT"));
        assert_eq!(mapped.price, Some(100.25));
        assert_eq!(mapped.amount, Some(0.5));
        assert_eq!(mapped.cost, Some(50.125));
        assert_eq!(mapped.timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn maps_bybit_kline_row_to_ccxt_ohlcv_tuple() {
        let raw = json!([
            "1700000060000",
            "100.0",
            "101.5",
            "99.8",
            "101.2",
            "45.678",
            "1234.56"
        ]);
        let mapped = map_kline_row(&raw).expect("kline should map");
        assert_eq!(
            mapped,
            (1_700_000_060_000, 100.0, 101.5, 99.8, 101.2, 45.678)
        );
    }

    #[test]
    fn parses_supported_bybit_kline_intervals() {
        assert_eq!(map_timeframe_to_bybit_interval("1m"), Some("1"));
        assert_eq!(map_timeframe_to_bybit_interval("1h"), Some("60"));
        assert_eq!(map_timeframe_to_bybit_interval("1d"), Some("D"));
        assert_eq!(map_timeframe_to_bybit_interval("1w"), Some("W"));
        assert_eq!(map_timeframe_to_bybit_interval("1M"), Some("M"));
        assert_eq!(map_timeframe_to_bybit_interval("3d"), None);
    }

    #[test]
    fn maps_bybit_order_book_levels_to_price_size_tuples() {
        let raw = json!([["101.0", "4.5"], ["100.5", "1.25"]]);
        let levels = map_order_book_levels(Some(&raw), "bids").expect("levels should map");
        assert_eq!(levels, vec![(101.0, 4.5), (100.5, 1.25)]);
    }

    #[test]
    fn maps_bybit_order_book_limit_by_category() {
        assert_eq!(max_order_book_limit_for_category(BybitCategory::Spot), 200);
        assert_eq!(
            max_order_book_limit_for_category(BybitCategory::Linear),
            500
        );
        assert_eq!(
            max_order_book_limit_for_category(BybitCategory::Inverse),
            500
        );
        assert_eq!(max_order_book_limit_for_category(BybitCategory::Option), 25);
    }

    #[test]
    fn maps_bybit_symbol_errors_to_bad_symbol() {
        let mapped = map_bybit_api_error(10021, "Futures/Options symbol does not exist.");
        assert!(matches!(mapped, ExchangeError::BadSymbol(_)));
    }
}
