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

        let endpoint = format!("{}/v5/market/recent-trade", self.base_url);
        let limit = requested_limit.to_string();
        let query = [
            ("category", category.as_str()),
            ("symbol", market_symbol.as_str()),
            ("limit", limit.as_str()),
        ];

        let response = self
            .http_client
            .get(endpoint)
            .query(&query)
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

        let envelope: BybitRecentTradesEnvelope = serde_json::from_str(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse Bybit recent trades response: {err}; body={}",
                truncate(&body, 240)
            ))
        })?;

        if envelope.ret_code != 0 {
            return Err(map_bybit_api_error(envelope.ret_code, &envelope.ret_msg));
        }

        let result = envelope.result.ok_or_else(|| {
            ExchangeError::UpstreamData("Bybit recent trades response missing `result`".to_string())
        })?;

        let mut mapped = Vec::with_capacity(result.list.len());
        for row in result.list {
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

    async fn fetch_ohlcv(
        &self,
        _params: FetchOhlcvParams,
    ) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        Err(ExchangeError::Internal(
            "bybit fetchOHLCV is not implemented yet".to_string(),
        ))
    }

    async fn fetch_order_book(
        &self,
        _params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        Err(ExchangeError::Internal(
            "bybit fetchOrderBook is not implemented yet".to_string(),
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitRecentTradesEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<BybitRecentTradesResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitRecentTradesResult {
    list: Vec<Value>,
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
        "option" => Ok(BybitCategory::Option),
        other => Err(ExchangeError::BadSymbol(format!(
            "unsupported Bybit category `{other}`"
        ))),
    }
}

fn map_trade_row(raw: Value, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
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
        info: raw,
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
        map_bybit_api_error, map_trade_row, normalize_market_symbol, parse_category_from_params,
        resolve_public_symbol, BybitCategory,
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

        let mapped = map_trade_row(raw, "BTC/USDT:USDT").expect("trade should map");
        assert_eq!(mapped.id.as_deref(), Some("abc123"));
        assert_eq!(mapped.side.as_deref(), Some("buy"));
        assert_eq!(mapped.symbol.as_deref(), Some("BTC/USDT:USDT"));
        assert_eq!(mapped.price, Some(100.25));
        assert_eq!(mapped.amount, Some(0.5));
        assert_eq!(mapped.cost, Some(50.125));
        assert_eq!(mapped.timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn maps_bybit_symbol_errors_to_bad_symbol() {
        let mapped = map_bybit_api_error(10021, "Futures/Options symbol does not exist.");
        assert!(matches!(mapped, ExchangeError::BadSymbol(_)));
    }
}
