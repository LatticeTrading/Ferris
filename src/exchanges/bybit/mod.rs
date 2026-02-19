use async_trait::async_trait;
use bybit::prelude::{
    Bybit, BybitError, Category, GeneralCode, MarketData, RecentTrade, RecentTradesRequest,
    ReturnCode, Side,
};
use chrono::{SecondsFormat, Utc};
use serde_json::Value;

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams,
    },
};

const DEFAULT_FETCH_TRADES_LIMIT: usize = 100;
const MAX_FETCH_TRADES_LIMIT: usize = 1_000;
const MAX_FETCH_TRADES_LIMIT_SPOT: usize = 60;

pub struct BybitExchange {
    client: MarketData,
}

impl BybitExchange {
    pub fn new(_timeout_ms: u64) -> Result<Self, ExchangeError> {
        let client: MarketData = Bybit::new(None, None);
        Ok(Self { client })
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

        let request = RecentTradesRequest::new(
            category,
            Some(market_symbol.as_str()),
            None,
            Some(requested_limit as u64),
        );

        let response = self
            .client
            .get_recent_trades(request)
            .await
            .map_err(map_bybit_error)?;

        let mut mapped = Vec::with_capacity(response.result.list.len());
        for row in response.result.list {
            match map_trade(row, &resolved_symbol) {
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

fn max_trade_limit_for_category(category: Category) -> usize {
    match category {
        Category::Spot => MAX_FETCH_TRADES_LIMIT_SPOT,
        _ => MAX_FETCH_TRADES_LIMIT,
    }
}

fn parse_category_from_params(params: &Value) -> Result<Category, ExchangeError> {
    let Some(raw) = params.get("category").and_then(Value::as_str) else {
        return Ok(Category::Linear);
    };

    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(Category::Linear);
    }

    match normalized.as_str() {
        "spot" => Ok(Category::Spot),
        "linear" => Ok(Category::Linear),
        "inverse" => Ok(Category::Inverse),
        "option" => Ok(Category::Option),
        other => Err(ExchangeError::BadSymbol(format!(
            "unsupported Bybit category `{other}`"
        ))),
    }
}

fn map_trade(raw: RecentTrade, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = raw.time;
    let datetime = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid timestamp `{timestamp}`")))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    let side = match raw.side {
        Side::Buy => "buy".to_string(),
        Side::Sell => "sell".to_string(),
    };

    let price = raw.price;
    let amount = raw.size;
    let cost = price * amount;
    let id = raw.exec_id.clone();

    let info = serde_json::to_value(&raw).map_err(|err| {
        ExchangeError::UpstreamData(format!("failed to serialize Bybit trade info: {err}"))
    })?;

    Ok(CcxtTrade {
        info,
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
        cost: Some(cost),
        fee: None,
    })
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

fn map_bybit_error(error: BybitError) -> ExchangeError {
    match error {
        BybitError::BybitError(content) => {
            let code = content.code;
            let message = content.msg;

            if is_bad_symbol_error(code, &message) {
                return ExchangeError::BadSymbol(format!(
                    "bybit rejected request with code={code}: {message}"
                ));
            }

            ExchangeError::UpstreamRequest(format!(
                "bybit rejected request with code={code}: {message}"
            ))
        }
        other => ExchangeError::UpstreamRequest(other.to_string()),
    }
}

fn is_bad_symbol_error(code: i32, message: &str) -> bool {
    if matches!(
        ReturnCode::from_code(code),
        Some(ReturnCode::General(
            GeneralCode::InvalidParameters
                | GeneralCode::SymbolNotExist
                | GeneralCode::SymbolNotTrading
                | GeneralCode::InvalidCategory
        ))
    ) {
        return true;
    }

    let normalized_message = message.to_ascii_lowercase();
    normalized_message.contains("symbol") || normalized_message.contains("category")
}

#[cfg(test)]
mod tests {
    use bybit::prelude::BybitContentError;
    use serde_json::json;

    use super::{
        map_bybit_error, map_trade, normalize_market_symbol, parse_category_from_params,
        resolve_public_symbol,
    };
    use crate::exchanges::traits::ExchangeError;
    use bybit::prelude::{BybitError, Category, RecentTrade, Side};

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
            Category::Linear
        );
        assert_eq!(
            parse_category_from_params(&json!({"category": "spot"})).unwrap(),
            Category::Spot
        );
        assert!(parse_category_from_params(&json!({"category": "invalid"})).is_err());
    }

    #[test]
    fn maps_bybit_trade_to_ccxt_shape() {
        let trade = RecentTrade {
            time: 1_700_000_000_000,
            symbol: "BTCUSDT".to_string(),
            price: 100.25,
            size: 0.5,
            side: Side::Buy,
            exec_id: "abc123".to_string(),
            is_block_trade: false,
            is_rpi_trade: false,
        };

        let mapped = map_trade(trade, "BTC/USDT:USDT").expect("trade should map");
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
        let error = BybitError::BybitError(BybitContentError {
            code: 10021,
            msg: "Futures/Options symbol does not exist.".to_string(),
        });

        let mapped = map_bybit_error(error);
        assert!(matches!(mapped, ExchangeError::BadSymbol(_)));
    }
}
