use async_trait::async_trait;
use binance_sdk::{
    common::{config::ConfigurationRestApi, errors::ConnectorError},
    derivatives_trading_usds_futures::{
        rest_api::{
            KlineCandlestickDataIntervalEnum, KlineCandlestickDataParams,
            KlineCandlestickDataResponseItemInner, OrderBookParams, RecentTradesListParams,
            RecentTradesListResponseInner,
        },
        DerivativesTradingUsdsFuturesRestApi,
    },
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
const DEFAULT_FETCH_OHLCV_LIMIT: usize = 200;
const MAX_FETCH_OHLCV_LIMIT: usize = 1_000;
const DEFAULT_FETCH_ORDER_BOOK_LEVELS: usize = 100;
const MAX_FETCH_ORDER_BOOK_LEVELS: usize = 1_000;

type BinanceRestApiClient = binance_sdk::derivatives_trading_usds_futures::rest_api::RestApi;

pub struct BinanceExchange {
    client: BinanceRestApiClient,
}

impl BinanceExchange {
    pub fn new(timeout_ms: u64) -> Result<Self, ExchangeError> {
        let configuration = ConfigurationRestApi::builder()
            .timeout(timeout_ms)
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build Binance REST client: {err}"))
            })?;

        let client = DerivativesTradingUsdsFuturesRestApi::production(configuration);
        Ok(Self { client })
    }
}

#[async_trait]
impl MarketDataExchange for BinanceExchange {
    fn id(&self) -> &'static str {
        "binance"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let resolved_symbol = resolve_public_symbol(&params.symbol, &market_symbol);

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_TRADES_LIMIT)
            .clamp(1, MAX_FETCH_TRADES_LIMIT);

        let request = RecentTradesListParams::builder(market_symbol)
            .limit(requested_limit as i64)
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build Binance trades request: {err}"))
            })?;

        let response = self
            .client
            .recent_trades_list(request)
            .await
            .map_err(map_anyhow_error)?;
        let raw_rows = response.data().await.map_err(map_connector_error)?;

        let mut mapped = Vec::with_capacity(raw_rows.len());
        for row in raw_rows {
            match map_trade(row, &resolved_symbol) {
                Ok(trade) => mapped.push(trade),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map Binance trade, skipping");
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
        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let timeframe = params
            .timeframe
            .unwrap_or_else(|| "1m".to_string())
            .trim()
            .to_string();
        let interval = parse_kline_interval(&timeframe).ok_or_else(|| {
            ExchangeError::BadSymbol(format!("unsupported timeframe `{timeframe}`"))
        })?;

        let requested_limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_OHLCV_LIMIT)
            .clamp(1, MAX_FETCH_OHLCV_LIMIT);

        let mut builder = KlineCandlestickDataParams::builder(market_symbol, interval)
            .limit(requested_limit as i64);

        if let Some(since) = params.since {
            builder = builder.start_time(u64_to_i64(since, "since")?);
        }
        if let Some(until) = extract_u64_field(&params.params, "until")
            .or_else(|| extract_u64_field(&params.params, "endTime"))
        {
            builder = builder.end_time(u64_to_i64(until, "until")?);
        }

        let request = builder.build().map_err(|err| {
            ExchangeError::Internal(format!("failed to build Binance kline request: {err}"))
        })?;

        let response = self
            .client
            .kline_candlestick_data(request)
            .await
            .map_err(map_anyhow_error)?;
        let rows = response.data().await.map_err(map_connector_error)?;

        let mut mapped = Vec::with_capacity(rows.len());
        for row in rows {
            match map_ohlcv_row(row) {
                Ok(candle) => mapped.push(candle),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map Binance OHLCV row, skipping");
                }
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
        let market_symbol = normalize_market_symbol(&params.symbol)?;
        let resolved_symbol = resolve_public_symbol(&params.symbol, &market_symbol);

        let requested_levels = params
            .limit
            .unwrap_or(DEFAULT_FETCH_ORDER_BOOK_LEVELS)
            .clamp(1, MAX_FETCH_ORDER_BOOK_LEVELS);
        let upstream_levels = to_binance_depth_limit(requested_levels);

        let request = OrderBookParams::builder(market_symbol)
            .limit(upstream_levels as i64)
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!(
                    "failed to build Binance order book request: {err}"
                ))
            })?;

        let response = self
            .client
            .order_book(request)
            .await
            .map_err(map_anyhow_error)?;
        let raw = response.data().await.map_err(map_connector_error)?;

        let mut bids = map_order_book_levels(raw.bids.as_ref(), "bids")?;
        let mut asks = map_order_book_levels(raw.asks.as_ref(), "asks")?;

        bids.sort_by(|left, right| right.0.total_cmp(&left.0));
        asks.sort_by(|left, right| left.0.total_cmp(&right.0));

        if bids.len() > requested_levels {
            bids.truncate(requested_levels);
        }
        if asks.len() > requested_levels {
            asks.truncate(requested_levels);
        }

        let timestamp = raw
            .t_uppercase
            .or(raw.e_uppercase)
            .and_then(|value| (value >= 0).then_some(value as u64));
        let datetime = timestamp.and_then(iso8601_millis);
        let nonce = raw
            .last_update_id
            .and_then(|value| (value >= 0).then_some(value as u64));

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

fn parse_kline_interval(timeframe: &str) -> Option<KlineCandlestickDataIntervalEnum> {
    match timeframe {
        "1m" => Some(KlineCandlestickDataIntervalEnum::Interval1m),
        "3m" => Some(KlineCandlestickDataIntervalEnum::Interval3m),
        "5m" => Some(KlineCandlestickDataIntervalEnum::Interval5m),
        "15m" => Some(KlineCandlestickDataIntervalEnum::Interval15m),
        "30m" => Some(KlineCandlestickDataIntervalEnum::Interval30m),
        "1h" => Some(KlineCandlestickDataIntervalEnum::Interval1h),
        "2h" => Some(KlineCandlestickDataIntervalEnum::Interval2h),
        "4h" => Some(KlineCandlestickDataIntervalEnum::Interval4h),
        "6h" => Some(KlineCandlestickDataIntervalEnum::Interval6h),
        "8h" => Some(KlineCandlestickDataIntervalEnum::Interval8h),
        "12h" => Some(KlineCandlestickDataIntervalEnum::Interval12h),
        "1d" => Some(KlineCandlestickDataIntervalEnum::Interval1d),
        "3d" => Some(KlineCandlestickDataIntervalEnum::Interval3d),
        "1w" => Some(KlineCandlestickDataIntervalEnum::Interval1w),
        "1M" => Some(KlineCandlestickDataIntervalEnum::Interval1M),
        _ => None,
    }
}

fn to_binance_depth_limit(requested: usize) -> usize {
    if requested <= 5 {
        5
    } else if requested <= 10 {
        10
    } else if requested <= 20 {
        20
    } else if requested <= 50 {
        50
    } else if requested <= 100 {
        100
    } else if requested <= 500 {
        500
    } else {
        1_000
    }
}

fn map_ohlcv_row(
    row: Vec<KlineCandlestickDataResponseItemInner>,
) -> Result<CcxtOhlcv, ExchangeError> {
    if row.len() < 6 {
        return Err(ExchangeError::UpstreamData(format!(
            "kline row expected at least 6 fields, got {}",
            row.len()
        )));
    }

    let timestamp = parse_kline_item_u64(&row[0], "open_time")?;
    let open = parse_kline_item_f64(&row[1], "open")?;
    let high = parse_kline_item_f64(&row[2], "high")?;
    let low = parse_kline_item_f64(&row[3], "low")?;
    let close = parse_kline_item_f64(&row[4], "close")?;
    let volume = parse_kline_item_f64(&row[5], "volume")?;

    Ok((timestamp, open, high, low, close, volume))
}

fn parse_kline_item_u64(
    item: &KlineCandlestickDataResponseItemInner,
    field: &str,
) -> Result<u64, ExchangeError> {
    match item {
        KlineCandlestickDataResponseItemInner::Integer(value) => {
            if *value >= 0 {
                Ok(*value as u64)
            } else {
                Err(ExchangeError::UpstreamData(format!(
                    "kline `{field}` is negative: {value}"
                )))
            }
        }
        KlineCandlestickDataResponseItemInner::String(value) => {
            value.parse::<u64>().map_err(|err| {
                ExchangeError::UpstreamData(format!(
                    "failed to parse kline `{field}` value `{value}` as u64: {err}"
                ))
            })
        }
        KlineCandlestickDataResponseItemInner::Other(value) => value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| (v >= 0).then_some(v as u64)))
            .or_else(|| value.as_str().and_then(|v| v.parse::<u64>().ok()))
            .ok_or_else(|| {
                ExchangeError::UpstreamData(format!(
                    "kline `{field}` has unsupported value type: {value}"
                ))
            }),
    }
}

fn parse_kline_item_f64(
    item: &KlineCandlestickDataResponseItemInner,
    field: &str,
) -> Result<f64, ExchangeError> {
    match item {
        KlineCandlestickDataResponseItemInner::Integer(value) => Ok(*value as f64),
        KlineCandlestickDataResponseItemInner::String(value) => {
            value.parse::<f64>().map_err(|err| {
                ExchangeError::UpstreamData(format!(
                    "failed to parse kline `{field}` value `{value}` as f64: {err}"
                ))
            })
        }
        KlineCandlestickDataResponseItemInner::Other(value) => value
            .as_f64()
            .or_else(|| value.as_i64().map(|v| v as f64))
            .or_else(|| value.as_u64().map(|v| v as f64))
            .or_else(|| value.as_str().and_then(|v| v.parse::<f64>().ok()))
            .ok_or_else(|| {
                ExchangeError::UpstreamData(format!(
                    "kline `{field}` has unsupported value type: {value}"
                ))
            }),
    }
}

fn map_order_book_levels(
    levels: Option<&Vec<Vec<String>>>,
    side: &str,
) -> Result<Vec<(f64, f64)>, ExchangeError> {
    let Some(levels) = levels else {
        return Ok(Vec::new());
    };

    let mut output = Vec::with_capacity(levels.len());
    for level in levels {
        if level.len() < 2 {
            continue;
        }

        let price = level[0].parse::<f64>().map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse {side} price `{}` as f64: {err}",
                level[0]
            ))
        })?;
        let amount = level[1].parse::<f64>().map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse {side} amount `{}` as f64: {err}",
                level[1]
            ))
        })?;

        output.push((price, amount));
    }

    Ok(output)
}

fn extract_u64_field(params: &Value, field: &str) -> Option<u64> {
    params.get(field).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| (v >= 0).then_some(v as u64)))
            .or_else(|| value.as_str().and_then(|v| v.parse::<u64>().ok()))
    })
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, ExchangeError> {
    i64::try_from(value)
        .map_err(|_| ExchangeError::BadSymbol(format!("`{field}` is too large: {value}")))
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
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
            "invalid Binance symbol `{symbol}`"
        )));
    }

    if core.contains('/') {
        let mut parts = core.split('/');
        let base = sanitize_asset(parts.next().unwrap_or_default())?;
        let quote = sanitize_asset(parts.next().unwrap_or_default())?;

        if parts.next().is_some() {
            return Err(ExchangeError::BadSymbol(format!(
                "invalid Binance symbol `{symbol}`"
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
            "invalid Binance symbol `{symbol}`"
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
    for quote in ["USDT", "USDC", "BUSD", "FDUSD", "USDS"] {
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

fn map_trade(raw: RecentTradesListResponseInner, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = raw
        .time
        .and_then(|value| (value >= 0).then_some(value as u64))
        .ok_or_else(|| ExchangeError::UpstreamData("trade is missing `time`".to_string()))?;

    let price = parse_f64(raw.price.as_deref(), "price")?;
    let amount = parse_f64(raw.qty.as_deref(), "qty")?;
    let quote_qty = parse_optional_f64(raw.quote_qty.as_deref(), "quoteQty")?;

    let datetime = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid timestamp `{timestamp}`")))?
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    let side = raw.is_buyer_maker.map(|is_buyer_maker| {
        if is_buyer_maker {
            "sell".to_string()
        } else {
            "buy".to_string()
        }
    });

    let id = raw.id.map(|value| value.to_string());
    let info = serde_json::to_value(&raw).map_err(|err| {
        ExchangeError::UpstreamData(format!("failed to serialize Binance trade info: {err}"))
    })?;

    Ok(CcxtTrade {
        info,
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
        cost: Some(quote_qty.unwrap_or(price * amount)),
        fee: None,
    })
}

fn parse_optional_f64(value: Option<&str>, field: &str) -> Result<Option<f64>, ExchangeError> {
    match value {
        Some(raw) => Ok(Some(raw.parse::<f64>().map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse `{field}` value `{raw}` as number: {err}"
            ))
        })?)),
        None => Ok(None),
    }
}

fn parse_f64(value: Option<&str>, field: &str) -> Result<f64, ExchangeError> {
    let raw = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ExchangeError::UpstreamData(format!("trade is missing `{field}`")))?;

    raw.parse::<f64>().map_err(|err| {
        ExchangeError::UpstreamData(format!(
            "failed to parse `{field}` value `{raw}` as number: {err}"
        ))
    })
}

fn map_connector_error(error: ConnectorError) -> ExchangeError {
    match error {
        ConnectorError::BadRequestError { msg, .. } | ConnectorError::NotFoundError { msg, .. } => {
            ExchangeError::BadSymbol(msg)
        }
        ConnectorError::ConnectorClientError { msg, .. }
            if msg.to_ascii_lowercase().contains("symbol") =>
        {
            ExchangeError::BadSymbol(msg)
        }
        other => ExchangeError::UpstreamRequest(other.to_string()),
    }
}

fn map_anyhow_error(error: anyhow::Error) -> ExchangeError {
    match error.downcast::<ConnectorError>() {
        Ok(connector_error) => map_connector_error(connector_error),
        Err(other) => ExchangeError::UpstreamRequest(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        map_ohlcv_row, map_order_book_levels, map_trade, normalize_market_symbol,
        parse_kline_interval, resolve_public_symbol, to_binance_depth_limit,
    };
    use binance_sdk::derivatives_trading_usds_futures::rest_api::{
        KlineCandlestickDataResponseItemInner, RecentTradesListResponseInner,
    };

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
    fn resolves_public_symbol_for_raw_binance_input() {
        assert_eq!(resolve_public_symbol("BTCUSDT", "BTCUSDT"), "BTC/USDT:USDT");
        assert_eq!(
            resolve_public_symbol("BTC/USDT:USDT", "BTCUSDT"),
            "BTC/USDT:USDT"
        );
    }

    #[test]
    fn maps_binance_trade_to_ccxt_shape() {
        let trade = RecentTradesListResponseInner {
            id: Some(28457),
            price: Some("4.00000100".to_string()),
            qty: Some("12.00000000".to_string()),
            quote_qty: Some("48.00".to_string()),
            time: Some(1499865549590),
            is_buyer_maker: Some(true),
            is_rpi_trade: Some(false),
        };

        let mapped = map_trade(trade, "BTC/USDT:USDT").expect("trade should map");
        assert_eq!(mapped.id.as_deref(), Some("28457"));
        assert_eq!(mapped.side.as_deref(), Some("sell"));
        assert_eq!(mapped.symbol.as_deref(), Some("BTC/USDT:USDT"));
        assert_eq!(mapped.price, Some(4.000001));
        assert_eq!(mapped.amount, Some(12.0));
        assert_eq!(mapped.cost, Some(48.0));
        assert_eq!(mapped.timestamp, Some(1499865549590));
    }

    #[test]
    fn maps_kline_row_to_ccxt_ohlcv_tuple() {
        let row = vec![
            KlineCandlestickDataResponseItemInner::Integer(1700000000000),
            KlineCandlestickDataResponseItemInner::String("43000.1".to_string()),
            KlineCandlestickDataResponseItemInner::String("43100.0".to_string()),
            KlineCandlestickDataResponseItemInner::String("42900.5".to_string()),
            KlineCandlestickDataResponseItemInner::String("43050.2".to_string()),
            KlineCandlestickDataResponseItemInner::String("12.345".to_string()),
        ];

        let candle = map_ohlcv_row(row).expect("kline row should map");
        assert_eq!(candle.0, 1700000000000);
        assert_eq!(candle.1, 43000.1);
        assert_eq!(candle.2, 43100.0);
        assert_eq!(candle.3, 42900.5);
        assert_eq!(candle.4, 43050.2);
        assert_eq!(candle.5, 12.345);
    }

    #[test]
    fn maps_order_book_levels_to_price_size_tuples() {
        let raw = vec![
            vec!["101.25".to_string(), "2.5".to_string()],
            vec!["101.30".to_string(), "1.2".to_string()],
        ];

        let mapped = map_order_book_levels(Some(&raw), "asks").expect("levels should parse");
        assert_eq!(mapped, vec![(101.25, 2.5), (101.30, 1.2)]);
    }

    #[test]
    fn parses_supported_kline_intervals() {
        assert!(parse_kline_interval("1m").is_some());
        assert!(parse_kline_interval("1h").is_some());
        assert!(parse_kline_interval("1M").is_some());
        assert!(parse_kline_interval("2m").is_none());
    }

    #[test]
    fn maps_depth_limit_to_supported_binance_values() {
        assert_eq!(to_binance_depth_limit(1), 5);
        assert_eq!(to_binance_depth_limit(7), 10);
        assert_eq!(to_binance_depth_limit(12), 20);
        assert_eq!(to_binance_depth_limit(22), 50);
        assert_eq!(to_binance_depth_limit(91), 100);
        assert_eq!(to_binance_depth_limit(300), 500);
        assert_eq!(to_binance_depth_limit(999), 1000);
    }
}
