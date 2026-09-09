use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, TimeZone, Utc};
use reqwest::StatusCode;
use serde_json::Value;

use crate::{
    binance_orderbook::{BinanceDepthLevel, BinanceDepthSnapshot, OrderBookSnapshotProvider},
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchMarketsParams, FetchOhlcvParams,
        FetchOrderBookParams, FetchTradesParams, UnifiedMarket, UnifiedMarketInfo,
        UnifiedMarketType,
    },
    ws_shared::{resolve_aster_ws_symbol, resolve_public_symbol as public_symbol, ASTER_QUOTES},
};

const ASTER_REST_BASE_URL: &str = "https://fapi.asterdex.com";
const DEFAULT_FETCH_TRADES_LIMIT: usize = 100;
const DEFAULT_FETCH_OHLCV_LIMIT: usize = 200;
const DEFAULT_FETCH_ORDER_BOOK_LEVELS: usize = 100;
const MAX_FETCH_TRADES_LIMIT: usize = 1_000;
const MAX_FETCH_OHLCV_LIMIT: usize = 1_500;
const MAX_FETCH_ORDER_BOOK_LEVELS: usize = 1_000;

pub struct AsterExchange {
    http_client: reqwest::Client,
}

impl AsterExchange {
    pub fn new(timeout_ms: u64) -> Result<Self, ExchangeError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build Aster REST client: {err}"))
            })?;
        Ok(Self { http_client })
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, ExchangeError> {
        let url = format!("{ASTER_REST_BASE_URL}{path}");
        let response = self
            .http_client
            .get(url)
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
                "Aster status={status} body={} query={query:?}",
                truncate(&body, 240)
            )));
        }
        serde_json::from_str(&body).map_err(|err| {
            ExchangeError::UpstreamData(format!(
                "failed to parse Aster response: {err}; body={}",
                truncate(&body, 240)
            ))
        })
    }

    async fn fetch_depth_snapshot(
        &self,
        symbol: &str,
    ) -> Result<BinanceDepthSnapshot, ExchangeError> {
        let value = self
            .get(
                "/fapi/v3/depth",
                &[
                    ("symbol", symbol.to_string()),
                    ("limit", "1000".to_string()),
                ],
            )
            .await?;
        let last_update_id = value
            .get("lastUpdateId")
            .and_then(parse_u64)
            .ok_or_else(|| {
                ExchangeError::UpstreamData("Aster depth missing lastUpdateId".to_string())
            })?;
        let bids = map_exact_levels(value.get("bids"), "bids")?;
        let asks = map_exact_levels(value.get("asks"), "asks")?;
        let timestamp = value
            .get("T")
            .or_else(|| value.get("E"))
            .and_then(parse_u64);
        Ok(BinanceDepthSnapshot {
            last_update_id,
            bids,
            asks,
            timestamp,
        })
    }
}

#[async_trait]
impl MarketDataExchange for AsterExchange {
    fn id(&self) -> &'static str {
        "aster"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let market_symbol = requested_symbol(&params.symbol, &params.params)?;
        let public_symbol = resolve_public_symbol(&params.symbol, &market_symbol);
        let limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_TRADES_LIMIT)
            .clamp(1, MAX_FETCH_TRADES_LIMIT);
        let value = self
            .get(
                "/fapi/v3/trades",
                &[("symbol", market_symbol), ("limit", limit.to_string())],
            )
            .await?;
        let rows = value.as_array().ok_or_else(|| {
            ExchangeError::UpstreamData("Aster trades response must be an array".to_string())
        })?;
        let mut trades = rows
            .iter()
            .map(|row| map_trade(row, &public_symbol))
            .collect::<Result<Vec<_>, _>>()?;
        trades.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        if let Some(since) = params.since {
            trades.retain(|trade| trade.timestamp.unwrap_or_default() >= since);
        }
        trades.truncate(limit);
        Ok(trades)
    }

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        let market_symbol = requested_symbol(&params.symbol, &params.params)?;
        let timeframe = params.timeframe.as_deref().unwrap_or("1m").trim();
        if !ASTER_INTERVALS.contains(&timeframe) {
            return Err(ExchangeError::BadSymbol(format!(
                "unsupported timeframe `{timeframe}`"
            )));
        }
        let limit = params
            .limit
            .unwrap_or(DEFAULT_FETCH_OHLCV_LIMIT)
            .clamp(1, MAX_FETCH_OHLCV_LIMIT);
        let mut query = vec![
            ("symbol", market_symbol),
            ("interval", timeframe.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(since) = params.since {
            query.push(("startTime", since.to_string()));
        }
        if let Some(until) =
            extract_u64(&params.params, "until").or_else(|| extract_u64(&params.params, "endTime"))
        {
            query.push(("endTime", until.to_string()));
        }
        let value = self.get("/fapi/v3/klines", &query).await?;
        let rows = value.as_array().ok_or_else(|| {
            ExchangeError::UpstreamData("Aster klines response must be an array".to_string())
        })?;
        let mut candles = rows.iter().map(map_ohlcv).collect::<Result<Vec<_>, _>>()?;
        candles.sort_by_key(|candle| candle.0);
        if let Some(since) = params.since {
            candles.retain(|candle| candle.0 >= since);
        }
        if candles.len() > limit {
            if params.since.is_some() {
                candles.truncate(limit);
            } else {
                candles.drain(..candles.len() - limit);
            }
        }
        Ok(candles)
    }

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        let market_symbol = requested_symbol(&params.symbol, &params.params)?;
        let public_symbol = resolve_public_symbol(&params.symbol, &market_symbol);
        let levels = params
            .limit
            .unwrap_or(DEFAULT_FETCH_ORDER_BOOK_LEVELS)
            .clamp(1, MAX_FETCH_ORDER_BOOK_LEVELS);
        let upstream = to_aster_depth_limit(levels);
        let value = self
            .get(
                "/fapi/v3/depth",
                &[("symbol", market_symbol), ("limit", upstream.to_string())],
            )
            .await?;
        map_order_book(&value, &public_symbol, levels)
    }

    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        let value = self.get("/fapi/v3/exchangeInfo", &[]).await?;
        let rows = value
            .get("symbols")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ExchangeError::UpstreamData("Aster exchangeInfo missing symbols".to_string())
            })?;
        Ok(rows
            .iter()
            .filter_map(|row| map_market(row, params.include_inactive))
            .collect())
    }
}

#[async_trait]
impl OrderBookSnapshotProvider for AsterExchange {
    async fn fetch_order_book_snapshot(
        &self,
        market_symbol: &str,
    ) -> Result<BinanceDepthSnapshot, String> {
        let symbol = normalize_market_symbol(market_symbol).map_err(|err| err.to_string())?;
        self.fetch_depth_snapshot(&symbol)
            .await
            .map_err(|err| err.to_string())
    }
}

const ASTER_INTERVALS: [&str; 15] = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M",
];

fn requested_symbol(symbol: &str, params: &Value) -> Result<String, ExchangeError> {
    let coin = params
        .get("coin")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    resolve_aster_ws_symbol(symbol, coin).map_err(ExchangeError::BadSymbol)
}

fn normalize_market_symbol(symbol: &str) -> Result<String, ExchangeError> {
    resolve_aster_ws_symbol(symbol, None).map_err(ExchangeError::BadSymbol)
}

fn sanitize_asset(value: &str) -> Result<String, ExchangeError> {
    let value = value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    if value.is_empty() {
        Err(ExchangeError::BadSymbol(
            "symbol must contain base and quote assets".to_string(),
        ))
    } else {
        Ok(value)
    }
}

fn resolve_public_symbol(original: &str, market: &str) -> String {
    public_symbol(original, market, ASTER_QUOTES)
}

fn map_trade(row: &Value, symbol: &str) -> Result<CcxtTrade, ExchangeError> {
    let timestamp = row
        .get("time")
        .and_then(parse_u64)
        .ok_or_else(|| ExchangeError::UpstreamData("trade missing time".to_string()))?;
    let price = parse_f64(
        row.get("price")
            .ok_or_else(|| ExchangeError::UpstreamData("trade missing price".to_string()))?,
        "price",
    )?;
    let amount = parse_f64(
        row.get("qty")
            .ok_or_else(|| ExchangeError::UpstreamData("trade missing qty".to_string()))?,
        "qty",
    )?;
    let cost = row
        .get("quoteQty")
        .map(|v| parse_f64(v, "quoteQty"))
        .transpose()?
        .unwrap_or(price * amount);
    Ok(CcxtTrade {
        info: row.clone(),
        amount: Some(amount),
        datetime: iso8601_millis(timestamp),
        id: row.get("id").and_then(stringify),
        order: None,
        price: Some(price),
        timestamp: Some(timestamp),
        trade_type: None,
        side: row
            .get("isBuyerMaker")
            .and_then(Value::as_bool)
            .map(|maker| if maker { "sell" } else { "buy" }.to_string()),
        symbol: Some(symbol.to_string()),
        taker_or_maker: None,
        cost: Some(cost),
        fee: None,
    })
}

fn map_ohlcv(row: &Value) -> Result<CcxtOhlcv, ExchangeError> {
    let values = row
        .as_array()
        .ok_or_else(|| ExchangeError::UpstreamData("kline row must be an array".to_string()))?;
    if values.len() < 6 {
        return Err(ExchangeError::UpstreamData(
            "kline row expected at least 6 fields".to_string(),
        ));
    }
    Ok((
        parse_u64(&values[0])
            .ok_or_else(|| ExchangeError::UpstreamData("invalid kline open time".to_string()))?,
        parse_f64(&values[1], "open")?,
        parse_f64(&values[2], "high")?,
        parse_f64(&values[3], "low")?,
        parse_f64(&values[4], "close")?,
        parse_f64(&values[5], "volume")?,
    ))
}

fn map_order_book(
    value: &Value,
    symbol: &str,
    levels: usize,
) -> Result<CcxtOrderBook, ExchangeError> {
    let mut bids = map_levels(value.get("bids"), "bids")?;
    let mut asks = map_levels(value.get("asks"), "asks")?;
    bids.sort_by(|a, b| b.0.total_cmp(&a.0));
    asks.sort_by(|a, b| a.0.total_cmp(&b.0));
    bids.truncate(levels);
    asks.truncate(levels);
    let timestamp = value
        .get("E")
        .or_else(|| value.get("T"))
        .and_then(parse_u64);
    Ok(CcxtOrderBook {
        asks,
        bids,
        datetime: timestamp.and_then(iso8601_millis),
        timestamp,
        nonce: value.get("lastUpdateId").and_then(parse_u64),
        symbol: Some(symbol.to_string()),
    })
}

fn map_levels(value: Option<&Value>, side: &str) -> Result<Vec<(f64, f64)>, ExchangeError> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("Aster {side} must be an array")))?;
    let mut mapped = Vec::with_capacity(levels.len());
    for row in levels {
        let row = row.as_array().filter(|row| row.len() >= 2).ok_or_else(|| {
            ExchangeError::UpstreamData(format!(
                "Aster {side} level must contain price and quantity"
            ))
        })?;
        mapped.push((
            parse_f64(&row[0], &format!("{side} price"))?,
            parse_f64(&row[1], &format!("{side} amount"))?,
        ));
    }
    Ok(mapped)
}

fn map_exact_levels(
    value: Option<&Value>,
    side: &str,
) -> Result<Vec<BinanceDepthLevel>, ExchangeError> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeError::UpstreamData(format!("Aster {side} must be an array")))?;
    rows.iter()
        .map(|row| {
            let row = row.as_array().ok_or_else(|| {
                ExchangeError::UpstreamData(format!("Aster {side} level must be an array"))
            })?;
            if row.len() < 2 {
                return Err(ExchangeError::UpstreamData(format!(
                    "Aster {side} level must contain price and quantity"
                )));
            }
            let price = row[0].as_str().ok_or_else(|| {
                ExchangeError::UpstreamData("depth price must be a string".to_string())
            })?;
            let quantity = row[1].as_str().ok_or_else(|| {
                ExchangeError::UpstreamData("depth quantity must be a string".to_string())
            })?;
            BinanceDepthLevel::parse(price, quantity).map_err(ExchangeError::UpstreamData)
        })
        .collect()
}

fn map_market(row: &Value, include_inactive: bool) -> Option<UnifiedMarket> {
    if row.get("contractType").and_then(Value::as_str) != Some("PERPETUAL") {
        return None;
    }
    let symbol = row.get("symbol").and_then(Value::as_str)?.trim();
    let base = sanitize_asset(row.get("baseAsset").and_then(Value::as_str)?).ok()?;
    let quote = sanitize_asset(row.get("quoteAsset").and_then(Value::as_str)?).ok()?;
    let active = row.get("status").and_then(Value::as_str) == Some("TRADING");
    if !include_inactive && !active {
        return None;
    }
    let min_order_size = filter_number(row, "LOT_SIZE", "minQty");
    let tick_size = filter_number(row, "PRICE_FILTER", "tickSize");
    Some(UnifiedMarket {
        exchange: "aster".to_string(),
        symbol: format!("{base}/{quote}"),
        base,
        quote,
        market_type: UnifiedMarketType::Perp,
        active,
        min_order_size,
        tick_size,
        contract_size: Some(1.0),
        info: UnifiedMarketInfo {
            category: Some("futures".to_string()),
            raw_symbol: Some(symbol.to_string()),
            exchange_symbol: Some(symbol.to_string()),
        },
    })
}

fn filter_number(row: &Value, kind: &str, field: &str) -> Option<f64> {
    row.get("filters")?
        .as_array()?
        .iter()
        .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some(kind))
        .and_then(|filter| filter.get(field))
        .and_then(|value| value.as_str()?.parse().ok())
}

fn parse_f64(value: &Value, field: &str) -> Result<f64, ExchangeError> {
    crate::ws_shared::parse_f64_lossy(value)
        .filter(|number| number.is_finite())
        .ok_or_else(|| ExchangeError::UpstreamData(format!("invalid Aster {field}: {value}")))
}
fn parse_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
fn extract_u64(value: &Value, field: &str) -> Option<u64> {
    value.get(field).and_then(parse_u64)
}
fn stringify(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_u64().map(|v| v.to_string()))
}
fn iso8601_millis(timestamp: u64) -> Option<String> {
    Utc.timestamp_millis_opt(timestamp as i64)
        .single()
        .map(|v| v.to_rfc3339_opts(SecondsFormat::Millis, true))
}
fn to_aster_depth_limit(requested: usize) -> usize {
    [5, 10, 20, 50, 100, 500, 1_000]
        .into_iter()
        .find(|limit| requested <= *limit)
        .unwrap_or(1_000)
}
fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        map_market, map_ohlcv, map_order_book, map_trade, normalize_market_symbol,
        resolve_public_symbol, to_aster_depth_limit, ASTER_INTERVALS,
    };
    #[test]
    fn suffixes_and_symbols() {
        assert_eq!(normalize_market_symbol("btc/usd1:usd1").unwrap(), "BTCUSD1");
        assert_eq!(normalize_market_symbol("BTCU").unwrap(), "BTCU");
        assert_eq!(resolve_public_symbol("BTCUSDT", "BTCUSDT"), "BTC/USDT:USDT");
    }
    #[test]
    fn trade_mapping() {
        let row = serde_json::json!({"id": 7, "price":"2.5", "qty":"4", "quoteQty":"10", "time":1700000000000u64, "isBuyerMaker":true});
        let trade = map_trade(&row, "BTC/USDT:USDT").unwrap();
        assert_eq!(trade.side.as_deref(), Some("sell"));
        assert_eq!(trade.cost, Some(10.0));
    }
    #[test]
    fn kline_mapping() {
        let row = serde_json::json!([1700000000000u64, "1", "2", "0.5", "1.5", "9"]);
        assert_eq!(
            map_ohlcv(&row).unwrap(),
            (1700000000000, 1.0, 2.0, 0.5, 1.5, 9.0)
        );
    }
    #[test]
    fn depth_buckets() {
        assert_eq!(to_aster_depth_limit(1), 5);
        assert_eq!(to_aster_depth_limit(51), 100);
        assert_eq!(to_aster_depth_limit(1000), 1000);
    }
    #[test]
    fn filters_perpetual_markets() {
        let mut row = serde_json::json!({"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.001"},{"filterType":"PRICE_FILTER","tickSize":"0.1"}]});
        let market = map_market(&row, false).unwrap();
        assert_eq!(market.symbol, "BTC/USDT");
        assert_eq!(market.min_order_size, Some(0.001));
        assert_eq!(market.tick_size, Some(0.1));
        row["status"] = serde_json::json!("PENDING_TRADING");
        assert!(map_market(&row, false).is_none());
        assert!(!map_market(&row, true).unwrap().active);
        row["contractType"] = serde_json::json!("");
        assert!(map_market(&row, true).is_none());
        row["contractType"] = serde_json::json!("CURRENT_QUARTER");
        assert!(map_market(&row, true).is_none());
    }

    #[test]
    fn validates_rows_and_sorts_depth_before_truncation() {
        assert!(ASTER_INTERVALS.contains(&"1M"));
        assert!(!ASTER_INTERVALS.contains(&"2m"));
        assert!(map_ohlcv(&serde_json::json!([0, "1"])).is_err());
        assert!(normalize_market_symbol("BTC").is_err());
        let book = map_order_book(&serde_json::json!({"lastUpdateId":42,"E":1700000000000u64,"bids":[["9","3"],["10","2"]],"asks":[["12","1"],["11","4"]]}), "BTC/USDT", 1).unwrap();
        assert_eq!(book.bids, vec![(10.0, 2.0)]);
        assert_eq!(book.asks, vec![(11.0, 4.0)]);
        assert_eq!(book.nonce, Some(42));
        assert_eq!(book.timestamp, Some(1700000000000));
    }
}
