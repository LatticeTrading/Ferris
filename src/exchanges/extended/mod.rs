use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;

use crate::{
    exchanges::traits::{ExchangeError, MarketDataExchange},
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchMarketsParams, FetchOhlcvParams,
        FetchOrderBookParams, FetchTradesParams, UnifiedMarket, UnifiedMarketInfo,
        UnifiedMarketType,
    },
    ws_shared::{
        extended_candle_type, extended_interval, extended_public_symbol, parse_extended_candle,
        parse_extended_trade, parse_f64_lossy, parse_u64_lossy, resolve_extended_ws_symbol,
        EXTENDED_USER_AGENT,
    },
};

pub const DEFAULT_EXTENDED_REST_BASE_URL: &str = "https://api.starknet.extended.exchange/api/v1";

pub struct ExtendedExchange {
    http_client: reqwest::Client,
    rest_base_url: String,
}

impl ExtendedExchange {
    pub fn new(rest_base_url: String, timeout_ms: u64) -> Result<Self, ExchangeError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .user_agent(EXTENDED_USER_AGENT)
            .build()
            .map_err(|err| {
                ExchangeError::Internal(format!("failed to build Extended REST client: {err}"))
            })?;
        Ok(Self {
            http_client,
            rest_base_url: rest_base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, ExchangeError> {
        let response = self
            .http_client
            .get(format!("{}{path}", self.rest_base_url))
            .query(query)
            .send()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(format!("Extended {path}: {err}")))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| ExchangeError::UpstreamRequest(format!("Extended {path}: {err}")))?;
        if !status.is_success() {
            return Err(ExchangeError::UpstreamRequest(format!(
                "Extended {path} status={status} body={}",
                body.chars().take(240).collect::<String>()
            )));
        }
        let value: Value = serde_json::from_str(&body)
            .map_err(|err| ExchangeError::UpstreamData(format!("Extended {path}: {err}")))?;
        if value
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("error"))
        {
            return Err(ExchangeError::UpstreamData(format!(
                "Extended {path}: {}",
                value["error"]
            )));
        }
        value
            .get("data")
            .cloned()
            .ok_or_else(|| ExchangeError::UpstreamData(format!("Extended {path} missing data")))
    }
}

#[async_trait]
impl MarketDataExchange for ExtendedExchange {
    fn id(&self) -> &'static str {
        "extended"
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        let market = requested_market(&params.symbol, &params.params)?;
        let limit = params.limit.unwrap_or(100).clamp(1, 1_000);
        let data = self
            .get(&format!("/info/markets/{market}/trades"), &[])
            .await?;
        let mut trades = Vec::new();
        for row in rows(&data)? {
            match map_trade(row, &market) {
                Ok(trade) => trades.push(trade),
                Err(err) => {
                    tracing::warn!(error = %err, "unable to map Extended trade row, skipping")
                }
            }
        }
        trades.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        if let Some(since) = params.since {
            trades.retain(|trade| trade.timestamp.unwrap_or_default() >= since);
        }
        trades.truncate(limit);
        Ok(trades)
    }

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        let market = requested_market(&params.symbol, &params.params)?;
        let interval = extended_interval(params.timeframe.as_deref().unwrap_or("1m"))
            .map_err(ExchangeError::BadSymbol)?;
        let candle_type = extended_candle_type(&params.params).map_err(ExchangeError::BadSymbol)?;
        let limit = params.limit.unwrap_or(200).clamp(1, 10_000);
        let mut query = vec![
            ("interval", interval.to_string()),
            ("limit", limit.to_string()),
        ];
        if let Some(until) = params
            .params
            .get("endTime")
            .or_else(|| params.params.get("until"))
        {
            let until = parse_u64_lossy(until).ok_or_else(|| {
                ExchangeError::BadSymbol(
                    "Extended endTime/until must be a non-negative millisecond timestamp"
                        .to_string(),
                )
            })?;
            query.push(("endTime", until.to_string()));
        }
        let data = self
            .get(&format!("/info/candles/{market}/{candle_type}"), &query)
            .await?;
        let mut candles = rows(&data)?
            .iter()
            .map(|row| {
                parse_extended_candle(row).ok_or_else(|| {
                    ExchangeError::UpstreamData("invalid Extended candle row".to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        candles.sort_by_key(|row| row.0);
        if let Some(since) = params.since {
            candles.retain(|row| row.0 >= since);
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
        let market = requested_market(&params.symbol, &params.params)?;
        let limit = params.limit.unwrap_or(100).clamp(1, 1_000);
        let data = self
            .get(&format!("/info/markets/{market}/orderbook"), &[])
            .await?;
        map_orderbook(
            &data,
            &extended_public_symbol(&params.symbol, &market),
            limit,
        )
    }

    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        let data = self.get("/info/markets", &[]).await?;
        Ok(rows(&data)?
            .iter()
            .filter_map(|row| map_market(row, params.include_inactive))
            .collect())
    }
}

fn requested_market(symbol: &str, params: &Value) -> Result<String, ExchangeError> {
    let coin =
        match params.get("coin") {
            None => None,
            Some(value) => Some(value.as_str().filter(|v| !v.trim().is_empty()).ok_or_else(
                || ExchangeError::BadSymbol("Extended coin must be a non-empty string".to_string()),
            )?),
        };
    resolve_extended_ws_symbol(symbol, coin).map_err(ExchangeError::BadSymbol)
}

fn rows(value: &Value) -> Result<&[Value], ExchangeError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| ExchangeError::UpstreamData("Extended data must be an array".to_string()))
}

fn map_trade(row: &Value, market: &str) -> Result<CcxtTrade, ExchangeError> {
    let trade = parse_extended_trade(row)
        .ok_or_else(|| ExchangeError::UpstreamData("invalid Extended trade row".to_string()))?;
    let market =
        resolve_extended_ws_symbol(row.get("m").and_then(Value::as_str).unwrap_or(market), None)
            .map_err(ExchangeError::UpstreamData)?;
    let trade_type = row
        .get("tT")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    Ok(CcxtTrade {
        info: trade.info,
        amount: trade.amount,
        datetime: trade.timestamp.and_then(iso8601_millis),
        id: trade.id,
        order: None,
        price: trade.price,
        timestamp: trade.timestamp,
        trade_type,
        side: trade.side,
        symbol: Some(extended_public_symbol("", &market)),
        taker_or_maker: None,
        cost: trade.cost,
        fee: None,
    })
}

fn map_orderbook(data: &Value, symbol: &str, limit: usize) -> Result<CcxtOrderBook, ExchangeError> {
    let mut bids = map_levels(&data["bid"])?;
    let mut asks = map_levels(&data["ask"])?;
    bids.sort_by(|a, b| b.0.total_cmp(&a.0));
    asks.sort_by(|a, b| a.0.total_cmp(&b.0));
    bids.truncate(limit);
    asks.truncate(limit);
    let timestamp = data
        .get("timestamp")
        .or_else(|| data.get("ts"))
        .or_else(|| data.get("T"))
        .and_then(parse_u64_lossy);
    Ok(CcxtOrderBook {
        asks,
        bids,
        datetime: timestamp.and_then(iso8601_millis),
        timestamp,
        nonce: None,
        symbol: Some(symbol.to_string()),
    })
}

fn map_levels(value: &Value) -> Result<Vec<(f64, f64)>, ExchangeError> {
    rows(value)?
        .iter()
        .map(|row| {
            let price = row
                .get("price")
                .and_then(parse_f64_lossy)
                .filter(|v| v.is_finite() && *v > 0.0)
                .ok_or_else(|| {
                    ExchangeError::UpstreamData("invalid Extended orderbook price".to_string())
                })?;
            let quantity = row
                .get("qty")
                .and_then(parse_f64_lossy)
                .filter(|v| v.is_finite() && *v >= 0.0)
                .ok_or_else(|| {
                    ExchangeError::UpstreamData("invalid Extended orderbook qty".to_string())
                })?;
            Ok((price, quantity))
        })
        .collect()
}

fn map_market(row: &Value, include_inactive: bool) -> Option<UnifiedMarket> {
    if row.get("type").and_then(Value::as_str) != Some("PERPETUAL") {
        return None;
    }
    let raw = row.get("name")?.as_str()?;
    let market = resolve_extended_ws_symbol(raw, None).ok()?;
    let base = row.get("assetName")?.as_str()?.to_ascii_uppercase();
    let quote = row
        .get("collateralAssetName")?
        .as_str()?
        .to_ascii_uppercase();
    let active = row.get("active").and_then(Value::as_bool) == Some(true)
        && row.get("status").and_then(Value::as_str) == Some("ACTIVE");
    if !include_inactive && !active {
        return None;
    }
    Some(UnifiedMarket {
        exchange: "extended".to_string(),
        symbol: market.replace('-', "/"),
        base,
        quote,
        market_type: UnifiedMarketType::Perp,
        active,
        min_order_size: row
            .pointer("/tradingConfig/minOrderSize")
            .and_then(parse_f64_lossy)
            .filter(|v| v.is_finite()),
        tick_size: row
            .pointer("/tradingConfig/minPriceChange")
            .and_then(parse_f64_lossy)
            .filter(|v| v.is_finite()),
        contract_size: Some(1.0),
        info: UnifiedMarketInfo {
            category: Some("perpetual".to_string()),
            raw_symbol: Some(raw.to_string()),
            exchange_symbol: Some(market),
        },
    })
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(i64::try_from(timestamp).ok()?)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extended_maps_public_market_data() {
        let trade = map_trade(&json!({"i":1844000421446684673u64,"m":"ETH-USD","T":1000,"S":"BUY","p":"2","q":"3","tT":"LIQUIDATION"}), "BTC-USD").unwrap();
        assert_eq!(trade.id.as_deref(), Some("1844000421446684673"));
        assert_eq!(trade.symbol.as_deref(), Some("ETH/USD:USD"));
        assert_eq!(trade.side.as_deref(), Some("buy"));
        assert_eq!(trade.cost, Some(6.0));
        assert_eq!(trade.trade_type.as_deref(), Some("LIQUIDATION"));
        assert_eq!(trade.info["q"], "3");
        assert!(map_trade(&json!({"T":1,"S":"BUY","p":"NaN","q":"3"}), "BTC-USD").is_err());
        let data = json!({"bid":[{"price":"1","qty":"4"},{"price":"2","qty":"3"}],"ask":[{"price":"4","qty":"1"},{"price":"3","qty":"2"}]});
        let book = map_orderbook(&data, "BTC/USD:USD", 1).unwrap();
        assert_eq!(book.bids, vec![(2.0, 3.0)]);
        assert_eq!(book.asks, vec![(3.0, 2.0)]);
        assert_eq!(
            (book.timestamp, book.datetime, book.nonce),
            (None, None, None)
        );
        let mut timed = data;
        timed["timestamp"] = json!(1000);
        assert_eq!(
            map_orderbook(&timed, "BTC/USD:USD", 2).unwrap().timestamp,
            Some(1000)
        );
    }

    #[test]
    fn extended_filters_perpetual_and_inactive_markets() {
        let mut row = json!({"name":"BTC-USD","assetName":"BTC","collateralAssetName":"USD","type":"PERPETUAL","active":true,"status":"ACTIVE","tradingConfig":{"minOrderSize":"0.001","minPriceChange":"0.1"}});
        let market = map_market(&row, false).unwrap();
        assert_eq!(market.symbol, "BTC/USD");
        assert_eq!(market.min_order_size, Some(0.001));
        assert_eq!(market.tick_size, Some(0.1));
        assert!(matches!(market.market_type, UnifiedMarketType::Perp));
        assert_eq!(market.info.category.as_deref(), Some("perpetual"));
        row["active"] = json!(false);
        assert!(map_market(&row, false).is_none());
        assert!(!map_market(&row, true).unwrap().active);
        row["active"] = json!(true);
        row["status"] = json!("DISABLED");
        assert!(map_market(&row, false).is_none());
        assert!(!map_market(&row, true).unwrap().active);
        row["type"] = json!("SPOT");
        assert!(map_market(&row, true).is_none());
    }

    #[tokio::test]
    async fn extended_rejects_invalid_candle_requests_before_network() {
        let exchange = ExtendedExchange::new("http://127.0.0.1:1".to_string(), 100).unwrap();
        for (timeframe, params) in [
            ("3m", json!({})),
            ("1m", json!({"candleType":"funding"})),
            ("1m", json!({"candleType":1})),
            ("1m", json!({"endTime":-1})),
        ] {
            let error = exchange
                .fetch_ohlcv(FetchOhlcvParams {
                    symbol: "BTC/USD:USD".to_string(),
                    timeframe: Some(timeframe.to_string()),
                    since: None,
                    limit: None,
                    params,
                })
                .await
                .unwrap_err();
            assert!(matches!(error, ExchangeError::BadSymbol(_)));
        }
    }
}
