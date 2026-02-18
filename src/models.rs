use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchTradesRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchTradesParams {
    pub symbol: String,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchTradesRequest {
    pub fn into_params(self) -> FetchTradesParams {
        FetchTradesParams {
            symbol: self.symbol,
            since: self.since,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchOhlcvRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub timeframe: Option<String>,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchOhlcvParams {
    pub symbol: String,
    pub timeframe: Option<String>,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchOhlcvRequest {
    pub fn into_params(self) -> FetchOhlcvParams {
        FetchOhlcvParams {
            symbol: self.symbol,
            timeframe: self.timeframe,
            since: self.since,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchOrderBookRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchOrderBookParams {
    pub symbol: String,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchOrderBookRequest {
    pub fn into_params(self) -> FetchOrderBookParams {
        FetchOrderBookParams {
            symbol: self.symbol,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CcxtTrade {
    pub info: Value,
    pub amount: Option<f64>,
    pub datetime: Option<String>,
    pub id: Option<String>,
    pub order: Option<String>,
    pub price: Option<f64>,
    pub timestamp: Option<u64>,
    #[serde(rename = "type")]
    pub trade_type: Option<String>,
    pub side: Option<String>,
    pub symbol: Option<String>,
    pub taker_or_maker: Option<String>,
    pub cost: Option<f64>,
    pub fee: Option<CcxtFee>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CcxtFee {
    pub currency: Option<String>,
    pub cost: Option<f64>,
    pub rate: Option<f64>,
}

pub type CcxtOhlcv = (u64, f64, f64, f64, f64, f64);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CcxtOrderBook {
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
    pub datetime: Option<String>,
    pub timestamp: Option<u64>,
    pub nonce: Option<u64>,
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

fn default_exchange() -> String {
    "hyperliquid".to_string()
}
