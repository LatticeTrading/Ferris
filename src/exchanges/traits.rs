use async_trait::async_trait;

use crate::models::{
    CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams, FetchTradesParams,
};

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("bad symbol: {0}")]
    BadSymbol(String),
    #[error("upstream request failed: {0}")]
    UpstreamRequest(String),
    #[error("upstream response invalid: {0}")]
    UpstreamData(String),
    #[error("internal exchange error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait MarketDataExchange: Send + Sync {
    fn id(&self) -> &'static str;

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError>;

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError>;

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError>;
}
