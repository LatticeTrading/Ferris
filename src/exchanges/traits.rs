use async_trait::async_trait;
use serde_json::Value;

use crate::market_stats::MarketStatsSourceSnapshot;
use crate::models::{
    CcxtOhlcv, CcxtOrderBook, CcxtTicker, CcxtTrade, ExchangeDataCapabilities,
    FetchMarketStatsParams, FetchMarketsParams, FetchOhlcvParams, FetchOrderBookParams,
    FetchTickersParams, FetchTradesParams, MarketStatsCapabilities, UnifiedMarket,
};
#[derive(Debug, Clone, thiserror::Error)]
pub enum ExchangeError {
    #[error("bad symbol: {0}")]
    BadSymbol(String),
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),
    #[error("upstream request failed: {0}")]
    UpstreamRequest(String),
    #[error("upstream response invalid: {0}")]
    UpstreamData(String),
    #[error("internal exchange error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait MarketStatsSource: Send + Sync {
    fn capabilities(&self) -> MarketStatsCapabilities;

    /// Source-specific request normalization and market identity rules. Native
    /// adapters can implement these without joining the CCXT venue enum.
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        if input.is_null() || input.is_object() {
            Ok(input.clone())
        } else {
            Err(ExchangeError::BadSymbol(
                "market statistics params must be an object or null".into(),
            ))
        }
    }

    fn acquisition_params(&self, params: &Value) -> Value {
        params.clone()
    }

    fn catalog_source(&self) -> &'static str {
        "exchange:market-catalog"
    }

    fn catalog_products(&self, _params: &Value) -> &'static [crate::models::UnifiedMarketType] {
        &[crate::models::UnifiedMarketType::Perp]
    }

    fn selection_matches(
        &self,
        product: crate::models::UnifiedMarketType,
        category: Option<&str>,
        dex: Option<&str>,
        _native_id: &str,
        _params: &Value,
    ) -> bool {
        product == crate::models::UnifiedMarketType::Perp && category.is_none() && dex.is_none()
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError>;

    /// Optional maintained source; the receiver owns its upstream demand lease.
    async fn subscribe_market_stats(
        &self,
    ) -> Result<Option<crate::realtime::RealtimeSubscription>, ExchangeError> {
        Ok(None)
    }
}

#[async_trait]
pub trait MarketDataExchange: Send + Sync {
    fn id(&self) -> &'static str;

    fn capabilities(&self) -> ExchangeDataCapabilities;

    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        None
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError>;

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError>;

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError>;
    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError>;
    async fn fetch_tickers(
        &self,
        _params: FetchTickersParams,
    ) -> Result<Vec<CcxtTicker>, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "fetchTickers is not implemented by this exchange".into(),
        ))
    }
}
