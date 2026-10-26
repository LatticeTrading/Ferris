//! Stock CCXT snapshots, statistics, and realtime behind isolated acquisition owners.

#![forbid(unsafe_code)]

mod capabilities;
mod catalog;
mod convert;
mod live;
mod owner;
mod rest;
mod statistics;
pub(crate) mod statistics_profile;
mod stream;
mod venue;
mod venues;

pub use catalog::{Catalog, CatalogMarket};
pub use owner::{CatalogSnapshot, CcxtService};
pub(crate) use stream::PreparedTopic;
pub use venue::{CatalogScope, Venue};

use crate::exchanges::traits::{ExchangeError, MarketDataExchange, MarketStatsSource};
use crate::models::{
    CcxtOhlcv, CcxtOrderBook, CcxtTicker, CcxtTrade, FetchMarketStatsParams, FetchMarketsParams,
    FetchOhlcvParams, FetchOrderBookParams, FetchTickersParams, FetchTradesParams,
    MarketStatsCapabilities, UnifiedMarket,
};
use async_trait::async_trait;
use rest::{SnapshotRequest, SnapshotResponse};
use serde_json::Value;

pub struct CcxtExchange {
    venue: Venue,
    service: CcxtService,
}

impl CcxtExchange {
    pub fn new(venue: Venue, service: CcxtService) -> Self {
        Self { venue, service }
    }
}

#[async_trait]
impl MarketDataExchange for CcxtExchange {
    fn id(&self) -> &'static str {
        self.venue.public_id()
    }

    fn capabilities(&self) -> crate::models::ExchangeDataCapabilities {
        venues::data_capabilities(self.venue)
    }

    fn market_stats_source(&self) -> Option<&dyn MarketStatsSource> {
        Some(self)
    }

    async fn fetch_markets(
        &self,
        params: FetchMarketsParams,
    ) -> Result<Vec<UnifiedMarket>, ExchangeError> {
        rest::validate_params(self.venue, rest::Operation::Markets, &params.params)?;
        let scope = catalog::catalog_scope(self.venue, &params.params)?;
        let snapshot = self.service.catalog(self.venue, scope).await?;
        snapshot
            .catalog
            .select(&params)
            .map(|rows| rows.into_iter().cloned().collect())
    }

    async fn fetch_trades(
        &self,
        params: FetchTradesParams,
    ) -> Result<Vec<CcxtTrade>, ExchangeError> {
        match self
            .service
            .snapshot(self.venue, SnapshotRequest::Trades(params))
            .await?
        {
            SnapshotResponse::Trades(rows) => Ok(rows),
            _ => unreachable!("owner returns the requested snapshot variant"),
        }
    }

    async fn fetch_ohlcv(&self, params: FetchOhlcvParams) -> Result<Vec<CcxtOhlcv>, ExchangeError> {
        match self
            .service
            .snapshot(self.venue, SnapshotRequest::Ohlcv(params))
            .await?
        {
            SnapshotResponse::Ohlcv(rows) => Ok(rows),
            _ => unreachable!("owner returns the requested snapshot variant"),
        }
    }

    async fn fetch_order_book(
        &self,
        params: FetchOrderBookParams,
    ) -> Result<CcxtOrderBook, ExchangeError> {
        match self
            .service
            .snapshot(self.venue, SnapshotRequest::Book(params))
            .await?
        {
            SnapshotResponse::Book(book) => Ok(book),
            _ => unreachable!("owner returns the requested snapshot variant"),
        }
    }
    async fn fetch_tickers(
        &self,
        _params: FetchTickersParams,
    ) -> Result<Vec<CcxtTicker>, ExchangeError> {
        Err(ExchangeError::UnsupportedFeature(
            "fetchTickers is not exposed by the native facade for stock CCXT venues".into(),
        ))
    }
}

#[async_trait]
impl MarketStatsSource for CcxtExchange {
    fn normalize_params(&self, input: &Value) -> Result<Value, ExchangeError> {
        statistics_profile::normalize_params(self.venue, input)
    }

    fn acquisition_params(&self, params: &Value) -> Value {
        statistics_profile::acquisition_params(self.venue, params)
    }

    fn catalog_source(&self) -> &'static str {
        statistics_profile::catalog_source(self.venue)
    }

    fn catalog_products(&self, params: &Value) -> &'static [crate::models::UnifiedMarketType] {
        statistics_profile::catalog_products(
            self.venue,
            statistics_profile::scope(self.venue, params)
                .unwrap_or(crate::exchanges::ccxt::CatalogScope::Default),
        )
    }

    fn selection_matches(
        &self,
        product: crate::models::UnifiedMarketType,
        category: Option<&str>,
        dex: Option<&str>,
        native_id: &str,
        params: &Value,
    ) -> bool {
        statistics_profile::selection_matches(self.venue, product, category, dex, native_id, params)
    }

    fn capabilities(&self) -> MarketStatsCapabilities {
        statistics_profile::capabilities(self.venue)
    }

    async fn fetch_market_stats(
        &self,
        params: FetchMarketStatsParams,
    ) -> Result<crate::market_stats::MarketStatsSourceSnapshot, ExchangeError> {
        self.service.statistics(self.venue, params).await
    }

    async fn subscribe_market_stats(
        &self,
    ) -> Result<Option<crate::realtime::RealtimeSubscription>, ExchangeError> {
        self.service.subscribe_statistics(self.venue).await
    }
}
