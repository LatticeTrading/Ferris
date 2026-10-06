//! Stock CCXT snapshots, statistics, and realtime behind isolated acquisition owners.

#![forbid(unsafe_code)]

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
    CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchMarketStatsParams, FetchMarketsParams,
    FetchOhlcvParams, FetchOrderBookParams, FetchTradesParams, MarketStatsCapabilities,
    UnifiedMarket,
};
use async_trait::async_trait;
use rest::{SnapshotRequest, SnapshotResponse};

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
}

#[async_trait]
impl MarketStatsSource for CcxtExchange {
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
