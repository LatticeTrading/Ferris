//! Qualified maintained feeds. Dispatch only: aggregate subscription semantics,
//! cache selection, native identity and mapping belong to the venue, not the
//! shared URL lifecycle. A new feed need not use `watch_tickers(null)`.

use std::{collections::BTreeMap, sync::Arc};

use ccxt::Value;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket, live::CatalogWatch, owner::CatalogSnapshot,
            venue::ProviderConfig, venues::lighter,
        },
        traits::ExchangeError,
    },
    models::{MarketStatsField, MarketStatsFieldName},
};

use super::{LiveProvider, LiveSpec, PreparedTopic};

#[derive(Clone, Copy)]
pub(crate) enum StatisticsFeed {
    Lighter,
}

impl StatisticsFeed {
    pub(in crate::exchanges::ccxt) async fn prepare(
        self,
        catalog: Arc<CatalogSnapshot>,
        config: &ProviderConfig,
        watch: Arc<CatalogWatch>,
    ) -> Result<PreparedTopic, ExchangeError> {
        match self {
            Self::Lighter => lighter::stream::prepare_statistics(catalog, config, watch).await,
        }
    }

    pub(in crate::exchanges::ccxt) fn enqueue(self, spec: &LiveSpec, unwatch: bool) {
        match self {
            Self::Lighter => lighter::stream::enqueue_statistics(spec, unwatch),
        }
    }

    pub(in crate::exchanges::ccxt) fn cache(self, provider: &LiveProvider) -> Value {
        match (self, provider) {
            (Self::Lighter, LiveProvider::Lighter(core)) => lighter::stream::statistics_cache(core),
            _ => unreachable!("statistics feed must match its provider"),
        }
    }

    pub(in crate::exchanges::ccxt) fn clear_cache(self, provider: &mut LiveProvider) {
        match (self, provider) {
            (Self::Lighter, LiveProvider::Lighter(core)) => {
                lighter::stream::clear_statistics_cache(core)
            }
            _ => unreachable!("statistics feed must match its provider"),
        }
    }

    pub(in crate::exchanges::ccxt) fn recognizes(self, raw: &Value) -> bool {
        match self {
            Self::Lighter => lighter::statistics::lighter_ticker_frame(raw),
        }
    }

    pub(in crate::exchanges::ccxt) fn catalog_key(self, entry: &CatalogMarket) -> Option<String> {
        match self {
            Self::Lighter => lighter::statistics::live_catalog_key(entry),
        }
    }

    pub(in crate::exchanges::ccxt) fn accepts_market(self, entry: &CatalogMarket) -> bool {
        match self {
            Self::Lighter => lighter::statistics::live_market(entry),
        }
    }

    pub(in crate::exchanges::ccxt) fn row_key(self, row: &Value) -> Option<String> {
        match self {
            Self::Lighter => lighter::statistics::live_row_key(row),
        }
    }

    pub(in crate::exchanges::ccxt) fn patch(
        self,
        row: &Value,
        entry: &CatalogMarket,
        received_timestamp: u64,
    ) -> Result<BTreeMap<MarketStatsFieldName, MarketStatsField>, ExchangeError> {
        match self {
            Self::Lighter => {
                lighter::statistics::lighter_ticker_patch(row, &entry.raw, received_timestamp)
            }
        }
    }
}
