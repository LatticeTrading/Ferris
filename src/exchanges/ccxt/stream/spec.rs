use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::{
    exchanges::ccxt::{
        live::CatalogWatch,
        statistics_profile,
        stream::statistics::StatisticsFeed,
        venue::{CatalogScope, ProviderConfig, Venue},
        venues,
    },
    realtime::{RealtimeChannel, RealtimeTopic},
};

/// Internal feed identity. Public client channels keep their own enum so the
/// statistics aggregate can share a URL without becoming a subscribable channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveChannel {
    Client(RealtimeChannel),
    Statistics,
}

impl LiveChannel {
    pub(in crate::exchanges::ccxt) fn client(self) -> Option<RealtimeChannel> {
        match self {
            Self::Client(channel) => Some(channel),
            Self::Statistics => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct LiveSpec {
    pub key: String,
    pub venue: Venue,
    pub scope: CatalogScope,
    pub channel: LiveChannel,
    pub symbol: String,
    pub timeframe: Option<String>,
    pub depth: Option<usize>,
    pub params: JsonValue,
    pub market: JsonValue,
    pub url: String,
    pub slots: Option<usize>,
    pub hash: String,
    /// Statistics only: shared owned catalog cell so newly listed markets resolve
    /// without restarting the URL. Never a stock `Value`; no book copy.
    pub catalog: Option<Arc<CatalogWatch>>,
    pub(in crate::exchanges::ccxt) config: ProviderConfig,
    pub(in crate::exchanges::ccxt) candle_cache_key: Option<String>,
}

impl LiveSpec {
    pub(in crate::exchanges::ccxt) fn statistics_feed(&self) -> StatisticsFeed {
        statistics_profile::live_statistics(self.venue)
            .expect("prepared statistics feed is qualified")
            .feed
    }

    pub(super) fn candle_key(&self) -> &str {
        self.candle_cache_key
            .as_deref()
            .or(self.timeframe.as_deref())
            .expect("candle timeframe")
    }

    pub(in crate::exchanges::ccxt) fn unsubscribe_hash(&self) -> String {
        venues::dispatch!(self.venue, exchange => exchange::stream::unsubscribe_hash(self))
    }
}

pub(crate) struct PreparedTopic {
    pub spec: Arc<LiveSpec>,
    pub topic: RealtimeTopic,
    pub levels: usize,
    pub client_key: String,
}
