//! Lighter Pro subscription policy, hashes and URL selection.

use std::sync::Arc;

use ccxt::Value;
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            live::CatalogWatch,
            owner::CatalogSnapshot,
            stream::{
                params::{field, unsupported},
                LiveChannel, LiveProvider, LiveSpec, PreparedTopic,
            },
            venue::{ProviderConfig, Venue},
        },
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
    realtime::{RealtimeChannel, RealtimeTopic},
};

const VENUE: Venue = Venue::Lighter;
pub(in crate::exchanges::ccxt) const CANDLES: bool = false;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;

pub(in crate::exchanges::ccxt) fn allows(_channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "marketId" | "market_id" => true,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    _market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    (20, usize::MAX, None)
}

pub(in crate::exchanges::ccxt) fn book_params(
    _market: &CatalogMarket,
    _input: &JsonValue,
    _params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    Ok(())
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    super::super::shared_stream::candle_params(VENUE, market, input, params, false)
}

pub(in crate::exchanges::ccxt) fn candle_cache_key(
    _timeframe: Option<&str>,
    _params: &JsonValue,
) -> Option<String> {
    None
}

pub(in crate::exchanges::ccxt) fn message_hash(spec: &LiveSpec) -> String {
    match spec.channel {
        LiveChannel::Statistics => "tickers".to_string(),
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade::{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook::{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => {
            unreachable!("rejected before metadata acquisition")
        }
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    format!("unsubscribe:{}", spec.hash)
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::lighter::LighterCore,
    _spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok(field(&core.urls, &["api", "ws"]))
}

/// Lighter alone qualifies one aggregate subscription on `market_stats/all`.
/// Other venues' `watch_tickers(null)` need not have the same semantics.
pub(in crate::exchanges::ccxt) fn enqueue_statistics(spec: &LiveSpec, unwatch: bool) {
    let method = if unwatch {
        "un_watch_tickers"
    } else {
        "watch_tickers"
    };
    ccxt::exchange_stubs::enqueue_spawn(method, vec![Value::Null, Value::from_json(&spec.params)]);
}

pub(in crate::exchanges::ccxt) fn statistics_cache(
    core: &ccxt_pro::pro::lighter::LighterCore,
) -> Value {
    core.tickers.clone()
}

pub(in crate::exchanges::ccxt) fn clear_statistics_cache(
    core: &mut ccxt_pro::pro::lighter::LighterCore,
) {
    // `Value::clear` is a no-op on a plain map; replace it outright.
    core.tickers = Value::from_json(&json!({}));
}

pub(in crate::exchanges::ccxt) async fn prepare_statistics(
    catalog: Arc<CatalogSnapshot>,
    config: &ProviderConfig,
    watch: Arc<CatalogWatch>,
) -> Result<PreparedTopic, ExchangeError> {
    if catalog.venue != Venue::Lighter {
        return Err(unsupported(
            catalog.venue,
            "statistics aggregate is Lighter-only",
        ));
    }
    let snapshot = watch.load();
    let representative = snapshot
        .catalog
        .entries()
        .iter()
        .find(|entry| entry.market.market_type == UnifiedMarketType::Perp)
        .ok_or_else(|| {
            ExchangeError::UpstreamData("Lighter catalog has no perpetual market".into())
        })?;
    let key = format!("statistics:{}", Venue::Lighter.public_id());
    let mut spec = LiveSpec {
        key: key.clone(),
        venue: Venue::Lighter,
        scope: catalog.scope,
        channel: LiveChannel::Statistics,
        symbol: "*".to_string(),
        timeframe: None,
        depth: None,
        params: json!({}),
        market: representative.raw.clone(),
        url: String::new(),
        hash: String::new(),
        slots: None,
        catalog: Some(Arc::clone(&watch)),
        config: config.clone(),
        candle_cache_key: None,
    };
    let mut pro = LiveProvider::new(&spec, None);
    spec.url = pro.url(&spec).await?;
    spec.hash = message_hash(&spec);
    Ok(PreparedTopic {
        spec: Arc::new(spec),
        topic: RealtimeTopic {
            exchange: Venue::Lighter.public_id().to_string(),
            symbol: "*".to_string(),
            params: json!({}),
        },
        levels: 0,
        client_key: key,
    })
}
