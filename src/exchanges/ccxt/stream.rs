//! Stock Pro dispatch and request policy. CCXT owns parsing and book maintenance.

use std::sync::Arc;

use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::traits::ExchangeError,
    realtime::{RealtimeChannel, RealtimeTopic},
};

use super::{
    catalog::catalog_scope,
    owner::CatalogSnapshot,
    venue::{CatalogScope, Provider, ProviderConfig, Venue},
    venues,
};

pub(in crate::exchanges::ccxt) mod control;
pub(in crate::exchanges::ccxt) mod params;
mod provider;
mod spec;
pub(in crate::exchanges::ccxt) mod statistics;

use params::{integer, string, unsupported};
pub(super) use provider::LiveProvider;
pub(crate) use spec::{LiveChannel, LiveSpec, PreparedTopic};

pub(super) fn live_scope(
    venue: Venue,
    channel: RealtimeChannel,
    params: &JsonValue,
) -> Result<CatalogScope, ExchangeError> {
    use RealtimeChannel::*;
    if channel == Ohlcv && !venues::dispatch!(venue, exchange => exchange::stream::CANDLES) {
        return Err(unsupported(venue, "stock CCXT Pro has no candle watcher"));
    }
    let map = params
        .as_object()
        .ok_or_else(|| ExchangeError::BadSymbol("`params` must be an object or null".into()))?;
    for key in map.keys().map(String::as_str) {
        let allowed = match key {
            "type" | "category" | "subType" | "settle" => true,
            "levels" | "depth" | "limit" => channel == OrderBook,
            "timeframe" | "interval" => channel == Ohlcv,
            _ => venues::dispatch!(venue, exchange => exchange::stream::allows(channel, key)),
        };
        if !allowed {
            return Err(unsupported(
                venue,
                &format!("unsupported live parameter `{key}`"),
            ));
        }
    }
    let scope = catalog_scope(venue, params)?;
    Ok(venue.data_scope(scope))
}

pub(super) async fn prepare_live(
    venue: Venue,
    channel: RealtimeChannel,
    mut topic: RealtimeTopic,
    catalog: Arc<CatalogSnapshot>,
    provider: &Provider,
    config: &ProviderConfig,
) -> Result<PreparedTopic, ExchangeError> {
    use RealtimeChannel::*;
    let market = catalog.catalog.resolve(&topic.symbol, &topic.params)?;
    let option = market.raw["option"].as_bool() == Some(true);
    let _contract = market.raw["contract"].as_bool() == Some(true);
    let mut params = json!({});
    let mut timeframe = None;
    let mut depth = None;
    let mut levels = usize::MAX;
    match channel {
        OrderBook => {
            let (default, maximum, acquisition) =
                venues::dispatch!(venue, exchange => exchange::stream::book_depth(market));
            levels = integer(&topic.params, &["levels", "depth", "limit"])?
                .map(usize::try_from)
                .transpose()
                .map_err(|_| ExchangeError::BadSymbol("book depth is too large".into()))?
                .unwrap_or(default);
            if levels == 0 {
                return Err(ExchangeError::BadSymbol(
                    "book depth must be positive".into(),
                ));
            }
            if levels > maximum {
                return Err(unsupported(
                    venue,
                    &format!("live book depth exceeds {maximum}"),
                ));
            }
            depth = acquisition;
            venues::dispatch!(venue, exchange => exchange::stream::book_params(market, &topic.params, &mut params))?;
        }
        Trades => {
            if let Some(name) = string(&topic.params, &["name"])? {
                if !matches!(name, "trade" | "aggTrade") {
                    return Err(unsupported(venue, "trade name must be trade or aggTrade"));
                }
                if name != "trade" {
                    params["name"] = json!(name);
                }
            }
        }
        Ohlcv => {
            if option && !venues::dispatch!(venue, exchange => exchange::rest::OPTION_CANDLES) {
                return Err(unsupported(venue, "Bybit option candles are not supported"));
            }
            let input = string(&topic.params, &["timeframe", "interval"])?.unwrap_or("1m");
            let normalized = if provider.has_timeframe(input) {
                input.to_string()
            } else {
                provider.timeframe_alias(input).ok_or_else(|| {
                    unsupported(venue, &format!("unsupported candle timeframe `{input}`"))
                })?
            };
            if !provider.has_market_timeframe(market, &normalized) {
                return Err(unsupported(
                    venue,
                    &format!("unsupported market timeframe `{normalized}`"),
                ));
            }
            timeframe = Some(normalized);
            venues::dispatch!(venue, exchange => exchange::stream::candle_params(market, &topic.params, &mut params))?;
        }
    }
    let symbol = market.ccxt_symbol.clone();
    let candle_cache_key = venues::dispatch!(venue, exchange => exchange::stream::candle_cache_key(timeframe.as_deref(), &params));
    let mut spec = LiveSpec {
        key: serde_json::to_string(&(
            venue.public_id(),
            &market.market.identity,
            channel.as_str(),
            &timeframe,
            &params,
        ))
        .map_err(|e| ExchangeError::Internal(e.to_string()))?,
        venue,
        scope: catalog.scope,
        channel: LiveChannel::Client(channel),
        symbol: symbol.clone(),
        timeframe,
        depth,
        params,
        market: market.raw.clone(),
        url: String::new(),
        hash: String::new(),
        slots: None,
        catalog: None,
        config: config.clone(),
        candle_cache_key,
    };
    let mut pro = LiveProvider::new(&spec, None);
    spec.slots = pro.slots(&spec)?;
    spec.url = pro.url(&spec).await?;
    spec.hash = venues::dispatch!(venue, exchange => exchange::stream::message_hash(&spec));
    topic.symbol = symbol;
    let client_key = serde_json::to_string(&(&spec.key, (channel == OrderBook).then_some(levels)))
        .map_err(|e| ExchangeError::Internal(e.to_string()))?;
    Ok(PreparedTopic {
        spec: Arc::new(spec),
        topic,
        levels,
        client_key,
    })
}

pub(super) fn panic_error(panic: Box<dyn std::any::Any + Send>) -> ExchangeError {
    let message = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown CCXT panic".into());
    ExchangeError::UpstreamRequest(message)
}
