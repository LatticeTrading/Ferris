//! Stock Pro cores and shared multi-hash driver/cache operations.

use std::sync::Arc;

use ccxt::{exchange::ExchangeRuntime, exchange_generated::ExchangeBase, Value};
use futures_util::FutureExt;
use serde_json::json;

use crate::{
    exchanges::{
        ccxt::{catalog::CatalogMarket, venue::Venue, venues},
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

use super::{
    control::{LiveEvent, UnsubscribeMode},
    panic_error,
    params::field,
    spec::{LiveChannel, LiveSpec},
};

// Concrete cores are deliberately not typed wrappers: ExchangeRuntime's
// multi-hash driver must own ALL channels at an actual URL, not await them in turn.
pub(in crate::exchanges::ccxt) enum LiveProvider {
    Binance(ccxt_pro::pro::binance::BinanceCore),
    Bybit(ccxt_pro::pro::bybit::BybitCore),
    Hyperliquid(ccxt_pro::pro::hyperliquid::HyperliquidCore),
    Lighter(ccxt_pro::pro::lighter::LighterCore),
    Aster(ccxt_pro::pro::aster::AsterCore),
    Extended(ccxt_pro::pro::extended::ExtendedCore),
    Apex(venues::apex::stream::Provider),
    Bitfinex(venues::bitfinex::stream::Provider),
    Kucoin(venues::kucoin::stream::Provider),
}

macro_rules! dispatch {
    ($provider:expr, $core:ident => $body:expr) => {
        match $provider {
            LiveProvider::Binance($core) => $body,
            LiveProvider::Bybit($core) => $body,
            LiveProvider::Hyperliquid($core) => $body,
            LiveProvider::Lighter($core) => $body,
            LiveProvider::Aster($core) => $body,
            LiveProvider::Extended($core) => $body,
            LiveProvider::Apex($core) => $body,
            LiveProvider::Bitfinex($core) => $body,
            LiveProvider::Kucoin($core) => $body,
        }
    };
}

impl LiveProvider {
    pub(in crate::exchanges::ccxt) fn new(spec: &LiveSpec, slot: Option<usize>) -> Self {
        let mut config = spec.config.for_scope(spec.venue, spec.scope);
        let markets = match &spec.catalog {
            // Statistics resolves rows for every loaded market: seed them all so
            // the stock parser names each row instead of building a synthetic one.
            Some(watch) => watch
                .load()
                .catalog
                .entries()
                .iter()
                .map(|entry| Value::from_json(&entry.raw))
                .collect(),
            None => vec![Value::from_json(&spec.market)],
        };
        ccxt::set_value(&mut config, &Value::from("markets"), Value::from(markets));
        if let Some(slot) = slot {
            let mut options = ccxt::value::get_value_k(&config, "options");
            ccxt::set_value(
                &mut options,
                &Value::from("streamIndex"),
                Value::Int(slot as i64 - 1),
            );
            ccxt::set_value(&mut config, &Value::from("options"), options);
        }
        let config = Some(config);
        match spec.venue {
            Venue::Binance => Self::Binance(ccxt_pro::pro::binance::BinanceCore::new(config)),
            Venue::Bybit => Self::Bybit(ccxt_pro::pro::bybit::BybitCore::new(config)),
            Venue::Hyperliquid => {
                Self::Hyperliquid(ccxt_pro::pro::hyperliquid::HyperliquidCore::new(config))
            }
            Venue::Lighter => Self::Lighter(ccxt_pro::pro::lighter::LighterCore::new(config)),
            Venue::Aster => Self::Aster(ccxt_pro::pro::aster::AsterCore::new(config)),
            Venue::Apex => Self::Apex(venues::apex::stream::provider(config)),
            Venue::Bitfinex => Self::Bitfinex(venues::bitfinex::stream::provider(config)),
            Venue::Kucoin => Self::Kucoin(venues::kucoin::stream::provider(config)),
            Venue::Extended => {
                Self::Extended(super::super::venues::extended::stream::provider(config))
            }
        }
    }

    pub(in crate::exchanges::ccxt) async fn url(
        &mut self,
        spec: &LiveSpec,
    ) -> Result<String, ExchangeError> {
        let url = match self {
            Self::Binance(core) => super::super::venues::binance::stream::url(core, spec).await?,
            Self::Bybit(core) => super::super::venues::bybit::stream::url(core, spec).await?,
            Self::Hyperliquid(core) => {
                super::super::venues::hyperliquid::stream::url(core, spec).await?
            }
            Self::Lighter(core) => super::super::venues::lighter::stream::url(core, spec).await?,
            Self::Aster(core) => super::super::venues::aster::stream::url(core, spec).await?,
            Self::Apex(core) => venues::apex::stream::url(&mut core.core, spec).await?,
            Self::Bitfinex(core) => venues::bitfinex::stream::url(&mut core.core, spec).await?,
            Self::Kucoin(core) => venues::kucoin::stream::url(&mut core.core, spec).await?,
            Self::Extended(core) => super::super::venues::extended::stream::url(core, spec).await?,
        };
        url.as_str()
            .filter(|s| s.starts_with("ws://") || s.starts_with("wss://"))
            .map(str::to_string)
            .ok_or_else(|| ExchangeError::UpstreamData("invalid stock websocket URL".into()))
    }

    /// KuCoin mints its public WebSocket URL from a REST `bullet-public`
    /// negotiation carrying an expiring token. The stable `url()` value is only
    /// an owner identity; a fresh session URL is negotiated on every connect.
    pub(in crate::exchanges::ccxt) async fn session_url(
        &mut self,
        spec: &LiveSpec,
    ) -> Result<String, ExchangeError> {
        match self {
            Self::Kucoin(core) => venues::kucoin::stream::session_url(&mut core.core, spec).await,
            _ => Err(ExchangeError::Internal(
                "session URL negotiation is only defined for KuCoin".into(),
            )),
        }
    }

    /// Bind every stock watch to the actual session URL, including its fresh
    /// connection timestamp. Owner/preparation identity stays timestamp-free.
    pub(in crate::exchanges::ccxt) fn bind_url(&mut self, url: &str) {
        if let Self::Bitfinex(core) = self {
            core.bind_url(url);
        }
        if let Self::Kucoin(core) = self {
            core.bind_url(url);
            venues::kucoin::stream::bind_url(&mut core.core, url);
        }
        if let Self::Apex(core) = self {
            core.bind_url(url);
            ccxt::set_value(
                &mut core.options,
                &Value::from("wsPublicUrl"),
                Value::from(url),
            );
        }
    }

    pub(super) fn slots(&self, spec: &LiveSpec) -> Result<Option<usize>, ExchangeError> {
        match self {
            Self::Binance(core) => venues::binance::stream::slots(core, spec),
            _ => Ok(None),
        }
    }

    pub(in crate::exchanges::ccxt) fn add_market(&mut self, spec: &LiveSpec) {
        if let Some(watch) = &spec.catalog {
            self.add_catalog(watch.load().catalog.entries());
            return;
        }
        dispatch!(self, core => {
            if ccxt::value::get_value_k(&core.markets, &spec.symbol).is_null() {
                let mut markets = core.markets.clone();
                ccxt::set_value(&mut markets, &Value::from(spec.symbol.as_str()), Value::from_json(&spec.market));
                core.set_markets(markets, &[]);
            }
        });
        if let Self::Apex(core) = self {
            venues::apex::stream::index_market_ids(&mut core.core);
        }
    }

    /// Merge catalog rows the live core has not seen yet. The statistics feed
    /// calls this when the shared catalog cell advances, so a newly listed
    /// market is parseable without restarting the shared URL.
    pub(in crate::exchanges::ccxt) fn add_catalog(&mut self, entries: &[CatalogMarket]) {
        dispatch!(self, core => {
            let mut markets = core.markets.clone();
            let mut changed = false;
            for entry in entries {
                if ccxt::value::get_value_k(&markets, &entry.ccxt_symbol).is_null() {
                    ccxt::set_value(
                        &mut markets,
                        &Value::from(entry.ccxt_symbol.as_str()),
                        Value::from_json(&entry.raw),
                    );
                    changed = true;
                }
            }
            if changed {
                core.set_markets(markets, &[]);
            }
        });
    }

    pub(in crate::exchanges::ccxt) fn unsubscribe_mode(spec: &LiveSpec) -> UnsubscribeMode {
        venues::dispatch!(spec.venue, exchange => exchange::stream::unsubscribe_mode(spec.channel))
    }

    pub(in crate::exchanges::ccxt) fn enqueue(
        &mut self,
        spec: &LiveSpec,
        unwatch: bool,
    ) -> Result<(), ExchangeError> {
        if unwatch && Self::unsubscribe_mode(spec) == UnsubscribeMode::Reconnect {
            return Err(ExchangeError::Internal(
                "reconnect-only feed cannot unwatch".into(),
            ));
        }
        if let Self::Apex(core) = self {
            if unwatch {
                return core.unsubscribe(spec);
            }
            core.subscribe(spec)?;
        }
        if let Self::Bitfinex(core) = self {
            if unwatch {
                return core.unsubscribe(spec);
            }
            core.subscribe(spec)?;
        }
        let symbol = Value::from(spec.symbol.as_str());
        let params = Value::from_json(&spec.params);
        if spec.channel == LiveChannel::Statistics {
            spec.statistics_feed().enqueue(spec, unwatch);
            return Ok(());
        }
        let limit = spec
            .depth
            .map(|depth| Value::Int(depth as i64))
            .unwrap_or_default();
        let timeframe = Value::from(spec.timeframe.as_deref().unwrap_or("1m"));
        let (method, args) = match (spec.channel.client().expect("client channel"), unwatch) {
            (RealtimeChannel::Trades, false) => (
                "watch_trades",
                vec![symbol, Value::Null, Value::Null, params],
            ),
            (RealtimeChannel::OrderBook, false) => {
                ("watch_order_book", vec![symbol, limit, params])
            }
            (RealtimeChannel::Ohlcv, false) => (
                "watch_ohlcv",
                vec![symbol, timeframe, Value::Null, Value::Null, params],
            ),
            (RealtimeChannel::Trades, true) => ("un_watch_trades", vec![symbol, params]),
            (RealtimeChannel::OrderBook, true) => {
                let mut params = params;
                if venues::dispatch!(spec.venue, exchange => exchange::stream::UNWATCH_BOOK_LIMIT) {
                    ccxt::set_value(&mut params, &Value::from("limit"), limit);
                }
                ("un_watch_order_book", vec![symbol, params])
            }
            (RealtimeChannel::Ohlcv, true) => ("un_watch_ohlcv", vec![symbol, timeframe, params]),
        };
        // Native watches invoked inside the single ws_run loop register their
        // stock frames and return via its same-URL nested branch. They must not
        // be awaited sequentially on quiet per-channel futures.
        ccxt::exchange_stubs::enqueue_spawn(method, args);
        Ok(())
    }

    pub(in crate::exchanges::ccxt) async fn next(
        &mut self,
        url: &str,
        hashes: &[String],
    ) -> Result<LiveEvent, ExchangeError> {
        std::panic::AssertUnwindSafe(async {
            if let Self::Apex(core) = self {
                return core.next(url, hashes).await;
            }
            if let Self::Bitfinex(core) = self { return core.next(url, hashes).await; }
            if let Self::Kucoin(core) = self { return core.next(url, hashes).await; }
            Ok(LiveEvent::Data(dispatch!(self, core => core.ws_run(url.to_string(), hashes.to_vec(), Value::Null, Vec::new(), Value::Null).await)))
        }).catch_unwind().await.map_err(panic_error)?
    }

    pub(in crate::exchanges::ccxt) fn cache(&self, spec: &LiveSpec) -> Value {
        if spec.channel == LiveChannel::Statistics {
            return spec.statistics_feed().cache(self);
        }
        dispatch!(self, core => match spec.channel.client().expect("client channel") {
            RealtimeChannel::OrderBook => ccxt::value::get_value_k(&core.orderbooks, &spec.symbol),
            RealtimeChannel::Trades => ccxt::value::get_value_k(&core.trades, &spec.symbol),
            RealtimeChannel::Ohlcv => field(&core.ohlcvs, &[&spec.symbol, spec.candle_key()]),
        })
    }

    pub(in crate::exchanges::ccxt) fn incremental(
        &self,
        spec: &LiveSpec,
        mut cache: Value,
    ) -> Result<Value, ExchangeError> {
        if spec.channel == LiveChannel::Statistics {
            // Tickers are a live keyed map, not an append-only cache: the
            // statistics path compares per-symbol cache pointers itself.
            return Ok(cache);
        }
        let limit = cache.get_limit(Value::from(spec.symbol.as_str()), Value::Null);
        if limit
            .as_i64()
            .zip(ccxt::runtime::get_array_length(&cache).as_i64())
            .is_some_and(|(count, size)| count > size)
        {
            return Err(ExchangeError::UpstreamData(
                "stock incremental cache overflow".into(),
            ));
        }
        Ok(dispatch!(self, core => core.filter_by_since_limit(cache, &[
            Value::Null, limit,
            if spec.channel == LiveChannel::Client(RealtimeChannel::Trades) { Value::from("timestamp") } else { Value::Int(0) },
            Value::Bool(true),
        ])))
    }

    pub(in crate::exchanges::ccxt) fn clear_feed(&mut self, spec: &LiveSpec) {
        if spec.channel == LiveChannel::Statistics {
            spec.statistics_feed().clear_cache(self);
            return;
        }
        let mut cache = self.cache(spec);
        if spec.channel == LiveChannel::Client(RealtimeChannel::OrderBook) {
            cache.reset(Value::from_json(&json!({"bids": [], "asks": []})));
            ccxt::set_value(
                &mut cache,
                &Value::from("cache"),
                Value::from(Vec::<Value>::new()),
            );
        } else {
            cache.clear();
        }
        dispatch!(self, core => {
            let map = match spec.channel.client().expect("client channel") {
                RealtimeChannel::OrderBook => &mut core.orderbooks,
                RealtimeChannel::Trades => &mut core.trades,
                RealtimeChannel::Ohlcv => &mut core.ohlcvs,
            };
            if let Value::Dict(map) = map {
                if spec.channel == LiveChannel::Client(RealtimeChannel::Ohlcv) {
                    if let Some(Value::Dict(timeframes)) = Arc::make_mut(map).get_mut(&spec.symbol) {
                        Arc::make_mut(timeframes).shift_remove(spec.candle_key());
                    }
                } else {
                    Arc::make_mut(map).shift_remove(&spec.symbol);
                }
            }
        });
    }

    pub(in crate::exchanges::ccxt) fn clear_retained(&self, cache: Option<Value>) {
        if let Some(mut cache) = cache {
            if ccxt::value::get_value_k(&cache, "__book_id")
                .as_i64()
                .is_some()
            {
                cache.reset(Value::from_json(&json!({"bids": [], "asks": []})));
                ccxt::set_value(
                    &mut cache,
                    &Value::from("cache"),
                    Value::from(Vec::<Value>::new()),
                );
            } else {
                cache.clear();
            }
        }
    }

    pub(in crate::exchanges::ccxt) fn expire_unsubscribe(&self, url: &str, spec: &LiveSpec) {
        let unsubscribe = spec.unsubscribe_hash();
        dispatch!(self, core => core.clean_unsubscription(
            ccxt_pro::pro::ws_client::client_value(url), Value::from(spec.hash.as_str()),
            Value::from(unsubscribe.as_str()), &[],
        ));
        if let Some(client) = ccxt_pro::pro::ws_client::get_client(url) {
            // Only this retired feed's settlements: never drain the shared
            // source queue or discard an unrelated warm hash.
            let hashes = [spec.hash.clone(), unsubscribe];
            while client.take_settled(&hashes).is_some() {}
        }
    }
}
