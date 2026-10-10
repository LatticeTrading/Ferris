use ccxt::{types::Market, Params, TypedExchange};
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::exchanges::traits::ExchangeError;

use super::{CatalogScope, ProviderConfig, Venue};

// These typed wrappers own their cores. This enum never leaves its OS thread.
// No derived Binance ID is used: USD-M is selected explicitly in the options.
pub(in crate::exchanges::ccxt) enum Provider {
    Binance(ccxt::Binance),
    Bybit(ccxt::Bybit),
    Hyperliquid(ccxt::Hyperliquid),
    Lighter(ccxt::Lighter),
    Aster(ccxt::Aster),
    Extended(ccxt::Extended),
    Apex(ccxt::Apex),
    Bitfinex(ccxt::Bitfinex),
    Kucoin(ccxt::Kucoin),
    Nado(ccxt::Nado),
}

macro_rules! dispatch {
    ($provider:expr, $exchange:ident => $body:expr) => {
        match $provider {
            Provider::Binance($exchange) => $body,
            Provider::Bybit($exchange) => $body,
            Provider::Hyperliquid($exchange) => $body,
            Provider::Lighter($exchange) => $body,
            Provider::Aster($exchange) => $body,
            Provider::Extended($exchange) => $body,
            Provider::Apex($exchange) => $body,
            Provider::Bitfinex($exchange) => $body,
            Provider::Kucoin($exchange) => $body,
            Provider::Nado($exchange) => $body,
        }
    };
}

impl Provider {
    pub(in crate::exchanges::ccxt) fn new(
        venue: Venue,
        scope: CatalogScope,
        config: &ProviderConfig,
    ) -> Self {
        let config = Some(config.for_scope(venue, scope));
        match venue {
            Venue::Binance => Self::Binance(ccxt::Binance::new(config)),
            Venue::Bybit => Self::Bybit(ccxt::Bybit::new(config)),
            Venue::Hyperliquid => Self::Hyperliquid(ccxt::Hyperliquid::new(config)),
            Venue::Lighter => Self::Lighter(ccxt::Lighter::new(config)),
            Venue::Aster => Self::Aster(ccxt::Aster::new(config)),
            Venue::Apex => Self::Apex(ccxt::Apex::new(config)),
            Venue::Bitfinex => Self::Bitfinex(ccxt::Bitfinex::new(config)),
            Venue::Kucoin => Self::Kucoin(ccxt::Kucoin::new(config)),
            Venue::Nado => Self::Nado(ccxt::Nado::new(config)),
            Venue::Extended => {
                Self::Extended(super::super::venues::extended::rest_provider(config))
            }
        }
    }

    pub(in crate::exchanges::ccxt) async fn load_markets(
        &mut self,
        reload: bool,
    ) -> ccxt::Result<Vec<Market>> {
        if let Self::Hyperliquid(exchange) = self {
            if reload || exchange.markets().is_empty() {
                let response = exchange
                    .call_raw(
                        "public_post_info",
                        vec![ccxt::Value::from_json(&json!({"type":"spotMeta"}))],
                    )
                    .await?;
                let raw = response.to_json();
                let mut cached = JsonMap::new();
                for token in raw["tokens"].as_array().into_iter().flatten() {
                    let Some(index) = token["index"].as_u64() else {
                        continue;
                    };
                    let Some(name) = token["name"].as_str() else {
                        continue;
                    };
                    cached.insert(index.to_string(), JsonValue::String(name.to_string()));
                }
                exchange.set_options(Params::new().with_json(
                    "cachedCurrenciesById",
                    &JsonValue::Object(cached).to_string(),
                ));
            }
        }
        // Stock's fallible inherent method preserves acquisition errors rather
        // than presenting a failed load as an empty metadata snapshot.
        dispatch!(self, exchange => exchange.try_load_markets(reload).await)
    }

    pub(in crate::exchanges::ccxt) fn precision_mode(&self) -> Result<i64, ExchangeError> {
        dispatch!(self, exchange => exchange.precisionMode.as_i64())
            .ok_or_else(|| ExchangeError::UpstreamData("missing CCXT precision mode".into()))
    }

    pub(in crate::exchanges::ccxt) fn supports(&self, capability: &str) -> bool {
        let value =
            dispatch!(self, exchange => ccxt::value::get_value_k(&exchange.has, capability));
        value.as_bool() == Some(true) || value.as_str() == Some("emulated")
    }

    pub(in crate::exchanges::ccxt) fn has_timeframe(&self, timeframe: &str) -> bool {
        dispatch!(self, exchange => exchange.timeframes.as_map().is_some_and(|map| map.contains_key(timeframe)))
    }

    /// Product-specific restrictions must be checked after resolving the market
    /// and normalizing aliases. KuCoin's top-level map includes spot-only keys.
    pub(in crate::exchanges::ccxt) fn has_market_timeframe(
        &self,
        market: &super::super::catalog::CatalogMarket,
        timeframe: &str,
    ) -> bool {
        if let Self::Kucoin(exchange) = self {
            if market.raw["contract"] == true {
                return super::super::stream::params::field(
                    &exchange.options,
                    &["timeframes", "swap", timeframe],
                )
                .as_i64()
                .is_some_and(|minutes| minutes > 0);
            }
        }
        self.has_timeframe(timeframe)
    }

    pub(in crate::exchanges::ccxt) fn timeframe_alias(&self, alias: &str) -> Option<String> {
        dispatch!(self, exchange => {
            let timeframes = exchange.timeframes.as_map()?;
            let mut matches = timeframes.iter()
                .filter(|(_, value)| value.as_str() == Some(alias));
            let (timeframe, _) = matches.next()?;
            matches.next().is_none().then(|| timeframe.clone())
        })
    }

    pub(in crate::exchanges::ccxt) fn timeframe_millis(&self, timeframe: &str) -> Option<i64> {
        dispatch!(self, exchange => exchange.parse_timeframe(ccxt::Value::from(timeframe)))
            .as_i64()?
            .checked_mul(1_000)
            .filter(|duration| *duration > 0)
    }

    pub(in crate::exchanges::ccxt) async fn call(
        &mut self,
        method: &str,
        args: Vec<ccxt::Value>,
    ) -> ccxt::Result<ccxt::Value> {
        dispatch!(self, exchange => exchange.call_raw(method, args).await)
    }
}

pub(in crate::exchanges::ccxt) fn exchange_error(
    venue: Venue,
    error: ccxt::ExchangeError,
) -> ExchangeError {
    let message = format!("{}: {error}", venue.public_id());
    if error.is("BadSymbol") {
        ExchangeError::BadSymbol(message)
    } else if error.is("BadResponse") || error.is("ChecksumError") {
        ExchangeError::UpstreamData(message)
    } else if error.is("NotSupported") {
        // Every catalog profile is source-qualified; this is a dispatch defect,
        // not permission to change the supported venue set on a failed request.
        ExchangeError::Internal(message)
    } else {
        ExchangeError::UpstreamRequest(message)
    }
}
