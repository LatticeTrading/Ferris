//! KuCoin spot and contract markets.
//!
//! Stock `KucoinCore` owns transport and every parser; these are Ferris
//! policies. One `load_markets` acquires spot and all contracts together
//! (linear/inverse perpetuals and dated futures), so the acquisition scope
//! cannot separate contract subtypes.
use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use crate::{
    config::Config,
    exchanges::{
        ccxt::{
            convert::{json_string, IdentityParts},
            venue::{CatalogScope, Venue},
        },
        traits::ExchangeError,
    },
    models::{UnifiedMarket, UnifiedMarketType},
};

pub(in crate::exchanges::ccxt) mod rest;
pub(in crate::exchanges::ccxt) mod statistics;
pub(in crate::exchanges::ccxt) mod stream;

pub(in crate::exchanges::ccxt) fn api(config: &Config) -> Result<JsonValue, ExchangeError> {
    let spot = config.kucoin_rest_base_url.trim_end_matches('/');
    let futures = config.kucoin_futures_rest_base_url.trim_end_matches('/');
    // The `ws` values are stable owner identities, not the connected URL: the
    // public socket URL is minted per session by a REST bullet negotiation.
    Ok(json!({
        "public": spot,
        "private": spot,
        "futuresPublic": futures,
        "futuresPrivate": futures,
        "uta": spot,
        "utaV2": spot,
        "utaPrivate": spot,
        "ws": {
            "spot": config.kucoin_ws_url,
            "futures": config.kucoin_futures_ws_url,
            "private": config.kucoin_ws_url
        }
    }))
}

pub(in crate::exchanges::ccxt) fn configure(
    _config: &Config,
    value: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // Ferris does not use ticker-embedded fees or the currencies catalog. Skip
    // both so one catalog load stays a symbols/contracts request.
    value["options"]["fetchMarkets"] = json!({"fetchTickersFees": false});
    // `load_markets` keys currencies off `has.fetchCurrencies`, not the option.
    value["has"] = json!({"fetchCurrencies": false});
    // The pinned Rust port measures the incremental book cache with the cache
    // handle Dict's length (always 1), not the backing buffer. With stock's
    // default delay of 5 the REST snapshot is never requested. One triggers it
    // on the first delta; stock still owns the snapshot and delta replay.
    value["options"]["watchOrderBook"] = json!({"snapshotDelay": 1});
    Ok(())
}

pub(in crate::exchanges::ccxt) fn scope(
    scope: CatalogScope,
) -> Result<CatalogScope, ExchangeError> {
    match scope {
        CatalogScope::Default
        | CatalogScope::Spot
        | CatalogScope::Linear
        | CatalogScope::Inverse => Ok(scope),
        _ => Err(super::scope_error(Venue::Kucoin, scope)),
    }
}

pub(in crate::exchanges::ccxt) fn market_types(
    scope: CatalogScope,
) -> Option<&'static [&'static str]> {
    match scope {
        CatalogScope::Spot => Some(&["spot"]),
        // `fetch_contract_markets` ignores this list and returns every
        // contract; subtype selection happens on the loaded catalog.
        CatalogScope::Linear | CatalogScope::Inverse => Some(&["swap"]),
        CatalogScope::Default => Some(&["spot", "swap"]),
        CatalogScope::Option => None,
    }
}

pub(in crate::exchanges::ccxt) fn identity(
    market: &Market,
    _product: UnifiedMarketType,
    info: &JsonValue,
    parts: &mut IdentityParts,
) -> Result<(), ExchangeError> {
    if market.contract {
        // Native settlement asset id (e.g. USDT, USD, BTC) for inverse display.
        parts.settlement_asset_id = json_string(info, "settleCurrency");
        if market.swap {
            parts.contract_type = Some("PERPETUAL".into());
        }
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn aliases(
    _source: &Market,
    converted: &UnifiedMarket,
    aliases: &mut Vec<String>,
) {
    // Base-only aliases are convenient but ambiguous across spot and perp; the
    // shared resolver rejects collisions. Bare BASE/QUOTE display pairs are
    // ambiguous too and require a native id or product selector.
    if converted.market_type == UnifiedMarketType::Perp && converted.quote == "USDT" {
        aliases.push(converted.base.clone());
    }
}

pub(in crate::exchanges::ccxt) use super::coin_override as symbol_override;
pub(in crate::exchanges::ccxt) use super::defaults::{
    catalog_default, category, display_quote, raw_symbol, trading_metadata,
};

/// KuCoin spot and perpetual display pairs share `BASE/QUOTE`, so a bare
/// display pair is ambiguous; distinct native ids and CCXT settlement symbols
/// resolve exactly, and a product selector disambiguates the rest.
pub(in crate::exchanges::ccxt) fn data_default() -> (Option<UnifiedMarketType>, Option<&'static str>)
{
    (None, None)
}

pub(in crate::exchanges::ccxt) const DATA_SCOPE: CatalogScope = CatalogScope::Default;
