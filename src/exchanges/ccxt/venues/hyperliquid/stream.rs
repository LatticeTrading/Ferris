//! Hyperliquid Pro subscription policy, hashes and URL selection.

use ccxt::Value;
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{
                params::{field, integer},
                LiveChannel, LiveSpec,
            },
            venue::Venue,
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

pub(in crate::exchanges::ccxt) use crate::exchanges::ccxt::stream::control::stock_unsubscribe as unsubscribe_mode;

const VENUE: Venue = Venue::Hyperliquid;
pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;

pub(in crate::exchanges::ccxt) fn allows(channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "coin" => true,
        "dex" => true,
        "nSigFigs" | "mantissa" => channel == RealtimeChannel::OrderBook,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    _market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    (20, 20, None)
}

pub(in crate::exchanges::ccxt) fn book_params(
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    let figures = integer(input, &["nSigFigs"])?;
    let mantissa = integer(input, &["mantissa"])?;
    if figures.is_some_and(|n| !(2..=5).contains(&n)) {
        return Err(ExchangeError::BadSymbol(
            "nSigFigs must be between 2 and 5".into(),
        ));
    }
    if mantissa.is_some_and(|m| figures != Some(5) || !matches!(m, 1 | 2 | 5)) {
        return Err(ExchangeError::BadSymbol(
            "mantissa requires nSigFigs=5 and 1, 2, or 5".into(),
        ));
    }
    // The stock watcher merges params at the request root. Its
    // subscription object is the supported way to set aggregation.
    if figures.is_some() {
        params["subscription"] = json!({
            "type": "l2Book",
            "coin": if market.raw["swap"] == true { &market.raw["baseName"] } else { &market.raw["id"] },
            "nSigFigs": figures,
            "mantissa": mantissa,
        });
    }
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
        LiveChannel::Client(RealtimeChannel::Trades) => format!("trade:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::OrderBook) => format!("orderbook:{}", spec.symbol),
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "candles:{}:{}",
            spec.timeframe.as_deref().unwrap(),
            spec.symbol
        ),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    format!("unsubscribe:{}", spec.hash)
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::hyperliquid::HyperliquidCore,
    _spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok(field(&core.urls, &["api", "ws", "public"]))
}
