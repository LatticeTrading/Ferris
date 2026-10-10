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

/// Seeding stock markets skips fetch_hip3_markets, which normally builds this
/// parser lookup. Preserve its native coin -> unified base/collateral mapping;
/// otherwise coin_to_market_id silently assumes USDC (also for unsubscribe ACKs).
pub(in crate::exchanges::ccxt) fn index_market(
    core: &mut ccxt_pro::pro::hyperliquid::HyperliquidCore,
    market: &JsonValue,
) {
    if !market["info"]["dex"]
        .as_str()
        .is_some_and(|dex| !dex.is_empty())
    {
        return;
    }
    let coin = market["baseName"].as_str().expect("validated native coin");
    let tokens = ccxt::runtime::get_value_mut(&mut core.options, &Value::from("hip3TokensByName"));
    if tokens.is_null() {
        *tokens = ccxt::Params::none().into_value_object();
    }
    if !ccxt::value::get_value_k(tokens, coin).is_null() {
        return;
    }
    ccxt::set_value(
        tokens,
        &Value::from(coin),
        ccxt::Params::none()
            .with_str("code", market["base"].as_str().expect("catalog base"))
            .with_str("quote", market["quote"].as_str().expect("catalog quote"))
            .into_value_object(),
    );
}

pub(in crate::exchanges::ccxt) fn subscription_params(
    market: &CatalogMarket,
    channel: RealtimeChannel,
    timeframe: Option<&str>,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    // Pro extends the request root, not the nested subscription. A root dex or
    // coin would not select the HIP3 market. Use the catalog's native coin for
    // both watch and unwatch; retain any book aggregation already prepared.
    if !params["subscription"].is_object() {
        params["subscription"] = json!({});
    }
    let subscription = &mut params["subscription"];
    subscription["coin"] = json!(super::native_coin(market)?);
    subscription["type"] = json!(match channel {
        RealtimeChannel::Trades => "trades",
        RealtimeChannel::OrderBook => "l2Book",
        RealtimeChannel::Ohlcv => "candle",
    });
    if let Some(timeframe) = timeframe {
        subscription["interval"] = json!(timeframe);
    }
    Ok(())
}

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
    _market: &CatalogMarket,
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
    // subscription_params supplies the native coin and channel afterward.
    if let Some(figures) = figures {
        params["subscription"] = json!({"nSigFigs": figures});
        if let Some(mantissa) = mantissa {
            params["subscription"]["mantissa"] = json!(mantissa);
        }
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
