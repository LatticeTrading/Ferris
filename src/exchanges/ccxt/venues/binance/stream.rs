//! Binance Pro subscription policy, hashes and URL selection.

use ccxt::Value;
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{
                params::{field, unsupported},
                LiveChannel, LiveSpec,
            },
            venue::Venue,
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

pub(in crate::exchanges::ccxt) use crate::exchanges::ccxt::stream::control::stock_unsubscribe as unsubscribe_mode;

const VENUE: Venue = Venue::Binance;
pub(in crate::exchanges::ccxt) const CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const UNWATCH_BOOK_LIMIT: bool = false;

pub(in crate::exchanges::ccxt) fn allows(channel: RealtimeChannel, key: &str) -> bool {
    match key {
        "coin" => true,
        "rpi" => channel == RealtimeChannel::OrderBook,
        "name" => channel == RealtimeChannel::Trades,
        "price" => channel == RealtimeChannel::Ohlcv,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn book_depth(
    market: &CatalogMarket,
) -> (usize, usize, Option<usize>) {
    if market.raw["contract"] != true {
        (20, 5_000, Some(5_000))
    } else {
        (20, 1_000, Some(1_000))
    }
}

pub(in crate::exchanges::ccxt) fn book_params(
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    if let Some(rpi) = input.get("rpi").filter(|v| !v.is_null()) {
        let rpi = rpi
            .as_bool()
            .ok_or_else(|| ExchangeError::BadSymbol("rpi must be a boolean".into()))?;
        if rpi && market.linear != Some(true) {
            return Err(unsupported(VENUE, "RPI books require a linear contract"));
        }
        if rpi {
            params["rpi"] = json!(true);
        }
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
) -> Result<(), ExchangeError> {
    super::super::shared_stream::candle_params(VENUE, market, input, params, true)
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
        LiveChannel::Client(RealtimeChannel::Ohlcv) => format!(
            "ohlcv::{}::{}",
            spec.symbol,
            spec.timeframe.as_deref().unwrap()
        ),
    }
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    super::super::shared_stream::unsubscribe_hash(spec)
}

pub(in crate::exchanges::ccxt) async fn url(
    core: &mut ccxt_pro::pro::binance::BinanceCore,
    spec: &LiveSpec,
) -> Result<Value, ExchangeError> {
    Ok({
        let kind = binance_kind(spec);
        let name = match spec.channel {
            LiveChannel::Client(RealtimeChannel::OrderBook) => {
                if spec.params["rpi"] == true {
                    "rpiDepth"
                } else {
                    "depth"
                }
            }
            LiveChannel::Client(RealtimeChannel::Trades) => {
                spec.params["name"].as_str().unwrap_or("trade")
            }
            LiveChannel::Client(RealtimeChannel::Ohlcv) => {
                spec.params["channel"].as_str().unwrap_or("kline")
            }
            LiveChannel::Statistics => unreachable!("statistics is Lighter-only"),
        };
        core.get_ws_url(
            Value::from(kind),
            core.get_future_ws_category(Value::from(name)),
        )
    })
}

fn binance_kind(spec: &LiveSpec) -> &'static str {
    if spec.market["option"] == true {
        if spec.channel == LiveChannel::Client(RealtimeChannel::Ohlcv) {
            "optionMarket"
        } else {
            "option"
        }
    } else if spec.market["contract"] == true {
        if spec.market["linear"] == true {
            "future"
        } else {
            "delivery"
        }
    } else {
        "spot"
    }
}

pub(in crate::exchanges::ccxt) fn slots(
    core: &ccxt_pro::pro::binance::BinanceCore,
    spec: &LiveSpec,
) -> Result<Option<usize>, ExchangeError> {
    let slots = field(&core.options, &["streamLimits", binance_kind(spec)])
        .as_i64()
        .and_then(|slots| usize::try_from(slots).ok());
    if !slots.is_some_and(|slots| slots > 0) {
        return Err(ExchangeError::Internal(
            "invalid stock Binance stream allocator".into(),
        ));
    }
    Ok(slots)
}
