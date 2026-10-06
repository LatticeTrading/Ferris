//! Shared Pro request shapes, without exchange dispatch.
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            stream::{
                params::{string, unsupported},
                LiveChannel, LiveSpec,
            },
            venue::Venue,
        },
        traits::ExchangeError,
    },
    realtime::RealtimeChannel,
};

pub(in crate::exchanges::ccxt) fn candle_params(
    venue: Venue,
    market: &CatalogMarket,
    input: &JsonValue,
    params: &mut JsonValue,
    binance_channels: bool,
) -> Result<(), ExchangeError> {
    if let Some(price) = string(input, &["price"])? {
        if market.raw["contract"] != true || !matches!(price, "mark" | "index") {
            return Err(unsupported(
                venue,
                "live price candles require a contract and mark or index",
            ));
        }
        if binance_channels {
            params["channel"] = json!(if price == "mark" {
                "markPriceKline"
            } else {
                "indexPriceKline"
            });
        } else {
            params["candleType"] = json!(if price == "mark" {
                "mark-prices"
            } else {
                "index-prices"
            });
        }
    }
    if let Some(kind) = string(input, &["candleType"])? {
        if !matches!(kind, "trades" | "mark-prices" | "index-prices") {
            return Err(unsupported(venue, "unsupported candleType"));
        }
        params["candleType"] = json!(kind);
    }
    if params["candleType"] == "trades" {
        params.as_object_mut().unwrap().remove("candleType");
    }
    Ok(())
}

pub(in crate::exchanges::ccxt) fn unsubscribe_hash(spec: &LiveSpec) -> String {
    let prefix = match spec.channel {
        LiveChannel::Client(RealtimeChannel::Trades) => "trade",
        LiveChannel::Client(RealtimeChannel::OrderBook) => "orderbook",
        LiveChannel::Client(RealtimeChannel::Ohlcv) => "ohlcv",
        LiveChannel::Statistics => return format!("unsubscribe:{}", spec.hash),
    };
    match spec.timeframe.as_deref() {
        Some(timeframe) => format!("unsubscribe:{prefix}:{}:{timeframe}", spec.symbol),
        None => format!("unsubscribe:{prefix}:{}", spec.symbol),
    }
}
