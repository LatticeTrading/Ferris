//! Hyperliquid REST request policy and stock-method exceptions.

use ccxt::{Params, Value};
use serde_json::{json, Value as JsonValue};

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            rest::{params::integer_param, Operation},
            venue::{exchange_error, Provider, Venue},
            venues::shared_rest,
        },
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
};

const VENUE: Venue = Venue::Hyperliquid;
pub(in crate::exchanges::ccxt) const PUBLIC_TRADES: bool = true;
pub(in crate::exchanges::ccxt) const OPTION_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const BOUND_CANDLES: bool = true;
pub(in crate::exchanges::ccxt) const END_KEYS: &[&str] = &["until", "endTime"];

pub(in crate::exchanges::ccxt) fn allows(operation: Operation, key: &str) -> bool {
    match key {
        "coin" => operation != Operation::Markets,
        "dex" => true,
        "nSigFigs" | "mantissa" => operation == Operation::Book,
        _ => false,
    }
}

pub(in crate::exchanges::ccxt) fn trades_max(_market: &CatalogMarket) -> usize {
    5_000
}

pub(in crate::exchanges::ccxt) fn ohlcv_max(_market: &CatalogMarket) -> usize {
    5_000
}

pub(in crate::exchanges::ccxt) fn book_max(_market: &CatalogMarket) -> u64 {
    20
}

pub(in crate::exchanges::ccxt) fn trade_params(_until: Option<i64>) -> Params {
    Params::none()
}

pub(in crate::exchanges::ccxt) async fn trades(
    provider: &mut Provider,
    market: &CatalogMarket,
    _since: Option<i64>,
    _limit: i64,
    _params: Params,
) -> Result<Value, ExchangeError> {
    // Unified fetch_trades is userFills, not public prints.
    let coin = if market.market.market_type == UnifiedMarketType::Perp {
        market.raw.get("baseName")
    } else {
        market.raw.get("id")
    }
    .and_then(JsonValue::as_str)
    .ok_or_else(|| ExchangeError::UpstreamData("missing stock trade coin".into()))?;
    let rows = provider
        .call(
            "public_post_info",
            vec![Value::from_json(
                &json!({"type":"recentTrades", "coin":coin}),
            )],
        )
        .await
        .map_err(|error| exchange_error(VENUE, error))?;
    shared_rest::parse_trades(VENUE, provider, market, rows).await
}

pub(in crate::exchanges::ccxt) fn candle_params(
    market: &CatalogMarket,
    input: &JsonValue,
) -> Result<Params, ExchangeError> {
    let params = shared_rest::candle_price(VENUE, market, input, false, false)?;
    Ok(params)
}

pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    input: &JsonValue,
) -> Result<Value, ExchangeError> {
    let mut params = Params::none();
    let figures = integer_param(input, &["nSigFigs"])?;
    if let Some(figures) = figures {
        if !(2..=5).contains(&figures) {
            return Err(ExchangeError::BadSymbol(
                "nSigFigs must be between 2 and 5".into(),
            ));
        }
        params = params.with_int("nSigFigs", figures as i64);
    }
    if let Some(mantissa) = integer_param(input, &["mantissa"])? {
        if figures != Some(5) || !matches!(mantissa, 1 | 2 | 5) {
            return Err(ExchangeError::BadSymbol(
                "mantissa requires nSigFigs=5 and must be 1, 2, or 5".into(),
            ));
        }
        params = params.with_int("mantissa", mantissa as i64);
    }
    shared_rest::book(VENUE, provider, market, limit, params).await
}
