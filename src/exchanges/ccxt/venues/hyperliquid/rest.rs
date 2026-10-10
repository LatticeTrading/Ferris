//! Hyperliquid REST request policy and stock-method exceptions.

use ccxt::{Params, Value};
use serde_json::{json, Value as JsonValue};

use crate::exchanges::{
    ccxt::{
        catalog::CatalogMarket,
        rest::{params::integer_param, Operation},
        venue::{exchange_error, Provider, Venue},
        venues::shared_rest,
    },
    traits::ExchangeError,
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
    // Unified fetch_trades is userFills, not public prints; recentTrades is the
    // public tape. It takes only a coin and returns the newest prints, so the
    // caller's since/until window is applied by the shared snapshot filter and
    // the coin must be the catalog's native handle, never display text.
    let coin = super::native_coin(market)?;
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
    _input: &JsonValue,
) -> Result<Params, ExchangeError> {
    // Hyperliquid's candleSnapshot `req` accepts only coin/interval/startTime/
    // endTime; there is no price-source selector, and `allows` rejects `price`
    // before this point. The coin and window are built by the stock method from
    // the same native handle this validates, so a catalog row that no longer
    // agrees with its loaded stock metadata is rejected before any request.
    super::native_coin(market)?;
    Ok(Params::none())
}

pub(in crate::exchanges::ccxt) async fn book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    input: &JsonValue,
) -> Result<Value, ExchangeError> {
    // Stock builds the l2Book coin from `baseName` (perps) or the market id
    // (spot); reject a catalog row that disagrees before requesting.
    super::native_coin(market)?;
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
