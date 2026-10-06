//! Shared stock request and parser paths used by exchange policies.

use ccxt::{Params, Value};
use serde_json::Value as JsonValue;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            rest::params::{string_param, unsupported},
            venue::{exchange_error, Provider, Venue},
        },
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
};

pub(in crate::exchanges::ccxt) async fn trades(
    venue: Venue,
    provider: &mut Provider,
    market: &CatalogMarket,
    since: Option<i64>,
    limit: i64,
    params: Params,
) -> Result<Value, ExchangeError> {
    provider
        .call(
            "fetch_trades",
            vec![
                Value::from(market.ccxt_symbol.as_str()),
                since.map(Value::Int).unwrap_or(Value::Null),
                Value::Int(limit),
                params.into_value_object(),
            ],
        )
        .await
        .map_err(|error| exchange_error(venue, error))
}

pub(in crate::exchanges::ccxt) async fn parse_trades(
    venue: Venue,
    provider: &mut Provider,
    market: &CatalogMarket,
    rows: Value,
) -> Result<Value, ExchangeError> {
    let Value::Arr(rows) = rows else {
        return Err(ExchangeError::UpstreamData(
            "stock public trades response is not an array".into(),
        ));
    };
    let raw_market = Value::from_json(&market.raw);
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        // Stock parser owns side, units, fees and raw-info semantics.
        parsed.push(
            provider
                .call("parse_trade", vec![row.clone(), raw_market.clone()])
                .await
                .map_err(|error| exchange_error(venue, error))?,
        );
    }
    Ok(Value::Arr(std::sync::Arc::new(parsed)))
}

pub(in crate::exchanges::ccxt) async fn book(
    venue: Venue,
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    params: Params,
) -> Result<Value, ExchangeError> {
    provider
        .call(
            "fetch_order_book",
            vec![
                Value::from(market.ccxt_symbol.as_str()),
                Value::Int(limit as i64),
                params.into_value_object(),
            ],
        )
        .await
        .map_err(|error| exchange_error(venue, error))
}

pub(in crate::exchanges::ccxt) fn candle_price(
    venue: Venue,
    market: &CatalogMarket,
    input: &JsonValue,
    premium: bool,
    premium_linear_only: bool,
) -> Result<Params, ExchangeError> {
    let mut params = Params::none();
    if let Some(price) = string_param(input, &["price"])? {
        if !matches!(price, "mark" | "index" | "premiumIndex") {
            return Err(unsupported(
                venue,
                "price must be mark, index, or premiumIndex",
            ));
        }
        if !matches!(
            market.market.market_type,
            UnifiedMarketType::Perp | UnifiedMarketType::Future
        ) || (price == "premiumIndex"
            && (!premium || (premium_linear_only && market.linear != Some(true))))
        {
            return Err(unsupported(
                venue,
                "requested candle price source is not supported for this product",
            ));
        }
        params = params.with_str("price", price);
    }
    Ok(params)
}
