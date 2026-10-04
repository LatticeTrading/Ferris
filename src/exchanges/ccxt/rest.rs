//! Ferris request policy around stock unified methods. No native transport or caches.

use ccxt::{Params, Value};
use serde_json::Value as JsonValue;

use crate::{
    exchanges::traits::ExchangeError,
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams, UnifiedMarketType,
    },
};

use super::{
    catalog::{catalog_scope, Catalog, CatalogMarket},
    convert::{convert_book, convert_candles, convert_trades},
    venue::{exchange_error, CatalogScope, Provider, Venue},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Markets,
    Trades,
    Ohlcv,
    Book,
}

pub(super) enum SnapshotRequest {
    Trades(FetchTradesParams),
    Ohlcv(FetchOhlcvParams),
    Book(FetchOrderBookParams),
}

pub(super) enum SnapshotResponse {
    Trades(Vec<CcxtTrade>),
    Ohlcv(Vec<CcxtOhlcv>),
    Book(CcxtOrderBook),
}

impl SnapshotRequest {
    fn operation(&self) -> Operation {
        match self {
            Self::Trades(_) => Operation::Trades,
            Self::Ohlcv(_) => Operation::Ohlcv,
            Self::Book(_) => Operation::Book,
        }
    }

    fn params(&self) -> &JsonValue {
        match self {
            Self::Trades(request) => &request.params,
            Self::Ohlcv(request) => &request.params,
            Self::Book(request) => &request.params,
        }
    }

    pub(super) fn scope(&self, venue: Venue) -> Result<CatalogScope, ExchangeError> {
        validate_params(venue, self.operation(), self.params())?;
        let scope = catalog_scope(venue, self.params())?;
        Ok(if venue == Venue::Bybit && scope == CatalogScope::Default {
            CatalogScope::Linear
        } else {
            scope
        })
    }

    /// Check source-declared support before acquiring metadata. Keep the normalized
    /// timeframe in the owned request so aliases are resolved only once.
    pub(super) fn prepare(
        &mut self,
        venue: Venue,
        scope: CatalogScope,
        provider: &Provider,
    ) -> Result<(), ExchangeError> {
        let capability = match self {
            Self::Trades(_) if matches!(venue, Venue::Hyperliquid | Venue::Lighter) => None,
            Self::Trades(_) => Some("fetchTrades"),
            Self::Ohlcv(_) => Some("fetchOHLCV"),
            Self::Book(_) => Some("fetchOrderBook"),
        };
        if capability.is_some_and(|capability| !provider.supports(capability)) {
            return Err(unsupported(
                venue,
                "operation is absent from this CCXT release",
            ));
        }
        if let Self::Ohlcv(request) = self {
            if venue == Venue::Bybit && scope == CatalogScope::Option {
                return Err(unsupported(venue, "option candles are not supported"));
            }
            let input = match request.timeframe.take() {
                Some(timeframe) => timeframe,
                None => string_param(&request.params, &["timeframe", "interval"])?
                    .unwrap_or("1m")
                    .to_string(),
            };
            let trimmed = input.trim();
            let timeframe = if provider.has_timeframe(trimmed) {
                if trimmed.len() == input.len() {
                    input
                } else {
                    trimmed.to_string()
                }
            } else {
                provider.timeframe_alias(trimmed).ok_or_else(|| {
                    unsupported(venue, &format!("unsupported timeframe `{trimmed}`"))
                })?
            };
            request.timeframe = Some(timeframe);
        }
        Ok(())
    }
}

pub(super) fn validate_params(
    venue: Venue,
    operation: Operation,
    params: &JsonValue,
) -> Result<(), ExchangeError> {
    let Some(params) = params.as_object() else {
        return if params.is_null() {
            Ok(())
        } else {
            Err(ExchangeError::BadSymbol(
                "`params` must be an object or null".into(),
            ))
        };
    };
    for key in params.keys().map(String::as_str) {
        let allowed = match key {
            "type" | "category" | "subType" | "settle" => true,
            "dex" => venue == Venue::Hyperliquid,
            "coin" => operation != Operation::Markets && venue != Venue::Lighter,
            "market_id" | "marketId" => operation != Operation::Markets && venue == Venue::Lighter,
            "until" | "endTime" => matches!(operation, Operation::Trades | Operation::Ohlcv),
            "endTimestamp" | "end_timestamp" => {
                operation == Operation::Ohlcv && venue == Venue::Lighter
            }
            "timeframe" | "interval" => operation == Operation::Ohlcv,
            "price" => {
                operation == Operation::Ohlcv
                    && matches!(
                        venue,
                        Venue::Binance | Venue::Bybit | Venue::Aster | Venue::Extended
                    )
            }
            "candleType" => operation == Operation::Ohlcv && venue == Venue::Extended,
            "set_timestamp_to_end" | "setTimestampToEnd" => {
                operation == Operation::Ohlcv && venue == Venue::Lighter
            }
            "levels" | "depth" | "limit" => operation == Operation::Book,
            "nSigFigs" | "mantissa" => operation == Operation::Book && venue == Venue::Hyperliquid,
            "rpi" => operation == Operation::Book && matches!(venue, Venue::Binance | Venue::Bybit),
            _ => false,
        };
        if !allowed {
            return Err(unsupported(
                venue,
                &format!("parameter `{key}` is not supported for {operation:?}"),
            ));
        }
    }
    Ok(())
}

pub(super) async fn fetch_snapshot(
    venue: Venue,
    provider: &mut Provider,
    catalog: &Catalog,
    request: SnapshotRequest,
) -> Result<SnapshotResponse, ExchangeError> {
    match request {
        SnapshotRequest::Trades(request) => {
            let market = catalog.resolve(&request.symbol, &request.params)?;
            let max = match venue {
                Venue::Lighter => 100,
                Venue::Hyperliquid => 5_000,
                Venue::Bybit if market.market.market_type == UnifiedMarketType::Spot => 60,
                _ => 1_000,
            };
            let limit = bounded_limit(request.limit, 100, max)?;
            let since = timestamp(request.since, "since")?;
            let until = end_time(venue, &request.params)?;
            check_time_range(since, until)?;
            let mut params = Params::none();
            if let Some(until) = until {
                match venue {
                    Venue::Binance => params = params.with_int("until", until),
                    // Stock Aster's `until` branch assigns a tuple to its request.
                    // Its supported native endTime parameter avoids that branch.
                    Venue::Aster => params = params.with_int("endTime", until),
                    _ => {} // Public recent-trade windows have no historical upper bound.
                }
            }
            let raw = provider
                .public_trades(venue, market, since, limit as i64, params)
                .await?;
            let mut trades = convert_trades(raw, &market.ccxt_symbol)?;
            trades.retain(|trade| within(trade.timestamp, since, until));
            trades.sort_unstable_by(|a, b| b.timestamp.cmp(&a.timestamp));
            trades.truncate(limit);
            Ok(SnapshotResponse::Trades(trades))
        }
        SnapshotRequest::Ohlcv(request) => {
            let market = catalog.resolve(&request.symbol, &request.params)?;
            if venue == Venue::Bybit && market.market.market_type == UnifiedMarketType::Option {
                return Err(unsupported(venue, "option candles are not supported"));
            }
            let max = match venue {
                Venue::Hyperliquid => 5_000,
                Venue::Lighter => 500,
                Venue::Aster => 1_500,
                Venue::Extended => 10_000,
                _ => 1_000,
            };
            let limit = bounded_limit(request.limit, 200, max)?;
            let since = timestamp(request.since, "since")?;
            let timeframe = request.timeframe.as_deref().expect("prepared timeframe");
            let mut until = end_time(venue, &request.params)?;
            if until.is_none()
                && matches!(venue, Venue::Hyperliquid | Venue::Lighter | Venue::Extended)
            {
                if let Some(since) = since {
                    let duration = provider.timeframe_millis(timeframe).ok_or_else(|| {
                        ExchangeError::UpstreamData("invalid stock candle duration".into())
                    })?;
                    until = Some(
                        duration
                            .checked_mul(limit as i64)
                            .and_then(|span| since.checked_add(span))
                            .ok_or_else(|| {
                                ExchangeError::BadSymbol("candle time range is too large".into())
                            })?,
                    );
                }
            }
            check_time_range(since, until)?;
            let mut params = candle_params(venue, market, &request.params)?;
            if let Some(until) = until {
                params = params.with_int("until", until);
            }
            let raw = provider
                .call(
                    "fetch_ohlcv",
                    vec![
                        Value::from(market.ccxt_symbol.as_str()),
                        Value::from(timeframe),
                        since.map(Value::Int).unwrap_or_default(),
                        Value::Int(limit as i64),
                        params.into_value_object(),
                    ],
                )
                .await
                .map_err(|error| exchange_error(venue, error))?;
            let mut candles = convert_candles(raw)?;
            candles.retain(|candle| within(Some(candle.0), since, until));
            if since.is_none() && candles.len() > limit {
                candles.drain(..candles.len() - limit);
            } else {
                candles.truncate(limit);
            }
            Ok(SnapshotResponse::Ohlcv(candles))
        }
        SnapshotRequest::Book(request) => {
            let market = catalog.resolve(&request.symbol, &request.params)?;
            let max = match venue {
                Venue::Hyperliquid => 20,
                Venue::Lighter => 100,
                Venue::Bybit if market.market.market_type == UnifiedMarketType::Option => 25,
                Venue::Bybit => 10_000,
                Venue::Binance if market.market.market_type == UnifiedMarketType::Spot => 5_000,
                _ => 1_000,
            };
            let requested = match request.limit {
                Some(limit) => limit as u64,
                None => integer_param(&request.params, &["levels", "depth", "limit"])?
                    .unwrap_or(100.min(max)),
            };
            if requested == 0 {
                return Err(ExchangeError::BadSymbol(
                    "book depth must be positive".into(),
                ));
            }
            if requested > max {
                return Err(unsupported(
                    venue,
                    &format!("stock REST book depth is limited to {max}"),
                ));
            }
            let upstream_limit = if venue == Venue::Binance
                && market.market.market_type != UnifiedMarketType::Spot
            {
                [5, 10, 20, 50, 100, 500, 1_000]
                    .into_iter()
                    .find(|limit| *limit >= requested)
                    .expect("bounded depth")
            } else {
                requested
            };
            let params = book_params(venue, market, &request.params)?;
            let bybit_rpi =
                venue == Venue::Bybit && bool_param(&request.params, &["rpi"])? == Some(true);
            let bybit_regular_max = if market.market.market_type == UnifiedMarketType::Spot {
                200
            } else {
                1_000
            };
            let raw = if venue == Venue::Bybit && (requested > bybit_regular_max || bybit_rpi) {
                fetch_bybit_special_book(provider, market, requested, bybit_rpi).await?
            } else {
                provider
                    .call(
                        "fetch_order_book",
                        vec![
                            Value::from(market.ccxt_symbol.as_str()),
                            Value::Int(upstream_limit as i64),
                            params.into_value_object(),
                        ],
                    )
                    .await
                    .map_err(|error| exchange_error(venue, error))?
            };
            let mut book = convert_book(raw, &market.ccxt_symbol)?;
            book.bids.truncate(requested as usize);
            book.asks.truncate(requested as usize);
            Ok(SnapshotResponse::Book(book))
        }
    }
}

async fn fetch_bybit_special_book(
    provider: &mut Provider,
    market: &CatalogMarket,
    limit: u64,
    rpi: bool,
) -> Result<Value, ExchangeError> {
    let category = market
        .market
        .info
        .category
        .as_deref()
        .ok_or_else(|| ExchangeError::UpstreamData("missing Bybit market category".into()))?;
    let native_id = market
        .raw
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ExchangeError::UpstreamData("missing Bybit market id".into()))?;
    let mut params = Params::none()
        .with_str("category", category)
        .with_str("symbol", native_id);
    let method = if rpi {
        if !matches!(category, "linear" | "inverse") || limit > 1_000 {
            return Err(unsupported(
                Venue::Bybit,
                "RPI books require a contract and at most 1000 levels",
            ));
        }
        params = params.with_int("limit", limit as i64);
        "public_get_v5_market_rpi_orderbook"
    } else {
        "public_get_v5_market_full_orderbook"
    };
    let response = provider
        .call(method, vec![params.into_value_object()])
        .await
        .map_err(|error| exchange_error(Venue::Bybit, error))?;
    let result = ccxt::value::get_value_k(&response, "result");
    let row = result
        .as_map()
        .filter(|row| {
            row.get("b").and_then(Value::as_array).is_some()
                && row.get("a").and_then(Value::as_array).is_some()
        })
        .ok_or_else(|| ExchangeError::UpstreamData("invalid Bybit book response".into()))?;
    let timestamp = row.get("ts").cloned().unwrap_or_default();
    provider
        .call(
            "parse_order_book",
            vec![
                result,
                Value::from(market.ccxt_symbol.as_str()),
                timestamp,
                Value::from("b"),
                Value::from("a"),
            ],
        )
        .await
        .map_err(|error| exchange_error(Venue::Bybit, error))
}

fn candle_params(
    venue: Venue,
    market: &CatalogMarket,
    input: &JsonValue,
) -> Result<Params, ExchangeError> {
    let mut params = Params::none();
    let price = string_param(input, &["price"])?;
    if let Some(price) = price {
        if !matches!(price, "mark" | "index" | "premiumIndex") {
            return Err(unsupported(
                venue,
                "price must be mark, index, or premiumIndex",
            ));
        }
        if !matches!(
            market.market.market_type,
            UnifiedMarketType::Perp | UnifiedMarketType::Future
        ) || (price == "premiumIndex" && !matches!(venue, Venue::Binance | Venue::Bybit))
            || (venue == Venue::Bybit && price == "premiumIndex" && market.linear != Some(true))
        {
            return Err(unsupported(
                venue,
                "requested candle price source is not supported for this product",
            ));
        }
        params = params.with_str("price", price);
    }
    if venue == Venue::Extended {
        if let Some(kind) = string_param(input, &["candleType"])? {
            if !matches!(kind, "trades" | "mark-prices" | "index-prices") {
                return Err(unsupported(venue, "unsupported candleType"));
            }
            // The explicit Extended candleType takes precedence over price in stock CCXT.
            params = params.with_str("candleType", kind);
        }
    }
    if venue == Venue::Lighter {
        if let Some(value) = bool_param(input, &["set_timestamp_to_end", "setTimestampToEnd"])? {
            params = params.with_bool("set_timestamp_to_end", value);
        }
    }
    Ok(params)
}

fn book_params(
    venue: Venue,
    market: &CatalogMarket,
    input: &JsonValue,
) -> Result<Params, ExchangeError> {
    let mut params = Params::none();
    if venue == Venue::Hyperliquid {
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
    }
    if venue == Venue::Binance {
        if let Some(rpi) = bool_param(input, &["rpi"])? {
            if rpi && market.linear != Some(true) {
                return Err(unsupported(venue, "RPI books require a linear contract"));
            }
            params = params.with_bool("rpi", rpi);
        }
    }
    Ok(params)
}

fn bounded_limit(limit: Option<usize>, default: usize, max: usize) -> Result<usize, ExchangeError> {
    if limit == Some(0) {
        return Err(ExchangeError::BadSymbol("limit must be positive".into()));
    }
    Ok(limit.unwrap_or(default).min(max))
}

fn end_time(venue: Venue, params: &JsonValue) -> Result<Option<i64>, ExchangeError> {
    let keys: &[&str] = match venue {
        Venue::Extended => &["endTime", "until"],
        Venue::Lighter => &["until", "endTimestamp", "end_timestamp", "endTime"],
        _ => &["until", "endTime"],
    };
    timestamp(integer_param(params, keys)?, "end time")
}

fn timestamp(value: Option<u64>, field: &str) -> Result<Option<i64>, ExchangeError> {
    value
        .map(|value| {
            i64::try_from(value)
                .map_err(|_| ExchangeError::BadSymbol(format!("{field} is too large")))
        })
        .transpose()
}

fn check_time_range(since: Option<i64>, until: Option<i64>) -> Result<(), ExchangeError> {
    if matches!((since, until), (Some(start), Some(end)) if start > end) {
        return Err(ExchangeError::BadSymbol("end time precedes since".into()));
    }
    Ok(())
}

fn within(time: Option<u64>, since: Option<i64>, until: Option<i64>) -> bool {
    since.is_none_or(|since| time.is_some_and(|time| time >= since as u64))
        && until.is_none_or(|until| time.is_some_and(|time| time <= until as u64))
}

fn integer_param(params: &JsonValue, keys: &[&str]) -> Result<Option<u64>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_u64()
                .or_else(|| value.as_str()?.parse().ok())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("`{key}` must be a nonnegative integer"))
                });
        }
    }
    Ok(None)
}

fn string_param<'a>(
    params: &'a JsonValue,
    keys: &[&str],
) -> Result<Option<&'a str>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(Some)
                .ok_or_else(|| {
                    ExchangeError::BadSymbol(format!("`{key}` must be a nonempty string"))
                });
        }
    }
    Ok(None)
}

fn bool_param(params: &JsonValue, keys: &[&str]) -> Result<Option<bool>, ExchangeError> {
    for key in keys {
        if let Some(value) = params.get(key).filter(|value| !value.is_null()) {
            return value
                .as_bool()
                .map(Some)
                .ok_or_else(|| ExchangeError::BadSymbol(format!("`{key}` must be a boolean")));
        }
    }
    Ok(None)
}

fn unsupported(venue: Venue, message: &str) -> ExchangeError {
    ExchangeError::UnsupportedFeature(format!("{}: {message}", venue.public_id()))
}
