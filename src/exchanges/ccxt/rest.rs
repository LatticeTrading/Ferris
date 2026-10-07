//! Ferris request policy around stock unified methods. No native transport or caches.

use ccxt::Value;
use serde_json::Value as JsonValue;

use crate::{
    exchanges::traits::ExchangeError,
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvParams, FetchOrderBookParams,
        FetchTradesParams, UnifiedMarketType,
    },
};

use super::{
    catalog::{catalog_scope, Catalog},
    convert::{convert_book, convert_candles, convert_trades},
    venue::{exchange_error, CatalogScope, Provider, Venue},
    venues,
};

pub(in crate::exchanges::ccxt) mod params;
use params::{
    bounded_limit, check_time_range, integer_param, string_param, timestamp, unsupported, within,
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
        Ok(venue.data_scope(scope))
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
            Self::Trades(_) if venues::dispatch!(venue, exchange => exchange::rest::PUBLIC_TRADES) => {
                None
            }
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
            if scope == CatalogScope::Option
                && !venues::dispatch!(venue, exchange => exchange::rest::OPTION_CANDLES)
            {
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
            "until" | "endTime" => matches!(operation, Operation::Trades | Operation::Ohlcv),
            "timeframe" | "interval" => operation == Operation::Ohlcv,
            "levels" | "depth" | "limit" => operation == Operation::Book,
            _ => venues::dispatch!(venue, exchange => exchange::rest::allows(operation, key)),
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
            let max = venues::dispatch!(venue, exchange => exchange::rest::trades_max(market));
            let limit = bounded_limit(request.limit, 100, max)?;
            let since = timestamp(request.since, "since")?;
            let until = end_time(venue, &request.params)?;
            check_time_range(since, until)?;
            let params = venues::dispatch!(venue, exchange => exchange::rest::trade_params(until));
            let raw = venues::dispatch!(venue, exchange => exchange::rest::trades(provider, market, since, limit as i64, params).await)?;
            let mut trades = convert_trades(venue, raw, &market.ccxt_symbol)?;
            trades.retain(|trade| within(trade.timestamp, since, until));
            trades.sort_unstable_by(|a, b| b.timestamp.cmp(&a.timestamp));
            trades.truncate(limit);
            Ok(SnapshotResponse::Trades(trades))
        }
        SnapshotRequest::Ohlcv(request) => {
            let market = catalog.resolve(&request.symbol, &request.params)?;
            if market.market.market_type == UnifiedMarketType::Option
                && !venues::dispatch!(venue, exchange => exchange::rest::OPTION_CANDLES)
            {
                return Err(unsupported(venue, "option candles are not supported"));
            }
            let max = venues::dispatch!(venue, exchange => exchange::rest::ohlcv_max(market));
            let limit = bounded_limit(request.limit, 200, max)?;
            let since = timestamp(request.since, "since")?;
            let timeframe = request.timeframe.as_deref().expect("prepared timeframe");
            let mut until = end_time(venue, &request.params)?;
            if until.is_none()
                && venues::dispatch!(venue, exchange => exchange::rest::BOUND_CANDLES)
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
            let mut params = venues::dispatch!(venue, exchange => exchange::rest::candle_params(market, &request.params))?;
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
            let max = venues::dispatch!(venue, exchange => exchange::rest::book_max(market));
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
            let raw = venues::dispatch!(venue, exchange => exchange::rest::book(provider, market, requested, &request.params).await)?;
            let mut book = convert_book(raw, &market.ccxt_symbol)?;
            book.bids.truncate(requested as usize);
            book.asks.truncate(requested as usize);
            Ok(SnapshotResponse::Book(book))
        }
    }
}

fn end_time(venue: Venue, params: &JsonValue) -> Result<Option<i64>, ExchangeError> {
    let keys = venues::dispatch!(venue, exchange => exchange::rest::END_KEYS);
    timestamp(integer_param(params, keys)?, "end time")
}
