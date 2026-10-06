//! Panic-isolated stock calls and source-indexed acquisition results.

use std::{collections::HashMap, panic::AssertUnwindSafe};

use ccxt::{Params, Value};
use futures_util::FutureExt;
use tokio::time::Instant;

use crate::{
    exchanges::{
        ccxt::{
            catalog::CatalogMarket,
            venue::{exchange_error, Provider, Venue},
        },
        traits::ExchangeError,
    },
    models::{MarketStatsField, MarketStatsSourceFailure},
};

use super::{
    failure, failure_reason,
    fields::oi_field,
    index::{index_intervals, index_tickers},
    Receipt,
};

#[derive(Default)]
pub(in crate::exchanges::ccxt) struct Acquired {
    pub rows: HashMap<&'static str, Result<HashMap<String, Value>, ExchangeError>>,
    pub singles: HashMap<(&'static str, String), (Result<Value, ExchangeError>, Receipt)>,
    pub intervals: Option<HashMap<String, u64>>,
    pub receipts: HashMap<&'static str, Receipt>,
    pub any_bulk_ok: bool,
}

#[derive(Default)]
pub(in crate::exchanges::ccxt) struct SourceRow<'a> {
    pub row: Option<&'a Value>,
    pub error: Option<&'static str>,
    pub receipt: Option<Receipt>,
}

impl Acquired {
    pub fn row(&self, source: &'static str, entry: &CatalogMarket) -> SourceRow<'_> {
        SourceRow {
            row: lookup(self.rows.get(source), entry),
            error: self
                .rows
                .get(source)
                .and_then(|result| result.as_ref().err().map(failure_reason)),
            receipt: self.receipts.get(source).copied(),
        }
    }

    pub fn single(
        &self,
        source: &'static str,
        key: &str,
    ) -> Option<&(Result<Value, ExchangeError>, Receipt)> {
        self.singles.get(&(source, key.to_string()))
    }

    pub fn open_interest(
        &self,
        source: &'static str,
        entry: &CatalogMarket,
    ) -> (MarketStatsField, Option<Instant>) {
        let value = entry
            .market
            .identity
            .as_ref()
            .and_then(|id| self.single(source, &id.market_id));
        oi_field(
            source,
            value.map(|(result, _)| result),
            value.map(|(_, receipt)| *receipt),
        )
    }

    pub fn interval(&self, entry: &CatalogMarket) -> Option<u64> {
        let native = entry
            .market
            .identity
            .as_ref()
            .map(|id| id.exchange_market_id.as_str())
            .unwrap_or_default();
        self.intervals.as_ref().and_then(|map| {
            map.get(native)
                .or_else(|| map.get(&entry.ccxt_symbol))
                .copied()
        })
    }
}

fn lookup<'a>(
    source: Option<&'a Result<HashMap<String, Value>, ExchangeError>>,
    entry: &CatalogMarket,
) -> Option<&'a Value> {
    let map = source?.as_ref().ok()?;
    if let Some(value) = map.get(&entry.ccxt_symbol) {
        return Some(value);
    }
    if let Some(identity) = entry.market.identity.as_ref() {
        if let Some(value) = map.get(&identity.exchange_market_id) {
            return Some(value);
        }
    }
    entry
        .market
        .info
        .raw_symbol
        .as_ref()
        .and_then(|raw| map.get(raw))
}

pub(in crate::exchanges::ccxt) async fn run_call(
    provider: &mut Provider,
    venue: Venue,
    method: &str,
    args: Vec<Value>,
) -> Result<Value, ExchangeError> {
    AssertUnwindSafe(provider.call(method, args))
        .catch_unwind()
        .await
        .map_err(|panic| {
            ExchangeError::UpstreamRequest(format!(
                "{} CCXT {method} panicked: {}",
                venue.public_id(),
                panic_message(panic)
            ))
        })?
        .map_err(|error| exchange_error(venue, error))
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

pub(in crate::exchanges::ccxt) fn finish(
    venue: Venue,
    source: &'static str,
    result: Result<Value, ExchangeError>,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) -> Result<HashMap<String, Value>, ExchangeError> {
    match result.and_then(|value| index_tickers(venue, &value)) {
        Ok(map) => {
            acquired.any_bulk_ok = true;
            acquired.receipts.insert(source, Receipt::now());
            Ok(map)
        }
        Err(error) => {
            failures.push(failure(source, &error));
            Err(error)
        }
    }
}

pub(in crate::exchanges::ccxt) fn finish_intervals(
    venue: Venue,
    source: &'static str,
    hours_key: &str,
    result: Result<Value, ExchangeError>,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    match result.and_then(|value| index_intervals(venue, &value, hours_key)) {
        Ok(map) => {
            acquired.any_bulk_ok = true;
            acquired.receipts.insert(source, Receipt::now());
            acquired.intervals = Some(map);
        }
        Err(error) => failures.push(failure(source, &error)),
    }
}

pub(in crate::exchanges::ccxt) async fn acquire_tickers(
    venue: Venue,
    source: &'static str,
    provider: &mut Provider,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let result = run_call(
        provider,
        venue,
        "fetch_tickers",
        vec![Value::Null, Params::none().into_value_object()],
    )
    .await;
    let rows = finish(venue, source, result, acquired, failures);
    acquired.rows.insert(source, rows);
}
