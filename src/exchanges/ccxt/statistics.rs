//! Stock-CCXT statistics acquisition.
//!
//! One owner-thread call per venue builds an owned [`MarketStatsSourceSnapshot`]
//! from the already-loaded [`CatalogSnapshot`]. Nothing here owns transport,
//! caches, or a second metadata load: the catalog supplies identity, and only
//! stock unified/implicit methods acquire values.
//!
//! Provenance is truthful: each field carries the literal stock method that
//! produced it (`{public_id}:ccxt:{method}`), not the retired native endpoint.
//! Funding/price strings keep the raw upstream spelling; the numeric
//! `volume24h`/`openInterest` variants are finite `f64`s (checked before any
//! JSON conversion can erase NaN/infinity). Fields this poll deliberately did
//! not acquire are `unavailable`/`not-requested` with no receipt, so projection
//! retains the previous observation without freshening it. Every stock call is
//! panic-isolated so one failing method keeps its healthy siblings.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::AssertUnwindSafe;

use ccxt::value::get_value_k;
use ccxt::{Params, Value};
use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use crate::{
    exchanges::traits::ExchangeError,
    market_stats::MarketStatsSourceSnapshot,
    models::{
        CapabilityState, FetchMarketStatsParams, FundingKind, FundingRateUnit, FundingValue,
        MarketStatsField, MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow,
        MarketStatsSourceFailure, MarketStatsValue, OpenInterestValue, PriceValue,
        UnifiedMarketType, Volume24hValue,
    },
};

use super::{
    catalog::{Catalog, CatalogMarket},
    owner::CatalogSnapshot,
    statistics_profile::{catalog_products, field_support, FIELDS, POLL_INTERVAL},
    venue::{exchange_error, CatalogScope, Provider, Venue},
};

/// Reason for a field this poll intentionally did not acquire. Projection
/// carries the previous observation and its original monotonic receipt.
pub(super) const NOT_REQUESTED: &str = "not-requested";

/// Source label for the maintained Lighter `market_stats/all` stream, which is
/// the only stock source for Lighter funding/mark/index. Matches the
/// coordinator's live-failure source constant.
pub(super) const LIGHTER_LIVE_SOURCE: &str = "lighterxyz:ccxt:watchTickers";

const BINANCE_TICKERS: &str = "binance:ccxt:fetchTickers";
const BINANCE_FUNDING: &str = "binance:ccxt:fetchFundingRates";
const BINANCE_INTERVALS: &str = "binance:ccxt:fetchFundingIntervals";
const BINANCE_MARK: &str = "binance:ccxt:fetchMarkPrices";
const BINANCE_OPTION_INDEX: &str = "binance:ccxt:eapiPublicGetIndex";
const BINANCE_OI: &str = "binance:ccxt:fetchOpenInterest";
const BYBIT_TICKERS: &str = "bybit:ccxt:fetchTickers";
const ASTER_TICKERS: &str = "aster:ccxt:fetchTickers";
const ASTER_FUNDING: &str = "aster:ccxt:fetchFundingRates";
const ASTER_INTERVALS: &str = "aster:ccxt:fetchFundingIntervals";
const HYPERLIQUID_TICKERS: &str = "hyperliquid:ccxt:fetchTickers";
const EXTENDED_TICKERS: &str = "extended:ccxt:fetchTickers";
const LIGHTER_TICKERS: &str = "lighterxyz:ccxt:fetchTickers";

/// Acquisition wall/monotonic receipt for one completed stock method.
#[derive(Debug, Clone, Copy)]
struct Receipt {
    at: Instant,
    wall: u64,
}

impl Receipt {
    fn now() -> Self {
        Self {
            at: Instant::now(),
            wall: now_millis(),
        }
    }
}

/// Acquire every bulk-supported field for the catalog profile. Binance singular
/// open interest is acquired only for the requested `marketIds` union.
pub(super) async fn fetch_statistics(
    venue: Venue,
    provider: &mut Provider,
    snapshot: &CatalogSnapshot,
    params: &FetchMarketStatsParams,
) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
    let scope = snapshot.scope;
    let catalog = &snapshot.catalog;
    let products: BTreeSet<UnifiedMarketType> =
        catalog_products(venue, scope).iter().copied().collect();

    let mut failures: Vec<MarketStatsSourceFailure> = Vec::new();
    let mut acquired = Acquired::default();

    if params.include_bulk {
        acquire_bulk(
            venue,
            scope,
            provider,
            catalog,
            &mut acquired,
            &mut failures,
        )
        .await;
    }
    if venue == Venue::Binance {
        acquire_binance_open_interest(provider, catalog, params, &mut acquired, &mut failures)
            .await;
    }

    let mut rows = Vec::new();
    let mut field_received_at = HashMap::new();
    for entry in catalog.entries() {
        let Some(identity) = entry.market.identity.as_ref() else {
            continue;
        };
        if !products.contains(&entry.market.market_type) {
            continue;
        }
        let interval = interval_for(venue, entry, &acquired);
        if venue == Venue::Bybit && interval.mismatch {
            failures.push(MarketStatsSourceFailure {
                source: BYBIT_TICKERS.to_string(),
                reason: "funding-interval-mismatch".to_string(),
                message: format!(
                    "Bybit {} ticker funding interval differs from instrument metadata",
                    identity.exchange_market_id
                ),
            });
        }
        let sources = RowSources {
            ticker: lookup(&acquired.tickers, entry),
            ticker_error: acquired.tickers.as_ref().and_then(error_reason),
            ticker_receipt: acquired.receipts.get(tickers_source(venue)).copied(),
            funding: lookup(&acquired.funding, entry),
            funding_error: acquired.funding.as_ref().and_then(error_reason),
            funding_receipt: acquired
                .receipts
                .get(&funding_source(venue, entry))
                .copied(),
            mark: lookup(&acquired.marks, entry),
            mark_error: acquired.marks.as_ref().and_then(error_reason),
            mark_receipt: acquired.receipts.get(BINANCE_MARK).copied(),
            option_index: option_underlying(entry)
                .and_then(|underlying| acquired.option_indices.get(underlying)),
            interval_ms: interval.ms,
            oi: acquired.open_interest.get(&identity.market_id),
            oi_receipt: acquired
                .oi_receipts
                .get(&identity.market_id)
                .copied()
                .or_else(|| acquired.receipts.get(tickers_source(venue)).copied()),
        };
        let (fields, receipts) = build_fields(venue, entry, &sources, params.include_bulk);
        if entry.market.active && !receipts.is_empty() {
            field_received_at.insert(identity.market_id.clone(), receipts);
        }
        rows.push(MarketStatsRow {
            market: entry.market.clone(),
            fields,
        });
    }
    rows.sort_unstable_by(|left, right| {
        left.market
            .identity
            .as_ref()
            .map(|identity| &identity.market_id)
            .cmp(
                &right
                    .market
                    .identity
                    .as_ref()
                    .map(|identity| &identity.market_id),
            )
    });

    let contexts_valid = !params.include_bulk || acquired.any_bulk_ok;
    let next_poll_at = if params.include_bulk {
        acquired
            .receipts
            .values()
            .map(|receipt| receipt.at)
            .max()
            .unwrap_or_else(Instant::now)
            + POLL_INTERVAL
    } else {
        // The coordinator preserves the prior bulk deadline after merging this
        // demand-only pass; never advance it from here.
        Instant::now()
    };
    let received_at = if params.include_bulk {
        acquired.receipts.values().map(|receipt| receipt.at).max()
    } else {
        None
    };
    Ok(MarketStatsSourceSnapshot {
        rows,
        // The owner-loaded catalog is authoritative and complete for its profile.
        catalog_known: true,
        complete_catalogs: products,
        contexts_valid,
        received_at,
        field_received_at,
        next_poll_at,
        source_failures: failures,
    })
}

fn tickers_source(venue: Venue) -> &'static str {
    match venue {
        Venue::Binance => BINANCE_TICKERS,
        Venue::Bybit => BYBIT_TICKERS,
        Venue::Aster => ASTER_TICKERS,
        Venue::Hyperliquid => HYPERLIQUID_TICKERS,
        Venue::Extended => EXTENDED_TICKERS,
        Venue::Lighter => LIGHTER_TICKERS,
    }
}

fn funding_source(venue: Venue, entry: &CatalogMarket) -> &'static str {
    match venue {
        Venue::Binance if entry.market.market_type == UnifiedMarketType::Option => BINANCE_MARK,
        Venue::Binance => BINANCE_FUNDING,
        Venue::Aster => ASTER_FUNDING,
        other => tickers_source(other),
    }
}

#[derive(Default)]
struct Acquired {
    tickers: Option<Result<HashMap<String, Value>, ExchangeError>>,
    funding: Option<Result<HashMap<String, Value>, ExchangeError>>,
    marks: Option<Result<HashMap<String, Value>, ExchangeError>>,
    option_indices: HashMap<String, (Result<Value, ExchangeError>, Receipt)>,
    intervals: Option<HashMap<String, u64>>,
    open_interest: HashMap<String, Result<Value, ExchangeError>>,
    receipts: HashMap<&'static str, Receipt>,
    oi_receipts: HashMap<String, Receipt>,
    any_bulk_ok: bool,
}

struct RowSources<'a> {
    ticker: Option<&'a Value>,
    ticker_error: Option<&'static str>,
    ticker_receipt: Option<Receipt>,
    funding: Option<&'a Value>,
    funding_error: Option<&'static str>,
    funding_receipt: Option<Receipt>,
    mark: Option<&'a Value>,
    mark_error: Option<&'static str>,
    mark_receipt: Option<Receipt>,
    option_index: Option<&'a (Result<Value, ExchangeError>, Receipt)>,
    interval_ms: Option<u64>,
    oi: Option<&'a Result<Value, ExchangeError>>,
    oi_receipt: Option<Receipt>,
}

fn error_reason(result: &Result<HashMap<String, Value>, ExchangeError>) -> Option<&'static str> {
    result.as_ref().err().map(failure_reason)
}

fn lookup<'a>(
    source: &'a Option<Result<HashMap<String, Value>, ExchangeError>>,
    entry: &CatalogMarket,
) -> Option<&'a Value> {
    let map = source.as_ref()?.as_ref().ok()?;
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

struct Interval {
    ms: Option<u64>,
    mismatch: bool,
}

fn interval_for(venue: Venue, entry: &CatalogMarket, acquired: &Acquired) -> Interval {
    match venue {
        Venue::Binance | Venue::Aster => {
            let native = entry
                .market
                .identity
                .as_ref()
                .map(|identity| identity.exchange_market_id.as_str())
                .unwrap_or_default();
            let ms = acquired.intervals.as_ref().and_then(|map| {
                map.get(native)
                    .or_else(|| map.get(&entry.ccxt_symbol))
                    .copied()
            });
            Interval {
                ms,
                mismatch: false,
            }
        }
        Venue::Bybit => {
            let (ms, mismatch) = bybit_interval(entry, lookup(&acquired.tickers, entry));
            Interval { ms, mismatch }
        }
        Venue::Hyperliquid | Venue::Lighter | Venue::Extended => Interval {
            ms: Some(3_600_000),
            mismatch: false,
        },
    }
}

/// Bybit funding interval: ticker raw `fundingIntervalHour` wins over the
/// instrument `fundingInterval` (minutes), and a disagreement is reported.
fn bybit_interval(entry: &CatalogMarket, ticker: Option<&Value>) -> (Option<u64>, bool) {
    let ticker_interval = ticker
        .and_then(Value::as_map)
        .and_then(|ticker| ticker.get("info"))
        .and_then(Value::as_map)
        .and_then(|info| info.get("fundingIntervalHour"));
    let ticker_ms = ticker_interval
        .and_then(positive_integer)
        .and_then(|hours| hours.checked_mul(3_600_000));
    if ticker_interval.is_some() && ticker_ms.is_none() {
        return (None, false);
    }
    let instrument_ms = entry
        .raw
        .get("info")
        .and_then(|info| info.get("fundingInterval"))
        .and_then(json_positive_integer)
        .and_then(|minutes| minutes.checked_mul(60_000));
    match (ticker_ms, instrument_ms) {
        (Some(ticker), Some(instrument)) => (Some(ticker), ticker != instrument),
        (Some(ticker), None) => (Some(ticker), false),
        (None, instrument) => (instrument, false),
    }
}

fn json_positive_integer(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(number) => number.as_u64().filter(|value| *value > 0),
        JsonValue::String(text) => text.parse::<u64>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Acquisition
// ---------------------------------------------------------------------------

/// Run one stock call with panic isolation. Stock cores can panic on an
/// unexpected product; a panic must become this call's failure, not abort the
/// whole snapshot.
async fn run_call(
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

async fn acquire_bulk(
    venue: Venue,
    scope: CatalogScope,
    provider: &mut Provider,
    catalog: &Catalog,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    match venue {
        Venue::Binance => {
            let tickers = run_call(
                provider,
                venue,
                "fetch_tickers",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            acquired.tickers = Some(finish(venue, BINANCE_TICKERS, tickers, acquired, failures));
            if matches!(scope, CatalogScope::Linear | CatalogScope::Inverse) {
                let funding = run_call(
                    provider,
                    venue,
                    "fetch_funding_rates",
                    vec![Value::Null, Params::none().into_value_object()],
                )
                .await;
                acquired.funding =
                    Some(finish(venue, BINANCE_FUNDING, funding, acquired, failures));
                let intervals = run_call(
                    provider,
                    venue,
                    "fetch_funding_intervals",
                    vec![Value::Null, Params::none().into_value_object()],
                )
                .await;
                finish_intervals(venue, BINANCE_INTERVALS, intervals, acquired, failures);
            } else if scope == CatalogScope::Option {
                let marks = run_call(
                    provider,
                    venue,
                    "fetch_mark_prices",
                    vec![
                        Value::Null,
                        Params::new().with_str("type", "option").into_value_object(),
                    ],
                )
                .await;
                acquired.marks = Some(finish(venue, BINANCE_MARK, marks, acquired, failures));
                acquire_binance_option_indices(provider, catalog, acquired, failures).await;
            }
        }
        Venue::Bybit => {
            let mut results: Vec<Result<HashMap<String, Value>, ExchangeError>> = Vec::new();
            if scope == CatalogScope::Option {
                let mut bases: BTreeSet<String> = BTreeSet::new();
                for entry in catalog.entries() {
                    bases.insert(entry.market.base.clone());
                }
                for base in bases {
                    let params = Params::new()
                        .with_str("type", "option")
                        .with_str("baseCoin", &base)
                        .into_value_object();
                    results.push(
                        run_call(provider, venue, "fetch_tickers", vec![Value::Null, params])
                            .await
                            .and_then(|value| index_tickers(venue, &value)),
                    );
                }
            } else {
                results.push(
                    run_call(
                        provider,
                        venue,
                        "fetch_tickers",
                        vec![Value::Null, Params::none().into_value_object()],
                    )
                    .await
                    .and_then(|value| index_tickers(venue, &value)),
                );
            }
            let mut merged: HashMap<String, Value> = HashMap::new();
            let mut first_error: Option<ExchangeError> = None;
            let mut any = false;
            for result in results {
                match result {
                    Ok(map) => {
                        any = true;
                        for (key, value) in map {
                            merged.entry(key).or_insert(value);
                        }
                    }
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            if any {
                acquired.any_bulk_ok = true;
                acquired.receipts.insert(BYBIT_TICKERS, Receipt::now());
                acquired.tickers = Some(Ok(merged));
            } else {
                let error = first_error.unwrap_or_else(|| {
                    ExchangeError::UpstreamData("Bybit returned no ticker groups".into())
                });
                failures.push(failure(BYBIT_TICKERS, &error));
                acquired.tickers = Some(Err(error));
            }
            acquire_bybit_open_interest(provider, catalog, acquired, failures).await;
        }
        Venue::Aster => {
            let swap = run_call(
                provider,
                venue,
                "fetch_tickers",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await
            .and_then(|value| index_tickers(venue, &value));
            let spot = if catalog_products(venue, scope).contains(&UnifiedMarketType::Spot) {
                let params = Params::new().with_str("type", "spot").into_value_object();
                Some(
                    run_call(provider, venue, "fetch_tickers", vec![Value::Null, params])
                        .await
                        .and_then(|value| index_tickers(venue, &value)),
                )
            } else {
                None
            };
            let mut merged: HashMap<String, Value> = HashMap::new();
            let mut first_error: Option<ExchangeError> = None;
            for result in std::iter::once(swap).chain(spot.into_iter()) {
                match result {
                    Ok(map) => {
                        acquired.any_bulk_ok = true;
                        for (key, value) in map {
                            merged.entry(key).or_insert(value);
                        }
                    }
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            match (merged.is_empty(), first_error) {
                (false, error) => {
                    if let Some(error) = error {
                        failures.push(failure(ASTER_TICKERS, &error));
                    }
                    acquired.receipts.insert(ASTER_TICKERS, Receipt::now());
                    acquired.tickers = Some(Ok(merged));
                }
                (true, Some(error)) => {
                    failures.push(failure(ASTER_TICKERS, &error));
                    acquired.tickers = Some(Err(error));
                }
                (true, None) => {}
            }
            if catalog_products(venue, scope).contains(&UnifiedMarketType::Perp) {
                let funding = run_call(
                    provider,
                    venue,
                    "fetch_funding_rates",
                    vec![Value::Null, Params::none().into_value_object()],
                )
                .await;
                acquired.funding = Some(finish(venue, ASTER_FUNDING, funding, acquired, failures));
                let intervals = run_call(
                    provider,
                    venue,
                    "fetch_funding_intervals",
                    vec![Value::Null, Params::none().into_value_object()],
                )
                .await;
                finish_intervals(venue, ASTER_INTERVALS, intervals, acquired, failures);
            }
        }
        Venue::Hyperliquid => {
            let tickers = run_call(
                provider,
                venue,
                "fetch_tickers",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            acquired.tickers = Some(finish(
                venue,
                HYPERLIQUID_TICKERS,
                tickers,
                acquired,
                failures,
            ));
        }
        Venue::Extended => {
            let tickers = run_call(
                provider,
                venue,
                "fetch_tickers",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            acquired.tickers = Some(finish(venue, EXTENDED_TICKERS, tickers, acquired, failures));
        }
        Venue::Lighter => {
            let tickers = run_call(
                provider,
                venue,
                "fetch_tickers",
                vec![Value::Null, Params::none().into_value_object()],
            )
            .await;
            acquired.tickers = Some(finish(venue, LIGHTER_TICKERS, tickers, acquired, failures));
        }
    }
}

fn finish(
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

fn finish_intervals(
    venue: Venue,
    source: &'static str,
    result: Result<Value, ExchangeError>,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    match result.and_then(|value| index_intervals(venue, &value)) {
        Ok(map) => {
            acquired.any_bulk_ok = true;
            acquired.receipts.insert(source, Receipt::now());
            acquired.intervals = Some(map);
        }
        Err(error) => failures.push(failure(source, &error)),
    }
}

/// Bybit open interest comes from the same bulk ticker row. The stock venue
/// parser owns the pinned linear-amount/inverse-value semantics.
async fn acquire_bybit_open_interest(
    provider: &mut Provider,
    catalog: &Catalog,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let ticker_receipt = acquired.receipts.get(BYBIT_TICKERS).copied();
    for entry in catalog.entries() {
        if entry.market.market_type == UnifiedMarketType::Spot {
            continue;
        }
        let Some(identity) = entry.market.identity.as_ref() else {
            continue;
        };
        let Some(ticker) = lookup(&acquired.tickers, entry) else {
            continue;
        };
        let raw = get_value_k(ticker, "info");
        let market = Value::from_json(&entry.raw);
        let result = run_call(
            provider,
            Venue::Bybit,
            "parse_open_interest",
            vec![raw, market],
        )
        .await;
        if let Err(error) = &result {
            failures.push(failure(BYBIT_TICKERS, error));
        }
        if let Some(receipt) = ticker_receipt {
            acquired
                .oi_receipts
                .insert(identity.market_id.clone(), receipt);
        }
        acquired
            .open_interest
            .insert(identity.market_id.clone(), result);
    }
}

fn option_underlying(entry: &CatalogMarket) -> Option<&str> {
    entry.raw.get("info")?.get("underlying")?.as_str()
}

async fn acquire_binance_option_indices(
    provider: &mut Provider,
    catalog: &Catalog,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let underlyings: BTreeSet<&str> = catalog
        .entries()
        .iter()
        .filter(|entry| {
            entry.market.active && entry.market.market_type == UnifiedMarketType::Option
        })
        .filter_map(option_underlying)
        .collect();
    for underlying in underlyings {
        // One stock index call per underlying, shared by all option contracts.
        // Ticker exercisePrice changes meaning near settlement; never use it.
        let result = run_call(
            provider,
            Venue::Binance,
            "eapi_public_get_index",
            vec![Params::new()
                .with_str("underlying", underlying)
                .into_value_object()],
        )
        .await;
        let receipt = Receipt::now();
        match &result {
            Ok(_) => {
                acquired.any_bulk_ok = true;
                acquired.receipts.insert(BINANCE_OPTION_INDEX, receipt);
            }
            Err(error) => failures.push(failure(BINANCE_OPTION_INDEX, error)),
        }
        acquired
            .option_indices
            .insert(underlying.to_string(), (result, receipt));
    }
}

async fn acquire_binance_open_interest(
    provider: &mut Provider,
    catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    for market_id in &params.open_interest_market_ids {
        let Some(entry) = catalog.entries().iter().find(|entry| {
            entry
                .market
                .identity
                .as_ref()
                .is_some_and(|identity| &identity.market_id == market_id)
        }) else {
            continue;
        };
        if !entry.market.active
            || field_support(
                Venue::Binance,
                entry.market.market_type,
                MarketStatsFieldName::OpenInterest,
            )
            .state
                != CapabilityState::Supported
        {
            continue;
        }
        let result = run_call(
            provider,
            Venue::Binance,
            "fetch_open_interest",
            vec![
                Value::Str(entry.ccxt_symbol.clone().into()),
                Params::none().into_value_object(),
            ],
        )
        .await;
        let receipt = Receipt::now();
        if let Err(error) = &result {
            failures.push(failure(BINANCE_OI, error));
        }
        acquired.oi_receipts.insert(market_id.clone(), receipt);
        acquired.open_interest.insert(market_id.clone(), result);
    }
}

// ---------------------------------------------------------------------------
// Identity indexing
// ---------------------------------------------------------------------------

fn index_tickers(venue: Venue, value: &Value) -> Result<HashMap<String, Value>, ExchangeError> {
    let map = value.as_map().ok_or_else(|| {
        ExchangeError::UpstreamData(format!(
            "{} statistics tickers response is not an object",
            venue.public_id()
        ))
    })?;
    let mut index: HashMap<String, Value> = HashMap::with_capacity(map.len());
    for (key, ticker) in map.iter() {
        for alias in identity_aliases(key, ticker) {
            match index.get(&alias) {
                Some(existing) if !same_row(existing, ticker) => {
                    return Err(ExchangeError::UpstreamData(format!(
                        "{} statistics response aliases {alias} to conflicting rows",
                        venue.public_id()
                    )));
                }
                Some(_) => {}
                None => {
                    index.insert(alias, ticker.clone());
                }
            }
        }
    }
    Ok(index)
}

fn index_intervals(venue: Venue, value: &Value) -> Result<HashMap<String, u64>, ExchangeError> {
    let map = value.as_map().ok_or_else(|| {
        ExchangeError::UpstreamData(format!(
            "{} funding-interval response is not an object",
            venue.public_id()
        ))
    })?;
    let mut index = HashMap::with_capacity(map.len());
    for (key, row) in map.iter() {
        let info = get_value_k(row, "info");
        let Some(ms) = positive_integer(&get_value_k(&info, "fundingIntervalHours"))
            .and_then(|hours| hours.checked_mul(3_600_000))
        else {
            continue;
        };
        for alias in identity_aliases(key, row) {
            index.entry(alias).or_insert(ms);
        }
    }
    Ok(index)
}

fn identity_aliases(key: &str, row: &Value) -> Vec<String> {
    let mut aliases = Vec::with_capacity(6);
    if !key.is_empty() {
        aliases.push(key.to_string());
    }
    if let Some(symbol) = get_value_k(row, "symbol").as_str() {
        if !symbol.is_empty() {
            aliases.push(symbol.to_string());
        }
    }
    let info = get_value_k(row, "info");
    for field in ["symbol", "name", "id", "market_id", "marketId", "baseId"] {
        let raw = get_value_k(&info, field);
        if let Some(text) = raw.as_str() {
            if !text.is_empty() {
                aliases.push(text.to_string());
            }
        } else if let Some(number) = raw.as_i64() {
            aliases.push(number.to_string());
        }
    }
    aliases
}

fn same_row(left: &Value, right: &Value) -> bool {
    let left_symbol = get_value_k(left, "symbol");
    let right_symbol = get_value_k(right, "symbol");
    if !left_symbol.is_null() && left_symbol == right_symbol {
        return true;
    }
    let left_id = get_value_k(&get_value_k(left, "info"), "symbol");
    let right_id = get_value_k(&get_value_k(right, "info"), "symbol");
    !left_id.is_null() && left_id == right_id
}

// ---------------------------------------------------------------------------
// Field construction
// ---------------------------------------------------------------------------

fn build_fields(
    venue: Venue,
    entry: &CatalogMarket,
    src: &RowSources<'_>,
    bulk: bool,
) -> (
    BTreeMap<MarketStatsFieldName, MarketStatsField>,
    BTreeMap<MarketStatsFieldName, Instant>,
) {
    let product = entry.market.market_type;
    let mut fields = BTreeMap::new();
    let mut receipts = BTreeMap::new();
    for name in FIELDS {
        let support = field_support(venue, product, name);
        let (field, receipt) = match support.state {
            CapabilityState::NotApplicable => (
                fixed(
                    MarketStatsFieldState::NotApplicable,
                    support.reason.as_deref(),
                ),
                None,
            ),
            CapabilityState::Unsupported => (
                fixed(
                    MarketStatsFieldState::Unsupported,
                    support.reason.as_deref(),
                ),
                None,
            ),
            CapabilityState::Supported => {
                let oi_target =
                    venue == Venue::Binance && name == MarketStatsFieldName::OpenInterest;
                if !bulk && !oi_target {
                    (not_requested(), None)
                } else if venue == Venue::Lighter
                    && product == UnifiedMarketType::Perp
                    && matches!(
                        name,
                        MarketStatsFieldName::Funding
                            | MarketStatsFieldName::LastSettledFunding
                            | MarketStatsFieldName::MarkPrice
                            | MarketStatsFieldName::IndexPrice
                    )
                {
                    // REST `fetch_tickers` does not carry these; the shared
                    // live `watchTickers` feed is the only stock source.
                    (not_requested(), None)
                } else if !entry.market.active {
                    (unavailable("inactive-market"), None)
                } else {
                    let (field, receipt) = observe(venue, entry, name, src);
                    (field, receipt)
                }
            }
        };
        if let Some(receipt) = receipt {
            receipts.insert(name, receipt);
        }
        fields.insert(name, field);
    }
    (fields, receipts)
}

fn observe(
    venue: Venue,
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    src: &RowSources<'_>,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    match venue {
        Venue::Binance => match name {
            MarketStatsFieldName::LastPrice => price_field(
                BINANCE_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "lastPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::Volume24h => volume_field(
                BINANCE_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                // Inverse reports base coin as `baseVolume`; never CCXT's derived
                // quote (baseVolume*weightedAvgPrice).
                if entry.inverse == Some(true) {
                    "baseVolume"
                } else if entry.market.market_type == UnifiedMarketType::Option
                    && entry
                        .raw
                        .get("info")
                        .and_then(|info| info.get("unit"))
                        .and_then(|unit| {
                            unit.as_f64()
                                .or_else(|| unit.as_str().and_then(lexical_f64))
                        })
                        != Some(1.0)
                {
                    // Native option volume is contracts, not base coins.
                    "\u{0}base-not-provided"
                } else {
                    "volume"
                },
                // Option `ticker24hr` reports quote turnover as `amount`.
                if entry.market.market_type == UnifiedMarketType::Option {
                    "amount"
                } else {
                    "quoteVolume"
                },
            ),
            MarketStatsFieldName::OpenInterest => oi_field(BINANCE_OI, src.oi, src.oi_receipt),
            MarketStatsFieldName::IndexPrice
                if entry.market.market_type == UnifiedMarketType::Option =>
            {
                let Some((result, receipt)) = src.option_index else {
                    return missing_pair(BINANCE_OPTION_INDEX, None);
                };
                match result {
                    Ok(info) => price_info_field(
                        BINANCE_OPTION_INDEX,
                        info,
                        None,
                        Some(*receipt),
                        "indexPrice",
                        base,
                        quote,
                    ),
                    Err(error) => missing_pair(BINANCE_OPTION_INDEX, Some(failure_reason(error))),
                }
            }
            MarketStatsFieldName::Funding
            | MarketStatsFieldName::MarkPrice
            | MarketStatsFieldName::IndexPrice => {
                let option = entry.market.market_type == UnifiedMarketType::Option;
                let (source, row, error, receipt) = if option {
                    (BINANCE_MARK, src.mark, src.mark_error, src.mark_receipt)
                } else {
                    (
                        BINANCE_FUNDING,
                        src.funding,
                        src.funding_error,
                        src.funding_receipt,
                    )
                };
                match name {
                    MarketStatsFieldName::Funding => funding_field(
                        source,
                        row,
                        error,
                        receipt,
                        "lastFundingRate",
                        FundingRateUnit::DecimalFraction,
                        FundingKind::CurrentUnclassified,
                        src.interval_ms,
                        Some("nextFundingTime"),
                    ),
                    MarketStatsFieldName::MarkPrice => {
                        price_field(source, row, error, receipt, "markPrice", base, quote)
                    }
                    _ => price_field(source, row, error, receipt, "indexPrice", base, quote),
                }
            }
            MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
        },
        Venue::Bybit => match name {
            MarketStatsFieldName::OpenInterest => match src.oi {
                Some(_) => oi_field(BYBIT_TICKERS, src.oi, src.oi_receipt),
                None if src.ticker.is_some() => not_requested_pair(),
                None => missing_pair(BYBIT_TICKERS, src.ticker_error),
            },
            MarketStatsFieldName::Volume24h => volume_field(
                BYBIT_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                if entry.inverse == Some(true) {
                    "turnover24h"
                } else {
                    "volume24h"
                },
                if entry.inverse == Some(true) {
                    "volume24h"
                } else {
                    "turnover24h"
                },
            ),
            MarketStatsFieldName::Funding => funding_field(
                BYBIT_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "fundingRate",
                FundingRateUnit::DecimalFraction,
                FundingKind::Estimate,
                src.interval_ms,
                Some("nextFundingTime"),
            ),
            MarketStatsFieldName::MarkPrice => price_field(
                BYBIT_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "markPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::IndexPrice => price_field(
                BYBIT_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "indexPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::LastPrice => price_field(
                BYBIT_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "lastPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
        },
        Venue::Aster => match name {
            MarketStatsFieldName::LastPrice => price_field(
                ASTER_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "lastPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::Volume24h => volume_field(
                ASTER_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "volume",
                "quoteVolume",
            ),
            MarketStatsFieldName::Funding => funding_field(
                ASTER_FUNDING,
                src.funding,
                src.funding_error,
                src.funding_receipt,
                "lastFundingRate",
                FundingRateUnit::DecimalFraction,
                FundingKind::Estimate,
                src.interval_ms,
                Some("nextFundingTime"),
            ),
            MarketStatsFieldName::MarkPrice => price_field(
                ASTER_FUNDING,
                src.funding,
                src.funding_error,
                src.funding_receipt,
                "markPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::IndexPrice => price_field(
                ASTER_FUNDING,
                src.funding,
                src.funding_error,
                src.funding_receipt,
                "indexPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::OpenInterest | MarketStatsFieldName::LastSettledFunding => {
                not_requested_pair()
            }
        },
        Venue::Hyperliquid => match name {
            MarketStatsFieldName::Funding => funding_field(
                HYPERLIQUID_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "funding",
                FundingRateUnit::DecimalFraction,
                FundingKind::CurrentUnclassified,
                Some(3_600_000),
                None,
            ),
            MarketStatsFieldName::MarkPrice => price_field(
                HYPERLIQUID_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "markPx",
                base,
                quote,
            ),
            MarketStatsFieldName::IndexPrice => price_field(
                HYPERLIQUID_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "oraclePx",
                base,
                quote,
            ),
            MarketStatsFieldName::Volume24h => volume_field(
                HYPERLIQUID_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                // No base volume in the swap context; never derive it.
                "\u{0}base-not-provided",
                "dayNtlVlm",
            ),
            MarketStatsFieldName::OpenInterest => {
                let Some(row) = src.ticker else {
                    return missing_pair(HYPERLIQUID_TICKERS, src.ticker_error);
                };
                let info = get_value_k(row, "info");
                match nonneg_number(&get_value_k(&info, "openInterest")) {
                    Err(()) => invalid_pair(HYPERLIQUID_TICKERS, src.ticker_receipt),
                    Ok(None) => missing_pair(HYPERLIQUID_TICKERS, src.ticker_error),
                    Ok(Some(amount)) => (
                        observed(
                            HYPERLIQUID_TICKERS,
                            MarketStatsValue::OpenInterest(OpenInterestValue {
                                open_interest_amount: Some(amount),
                                open_interest_value: None,
                            }),
                            exchange_time(&info),
                            src.ticker_receipt,
                        ),
                        src.ticker_receipt.map(|receipt| receipt.at),
                    ),
                }
            }
            MarketStatsFieldName::LastPrice | MarketStatsFieldName::LastSettledFunding => {
                not_requested_pair()
            }
        },
        Venue::Extended => match name {
            MarketStatsFieldName::Funding => funding_field(
                EXTENDED_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "fundingRate",
                FundingRateUnit::DecimalFraction,
                FundingKind::Estimate,
                Some(3_600_000),
                None,
            ),
            MarketStatsFieldName::MarkPrice => price_field(
                EXTENDED_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "markPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::IndexPrice => price_field(
                EXTENDED_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "indexPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::LastPrice => price_field(
                EXTENDED_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "lastPrice",
                base,
                quote,
            ),
            MarketStatsFieldName::Volume24h => volume_field(
                EXTENDED_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "dailyVolumeBase",
                "dailyVolume",
            ),
            MarketStatsFieldName::OpenInterest => {
                let Some(row) = src.ticker else {
                    return missing_pair(EXTENDED_TICKERS, src.ticker_error);
                };
                let info = get_value_k(row, "info");
                match (
                    nonneg_number(&get_value_k(&info, "openInterestBase")),
                    nonneg_number(&get_value_k(&info, "openInterest")),
                ) {
                    (Err(()), _) | (_, Err(())) => {
                        invalid_pair(EXTENDED_TICKERS, src.ticker_receipt)
                    }
                    (Ok(None), Ok(None)) => missing_pair(EXTENDED_TICKERS, src.ticker_error),
                    (amount, value) => {
                        let amount = amount.ok().flatten();
                        let value = value.ok().flatten();
                        (
                            observed(
                                EXTENDED_TICKERS,
                                MarketStatsValue::OpenInterest(OpenInterestValue {
                                    open_interest_amount: amount,
                                    open_interest_value: value,
                                }),
                                exchange_time(&info),
                                src.ticker_receipt,
                            ),
                            src.ticker_receipt.map(|receipt| receipt.at),
                        )
                    }
                }
            }
            MarketStatsFieldName::LastSettledFunding => not_requested_pair(),
        },
        Venue::Lighter => match name {
            MarketStatsFieldName::LastPrice => price_field(
                LIGHTER_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "last_trade_price",
                base,
                quote,
            ),
            MarketStatsFieldName::Volume24h => volume_field(
                LIGHTER_TICKERS,
                src.ticker,
                src.ticker_error,
                src.ticker_receipt,
                "daily_base_token_volume",
                "daily_quote_token_volume",
            ),
            MarketStatsFieldName::Funding
            | MarketStatsFieldName::LastSettledFunding
            | MarketStatsFieldName::MarkPrice
            | MarketStatsFieldName::IndexPrice
            | MarketStatsFieldName::OpenInterest => not_requested_pair(),
        },
    }
}

// ---------------------------------------------------------------------------
// Field builders
// ---------------------------------------------------------------------------

fn price_field(
    source: &'static str,
    row: Option<&Value>,
    error: Option<&'static str>,
    receipt: Option<Receipt>,
    key: &str,
    base: &str,
    quote: &str,
) -> (MarketStatsField, Option<Instant>) {
    let Some(row) = row else {
        return (missing(source, error), None);
    };
    let info = get_value_k(row, "info");
    price_info_field(source, &info, error, receipt, key, base, quote)
}

fn price_info_field(
    source: &'static str,
    info: &Value,
    error: Option<&'static str>,
    receipt: Option<Receipt>,
    key: &str,
    base: &str,
    quote: &str,
) -> (MarketStatsField, Option<Instant>) {
    if !info.as_map().is_some_and(|info| info.contains_key(key)) {
        return (missing(source, error), None);
    }
    let Some(receipt) = receipt else {
        return (missing(source, error), None);
    };
    match price_value(raw_lexical(info, key), base, quote) {
        Some(value) => (
            observed(source, value, exchange_time(info), Some(receipt)),
            Some(receipt.at),
        ),
        None => (invalid(source, receipt), Some(receipt.at)),
    }
}

#[allow(clippy::too_many_arguments)]
fn funding_field(
    source: &'static str,
    row: Option<&Value>,
    error: Option<&'static str>,
    receipt: Option<Receipt>,
    rate_key: &str,
    unit: FundingRateUnit,
    kind: FundingKind,
    interval_ms: Option<u64>,
    next_payment_key: Option<&str>,
) -> (MarketStatsField, Option<Instant>) {
    let Some(row) = row else {
        return (missing(source, error), None);
    };
    let info = get_value_k(row, "info");
    if !info
        .as_map()
        .is_some_and(|info| info.contains_key(rate_key))
    {
        return (missing(source, error), None);
    }
    let Some(receipt) = receipt else {
        return (missing(source, error), None);
    };
    let Some(rate) = raw_lexical(&info, rate_key) else {
        return (invalid(source, receipt), Some(receipt.at));
    };
    let next_payment = next_payment_key
        .map(|key| positive_integer(&get_value_k(&info, key)))
        .unwrap_or(None);
    (
        observed(
            source,
            MarketStatsValue::Funding(FundingValue::new(
                rate,
                unit,
                kind,
                interval_ms,
                interval_ms,
                None,
                next_payment,
            )),
            exchange_time(&info),
            Some(receipt),
        ),
        Some(receipt.at),
    )
}

fn volume_field(
    source: &'static str,
    row: Option<&Value>,
    error: Option<&'static str>,
    receipt: Option<Receipt>,
    base_key: &str,
    quote_key: &str,
) -> (MarketStatsField, Option<Instant>) {
    let Some(row) = row else {
        return (missing(source, error), None);
    };
    let info = get_value_k(row, "info");
    let Some(receipt) = receipt else {
        return (missing(source, error), None);
    };
    match (
        nonneg_member(&get_value_k(&info, base_key)),
        nonneg_member(&get_value_k(&info, quote_key)),
    ) {
        (Err(()), _) | (_, Err(())) => (invalid(source, receipt), Some(receipt.at)),
        (Ok(None), Ok(None)) => (missing(source, error), None),
        (Ok(base_volume), Ok(quote_volume)) => (
            observed(
                source,
                MarketStatsValue::Volume24h(Volume24hValue {
                    base_volume,
                    quote_volume,
                }),
                exchange_time(&info),
                Some(receipt),
            ),
            Some(receipt.at),
        ),
    }
}

fn oi_field(
    source: &'static str,
    result: Option<&Result<Value, ExchangeError>>,
    receipt: Option<Receipt>,
) -> (MarketStatsField, Option<Instant>) {
    let Some(result) = result else {
        return (not_requested(), None);
    };
    let Ok(unified) = result else {
        return (
            missing(source, Some(failure_reason(result.as_ref().unwrap_err()))),
            None,
        );
    };
    let Some(receipt) = receipt else {
        return (missing(source, None), None);
    };
    // Stock safe-number parsing can erase malformed input as null. Validate
    // its retained source members without replacing the stock value mapping.
    let invalid_raw = unified
        .as_map()
        .and_then(|row| row.get("info"))
        .and_then(Value::as_map)
        .is_some_and(|info| {
            [
                "openInterest",
                "sumOpenInterest",
                "sumOpenInterestValue",
                "sumOpenInterestUsd",
            ]
            .iter()
            .filter_map(|key| info.get(*key))
            .any(|value| nonneg_number(value).is_err())
        });
    if invalid_raw {
        return (invalid(source, receipt), Some(receipt.at));
    }
    match (
        nonneg_number(&get_value_k(unified, "openInterestAmount")),
        nonneg_number(&get_value_k(unified, "openInterestValue")),
    ) {
        (Err(()), _) | (_, Err(())) => (invalid(source, receipt), Some(receipt.at)),
        (Ok(None), Ok(None)) => (missing(source, None), None),
        (Ok(amount), Ok(value)) => (
            observed(
                source,
                MarketStatsValue::OpenInterest(OpenInterestValue {
                    open_interest_amount: amount,
                    open_interest_value: value,
                }),
                positive_integer(&get_value_k(unified, "timestamp")),
                Some(receipt),
            ),
            Some(receipt.at),
        ),
    }
}

fn price_value(raw: Option<String>, base: &str, quote: &str) -> Option<MarketStatsValue> {
    let raw = raw?;
    if raw.starts_with('-') || !raw.bytes().any(|byte| matches!(byte, b'1'..=b'9')) {
        return None;
    }
    Some(MarketStatsValue::Price(PriceValue {
        amount: raw,
        base_asset: base.to_string(),
        quote_asset: quote.to_string(),
    }))
}

fn observed(
    source: &str,
    value: MarketStatsValue,
    exchange_timestamp: Option<u64>,
    receipt: Option<Receipt>,
) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Available,
        value: Some(value),
        reason: None,
        exchange_timestamp,
        received_timestamp: receipt.map(|receipt| receipt.wall),
        source: Some(source.to_string()),
    }
}

fn invalid(source: &str, receipt: Receipt) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: None,
        received_timestamp: Some(receipt.wall),
        source: Some(source.to_string()),
    }
}

fn missing(source: &str, reason: Option<&'static str>) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(reason.unwrap_or("missing-upstream-row").to_string()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: Some(source.to_string()),
    }
}

fn missing_pair(
    source: &'static str,
    error: Option<&'static str>,
) -> (MarketStatsField, Option<Instant>) {
    (missing(source, error), None)
}

fn invalid_pair(
    source: &'static str,
    receipt: Option<Receipt>,
) -> (MarketStatsField, Option<Instant>) {
    match receipt {
        Some(receipt) => (invalid(source, receipt), Some(receipt.at)),
        None => (missing(source, None), None),
    }
}

fn not_requested_pair() -> (MarketStatsField, Option<Instant>) {
    (not_requested(), None)
}

fn unavailable(reason: &str) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(reason.to_string()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}

fn not_requested() -> MarketStatsField {
    unavailable(NOT_REQUESTED)
}

fn fixed(state: MarketStatsFieldState, reason: Option<&str>) -> MarketStatsField {
    MarketStatsField {
        state,
        value: None,
        reason: reason.map(str::to_string),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

/// Raw lexical string from a stock `info` object, preserving native spelling.
fn raw_lexical(info: &Value, key: &str) -> Option<String> {
    lexical(&get_value_k(info, key))
}

fn lexical(value: &Value) -> Option<String> {
    match value {
        Value::Str(text) => decimal_string(text).map(str::to_string),
        Value::Int(number) => Some(number.to_string()),
        Value::Float(number) if number.is_finite() => Some(format_f64(*number)),
        _ => None,
    }
}

fn lexical_f64(value: &str) -> Option<f64> {
    let parsed: f64 = value.parse().ok()?;
    parsed.is_finite().then_some(parsed)
}

/// A nullable numeric member. `Err(())` means present-but-invalid (non-finite,
/// non-numeric, or negative), which clears the whole outer field.
fn nonneg_member(value: &Value) -> Result<Option<f64>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    let text = lexical(value).ok_or(())?;
    let number = lexical_f64(&text).ok_or(())?;
    if number < 0.0 {
        return Err(());
    }
    Ok(Some(number))
}

/// Same policy for a stock unified numeric member.
fn nonneg_number(value: &Value) -> Result<Option<f64>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    let number = unified_number(value).ok_or(())?;
    if number < 0.0 {
        return Err(());
    }
    Ok(Some(number))
}

fn unified_number(value: &Value) -> Option<f64> {
    match value {
        Value::Int(number) => Some(*number as f64),
        Value::Float(number) if number.is_finite() => Some(*number),
        Value::Str(text) => lexical_f64(text),
        _ => None,
    }
}

fn positive_integer(value: &Value) -> Option<u64> {
    match value {
        Value::Int(number) if *number > 0 => Some(*number as u64),
        Value::Float(number)
            if number.is_finite()
                && *number > 0.0
                && number.fract() == 0.0
                && *number < u64::MAX as f64 =>
        {
            Some(*number as u64)
        }
        Value::Str(text) => text.parse::<u64>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

fn exchange_time(info: &Value) -> Option<u64> {
    positive_integer(&get_value_k(info, "time"))
        .or_else(|| positive_integer(&get_value_k(info, "closeTime")))
}

fn format_f64(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// Validate lexically, without floating-point conversion or changing precision.
fn decimal_string(value: &str) -> Option<&str> {
    let digits = value.strip_prefix('-').unwrap_or(value).as_bytes();
    let integer_end = digits
        .iter()
        .position(|byte| *byte == b'.')
        .unwrap_or(digits.len());
    if integer_end == 0 || !digits[..integer_end].iter().all(u8::is_ascii_digit) {
        return None;
    }
    if integer_end < digits.len() {
        let fraction = &digits[integer_end + 1..];
        if fraction.is_empty() || !fraction.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }
    Some(value)
}

fn failure_reason(error: &ExchangeError) -> &'static str {
    match error {
        ExchangeError::UpstreamData(_) => "invalid-upstream-data",
        _ => "upstream-failure",
    }
}

fn failure(source: &str, error: &ExchangeError) -> MarketStatsSourceFailure {
    MarketStatsSourceFailure {
        source: source.to_string(),
        reason: failure_reason(error).to_string(),
        message: error.to_string(),
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Lighter live ticker patch
// ---------------------------------------------------------------------------

/// Map one stock Lighter `watchTickers` row to a sparse statistics patch.
///
/// Only native keys present in the row are emitted: absence must not overwrite
/// or freshen a prior observation. `funding_timestamp` is the settled payment
/// time only and is never used as an observation timestamp.
pub(super) fn lighter_ticker_patch(
    ticker: &Value,
    market: &JsonValue,
    received_timestamp: u64,
) -> Result<BTreeMap<MarketStatsFieldName, MarketStatsField>, ExchangeError> {
    let info = get_value_k(ticker, "info");
    let info = info.as_map().ok_or_else(|| {
        ExchangeError::UpstreamData("Lighter statistics ticker has no raw info object".into())
    })?;
    let base = market
        .get("base")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let quote = market
        .get("quote")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let receipt = Receipt {
        at: Instant::now(),
        wall: received_timestamp,
    };
    let mut fields = BTreeMap::new();

    if let Some(raw) = info.get("current_funding_rate") {
        fields.insert(
            MarketStatsFieldName::Funding,
            lighter_funding(raw, FundingKind::Estimate, None, receipt),
        );
    }
    if let Some(raw) = info.get("funding_rate") {
        let payment = info
            .get("funding_timestamp")
            .and_then(normalize_timestamp_ms);
        fields.insert(
            MarketStatsFieldName::LastSettledFunding,
            lighter_funding(raw, FundingKind::Settled, payment, receipt),
        );
    }
    for (key, name) in [
        ("mark_price", MarketStatsFieldName::MarkPrice),
        ("index_price", MarketStatsFieldName::IndexPrice),
        ("last_trade_price", MarketStatsFieldName::LastPrice),
    ] {
        if let Some(raw) = info.get(key) {
            fields.insert(name, lighter_price(raw, base, quote, receipt));
        }
    }
    if info.contains_key("daily_base_token_volume") || info.contains_key("daily_quote_token_volume")
    {
        let base = info
            .get("daily_base_token_volume")
            .map(nonneg_number)
            .unwrap_or(Ok(None));
        let quote = info
            .get("daily_quote_token_volume")
            .map(nonneg_number)
            .unwrap_or(Ok(None));
        fields.insert(
            MarketStatsFieldName::Volume24h,
            lighter_volume(base, quote, receipt),
        );
    }
    Ok(fields)
}

fn lighter_funding(
    raw: &Value,
    kind: FundingKind,
    payment_timestamp: Option<u64>,
    receipt: Receipt,
) -> MarketStatsField {
    let Some(rate) = lexical(raw) else {
        return invalid(LIGHTER_LIVE_SOURCE, receipt);
    };
    observed(
        LIGHTER_LIVE_SOURCE,
        MarketStatsValue::Funding(FundingValue::new(
            rate,
            FundingRateUnit::Percent,
            kind,
            Some(3_600_000),
            Some(3_600_000),
            payment_timestamp,
            None,
        )),
        None,
        Some(receipt),
    )
}

fn lighter_price(raw: &Value, base: &str, quote: &str, receipt: Receipt) -> MarketStatsField {
    match lexical(raw).and_then(|raw| price_value(Some(raw), base, quote)) {
        Some(value) => observed(LIGHTER_LIVE_SOURCE, value, None, Some(receipt)),
        None => invalid(LIGHTER_LIVE_SOURCE, receipt),
    }
}

fn lighter_volume(
    base: Result<Option<f64>, ()>,
    quote: Result<Option<f64>, ()>,
    receipt: Receipt,
) -> MarketStatsField {
    match (base, quote) {
        (Err(()), _) | (_, Err(())) => invalid(LIGHTER_LIVE_SOURCE, receipt),
        (Ok(None), Ok(None)) => MarketStatsField {
            state: MarketStatsFieldState::Unavailable,
            value: None,
            reason: Some("missing-upstream-row".to_string()),
            exchange_timestamp: None,
            received_timestamp: Some(receipt.wall),
            source: Some(LIGHTER_LIVE_SOURCE.to_string()),
        },
        (Ok(base_volume), Ok(quote_volume)) => observed(
            LIGHTER_LIVE_SOURCE,
            MarketStatsValue::Volume24h(Volume24hValue {
                base_volume,
                quote_volume,
            }),
            None,
            Some(receipt),
        ),
    }
}

fn normalize_timestamp_ms(value: &Value) -> Option<u64> {
    let timestamp = positive_integer(value)?;
    Some(if timestamp < 1_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else if timestamp >= 1_000_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{UnifiedMarket, UnifiedMarketInfo};

    fn market(base: &str, quote: &str, product: UnifiedMarketType) -> UnifiedMarket {
        UnifiedMarket {
            exchange: "test".into(),
            symbol: format!("{base}/{quote}"),
            base: base.into(),
            quote: quote.into(),
            market_type: product,
            active: true,
            min_order_size: None,
            tick_size: None,
            contract_size: None,
            info: UnifiedMarketInfo {
                category: None,
                raw_symbol: None,
                exchange_symbol: None,
                is_rfq: None,
                is_off_hours: None,
            },
            identity: None,
        }
    }

    fn entry(
        base: &str,
        quote: &str,
        product: UnifiedMarketType,
        inverse: Option<bool>,
        raw: JsonValue,
    ) -> CatalogMarket {
        CatalogMarket {
            market: market(base, quote, product),
            ccxt_symbol: format!("{base}/{quote}"),
            linear: inverse.map(|inverse| !inverse),
            inverse,
            raw,
        }
    }

    fn sources<'a>(ticker: Option<&'a Value>) -> RowSources<'a> {
        let receipt = ticker.map(|_| Receipt {
            at: Instant::now(),
            wall: 5,
        });
        RowSources {
            ticker,
            ticker_error: None,
            ticker_receipt: receipt,
            funding: None,
            funding_error: None,
            funding_receipt: None,
            mark: None,
            mark_error: None,
            mark_receipt: None,
            option_index: None,
            interval_ms: None,
            oi: None,
            oi_receipt: None,
        }
    }

    fn ticker(info: JsonValue) -> Value {
        Value::from_json(&serde_json::json!({ "info": info }))
    }

    #[test]
    fn lighter_patch_is_sparse_and_keeps_receipts_separate_from_funding_time() {
        let value = ticker(serde_json::json!({
            "market_id": 0,
            "mark_price": "3013.91",
            "last_trade_price": "3013.13",
            "current_funding_rate": "0.0012",
            "funding_rate": "0.0000",
            "funding_timestamp": 1_763_532_000_004u64,
            "daily_base_token_volume": 643235.2763,
            "daily_quote_token_volume": 1983505435.673896
        }));
        let fields = lighter_ticker_patch(
            &value,
            &serde_json::json!({"base":"ETH","quote":"USDC"}),
            42,
        )
        .unwrap();
        assert!(fields.contains_key(&MarketStatsFieldName::MarkPrice));
        assert!(fields.contains_key(&MarketStatsFieldName::LastPrice));
        assert!(!fields.contains_key(&MarketStatsFieldName::IndexPrice));
        assert!(!fields.contains_key(&MarketStatsFieldName::OpenInterest));
        assert_eq!(
            fields[&MarketStatsFieldName::MarkPrice].received_timestamp,
            Some(42)
        );
        assert_eq!(
            fields[&MarketStatsFieldName::MarkPrice].exchange_timestamp,
            None
        );
        assert_eq!(
            fields[&MarketStatsFieldName::MarkPrice].source.as_deref(),
            Some(LIGHTER_LIVE_SOURCE)
        );
        let MarketStatsValue::Funding(funding) = fields[&MarketStatsFieldName::LastSettledFunding]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("settled funding variant");
        };
        assert_eq!(funding.kind, FundingKind::Settled);
        assert_eq!(funding.payment_timestamp, Some(1_763_532_000_004));
        assert_eq!(funding.rate_unit, FundingRateUnit::Percent);
        assert_eq!(funding.rate, "0.0000");
    }

    #[test]
    fn lighter_patch_zero_funding_valid_and_invalid_price_clears() {
        let value = ticker(serde_json::json!({
            "market_id": 0,
            "current_funding_rate": "0.0000",
            "mark_price": "not-a-number"
        }));
        let fields =
            lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7)
                .unwrap();
        assert_eq!(
            fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
        let mark = &fields[&MarketStatsFieldName::MarkPrice];
        assert_eq!(mark.state, MarketStatsFieldState::Unavailable);
        assert_eq!(mark.value, None);
        assert_eq!(mark.reason.as_deref(), Some("invalid-upstream-value"));
    }

    #[test]
    fn lighter_patch_volume_one_member_and_zero() {
        let value = ticker(serde_json::json!({
            "market_id": 0,
            "daily_base_token_volume": 0,
            "daily_quote_token_volume": 12.5
        }));
        let fields =
            lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7)
                .unwrap();
        let MarketStatsValue::Volume24h(volume) = fields[&MarketStatsFieldName::Volume24h]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("volume variant");
        };
        assert_eq!(volume.base_volume, Some(0.0));
        assert_eq!(volume.quote_volume, Some(12.5));
    }

    #[test]
    fn lighter_patch_rejects_nonfinite_volume_member() {
        let value = ticker(serde_json::json!({
            "market_id": 0,
            "daily_base_token_volume": "NaN",
            "daily_quote_token_volume": 12.5
        }));
        let fields =
            lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7)
                .unwrap();
        let field = &fields[&MarketStatsFieldName::Volume24h];
        assert_eq!(field.state, MarketStatsFieldState::Unavailable);
        assert_eq!(field.reason.as_deref(), Some("invalid-upstream-value"));
    }

    #[test]
    fn lexical_and_decimal_validation() {
        assert_eq!(decimal_string("0.0000"), Some("0.0000"));
        assert_eq!(decimal_string("-0.001034"), Some("-0.001034"));
        assert_eq!(decimal_string("1e-8"), None);
        assert_eq!(decimal_string(""), None);
        assert_eq!(decimal_string(".5"), None);
        assert_eq!(decimal_string("1."), None);
        assert_eq!(unified_number(&Value::Float(f64::NAN)), None);
        assert_eq!(unified_number(&Value::Float(f64::INFINITY)), None);
        assert_eq!(
            unified_number(&Value::from_json(&serde_json::json!("1.5"))),
            Some(1.5)
        );
        assert_eq!(positive_integer(&Value::Int(0)), None);
        assert_eq!(
            positive_integer(&Value::from_json(&serde_json::json!("1672387200000"))),
            Some(1_672_387_200_000)
        );
    }

    #[test]
    fn bybit_inverse_volume_swaps_members() {
        let info = serde_json::json!({
            "volume24h": "13713832.0000",
            "turnover24h": "115.6907"
        });
        let catalog = entry(
            "BTC",
            "USD",
            UnifiedMarketType::Perp,
            Some(true),
            serde_json::json!({}),
        );
        let value = ticker(info);
        let src = sources(Some(&value));
        let (fields, _) = build_fields(Venue::Bybit, &catalog, &src, true);
        let MarketStatsValue::Volume24h(volume) = fields[&MarketStatsFieldName::Volume24h]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("volume variant");
        };
        assert_eq!(volume.base_volume, Some(115.6907));
        assert_eq!(volume.quote_volume, Some(13_713_832.0));
    }

    #[test]
    fn bybit_interval_ticker_wins_and_reports_mismatch() {
        let catalog = entry(
            "BTC",
            "USDT",
            UnifiedMarketType::Perp,
            Some(false),
            serde_json::json!({"info": {"fundingInterval": "240"}}),
        );
        let ticker = Value::from_json(&serde_json::json!({"info": {"fundingIntervalHour": "8"}}));
        assert_eq!(
            bybit_interval(&catalog, Some(&ticker)),
            (Some(28_800_000), true)
        );
        assert_eq!(bybit_interval(&catalog, None), (Some(14_400_000), false));
        for interval in [
            serde_json::json!(null),
            serde_json::json!(0),
            serde_json::json!(1.5),
            serde_json::json!("invalid"),
        ] {
            let ticker =
                Value::from_json(&serde_json::json!({"info": {"fundingIntervalHour": interval}}));
            assert_eq!(bybit_interval(&catalog, Some(&ticker)), (None, false));
        }
    }

    #[test]
    fn hyperliquid_never_synthesizes_last_price_and_keeps_quote_only_volume() {
        let info = serde_json::json!({
            "funding": "0.0000198",
            "markPx": "108.04",
            "oraclePx": "108.01",
            "dayNtlVlm": "299643445.12560016",
            "openInterest": "10764.48",
            "midPx": "108.02"
        });
        let value = ticker(info);
        let catalog = entry(
            "SOL",
            "USDC",
            UnifiedMarketType::Perp,
            Some(false),
            serde_json::json!({}),
        );
        let src = sources(Some(&value));
        let (fields, _) = build_fields(Venue::Hyperliquid, &catalog, &src, true);
        assert_eq!(
            fields[&MarketStatsFieldName::LastPrice].state,
            MarketStatsFieldState::Unsupported
        );
        let MarketStatsValue::Volume24h(volume) = fields[&MarketStatsFieldName::Volume24h]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("volume variant");
        };
        assert_eq!(volume.base_volume, None);
        assert_eq!(volume.quote_volume, Some(299_643_445.12560016));
        let MarketStatsValue::OpenInterest(oi) = fields[&MarketStatsFieldName::OpenInterest]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("oi variant");
        };
        assert_eq!(oi.open_interest_amount, Some(10764.48));
        assert_eq!(oi.open_interest_value, None);
        let MarketStatsValue::Funding(funding) = fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!("funding variant");
        };
        assert_eq!(funding.kind, FundingKind::CurrentUnclassified);
        assert_eq!(funding.next_payment_timestamp, None);
    }

    #[test]
    fn not_requested_fields_carry_no_receipt() {
        let catalog = entry(
            "BTC",
            "USDT",
            UnifiedMarketType::Perp,
            Some(false),
            serde_json::json!({}),
        );
        let src = sources(None);
        let (fields, receipts) = build_fields(Venue::Binance, &catalog, &src, false);
        for name in [
            MarketStatsFieldName::LastPrice,
            MarketStatsFieldName::Volume24h,
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::OpenInterest,
        ] {
            assert_eq!(fields[&name].reason.as_deref(), Some(NOT_REQUESTED));
            assert_eq!(fields[&name].received_timestamp, None);
            assert!(!receipts.contains_key(&name));
        }
        assert_eq!(
            fields[&MarketStatsFieldName::LastSettledFunding].state,
            MarketStatsFieldState::Unsupported
        );
    }

    #[test]
    fn lighter_perp_live_fields_are_not_requested_from_rest() {
        let catalog = entry(
            "ETH",
            "USDC",
            UnifiedMarketType::Perp,
            Some(false),
            serde_json::json!({}),
        );
        let value = ticker(serde_json::json!({
            "last_trade_price": "3013.13",
            "daily_base_token_volume": "1",
            "daily_quote_token_volume": "2"
        }));
        let src = sources(Some(&value));
        let (fields, _) = build_fields(Venue::Lighter, &catalog, &src, true);
        for name in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::LastSettledFunding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::IndexPrice,
        ] {
            assert_eq!(fields[&name].reason.as_deref(), Some(NOT_REQUESTED));
            assert_eq!(fields[&name].received_timestamp, None);
        }
        assert_eq!(
            fields[&MarketStatsFieldName::LastPrice].state,
            MarketStatsFieldState::Available
        );
        assert_eq!(
            fields[&MarketStatsFieldName::OpenInterest].state,
            MarketStatsFieldState::Unsupported
        );
    }

    #[test]
    fn per_call_receipts_do_not_freshen_siblings() {
        let catalog = entry(
            "BTC",
            "USDT",
            UnifiedMarketType::Perp,
            Some(false),
            serde_json::json!({}),
        );
        let value = ticker(serde_json::json!({
            "lastPrice": "100",
            "volume": "1",
            "quoteVolume": "2"
        }));
        let mut src = sources(Some(&value));
        src.ticker_receipt = Some(Receipt {
            at: Instant::now(),
            wall: 1,
        });
        // A selected OI call that completes later must not share the ticker receipt.
        let oi = Ok(Value::from_json(&serde_json::json!({
            "openInterestAmount": 12.5,
            "openInterestValue": null
        })));
        src.oi = Some(&oi);
        src.oi_receipt = Some(Receipt {
            at: Instant::now(),
            wall: 999,
        });
        let (fields, receipts) = build_fields(Venue::Binance, &catalog, &src, true);
        assert_eq!(
            fields[&MarketStatsFieldName::LastPrice].received_timestamp,
            Some(1)
        );
        assert_eq!(
            fields[&MarketStatsFieldName::OpenInterest].received_timestamp,
            Some(999)
        );
        assert_eq!(
            fields[&MarketStatsFieldName::OpenInterest]
                .source
                .as_deref(),
            Some(BINANCE_OI)
        );
        assert_eq!(
            receipts[&MarketStatsFieldName::OpenInterest],
            src.oi_receipt.unwrap().at
        );
    }
}
