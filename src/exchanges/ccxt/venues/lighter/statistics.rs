//! Lighter statistics: stock acquisition and native field semantics.

use std::collections::BTreeMap;

use ccxt::{value::get_value_k, Value};
use serde_json::{json, Map, Value as JsonValue};
use tokio::time::Instant;

use crate::{
    exchanges::{
        ccxt::{
            catalog::{Catalog, CatalogMarket},
            statistics::{
                acquisition::{acquire_tickers, Acquired},
                fields::{
                    invalid, missing, not_requested_pair, observed, price_field, price_value,
                    volume_field,
                },
                scalars::{lexical, nonneg_number, positive_integer},
                Receipt,
            },
            statistics_profile::{LiveStatisticsPolicy, Profile},
            stream::statistics::StatisticsFeed,
            venue::{CatalogScope, Provider, Venue},
        },
        traits::ExchangeError,
    },
    models::{
        FetchMarketStatsParams, FundingKind, FundingRateUnit, FundingValue, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsSourceFailure, MarketStatsValue,
        OpenInterestValue, UnifiedMarketType, Volume24hValue,
    },
};

const LIGHTER_TICKERS: &str = "lighterxyz:ccxt:fetchTickers";

pub(in crate::exchanges::ccxt) async fn acquire(
    provider: &mut Provider,
    _scope: CatalogScope,
    _catalog: &Catalog,
    params: &FetchMarketStatsParams,
    acquired: &mut Acquired,
    failures: &mut Vec<MarketStatsSourceFailure>,
) {
    let venue = Venue::Lighter;
    if params.include_bulk {
        acquire_tickers(venue, LIGHTER_TICKERS, provider, acquired, failures).await;
    }
}

pub(in crate::exchanges::ccxt) fn observe(
    entry: &CatalogMarket,
    name: MarketStatsFieldName,
    data: &Acquired,
) -> (MarketStatsField, Option<Instant>) {
    let base = entry.market.base.as_str();
    let quote = entry.market.quote.as_str();
    let ticker = data.row(LIGHTER_TICKERS, entry);
    match name {
        MarketStatsFieldName::LastPrice => price_field(
            LIGHTER_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "last_trade_price",
            base,
            quote,
        ),
        MarketStatsFieldName::Volume24h => volume_field(
            LIGHTER_TICKERS,
            ticker.row,
            ticker.error,
            ticker.receipt,
            "daily_base_token_volume",
            "daily_quote_token_volume",
        ),
        MarketStatsFieldName::Funding
        | MarketStatsFieldName::LastSettledFunding
        | MarketStatsFieldName::MarkPrice
        | MarketStatsFieldName::IndexPrice
        | MarketStatsFieldName::OpenInterest => not_requested_pair(),
    }
}

/// Source label for the maintained Lighter `market_stats/all` stream, which is
/// the source for Lighter funding/mark/index/two-sided OI and any optional
/// last-price/volume patches. Also used for maintenance failure attribution.
pub(in crate::exchanges::ccxt) const LIGHTER_LIVE_SOURCE: &str = "lighterxyz:ccxt:watchTickers";

/// Map one stock Lighter `watchTickers` row to a sparse statistics patch.
///
/// Only native keys present in the row are emitted: absence must not overwrite
/// or freshen a prior observation. `funding_timestamp` is the settled payment
/// time only and is never used as an observation timestamp.
pub(in crate::exchanges::ccxt) fn lighter_ticker_patch(
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
    if let Some(raw) = info.get("open_interest") {
        fields.insert(
            MarketStatsFieldName::OpenInterest,
            lighter_open_interest(raw, receipt),
        );
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

fn lighter_open_interest(raw: &Value, receipt: Receipt) -> MarketStatsField {
    match nonneg_number(raw) {
        Err(()) => invalid(LIGHTER_LIVE_SOURCE, receipt),
        Ok(None) => MarketStatsField {
            received_timestamp: Some(receipt.wall),
            ..missing(LIGHTER_LIVE_SOURCE, None)
        },
        Ok(Some(one_sided)) => {
            // WS OI is one-sided USDC notional; Lighter displays longs + shorts.
            let two_sided = 2.0 * one_sided;
            if !two_sided.is_finite() {
                return invalid(LIGHTER_LIVE_SOURCE, receipt);
            }
            observed(
                LIGHTER_LIVE_SOURCE,
                MarketStatsValue::OpenInterest(OpenInterestValue {
                    open_interest_amount: None,
                    open_interest_value: Some(two_sided),
                }),
                None,
                Some(receipt),
            )
        }
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

use crate::models::UnifiedMarketType::{Perp, Spot};

pub(in crate::exchanges::ccxt) const PROFILE: Profile = Profile {
    catalog_source: "lighterxyz:ccxt:loadMarkets",
    products: &[Spot, Perp],
    unsupported: &[],
    funding_kinds: &[FundingKind::Estimate, FundingKind::Settled],
    hourly: true,
    percent: true,
    selected_open_interest: None,
    expire_funding_at_payment: false,
    live: Some(&LiveStatisticsPolicy {
        feed: StatisticsFeed::Lighter,
        scope: CatalogScope::Default,
        source: LIGHTER_LIVE_SOURCE,
        // These fields fail even before a first live observation. Last/volume
        // also fail when live-sourced, but healthy REST observations survive.
        failure_products: &[Perp],
        failure_fields: &[
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::LastSettledFunding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::IndexPrice,
            MarketStatsFieldName::OpenInterest,
        ],
    }),
    live_perp_fields: &[
        MarketStatsFieldName::Funding,
        MarketStatsFieldName::LastSettledFunding,
        MarketStatsFieldName::MarkPrice,
        MarketStatsFieldName::IndexPrice,
        MarketStatsFieldName::OpenInterest,
    ],
    limitations: &[
        "funding-prices-via-stock-watchTickers",
        "open-interest-two-sided-USDC-value-only-via-stock-watchTickers",
    ],
};

pub(in crate::exchanges::ccxt) fn catalog_products(
    _scope: CatalogScope,
) -> &'static [UnifiedMarketType] {
    &[Perp, Spot]
}

pub(in crate::exchanges::ccxt) fn normalize(
    _scope: CatalogScope,
    product: Option<UnifiedMarketType>,
    selectors: &Map<String, JsonValue>,
) -> JsonValue {
    if product == Some(UnifiedMarketType::Spot)
        || selectors.get("category").and_then(JsonValue::as_str) == Some("spot")
    {
        json!({"type":"spot"})
    } else {
        json!({})
    }
}

pub(in crate::exchanges::ccxt) fn acquisition_params(_params: &JsonValue) -> JsonValue {
    json!({})
}

pub(in crate::exchanges::ccxt) use super::super::defaults::statistics_noop_param as accepts_noop_param;

pub(in crate::exchanges::ccxt) fn selection_matches(
    _product: UnifiedMarketType,
    category: Option<&str>,
    dex: Option<&str>,
    native_id: &str,
    _params: &JsonValue,
) -> bool {
    category.is_none()
        && dex.is_none()
        && !(native_id.len() > 1 && native_id.starts_with('0'))
        && native_id.bytes().all(|byte| byte.is_ascii_digit())
        && native_id.parse::<u64>().is_ok()
}

pub(in crate::exchanges::ccxt) fn live_catalog_key(entry: &CatalogMarket) -> Option<String> {
    entry
        .market
        .identity
        .as_ref()
        .map(|id| id.exchange_market_id.clone())
}

/// `market_stats/all` is perpetual-only, though the shared catalog has spot too.
pub(in crate::exchanges::ccxt) fn live_market(entry: &CatalogMarket) -> bool {
    entry.market.market_type == UnifiedMarketType::Perp
}

pub(in crate::exchanges::ccxt) fn live_row_key(ticker: &Value) -> Option<String> {
    let info = get_value_k(ticker, "info");
    match get_value_k(&info, "market_id") {
        Value::Str(value) => Some(value.to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

/// True when a settled frame is a Lighter `market_stats` ticker rather than a
/// book/trade payload sharing the same URL. Books carry `bids`/`asks`; trades
/// carry trade keys, not market-statistics keys.
pub(in crate::exchanges::ccxt) fn lighter_ticker_frame(raw: &ccxt::Value) -> bool {
    let Some(fields) = raw.as_map() else {
        return false;
    };
    if fields.contains_key("bids") || fields.contains_key("asks") {
        return false;
    }
    let info = ccxt::value::get_value_k(raw, "info");
    let Some(info) = info.as_map() else {
        return false;
    };
    info.contains_key("market_id")
        && [
            "mark_price",
            "index_price",
            "open_interest",
            "last_trade_price",
            "current_funding_rate",
            "daily_base_token_volume",
        ]
        .iter()
        .any(|key| info.contains_key(*key))
}
