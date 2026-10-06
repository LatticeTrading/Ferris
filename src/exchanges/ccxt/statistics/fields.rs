//! Field states, provenance and per-method receipts. No exchange dispatch.

use ccxt::{value::get_value_k, Value};
use tokio::time::Instant;

use crate::{
    exchanges::traits::ExchangeError,
    models::{
        FundingKind, FundingRateUnit, FundingValue, MarketStatsField, MarketStatsFieldState,
        MarketStatsValue, OpenInterestValue, PriceValue, Volume24hValue,
    },
};

use super::{
    failure_reason,
    scalars::{exchange_time, nonneg_member, nonneg_number, positive_integer, raw_lexical},
    Receipt, NOT_REQUESTED,
};

pub(in crate::exchanges::ccxt) fn price_field(
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

pub(in crate::exchanges::ccxt) fn price_info_field(
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
pub(in crate::exchanges::ccxt) fn funding_field(
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

pub(in crate::exchanges::ccxt) fn volume_field(
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

pub(in crate::exchanges::ccxt) fn oi_field(
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

pub(in crate::exchanges::ccxt) fn price_value(
    raw: Option<String>,
    base: &str,
    quote: &str,
) -> Option<MarketStatsValue> {
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

pub(in crate::exchanges::ccxt) fn observed(
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

pub(in crate::exchanges::ccxt) fn invalid(source: &str, receipt: Receipt) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        exchange_timestamp: None,
        received_timestamp: Some(receipt.wall),
        source: Some(source.to_string()),
    }
}

pub(in crate::exchanges::ccxt) fn missing(
    source: &str,
    reason: Option<&'static str>,
) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(reason.unwrap_or("missing-upstream-row").to_string()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: Some(source.to_string()),
    }
}

pub(in crate::exchanges::ccxt) fn missing_pair(
    source: &'static str,
    error: Option<&'static str>,
) -> (MarketStatsField, Option<Instant>) {
    (missing(source, error), None)
}

pub(in crate::exchanges::ccxt) fn invalid_pair(
    source: &'static str,
    receipt: Option<Receipt>,
) -> (MarketStatsField, Option<Instant>) {
    match receipt {
        Some(receipt) => (invalid(source, receipt), Some(receipt.at)),
        None => (missing(source, None), None),
    }
}

pub(in crate::exchanges::ccxt) fn not_requested_pair() -> (MarketStatsField, Option<Instant>) {
    (not_requested(), None)
}

pub(in crate::exchanges::ccxt) fn unavailable(reason: &str) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(reason.to_string()),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}

pub(in crate::exchanges::ccxt) fn not_requested() -> MarketStatsField {
    unavailable(NOT_REQUESTED)
}

pub(in crate::exchanges::ccxt) fn fixed(
    state: MarketStatsFieldState,
    reason: Option<&str>,
) -> MarketStatsField {
    MarketStatsField {
        state,
        value: None,
        reason: reason.map(str::to_string),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}
