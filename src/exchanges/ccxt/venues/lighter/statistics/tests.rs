use super::*;
use crate::exchanges::ccxt::statistics::{
    build_fields,
    test_support::{entry, sources, ticker},
    NOT_REQUESTED,
};

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
        lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7).unwrap();
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
fn lighter_open_interest_doubles_notional_and_rejects_invalid_values() {
    let receipt = Receipt {
        at: Instant::now(),
        wall: 42,
    };
    for (raw, expected) in [
        (Value::from("118223213"), 236446426.0),
        (Value::Float(123.25), 246.5),
        (Value::Int(0), 0.0),
    ] {
        let field = lighter_open_interest(&raw, receipt);
        assert_eq!(field.state, MarketStatsFieldState::Available);
        assert_eq!(
            serde_json::to_value(field.value).unwrap(),
            serde_json::json!({"openInterestAmount":null,"openInterestValue":expected})
        );
        assert_eq!(field.received_timestamp, Some(42));
        assert_eq!(field.exchange_timestamp, None);
    }
    for raw in [
        Value::from("-1"),
        Value::from("not-a-number"),
        Value::from("NaN"),
        Value::Float(f64::INFINITY),
        Value::Float(f64::MAX),
    ] {
        let field = lighter_open_interest(&raw, receipt);
        assert_eq!(field.state, MarketStatsFieldState::Unavailable);
        assert_eq!(field.value, None);
        assert_eq!(field.reason.as_deref(), Some("invalid-upstream-value"));
    }
    let field = lighter_open_interest(&Value::from_json(&JsonValue::Null), receipt);
    assert_eq!(field.state, MarketStatsFieldState::Unavailable);
    assert_eq!(field.value, None);
    assert_eq!(field.reason.as_deref(), Some("missing-upstream-row"));
    assert_eq!(field.received_timestamp, Some(42));
}

#[test]
fn lighter_patch_volume_one_member_and_zero() {
    let value = ticker(serde_json::json!({
        "market_id": 0,
        "daily_base_token_volume": 0,
        "daily_quote_token_volume": 12.5
    }));
    let fields =
        lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7).unwrap();
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
        lighter_ticker_patch(&value, &serde_json::json!({"base":"ETH","quote":"USDC"}), 7).unwrap();
    let field = &fields[&MarketStatsFieldName::Volume24h];
    assert_eq!(field.state, MarketStatsFieldState::Unavailable);
    assert_eq!(field.reason.as_deref(), Some("invalid-upstream-value"));
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
        "daily_quote_token_volume": "2",
        "open_interest": "43923.19"
    }));
    let src = sources(Some(&value), &catalog, LIGHTER_TICKERS);
    let (fields, _) = build_fields(Venue::Lighter, &catalog, &src, true);
    for name in [
        MarketStatsFieldName::Funding,
        MarketStatsFieldName::LastSettledFunding,
        MarketStatsFieldName::MarkPrice,
        MarketStatsFieldName::IndexPrice,
        MarketStatsFieldName::OpenInterest,
    ] {
        assert_eq!(fields[&name].reason.as_deref(), Some(NOT_REQUESTED));
        assert_eq!(fields[&name].received_timestamp, None);
    }
    assert_eq!(
        fields[&MarketStatsFieldName::LastPrice].state,
        MarketStatsFieldState::Available
    );
}
