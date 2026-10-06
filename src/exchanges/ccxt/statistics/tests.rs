use ccxt::Value;

use super::{
    build_fields,
    scalars::{decimal_string, positive_integer, unified_number},
    test_support::{entry, sources},
    NOT_REQUESTED,
};
use crate::{
    exchanges::ccxt::venue::Venue,
    models::{MarketStatsFieldName, MarketStatsFieldState, UnifiedMarketType},
};

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
fn not_requested_fields_carry_no_receipt() {
    let catalog = entry(
        "BTC",
        "USDT",
        UnifiedMarketType::Perp,
        Some(false),
        serde_json::json!({}),
    );
    let src = sources(None, &catalog, "binance:ccxt:fetchTickers");
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
