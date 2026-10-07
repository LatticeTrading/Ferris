use super::*;
use crate::{
    exchanges::ccxt::statistics::{
        build_fields,
        test_support::{entry, sources, ticker},
    },
    models::MarketStatsFieldState,
};

#[test]
fn apex_funding_dates_are_not_guessed_and_numeric_zero_is_preserved() {
    let catalog = entry(
        "BTC",
        "USDT",
        UnifiedMarketType::Perp,
        Some(false),
        json!({}),
    );
    for (date, expected) in [
        (json!("2026-10-06T01:00:00Z"), Some(1791248400000)),
        (json!("2026-10-06T03:00:00+02:00"), Some(1791248400000)),
        (json!("10:00:00"), None),
        (json!("not-a-date"), None),
        (json!(null), None),
    ] {
        let raw = ticker(
            json!({"fundingRate":"0.0000", "predictedFundingRate":"0.5", "nextFundingTime":date,
            "openInterest":"0", "volume24h":0, "turnover24h":"0"}),
        );
        let data = sources(Some(&raw), &catalog, TICKERS);
        let (fields, _) = build_fields(Venue::Apex, &catalog, &data, true);
        let MarketStatsValue::Funding(value) = fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .unwrap()
        else {
            panic!();
        };
        assert_eq!(value.rate, "0.0000");
        assert_eq!(value.kind, FundingKind::CurrentUnclassified);
        assert_eq!(value.next_payment_timestamp, expected);
        assert_eq!(value.payment_timestamp, None);
        assert_eq!(value.rate_interval_ms, Some(3_600_000));
        assert_eq!(
            fields[&MarketStatsFieldName::OpenInterest].value,
            Some(MarketStatsValue::OpenInterest(OpenInterestValue {
                open_interest_amount: Some(0.0),
                open_interest_value: None,
            }))
        );
        assert_eq!(
            fields[&MarketStatsFieldName::LastSettledFunding].state,
            MarketStatsFieldState::Unsupported
        );
    }
}

#[test]
fn apex_invalid_and_missing_fields_do_not_erase_other_observations() {
    let catalog = entry(
        "BTC",
        "USDT",
        UnifiedMarketType::Perp,
        Some(false),
        json!({}),
    );
    for bad in [json!("NaN"), json!("Infinity"), json!("-1"), json!({})] {
        let raw = ticker(
            json!({"fundingRate":"oops", "markPrice":"0", "lastPrice":"123.4500",
            "openInterest":bad, "volume24h":bad, "turnover24h":"1"}),
        );
        let data = sources(Some(&raw), &catalog, TICKERS);
        let (fields, _) = build_fields(Venue::Apex, &catalog, &data, true);
        for name in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::OpenInterest,
            MarketStatsFieldName::Volume24h,
        ] {
            assert_eq!(
                fields[&name].reason.as_deref(),
                Some("invalid-upstream-value")
            );
        }
        assert_eq!(
            fields[&MarketStatsFieldName::LastPrice].state,
            MarketStatsFieldState::Available
        );
        assert_eq!(
            fields[&MarketStatsFieldName::IndexPrice].state,
            MarketStatsFieldState::Unavailable
        );
    }
}
