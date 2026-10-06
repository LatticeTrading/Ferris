use super::*;
use crate::{
    exchanges::ccxt::statistics::{
        build_fields,
        test_support::{entry, sources, ticker},
    },
    models::MarketStatsFieldState,
};

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
    let src = sources(Some(&value), &catalog, HYPERLIQUID_TICKERS);
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
