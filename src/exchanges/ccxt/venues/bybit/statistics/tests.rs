use super::*;
use crate::{
    exchanges::ccxt::statistics::{
        build_fields,
        test_support::{entry, sources, ticker},
    },
    models::MarketStatsValue,
};

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
    let src = sources(Some(&value), &catalog, BYBIT_TICKERS);
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
