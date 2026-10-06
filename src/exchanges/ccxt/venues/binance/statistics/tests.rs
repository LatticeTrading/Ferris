use super::*;
use crate::exchanges::ccxt::statistics::{
    build_fields,
    test_support::{entry, sources, ticker},
};

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
    let mut src = sources(Some(&value), &catalog, BINANCE_TICKERS);
    src.receipts.insert(
        BINANCE_TICKERS,
        Receipt {
            at: Instant::now(),
            wall: 1,
        },
    );
    // A later selected OI call must not share the ticker receipt.
    let receipt = Receipt {
        at: Instant::now(),
        wall: 999,
    };
    src.singles.insert(
        (
            BINANCE_OI,
            catalog.market.identity.as_ref().unwrap().market_id.clone(),
        ),
        (
            Ok(Value::from_json(
                &serde_json::json!({"openInterestAmount":12.5,"openInterestValue":null}),
            )),
            receipt,
        ),
    );
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
    assert_eq!(receipts[&MarketStatsFieldName::OpenInterest], receipt.at);
}
