use super::*;
use serde_json::json;

fn book(policy: BookPolicy) -> CcxtOrderBook {
    CcxtOrderBook {
        bids: vec![(100.0, 2.0), (50.0, 4.0)],
        asks: vec![(200.0, 3.0), (250.0, 0.0)],
        symbol: Some("BTC/USD:BTC".into()),
        timestamp: Some(123),
        nonce: Some(456),
        ..Default::default()
    }
    .with_policy(policy)
}

#[test]
fn linear_and_inverse_preserve_prices_provenance_and_zero() {
    for inverse in [false, true] {
        let native = book(BookPolicy::contracts(
            "BTC",
            "USD",
            Some(0.5),
            inverse,
            "test",
        ));
        let original = serde_json::to_value(&native).unwrap();
        assert_eq!(original["amountUnit"], "contracts");
        assert_eq!(original["amountCurrency"], Value::Null);
        assert_eq!(
            original["amountConversion"]["contractSizeApplied"],
            Value::Null
        );
        let base = native.into_amount_mode(BookAmountMode::Base).unwrap();
        assert_eq!(
            base.bids,
            if inverse {
                vec![(100.0, 0.01), (50.0, 0.04)]
            } else {
                vec![(100.0, 1.0), (50.0, 2.0)]
            }
        );
        assert_eq!(base.asks[1], (250.0, 0.0));
        let output = serde_json::to_value(&base).unwrap();
        for key in ["symbol", "timestamp", "datetime", "nonce"] {
            assert_eq!(output[key], original[key]);
        }
        assert_eq!(output["amountUnit"], "base");
        assert_eq!(output["sourceAmountUnit"], "contracts");
        assert_eq!(output["amountCurrency"], "BTC");
        assert_eq!(output["amountConversion"]["contractSizeApplied"], 0.5);
        assert_eq!(
            output["amountConversion"]["contractSizeCurrency"],
            if inverse { "USD" } else { "BTC" }
        );
        assert!(output.get("base_view").is_none());
        assert!(output.get("amount_policy").is_none());
        let twice = base.clone().into_amount_mode(BookAmountMode::Base).unwrap();
        assert_eq!(base.bids, twice.bids);
        assert_eq!(base.asks, twice.asks);
    }
}

#[test]
fn shared_base_view_is_lazy_once_per_publication_and_never_mutates_native() {
    let native = Arc::new(book(BookPolicy::contracts(
        "BTC",
        "USDT",
        Some(0.001),
        false,
        "test",
    )));
    assert!(native.base_view.get().is_none());
    let unchanged = native.amount_view(BookAmountMode::Native).unwrap();
    assert!(Arc::ptr_eq(&native, &unchanged));
    assert!(native.base_view.get().is_none());
    let workers = (0..8)
        .map(|_| {
            let native = native.clone();
            std::thread::spawn(move || native.amount_view(BookAmountMode::Base).unwrap())
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert!(results.iter().all(|b| Arc::ptr_eq(&results[0], b)));
    assert_eq!(native.bids[0].1, 2.0);
    assert_eq!(results[0].bids[0].1, 0.002);
    let identity = Arc::new(book(BookPolicy::base("BTC", "test")));
    assert!(Arc::ptr_eq(
        &identity,
        &identity.amount_view(BookAmountMode::Base).unwrap()
    ));
}

#[test]
fn unqualified_metadata_fails_closed_even_for_empty_books() {
    for size in [
        None,
        Some(0.0),
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
    ] {
        let native = book(BookPolicy::contracts("BTC", "USD", size, true, "test"));
        assert!(native
            .clone()
            .into_amount_mode(BookAmountMode::Native)
            .is_ok());
        assert!(matches!(
            native.into_amount_mode(BookAmountMode::Base),
            Err(ExchangeError::UnsupportedFeature(_))
        ));
    }
    assert!(CcxtOrderBook::default()
        .into_amount_mode(BookAmountMode::Base)
        .is_err());
    assert_eq!(
        serde_json::to_value(CcxtOrderBook::default()).unwrap()["amountUnit"],
        "unknown"
    );
}

#[test]
fn invalid_levels_and_nonfinite_conversions_never_change_native() {
    for (price, amount, size) in [
        (0.0, 1.0, 1.0),
        (-1.0, 1.0, 1.0),
        (f64::NAN, 1.0, 1.0),
        (1.0, -1.0, 1.0),
        (1.0, f64::INFINITY, 1.0),
        (1.0, f64::MAX, 2.0),
        (1.0, f64::MIN_POSITIVE, f64::MIN_POSITIVE),
    ] {
        let mut native = book(BookPolicy::contracts(
            "BTC",
            "USD",
            Some(size),
            true,
            "test",
        ));
        native.asks = vec![(price, amount)];
        let native = Arc::new(native);
        assert!(matches!(
            native.amount_view(BookAmountMode::Base),
            Err(ExchangeError::UpstreamData(_))
        ));
        assert_eq!(native.bids[0], (100.0, 2.0));
        assert!(native.amount_view(BookAmountMode::Native).is_ok());
    }
}

#[test]
fn mode_validation_and_dynamic_liquidity_are_independent_of_amounts() {
    for params in [
        Value::Null,
        json!({}),
        json!({"amountUnit":null}),
        json!({"amountUnit":"native"}),
    ] {
        assert_eq!(
            BookAmountMode::parse(&params).unwrap(),
            BookAmountMode::Native
        );
    }
    assert_eq!(
        BookAmountMode::parse(&json!({"amountUnit":"base"})).unwrap(),
        BookAmountMode::Base
    );
    for invalid in [
        json!(true),
        json!([]),
        json!({"amountUnit":"contracts"}),
        json!({"amountUnit":1}),
        json!({"amountUnit":"BASE"}),
    ] {
        assert!(BookAmountMode::parse(&invalid).is_err());
    }
    let policy =
        BookPolicy::base("BTC", "extended").with_liquidity(LiquidityKind::Indicative, 1_000);
    assert_eq!(policy.metadata(1_000).amount_unit, AmountUnit::Base);
    assert_eq!(
        policy.metadata(30_999).liquidity_kind,
        LiquidityKind::Indicative
    );
    assert_eq!(
        policy.metadata(31_000).liquidity_kind,
        LiquidityKind::Unknown
    );
    assert_eq!(policy.metadata(999).liquidity_kind, LiquidityKind::Unknown);
    assert!(policy.same_definition(
        &policy
            .clone()
            .with_liquidity(LiquidityKind::Indicative, 2_000)
    ));
    assert!(!policy.same_definition(
        &policy
            .clone()
            .with_liquidity(LiquidityKind::OrderBook, 2_000)
    ));
}

#[tokio::test]
async fn receiver_selects_shared_views_and_reports_conversion_errors_without_native_fallback() {
    use crate::realtime::{DeliveryState, Publication, RealtimeReceiver, RealtimeUpdate};
    use tokio::sync::{broadcast, Notify};
    let (tx, rx) = broadcast::channel(8);
    let state = Arc::new(DeliveryState::default());
    let released = Arc::new(Notify::new());
    let mut native = RealtimeReceiver::new(rx, state.clone(), released.clone());
    let mut base = RealtimeReceiver::new(tx.subscribe(), state, released)
        .with_book_amount_mode(BookAmountMode::Base);
    let raw = Arc::new(book(BookPolicy::contracts(
        "BTC",
        "USD",
        Some(1.0),
        true,
        "test",
    )));
    tx.send(Publication::new(RealtimeUpdate::OrderBook(raw)))
        .ok()
        .unwrap();
    let RealtimeUpdate::OrderBook(raw) = native.recv().await.unwrap() else {
        panic!("native book")
    };
    let RealtimeUpdate::OrderBook(converted) = base.recv().await.unwrap() else {
        panic!("base book")
    };
    assert_eq!(raw.bids[0].1, 2.0);
    assert_eq!(converted.bids[0].1, 0.02);
    tx.send(Publication::new(RealtimeUpdate::OrderBook(Arc::new(
        CcxtOrderBook::default(),
    ))))
    .ok()
    .unwrap();
    assert!(matches!(
        native.recv().await.unwrap(),
        RealtimeUpdate::OrderBook(_)
    ));
    assert!(
        matches!(base.recv().await.unwrap(), RealtimeUpdate::Error(message) if message.contains("AMOUNT_NORMALIZATION_UNAVAILABLE"))
    );
}
