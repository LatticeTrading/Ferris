use super::*;

#[test]
fn tick_size_respects_precision_mode() {
    assert_eq!(tick_size_from_precision(Some(0.5), TICK_SIZE), Some(0.5));
    assert_eq!(tick_size_from_precision(Some(0.0), TICK_SIZE), None);
    assert_eq!(tick_size_from_precision(None, TICK_SIZE), None);

    assert_eq!(
        tick_size_from_precision(Some(2.0), DECIMAL_PLACES),
        Some(0.01)
    );
    assert_eq!(tick_size_from_precision(Some(2.5), DECIMAL_PLACES), None);
    assert_eq!(
        tick_size_from_precision(Some(-1.0), DECIMAL_PLACES),
        Some(10.0)
    );

    // Significant digits are not a fixed tick and must not be relabeled one.
    assert_eq!(
        tick_size_from_precision(Some(3.0), SIGNIFICANT_DIGITS),
        None
    );
    assert_eq!(tick_size_from_precision(Some(f64::NAN), TICK_SIZE), None);
}

#[test]
fn incomplete_or_nonfinite_candles_are_not_fabricated() {
    use ccxt::Value;
    let valid = vec![
        Value::Int(1_700_000_000_000),
        Value::Float(10.0),
        Value::Float(12.0),
        Value::Float(9.0),
        Value::Float(11.0),
        Value::Float(3.0),
    ];
    for missing in 0..5 {
        let mut cells = valid.clone();
        cells[missing] = Value::Null;
        assert!(matches!(
            convert_candles(Value::from(vec![Value::from(cells)])),
            Err(ExchangeError::UpstreamData(_))
        ));
    }
    let mut cells = valid.clone();
    cells[5] = Value::Float(f64::NAN);
    assert!(matches!(
        convert_candles(Value::from(vec![Value::from(cells)])),
        Err(ExchangeError::UpstreamData(_))
    ));
    assert_eq!(
        convert_candles(Value::from(vec![Value::from(valid)])).unwrap(),
        vec![(1_700_000_000_000, 10.0, 12.0, 9.0, 11.0, Some(3.0))]
    );
    let no_volume = serde_json::json!([[1_700_000_000_000u64, 10, 12, 9, 11, null]]);
    let candles = convert_candles(Value::from_json(&no_volume)).unwrap();
    assert_eq!(
        candles,
        vec![(1_700_000_000_000, 10.0, 12.0, 9.0, 11.0, None)]
    );
    assert!(serde_json::to_value(candles).unwrap()[0][5].is_null());
}

#[test]
fn invalid_levels_fail_the_entire_book_and_foreign_symbols_are_rejected() {
    use serde_json::json;
    let mut book = json!({"symbol":"BTC/USDT:USDT", "bids":[[10,2],[9,1]], "asks":[[11,3]]});
    for bad_level in [
        json!([9]),
        json!([9, null]),
        json!([9, -1]),
        json!(["NaN", 1]),
    ] {
        book["bids"][1] = bad_level;
        assert!(matches!(
            convert_book(ccxt::Value::from_json(&book), "BTC/USDT:USDT"),
            Err(ExchangeError::UpstreamData(_))
        ));
    }
    book["bids"][1] = json!([9, 1]);
    assert!(matches!(
        convert_book(ccxt::Value::from_json(&book), "ETH/USDT:USDT"),
        Err(ExchangeError::UpstreamData(_))
    ));
    let book = convert_book(ccxt::Value::from_json(&book), "BTC/USDT:USDT").unwrap();
    assert_eq!(book.bids, vec![(10.0, 2.0), (9.0, 1.0)]);
    assert_eq!(book.timestamp, None);
    assert_eq!(book.nonce, None);
}
