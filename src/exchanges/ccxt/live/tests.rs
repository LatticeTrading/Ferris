mod catalog;

use super::*;
use crate::exchanges::ccxt::{statistics_profile, Venue};

fn feed() -> StatisticsFeed {
    statistics_profile::live_statistics(Venue::Lighter)
        .unwrap()
        .feed
}
use serde_json::json;

fn value(input: serde_json::Value) -> ccxt::Value {
    ccxt::Value::from_json(&input)
}

#[test]
fn statistics_frame_is_distinguished_from_book_and_trade() {
    let ticker = value(json!({
        "symbol": "ETH/USDC:USDC",
        "info": {
            "market_id": 0,
            "mark_price": "3013.91",
            "index_price": "3015.56",
            "last_trade_price": "3013.13",
            "current_funding_rate": "0.0012",
            "funding_rate": "0.0012",
            "daily_base_token_volume": 643235.2763,
        }
    }));
    let book = value(json!({
        "symbol": "ETH/USDC:USDC",
        "bids": [[3013.0, 1.0]],
        "asks": [[3014.0, 1.0]],
    }));
    let trade = value(json!({
        "symbol": "ETH/USDC:USDC",
        "info": {
            "market_id": 0,
            "trade_id": 526801155,
            "size": "0.0346",
            "price": "3028.85",
        }
    }));
    let ack = ccxt::Value::Bool(true);
    let book_with_info = value(json!({
        "symbol": "ETH/USDC:USDC",
        "bids": [[3013.0, 1.0]],
        "asks": [[3014.0, 1.0]],
        "info": {"market_id": 0, "mark_price": "3013.91"},
    }));
    assert!(feed().recognizes(&ticker));
    assert!(!feed().recognizes(&book));
    assert!(!feed().recognizes(&book_with_info));
    assert!(!feed().recognizes(&trade));
    assert!(!feed().recognizes(&ack));
}

#[test]
fn unchanged_rows_keep_their_receipts_across_repeated_frames() {
    let first = value(json!({
        "symbol": "ETH/USDC:USDC",
        "info": {"market_id": 0, "mark_price": "3013.91"},
    }));
    let second = value(json!({
        "symbol": "SOL/USDC:USDC",
        "info": {"market_id": 1, "mark_price": "1.15954"},
    }));
    let book = value(json!({"symbol": "ETH/USDC:USDC", "bids": [], "asks": []}));
    let mut tickers = value(json!({}));
    ccxt::set_value(
        &mut tickers,
        &ccxt::Value::from("ETH/USDC:USDC"),
        first.clone(),
    );
    ccxt::set_value(
        &mut tickers,
        &ccxt::Value::from("SOL/USDC:USDC"),
        second.clone(),
    );
    ccxt::set_value(&mut tickers, &ccxt::Value::from("BTC/USDC:USDC"), book);

    // A finite multi-row frame delivers every changed row at once.
    let changed = changed_tickers(feed(), &HashMap::<String, ccxt::Value>::new(), &tickers);
    assert_eq!(changed.len(), 2);

    // The held cache values make unchanged rows pointer-identical, so a
    // later identical frame has nothing new to publish.
    let marks: HashMap<String, ccxt::Value> = changed
        .iter()
        .map(|(symbol, ticker)| (symbol.clone(), (*ticker).clone()))
        .collect();
    assert!(changed_tickers(feed(), &marks, &tickers).is_empty());

    // Replacing one symbol's cache value (stock re-parse) re-delivers only it.
    let mut replaced = tickers.clone();
    ccxt::set_value(
        &mut replaced,
        &ccxt::Value::from("ETH/USDC:USDC"),
        value(json!({
            "symbol": "ETH/USDC:USDC",
            "info": {"market_id": 0, "mark_price": "3014.00"},
        })),
    );
    let changed = changed_tickers(feed(), &marks, &replaced);
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].0, "ETH/USDC:USDC");
}

#[test]
fn feed_identity_is_native_and_perpetual_only() {
    use crate::exchanges::ccxt::statistics::test_support::entry;
    use crate::models::UnifiedMarketType;

    for native in [json!(7), json!("7"), json!(7.0)] {
        let ticker = value(json!({"symbol":"wrong", "info":{"market_id":native}}));
        assert_eq!(feed().row_key(&ticker), Some("7".into()));
    }
    for native in [json!(null), json!(true)] {
        assert_eq!(
            feed().row_key(&value(json!({"info":{"market_id":native}}))),
            None
        );
    }
    let perp = entry("7", "USDC", UnifiedMarketType::Perp, Some(false), json!({}));
    let spot = entry("7", "USDC", UnifiedMarketType::Spot, None, json!({}));
    assert_eq!(feed().catalog_key(&perp), Some("7".into()));
    assert_eq!(feed().catalog_key(&spot), Some("7".into()));
    assert!(feed().accepts_market(&perp));
    assert!(!feed().accepts_market(&spot));
}
