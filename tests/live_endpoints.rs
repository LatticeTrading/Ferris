use std::{env, time::Duration};

use reqwest::Client;
use serde_json::{json, Value};

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn base_url() -> String {
    env_or_default("FERRIS_BASE_URL", "http://127.0.0.1:8080")
}

fn exchange() -> String {
    env_or_default("FERRIS_TEST_EXCHANGE", "hyperliquid")
}

fn symbol() -> String {
    env_or_default("FERRIS_TEST_SYMBOL", "BTC/USDC:USDC")
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("failed to create reqwest client")
}

async fn post_json(client: &Client, path: &str, payload: Value) -> Value {
    let url = format!("{}{}", base_url(), path);
    let response = client
        .post(url.clone())
        .json(&payload)
        .send()
        .await
        .unwrap_or_else(|err| panic!("request failed for {url}: {err}"));

    let status = response.status();
    let body_text = response
        .text()
        .await
        .expect("failed to read response body as text");

    assert!(
        status.is_success(),
        "request to {url} failed with status={status} body={body_text}"
    );

    serde_json::from_str::<Value>(&body_text)
        .unwrap_or_else(|err| panic!("invalid json from {url}: {err}; body={body_text}"))
}

async fn get_json(client: &Client, path: &str) -> Value {
    let url = format!("{}{}", base_url(), path);
    let response = client
        .get(url.clone())
        .send()
        .await
        .unwrap_or_else(|err| panic!("request failed for {url}: {err}"));

    let status = response.status();
    let body_text = response
        .text()
        .await
        .expect("failed to read response body as text");

    assert!(
        status.is_success(),
        "request to {url} failed with status={status} body={body_text}"
    );

    serde_json::from_str::<Value>(&body_text)
        .unwrap_or_else(|err| panic!("invalid json from {url}: {err}; body={body_text}"))
}

#[tokio::test]
#[ignore = "requires a running backend; run with: cargo test --test live_endpoints -- --ignored"]
async fn live_healthz_ok() {
    let client = http_client();
    let body = get_json(&client, "/healthz").await;

    assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
}

#[tokio::test]
#[ignore = "requires a running backend; run with: cargo test --test live_endpoints -- --ignored"]
async fn live_fetch_trades_shape() {
    let client = http_client();
    let expected_symbol = symbol();
    let body = post_json(
        &client,
        "/v1/fetchTrades",
        json!({
            "exchange": exchange(),
            "symbol": expected_symbol,
            "limit": 5,
            "params": {}
        }),
    )
    .await;

    let trades = body
        .as_array()
        .expect("fetchTrades response must be an array");
    assert!(!trades.is_empty(), "fetchTrades returned no trades");

    let first = trades[0]
        .as_object()
        .expect("trade entry must be a json object");

    for key in [
        "info",
        "amount",
        "datetime",
        "id",
        "order",
        "price",
        "timestamp",
        "type",
        "side",
        "symbol",
        "takerOrMaker",
        "cost",
        "fee",
    ] {
        assert!(
            first.contains_key(key),
            "trade missing expected key `{key}`"
        );
    }

    assert_eq!(
        first.get("symbol").and_then(Value::as_str),
        Some(expected_symbol.as_str())
    );
}

#[tokio::test]
#[ignore = "requires a running backend; run with: cargo test --test live_endpoints -- --ignored"]
async fn live_fetch_ohlcv_shape() {
    let client = http_client();
    let expected_symbol = symbol();
    let body = post_json(
        &client,
        "/v1/fetchOHLCV",
        json!({
            "exchange": exchange(),
            "symbol": expected_symbol,
            "timeframe": "1m",
            "limit": 3,
            "params": {}
        }),
    )
    .await;

    let candles = body
        .as_array()
        .expect("fetchOHLCV response must be an array");
    assert!(!candles.is_empty(), "fetchOHLCV returned no candles");

    let first = candles[0]
        .as_array()
        .expect("OHLCV item must be an array tuple");
    assert_eq!(first.len(), 6, "OHLCV tuple must have 6 items");

    assert!(first[0].as_u64().is_some(), "OHLCV[0] must be timestamp");
    for index in 1..=5 {
        assert!(
            first[index].as_f64().is_some(),
            "OHLCV[{index}] must be a number"
        );
    }
}

#[tokio::test]
#[ignore = "requires a running backend; run with: cargo test --test live_endpoints -- --ignored"]
async fn live_fetch_order_book_shape() {
    let client = http_client();
    let expected_symbol = symbol();
    let body = post_json(
        &client,
        "/v1/fetchOrderBook",
        json!({
            "exchange": exchange(),
            "symbol": expected_symbol,
            "limit": 2,
            "params": {}
        }),
    )
    .await;

    let object = body
        .as_object()
        .expect("fetchOrderBook response must be an object");

    for key in ["asks", "bids", "datetime", "timestamp", "nonce", "symbol"] {
        assert!(
            object.contains_key(key),
            "order book missing expected key `{key}`"
        );
    }

    assert_eq!(
        object.get("symbol").and_then(Value::as_str),
        Some(expected_symbol.as_str())
    );

    let asks = object
        .get("asks")
        .and_then(Value::as_array)
        .expect("asks must be an array");
    let bids = object
        .get("bids")
        .and_then(Value::as_array)
        .expect("bids must be an array");

    assert!(!asks.is_empty(), "asks cannot be empty");
    assert!(!bids.is_empty(), "bids cannot be empty");

    let first_ask = asks[0].as_array().expect("ask level must be [price, size]");
    let first_bid = bids[0].as_array().expect("bid level must be [price, size]");

    assert_eq!(first_ask.len(), 2, "ask level must have 2 items");
    assert_eq!(first_bid.len(), 2, "bid level must have 2 items");
    assert!(first_ask[0].as_f64().is_some(), "ask price must be number");
    assert!(first_ask[1].as_f64().is_some(), "ask size must be number");
    assert!(first_bid[0].as_f64().is_some(), "bid price must be number");
    assert!(first_bid[1].as_f64().is_some(), "bid size must be number");
}
