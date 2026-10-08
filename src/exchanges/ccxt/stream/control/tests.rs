use super::*;
use crate::{
    exchanges::ccxt::{venues, Venue},
    realtime::RealtimeChannel,
};
use serde_json::json;

struct TestProtocol;
impl Protocol for TestProtocol {
    type Core = ccxt::exchange::BaseCore;
    fn subscription(_: &LiveSpec, _: &Value) -> Result<Subscription, ExchangeError> {
        unreachable!()
    }
    fn unsubscribe(topic: &str) -> Value {
        Value::from_json(&json!({"op":"unsubscribe","args":[topic]}))
    }
    fn incoming(value: &Value) -> Incoming {
        let value = value.to_json();
        if let Some(kind) = value["ack"].as_str() {
            Incoming::Ack {
                topics: value["topics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect(),
                unsubscribe: kind == "unsubscribe",
                accepted: value["ok"] == true,
            }
        } else {
            Incoming::Data {
                topic: value["topic"].as_str().unwrap().into(),
                snapshot: value["snapshot"] == true,
            }
        }
    }
}
type Driver = Controlled<ccxt::exchange::BaseCore, TestProtocol>;

struct ReplayProtocol;
impl Protocol for ReplayProtocol {
    type Core = ccxt::exchange::BaseCore;
    const REPLAY_BOOK_DELTAS: bool = true;
    fn subscription(_: &LiveSpec, _: &Value) -> Result<Subscription, ExchangeError> {
        unreachable!()
    }
    fn unsubscribe(_: &str) -> Value {
        Value::Null
    }
    fn incoming(_: &Value) -> Incoming {
        Incoming::Other
    }
}

fn driver() -> Driver {
    Controlled::new(ccxt::exchange::BaseCore::new(Exchange::new(None)))
}
fn held(topic: &str) -> Held {
    Held {
        subscription: Subscription {
            topic: topic.into(),
            hash: format!("hash:{topic}"),
            snapshot_required: true,
        },
        retiring: false,
        unsubscribe_sent: false,
        awaiting_subscribe: true,
        awaiting_snapshot: true,
        channel_id: None,
    }
}
fn input(json: serde_json::Value) -> Value {
    Value::from_json(&json)
}

#[test]
fn unsubscribe_policy_is_per_channel_and_preserves_stock_defaults() {
    for venue in Venue::ALL {
        for channel in [
            LiveChannel::Client(RealtimeChannel::Trades),
            LiveChannel::Client(RealtimeChannel::OrderBook),
            LiveChannel::Client(RealtimeChannel::Ohlcv),
            LiveChannel::Statistics,
        ] {
            let actual =
                venues::dispatch!(venue, exchange => exchange::stream::unsubscribe_mode(channel));
            let expected = match (venue, channel) {
                (Venue::Extended, _)
                | (Venue::Apex | Venue::Bitfinex | Venue::Kucoin, LiveChannel::Statistics) => {
                    UnsubscribeMode::Reconnect
                }
                (Venue::Apex | Venue::Bitfinex, _) => UnsubscribeMode::Native,
                _ => UnsubscribeMode::Stock,
            };
            assert_eq!(actual, expected, "{venue:?} {channel:?}");
        }
    }
}

#[test]
fn native_control_requires_ordered_ack_and_snapshot_and_retires_only_matching_topics() {
    let mut driver = driver();
    driver.held.insert("book".into(), held("book"));
    let snapshot = input(json!({"topic":"book","snapshot":true}));
    assert!(driver.intercept(&input(json!({"topic":"book","snapshot":false}))));
    assert!(!driver.intercept(&snapshot)); // Apex sends snapshots BEFORE subscribe ACK
    driver.intercept(&input(
        json!({"ack":"subscribe","topics":["book"],"ok":true}),
    ));
    assert!(!driver.intercept(&input(json!({"topic":"book","snapshot":false}))));
    driver.held.get_mut("book").unwrap().retiring = true;
    let ack = input(json!({"ack":"unsubscribe","topics":["book","unknown"],"ok":true}));
    driver.intercept(&ack); // not sent yet; a stale ACK cannot retire this generation
    assert!(driver.events.is_empty());
    driver.held.get_mut("book").unwrap().unsubscribe_sent = true;
    assert!(driver.intercept(&snapshot));
    driver.intercept(&ack);
    assert!(
        matches!(driver.events.pop_front(),Some(Ok(LiveEvent::Unsubscribed(hash))) if hash=="hash:book")
    );
    driver.intercept(&ack);
    assert!(driver.events.is_empty());
    assert!(driver.held.is_empty());
    driver.held.insert("book".into(), held("book"));
    driver.intercept(&ack); // duplicate from previous generation is harmless while active
    assert!(driver.held.contains_key("book"));
    assert!(driver.intercept(&input(json!({"topic":"book","snapshot":false}))));
    assert!(!driver.intercept(&snapshot));
}

#[test]
fn one_batched_ack_preserves_all_retirement_events() {
    let mut driver = driver();
    for topic in ["one", "two"] {
        let mut held = held(topic);
        held.retiring = true;
        held.unsubscribe_sent = true;
        driver.held.insert(topic.into(), held);
    }
    driver.intercept(&input(
        json!({"ack":"unsubscribe","topics":["one","two"],"ok":true}),
    ));
    assert_eq!(driver.events.len(), 2);
    assert!(driver.held.is_empty());
}

#[test]
fn bitfinex_channel_ids_fence_retirement_and_cleanup_stock_routes() {
    use venues::bitfinex::stream::BitfinexControl;
    let url = "ws://bitfinex-control-test.invalid/cleanup";
    ccxt_pro::pro::ws_client::mock_setup(url);
    let mut driver: Controlled<_, BitfinexControl> =
        Controlled::new(ccxt_pro::pro::bitfinex::BitfinexCore::new(None));
    driver.bind_url(url);
    driver
        .held
        .insert("book:tBTCUSD".into(), held("book:tBTCUSD"));
    assert!(!driver.intercept(&input(
        json!({"event":"subscribed","channel":"book","symbol":"tBTCUSD","chanId":7})
    )));
    let client = ccxt_pro::pro::ws_client::get_client(url).unwrap();
    ccxt_pro::pro::ws_client::value_subs_insert(url, "7", input(json!({"channel":"book"})));
    ccxt_pro::pro::ws_client::value_subs_insert(
        url,
        "unsubscribe:orderbook:BTC/USD",
        Value::from("7"),
    );
    ccxt_pro::pro::ws_client::value_subs_insert(url, "unrelated", Value::from("8"));
    let retired = input(json!({"event":"unsubscribed","status":"OK","chanId":7}));
    driver.intercept(&retired); // active, not retiring
    assert!(driver.events.is_empty());
    let book = driver.held.get_mut("book:tBTCUSD").unwrap();
    book.retiring = true;
    book.unsubscribe_sent = true;
    driver.intercept(&retired);
    assert_eq!(driver.events.len(), 1);
    assert!(driver.routes.is_empty());
    let subs = client.subscriptions_value();
    assert!(ccxt::value::get_value_k(&subs, "7").is_null());
    assert!(ccxt::value::get_value_k(&subs, "unsubscribe:orderbook:BTC/USD").is_null());
    assert_eq!(
        ccxt::value::get_value_k(&subs, "unrelated").as_str(),
        Some("8")
    );
    driver.events.clear();
    assert!(driver.intercept(&input(json!([7, [[1, 1, 2]]]))));
    driver.intercept(&retired);
    assert!(driver.events.is_empty()); // retired late data and duplicate ACK
    ccxt_pro::pro::ws_client::drop_client(url);
}

#[test]
fn bitfinex_invalid_assignments_control_errors_and_bounded_history_reconnect() {
    use venues::bitfinex::stream::BitfinexControl;
    for message in [
        json!([99, [[1, 1, 2]]]), // data without assignment
        json!({"event":"subscribed","channel":"book","symbol":"unknown","chanId":2}),
        json!({"event":"subscribed","channel":"book","symbol":"tBTCUSD"}),
        json!({"event":"error","code":10300,"msg":"subscription failed"}),
        json!({"event":"info","code":20060}),
        json!({"event":"info","platform":{"status":0}}),
    ] {
        let mut driver: Controlled<_, BitfinexControl> =
            Controlled::new(ccxt_pro::pro::bitfinex::BitfinexCore::new(None));
        assert!(driver.intercept(&input(message)));
        assert!(matches!(driver.events.pop_front(), Some(Err(_))));
    }
    let mut driver: Controlled<_, BitfinexControl> =
        Controlled::new(ccxt_pro::pro::bitfinex::BitfinexCore::new(None));
    driver
        .held
        .insert("book:tBTCUSD".into(), held("book:tBTCUSD"));
    driver.seen_ids.extend((1..=4096).map(|n| n.to_string()));
    assert!(driver.intercept(&input(
        json!({"event":"subscribed","channel":"book","symbol":"tBTCUSD","chanId":5000})
    )));
    assert!(matches!(driver.events.pop_front(), Some(Err(_))));
    assert_eq!(driver.seen_ids.len(), 4096);
}

#[tokio::test]
async fn malformed_replay_arguments_are_safe() {
    let mut driver: Controlled<_, ReplayProtocol> =
        Controlled::new(ccxt::exchange::BaseCore::new(Exchange::new(None)));
    assert_eq!(
        driver.call_dynamic("handle_deltas", Vec::new()).await,
        Value::Null
    );
    assert_eq!(
        driver
            .call_dynamic("handle_deltas", vec![Value::Null, Value::Null])
            .await,
        Value::Null
    );
}

#[tokio::test]
#[should_panic(expected = "buffered book replay has no stock delta dispatcher")]
async fn opted_in_replay_cannot_silently_miss_dispatch() {
    let mut driver: Controlled<_, ReplayProtocol> =
        Controlled::new(ccxt::exchange::BaseCore::new(Exchange::new(None)));
    driver
        .call_dynamic("handle_deltas", vec![input(json!({})), input(json!([{}]))])
        .await;
}

#[tokio::test]
async fn kucoin_replay_uses_stock_handles_order_and_cache_selection() {
    use venues::kucoin::stream::KucoinControl;
    let mut driver: Controlled<_, KucoinControl> =
        Controlled::new(ccxt_pro::pro::kucoin::KucoinCore::new(None));
    let mut book = driver.core.order_book(&[]);
    book.reset(input(json!({"nonce":1,"bids":[[100,1]],"asks":[[101,1]]})));
    let deltas = input(json!([
        {"sequence":"2", "change":"100,buy,4", "timestamp":1700000000000u64},
        {"sequence":3, "change":"100,buy,9", "timestamp":1700000000001u64}
    ]));
    assert_eq!(
        driver
            .call_dynamic("get_cache_index", vec![book.clone(), deltas.clone()])
            .await,
        Value::Int(0)
    );
    let before = book.clone();
    driver
        .call_dynamic("handle_deltas", vec![book.clone(), deltas])
        .await;
    assert_eq!(
        ccxt::get_value(&before, &Value::from("nonce")),
        Value::Int(3)
    );
    let bids = ccxt::get_value(&before, &Value::from("bids"));
    assert_eq!(
        ccxt::get_value(&ccxt::get_value(&bids, &Value::Int(0)), &Value::Int(1)).as_f64(),
        Some(9.0)
    );
    for deltas in [
        json!([]),
        json!([null]),
        json!([{"sequence":2}, null, {"sequence":4}]),
    ] {
        let deltas = input(deltas);
        assert_eq!(
            driver
                .call_dynamic("get_cache_index", vec![book.clone(), deltas.clone()])
                .await,
            driver.core.get_cache_index(book.clone(), deltas)
        );
    }
    for args in [
        vec![],
        vec![book.clone()],
        vec![Value::Null, input(json!([]))],
        vec![book.clone(), Value::Null],
        vec![book, input(json!([]))],
    ] {
        assert_eq!(
            driver.call_dynamic("handle_deltas", args).await,
            Value::Null
        );
    }
}

#[tokio::test(start_paused = true)]
async fn kucoin_stock_json_heartbeat_is_correlated_and_times_out() {
    use venues::kucoin::stream::KucoinControl;
    let url = "ws://kucoin-heartbeat.invalid";
    ccxt_pro::pro::ws_client::mock_setup(url);
    let mut driver: Controlled<_, KucoinControl> =
        Controlled::new(ccxt_pro::pro::kucoin::KucoinCore::new(None));
    driver.bind_url(url);
    driver.send_heartbeat().unwrap();
    let first = driver.pending_ping.clone().unwrap();
    driver.intercept(&input(json!({"type":"pong","id":"wrong"})));
    assert_eq!(driver.pending_ping.as_deref(), Some(first.as_str()));
    driver.intercept(&input(json!({"type":"pong","id":first})));
    assert!(driver.pending_ping.is_none());
    driver.send_heartbeat().unwrap();
    assert_ne!(driver.pending_ping.as_deref(), Some(first.as_str()));
    assert!(driver
        .send_heartbeat()
        .unwrap_err()
        .to_string()
        .contains("timed out"));
    ccxt_pro::pro::ws_client::drop_client(url);
}

#[tokio::test]
async fn native_send_failure_is_reported_without_a_second_socket() {
    let mut driver = driver();
    driver
        .call_dynamic(UNSUBSCRIBE, vec![Value::from("book")])
        .await;
    assert!(
        matches!(driver.events.pop_front(), Some(Err(ExchangeError::UpstreamRequest(message))) if message.contains("send failed"))
    );
}
