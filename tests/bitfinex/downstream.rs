//! Exercise Ferris's public websocket envelope with the real stock provider.
use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
async fn send(client: &mut Client, value: Value) {
    client.send(Message::Text(value.to_string())).await.unwrap();
}
async fn receive(client: &mut Client) -> Value {
    timeout(WINDOW, async {
        loop {
            match client.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(payload) => client.send(Message::Pong(payload)).await.unwrap(),
                other => panic!("unexpected client frame: {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}
fn command(op: &str, channel: &str, params: Value) -> Value {
    json!({"op":op,"channel":channel,"exchange":"bitfinex","symbol":NATIVE,"params":params})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_shared_views_mixed_channels_and_errors() {
    let h = Harness::start().await;
    let (mut client, _) = connect_async(format!("{}/v1/ws", h.base.replace("http:", "ws:")))
        .await
        .unwrap();
    for (channel, params) in [
        ("trades", json!({})),
        ("orderbook", json!({"depth":1})),
        ("orderbook", json!({"depth":50})),
        ("ohlcv", json!({"timeframe":"1D"})),
    ] {
        send(&mut client, command("subscribe", channel, params)).await;
        let ack = receive(&mut client).await;
        assert_eq!(ack["type"], "subscribed", "{ack}");
        assert_eq!(ack["topic"]["symbol"], SYMBOL);
    }
    send(
        &mut client,
        command("subscribe", "orderbook", json!({"depth":1})),
    )
    .await;
    assert_eq!(receive(&mut client).await["type"], "alreadySubscribed");
    let (conn, bc) = h.command("subscribe", Some(BOOK), 1).await;
    let (_, tc) = h.command("subscribe", Some(TRADES), 1).await;
    let (_, cc) = h.command("subscribe", Some(CANDLES), 1).await;
    h.inject(json!([
        bc["chanId"],
        [[60000, 1, 2], [59999, 1, 3], [60001, 1, -4]]
    ]));
    let mut views = [receive(&mut client).await, receive(&mut client).await];
    views.sort_by_key(|v| v["data"]["bids"].as_array().unwrap().len());
    assert_eq!(views[0]["data"]["bids"].as_array().unwrap().len(), 1);
    assert_eq!(views[1]["data"]["bids"].as_array().unwrap().len(), 2);
    assert_eq!(views[0]["data"]["timestamp"], Value::Null);
    h.inject(trade(&tc["chanId"], 50));
    assert_eq!(receive(&mut client).await["type"], "trades");
    h.inject(json!([
        cc["chanId"],
        [[TIME, 60000, 60001, 60002, 59999, 3]]
    ]));
    assert_eq!(receive(&mut client).await["type"], "ohlcv");
    send(
        &mut client,
        command("unsubscribe", "orderbook", json!({"depth":1})),
    )
    .await;
    assert_eq!(receive(&mut client).await["type"], "unsubscribed");
    h.inject(json!([bc["chanId"], [60000, 1, 7]]));
    assert_eq!(receive(&mut client).await["data"]["bids"][0][1], 7.0);
    assert!(!h
        .mock
        .commands
        .borrow()
        .iter()
        .any(|(_, v)| v["event"] == "unsubscribe"));
    for (channel, params) in [
        ("orderbook", json!({"depth":101})),
        ("ohlcv", json!({"timeframe":"3m"})),
        ("ohlcv", json!({"price":"index"})),
        ("trades", json!({"category":"inverse"})),
    ] {
        send(&mut client, command("subscribe", channel, params)).await;
        assert_eq!(receive(&mut client).await["code"], "INVALID_TOPIC");
    }
    // A service restart/control error invalidates all sibling continuity, then
    // rebuilds from shared demand (no silent stale cache publication).
    h.inject(json!({"event":"info","code":20051}));
    for _ in 0..3 {
        assert_eq!(receive(&mut client).await["code"], "UPSTREAM_ERROR");
    }
    let (new_conn, _) = h.command("subscribe", Some(BOOK), 2).await;
    assert!(new_conn > conn);
    client.close(None).await.unwrap();
    h.stop().await;
}
