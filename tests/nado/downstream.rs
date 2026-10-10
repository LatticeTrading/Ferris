use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
pub(super) async fn send(client: &mut Client, v: Value) {
    client.send(Message::Text(v.to_string())).await.unwrap();
}
pub(super) async fn receive(client: &mut Client) -> Value {
    timeout(Duration::from_secs(60), async {
        loop {
            match client.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(p) => client.send(Message::Pong(p)).await.unwrap(),
                other => panic!("{other:?}"),
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_websocket_envelopes_statistics_and_explicit_book_limitation() {
    let h = Harness::start().await;
    let (mut client, _) = connect_async(format!("{}/v1/ws", h.base.replace("http:", "ws:")))
        .await
        .unwrap();
    send(
        &mut client,
        json!({"op":"subscribe","channel":"trades","exchange":"nado","symbol":NATIVE}),
    )
    .await;
    let ack = receive(&mut client).await;
    assert_eq!(ack["type"], "subscribed", "{ack}");
    assert_eq!(ack["topic"]["symbol"], PERP);
    h.command("trade", 2, None, 1).await;
    h.inject(trade(2, TIME));
    let update = receive(&mut client).await;
    assert_eq!(update["type"], "trades", "{update}");
    send(&mut client,json!({"op":"subscribe","channel":"ohlcv","exchange":"nado","symbol":NATIVE,"params":{"timeframe":"1m"}})).await;
    assert_eq!(receive(&mut client).await["type"], "subscribed");
    h.command("latest_candlestick", 2, Some(60), 1).await;
    let mut c = candle();
    c["type"] = json!("latest_candlestick");
    h.inject(c);
    let update = receive(&mut client).await;
    assert_eq!(update["type"], "ohlcv", "{update}");
    assert_eq!(update["data"][0][0], TIME);
    send(
        &mut client,
        json!({"op":"subscribe","channel":"orderbook","exchange":"nado","symbol":NATIVE}),
    )
    .await;
    let error = receive(&mut client).await;
    assert_eq!(error["code"], "INVALID_TOPIC", "{error}");
    send(&mut client,json!({"op":"subscribe","channel":"marketstats","exchange":"nado","fields":["funding","openInterest"]})).await;
    let ack = receive(&mut client).await;
    assert_eq!(ack["type"], "subscribed", "{ack}");
    let snapshot = receive(&mut client).await;
    assert_eq!(snapshot["type"], "marketstats", "{snapshot}");
    assert_eq!(
        snapshot["markets"][0]["fields"]["openInterest"]["state"], "available",
        "{snapshot}"
    );
    // Subscription rejection must not be swallowed on an unwatched control hash.
    h.inject(json!({"id":999,"error":"fixture rejection"}));
    let error = receive(&mut client).await;
    assert_eq!(error["code"], "UPSTREAM_ERROR", "{error}");
    client.close(None).await.unwrap();
    h.stop().await;
}
