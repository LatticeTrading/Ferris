use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn frame(socket: &mut Socket) -> Value {
    timeout(WINDOW, async {
        loop {
            if let Message::Text(text) = socket.next().await.unwrap().unwrap() {
                return serde_json::from_str(&text).unwrap();
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_and_ws_native_base_views_share_one_feed_and_preserve_deletions() {
    let h = Harness::start().await;
    let native = h
        .post(
            "fetchOrderBook",
            json!({"symbol":PERP,"limit":2}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(native["amountUnit"], "contracts");
    assert_eq!(
        native["amountConversion"]["contractSizeApplied"],
        Value::Null
    );
    let base = h
        .post(
            "fetchOrderBook",
            json!({"symbol":PERP,"limit":2,"params":{"amountUnit":"base"}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(base["bids"], json!([[67190.0, 0.002], [67189.0, 0.003]]));
    assert_eq!(base["asks"], json!([[67192.0, 0.001], [67193.0, 0.002]]));
    assert_eq!(base["amountUnit"], "base");
    assert_eq!(base["sourceAmountUnit"], "contracts");
    assert_eq!(base["amountCurrency"], "BTC");
    assert_eq!(base["amountConversion"]["contractSizeApplied"], 0.001);
    let spot = h
        .post(
            "fetchOrderBook",
            json!({"symbol":SPOT_NATIVE,"params":{"amountUnit":"base"}}),
            StatusCode::OK,
        )
        .await;
    assert_eq!(spot["amountUnit"], "base");
    assert_eq!(spot["bids"][0][1], 0.5);
    assert_eq!(spot["amountConversion"]["contractSizeApplied"], Value::Null);
    h.post(
        "fetchOrderBook",
        json!({"symbol":PERP,"params":{"amountUnit":"contracts"}}),
        StatusCode::BAD_REQUEST,
    )
    .await;

    let (mut ws, _) = connect_async(format!("{}/v1/ws", h.base.replace("http://", "ws://")))
        .await
        .unwrap();
    for mode in ["native", "base"] {
        ws.send(Message::Text(json!({"op":"subscribe","channel":"orderbook","exchange":"kucoin","symbol":PERP,"params":{"levels":1,"amountUnit":mode}}).to_string())).await.unwrap();
        let ack = frame(&mut ws).await;
        assert_eq!(ack["type"], "subscribed", "{ack}");
        assert_ne!(ack["alreadySubscribed"], true, "{ack}");
    }
    h.command("subscribe", "/contractMarket/level2:XBTUSDTM", 1)
        .await;
    for (sequence, amount) in [(2, "4"), (3, "0"), (4, "7")] {
        h.inject(trade_frame("/contractMarket/level2:XBTUSDTM", "level2", json!({"sequence":sequence,"change":format!("67190,buy,{amount}"),"timestamp":1700000060000u64+sequence})));
        let mut pair = std::collections::HashMap::new();
        while pair.len() < 2 {
            let value = frame(&mut ws).await;
            assert_eq!(value["type"], "orderbook", "{value}");
            if value["data"]["nonce"] != sequence {
                continue;
            }
            let mode = value["topic"]["params"]["amountUnit"]
                .as_str()
                .unwrap()
                .to_string();
            pair.insert(mode, value["data"].clone());
        }
        let raw = &pair["native"];
        let normalized = &pair["base"];
        assert_eq!(raw["bids"].as_array().unwrap().len(), 1);
        assert_eq!(normalized["amountConversion"], base["amountConversion"]);
        assert_eq!(normalized["amountUnit"], "base");
        assert_eq!(raw["amountUnit"], "contracts");
        assert_eq!(raw["bids"][0][0], normalized["bids"][0][0]);
        assert_eq!(
            normalized["bids"][0][1].as_f64().unwrap(),
            raw["bids"][0][1].as_f64().unwrap() * 0.001
        );
        if sequence == 3 {
            assert_eq!(raw["bids"][0][0], 67189.0);
        }
    }
    let commands = h.mock.commands.borrow().clone();
    assert_eq!(
        commands
            .iter()
            .filter(|(_, v)| v["type"] == "subscribe"
                && v["topic"] == "/contractMarket/level2:XBTUSDTM")
            .count(),
        1
    );
    assert!(h
        .mock
        .calls
        .lock()
        .await
        .iter()
        .all(|uri| !uri.to_string().contains("amountUnit")));
    ws.close(None).await.unwrap();
    h.stop().await;
}

#[tokio::test]
async fn inverse_rest_uses_signed_native_multiplier_and_level_price() {
    let h = Harness::start().await;
    h.mock.expanded_catalog.store(true, Ordering::SeqCst);
    let b = h.post("fetchOrderBook", json!({"symbol":"XBTUSDM","limit":2,"params":{"category":"inverse","amountUnit":"base"}}), StatusCode::OK).await;
    assert_eq!(b["amountUnit"], "base");
    assert_eq!(b["amountConversion"]["contractSizeApplied"], 1.0);
    assert_eq!(b["amountConversion"]["contractSizeCurrency"], "USD");
    assert_eq!(b["bids"][0][1].as_f64().unwrap(), 2.0 / 67190.0);
    assert_eq!(b["bids"][1][1].as_f64().unwrap(), 3.0 / 67189.0);
    h.stop().await;
}
