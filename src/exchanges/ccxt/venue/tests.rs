use super::*;
use crate::exchanges::ccxt::{
    catalog::Catalog, live::LiveHub, owner::CatalogSnapshot, stream::prepare_live,
};
use crate::realtime::{RealtimeChannel, RealtimeTopic, RealtimeUpdate};
use ccxt::types::Market;
use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn trade_owner_delivers_new_rows(venue: Venue) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (outgoing, mut input) = mpsc::channel::<Value>(4);
    let (ready, registered) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        let mut ready = Some(ready);
        loop {
            tokio::select! {
                frame = socket.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        let request: Value = serde_json::from_str(&text).unwrap();
                        let subscribed = request["op"] == "subscribe" || request["method"] == "SUBSCRIBE";
                        if subscribed {
                            let ack = if venue == Venue::Binance {
                                json!({"id": request["id"], "result": null})
                            } else {
                                json!({"op":"subscribe", "success":true, "req_id":request["req_id"]})
                            };
                            socket.send(Message::Text(ack.to_string().into())).await.unwrap();
                            if let Some(ready) = ready.take() { let _ = ready.send(()); }
                        }
                    },
                    Some(Ok(Message::Ping(data))) => { let _ = socket.send(Message::Pong(data)).await; },
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {},
                },
                frame = input.recv() => match frame {
                    Some(frame) => socket.send(Message::Text(frame.to_string().into())).await.unwrap(),
                    None => break,
                },
            }
        }
    });
    let ws = if venue == Venue::Binance {
        json!({"future":url})
    } else {
        json!({"public":{"linear":url}})
    };
    let config = ProviderConfig {
        value: json!({
            "enableRateLimit":true, "urls":{"api":{"ws":ws}},
            "options":{"defaultType":"swap", "defaultSubType":"linear"}
        }),
    };
    let market = json!({
        "id":"BTCUSDT", "symbol":"BTC/USDT:USDT", "base":"BTC", "quote":"USDT", "settle":"USDT",
        "type":"swap", "spot":false, "swap":true, "future":false, "option":false,
        "active":true, "contract":true, "linear":true, "inverse":false, "contractSize":1,
        "precision":{"amount":0.001,"price":0.1}, "limits":{"amount":{"min":0.001}},
        "info":{"symbol":"BTCUSDT", "contractType":"PERPETUAL", "settleCoin":"USDT", "marginAsset":"USDT"}
    });
    let catalog = Arc::new(CatalogSnapshot {
        venue,
        scope: CatalogScope::Linear,
        timestamp: 1,
        generation: 1,
        catalog: Catalog::from_markets(
            venue,
            vec![Market::from_value(ccxt::Value::from_json(&market))],
            ccxt::runtime::TICK_SIZE,
        )
        .unwrap(),
    });
    let provider = Provider::new(venue, CatalogScope::Linear, &config);
    let prepared = prepare_live(
        venue,
        RealtimeChannel::Trades,
        RealtimeTopic {
            exchange: venue.public_id().into(),
            symbol: "BTCUSDT".into(),
            params: json!({}),
        },
        catalog,
        &provider,
        &config,
    )
    .await
    .unwrap();
    let hub = LiveHub::default();
    let mut subscription = hub.subscribe(prepared).await.unwrap();
    timeout(Duration::from_secs(5), registered)
        .await
        .unwrap()
        .unwrap();
    for id in [101, 102] {
        let time = 1_700_000_000_000u64 + id;
        let wire = if venue == Venue::Binance {
            json!({"e":"trade", "E":time, "T":time, "s":"BTCUSDT", "t":id, "p":"60000", "q":"0.2", "m":false})
        } else {
            json!({"topic":"publicTrade.BTCUSDT", "type":"snapshot", "ts":time,
                "data":[{"T":time,"s":"BTCUSDT","S":"Buy","v":"0.2","p":"60000","i":id.to_string(),"BT":false}]})
        };
        outgoing.send(wire).await.unwrap();
        let update = timeout(Duration::from_secs(5), subscription.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let RealtimeUpdate::Trades(rows) = update else {
            panic!("expected a trade update");
        };
        assert_eq!(
            rows.iter()
                .map(|trade| trade.id.as_deref())
                .collect::<Vec<_>>(),
            [Some(id.to_string().as_str())]
        );
        assert_eq!(rows[0].symbol.as_deref(), Some("BTC/USDT:USDT"));
        assert_eq!(rows[0].price, Some(60_000.0));
        assert_eq!(rows[0].amount, Some(0.2));
        assert_eq!(rows[0].timestamp, Some(time));
    }
    assert!(
        timeout(Duration::from_millis(100), subscription.receiver.recv())
            .await
            .is_err()
    );
    drop(subscription);
    hub.shutdown().await.unwrap();
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn bybit_trade_results_reach_the_owner_without_cache_replay() {
    trade_owner_delivers_new_rows(Venue::Bybit).await;
}

#[tokio::test]
async fn binance_trade_registration_fits_the_live_worker_stack() {
    trade_owner_delivers_new_rows(Venue::Binance).await;
}
