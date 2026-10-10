//! Opt-in smoke test against a running Ferris instance and public Nado.
use super::downstream::{receive, send};
use super::*;
use tokio_tungstenite::connect_async;

#[tokio::test]
#[ignore = "requires running backend + public Nado; FERRIS_BASE_URL=http://127.0.0.1:8787 cargo test --test nado -- --ignored"]
async fn public_nado_trades_candles_and_heartbeat() {
    let base = std::env::var("FERRIS_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8787".into());
    let (mut client, _) = connect_async(format!(
        "{}/v1/ws",
        base.replace("http:", "ws:").replace("https:", "wss:")
    ))
    .await
    .unwrap();
    for (channel, params) in [("trades", json!({})), ("ohlcv", json!({"timeframe":"1m"}))] {
        send(&mut client, json!({"op":"subscribe","channel":channel,"exchange":"nado","symbol":PERP,"params":params})).await;
        // A live trade may arrive between the two subscription ACKs.
        loop {
            let v = receive(&mut client).await;
            assert_ne!(v["type"], "error", "{v}");
            if v["type"] == "subscribed" {
                break;
            }
        }
    }
    let started = tokio::time::Instant::now();
    let mut trades = false;
    let mut candles = false;
    // Stay up past two application heartbeat rounds. Fail on a reconnect/error,
    // rather than accepting a new socket as proof the original survived.
    timeout(Duration::from_secs(90), async {
        while !trades || !candles || started.elapsed() < Duration::from_secs(35) {
            let v = receive(&mut client).await;
            assert_ne!(v["type"], "error", "{v}");
            if v["type"] == "trades" {
                trades = true;
                assert!(v["data"][0]["price"].as_f64().unwrap() > 0.0);
            }
            if v["type"] == "ohlcv" {
                candles = true;
                assert!(v["data"][0][1].as_f64().unwrap() > 0.0);
            }
        }
    })
    .await
    .unwrap();
    client.close(None).await.unwrap();
}
