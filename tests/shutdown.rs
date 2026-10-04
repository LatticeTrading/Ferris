#![cfg(unix)]

use std::{process::Command, time::Duration};

use axum::{extract::State, routing::post, Router};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// Keep the real executable's signal path covered: a stalled stock request must
// not hold Axum's graceful drain open while owner cancellation waits behind it.
#[tokio::test]
async fn sigterm_cancels_pending_acquisition_and_closes_websockets() {
    timeout(Duration::from_secs(15), async {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let (entered, mut requests) = mpsc::unbounded_channel();
        let fixture = tokio::spawn(async move {
            axum::serve(
                upstream,
                Router::new()
                    .route("/info", post(|State(entered): State<mpsc::UnboundedSender<()>>| async move {
                        entered.send(()).unwrap();
                        std::future::pending::<String>().await
                    }))
                    .with_state(entered),
            )
            .await
            .unwrap();
        });
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserved.local_addr().unwrap();
        drop(reserved);
        let child = Command::new(env!("CARGO_BIN_EXE_ferris-market-data-backend"))
            .env("HOST", "127.0.0.1")
            .env("PORT", addr.port().to_string())
            .env("HYPERLIQUID_BASE_URL", format!("http://{upstream_addr}"))
            .env("REQUEST_TIMEOUT_MS", "60000")
            .spawn()
            .unwrap();
        let mut process = ChildGuard(child);
        let client = reqwest::Client::new();
        loop {
            if client.get(format!("http://{addr}/healthz")).send().await.is_ok() {
                break;
            }
            assert!(process.0.try_wait().unwrap().is_none(), "backend exited before listening");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (mut idle, _) = connect_async(format!("ws://{addr}/v1/ws")).await.unwrap();
        idle.send(Message::Text(json!({"op":"ping"}).to_string())).await.unwrap();
        assert_eq!(serde_json::from_str::<serde_json::Value>(&idle.next().await.unwrap().unwrap().into_text().unwrap()).unwrap(), json!({"type":"pong"}));
        let http = tokio::spawn(async move {
            client.post(format!("http://{addr}/v1/fetchTrades"))
                .json(&json!({"symbol":"BTC"}))
                .send().await.unwrap()
        });
        requests.recv().await.unwrap();
        let (mut pending, _) = connect_async(format!("ws://{addr}/v1/ws")).await.unwrap();
        pending.send(Message::Text(json!({"op":"subscribe","channel":"trades","symbol":"BTC"}).to_string())).await.unwrap();
        assert!(Command::new("kill").args(["-TERM", &process.0.id().to_string()]).status().unwrap().success());
        timeout(Duration::from_secs(5), async {
            for socket in [&mut idle, &mut pending] {
                loop {
                    match socket.next().await {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                        Some(Ok(_)) => {},
                    }
                }
            }
            assert!(http.await.unwrap().status().is_server_error());
            loop {
                if let Some(status) = process.0.try_wait().unwrap() {
                    assert!(status.success(), "shutdown exit: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("SIGTERM must cancel acquisition, close clients and exit without the 60s upstream timeout");
        fixture.abort();
        let _ = fixture.await;
    }).await.unwrap();
}

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
