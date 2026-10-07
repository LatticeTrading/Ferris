use std::{sync::Arc, time::Duration};

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use ferris_market_data_backend::{
    config::Config,
    exchanges::{
        ccxt::{CatalogScope, CcxtService, Venue},
        traits::ExchangeError,
    },
};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

type Response = (StatusCode, Json<Value>);

struct Source {
    config: Config,
    requests: mpsc::UnboundedReceiver<oneshot::Sender<Response>>,
    server: tokio::task::JoinHandle<()>,
}

impl Source {
    async fn start() -> Self {
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let app =
            Router::new()
                .route(
                    "/fapi/v1/exchangeInfo",
                    get(
                        |State(requests): State<
                            mpsc::UnboundedSender<oneshot::Sender<Response>>,
                        >| async move {
                            let (reply, response) = oneshot::channel();
                            requests.send(reply).unwrap();
                            response
                                .await
                                .unwrap_or((StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))))
                        },
                    ),
                )
                .with_state(requests_tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Explicit configuration keeps these tests independent of deployment env.
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            hyperliquid_base_url: base_url.clone(),
            extended_rest_base_url: format!("{base_url}/api/v1"),
            extended_ws_url: "ws://127.0.0.1/unused".into(),
            lighter_rest_base_url: base_url.clone(),
            lighter_ws_url: "ws://127.0.0.1/unused".into(),
            binance_base_url: base_url.clone(),
            bybit_base_url: base_url.clone(),
            apex_rest_base_url: format!("{base_url}/api"),
            apex_ws_url: "ws://127.0.0.1/unused".into(),
            aster_base_url: base_url,
            request_timeout_ms: 30_000,
        };
        Self {
            config,
            requests,
            server,
        }
    }

    async fn request(&mut self) -> oneshot::Sender<Response> {
        tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn metadata(base: &str) -> Value {
    json!({"symbols": [{
        "symbol": format!("{base}USDT"), "pair": format!("{base}USDT"),
        "contractType": "PERPETUAL", "status": "TRADING",
        "baseAsset": base, "quoteAsset": "USDT", "marginAsset": "USDT",
        "pricePrecision": 2, "quantityPrecision": 3,
        "filters": [
            {"filterType":"PRICE_FILTER", "minPrice":"0.1", "maxPrice":"1000000", "tickSize":"0.1"},
            {"filterType":"LOT_SIZE", "minQty":"0.001", "maxQty":"10000", "stepSize":"0.001"}
        ]
    }]})
}

fn load(
    service: &CcxtService,
) -> tokio::task::JoinHandle<
    Result<Arc<ferris_market_data_backend::exchanges::ccxt::CatalogSnapshot>, ExchangeError>,
> {
    let service = service.clone();
    tokio::spawn(async move { service.catalog(Venue::Binance, CatalogScope::Default).await })
}

#[tokio::test]
async fn shared_refresh_retains_last_observation_on_failure() {
    let mut source = Source::start().await;
    let service = CcxtService::start(&source.config).unwrap();
    let first = load(&service);
    let second = load(&service);
    source
        .request()
        .await
        .send((StatusCode::OK, Json(metadata("BTC"))))
        .unwrap();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(
        first
            .catalog
            .resolve("BTCUSDT", &json!({}))
            .unwrap()
            .market
            .tick_size,
        Some(0.1)
    );

    let refresh_service = service.clone();
    let previous = first.clone();
    let failed = tokio::spawn(async move { refresh_service.refresh_catalog(&previous).await });
    source
        .request()
        .await
        .send((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"msg":"outage"})),
        ))
        .unwrap();
    assert!(matches!(
        failed.await.unwrap(),
        Err(ExchangeError::UpstreamRequest(_))
    ));
    let cached = service
        .catalog(Venue::Binance, CatalogScope::Linear)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &cached));
    assert_eq!(cached.timestamp, first.timestamp);

    let refresh_service = service.clone();
    let previous = first.clone();
    let refresh = tokio::spawn(async move {
        tokio::join!(
            refresh_service.refresh_catalog(&previous),
            refresh_service.refresh_catalog(&previous)
        )
    });
    source
        .request()
        .await
        .send((StatusCode::OK, Json(metadata("ETH"))))
        .unwrap();
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), refresh)
        .await
        .unwrap()
        .unwrap();
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(a.generation, first.generation + 1);
    assert_eq!(
        a.catalog
            .resolve("ETHUSDT", &json!({}))
            .unwrap()
            .market
            .base,
        "ETH"
    );
    assert!(a.catalog.resolve("BTCUSDT", &json!({})).is_err());
    assert_eq!(
        first
            .catalog
            .resolve("BTCUSDT", &json!({}))
            .unwrap()
            .market
            .base,
        "BTC"
    );
    assert!(first.catalog.resolve("ETHUSDT", &json!({})).is_err());
    assert!(source.requests.try_recv().is_err());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn requester_cancellation_does_not_cancel_load_but_shutdown_does() {
    let mut source = Source::start().await;
    let service = CcxtService::start(&source.config).unwrap();
    let cancelled = load(&service);
    let response = source.request().await;
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    let surviving = load(&service);
    response
        .send((StatusCode::OK, Json(metadata("BTC"))))
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), surviving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .catalog
            .resolve("BTCUSDT", &json!({}))
            .unwrap()
            .market
            .base,
        "BTC"
    );

    let refreshing = service.clone();
    let pending = tokio::spawn(async move { refreshing.refresh_catalog(&snapshot).await });
    let _held_response = source.request().await;
    tokio::time::timeout(Duration::from_secs(5), service.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(service
        .catalog(Venue::Binance, CatalogScope::Default)
        .await
        .is_err());
    service.shutdown().await.unwrap();
}
