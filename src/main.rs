use std::{future::IntoFuture, net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    routing::{get, post},
    Router,
};
use ferris_market_data_backend::{
    config::Config,
    exchanges::{
        ccxt::{CcxtExchange, CcxtService, Venue},
        registry::ExchangeRegistry,
    },
    realtime::RealtimeService,
    web::{self, AppState},
};
use tokio::signal;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env().context("failed to load configuration")?;
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("invalid bind address")?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind tcp listener")?;
    let ccxt = CcxtService::start(&config).context("failed to start CCXT owners")?;

    let mut registry = ExchangeRegistry::new();
    for venue in Venue::ALL {
        registry.register(Arc::new(CcxtExchange::new(venue, ccxt.clone())));
    }

    let realtime = RealtimeService::new(ccxt.clone());

    let state = AppState::new(Arc::new(registry), realtime);

    let app = Router::new()
        .route("/healthz", get(web::health))
        .route("/v1/fetchTrades", post(web::fetch_trades))
        .route("/v1/fetchOHLCV", post(web::fetch_ohlcv))
        .route("/v1/fetchOrderBook", post(web::fetch_order_book))
        .route("/v1/fetchMarkets", post(web::fetch_markets))
        .route("/v1/fetchMarketStats", post(web::fetch_market_stats))
        .route("/v1/capabilities", get(web::capabilities))
        .route("/v1/ws", get(web::trades_stream_ws))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    info!(
        host = %config.host,
        port = config.port,
        hyperliquid_url = %config.hyperliquid_base_url,
        lighter_rest_url = %config.lighter_rest_base_url,
        lighter_ws_url = %config.lighter_ws_url,
        "server started"
    );

    let (stop, stopping) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = stopping.await;
        })
        .into_future();
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => Some(result),
        _ = shutdown_signal() => None,
    };
    let _ = stop.send(());
    // Cancel acquisition before awaiting Axum's in-flight HTTP drain. Upgraded
    // websockets are separate tasks and must be closed/joined explicitly.
    state.shutdown_websockets().await;
    state.shutdown_market_stats().await;
    let owners = ccxt.shutdown().await.context("failed to stop CCXT owners");
    let result = match result {
        Some(result) => result,
        None => server.await,
    };
    owners?;
    result.context("server error")?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_max_level(Level::INFO)
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = signal::ctrl_c().await {
            tracing::error!(error = %err, "failed to listen for ctrl-c signal");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal_stream) => {
                signal_stream.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to listen for terminate signal");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
}
