mod config;
mod errors;
mod exchanges;
mod models;
mod web;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    routing::{get, post},
    Router,
};
use config::Config;
use exchanges::{
    binance::BinanceExchange, bybit::BybitExchange, hyperliquid::HyperliquidExchange,
    registry::ExchangeRegistry,
};
use tokio::signal;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::{fmt, EnvFilter};
use web::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env().context("failed to load configuration")?;

    let mut registry = ExchangeRegistry::new();
    registry.register(Arc::new(BinanceExchange::new(config.request_timeout_ms)?));
    registry.register(Arc::new(BybitExchange::new(config.request_timeout_ms)?));
    registry.register(Arc::new(HyperliquidExchange::new(
        config.hyperliquid_base_url.clone(),
        config.request_timeout_ms,
        config.trade_cache_capacity_per_coin,
        config.trade_cache_retention_ms,
        config.trade_collector_enabled,
    )?));

    let state = AppState::new(Arc::new(registry));

    let app = Router::new()
        .route("/healthz", get(web::health))
        .route("/v1/fetchTrades", post(web::fetch_trades))
        .route("/v1/fetchOHLCV", post(web::fetch_ohlcv))
        .route("/v1/fetchOrderBook", post(web::fetch_order_book))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("invalid bind address")?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind tcp listener")?;

    info!(
        host = %config.host,
        port = config.port,
        hyperliquid_url = %config.hyperliquid_base_url,
        trade_cache_capacity = config.trade_cache_capacity_per_coin,
        trade_cache_retention_ms = config.trade_cache_retention_ms,
        trade_collector_enabled = config.trade_collector_enabled,
        "server started"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

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
