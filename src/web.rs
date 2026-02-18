use std::sync::Arc;

use axum::{extract::State, Json};

use crate::{
    errors::ApiError,
    exchanges::registry::ExchangeRegistry,
    models::{
        CcxtOhlcv, CcxtOrderBook, CcxtTrade, FetchOhlcvRequest, FetchOrderBookRequest,
        FetchTradesRequest, HealthResponse,
    },
};

#[derive(Clone)]
pub struct AppState {
    exchange_registry: Arc<ExchangeRegistry>,
}

impl AppState {
    pub fn new(exchange_registry: Arc<ExchangeRegistry>) -> Self {
        Self { exchange_registry }
    }
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn fetch_trades(
    State(state): State<AppState>,
    Json(request): Json<FetchTradesRequest>,
) -> Result<Json<Vec<CcxtTrade>>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let trades = exchange.fetch_trades(request.into_params()).await?;
    Ok(Json(trades))
}

pub async fn fetch_ohlcv(
    State(state): State<AppState>,
    Json(request): Json<FetchOhlcvRequest>,
) -> Result<Json<Vec<CcxtOhlcv>>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(timeframe) = request.timeframe.as_ref() {
        if timeframe.trim().is_empty() {
            return Err(ApiError::Validation(
                "`timeframe` cannot be empty when provided".to_string(),
            ));
        }
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let candles = exchange.fetch_ohlcv(request.into_params()).await?;
    Ok(Json(candles))
}

pub async fn fetch_order_book(
    State(state): State<AppState>,
    Json(request): Json<FetchOrderBookRequest>,
) -> Result<Json<CcxtOrderBook>, ApiError> {
    if request.symbol.trim().is_empty() {
        return Err(ApiError::Validation("`symbol` cannot be empty".to_string()));
    }

    if let Some(limit) = request.limit {
        if limit == 0 {
            return Err(ApiError::Validation(
                "`limit` must be greater than 0".to_string(),
            ));
        }
    }

    if !(request.params.is_null() || request.params.is_object()) {
        return Err(ApiError::Validation(
            "`params` must be an object or null".to_string(),
        ));
    }

    let exchange_id = request.exchange.trim().to_ascii_lowercase();
    let Some(exchange) = state.exchange_registry.get(&exchange_id) else {
        return Err(ApiError::UnsupportedExchange(exchange_id));
    };

    let order_book = exchange.fetch_order_book(request.into_params()).await?;
    Ok(Json(order_book))
}
