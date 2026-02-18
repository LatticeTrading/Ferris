use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::exchanges::traits::ExchangeError;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("unsupported exchange: {0}")]
    UnsupportedExchange(String),
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::Validation(message) => (StatusCode::BAD_REQUEST, "VALIDATION_ERROR", message),
            ApiError::UnsupportedExchange(exchange_id) => (
                StatusCode::BAD_REQUEST,
                "UNSUPPORTED_EXCHANGE",
                format!("exchange `{exchange_id}` is not supported"),
            ),
            ApiError::Exchange(exchange_error) => map_exchange_error(exchange_error),
        };

        let body = ErrorBody { code, message };
        (status, Json(body)).into_response()
    }
}

fn map_exchange_error(error: ExchangeError) -> (StatusCode, &'static str, String) {
    match error {
        ExchangeError::BadSymbol(message) => (StatusCode::BAD_REQUEST, "BAD_SYMBOL", message),
        ExchangeError::UpstreamRequest(message) => {
            (StatusCode::BAD_GATEWAY, "UPSTREAM_REQUEST_FAILED", message)
        }
        ExchangeError::UpstreamData(message) => {
            (StatusCode::BAD_GATEWAY, "UPSTREAM_DATA_INVALID", message)
        }
        ExchangeError::Internal(message) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_EXCHANGE_ERROR",
            message,
        ),
    }
}
