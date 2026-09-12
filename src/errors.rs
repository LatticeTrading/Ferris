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
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),
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
            ApiError::UnsupportedFeature(message) => {
                (StatusCode::NOT_IMPLEMENTED, "UNSUPPORTED_FEATURE", message)
            }
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

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::{json, Value};

    use super::*;

    #[tokio::test]
    async fn market_stats_unsupported_feature_uses_flat_error_contract() {
        let response = ApiError::UnsupportedFeature(
            "market statistics are not implemented for exchange 'bybit'".to_string(),
        )
        .into_response();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "code": "UNSUPPORTED_FEATURE",
                "message": "market statistics are not implemented for exchange 'bybit'",
            }),
        );
    }
}
