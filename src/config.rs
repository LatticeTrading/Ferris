use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub hyperliquid_base_url: String,
    pub extended_rest_base_url: String,
    pub extended_ws_url: String,
    pub lighter_rest_base_url: String,
    pub lighter_ws_url: String,
    pub binance_base_url: String,
    pub bybit_base_url: String,
    pub aster_base_url: String,
    pub request_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let host = std::env::var("HOST")
            .unwrap_or_else(|_| "0.0.0.0".to_string())
            .trim()
            .to_string();

        let port = match std::env::var("PORT") {
            Ok(value) => value
                .trim()
                .parse::<u16>()
                .with_context(|| format!("invalid PORT value: {value}"))?,
            Err(_) => 8787,
        };

        let hyperliquid_base_url = std::env::var("HYPERLIQUID_BASE_URL")
            .unwrap_or_else(|_| "https://api.hyperliquid.xyz".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();

        let extended_rest_base_url = std::env::var("EXTENDED_REST_BASE_URL")
            .unwrap_or_else(|_| "https://api.starknet.extended.exchange/api/v1".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();

        let extended_ws_url = std::env::var("EXTENDED_WS_URL")
            .unwrap_or_else(|_| {
                "wss://api.starknet.extended.exchange/stream.extended.exchange/v1".to_string()
            })
            .trim()
            .trim_end_matches('/')
            .to_string();

        let lighter_rest_base_url = std::env::var("LIGHTER_REST_BASE_URL")
            .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();

        let lighter_ws_url = std::env::var("LIGHTER_WS_URL")
            .unwrap_or_else(|_| "wss://mainnet.zklighter.elliot.ai/stream".to_string())
            .trim()
            .to_string();

        let binance_base_url = std::env::var("BINANCE_BASE_URL")
            .unwrap_or_else(|_| "https://fapi.binance.com".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();
        let bybit_base_url = std::env::var("BYBIT_BASE_URL")
            .unwrap_or_else(|_| "https://api.bybit.com".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();
        let aster_base_url = std::env::var("ASTER_BASE_URL")
            .unwrap_or_else(|_| "https://fapi.asterdex.com".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();

        let request_timeout_ms = match std::env::var("REQUEST_TIMEOUT_MS") {
            Ok(value) => value
                .trim()
                .parse::<u64>()
                .with_context(|| format!("invalid REQUEST_TIMEOUT_MS value: {value}"))?,
            Err(_) => 10_000,
        };

        Ok(Self {
            host,
            port,
            hyperliquid_base_url,
            extended_rest_base_url,
            extended_ws_url,
            lighter_rest_base_url,
            lighter_ws_url,
            binance_base_url,
            bybit_base_url,
            aster_base_url,
            request_timeout_ms,
        })
    }
}
