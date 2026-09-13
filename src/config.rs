use anyhow::{anyhow, Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub hyperliquid_base_url: String,
    pub extended_rest_base_url: String,
    pub extended_ws_url: String,
    pub lighter_rest_base_url: String,
    pub lighter_markets_url: String,
    pub lighter_ws_url: String,
    pub lighter_market_catalog_refresh_ms: u64,
    pub binance_base_url: String,
    pub request_timeout_ms: u64,
    pub trade_cache_capacity_per_coin: usize,
    pub trade_cache_retention_ms: u64,
    pub trade_collector_enabled: bool,
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
            .unwrap_or_else(|_| {
                crate::exchanges::extended::DEFAULT_EXTENDED_REST_BASE_URL.to_string()
            })
            .trim()
            .trim_end_matches('/')
            .to_string();

        let extended_ws_url = std::env::var("EXTENDED_WS_URL")
            .unwrap_or_else(|_| crate::ws_shared::EXTENDED_WS_BASE_URL.to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();

        let lighter_rest_base_url = std::env::var("LIGHTER_REST_BASE_URL")
            .unwrap_or_else(|_| {
                crate::exchanges::lighterxyz::DEFAULT_LIGHTER_REST_BASE_URL.to_string()
            })
            .trim()
            .trim_end_matches('/')
            .to_string();

        let lighter_markets_url = std::env::var("LIGHTER_MARKETS_URL")
            .unwrap_or_else(|_| {
                crate::exchanges::lighterxyz::DEFAULT_LIGHTER_MARKETS_URL.to_string()
            })
            .trim()
            .to_string();

        let lighter_ws_url = std::env::var("LIGHTER_WS_URL")
            .unwrap_or_else(|_| crate::exchanges::lighterxyz::DEFAULT_LIGHTER_WS_URL.to_string())
            .trim()
            .to_string();

        let lighter_market_catalog_refresh_ms =
            match std::env::var("LIGHTER_MARKET_CATALOG_REFRESH_MS") {
                Ok(value) => value.trim().parse::<u64>().with_context(|| {
                    format!("invalid LIGHTER_MARKET_CATALOG_REFRESH_MS value: {value}")
                })?,
                Err(_) => crate::exchanges::lighterxyz::DEFAULT_LIGHTER_MARKET_CATALOG_REFRESH_MS,
            };
        let binance_base_url = std::env::var("BINANCE_BASE_URL")
            .unwrap_or_else(|_| "https://fapi.binance.com".to_string())
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

        let trade_cache_capacity_per_coin = match std::env::var("TRADE_CACHE_CAPACITY_PER_COIN") {
            Ok(value) => value
                .trim()
                .parse::<usize>()
                .with_context(|| format!("invalid TRADE_CACHE_CAPACITY_PER_COIN value: {value}"))?,
            Err(_) => 5_000,
        };

        let trade_cache_retention_ms = match std::env::var("TRADE_CACHE_RETENTION_MS") {
            Ok(value) => value
                .trim()
                .parse::<u64>()
                .with_context(|| format!("invalid TRADE_CACHE_RETENTION_MS value: {value}"))?,
            Err(_) => 86_400_000,
        };

        let trade_collector_enabled = match std::env::var("TRADE_COLLECTOR_ENABLED") {
            Ok(value) => parse_bool(&value)
                .ok_or_else(|| anyhow!("invalid TRADE_COLLECTOR_ENABLED value: {value}"))?,
            Err(_) => true,
        };

        Ok(Self {
            host,
            port,
            hyperliquid_base_url,
            extended_rest_base_url,
            extended_ws_url,
            lighter_rest_base_url,
            lighter_markets_url,
            lighter_ws_url,
            lighter_market_catalog_refresh_ms,
            binance_base_url,
            request_timeout_ms,
            trade_cache_capacity_per_coin,
            trade_cache_retention_ms,
            trade_collector_enabled,
        })
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}
