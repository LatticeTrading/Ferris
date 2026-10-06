use serde_json::{json, Value};

use crate::{config::Config, exchanges::traits::ExchangeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    Binance,
    Bybit,
    Hyperliquid,
    Lighter,
    Aster,
    Extended,
}

impl Venue {
    pub const ALL: [Self; 6] = [
        Self::Binance,
        Self::Bybit,
        Self::Hyperliquid,
        Self::Lighter,
        Self::Aster,
        Self::Extended,
    ];

    pub fn public_id(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
            Self::Lighter => "lighterxyz",
            Self::Aster => "aster",
            Self::Extended => "extended",
        }
    }

    pub fn ccxt_id(self) -> &'static str {
        match self {
            Self::Lighter => "lighter",
            other => other.public_id(),
        }
    }

    pub fn from_public_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|venue| venue.public_id() == id)
    }

    pub(super) fn data_scope(self, scope: CatalogScope) -> CatalogScope {
        if scope == CatalogScope::Default {
            super::venues::dispatch!(self, exchange => exchange::DATA_SCOPE)
        } else {
            scope
        }
    }

    pub(super) fn scope(self, scope: CatalogScope) -> Result<CatalogScope, ExchangeError> {
        super::venues::dispatch!(self, exchange => exchange::scope(scope))
    }
}

/// Acquisition profiles, not client display filters. Aster and Lighter stock
/// loadMarkets always load spot and swap together and therefore share one profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CatalogScope {
    #[default]
    Default,
    Spot,
    Linear,
    Inverse,
    Option,
}

/// Only owned configuration crosses into an owner. No caller can inject a
/// mutable CCXT object, a transport implementation, or a raw dispatch command.
#[derive(Clone)]
pub(super) struct ProviderConfig {
    value: Value,
}

impl ProviderConfig {
    pub(super) fn new(venue: Venue, config: &Config) -> Result<Self, ExchangeError> {
        let timeout = i64::try_from(config.request_timeout_ms)
            .ok()
            .filter(|timeout| *timeout > 0)
            .ok_or_else(|| ExchangeError::Internal("invalid CCXT request timeout".into()))?;
        let api = super::venues::dispatch!(venue, exchange => exchange::api(config))?;
        let mut value = json!({
            "timeout": timeout,
            "enableRateLimit": true,
            "urls": {"api": api},
            "options": {"defaultType": "swap", "defaultSubType": "linear"}
        });
        super::venues::dispatch!(venue, exchange => exchange::configure(config, &mut value))?;
        Ok(Self { value })
    }

    pub(super) fn for_scope(&self, venue: Venue, scope: CatalogScope) -> ccxt::Value {
        use CatalogScope::*;
        let mut value = self.value.clone();
        let types = super::venues::dispatch!(venue, exchange => exchange::market_types(scope));
        if let Some(types) = types {
            value["options"]["fetchMarkets"] = json!({"types": types});
        }
        if scope == Spot {
            value["options"]["defaultType"] = json!("spot");
        } else if scope == Option {
            value["options"]["defaultType"] = json!("option");
        }
        if scope == Inverse {
            value["options"]["defaultSubType"] = json!("inverse");
        } else if matches!(scope, Spot | Option) {
            // A linear default takes precedence over Binance's spot/option type.
            value["options"]["defaultSubType"] = json!(null);
        }
        ccxt::Value::from_json(&value)
    }
}

mod provider;
pub(super) use provider::{exchange_error, Provider};

#[cfg(test)]
mod tests;
