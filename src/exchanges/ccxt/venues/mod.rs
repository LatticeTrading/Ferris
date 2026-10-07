//! Built-in integrations. Dispatch only; behavior lives with each exchange.

use super::{
    statistics_profile::Profile,
    venue::{CatalogScope, Venue},
};
use crate::exchanges::traits::ExchangeError;
use serde_json::Value as JsonValue;

pub(super) mod apex;
pub(super) mod aster;
pub(super) mod binance;
pub(super) mod bybit;
pub(super) mod extended;
pub(super) mod hyperliquid;
pub(super) mod lighter;

macro_rules! dispatch {
    ($venue:expr, $exchange:ident => $body:expr) => {
        match $venue {
            $crate::exchanges::ccxt::Venue::Apex => {
                use $crate::exchanges::ccxt::venues::apex as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Binance => {
                use $crate::exchanges::ccxt::venues::binance as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Bybit => {
                use $crate::exchanges::ccxt::venues::bybit as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Aster => {
                use $crate::exchanges::ccxt::venues::aster as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Hyperliquid => {
                use $crate::exchanges::ccxt::venues::hyperliquid as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Extended => {
                use $crate::exchanges::ccxt::venues::extended as $exchange;
                $body
            }
            $crate::exchanges::ccxt::Venue::Lighter => {
                use $crate::exchanges::ccxt::venues::lighter as $exchange;
                $body
            }
        }
    };
}
pub(super) use dispatch;

pub(super) fn statistics_profile(venue: Venue) -> &'static Profile {
    dispatch!(venue, exchange => &exchange::statistics::PROFILE)
}

fn scope_error(venue: Venue, scope: CatalogScope) -> ExchangeError {
    ExchangeError::BadSymbol(format!(
        "{} has no qualified {scope:?} catalog profile",
        venue.public_id()
    ))
}

pub(in crate::exchanges::ccxt) fn coin_override(
    params: &JsonValue,
) -> Result<Option<std::borrow::Cow<'_, str>>, ExchangeError> {
    let value = params.as_object().and_then(|params| params.get("coin"));
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => {
            Ok((!value.trim().is_empty()).then(|| std::borrow::Cow::Borrowed(value.trim())))
        }
        Some(_) => Err(ExchangeError::BadSymbol("`coin` must be a string".into())),
    }
}

mod shared_rest;

mod shared_stream;

mod defaults;
