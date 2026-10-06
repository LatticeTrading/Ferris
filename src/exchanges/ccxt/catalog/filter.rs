//! Product selector validation shared by catalog acquisition and resolution.

use serde_json::Value as JsonValue;

use crate::{
    exchanges::{ccxt::venue::Venue, traits::ExchangeError},
    models::UnifiedMarketType,
};

use super::CatalogMarket;

#[derive(Default)]
pub(super) struct ProductFilter {
    pub(super) market_type: Option<UnifiedMarketType>,
    pub(super) category: Option<String>,
    pub(super) subtype: Option<String>,
    settle: Option<String>,
    dex: Option<String>,
}

impl ProductFilter {
    pub(super) fn parse(params: &JsonValue) -> Result<Self, ExchangeError> {
        let JsonValue::Object(params) = params else {
            return if params.is_null() {
                Ok(Self::default())
            } else {
                Err(ExchangeError::BadSymbol(
                    "market parameters must be an object".into(),
                ))
            };
        };
        let mut filter = Self::default();
        if let Some(value) = selector(params, "type")? {
            filter.market_type = Some(match value.to_ascii_lowercase().as_str() {
                "spot" => UnifiedMarketType::Spot,
                "swap" | "perp" | "perpetual" => UnifiedMarketType::Perp,
                "future" | "futures" | "delivery" => UnifiedMarketType::Future,
                "option" | "options" => UnifiedMarketType::Option,
                _ => return Err(ExchangeError::BadSymbol("unsupported market type".into())),
            });
        }
        if let Some(value) = selector(params, "category")? {
            let category = match value.to_ascii_lowercase().as_str() {
                "" => None,
                "options" => Some("option".to_string()),
                value => Some(value.to_string()),
            };
            if let Some(category) = category {
                if !matches!(category.as_str(), "spot" | "linear" | "inverse" | "option") {
                    return Err(ExchangeError::BadSymbol(
                        "unsupported market category".into(),
                    ));
                }
                filter.category = Some(category);
            }
        }
        if let Some(value) = selector(params, "subType")? {
            let subtype = value.to_ascii_lowercase();
            if !matches!(subtype.as_str(), "linear" | "inverse") {
                return Err(ExchangeError::BadSymbol(
                    "unsupported market subType".into(),
                ));
            }
            filter.subtype = Some(subtype);
        }
        filter.settle = selector(params, "settle")?.map(str::to_string);
        filter.dex = selector(params, "dex")?.map(str::to_string);
        let incompatible = match (filter.market_type, filter.category.as_deref()) {
            (Some(UnifiedMarketType::Spot), Some(category)) => category != "spot",
            (Some(UnifiedMarketType::Option), Some(category)) => category != "option",
            (Some(UnifiedMarketType::Perp | UnifiedMarketType::Future), Some(category)) => {
                matches!(category, "spot" | "option")
            }
            _ => false,
        } || matches!(
            (filter.category.as_deref(), filter.subtype.as_deref()),
            (Some("linear"), Some("inverse")) | (Some("inverse"), Some("linear"))
        ) || (filter.subtype.is_some()
            && (filter.market_type == Some(UnifiedMarketType::Spot)
                || filter.category.as_deref() == Some("spot")));
        if incompatible {
            return Err(ExchangeError::BadSymbol(
                "conflicting market product selectors".into(),
            ));
        }
        Ok(filter)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.market_type.is_none()
            && self.category.is_none()
            && self.subtype.is_none()
            && self.settle.is_none()
            && self.dex.is_none()
    }

    pub(super) fn apply_data_default(&mut self, venue: Venue) {
        if self.market_type.is_none() && self.category.is_none() && self.subtype.is_none() {
            let (market_type, category) =
                super::super::venues::dispatch!(venue, exchange => exchange::data_default());
            self.market_type = market_type;
            self.category = category.map(str::to_string);
        }
    }

    pub(super) fn matches(&self, entry: &CatalogMarket) -> bool {
        let identity = entry.market.identity.as_ref();
        if self
            .market_type
            .is_some_and(|kind| entry.market.market_type != kind)
        {
            return false;
        }
        if let Some(category) = self.category.as_deref() {
            let matches = match category {
                "spot" => entry.market.market_type == UnifiedMarketType::Spot,
                "option" => entry.market.market_type == UnifiedMarketType::Option,
                "linear" => {
                    entry.linear == Some(true)
                        && entry.market.market_type != UnifiedMarketType::Option
                }
                "inverse" => {
                    entry.inverse == Some(true)
                        && entry.market.market_type != UnifiedMarketType::Option
                }
                _ => false,
            };
            if !matches {
                return false;
            }
        }
        if let Some(subtype) = self.subtype.as_deref() {
            if (subtype == "linear" && entry.linear != Some(true))
                || (subtype == "inverse" && entry.inverse != Some(true))
            {
                return false;
            }
        }
        if let Some(settle) = self.settle.as_deref() {
            if identity.and_then(|id| id.settle.as_deref()) != Some(settle) {
                return false;
            }
        }
        if let Some(dex) = self.dex.as_deref() {
            if identity.and_then(|id| id.dex.as_deref()) != Some(dex) {
                return false;
            }
        }
        true
    }
}

pub(super) fn selector<'a>(
    params: &'a serde_json::Map<String, JsonValue>,
    key: &str,
) -> Result<Option<&'a str>, ExchangeError> {
    match params.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.trim())),
        Some(_) => Err(ExchangeError::BadSymbol(format!(
            "`{key}` must be a string"
        ))),
    }
}
