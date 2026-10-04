//! Shared, owned market catalog built from loaded stock CCXT metadata.
//!
//! The catalog is the single Ferris-owned projection of a venue's markets: it
//! holds [`CatalogMarket`] rows (owned [`UnifiedMarket`] + owned raw JSON) and
//! the private alias index used to resolve client symbols and native ids.
//! Resolution is deterministic and exact: a symbol that names more than one
//! loaded market is rejected as ambiguous rather than silently resolved to the
//! first row.
//!
//! Acquisition scope belongs to the owner. Selection filters a loaded catalog;
//! callers must acquire the matching profile before requesting another product.

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::exchanges::traits::ExchangeError;
use crate::models::{FetchMarketsParams, UnifiedMarket, UnifiedMarketType};

use super::convert::convert_market;
use super::venue::Venue;

/// One owned catalog row. No CCXT `Value`/`Market` is retained.
#[derive(Debug, Clone)]
pub struct CatalogMarket {
    pub market: UnifiedMarket,
    /// The stock unified symbol this row was loaded under.
    pub ccxt_symbol: String,
    pub linear: Option<bool>,
    pub inverse: Option<bool>,
    /// Owned raw stock market JSON (unified fields plus the original `info`).
    pub raw: JsonValue,
}

pub struct Catalog {
    venue: Venue,
    entries: Vec<CatalogMarket>,
    /// Every resolvable string (display symbol, native id, Ferris market id,
    /// public aliases) mapped to all rows claiming it. Multiple rows mean the
    /// alias is ambiguous and must be disambiguated by parameters.
    aliases: HashMap<String, Vec<usize>>,
    market_ids: HashMap<String, usize>,
    ccxt_symbols: HashMap<String, usize>,
}

impl Catalog {
    /// Convert loaded stock markets into an owned catalog.
    ///
    /// Core identity failures (missing id/base/quote/symbol) and collisions in
    /// the CCXT symbol or Ferris market id are source failures and are rejected.
    pub(super) fn from_markets(
        venue: Venue,
        markets: Vec<ccxt::types::Market>,
        precision_mode: i64,
    ) -> Result<Self, ExchangeError> {
        let mut entries = Vec::with_capacity(markets.len());
        let mut by_market_id: HashMap<String, usize> = HashMap::new();
        let mut by_ccxt_symbol: HashMap<String, usize> = HashMap::new();
        let mut aliases: HashMap<String, Vec<usize>> = HashMap::new();

        for market in &markets {
            let (entry, entry_aliases) = convert_market(venue, market, precision_mode)?;
            let index = entries.len();

            if let Some(identity) = entry.market.identity.as_ref() {
                insert_unique(
                    &mut by_market_id,
                    &identity.market_id,
                    index,
                    "Ferris market id",
                )?;
            }
            insert_unique(
                &mut by_ccxt_symbol,
                &entry.ccxt_symbol,
                index,
                "CCXT symbol",
            )?;

            for alias in entry_aliases {
                let list = aliases.entry(alias).or_default();
                if !list.contains(&index) {
                    list.push(index);
                }
            }

            entries.push(entry);
        }

        Ok(Self {
            venue,
            entries,
            aliases,
            market_ids: by_market_id,
            ccxt_symbols: by_ccxt_symbol,
        })
    }

    pub fn entries(&self) -> &[CatalogMarket] {
        &self.entries
    }

    /// Resolve a client symbol to exactly one loaded market.
    ///
    /// Accepts CCXT symbols, native ids, Ferris market ids and public aliases.
    /// Explicit `type`/`category`/`subType`/`settle`/`dex` parameters narrow the
    /// candidate set; an alias that still names more than one market is rejected.
    /// `coin` and Lighter `market_id`/`marketId` override the symbol, but must
    /// still resolve to loaded metadata and agree with product selectors.
    pub fn resolve(
        &self,
        symbol: &str,
        params: &JsonValue,
    ) -> Result<&CatalogMarket, ExchangeError> {
        let override_symbol = match self.venue {
            Venue::Lighter => params
                .get("market_id")
                .or_else(|| params.get("marketId"))
                .filter(|value| !value.is_null())
                .map(|value| match value {
                    JsonValue::String(value) if value.trim().parse::<u64>().is_ok() => {
                        Ok(std::borrow::Cow::Borrowed(value.trim()))
                    }
                    JsonValue::Number(value) => value
                        .as_u64()
                        .map(|id| std::borrow::Cow::Owned(id.to_string()))
                        .ok_or_else(|| {
                            ExchangeError::BadSymbol("invalid Lighter market id".into())
                        }),
                    _ => Err(ExchangeError::BadSymbol("invalid Lighter market id".into())),
                })
                .transpose()?,
            _ => params
                .as_object()
                .map(|params| selector(params, "coin"))
                .transpose()?
                .flatten()
                .filter(|coin| !coin.is_empty())
                .map(std::borrow::Cow::Borrowed),
        };
        let symbol = override_symbol.as_deref().unwrap_or(symbol).trim();
        if symbol.is_empty() {
            return Err(ExchangeError::BadSymbol(
                "symbol cannot be empty".to_string(),
            ));
        }

        let mut filter = ProductFilter::parse(params)?;
        // Opaque identities and settlement/expiry-qualified CCXT symbols beat
        // legacy defaults. Bare BASE/QUOTE is a legacy display alias, not proof
        // the caller intended spot when a derivatives default applies.
        let exact = self.market_ids.get(symbol).or_else(|| {
            self.ccxt_symbols
                .get(symbol)
                .filter(|index| self.entries[**index].market.symbol != symbol)
        });
        if let Some(index) = exact {
            let entry = &self.entries[*index];
            return if filter.matches(entry) {
                Ok(entry)
            } else {
                Err(ExchangeError::BadSymbol(
                    "market identity conflicts with parameters".into(),
                ))
            };
        }
        filter.apply_data_default(self.venue);
        let Some(candidates) = self.aliases.get(symbol) else {
            return Err(ExchangeError::BadSymbol(format!(
                "unknown {} symbol `{symbol}`",
                self.venue.public_id()
            )));
        };

        let mut matched = candidates
            .iter()
            .map(|index| &self.entries[*index])
            .filter(|entry| filter.matches(entry));
        match (matched.next(), matched.next()) {
            (Some(entry), None) => Ok(entry),
            (None, _) => Err(ExchangeError::BadSymbol(format!(
                "no {} market matches `{symbol}` with the requested parameters",
                self.venue.public_id()
            ))),
            _ => Err(ExchangeError::BadSymbol(format!(
                "ambiguous {} symbol `{symbol}`; use a catalog marketId or explicit product/settlement",
                self.venue.public_id()
            ))),
        }
    }

    /// Select loaded markets for the public catalog response.
    ///
    /// `include_inactive` is honored first. Explicit product selectors filter
    /// the loaded rows; otherwise the venue's historical catalog default applies
    /// (Binance USD-M linear, Bybit linear+inverse+spot, everything loaded
    /// elsewhere).
    pub fn select(
        &self,
        params: &FetchMarketsParams,
    ) -> Result<Vec<&UnifiedMarket>, ExchangeError> {
        let filter = ProductFilter::parse(&params.params)?;
        let explicit = !filter.is_empty();

        let mut selected: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| params.include_inactive || entry.market.active)
            .filter(|entry| {
                if explicit {
                    filter.matches(entry)
                } else {
                    self.matches_default(entry)
                }
            })
            .map(|entry| &entry.market)
            .collect();
        selected.sort_unstable_by(|a, b| {
            a.symbol
                .cmp(&b.symbol)
                .then_with(|| a.market_type.cmp(&b.market_type))
                .then_with(|| {
                    a.identity
                        .as_ref()
                        .map(|id| &id.market_id)
                        .cmp(&b.identity.as_ref().map(|id| &id.market_id))
                })
        });
        Ok(selected)
    }

    fn matches_default(&self, entry: &CatalogMarket) -> bool {
        match self.venue {
            // The native Binance catalog was USD-M (linear) only.
            Venue::Binance => {
                entry.linear == Some(true) && entry.market.market_type != UnifiedMarketType::Option
            }
            // An omitted Bybit category combined linear/inverse/spot, not options.
            Venue::Bybit => entry.market.market_type != UnifiedMarketType::Option,
            Venue::Aster => entry.market.market_type == UnifiedMarketType::Perp,
            _ => true,
        }
    }
}

fn insert_unique(
    map: &mut HashMap<String, usize>,
    key: &str,
    index: usize,
    label: &str,
) -> Result<(), ExchangeError> {
    if let Some(existing) = map.insert(key.to_string(), index) {
        if existing != index {
            return Err(ExchangeError::UpstreamData(format!(
                "duplicate {label} `{key}` across loaded markets"
            )));
        }
    }
    Ok(())
}

pub(super) fn catalog_scope(
    venue: Venue,
    params: &JsonValue,
) -> Result<super::CatalogScope, ExchangeError> {
    use super::CatalogScope;
    let filter = ProductFilter::parse(params)?;
    let scope = match (
        filter.market_type,
        filter.category.as_deref(),
        filter.subtype.as_deref(),
    ) {
        (Some(UnifiedMarketType::Spot), _, _) | (_, Some("spot"), _) => CatalogScope::Spot,
        (Some(UnifiedMarketType::Option), _, _) | (_, Some("option"), _) => CatalogScope::Option,
        (_, Some("inverse"), _) | (_, _, Some("inverse")) => CatalogScope::Inverse,
        (Some(UnifiedMarketType::Perp | UnifiedMarketType::Future), _, _)
        | (_, Some("linear"), _)
        | (_, _, Some("linear")) => CatalogScope::Linear,
        _ => CatalogScope::Default,
    };
    venue.scope(scope)
}

#[derive(Default)]
struct ProductFilter {
    market_type: Option<UnifiedMarketType>,
    category: Option<String>,
    subtype: Option<String>,
    settle: Option<String>,
    dex: Option<String>,
}

impl ProductFilter {
    fn parse(params: &JsonValue) -> Result<Self, ExchangeError> {
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

    fn is_empty(&self) -> bool {
        self.market_type.is_none()
            && self.category.is_none()
            && self.subtype.is_none()
            && self.settle.is_none()
            && self.dex.is_none()
    }

    fn apply_data_default(&mut self, venue: Venue) {
        if self.market_type.is_none() && self.category.is_none() && self.subtype.is_none() {
            match venue {
                Venue::Binance | Venue::Bybit => self.category = Some("linear".into()),
                Venue::Aster => self.market_type = Some(UnifiedMarketType::Perp),
                _ => {}
            }
        }
    }

    fn matches(&self, entry: &CatalogMarket) -> bool {
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

fn selector<'a>(
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use ccxt::runtime::TICK_SIZE;

    use super::*;

    fn market(value: JsonValue) -> ccxt::types::Market {
        ccxt::types::Market::from_value(ccxt::Value::from_json(&value))
    }

    fn params(params: JsonValue, include_inactive: bool) -> FetchMarketsParams {
        FetchMarketsParams {
            params,
            include_inactive,
        }
    }

    fn bybit_linear() -> JsonValue {
        json!({
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "type": "swap",
            "spot": false,
            "swap": true,
            "future": false,
            "option": false,
            "active": true,
            "contract": true,
            "linear": true,
            "inverse": false,
            "contractSize": 1.0,
            "precision": {"amount": 0.001, "price": 0.1},
            "limits": {"amount": {"min": 0.001, "max": null}},
            "info": {
                "symbol": "BTCUSDT",
                "contractType": "LinearPerpetual",
                "status": "Trading",
                "settleCoin": "USDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT"
            }
        })
    }

    fn bybit_inverse() -> JsonValue {
        json!({
            "id": "BTCUSD",
            "symbol": "BTC/USD:BTC",
            "base": "BTC",
            "quote": "USD",
            "settle": "BTC",
            "type": "swap",
            "spot": false,
            "swap": true,
            "future": false,
            "option": false,
            "active": true,
            "contract": true,
            "linear": false,
            "inverse": true,
            "contractSize": 1.0,
            "precision": {"amount": 0.001, "price": 0.5},
            "limits": {"amount": {"min": 0.001, "max": null}},
            "info": {
                "symbol": "BTCUSD",
                "contractType": "InversePerpetual",
                "status": "Trading",
                "settleCoin": "BTC",
                "baseCoin": "BTC",
                "quoteCoin": "USD"
            }
        })
    }

    fn bybit_spot() -> JsonValue {
        json!({
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "settle": null,
            "type": "spot",
            "spot": true,
            "swap": false,
            "future": false,
            "option": false,
            "active": true,
            "contract": false,
            "linear": null,
            "inverse": null,
            "precision": {"amount": 0.000001, "price": 0.01},
            "limits": {"amount": {"min": null, "max": null}},
            "info": {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "status": "Trading"
            }
        })
    }

    fn bybit_option() -> JsonValue {
        json!({
            "id": "BTC-27FEB26-100000-C-USDT",
            "symbol": "BTC/USDT:USDT-260227-100000-C",
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "type": "option",
            "spot": false,
            "swap": false,
            "future": false,
            "option": true,
            "active": true,
            "contract": true,
            "linear": null,
            "inverse": null,
            "contractSize": 0.01,
            "precision": {"amount": 0.01, "price": 5.0},
            "limits": {"amount": {"min": 0.01, "max": null}},
            "info": {
                "symbol": "BTC-27FEB26-100000-C-USDT",
                "contractType": "Option",
                "status": "Trading"
            }
        })
    }

    fn binance_linear() -> JsonValue {
        json!({
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "type": "swap",
            "spot": false,
            "swap": true,
            "future": false,
            "option": false,
            "active": true,
            "contract": true,
            "linear": true,
            "inverse": false,
            "contractSize": 1.0,
            "precision": {"amount": 0.001, "price": 0.1},
            "limits": {"amount": {"min": 0.001, "max": null}},
            "info": {
                "symbol": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "marginAsset": "USDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            }
        })
    }

    fn binance_spot() -> JsonValue {
        json!({
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "settle": null,
            "type": "spot",
            "spot": true,
            "swap": false,
            "future": false,
            "option": false,
            "active": true,
            "contract": false,
            "linear": null,
            "inverse": null,
            "precision": {"amount": 0.000001, "price": 0.01},
            "limits": {"amount": {"min": null, "max": null}},
            "info": {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            }
        })
    }

    fn hyperliquid_perp(name: &str, id: &str, dex: Option<&str>) -> JsonValue {
        let mut info = json!({"name": name, "szDecimals": 4, "maxLeverage": 50});
        if let Some(dex) = dex {
            info["dex"] = json!(dex);
            info["collateralTokenName"] = json!("USDC");
        }
        json!({
            "id": id,
            "symbol": format!("{name}/USDC:USDC"),
            "base": name,
            "quote": "USDC",
            "settle": "USDC",
            "type": "swap",
            "spot": false,
            "swap": true,
            "future": false,
            "option": false,
            "active": true,
            "contract": true,
            "linear": true,
            "inverse": false,
            "contractSize": 1.0,
            "precision": {"amount": 0.0001, "price": 0.1},
            "limits": {"amount": {"min": null, "max": null}},
            "info": info
        })
    }

    fn hyperliquid_spot() -> JsonValue {
        json!({
            "id": "PURR/USDC",
            "symbol": "PURR/USDC",
            "base": "PURR",
            "quote": "USDC",
            "settle": null,
            "type": "spot",
            "spot": true,
            "swap": false,
            "future": false,
            "option": false,
            "active": true,
            "contract": false,
            "linear": null,
            "inverse": null,
            "precision": {"amount": 1.0, "price": 0.001},
            "limits": {"amount": {"min": null, "max": null}},
            "info": {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}
        })
    }

    #[test]
    fn hyperliquid_perp_identity_uses_native_name_and_primary_dex() {
        let catalog = Catalog::from_markets(
            Venue::Hyperliquid,
            vec![market(hyperliquid_perp("BTC", "0", None))],
            TICK_SIZE,
        )
        .unwrap();

        let entry = catalog.resolve("BTC", &json!({})).unwrap();
        let identity = entry.market.identity.as_ref().unwrap();

        assert_eq!(entry.market.market_type, UnifiedMarketType::Perp);
        assert_eq!(entry.market.symbol, "BTC/USDC");
        assert_eq!(identity.exchange_market_id, "BTC");
        assert_eq!(identity.dex.as_deref(), Some(""));
        assert_eq!(identity.settle.as_deref(), Some("USDC"));
        assert_eq!(
            identity.market_id,
            crate::market_stats::make_market_id(
                "hyperliquid",
                UnifiedMarketType::Perp,
                None,
                Some(""),
                "BTC"
            )
            .unwrap()
        );
        // TICK_SIZE price precision is the actual tick, not a significant-digit count.
        assert_eq!(entry.market.tick_size, Some(0.1));
        assert_eq!(
            catalog
                .resolve("BTC/USDC:USDC", &json!({}))
                .unwrap()
                .ccxt_symbol,
            "BTC/USDC:USDC"
        );
    }

    #[test]
    fn hyperliquid_spot_identity_uses_metadata_index() {
        let catalog = Catalog::from_markets(
            Venue::Hyperliquid,
            vec![market(hyperliquid_spot())],
            TICK_SIZE,
        )
        .unwrap();

        let entry = catalog.resolve("PURR/USDC", &json!({})).unwrap();
        let identity = entry.market.identity.as_ref().unwrap();

        assert_eq!(entry.market.market_type, UnifiedMarketType::Spot);
        assert_eq!(identity.exchange_market_id, "@0");
        assert_eq!(
            identity.market_id,
            crate::market_stats::make_market_id(
                "hyperliquid",
                UnifiedMarketType::Spot,
                None,
                None,
                "@0"
            )
            .unwrap()
        );
        assert_eq!(
            catalog.resolve("@0", &json!({})).unwrap().ccxt_symbol,
            "PURR/USDC"
        );
        assert!(identity.settle.is_none());
    }

    #[test]
    fn bybit_perp_identity_carries_category_and_native_settle() {
        let catalog =
            Catalog::from_markets(Venue::Bybit, vec![market(bybit_linear())], TICK_SIZE).unwrap();
        let entry = catalog.resolve("BTCUSDT", &json!({})).unwrap();
        let identity = entry.market.identity.as_ref().unwrap();

        assert_eq!(identity.category.as_deref(), Some("linear"));
        assert_eq!(identity.settle.as_deref(), Some("USDT"));
        assert_eq!(entry.market.info.category.as_deref(), Some("linear"));
        assert_eq!(
            identity.market_id,
            crate::market_stats::make_market_id(
                "bybit",
                UnifiedMarketType::Perp,
                Some("linear"),
                None,
                "BTCUSDT"
            )
            .unwrap()
        );
    }

    #[test]
    fn bybit_prelisting_contract_is_inactive() {
        let mut value = bybit_linear();
        value["info"]["isPreListing"] = json!(true);
        let catalog = Catalog::from_markets(Venue::Bybit, vec![market(value)], TICK_SIZE).unwrap();
        let entry = catalog.resolve("BTCUSDT", &json!({})).unwrap();
        assert!(!entry.market.active);
    }

    #[test]
    fn bybit_ambiguous_display_and_native_aliases_are_rejected_or_disambiguated() {
        let catalog = Catalog::from_markets(
            Venue::Bybit,
            vec![market(bybit_linear()), market(bybit_spot())],
            TICK_SIZE,
        )
        .unwrap();

        assert_eq!(
            catalog.resolve("BTC/USDT", &json!({})).unwrap().ccxt_symbol,
            "BTC/USDT:USDT"
        );
        assert_eq!(
            catalog.resolve("BTCUSDT", &json!({})).unwrap().ccxt_symbol,
            "BTC/USDT:USDT"
        );

        let linear = catalog
            .resolve("BTC/USDT", &json!({"category": "linear"}))
            .unwrap();
        assert_eq!(linear.ccxt_symbol, "BTC/USDT:USDT");
        assert_eq!(linear.market.market_type, UnifiedMarketType::Perp);

        let spot = catalog
            .resolve("BTC/USDT", &json!({"type": "spot"}))
            .unwrap();
        assert_eq!(spot.ccxt_symbol, "BTC/USDT");
        assert_eq!(spot.market.market_type, UnifiedMarketType::Spot);

        let native_spot = catalog
            .resolve("BTCUSDT", &json!({"category": "spot"}))
            .unwrap();
        assert_eq!(native_spot.market.market_type, UnifiedMarketType::Spot);
    }

    #[test]
    fn bybit_omitted_category_selects_linear_inverse_spot() {
        let catalog = Catalog::from_markets(
            Venue::Bybit,
            vec![
                market(bybit_linear()),
                market(bybit_inverse()),
                market(bybit_spot()),
                market(bybit_option()),
            ],
            TICK_SIZE,
        )
        .unwrap();

        let selected = catalog.select(&params(json!({}), false)).unwrap();
        assert_eq!(selected.len(), 3);
        assert!(selected
            .iter()
            .all(|market| market.market_type != UnifiedMarketType::Option));

        let options = catalog
            .select(&params(json!({"category": "option"}), false))
            .unwrap();
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].market_type, UnifiedMarketType::Option);
    }

    #[test]
    fn binance_default_select_is_usd_m_linear() {
        let catalog = Catalog::from_markets(
            Venue::Binance,
            vec![market(binance_linear()), market(binance_spot())],
            TICK_SIZE,
        )
        .unwrap();

        let default = catalog.select(&params(json!({}), false)).unwrap();
        assert_eq!(default.len(), 1);
        assert_eq!(default[0].market_type, UnifiedMarketType::Perp);

        let spot = catalog
            .select(&params(json!({"type": "spot"}), false))
            .unwrap();
        assert_eq!(spot.len(), 1);
        assert_eq!(spot[0].market_type, UnifiedMarketType::Spot);
    }

    #[test]
    fn select_respects_explicit_dex_filter() {
        let catalog = Catalog::from_markets(
            Venue::Hyperliquid,
            vec![
                market(hyperliquid_perp("BTC", "0", None)),
                market(hyperliquid_perp("ETH", "1", Some("hip3"))),
            ],
            TICK_SIZE,
        )
        .unwrap();

        let primary = catalog.select(&params(json!({"dex": ""}), false)).unwrap();
        assert_eq!(primary.len(), 1);
        assert_eq!(primary[0].base, "BTC");

        let hip3 = catalog
            .select(&params(json!({"dex": "hip3"}), false))
            .unwrap();
        assert_eq!(hip3.len(), 1);
        assert_eq!(hip3[0].base, "ETH");
    }

    #[test]
    fn select_filters_inactive_rows() {
        let mut value = binance_linear();
        value["active"] = json!(false);
        let catalog =
            Catalog::from_markets(Venue::Binance, vec![market(value)], TICK_SIZE).unwrap();

        assert!(catalog
            .select(&params(json!({}), false))
            .unwrap()
            .is_empty());
        assert_eq!(catalog.select(&params(json!({}), true)).unwrap().len(), 1);
    }

    #[test]
    fn conversion_rejects_invalid_core_identity() {
        let mut value = binance_linear();
        value["base"] = json!("");
        let result = Catalog::from_markets(Venue::Binance, vec![market(value)], TICK_SIZE);
        assert!(matches!(result, Err(ExchangeError::UpstreamData(_))));
    }

    #[test]
    fn conversion_rejects_duplicate_ccxt_symbols() {
        let result = Catalog::from_markets(
            Venue::Binance,
            vec![market(binance_linear()), market(binance_linear())],
            TICK_SIZE,
        );
        assert!(matches!(result, Err(ExchangeError::UpstreamData(_))));
    }
    #[test]
    fn settlement_variants_require_exact_identity_and_preserve_assets() {
        let mut usdt = bybit_linear();
        usdt["base"] = json!("字.μ");
        usdt["id"] = json!("字.μ-USDT");
        usdt["symbol"] = json!("字.μ/USDT:USDT");
        let mut usdc = usdt.clone();
        usdc["id"] = json!("字.μ-USDC");
        usdc["symbol"] = json!("字.μ/USDT:USDC");
        usdc["settle"] = json!("USDC");
        usdc["info"]["settleCoin"] = json!("USDC");
        let catalog =
            Catalog::from_markets(Venue::Bybit, vec![market(usdt), market(usdc)], TICK_SIZE)
                .unwrap();
        assert!(matches!(
            catalog.resolve("字.μ/USDT", &json!({})),
            Err(ExchangeError::BadSymbol(_))
        ));
        let row = catalog
            .resolve("字.μ/USDT", &json!({"settle": "USDC"}))
            .unwrap();
        assert_eq!(row.market.base, "字.μ");
        assert_eq!(
            row.market.identity.as_ref().unwrap().settle.as_deref(),
            Some("USDC")
        );
        for row in catalog.entries() {
            let identity = row.market.identity.as_ref().unwrap();
            assert_eq!(
                catalog
                    .resolve(&identity.market_id, &json!({}))
                    .unwrap()
                    .ccxt_symbol,
                row.ccxt_symbol
            );
        }
    }

    #[test]
    fn product_conflicts_never_resolve_to_a_different_market() {
        let catalog = Catalog::from_markets(
            Venue::Bybit,
            vec![
                market(bybit_linear()),
                market(bybit_spot()),
                market(bybit_inverse()),
                market(bybit_option()),
            ],
            TICK_SIZE,
        )
        .unwrap();
        assert!(catalog
            .resolve("BTCUSDT", &json!({"type":"spot", "category":"linear"}))
            .is_err());
        assert!(catalog
            .resolve(
                "BTCUSDT",
                &json!({"category":"linear", "subType":"inverse"})
            )
            .is_err());
        let inverse = catalog.resolve("BTC/USD:BTC", &json!({})).unwrap();
        assert_eq!(
            inverse
                .market
                .identity
                .as_ref()
                .unwrap()
                .category
                .as_deref(),
            Some("inverse")
        );
        let spot = catalog
            .entries()
            .iter()
            .find(|row| row.market.market_type == UnifiedMarketType::Spot)
            .unwrap();
        let id = &spot.market.identity.as_ref().unwrap().market_id;
        assert_eq!(
            catalog.resolve(id, &json!({})).unwrap().market.market_type,
            UnifiedMarketType::Spot
        );
        assert!(catalog.resolve(id, &json!({"category":"linear"})).is_err());
    }

    #[test]
    fn duplicate_native_identity_and_unqualified_collateral_fail_closed() {
        let mut other = bybit_linear();
        other["symbol"] = json!("OTHER/USDT:USDT");
        other["base"] = json!("OTHER");
        let duplicate = Catalog::from_markets(
            Venue::Bybit,
            vec![market(bybit_linear()), market(other)],
            TICK_SIZE,
        );
        assert!(matches!(duplicate, Err(ExchangeError::UpstreamData(_))));
        let mut hip3 = hyperliquid_perp("dex:BTC", "110000", Some("dex"));
        hip3["info"]
            .as_object_mut()
            .unwrap()
            .remove("collateralTokenName");
        assert!(matches!(
            Catalog::from_markets(Venue::Hyperliquid, vec![market(hip3)], TICK_SIZE),
            Err(ExchangeError::UpstreamData(_))
        ));
    }

    #[test]
    fn explicit_native_selectors_override_display_without_guessing_product() {
        let mut spot = hyperliquid_spot();
        spot["id"] = json!("@42");
        spot["symbol"] = json!("BTC/USDC");
        spot["base"] = json!("BTC");
        spot["info"]["index"] = json!(42);
        let catalog = Catalog::from_markets(
            Venue::Hyperliquid,
            vec![
                market(hyperliquid_perp("BTC", "0", None)),
                market(spot.clone()),
            ],
            TICK_SIZE,
        )
        .unwrap();
        assert!(matches!(
            catalog.resolve("BTC", &json!({})),
            Err(ExchangeError::BadSymbol(_))
        ));
        assert_eq!(
            catalog
                .resolve("ignored", &json!({"coin":"@42"}))
                .unwrap()
                .market
                .market_type,
            UnifiedMarketType::Spot
        );
        assert_eq!(
            catalog
                .resolve("ignored", &json!({"coin":"BTC", "type":"swap"}))
                .unwrap()
                .market
                .market_type,
            UnifiedMarketType::Perp
        );
        assert!(catalog
            .resolve("BTC/USDC:USDC", &json!({"coin":"missing"}))
            .is_err());
        spot["id"] = json!("42");
        let lighter = Catalog::from_markets(Venue::Lighter, vec![market(spot)], TICK_SIZE).unwrap();
        assert_eq!(
            lighter
                .resolve("ignored", &json!({"market_id":42, "marketId":999}))
                .unwrap()
                .ccxt_symbol,
            "BTC/USDC"
        );
        assert!(lighter
            .resolve("BTC", &json!({"market_id":42, "type":"swap"}))
            .is_err());
        assert!(lighter.resolve("BTC", &json!({"market_id":42.5})).is_err());
    }
}
