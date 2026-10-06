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
mod filter;
use filter::ProductFilter;

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
        let override_symbol =
            super::venues::dispatch!(self.venue, exchange => exchange::symbol_override(params))?;
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
        super::venues::dispatch!(self.venue, exchange => exchange::catalog_default(entry))
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

#[cfg(test)]
mod tests;
