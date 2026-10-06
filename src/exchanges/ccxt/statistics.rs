//! Stock-CCXT statistics acquisition.
//!
//! One owner-thread call per venue builds an owned [`MarketStatsSourceSnapshot`]
//! from the already-loaded [`CatalogSnapshot`]. Nothing here owns transport,
//! caches, or a second metadata load: the catalog supplies identity, and only
//! stock unified/implicit methods acquire values.
//!
//! Provenance is truthful: each field carries the literal stock method that
//! produced it (`{public_id}:ccxt:{method}`), not the retired native endpoint.
//! Funding/price strings keep the raw upstream spelling; the numeric
//! `volume24h`/`openInterest` variants are finite `f64`s (checked before any
//! JSON conversion can erase NaN/infinity). Fields this poll deliberately did
//! not acquire are `unavailable`/`not-requested` with no receipt, so projection
//! retains the previous observation without freshening it. Every stock call is
//! panic-isolated so one failing method keeps its healthy siblings.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tokio::time::Instant;

use crate::{
    exchanges::traits::ExchangeError,
    market_stats::MarketStatsSourceSnapshot,
    models::{
        CapabilityState, FetchMarketStatsParams, MarketStatsField, MarketStatsFieldName,
        MarketStatsFieldState, MarketStatsRow, MarketStatsSourceFailure, UnifiedMarketType,
    },
};

use super::{
    catalog::CatalogMarket,
    owner::CatalogSnapshot,
    statistics_profile::{catalog_products, field_support, FIELDS, POLL_INTERVAL},
    venue::{Provider, Venue},
    venues,
};

// Integrations import helpers from their defining modules, not through an
// implicit prelude on the orchestration module.
pub(in crate::exchanges::ccxt) mod acquisition;
pub(in crate::exchanges::ccxt) mod fields;
pub(in crate::exchanges::ccxt) mod index;
pub(in crate::exchanges::ccxt) mod scalars;

use acquisition::Acquired;
use fields::{fixed, not_requested, unavailable};

/// Reason for a field this poll intentionally did not acquire. Projection
/// carries the previous observation and its original monotonic receipt.
pub(super) const NOT_REQUESTED: &str = "not-requested";

/// Acquisition wall/monotonic receipt for one completed stock method.
#[derive(Debug, Clone, Copy)]
pub(in crate::exchanges::ccxt) struct Receipt {
    pub at: Instant,
    pub wall: u64,
}

impl Receipt {
    pub fn now() -> Self {
        Self {
            at: Instant::now(),
            wall: now_millis(),
        }
    }
}

pub(super) async fn fetch_statistics(
    venue: Venue,
    provider: &mut Provider,
    snapshot: &CatalogSnapshot,
    params: &FetchMarketStatsParams,
) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
    let scope = snapshot.scope;
    let catalog = &snapshot.catalog;
    let products: BTreeSet<UnifiedMarketType> =
        catalog_products(venue, scope).iter().copied().collect();

    let mut failures: Vec<MarketStatsSourceFailure> = Vec::new();
    let mut acquired = Acquired::default();

    venues::dispatch!(venue, exchange => exchange::statistics::acquire(
        provider, scope, catalog, params, &mut acquired, &mut failures
    ).await);

    let mut rows = Vec::new();
    let mut field_received_at = HashMap::new();
    for entry in catalog.entries() {
        let Some(identity) = entry.market.identity.as_ref() else {
            continue;
        };
        if !products.contains(&entry.market.market_type) {
            continue;
        }
        let (fields, receipts) = build_fields(venue, entry, &acquired, params.include_bulk);
        if entry.market.active && !receipts.is_empty() {
            field_received_at.insert(identity.market_id.clone(), receipts);
        }
        rows.push(MarketStatsRow {
            market: entry.market.clone(),
            fields,
        });
    }
    rows.sort_unstable_by(|left, right| {
        left.market
            .identity
            .as_ref()
            .map(|identity| &identity.market_id)
            .cmp(
                &right
                    .market
                    .identity
                    .as_ref()
                    .map(|identity| &identity.market_id),
            )
    });

    let contexts_valid = !params.include_bulk || acquired.any_bulk_ok;
    let next_poll_at = if params.include_bulk {
        acquired
            .receipts
            .values()
            .map(|receipt| receipt.at)
            .max()
            .unwrap_or_else(Instant::now)
            + POLL_INTERVAL
    } else {
        // The coordinator preserves the prior bulk deadline after merging this
        // demand-only pass; never advance it from here.
        Instant::now()
    };
    let received_at = if params.include_bulk {
        acquired.receipts.values().map(|receipt| receipt.at).max()
    } else {
        None
    };
    Ok(MarketStatsSourceSnapshot {
        rows,
        // The owner-loaded catalog is authoritative and complete for its profile.
        catalog_known: true,
        complete_catalogs: products,
        contexts_valid,
        received_at,
        field_received_at,
        next_poll_at,
        source_failures: failures,
    })
}

pub(in crate::exchanges::ccxt) fn build_fields(
    venue: Venue,
    entry: &CatalogMarket,
    data: &Acquired,
    bulk: bool,
) -> (
    BTreeMap<MarketStatsFieldName, MarketStatsField>,
    BTreeMap<MarketStatsFieldName, Instant>,
) {
    let product = entry.market.market_type;
    let mut fields = BTreeMap::new();
    let mut receipts = BTreeMap::new();
    for name in FIELDS {
        let support = field_support(venue, product, name);
        let (field, receipt) = match support.state {
            CapabilityState::NotApplicable => (
                fixed(
                    MarketStatsFieldState::NotApplicable,
                    support.reason.as_deref(),
                ),
                None,
            ),
            CapabilityState::Unsupported => (
                fixed(
                    MarketStatsFieldState::Unsupported,
                    support.reason.as_deref(),
                ),
                None,
            ),
            CapabilityState::Supported => {
                let profile = venues::statistics_profile(venue);
                if (!bulk
                    && !(name == MarketStatsFieldName::OpenInterest
                        && profile.selected_open_interest.is_some()))
                    || (product == UnifiedMarketType::Perp
                        && profile.live_perp_fields.contains(&name))
                {
                    (not_requested(), None)
                } else if !entry.market.active {
                    (unavailable("inactive-market"), None)
                } else {
                    let (field, receipt) = venues::dispatch!(venue, exchange => exchange::statistics::observe(entry, name, data));
                    (field, receipt)
                }
            }
        };
        if let Some(receipt) = receipt {
            receipts.insert(name, receipt);
        }
        fields.insert(name, field);
    }
    (fields, receipts)
}

pub(in crate::exchanges::ccxt) fn failure_reason(error: &ExchangeError) -> &'static str {
    match error {
        ExchangeError::UpstreamData(_) => "invalid-upstream-data",
        _ => "upstream-failure",
    }
}

pub(in crate::exchanges::ccxt) fn failure(
    source: &str,
    error: &ExchangeError,
) -> MarketStatsSourceFailure {
    MarketStatsSourceFailure {
        source: source.to_string(),
        reason: failure_reason(error).to_string(),
        message: error.to_string(),
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::exchanges::ccxt) mod test_support;
