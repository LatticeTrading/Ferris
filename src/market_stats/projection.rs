use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::de::IgnoredAny;
use serde_json::Value;
use tokio::time::Instant;

use super::{make_market_id, MarketStatsSourceSnapshot};
use crate::{
    errors::ApiError,
    exchanges::{
        ccxt::{statistics_profile, Venue},
        traits::ExchangeError,
    },
    models::{
        FetchMarketStatsRequest, MarketStatsCoverage, MarketStatsField, MarketStatsFieldName,
        MarketStatsFieldState, MarketStatsRow, MarketStatsScope, MarketStatsSnapshot,
        MarketStatsSourceFailure, MarketStatsTopic, UnifiedMarketType,
    },
};

#[cfg(test)]
const PRIMARY_SOURCE: &str = "hyperliquid:primary:metaAndAssetCtxs";
#[cfg(test)]
const SPOT_SOURCE: &str = "hyperliquid:spotMeta";
use statistics_profile::STALE_AFTER;

fn catalog_source(exchange: &str, _product: UnifiedMarketType, _params: &Value) -> &'static str {
    Venue::from_public_id(exchange)
        .map(statistics_profile::catalog_source)
        .unwrap_or("ccxt:loadMarkets")
}

const POLL_INTERVAL: Duration = Duration::from_secs(30);

pub fn normalize_topic(request: FetchMarketStatsRequest) -> Result<MarketStatsTopic, ApiError> {
    let exchange = request.exchange.trim().to_ascii_lowercase();
    let venue = Venue::from_public_id(&exchange)
        .ok_or_else(|| ApiError::UnsupportedExchange(exchange.clone()))?;
    let params = statistics_profile::normalize_params(venue, &request.params)
        .map_err(|error| ApiError::Validation(error.to_string()))?;

    let market_ids = match request.market_ids {
        Some(mut ids) => {
            if ids.is_empty() || ids.len() > 100 {
                return Err(ApiError::Validation(
                    "marketIds must contain between 1 and 100 IDs".to_string(),
                ));
            }
            for id in &ids {
                market_id_type(id, &exchange, &params)?;
            }
            ids.sort_unstable();
            ids.dedup();
            Some(ids)
        }
        None => None,
    };
    let mut fields = request
        .fields
        .unwrap_or_else(|| vec![MarketStatsFieldName::Funding]);
    if fields.is_empty() {
        return Err(ApiError::Validation("fields must not be empty".to_string()));
    }
    fields.sort_unstable();
    fields.dedup();
    if fields.contains(&MarketStatsFieldName::OpenInterest) && market_ids.is_none() {
        if let Some(message) = statistics_profile::selected_open_interest(venue) {
            return Err(ApiError::Validation(message.into()));
        }
    }
    Ok(MarketStatsTopic {
        exchange,
        params,
        market_ids,
        fields,
    })
}

fn market_id_type(id: &str, exchange: &str, params: &Value) -> Result<UnifiedMarketType, ApiError> {
    let invalid = || ApiError::Validation(format!("invalid marketId: {id}"));
    let (id_exchange, product, category, dex, native_id): (
        String,
        UnifiedMarketType,
        Option<String>,
        Option<String>,
        String,
    ) = serde_json::from_str(id).map_err(|_| invalid())?;
    let venue = Venue::from_public_id(exchange).ok_or_else(invalid)?;
    let scope = statistics_profile::scope(venue, params).map_err(|_| invalid())?;
    if id_exchange != exchange
        || native_id.is_empty()
        || !statistics_profile::catalog_products(venue, scope).contains(&product)
        || !statistics_profile::selection_matches(
            venue,
            product,
            category.as_deref(),
            dex.as_deref(),
            &native_id,
            params,
        )
    {
        return Err(invalid());
    }
    let canonical = make_market_id(
        &id_exchange,
        product,
        category.as_deref(),
        dex.as_deref(),
        &native_id,
    )?;
    if canonical != id {
        return Err(invalid());
    }
    Ok(product)
}

pub fn validate_selection(
    topic: &MarketStatsTopic,
    snapshot: &MarketStatsSourceSnapshot,
) -> Result<(), ApiError> {
    let Some(ids) = &topic.market_ids else {
        return Ok(());
    };
    let known: HashSet<&str> = snapshot.rows.iter().filter_map(row_id).collect();
    for id in ids {
        let product = market_id_type(id, &topic.exchange, &topic.params)?;
        if known.contains(id.as_str()) {
            continue;
        }
        let complete = snapshot.complete_catalogs.contains(&product);
        let source = catalog_source(&topic.exchange, product, &topic.params);
        if complete {
            return Err(ApiError::Validation(format!("unknown marketId: {id}")));
        }
        return Err(incomplete_catalog_error(snapshot, source));
    }
    Ok(())
}

fn incomplete_catalog_error(snapshot: &MarketStatsSourceSnapshot, source: &str) -> ApiError {
    if let Some(failure) = snapshot.source_failures.iter().find(|failure| {
        failure.source == source
            && matches!(
                failure.reason.as_str(),
                "invalid-upstream-data" | "context-mismatch" | "upstream-failure"
            )
    }) {
        return match failure.reason.as_str() {
            "upstream-failure" => ExchangeError::UpstreamRequest(failure.message.clone()).into(),
            _ => ExchangeError::UpstreamData(failure.message.clone()).into(),
        };
    }
    ExchangeError::UpstreamRequest(format!(
        "{source} catalog is incomplete; cannot establish selected market identity"
    ))
    .into()
}

pub fn project_snapshot(
    topic: &MarketStatsTopic,
    snapshot: &MarketStatsSourceSnapshot,
) -> MarketStatsSnapshot {
    let all_product = statistics_profile::all_market_product(&topic.params);
    let requested_dex = topic.params.get("dex").and_then(Value::as_str);
    let matches_scope = |row: &MarketStatsRow| {
        requested_dex.is_none_or(|dex| {
            row.market
                .identity
                .as_ref()
                .and_then(|id| id.dex.as_deref())
                == Some(dex)
        })
    };
    let mut markets: Vec<_> = snapshot
        .rows
        .iter()
        .filter(|row| matches_scope(row))
        .filter(|row| {
            let Some(id) = row_id(row) else { return false };
            match &topic.market_ids {
                Some(ids) => ids
                    .binary_search_by(|selected| selected.as_str().cmp(id))
                    .is_ok(),
                None => row.market.market_type == all_product && row.market.active,
            }
        })
        .map(|row| MarketStatsRow {
            market: row.market.clone(),
            fields: topic
                .fields
                .iter()
                .map(|name| (*name, row.fields[name].clone()))
                .collect(),
        })
        .collect();
    markets.sort_unstable_by(|left, right| row_id(left).cmp(&row_id(right)));

    let (expected_markets, enumeration_complete) = match &topic.market_ids {
        Some(ids) => {
            let complete = ids.iter().all(|id| {
                // Topics are already validated. Ignore native strings rather than allocating
                // and reserializing every identity for each client projection.
                let decoded: Result<
                    (
                        IgnoredAny,
                        UnifiedMarketType,
                        IgnoredAny,
                        IgnoredAny,
                        IgnoredAny,
                    ),
                    _,
                > = serde_json::from_str(id);
                match decoded {
                    Ok((_, product, _, _, _)) => snapshot.complete_catalogs.contains(&product),
                    _ => false,
                }
            });
            (Some(ids.len()), complete)
        }
        None => (
            snapshot.catalog_known.then(|| {
                snapshot
                    .rows
                    .iter()
                    .filter(|row| matches_scope(row))
                    .filter(|row| row.market.market_type == all_product && row.market.active)
                    .count()
            }),
            snapshot.complete_catalogs.contains(&all_product),
        ),
    };
    MarketStatsSnapshot {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        scope: MarketStatsScope {
            exchange: topic.exchange.clone(),
            params: topic.params.clone(),
        },
        coverage: MarketStatsCoverage {
            expected_markets,
            returned_markets: markets.len(),
            enumeration_complete,
            source_failures: snapshot.source_failures.clone(),
        },
        markets,
    }
}

pub fn merge_outcome(
    previous: &MarketStatsSourceSnapshot,
    outcome: Result<MarketStatsSourceSnapshot, ExchangeError>,
    exchange: &str,
    params: &Value,
) -> MarketStatsSourceSnapshot {
    let mut next = match outcome {
        Ok(next) => next,
        Err(error) => {
            let reason = match error {
                ExchangeError::UpstreamData(_) => "invalid-upstream-data",
                _ => "upstream-failure",
            };
            let mut next = previous.clone();
            next.complete_catalogs.clear();
            next.contexts_valid = false;
            next.next_poll_at = Instant::now() + POLL_INTERVAL;
            // The failed source call publishes no secondary catalog proof. Retain known
            // spot rows, but do not declare a missing selection unknown from old metadata.
            let source = catalog_source(exchange, UnifiedMarketType::Perp, params);
            next.source_failures
                .retain(|failure| failure.source != source);
            next.source_failures.push(MarketStatsSourceFailure {
                source: source.to_string(),
                reason: reason.to_string(),
                message: error.to_string(),
            });
            for row in &mut next.rows {
                fail_row(row, reason);
            }
            return next;
        }
    };
    if next.received_at.is_none() {
        next.received_at = previous.received_at;
    }

    let prior_rows: HashMap<&str, &MarketStatsRow> = previous
        .rows
        .iter()
        .filter_map(|row| row_id(row).map(|id| (id, row)))
        .collect();
    for row in &mut next.rows {
        if !row.market.active {
            continue;
        }
        let row_key = row_id(row).map(str::to_owned);
        let prior = row_key
            .as_deref()
            .and_then(|id| prior_rows.get(id).copied());
        for (name, field) in &mut row.fields {
            // This poll did not acquire this field (maintained stream or singular
            // demand not due). Carry its observation and original monotonic receipt.
            if field.reason.as_deref() == Some("not-requested") {
                if let Some(old) = prior.and_then(|prior| prior.fields.get(name)) {
                    *field = old.clone();
                    if let Some((id, receipt)) = row_key.as_deref().and_then(|id| {
                        previous
                            .field_received_at
                            .get(id)?
                            .get(name)
                            .map(|at| (id, *at))
                    }) {
                        next.field_received_at
                            .entry(id.to_string())
                            .or_default()
                            .insert(*name, receipt);
                    }
                }
                continue;
            }
            if structural(field) {
                continue;
            }
            let field_failure = field.state == MarketStatsFieldState::Unavailable
                && matches!(
                    field.reason.as_deref(),
                    Some(
                        "upstream-failure"
                            | "invalid-upstream-data"
                            | "context-mismatch"
                            | "missing-upstream-row"
                    )
                );
            let incoming_receipt = row_key
                .as_deref()
                .and_then(|id| next.field_received_at.get(id))
                .and_then(|fields| fields.get(name))
                .copied();
            let prior_receipt = row_key
                .as_deref()
                .and_then(|id| previous.field_received_at.get(id))
                .and_then(|fields| fields.get(name))
                .copied();
            if !field_failure
                && matches!((incoming_receipt, prior_receipt), (Some(incoming), Some(prior)) if incoming <= prior)
            {
                if let Some(old) = prior.and_then(|prior| prior.fields.get(name)) {
                    *field = old.clone();
                    next.field_received_at
                        .entry(row_key.as_ref().unwrap().clone())
                        .or_default()
                        .insert(*name, prior_receipt.unwrap());
                }
                continue;
            }
            if field.reason.as_deref() == Some("invalid-upstream-value") {
                continue;
            }
            let newly_observed = field.state == MarketStatsFieldState::Available
                && incoming_receipt.is_some()
                && incoming_receipt > prior_receipt;
            if (next.contexts_valid && !field_failure) || newly_observed {
                continue;
            }
            let incoming_reason = if field_failure {
                field.reason.clone().unwrap()
            } else {
                let source = field.source.as_deref().unwrap_or("");
                next.source_failures
                    .iter()
                    .find(|failure| failure.source == source)
                    .map(|failure| failure.reason.clone())
                    .unwrap_or_else(|| "context-mismatch".to_string())
            };
            if let Some(old) = prior.and_then(|prior| prior.fields.get(name)) {
                if usable(old) || old.reason.as_deref() == Some("invalid-upstream-value") {
                    *field = old.clone();
                    if let Some(id) = row_key.as_deref() {
                        if let Some(receipt) = previous
                            .field_received_at
                            .get(id)
                            .and_then(|fields| fields.get(name))
                        {
                            next.field_received_at
                                .entry(id.to_string())
                                .or_default()
                                .insert(*name, *receipt);
                        }
                    }
                }
            }
            fail_field(field, &incoming_reason);
        }
    }

    // Only a complete product catalog can remove identities. A secondary partial result can
    // update known spot rows, but cannot make previously selected spot identities disappear.
    let incoming_ids: HashSet<&str> = next.rows.iter().filter_map(row_id).collect();
    let retained: Vec<_> = previous
        .rows
        .iter()
        .filter(|row| {
            let complete = next.complete_catalogs.contains(&row.market.market_type);
            !complete && row_id(row).is_some_and(|id| !incoming_ids.contains(id))
        })
        .cloned()
        .collect();
    for mut row in retained {
        let retained_id = row_id(&row).map(str::to_owned);
        if row.market.active {
            let reason = next
                .source_failures
                .iter()
                .find(|failure| {
                    failure.source == catalog_source(exchange, UnifiedMarketType::Perp, params)
                })
                .map(|failure| failure.reason.as_str())
                .unwrap_or("upstream-failure");
            fail_row(&mut row, reason);
        }
        if let Some(id) = retained_id {
            if let Some(times) = previous.field_received_at.get(&id) {
                next.field_received_at
                    .entry(id)
                    .or_insert_with(|| times.clone());
            }
        }
        next.rows.push(row);
    }
    next.catalog_known |= previous.catalog_known;
    next.rows
        .sort_unstable_by(|left, right| row_id(left).cmp(&row_id(right)));
    next
}

pub fn expire_snapshot(
    snapshot: &MarketStatsSourceSnapshot,
    now: Instant,
) -> Option<MarketStatsSourceSnapshot> {
    let mut expired: Option<MarketStatsSourceSnapshot> = None;
    for row in &snapshot.rows {
        if !row.market.active {
            continue;
        }
        let id = row_id(row);
        for (name, field) in &row.fields {
            if !structural(field) && field.state == MarketStatsFieldState::Available {
                let receipt = id
                    .and_then(|id| snapshot.field_received_at.get(id))
                    .and_then(|fields| fields.get(name))
                    .copied()
                    .or(snapshot.received_at);
                let reason = if Venue::from_public_id(&row.market.exchange)
                    .is_some_and(statistics_profile::expire_funding_at_payment)
                    && *name == MarketStatsFieldName::Funding
                    && funding_payment_passed(field, receipt, now)
                {
                    Some("funding-payment-passed")
                } else if receipt.is_some_and(|at| now.saturating_duration_since(at) >= STALE_AFTER)
                {
                    Some("stale-threshold")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    if expired.is_none() {
                        expired = Some(snapshot.clone());
                    }
                    let expired = expired.as_mut().unwrap();
                    if let Some(expired_row) = expired
                        .rows
                        .iter_mut()
                        .find(|candidate| row_id(candidate) == id)
                    {
                        if let Some(expired_field) = expired_row.fields.get_mut(name) {
                            fail_field(expired_field, reason);
                        }
                    }
                }
            }
        }
    }
    expired
}

fn funding_payment_passed(
    field: &MarketStatsField,
    receipt: Option<Instant>,
    now: Instant,
) -> bool {
    let Some(crate::models::MarketStatsValue::Funding(value)) = &field.value else {
        return false;
    };
    if value.kind != crate::models::FundingKind::Estimate {
        return false;
    }
    let (Some(next_payment), Some(observed_at), Some(receipt)) = (
        value.next_payment_timestamp,
        field.exchange_timestamp.or(field.received_timestamp),
        receipt,
    ) else {
        return false;
    };
    // Follow the source clock using monotonic elapsed time, not the server's wall clock.
    now.saturating_duration_since(receipt).as_millis()
        >= u128::from(next_payment.saturating_sub(observed_at))
}

fn row_id(row: &MarketStatsRow) -> Option<&str> {
    row.market
        .identity
        .as_ref()
        .map(|identity| identity.market_id.as_str())
}

#[cfg(test)]
fn implemented(name: MarketStatsFieldName) -> bool {
    matches!(
        name,
        MarketStatsFieldName::Funding
            | MarketStatsFieldName::MarkPrice
            | MarketStatsFieldName::IndexPrice
    )
}

fn structural(field: &MarketStatsField) -> bool {
    matches!(
        field.state,
        MarketStatsFieldState::Unsupported | MarketStatsFieldState::NotApplicable
    ) || field.reason.as_deref() == Some("inactive-market")
}

fn usable(field: &MarketStatsField) -> bool {
    matches!(
        field.state,
        MarketStatsFieldState::Available | MarketStatsFieldState::Stale
    ) && field.value.is_some()
}

fn fail_row(row: &mut MarketStatsRow, reason: &str) {
    if row.market.active {
        for field in row.fields.values_mut() {
            fail_field(field, reason);
        }
    }
}

pub(super) fn fail_field(field: &mut MarketStatsField, reason: &str) {
    if structural(field) || field.reason.as_deref() == Some("invalid-upstream-value") {
        return;
    }
    field.state = if usable(field) {
        MarketStatsFieldState::Stale
    } else {
        MarketStatsFieldState::Unavailable
    };
    field.reason = Some(reason.to_string());
}

#[cfg(test)]
mod tests;
