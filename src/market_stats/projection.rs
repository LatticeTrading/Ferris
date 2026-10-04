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

fn is_lighter(exchange: &str) -> bool {
    exchange == "lighterxyz"
}

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
    if venue == Venue::Binance
        && fields.contains(&MarketStatsFieldName::OpenInterest)
        && market_ids.is_none()
    {
        return Err(ApiError::Validation(
            "Binance openInterest requires selected marketIds".into(),
        ));
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
    let category_matches = if venue == Venue::Bybit {
        category.as_deref() == params.get("category").and_then(Value::as_str)
    } else {
        category.is_none()
    };
    let dex_matches = if venue == Venue::Hyperliquid && product == UnifiedMarketType::Perp {
        dex.as_deref() == Some("")
    } else {
        dex.is_none()
    };
    if id_exchange != exchange
        || !category_matches
        || !dex_matches
        || native_id.is_empty()
        || !statistics_profile::catalog_products(venue, scope).contains(&product)
        || (is_lighter(exchange)
            && ((native_id.len() > 1 && native_id.starts_with('0'))
                || !native_id.bytes().all(|byte| byte.is_ascii_digit())
                || native_id.parse::<u64>().is_err()))
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
    let mut markets: Vec<_> = snapshot
        .rows
        .iter()
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
                let reason = if matches!(row.market.exchange.as_str(), "aster" | "bybit")
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
mod tests {
    use serde_json::json;
    use std::collections::BTreeMap;

    use super::*;
    use crate::models::{
        FundingKind, FundingValue, MarketIdentity, MarketStatsValue, PriceValue, UnifiedMarket,
        UnifiedMarketInfo,
    };

    fn id(native: &str, product: UnifiedMarketType) -> String {
        make_market_id(
            "hyperliquid",
            product,
            None,
            (product == UnifiedMarketType::Perp).then_some(""),
            native,
        )
        .unwrap()
    }

    fn request(value: Value) -> FetchMarketStatsRequest {
        serde_json::from_value(value).unwrap()
    }

    fn topic(ids: Option<Vec<String>>) -> MarketStatsTopic {
        normalize_topic(request(json!({"marketIds": ids}))).unwrap()
    }

    fn field(state: MarketStatsFieldState, reason: &str) -> MarketStatsField {
        MarketStatsField {
            state,
            value: None,
            reason: Some(reason.to_string()),
            exchange_timestamp: None,
            received_timestamp: None,
            source: None,
        }
    }

    fn row(
        native: &str,
        product: UnifiedMarketType,
        active: bool,
        receipt: u64,
        rate: Option<&str>,
    ) -> MarketStatsRow {
        let fields = [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::IndexPrice,
            MarketStatsFieldName::LastSettledFunding,
            MarketStatsFieldName::LastPrice,
            MarketStatsFieldName::Volume24h,
            MarketStatsFieldName::OpenInterest,
        ]
        .into_iter()
        .map(|name| {
            let value = if product == UnifiedMarketType::Spot
                && matches!(
                    name,
                    MarketStatsFieldName::Funding | MarketStatsFieldName::LastSettledFunding
                ) {
                field(MarketStatsFieldState::NotApplicable, "non-perpetual-market")
            } else if product == UnifiedMarketType::Spot || !implemented(name) {
                field(
                    MarketStatsFieldState::Unsupported,
                    "adapter-not-implemented",
                )
            } else if !active {
                field(MarketStatsFieldState::Unavailable, "inactive-market")
            } else {
                let value = if name == MarketStatsFieldName::Funding {
                    rate.map(|rate| {
                        MarketStatsValue::Funding(FundingValue::new(
                            rate.to_string(),
                            crate::models::FundingRateUnit::DecimalFraction,
                            FundingKind::CurrentUnclassified,
                            Some(3_600_000),
                            Some(3_600_000),
                            None,
                            None,
                        ))
                    })
                } else {
                    Some(MarketStatsValue::Price(PriceValue {
                        amount: "100".to_string(),
                        base_asset: native.to_string(),
                        quote_asset: "USDT".to_string(),
                    }))
                };
                MarketStatsField {
                    state: if value.is_some() {
                        MarketStatsFieldState::Available
                    } else {
                        MarketStatsFieldState::Unavailable
                    },
                    reason: if value.is_none() {
                        Some("invalid-upstream-value".to_string())
                    } else {
                        None
                    },
                    value,
                    exchange_timestamp: None,
                    received_timestamp: Some(receipt),
                    source: Some(PRIMARY_SOURCE.to_string()),
                }
            };
            (name, value)
        })
        .collect::<BTreeMap<_, _>>();
        MarketStatsRow {
            market: UnifiedMarket {
                exchange: "hyperliquid".to_string(),
                symbol: "SAME/USDC".to_string(),
                base: "SAME".to_string(),
                quote: "USDC".to_string(),
                market_type: product,
                active,
                min_order_size: None,
                tick_size: None,
                contract_size: None,
                info: UnifiedMarketInfo::default(),
                identity: Some(MarketIdentity {
                    market_id: id(native, product),
                    exchange_market_id: native.to_string(),
                    category: None,
                    dex: (product == UnifiedMarketType::Perp).then(String::new),
                    contract_type: None,
                    settle: None,
                    settlement_asset_id: None,
                }),
            },
            fields,
        }
    }

    fn source(rows: Vec<MarketStatsRow>, received_at: Instant) -> MarketStatsSourceSnapshot {
        MarketStatsSourceSnapshot {
            rows,
            catalog_known: true,
            complete_catalogs: [UnifiedMarketType::Perp, UnifiedMarketType::Spot]
                .into_iter()
                .collect(),
            contexts_valid: true,
            received_at: Some(received_at),
            field_received_at: Default::default(),
            next_poll_at: received_at + POLL_INTERVAL,
            source_failures: Vec::new(),
        }
    }

    fn mismatch(mut snapshot: MarketStatsSourceSnapshot) -> MarketStatsSourceSnapshot {
        snapshot.contexts_valid = false;
        snapshot.source_failures.push(MarketStatsSourceFailure {
            source: PRIMARY_SOURCE.to_string(),
            reason: "context-mismatch".to_string(),
            message: "unaligned contexts".to_string(),
        });
        for row in &mut snapshot.rows {
            if row.market.market_type == UnifiedMarketType::Perp && row.market.active {
                for field in row.fields.values_mut() {
                    if !structural(field) {
                        field.state = MarketStatsFieldState::Unavailable;
                        field.value = None;
                        field.reason = Some("context-mismatch".to_string());
                    }
                }
            }
        }
        snapshot
    }

    #[test]
    fn market_stats_topics_canonicalize_without_rewriting_identity() {
        let upper = id("A\"\\/雪", UnifiedMarketType::Perp);
        let lower = id("a\"\\/雪", UnifiedMarketType::Perp);
        let first = normalize_topic(request(json!({
            "exchange": " HyperLiquid ", "params": {},
            "marketIds": [upper, lower, upper],
            "fields": ["markPrice", "funding", "funding"],
        })))
        .unwrap();
        let second = normalize_topic(request(json!({
            "params": {"dex": ""}, "marketIds": [lower, upper],
            "fields": ["funding", "markPrice"],
        })))
        .unwrap();
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&second).unwrap()
        );
        assert_eq!(first.market_ids, Some(vec![upper, lower]));
        assert_eq!(
            topic(None),
            normalize_topic(request(
                json!({"marketIds": null, "fields": null, "params": null})
            ))
            .unwrap()
        );
    }

    #[test]
    fn market_stats_rejects_noncanonical_or_out_of_scope_ids_before_catalog() {
        let invalid = [
            "BTC",
            r#"["hyperliquid","perp",null,"","BTC",0]"#,
            r#"["hyperliquid","perp",null,"","\u0042TC"]"#,
            r#"["hyperliquid", "perp",null,"","BTC"]"#,
            r#"["Hyperliquid","perp",null,"","BTC"]"#,
            r#"["bybit","perp",null,"","BTC"]"#,
            r#"["hyperliquid","perp","", "","BTC"]"#,
            r#"["hyperliquid","perp",null,null,"BTC"]"#,
            r#"["hyperliquid","perp",null,"other","BTC"]"#,
            r#"["hyperliquid","spot",null,"","0"]"#,
            r#"["hyperliquid","future",null,"","BTC"]"#,
            r#"["hyperliquid","perp",null,"",""]"#,
        ];
        for invalid in invalid {
            assert!(
                matches!(
                    normalize_topic(request(json!({"marketIds": [invalid]}))),
                    Err(ApiError::Validation(_))
                ),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn market_stats_rejects_empty_lists_oversized_input_and_invalid_scope() {
        for params in [
            json!([]),
            json!(""),
            json!({"dex": null}),
            json!({"dex": "other"}),
            json!({"category": null}),
            json!({"dex": "", "extra": true}),
        ] {
            assert!(matches!(
                normalize_topic(request(json!({"params": params}))),
                Err(ApiError::Validation(_))
            ));
        }
        for body in [
            json!({"marketIds": []}),
            json!({"fields": []}),
            json!({"marketIds": vec![id("BTC", UnifiedMarketType::Perp); 101]}),
        ] {
            assert!(matches!(
                normalize_topic(request(body)),
                Err(ApiError::Validation(_))
            ));
        }
        assert!(
            serde_json::from_value::<FetchMarketStatsRequest>(json!({"fields": ["unknown"]}))
                .is_err()
        );
    }

    #[test]
    fn market_stats_selected_unknown_requires_current_product_catalog_proof() {
        let now = Instant::now();
        let btc = id("BTC", UnifiedMarketType::Perp);
        let spot = id("0", UnifiedMarketType::Spot);
        let mut cold = source(Vec::new(), now);
        cold.catalog_known = false;
        cold.complete_catalogs.clear();
        cold.received_at = None;
        let empty = project_snapshot(&topic(None), &cold);
        assert_eq!(empty.coverage.expected_markets, None);
        assert!(!empty.coverage.enumeration_complete);
        assert!(empty.markets.is_empty());
        assert!(matches!(
            validate_selection(&topic(Some(vec![btc.clone()])), &cold),
            Err(ApiError::Exchange(ExchangeError::UpstreamRequest(_)))
        ));
        let failed = merge_outcome(
            &cold,
            Err(ExchangeError::UpstreamData("bad metadata".to_string())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert!(matches!(
            validate_selection(&topic(Some(vec![btc.clone()])), &failed),
            Err(ApiError::Exchange(ExchangeError::UpstreamData(_)))
        ));

        let mut complete_perps = source(Vec::new(), now);
        complete_perps
            .complete_catalogs
            .remove(&UnifiedMarketType::Spot);
        assert!(matches!(
            validate_selection(&topic(Some(vec![btc.clone()])), &complete_perps),
            Err(ApiError::Validation(_))
        ));
        assert!(matches!(
            validate_selection(&topic(Some(vec![spot.clone()])), &complete_perps),
            Err(ApiError::Exchange(ExchangeError::UpstreamRequest(_)))
        ));
        complete_perps
            .rows
            .push(row("0", UnifiedMarketType::Spot, true, 1, None));
        validate_selection(&topic(Some(vec![spot])), &complete_perps).unwrap();
        complete_perps
            .complete_catalogs
            .insert(UnifiedMarketType::Spot);
        let failed = merge_outcome(
            &complete_perps,
            Err(ExchangeError::UpstreamRequest("offline".into())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        validate_selection(
            &topic(Some(vec![id("0", UnifiedMarketType::Spot)])),
            &failed,
        )
        .unwrap();
        assert!(matches!(
            validate_selection(
                &topic(Some(vec![id("999", UnifiedMarketType::Spot)])),
                &failed
            ),
            Err(ApiError::Exchange(ExchangeError::UpstreamRequest(_)))
        ));
    }

    #[test]
    fn market_stats_projects_requested_fields_active_perps_and_opaque_selection() {
        let now = Instant::now();
        let snapshot = source(
            vec![
                row("0", UnifiedMarketType::Spot, true, 1, None),
                row("OLD", UnifiedMarketType::Perp, false, 1, Some("1")),
                row("B", UnifiedMarketType::Perp, true, 1, Some("-0.1")),
                row("A", UnifiedMarketType::Perp, true, 1, Some("0")),
            ],
            now,
        );
        let all = project_snapshot(&topic(None), &snapshot);
        assert_eq!(
            all.markets.iter().filter_map(row_id).collect::<Vec<_>>(),
            vec![
                id("A", UnifiedMarketType::Perp),
                id("B", UnifiedMarketType::Perp)
            ]
        );
        assert_eq!(all.coverage.expected_markets, Some(2));
        assert_eq!(all.coverage.returned_markets, 2);
        assert!(all
            .markets
            .iter()
            .all(|row| row.fields.keys().copied().collect::<Vec<_>>()
                == vec![MarketStatsFieldName::Funding]));
        let selected = topic(Some(vec![
            id("OLD", UnifiedMarketType::Perp),
            id("0", UnifiedMarketType::Spot),
        ]));
        let view = project_snapshot(&selected, &snapshot);
        assert_eq!(
            view.markets[0].fields[&MarketStatsFieldName::Funding]
                .reason
                .as_deref(),
            Some("inactive-market")
        );
        assert_eq!(
            view.markets[1].fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::NotApplicable
        );
    }

    #[test]
    fn market_stats_failure_retains_zero_and_recovery_replaces_observation_objects() {
        let now = Instant::now();
        let baseline = source(
            vec![row("BTC", UnifiedMarketType::Perp, true, 10, Some("0"))],
            now,
        );
        let failed = merge_outcome(
            &baseline,
            Err(ExchangeError::UpstreamRequest("offline".to_string())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        let field = &failed.rows[0].fields[&MarketStatsFieldName::Funding];
        assert_eq!(field.state, MarketStatsFieldState::Stale);
        assert_eq!(
            field.value,
            baseline.rows[0].fields[&MarketStatsFieldName::Funding].value
        );
        assert_eq!(field.received_timestamp, Some(10));
        assert_eq!(field.reason.as_deref(), Some("upstream-failure"));
        assert!(
            !project_snapshot(&topic(None), &failed)
                .coverage
                .enumeration_complete
        );
        let fresh = source(
            vec![row("BTC", UnifiedMarketType::Perp, true, 20, Some("0"))],
            now + POLL_INTERVAL,
        );
        let recovered = merge_outcome(
            &failed,
            Ok(fresh.clone()),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert_eq!(recovered, fresh);
        assert_eq!(
            recovered.rows[0].fields[&MarketStatsFieldName::Funding].received_timestamp,
            Some(20)
        );
    }

    #[test]
    fn market_stats_invalid_scalar_cannot_resurrect_after_mismatch_or_transport_failure() {
        let now = Instant::now();
        let baseline = source(
            vec![row("BTC", UnifiedMarketType::Perp, true, 10, Some("-0.01"))],
            now,
        );
        let invalid = source(
            vec![row("BTC", UnifiedMarketType::Perp, true, 20, None)],
            now + POLL_INTERVAL,
        );
        let cleared = merge_outcome(&baseline, Ok(invalid), "hyperliquid", &json!({"dex": ""}));
        let mismatched = merge_outcome(
            &cleared,
            Ok(mismatch(source(
                vec![row("BTC", UnifiedMarketType::Perp, true, 30, Some("1"))],
                now + POLL_INTERVAL * 2,
            ))),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        let failed = merge_outcome(
            &mismatched,
            Err(ExchangeError::UpstreamRequest("offline".to_string())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        let funding = &failed.rows[0].fields[&MarketStatsFieldName::Funding];
        assert_eq!(funding.state, MarketStatsFieldState::Unavailable);
        assert_eq!(funding.value, None);
        assert_eq!(funding.reason.as_deref(), Some("invalid-upstream-value"));
        let mark = &failed.rows[0].fields[&MarketStatsFieldName::MarkPrice];
        assert_eq!(mark.state, MarketStatsFieldState::Stale);
        assert_eq!(mark.received_timestamp, Some(20));
        let recovered = merge_outcome(
            &failed,
            Ok(source(
                vec![row("BTC", UnifiedMarketType::Perp, true, 40, Some("0"))],
                now + STALE_AFTER,
            )),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert_eq!(
            recovered.rows[0].fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
    }

    #[test]
    fn market_stats_context_mismatch_retains_by_id_but_applies_authoritative_membership() {
        let now = Instant::now();
        let baseline = source(
            vec![
                row("A", UnifiedMarketType::Perp, true, 10, Some("-1")),
                row("B", UnifiedMarketType::Perp, true, 10, Some("2")),
                row("GONE", UnifiedMarketType::Perp, true, 10, Some("3")),
            ],
            now,
        );
        let selected = topic(Some(vec![id("GONE", UnifiedMarketType::Perp)]));
        let next = merge_outcome(
            &baseline,
            Ok(mismatch(source(
                vec![
                    row("B", UnifiedMarketType::Perp, false, 20, Some("20")),
                    row("NEW", UnifiedMarketType::Perp, true, 20, Some("30")),
                    row("A", UnifiedMarketType::Perp, true, 20, Some("10")),
                ],
                now + POLL_INTERVAL,
            ))),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        let view = project_snapshot(&topic(None), &next);
        assert_eq!(view.coverage.expected_markets, Some(2));
        assert!(view.coverage.enumeration_complete);
        let funding = &view.markets[0].fields[&MarketStatsFieldName::Funding];
        assert_eq!(
            funding.value,
            baseline.rows[0].fields[&MarketStatsFieldName::Funding].value
        );
        assert_eq!(funding.state, MarketStatsFieldState::Stale);
        assert_eq!(funding.reason.as_deref(), Some("context-mismatch"));
        assert_eq!(funding.received_timestamp, Some(10));
        assert_eq!(
            view.markets[1].fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Unavailable
        );
        let inactive =
            project_snapshot(&topic(Some(vec![id("B", UnifiedMarketType::Perp)])), &next);
        assert_eq!(
            inactive.markets[0].fields[&MarketStatsFieldName::Funding]
                .reason
                .as_deref(),
            Some("inactive-market")
        );
        let existing = project_snapshot(&selected, &next);
        assert!(existing.markets.is_empty());
        assert_eq!(existing.coverage.expected_markets, Some(1));
        assert_eq!(existing.coverage.returned_markets, 0);
        assert!(matches!(
            validate_selection(&selected, &next),
            Err(ApiError::Validation(_))
        ));
    }

    #[test]
    fn market_stats_partial_spot_catalog_preserves_selection_until_authoritative_removal() {
        let now = Instant::now();
        let baseline = source(vec![row("0", UnifiedMarketType::Spot, true, 10, None)], now);
        let selected = topic(Some(vec![id("0", UnifiedMarketType::Spot)]));
        let mut partial = source(
            vec![row("1", UnifiedMarketType::Spot, true, 20, None)],
            now + POLL_INTERVAL,
        );
        partial.complete_catalogs.remove(&UnifiedMarketType::Spot);
        partial.source_failures.push(MarketStatsSourceFailure {
            source: SPOT_SOURCE.to_string(),
            reason: "upstream-failure".to_string(),
            message: "offline".to_string(),
        });
        let retained = merge_outcome(&baseline, Ok(partial), "hyperliquid", &json!({"dex": ""}));
        validate_selection(&selected, &retained).unwrap();
        let view = project_snapshot(&selected, &retained);
        assert_eq!(view.markets, project_snapshot(&selected, &baseline).markets);
        assert!(!view.coverage.enumeration_complete);
        let failed = merge_outcome(
            &retained,
            Err(ExchangeError::UpstreamData("broken primary".to_string())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert_eq!(project_snapshot(&selected, &failed).markets, view.markets);
        let removed = merge_outcome(
            &failed,
            Ok(source(Vec::new(), now + STALE_AFTER)),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert!(project_snapshot(&selected, &removed).markets.is_empty());
        assert!(matches!(
            validate_selection(&selected, &removed),
            Err(ApiError::Validation(_))
        ));
        assert!(removed.source_failures.is_empty());
    }

    #[test]
    fn market_stats_expiry_transitions_at_threshold_without_overwriting_failure_reasons() {
        let now = Instant::now();
        let baseline = source(
            vec![
                row("BTC", UnifiedMarketType::Perp, true, 10, Some("0")),
                row("OLD", UnifiedMarketType::Perp, false, 10, Some("1")),
                row("0", UnifiedMarketType::Spot, true, 10, None),
            ],
            now,
        );
        assert!(expire_snapshot(&baseline, now + STALE_AFTER - Duration::from_millis(1)).is_none());
        let expired = expire_snapshot(&baseline, now + STALE_AFTER).unwrap();
        let funding = &expired.rows[0].fields[&MarketStatsFieldName::Funding];
        assert_eq!(funding.state, MarketStatsFieldState::Stale);
        assert_eq!(funding.reason.as_deref(), Some("stale-threshold"));
        assert_eq!(funding.received_timestamp, Some(10));
        assert_eq!(expired.rows[1..], baseline.rows[1..]);
        assert_eq!(
            expired.rows[0].fields[&MarketStatsFieldName::OpenInterest],
            baseline.rows[0].fields[&MarketStatsFieldName::OpenInterest]
        );
        assert!(expire_snapshot(&expired, now + STALE_AFTER * 2).is_none());
        let failed = merge_outcome(
            &baseline,
            Err(ExchangeError::UpstreamData("bad metadata".to_string())),
            "hyperliquid",
            &json!({"dex": ""}),
        );
        assert!(expire_snapshot(&failed, now + STALE_AFTER * 2).is_none());
        assert_eq!(
            failed.rows[0].fields[&MarketStatsFieldName::Funding]
                .reason
                .as_deref(),
            Some("invalid-upstream-data")
        );
        assert_eq!(failed.source_failures[0].reason, "invalid-upstream-data");
    }

    #[test]
    fn bybit_estimate_expires_at_payment_without_expiring_prices_or_inventing_settlement() {
        let now = Instant::now();
        let mut market = row("BTCUSDT", UnifiedMarketType::Perp, true, 5_000, Some("0"));
        market.market.exchange = "bybit".to_string();
        let identity = market.market.identity.as_mut().unwrap();
        identity.category = Some("linear".to_string());
        identity.dex = None;
        identity.market_id = make_market_id(
            "bybit",
            UnifiedMarketType::Perp,
            Some("linear"),
            None,
            "BTCUSDT",
        )
        .unwrap();
        let funding = market
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap();
        funding.exchange_timestamp = Some(10_000);
        let Some(MarketStatsValue::Funding(value)) = &mut funding.value else {
            panic!("expected funding")
        };
        value.kind = FundingKind::Estimate;
        value.next_payment_timestamp = Some(11_000);
        let mut baseline = source(vec![market], now);
        assert!(expire_snapshot(&baseline, now + Duration::from_millis(999)).is_none());
        let expired = expire_snapshot(&baseline, now + Duration::from_secs(1)).unwrap();
        let funding = &expired.rows[0].fields[&MarketStatsFieldName::Funding];
        assert_eq!(funding.state, MarketStatsFieldState::Stale);
        assert_eq!(funding.reason.as_deref(), Some("funding-payment-passed"));
        assert_eq!(
            funding.value,
            baseline.rows[0].fields[&MarketStatsFieldName::Funding].value
        );
        assert_eq!(funding.received_timestamp, Some(5_000));
        assert_eq!(
            expired.rows[0].fields[&MarketStatsFieldName::MarkPrice].state,
            MarketStatsFieldState::Available
        );
        assert!(expire_snapshot(&expired, now + Duration::from_secs(2)).is_none());

        let Some(MarketStatsValue::Funding(value)) = &mut baseline.rows[0]
            .fields
            .get_mut(&MarketStatsFieldName::Funding)
            .unwrap()
            .value
        else {
            unreachable!()
        };
        value.next_payment_timestamp = Some(50_000);
        let recovered = merge_outcome(
            &expired,
            Ok(baseline),
            "bybit",
            &json!({"category": "linear"}),
        );
        assert_eq!(
            recovered.rows[0].fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
        assert!(expire_snapshot(&recovered, now + Duration::from_secs(2)).is_none());
    }
}
