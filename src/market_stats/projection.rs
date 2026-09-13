use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::de::IgnoredAny;
use serde_json::{json, Value};
use tokio::time::Instant;

use super::{make_market_id, MarketStatsSourceSnapshot};
use crate::{
    errors::ApiError,
    exchanges::traits::ExchangeError,
    models::{
        FetchMarketStatsRequest, MarketStatsCoverage, MarketStatsField, MarketStatsFieldName,
        MarketStatsFieldState, MarketStatsRow, MarketStatsScope, MarketStatsSnapshot,
        MarketStatsSourceFailure, MarketStatsTopic, UnifiedMarketType,
    },
};

const PRIMARY_SOURCE: &str = "hyperliquid:primary:metaAndAssetCtxs";
const SPOT_SOURCE: &str = "hyperliquid:spotMeta";
const STALE_AFTER: Duration = Duration::from_secs(90);
fn is_binance(exchange: &str) -> bool {
    exchange == "binance"
}

fn is_lighter(exchange: &str) -> bool {
    exchange == "lighterxyz"
}

fn catalog_source(exchange: &str, product: UnifiedMarketType) -> &'static str {
    if is_binance(exchange) {
        "binance:exchangeInfo"
    } else if is_lighter(exchange) {
        "lighterxyz:orderBookDetails"
    } else {
        match product {
            UnifiedMarketType::Perp => PRIMARY_SOURCE,
            UnifiedMarketType::Spot => SPOT_SOURCE,
            _ => PRIMARY_SOURCE,
        }
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(30);

pub fn normalize_topic(request: FetchMarketStatsRequest) -> Result<MarketStatsTopic, ApiError> {
    let exchange = request.exchange.trim().to_ascii_lowercase();
    match (&exchange[..], &request.params) {
        (_, Value::Null) => {}
        ("binance", Value::Object(params)) if params.is_empty() => {}
        ("lighterxyz", Value::Object(params)) if params.is_empty() => {}
        (_, Value::Object(params))
            if !is_binance(&exchange)
                && !is_lighter(&exchange)
                && (params.is_empty()
                    || (params.len() == 1
                        && params.get("dex").and_then(Value::as_str) == Some(""))) => {}
        _ => {
            return Err(ApiError::Validation(
                if is_binance(&exchange) {
                    "Binance market statistics params must be null or {}"
                } else if is_lighter(&exchange) {
                    "Lighter market statistics params must be null or {}"
                } else {
                    "market statistics params must be null, {}, or {\"dex\":\"\"}"
                }
                .to_string(),
            ));
        }
    }

    let market_ids = match request.market_ids {
        Some(mut ids) => {
            if ids.is_empty() || ids.len() > 100 {
                return Err(ApiError::Validation(
                    "marketIds must contain between 1 and 100 IDs".to_string(),
                ));
            }
            for id in &ids {
                market_id_type(id, &exchange)?;
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
    Ok(MarketStatsTopic {
        exchange: exchange.clone(),
        params: if is_binance(&exchange) || is_lighter(&exchange) {
            json!({})
        } else {
            json!({"dex": ""})
        },
        market_ids,
        fields,
    })
}

fn market_id_type(id: &str, exchange: &str) -> Result<UnifiedMarketType, ApiError> {
    let invalid = || ApiError::Validation(format!("invalid marketId: {id}"));
    let (id_exchange, product, category, dex, native_id): (
        String,
        UnifiedMarketType,
        Option<String>,
        Option<String>,
        String,
    ) = serde_json::from_str(id).map_err(|_| invalid())?;
    if id_exchange != exchange
        || category.is_some()
        || native_id.is_empty()
        || (is_lighter(exchange)
            && ((native_id.len() > 1 && native_id.starts_with('0'))
                || !native_id.bytes().all(|byte| byte.is_ascii_digit())
                || native_id.parse::<u64>().is_err()))
        || !(if is_binance(exchange) {
            matches!((product, dex.as_deref()), (UnifiedMarketType::Perp, None))
        } else if is_lighter(exchange) {
            matches!(
                (product, dex.as_deref()),
                (UnifiedMarketType::Perp, None) | (UnifiedMarketType::Spot, None)
            )
        } else {
            matches!(
                (product, dex.as_deref()),
                (UnifiedMarketType::Perp, Some("")) | (UnifiedMarketType::Spot, None)
            )
        })
    {
        return Err(invalid());
    }
    let canonical = make_market_id(&id_exchange, product, None, dex.as_deref(), &native_id)?;
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
        let product = market_id_type(id, &topic.exchange)?;
        if known.contains(id.as_str()) {
            continue;
        }
        let (complete, source) = match product {
            UnifiedMarketType::Perp => (
                snapshot.perp_enumeration_complete,
                catalog_source(&topic.exchange, product),
            ),
            UnifiedMarketType::Spot => (
                snapshot.spot_enumeration_complete,
                catalog_source(&topic.exchange, product),
            ),
            _ => unreachable!("market_id_type only accepts supported products"),
        };
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
    let mut markets: Vec<_> = snapshot
        .rows
        .iter()
        .filter(|row| {
            let Some(id) = row_id(row) else { return false };
            match &topic.market_ids {
                Some(ids) => ids
                    .binary_search_by(|selected| selected.as_str().cmp(id))
                    .is_ok(),
                None => row.market.market_type == UnifiedMarketType::Perp && row.market.active,
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
                    Ok((_, UnifiedMarketType::Perp, _, _, _)) => snapshot.perp_enumeration_complete,
                    Ok((_, UnifiedMarketType::Spot, _, _, _)) => snapshot.spot_enumeration_complete,
                    _ => false,
                }
            });
            (Some(ids.len()), complete)
        }
        None => (
            snapshot.perp_catalog_known.then(|| {
                snapshot
                    .rows
                    .iter()
                    .filter(|row| {
                        row.market.market_type == UnifiedMarketType::Perp && row.market.active
                    })
                    .count()
            }),
            snapshot.perp_enumeration_complete,
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
) -> MarketStatsSourceSnapshot {
    let mut next = match outcome {
        Ok(next) => next,
        Err(error) => {
            let reason = match error {
                ExchangeError::UpstreamData(_) => "invalid-upstream-data",
                _ => "upstream-failure",
            };
            let mut next = previous.clone();
            next.perp_enumeration_complete = false;
            next.spot_enumeration_complete = false;
            next.contexts_valid = false;
            next.next_poll_at = Instant::now() + POLL_INTERVAL;
            // The failed source call publishes no secondary catalog proof. Retain known
            // spot rows, but do not declare a missing selection unknown from old metadata.
            let source = catalog_source(exchange, UnifiedMarketType::Perp);
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
        if row.market.market_type != UnifiedMarketType::Perp || !row.market.active {
            continue;
        }
        let prior = row_id(row).and_then(|id| prior_rows.get(id).copied());
        for (name, field) in &mut row.fields {
            if !field_implemented_for_exchange(exchange, *name)
                || field.reason.as_deref() == Some("invalid-upstream-value")
            {
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
            if next.contexts_valid && !field_failure {
                continue;
            }
            let incoming_reason = if field_failure {
                field.reason.clone().unwrap()
            } else {
                let source = if is_lighter(exchange) {
                    "lighterxyz:market_stats"
                } else {
                    field.source.as_deref().unwrap_or(if is_binance(exchange) {
                        "binance:premiumIndex"
                    } else {
                        PRIMARY_SOURCE
                    })
                };
                next.source_failures
                    .iter()
                    .find(|failure| failure.source == source)
                    .map(|failure| failure.reason.clone())
                    .unwrap_or_else(|| "context-mismatch".to_string())
            };
            if let Some(old) = prior.and_then(|prior| prior.fields.get(name)) {
                if usable(old) || old.reason.as_deref() == Some("invalid-upstream-value") {
                    *field = old.clone();
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
            let complete = match row.market.market_type {
                UnifiedMarketType::Perp => next.perp_enumeration_complete,
                UnifiedMarketType::Spot => next.spot_enumeration_complete,
                _ => true,
            };
            !complete && row_id(row).is_some_and(|id| !incoming_ids.contains(id))
        })
        .cloned()
        .collect();
    for mut row in retained {
        if row.market.market_type == UnifiedMarketType::Perp {
            let reason = next
                .source_failures
                .iter()
                .find(|failure| failure.source == catalog_source(exchange, UnifiedMarketType::Perp))
                .map(|failure| failure.reason.as_str())
                .unwrap_or("upstream-failure");
            fail_row(&mut row, reason);
        }
        next.rows.push(row);
    }
    if !next.perp_enumeration_complete {
        next.perp_catalog_known |= previous.perp_catalog_known;
    }
    next.rows
        .sort_unstable_by(|left, right| row_id(left).cmp(&row_id(right)));
    next
}

pub fn expire_snapshot(
    snapshot: &MarketStatsSourceSnapshot,
    now: Instant,
) -> Option<MarketStatsSourceSnapshot> {
    let received_at = snapshot.received_at?;
    if now.saturating_duration_since(received_at) < STALE_AFTER {
        return None;
    }
    let has_available = snapshot.rows.iter().any(|row| {
        row.market.market_type == UnifiedMarketType::Perp
            && row.market.active
            && row.fields.iter().any(|(name, field)| {
                field_implemented_for_row(row, *name)
                    && field.state == MarketStatsFieldState::Available
            })
    });
    if !has_available {
        return None;
    }
    let mut expired = snapshot.clone();
    for row in &mut expired.rows {
        if row.market.market_type != UnifiedMarketType::Perp || !row.market.active {
            continue;
        }
        for (name, field) in &mut row.fields {
            if field_implemented_for_exchange(&row.market.exchange, *name)
                && field.state == MarketStatsFieldState::Available
            {
                fail_field(field, "stale-threshold");
            }
        }
    }
    Some(expired)
}

fn field_implemented_for_exchange(exchange: &str, name: MarketStatsFieldName) -> bool {
    (is_lighter(exchange)
        && matches!(
            name,
            MarketStatsFieldName::Funding
                | MarketStatsFieldName::LastSettledFunding
                | MarketStatsFieldName::LastPrice
                | MarketStatsFieldName::MarkPrice
                | MarketStatsFieldName::IndexPrice
        ))
        || implemented(name)
}

fn row_id(row: &MarketStatsRow) -> Option<&str> {
    row.market
        .identity
        .as_ref()
        .map(|identity| identity.market_id.as_str())
}

fn field_implemented_for_row(row: &MarketStatsRow, name: MarketStatsFieldName) -> bool {
    field_implemented_for_exchange(&row.market.exchange, name)
}

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
    if row.market.market_type == UnifiedMarketType::Perp && row.market.active {
        for (name, field) in &mut row.fields {
            if field_implemented_for_exchange(&row.market.exchange, *name) {
                fail_field(field, reason);
            }
        }
    }
}

fn fail_field(field: &mut MarketStatsField, reason: &str) {
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
                        MarketStatsValue::Funding(FundingValue {
                            rate: rate.to_string(),
                            kind: FundingKind::CurrentUnclassified,
                            rate_interval_ms: None,
                            payment_interval_ms: Some(3_600_000),
                            payment_timestamp: None,
                            next_payment_timestamp: None,
                        })
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
                    } else if name == MarketStatsFieldName::Funding {
                        Some("rate-basis-unverified".to_string())
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
            perp_catalog_known: true,
            perp_enumeration_complete: true,
            spot_enumeration_complete: true,
            contexts_valid: true,
            received_at: Some(received_at),
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
                for (name, field) in &mut row.fields {
                    if field_implemented_for_exchange(&row.market.exchange, *name) {
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
        cold.perp_catalog_known = false;
        cold.perp_enumeration_complete = false;
        cold.spot_enumeration_complete = false;
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
        );
        assert!(matches!(
            validate_selection(&topic(Some(vec![btc.clone()])), &failed),
            Err(ApiError::Exchange(ExchangeError::UpstreamData(_)))
        ));

        let mut complete_perps = source(Vec::new(), now);
        complete_perps.spot_enumeration_complete = false;
        assert!(
            matches!(validate_selection(&topic(Some(vec![btc.clone()])), &complete_perps), Err(ApiError::Validation(message)) if message == format!("unknown marketId: {btc}"))
        );
        assert!(matches!(
            validate_selection(&topic(Some(vec![spot.clone()])), &complete_perps),
            Err(ApiError::Exchange(ExchangeError::UpstreamRequest(_)))
        ));
        complete_perps
            .rows
            .push(row("0", UnifiedMarketType::Spot, true, 1, None));
        validate_selection(&topic(Some(vec![spot])), &complete_perps).unwrap();
        complete_perps.spot_enumeration_complete = true;
        let failed = merge_outcome(
            &complete_perps,
            Err(ExchangeError::UpstreamRequest("offline".into())),
            "hyperliquid",
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
        let recovered = merge_outcome(&failed, Ok(fresh.clone()), "hyperliquid");
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
        let cleared = merge_outcome(&baseline, Ok(invalid), "hyperliquid");
        let mismatched = merge_outcome(
            &cleared,
            Ok(mismatch(source(
                vec![row("BTC", UnifiedMarketType::Perp, true, 30, Some("1"))],
                now + POLL_INTERVAL * 2,
            ))),
            "hyperliquid",
        );
        let failed = merge_outcome(
            &mismatched,
            Err(ExchangeError::UpstreamRequest("offline".to_string())),
            "hyperliquid",
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
        partial.spot_enumeration_complete = false;
        partial.source_failures.push(MarketStatsSourceFailure {
            source: SPOT_SOURCE.to_string(),
            reason: "upstream-failure".to_string(),
            message: "offline".to_string(),
        });
        let retained = merge_outcome(&baseline, Ok(partial), "hyperliquid");
        validate_selection(&selected, &retained).unwrap();
        let view = project_snapshot(&selected, &retained);
        assert_eq!(view.markets, project_snapshot(&selected, &baseline).markets);
        assert!(!view.coverage.enumeration_complete);
        let failed = merge_outcome(
            &retained,
            Err(ExchangeError::UpstreamData("broken primary".to_string())),
            "hyperliquid",
        );
        assert_eq!(project_snapshot(&selected, &failed).markets, view.markets);
        let removed = merge_outcome(
            &failed,
            Ok(source(Vec::new(), now + STALE_AFTER)),
            "hyperliquid",
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
}
