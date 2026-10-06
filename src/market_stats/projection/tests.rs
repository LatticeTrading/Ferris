mod policy;

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
        serde_json::from_value::<FetchMarketStatsRequest>(json!({"fields": ["unknown"]})).is_err()
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
    let inactive = project_snapshot(&topic(Some(vec![id("B", UnifiedMarketType::Perp)])), &next);
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
