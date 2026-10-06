use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use tokio::time::Instant;

use crate::{
    exchanges::ccxt::{statistics_profile, Venue},
    market_stats::{apply_live, collect_live, fail_live, LiveFields, MarketStatsSourceSnapshot},
    models::{
        MarketStatsField, MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow,
        MarketStatsSourceFailure, MarketStatsValue, PriceValue, UnifiedMarket, UnifiedMarketInfo,
        UnifiedMarketType,
    },
    realtime::{StatisticsRowUpdate, StatisticsUpdate},
};

fn field(source: &str) -> MarketStatsField {
    MarketStatsField {
        state: MarketStatsFieldState::Available,
        value: Some(MarketStatsValue::Price(PriceValue {
            amount: "1".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDC".into(),
        })),
        reason: None,
        source: Some(source.into()),
        received_timestamp: Some(10),
        exchange_timestamp: None,
    }
}

#[test]
fn live_failure_is_not_the_polling_exclusion_list_or_all_emitted_fields() {
    let policy = statistics_profile::live_statistics(Venue::Lighter).unwrap();
    let rest = "lighterxyz:ccxt:fetchTickers";
    let mut fields = BTreeMap::from([
        (MarketStatsFieldName::Funding, field(rest)),
        (MarketStatsFieldName::LastPrice, field(rest)),
        (MarketStatsFieldName::Volume24h, field(policy.source)),
        (MarketStatsFieldName::OpenInterest, field(policy.source)),
    ]);
    fields
        .get_mut(&MarketStatsFieldName::OpenInterest)
        .unwrap()
        .reason = Some("invalid-upstream-value".into());
    let row = MarketStatsRow {
        market: UnifiedMarket {
            exchange: "lighterxyz".into(),
            symbol: "BTC/USDC".into(),
            base: "BTC".into(),
            quote: "USDC".into(),
            market_type: UnifiedMarketType::Perp,
            active: true,
            min_order_size: None,
            tick_size: None,
            contract_size: None,
            info: UnifiedMarketInfo::default(),
            identity: None,
        },
        fields,
    };
    let mut inactive = row.clone();
    inactive.market.active = false;
    let mut spot = row.clone();
    spot.market.market_type = UnifiedMarketType::Spot;
    let mut snapshot = MarketStatsSourceSnapshot {
        rows: vec![row, inactive.clone(), spot.clone()],
        catalog_known: true,
        contexts_valid: true,
        complete_catalogs: Default::default(),
        received_at: Some(Instant::now()),
        field_received_at: HashMap::new(),
        next_poll_at: Instant::now() + Duration::from_secs(30),
        source_failures: vec![MarketStatsSourceFailure {
            source: rest.into(),
            reason: "other".into(),
            message: "other".into(),
        }],
    };
    fail_live(&mut snapshot, policy, "first");
    fail_live(&mut snapshot, policy, "second");
    let fields = &snapshot.rows[0].fields;
    assert_eq!(
        fields[&MarketStatsFieldName::Funding].state,
        MarketStatsFieldState::Stale
    );
    assert_eq!(
        fields[&MarketStatsFieldName::LastPrice].state,
        MarketStatsFieldState::Available
    );
    assert_eq!(
        fields[&MarketStatsFieldName::Volume24h].state,
        MarketStatsFieldState::Stale
    );
    assert_eq!(
        fields[&MarketStatsFieldName::OpenInterest]
            .reason
            .as_deref(),
        Some("invalid-upstream-value")
    );
    assert_eq!(snapshot.rows[1], inactive);
    assert_eq!(snapshot.rows[2], spot);
    assert_eq!(snapshot.source_failures.len(), 2);
    assert_eq!(snapshot.source_failures[1].source, policy.source);
    assert_eq!(snapshot.source_failures[1].message, "second");
    assert!(snapshot.field_received_at.is_empty());

    let identity = crate::models::MarketIdentity {
        market_id: "id".into(),
        exchange_market_id: "0".into(),
        category: None,
        dex: None,
        contract_type: None,
        settle: None,
        settlement_asset_id: None,
    };
    snapshot.rows[0].market.identity = Some(identity);
    let at = Instant::now();
    let mut pending = LiveFields::new();
    let update = |at| StatisticsUpdate {
        rows: vec![StatisticsRowUpdate {
            market_id: "id".into(),
            fields: BTreeMap::from([(MarketStatsFieldName::LastPrice, field(policy.source))]),
            received_at: at,
        }],
    };
    collect_live(&mut pending, &update(at));
    apply_live(&mut snapshot, &mut pending);
    fail_live(&mut snapshot, policy, "third");
    assert_eq!(
        snapshot.rows[0].fields[&MarketStatsFieldName::LastPrice].state,
        MarketStatsFieldState::Stale
    );
    let failed = snapshot.clone();
    collect_live(&mut pending, &update(at));
    apply_live(&mut snapshot, &mut pending);
    assert_eq!(
        snapshot, failed,
        "same receipt must not resurrect a failed field"
    );
    collect_live(&mut pending, &update(at + Duration::from_millis(1)));
    apply_live(&mut snapshot, &mut pending);
    assert_eq!(
        snapshot.rows[0].fields[&MarketStatsFieldName::LastPrice].state,
        MarketStatsFieldState::Available
    );
    assert_eq!(
        snapshot.rows[0].fields[&MarketStatsFieldName::Funding],
        failed.rows[0].fields[&MarketStatsFieldName::Funding]
    );
}
