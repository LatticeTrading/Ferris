//! Resolution invariants around metadata-dependent aliases and identities.
use ccxt::runtime::TICK_SIZE;
use serde_json::{json, Value as JsonValue};

use super::{fixtures::hyperliquid_perp, market, params};
use crate::{
    exchanges::{
        ccxt::{catalog::Catalog, venue::Venue},
        traits::ExchangeError,
    },
    models::UnifiedMarketType,
};

fn extended() -> JsonValue {
    let fixtures: JsonValue =
        serde_json::from_str(include_str!("../../convert/market_tests/markets.json")).unwrap();
    fixtures["extended"].clone()
}

#[test]
fn native_display_and_unified_assets_keep_distinct_resolution_roles() {
    let catalog =
        Catalog::from_markets(Venue::Extended, vec![market(extended())], TICK_SIZE).unwrap();
    for alias in ["BTC", "BTC-USD", "BTC/USD", "BTC/USD:USD", "BTC/USDC:USDC"] {
        let row = catalog.resolve(alias, &json!({})).unwrap();
        assert_eq!(row.market.base, "BTC");
        assert_eq!(row.market.quote, "USD");
        assert_eq!(row.market.symbol, "BTC/USD");
        assert_eq!(row.raw["quote"], "USDC");
        assert_eq!(row.raw["quoteId"], "USD");
        assert_eq!(
            row.market.identity.as_ref().unwrap().settle.as_deref(),
            Some("USD")
        );
        assert!(catalog.resolve(alias, &json!({"settle":"USD"})).is_ok());
        assert!(catalog.resolve(alias, &json!({"settle":"USDC"})).is_err());
        assert!(catalog.resolve(alias, &json!({"type":"spot"})).is_err());
    }
    assert!(catalog.resolve("BTC/USDC", &json!({})).is_err());
    let identity = &catalog.entries()[0]
        .market
        .identity
        .as_ref()
        .unwrap()
        .market_id;
    assert!(catalog.resolve(identity, &json!({"settle":"USD"})).is_ok());
    assert!(catalog
        .resolve(identity, &json!({"settle":"USDC"}))
        .is_err());
}

#[test]
fn metadata_alias_collisions_require_selectors_but_never_shadow_exact_identity() {
    let first = extended();
    let mut second = first.clone();
    second["id"] = json!("BTC-EUR");
    second["symbol"] = json!("BTC/USDC:EUR");
    second["info"]["collateralAssetName"] = json!("EUR");
    // A shared original-info alias can collide even across different displays.
    second["info"]["name"] = json!("BTC-USD");
    let catalog = Catalog::from_markets(
        Venue::Extended,
        vec![market(first), market(second)],
        TICK_SIZE,
    )
    .unwrap();
    for alias in ["BTC", "BTC-USD"] {
        assert!(
            matches!(catalog.resolve(alias, &json!({})), Err(ExchangeError::BadSymbol(message)) if message.starts_with("ambiguous"))
        );
        assert_eq!(
            catalog
                .resolve(alias, &json!({"settle":"EUR"}))
                .unwrap()
                .ccxt_symbol,
            "BTC/USDC:EUR"
        );
        assert_eq!(
            catalog
                .resolve(alias, &json!({"settle":"USD"}))
                .unwrap()
                .ccxt_symbol,
            "BTC/USDC:USDC"
        );
        assert!(catalog
            .resolve(alias, &json!({"type":"spot", "category":"linear"}))
            .is_err());
    }
    for row in catalog.entries() {
        let id = row.market.identity.as_ref().unwrap();
        for exact in [&row.ccxt_symbol, &id.market_id] {
            assert_eq!(
                catalog.resolve(exact, &json!({})).unwrap().ccxt_symbol,
                row.ccxt_symbol
            );
            assert!(catalog.resolve(exact, &json!({"type":"spot"})).is_err());
            assert!(catalog
                .resolve(exact, &json!({"settle":"missing"}))
                .is_err());
        }
    }

    // Even an original-info name equal to someone else's exact identity cannot
    // steal it; nor may a conflicting selector fall through to the alias index.
    for exact in [
        catalog.entries()[0].ccxt_symbol.clone(),
        catalog.entries()[0]
            .market
            .identity
            .as_ref()
            .unwrap()
            .market_id
            .clone(),
    ] {
        let mut other = extended();
        other["id"] = json!("ETH-USD");
        other["base"] = json!("ETH");
        other["symbol"] = json!("ETH/USDC:USDC");
        other["info"]["name"] = json!(exact);
        other["info"]["collateralAssetName"] = json!("EUR");
        let catalog = Catalog::from_markets(
            Venue::Extended,
            vec![market(extended()), market(other)],
            TICK_SIZE,
        )
        .unwrap();
        assert_eq!(
            catalog.resolve(&exact, &json!({})).unwrap().market.base,
            "BTC"
        );
        assert!(catalog.resolve(&exact, &json!({"settle":"EUR"})).is_err());
    }
}

#[test]
fn shared_prelisting_policy_still_controls_catalog_selection_outside_bybit() {
    let mut raw = extended();
    raw["info"]["isPreListing"] = json!(true);
    let catalog = Catalog::from_markets(Venue::Extended, vec![market(raw)], TICK_SIZE).unwrap();
    assert!(catalog
        .select(&params(json!({}), false))
        .unwrap()
        .is_empty());
    assert_eq!(catalog.select(&params(json!({}), true)).unwrap().len(), 1);
    // Resolution does not silently hide an inactive market.
    assert!(!catalog.resolve("BTC", &json!({})).unwrap().market.active);
}

#[test]
fn ccxt_symbol_and_final_metadata_identity_collisions_are_independently_rejected() {
    let mut second = extended();
    second["id"] = json!("different-native-id");
    let duplicate_symbol = Catalog::from_markets(
        Venue::Extended,
        vec![market(extended()), market(second)],
        TICK_SIZE,
    );
    assert!(
        matches!(duplicate_symbol, Err(ExchangeError::UpstreamData(message)) if message.starts_with("duplicate CCXT symbol"))
    );

    // Different stock ids/symbols collapse to the same metadata-derived identity.
    let first = hyperliquid_perp("BTC", "0", None);
    let mut second = first.clone();
    second["id"] = json!("1");
    second["symbol"] = json!("OTHER/USDC:USDC");
    second["base"] = json!("OTHER");
    let duplicate_identity = Catalog::from_markets(
        Venue::Hyperliquid,
        vec![market(first), market(second)],
        TICK_SIZE,
    );
    assert!(
        matches!(duplicate_identity, Err(ExchangeError::UpstreamData(message)) if message.starts_with("duplicate Ferris market id"))
    );
}

#[test]
fn metadata_identity_failure_cannot_be_repaired_by_aliases() {
    let mut raw = hyperliquid_perp("BTC", "0", None);
    raw["info"]["name"] = json!(null);
    raw["id2"] = json!("BTC");
    assert!(matches!(
        Catalog::from_markets(Venue::Hyperliquid, vec![market(raw)], TICK_SIZE),
        Err(ExchangeError::UpstreamData(_))
    ));

    let mut raw = hyperliquid_perp("native:BTC", "110000", Some("native"));
    raw["base"] = json!("UNIFIED-BTC");
    let catalog = Catalog::from_markets(Venue::Hyperliquid, vec![market(raw)], TICK_SIZE).unwrap();
    let row = catalog
        .resolve("native:BTC", &json!({"dex":"native"}))
        .unwrap();
    assert_eq!(row.market.base, "UNIFIED-BTC");
    assert_eq!(row.market.market_type, UnifiedMarketType::Perp);
    assert_eq!(
        row.market.identity.as_ref().unwrap().exchange_market_id,
        "native:BTC"
    );
    assert_eq!(
        catalog
            .resolve("UNIFIED-BTC", &json!({}))
            .unwrap()
            .ccxt_symbol,
        row.ccxt_symbol
    );
    assert!(catalog
        .resolve("native:BTC", &json!({"dex":"other"}))
        .is_err());
}
