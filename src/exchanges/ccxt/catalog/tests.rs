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

mod fixtures;
mod metadata;
use fixtures::*;

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
    let catalog = Catalog::from_markets(Venue::Binance, vec![market(value)], TICK_SIZE).unwrap();

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
        Catalog::from_markets(Venue::Bybit, vec![market(usdt), market(usdc)], TICK_SIZE).unwrap();
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
