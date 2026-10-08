//! Catalog conversion regressions. Fixtures are loaded stock market shapes, not
//! new exchange support. Nonfinite stock Values are injected before conversion.
use std::collections::BTreeSet;

use ccxt::{runtime::TICK_SIZE, types::Market};
use serde_json::{json, Value as JsonValue};

use super::convert_market;
use crate::exchanges::{ccxt::venue::Venue, traits::ExchangeError};

fn fixture(venue: Venue) -> JsonValue {
    let fixtures: JsonValue =
        serde_json::from_str(include_str!("market_tests/markets.json")).unwrap();
    fixtures[venue.public_id()].clone()
}

fn market(raw: JsonValue) -> Market {
    Market::from_value(ccxt::Value::from_json(&raw))
}

#[test]
fn existing_alias_sets_and_native_metadata_are_unchanged() {
    for (venue, names, native, settle, asset_id, category, dex, contract_type, raw_symbol) in [
        (
            Venue::Binance,
            vec!["BTC/USDT", "BTC/USDT:USDT", "BTCUSDT", "BTC"],
            "BTCUSDT",
            "USDT",
            Some("USDT"),
            None,
            None,
            Some("PERPETUAL"),
            "BTCUSDT",
        ),
        (
            Venue::Bybit,
            vec!["BTC/USDT", "BTC/USDT:USDT", "BTCUSDT", "BTC"],
            "BTCUSDT",
            "USDT",
            Some("USDT"),
            Some("linear"),
            None,
            Some("LinearPerpetual"),
            "BTCUSDT",
        ),
        (
            Venue::Aster,
            vec!["BTC/USDT", "BTC/USDT:USDT", "BTCUSDT", "BTC"],
            "BTCUSDT",
            "USDT",
            Some("USDT"),
            None,
            None,
            Some("PERPETUAL"),
            "BTCUSDT",
        ),
        (
            Venue::Hyperliquid,
            vec!["BTC/USDC", "BTC/USDC:USDC", "0", "BTC"],
            "BTC",
            "USDC",
            None,
            None,
            Some(""),
            None,
            "BTC",
        ),
        (
            Venue::Extended,
            vec!["BTC/USD", "BTC/USDC:USDC", "BTC-USD", "BTC", "BTC/USD:USD"],
            "BTC-USD",
            "USD",
            Some("0x123"),
            None,
            None,
            Some("PERPETUAL"),
            "BTC-USD",
        ),
        (
            Venue::Lighter,
            vec!["BTC/USDC", "BTC/USDC:USDC", "7", "BTC", "BTC/USD"],
            "7",
            "USDC",
            Some("3"),
            None,
            None,
            None,
            "BTC",
        ),
        (
            Venue::Bitfinex,
            vec!["BTC/USDT", "BTC/USDT:USDT", "tBTCF0:USTF0", "BTCF0:USTF0"],
            "tBTCF0:USTF0",
            "USDT",
            Some("USTF0"),
            None,
            None,
            Some("PERPETUAL"),
            "tBTCF0:USTF0",
        ),
        (
            Venue::Kucoin,
            vec!["BTC/USDT", "BTC/USDT:USDT", "XBTUSDTM", "BTC"],
            "XBTUSDTM",
            "USDT",
            Some("USDT"),
            None,
            None,
            Some("PERPETUAL"),
            "XBTUSDTM",
        ),
    ] {
        let mut raw = fixture(venue);
        // Even when present, id2 must NOT become a new public alias by default.
        raw["id2"] = json!("secondary-only");
        let source = market(raw.clone());
        let (entry, aliases) = convert_market(venue, &source, TICK_SIZE).unwrap();
        let identity = entry.market.identity.as_ref().unwrap();
        assert_eq!(identity.exchange_market_id, native, "{venue:?}");
        assert_eq!(identity.settle.as_deref(), Some(settle));
        assert_eq!(identity.settlement_asset_id.as_deref(), asset_id);
        assert_eq!(identity.category.as_deref(), category);
        assert_eq!(identity.dex.as_deref(), dex);
        assert_eq!(identity.contract_type.as_deref(), contract_type);
        assert_eq!(entry.market.info.raw_symbol.as_deref(), Some(raw_symbol));
        assert_eq!(
            entry.market.info.exchange_symbol.as_deref(),
            Some(source.id.as_str())
        );
        let mut expected: BTreeSet<String> = names.into_iter().map(str::to_string).collect();
        expected.insert(identity.market_id.clone());
        assert_eq!(
            aliases.into_iter().collect::<BTreeSet<_>>(),
            expected,
            "{venue:?}"
        );
        assert_eq!(entry.raw, raw);
        // The completed row owns both projections independently of stock data.
        drop(source);
        assert_eq!(entry.raw["id2"], "secondary-only");
    }
}

#[test]
fn native_settlement_metadata_does_not_rename_unified_display_assets() {
    for (venue, key) in [
        (Venue::Binance, "marginAsset"),
        (Venue::Bybit, "settleCoin"),
        (Venue::Aster, "marginAsset"),
    ] {
        for (native, expected) in [
            (json!(" NATIVE-USDT "), "NATIVE-USDT"),
            (json!(null), "USDT"),
            (json!("  "), "USDT"),
        ] {
            let mut raw = fixture(venue);
            raw["info"][key] = native;
            let (entry, aliases) = convert_market(venue, &market(raw), TICK_SIZE).unwrap();
            let identity = entry.market.identity.as_ref().unwrap();
            assert_eq!(identity.settle.as_deref(), Some(expected));
            assert_eq!(identity.settlement_asset_id.as_deref(), Some(expected));
            assert_eq!(entry.market.quote, "USDT");
            assert_eq!(entry.market.symbol, "BTC/USDT");
            assert!(aliases.contains(&"BTC".to_string()));
            assert!(!aliases.contains(&"BTC/NATIVE-USDT".to_string()));
        }
    }
}

#[test]
fn original_info_name_remains_a_shared_alias() {
    for venue in Venue::ALL {
        let mut raw = fixture(venue);
        raw["info"]["name"] = json!("  native-name  ");
        let (_, aliases) = convert_market(venue, &market(raw), TICK_SIZE).unwrap();
        assert!(aliases.contains(&"native-name".to_string()), "{venue:?}");
        assert!(!aliases.contains(&"  native-name  ".to_string()));
    }
}

#[test]
fn active_flags_keep_historical_semantics_for_every_venue_and_product() {
    for venue in Venue::ALL {
        for spot in [false, true] {
            for (active, prelisting, info_active, expected) in [
                (true, json!(null), json!(null), true),
                (false, json!(false), json!(true), false),
                (true, json!(true), json!(true), false),
                (true, json!(false), json!(false), false),
                (true, json!(false), json!(true), true),
                (true, json!("true"), json!("false"), true),
                (true, json!(1), json!(0), true),
            ] {
                let mut raw = fixture(venue);
                if spot {
                    raw["type"] = json!("spot");
                    raw["spot"] = json!(true);
                    raw["swap"] = json!(false);
                    raw["contract"] = json!(false);
                    raw["info"]["index"] = json!(42); // Hyperliquid spot identity
                }
                raw["active"] = json!(active);
                raw["info"]["isPreListing"] = prelisting;
                raw["info"]["active"] = info_active;
                // Only Apex qualifies isPrelaunch; the older venues retain
                // their historical behavior even when the flag is present.
                raw["info"]["isPrelaunch"] = json!(venue != Venue::Apex);
                raw["info"]["enableTrade"] = json!(false);
                raw["info"]["enableDisplay"] = json!(false);
                raw["info"]["enableOpenPosition"] = json!(false);
                let (entry, _) = convert_market(venue, &market(raw), TICK_SIZE).unwrap();
                assert_eq!(entry.market.active, expected, "{venue:?}, spot={spot}");
            }
        }
        for info in [None, Some(JsonValue::Null)] {
            let mut source = market(fixture(venue));
            // Only the trading policy is under test; some identity policies need info.
            let mut raw = fixture(venue);
            raw.as_object_mut().unwrap().remove("info");
            if let Some(info) = info {
                raw["info"] = info;
            }
            source.raw = ccxt::Value::from_json(&raw);
            let metadata = crate::exchanges::ccxt::venues::dispatch!(venue, exchange =>
                exchange::trading_metadata(&source, raw.get("info").unwrap_or(&JsonValue::Null)));
            assert!(metadata.active);
        }
    }
}

#[test]
fn contract_size_preserves_stock_number_semantics_without_derivation() {
    // Apex explicitly suppresses stock's minOrderSize-derived multiplier.
    for venue in Venue::ALL.into_iter().filter(|venue| *venue != Venue::Apex) {
        for (value, expected) in [
            (None, None),
            (Some(json!(null)), None),
            (Some(json!(0)), Some(0.0)),
            (Some(json!(0.25)), Some(0.25)),
            (Some(json!("2.5")), None),
            (Some(json!("NaN")), None),
            (Some(json!(false)), None),
            (Some(json!({})), None),
        ] {
            let mut raw = fixture(venue);
            raw.as_object_mut().unwrap().remove("contractSize");
            if let Some(value) = value {
                raw["contractSize"] = value;
            }
            raw["limits"]["amount"]["min"] = json!(3.0);
            raw["info"]["minOrderSize"] = json!(7.0);
            let (entry, _) = convert_market(venue, &market(raw), TICK_SIZE).unwrap();
            assert_eq!(entry.market.contract_size, expected, "{venue:?}");
            assert_eq!(entry.market.min_order_size, Some(3.0));
        }
        for invalid in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut source = market(fixture(venue));
            ccxt::value::set_value(
                &mut source.raw,
                &ccxt::Value::from("contractSize"),
                ccxt::Value::Float(invalid),
            );
            assert!(
                matches!(convert_market(venue, &source, TICK_SIZE),
                Err(ExchangeError::UpstreamData(message)) if message == "invalid contract size"),
                "{venue:?}, {invalid}"
            );
        }
    }
}

#[test]
fn minimum_size_and_precision_validation_remain_shared() {
    use ccxt::runtime::{DECIMAL_PLACES, SIGNIFICANT_DIGITS};
    for venue in Venue::ALL {
        for (min, expected) in [
            (json!(null), None),
            (json!(0), Some(0.0)),
            (json!("0.5"), Some(0.5)),
            (json!("invalid"), None),
        ] {
            let mut raw = fixture(venue);
            raw["limits"]["amount"]["min"] = min;
            let (entry, _) = convert_market(venue, &market(raw), TICK_SIZE).unwrap();
            assert_eq!(entry.market.min_order_size, expected);
        }
        for invalid in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut source = market(fixture(venue));
            source.limits.amount.min = Some(invalid);
            assert!(matches!(convert_market(venue, &source, TICK_SIZE),
                Err(ExchangeError::UpstreamData(message)) if message == "invalid minimum order size"));
        }
        for (precision, mode, expected) in [
            (None, TICK_SIZE, None),
            (Some(0.0), TICK_SIZE, None),
            (Some(-1.0), TICK_SIZE, None),
            (Some(f64::NAN), TICK_SIZE, None),
            (Some(f64::INFINITY), TICK_SIZE, None),
            (Some(0.25), TICK_SIZE, Some(0.25)),
            (Some(0.0), DECIMAL_PLACES, Some(1.0)),
            (Some(2.0), DECIMAL_PLACES, Some(0.01)),
            (Some(-1.0), DECIMAL_PLACES, Some(10.0)),
            (Some(2.5), DECIMAL_PLACES, None),
            (Some(309.0), DECIMAL_PLACES, None),
            (Some(-309.0), DECIMAL_PLACES, None),
            (Some(3.0), SIGNIFICANT_DIGITS, None),
            (Some(0.1), -1, None),
        ] {
            let mut source = market(fixture(venue));
            source.precision.price = precision;
            let (entry, _) = convert_market(venue, &source, mode).unwrap();
            assert_eq!(
                entry.market.tick_size, expected,
                "{venue:?}, {precision:?}, {mode}"
            );
        }
    }
}

#[test]
fn every_venue_rejects_missing_or_invalid_core_identity_and_product() {
    for venue in Venue::ALL {
        for field in ["id", "symbol", "base", "quote"] {
            for invalid in [
                None,
                Some(json!(null)),
                Some(json!("")),
                Some(json!(" BTC")),
                Some(json!("BTC ")),
            ] {
                let mut raw = fixture(venue);
                raw.as_object_mut().unwrap().remove(field);
                if let Some(invalid) = invalid {
                    raw[field] = invalid;
                }
                assert!(
                    matches!(
                        convert_market(venue, &market(raw), TICK_SIZE),
                        Err(ExchangeError::UpstreamData(_))
                    ),
                    "{venue:?}, {field}"
                );
            }
        }
        for product in ["unknown", "spot", "future", "option"] {
            let mut raw = fixture(venue);
            raw["type"] = json!(product);
            assert!(
                matches!(
                    convert_market(venue, &market(raw), TICK_SIZE),
                    Err(ExchangeError::UpstreamData(_))
                ),
                "{venue:?}, {product}"
            );
        }
    }
}

#[test]
fn apex_native_ids_and_prelaunch_policy_do_not_invent_contract_multipliers() {
    for (prelaunch, active) in [(false, true), (true, false)] {
        let mut raw = fixture(Venue::Apex);
        raw["info"]["isPrelaunch"] = json!(prelaunch);
        raw["info"]["enableOpenPosition"] = json!(false);
        raw["info"]["enableDisplay"] = json!(false);
        let (entry, aliases) = convert_market(Venue::Apex, &market(raw), TICK_SIZE).unwrap();
        assert_eq!(entry.market.active, active);
        assert_eq!(entry.market.contract_size, None);
        assert_eq!(entry.market.min_order_size, Some(0.001));
        let identity = entry.market.identity.as_ref().unwrap();
        assert_eq!(identity.exchange_market_id, "BTCUSDT");
        assert_eq!(identity.settlement_asset_id.as_deref(), Some("USDT"));
        for alias in ["BTC", "BTCUSDT", "BTC-USDT", "BTC/USDT", "BTC/USDT:USDT"] {
            assert!(aliases.contains(&alias.to_string()), "{alias}");
        }
    }
    for id in [json!(null), json!("")] {
        let mut raw = fixture(Venue::Apex);
        raw["id2"] = id;
        assert!(convert_market(Venue::Apex, &market(raw), TICK_SIZE).is_err());
    }
}

mod policy;
