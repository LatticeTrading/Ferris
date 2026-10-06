//! A test-only policy example: no new venue, public aliases, or injectable
//! production policy registry. These functions use the actual dispatch signatures.
use ccxt::types::Market;
use serde_json::{json, Value as JsonValue};

use super::{fixture, market};
use crate::{
    exchanges::ccxt::{
        convert::{convert_market, TradingMetadata},
        venue::Venue,
        venues,
    },
    models::UnifiedMarket,
};

mod example {
    use super::{JsonValue, Market, TradingMetadata, UnifiedMarket};
    use crate::exchanges::ccxt::convert::json_string;

    pub(super) fn aliases(source: &Market, converted: &UnifiedMarket, aliases: &mut Vec<String>) {
        let raw = source.raw.to_json();
        if let Some(id2) = json_string(&raw, "id2") {
            aliases.push(id2);
        }
        if let Some(prefix) = raw.pointer("/info/aliasPrefix").and_then(JsonValue::as_str) {
            let identity = converted.identity.as_ref().unwrap();
            aliases.push(format!(
                "{prefix}:{}:{}",
                identity.exchange_market_id, converted.symbol
            ));
        }
    }

    pub(super) fn trading_metadata(source: &Market, info: &JsonValue) -> TradingMetadata {
        let defaults = super::venues::extended::trading_metadata(source, info);
        // Illustrative rules only: flags/provenance must be qualified before a
        // real exchange uses such an exception. No minimum-size substitution.
        TradingMetadata {
            active: defaults.active
                && info.get("isPrelaunch").and_then(JsonValue::as_bool) != Some(true),
            contract_size: if ccxt::value::get_value_k(&source.raw, "contractSizeProvenance")
                .as_str()
                == Some("minimum-order-size")
            {
                None
            } else {
                defaults.contract_size
            },
        }
    }
}

#[test]
fn local_policy_signatures_expose_stock_raw_info_and_final_owned_values() {
    type Aliases = fn(&Market, &UnifiedMarket, &mut Vec<String>);
    type Metadata = fn(&Market, &JsonValue) -> TradingMetadata;
    // Check the example against every production policy's exact signature.
    let mut alias_policies: Vec<Aliases> = Vec::new();
    let mut metadata_policies: Vec<Metadata> = Vec::new();
    for venue in Venue::ALL {
        alias_policies.push(venues::dispatch!(venue, exchange => exchange::aliases));
        metadata_policies.push(venues::dispatch!(venue, exchange => exchange::trading_metadata));
    }
    alias_policies.push(example::aliases);
    metadata_policies.push(example::trading_metadata);

    // Hyperliquid already changes the identity from stock's numeric id to name;
    // Extended already changes display/settlement naming. Both must be final
    // before a metadata-dependent alias policy runs.
    for venue in [Venue::Hyperliquid, Venue::Extended] {
        let mut raw = fixture(venue);
        raw["id2"] = json!(" secondary-stream-id ");
        raw["contractSizeProvenance"] = json!("minimum-order-size");
        raw["info"]["aliasPrefix"] = json!("native");
        raw["info"]["isPrelaunch"] = json!(true);
        let source = market(raw.clone());
        let (entry, ordinary_aliases) =
            convert_market(venue, &source, ccxt::runtime::TICK_SIZE).unwrap();
        assert!(entry.market.active);
        assert_eq!(entry.market.contract_size, Some(1.0));
        assert!(!ordinary_aliases.contains(&"secondary-stream-id".to_string()));

        let mut aliases = ordinary_aliases.clone();
        alias_policies.last().unwrap()(&source, &entry.market, &mut aliases);
        assert_eq!(
            &aliases[..ordinary_aliases.len()],
            ordinary_aliases.as_slice()
        );
        assert_eq!(aliases[ordinary_aliases.len()], "secondary-stream-id");
        let expected = if venue == Venue::Hyperliquid {
            "native:BTC:BTC/USDC"
        } else {
            "native:BTC-USD:BTC/USD"
        };
        assert_eq!(aliases.last().unwrap(), expected);

        let metadata = metadata_policies.last().unwrap()(&source, &raw["info"]);
        assert!(!metadata.active);
        assert_eq!(metadata.contract_size, None);
        // Borrowed input is untouched; only explicit, owned outputs were made.
        assert_eq!(source.raw.to_json(), raw);
        assert_eq!(entry.raw, raw);
    }
}
