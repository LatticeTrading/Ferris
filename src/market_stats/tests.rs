mod live_policy;

use std::collections::HashSet;

use super::*;

#[test]
fn market_stats_ids_have_exact_catalog_encoding() {
    assert_eq!(
        make_market_id(
            "hyperliquid",
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "BTC"
        )
        .unwrap(),
        r#"["hyperliquid","perp",null,"","BTC"]"#,
    );
    assert_eq!(
        make_market_id("hyperliquid", UnifiedMarketType::Spot, None, None, "0").unwrap(),
        r#"["hyperliquid","spot",null,null,"0"]"#,
    );
}

#[test]
fn market_stats_ids_preserve_native_names_and_namespace_boundaries() {
    let identities = [
        (
            "hyperliquid",
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "A:B",
        ),
        ("hyperliquid", UnifiedMarketType::Perp, None, Some("A"), "B"),
        (
            "hyperliquid",
            UnifiedMarketType::Perp,
            Some(""),
            None,
            "A:B",
        ),
        ("hyperliquid", UnifiedMarketType::Perp, None, None, "A:B"),
        ("hyperliquid", UnifiedMarketType::Spot, None, None, "A:B"),
        ("other", UnifiedMarketType::Perp, None, Some(""), "A:B"),
        ("hyperliquid", UnifiedMarketType::Perp, None, Some(""), "AB"),
        (
            "hyperliquid",
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "a:b",
        ),
        (
            "hyperliquid",
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "A\"\\/雪",
        ),
    ];
    let mut ids = HashSet::new();
    for (exchange, market_type, category, dex, native_id) in identities {
        let id = make_market_id(exchange, market_type, category, dex, native_id).unwrap();
        let decoded: (
            String,
            UnifiedMarketType,
            Option<String>,
            Option<String>,
            String,
        ) = serde_json::from_str(&id).unwrap();
        assert_eq!(
            decoded,
            (
                exchange.to_string(),
                market_type,
                category.map(str::to_string),
                dex.map(str::to_string),
                native_id.to_string(),
            ),
        );
        assert!(ids.insert(id), "distinct native identities collided");
    }
}
