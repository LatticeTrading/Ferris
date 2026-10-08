use super::*;

#[test]
fn selected_oi_validation_uses_the_same_policy_as_acquisition() {
    for venue in Venue::ALL {
        let exchange = venue.public_id();
        let request = request(json!({"exchange": exchange, "fields":["openInterest"]}));
        let result = normalize_topic(request);
        if venue == Venue::Binance {
            let Err(ApiError::Validation(message)) = result else {
                panic!("selection must be required")
            };
            assert_eq!(message, "Binance openInterest requires selected marketIds");
        } else {
            assert!(
                result.is_ok(),
                "{} incorrectly requires selected OI",
                exchange
            );
        }
    }
}

#[test]
fn selection_identity_rules_remain_venue_local() {
    for (exchange, params, product, category, dex, native, valid) in [
        (
            "binance",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "BTCUSDT",
            true,
        ),
        (
            "binance",
            json!({}),
            UnifiedMarketType::Perp,
            Some("linear"),
            None,
            "BTCUSDT",
            false,
        ),
        (
            "bybit",
            json!({}),
            UnifiedMarketType::Perp,
            Some("linear"),
            None,
            "BTCUSDT",
            true,
        ),
        (
            "bybit",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "BTCUSDT",
            false,
        ),
        (
            "bybit",
            json!({}),
            UnifiedMarketType::Perp,
            Some("inverse"),
            None,
            "BTCUSD",
            false,
        ),
        (
            "bybit",
            json!({"category":"inverse"}),
            UnifiedMarketType::Perp,
            Some("inverse"),
            None,
            "BTCUSD",
            true,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "0",
            true,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Spot,
            None,
            None,
            "1",
            true,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "01",
            false,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "-1",
            false,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "18446744073709551616",
            false,
        ),
        (
            "lighterxyz",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "0",
            false,
        ),
        (
            "hyperliquid",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            Some(""),
            "BTC",
            true,
        ),
        (
            "hyperliquid",
            json!({}),
            UnifiedMarketType::Spot,
            None,
            None,
            "0",
            true,
        ),
        (
            "hyperliquid",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "BTC",
            false,
        ),
        (
            "aster",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "BTCUSDT",
            true,
        ),
        (
            "extended",
            json!({}),
            UnifiedMarketType::Perp,
            None,
            None,
            "BTC-USD",
            true,
        ),
    ] {
        let id = make_market_id(exchange, product, category, dex, native).unwrap();
        let result = normalize_topic(request(
            json!({"exchange":exchange, "params":params, "marketIds":[id], "fields":["openInterest"]}),
        ));
        assert_eq!(result.is_ok(), valid, "{id}: {result:?}");
    }
}

#[test]
fn payment_expiry_is_opt_in_and_uses_original_per_field_source_clock() {
    let now = Instant::now();
    for venue in Venue::ALL {
        for kind in [
            FundingKind::Estimate,
            FundingKind::CurrentUnclassified,
            FundingKind::Settled,
        ] {
            for exchange_time in [Some(10_000), None] {
                let mut market = row("BTC", UnifiedMarketType::Perp, true, 10_000, Some("0"));
                market.market.exchange = venue.public_id().into();
                let funding = market
                    .fields
                    .get_mut(&MarketStatsFieldName::Funding)
                    .unwrap();
                funding.exchange_timestamp = exchange_time;
                let Some(MarketStatsValue::Funding(value)) = &mut funding.value else {
                    unreachable!()
                };
                value.kind = kind;
                value.next_payment_timestamp = Some(11_000);
                let funding = funding.clone();
                market
                    .fields
                    .insert(MarketStatsFieldName::LastSettledFunding, funding);
                let id = row_id(&market).unwrap().to_string();
                // Bulk receipt is newer: expiry must follow the original funding receipt.
                let mut baseline = source(vec![market], now + Duration::from_secs(10));
                baseline.field_received_at.insert(
                    id,
                    BTreeMap::from([
                        (MarketStatsFieldName::Funding, now),
                        (MarketStatsFieldName::LastSettledFunding, now),
                    ]),
                );
                assert!(expire_snapshot(&baseline, now + Duration::from_millis(999)).is_none());
                let expired = expire_snapshot(&baseline, now + Duration::from_secs(1));
                let enabled = matches!(venue, Venue::Aster | Venue::Bybit | Venue::Kucoin)
                    && kind == FundingKind::Estimate;
                assert_eq!(expired.is_some(), enabled, "{venue:?} {kind:?}");
                if let Some(expired) = expired {
                    let field = &expired.rows[0].fields[&MarketStatsFieldName::Funding];
                    assert_eq!(field.reason.as_deref(), Some("funding-payment-passed"));
                    assert_eq!(
                        field.value,
                        baseline.rows[0].fields[&MarketStatsFieldName::Funding].value
                    );
                    assert_eq!(
                        expired.rows[0].fields[&MarketStatsFieldName::LastSettledFunding].state,
                        MarketStatsFieldState::Available
                    );
                    assert_eq!(
                        expired.rows[0].fields[&MarketStatsFieldName::MarkPrice].state,
                        MarketStatsFieldState::Available
                    );
                } else {
                    let expired = expire_snapshot(&baseline, now + STALE_AFTER).unwrap();
                    assert_eq!(
                        expired.rows[0].fields[&MarketStatsFieldName::Funding]
                            .reason
                            .as_deref(),
                        Some("stale-threshold")
                    );
                }
            }
        }
    }
}
