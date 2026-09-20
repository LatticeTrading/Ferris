use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::{
    map_perp_market, map_spot_market, resolve_settlement, HyperliquidExchange, InfoKey,
    InfoObservation,
};
use crate::{
    exchanges::traits::{ExchangeError, MarketStatsSource},
    market_stats::MarketStatsSourceSnapshot,
    models::{
        CapabilityState, FeatureCapability, FetchMarketStatsParams, FundingKind, FundingRateUnit,
        FundingValue, MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSourceFailure,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability, PriceValue,
        UnifiedMarket, UnifiedMarketType,
    },
};

const PRIMARY_SOURCE: &str = "hyperliquid:primary:metaAndAssetCtxs";
const SPOT_SOURCE: &str = "hyperliquid:spotMeta";
const FIELDS: [MarketStatsFieldName; 7] = [
    MarketStatsFieldName::Funding,
    MarketStatsFieldName::LastSettledFunding,
    MarketStatsFieldName::MarkPrice,
    MarketStatsFieldName::IndexPrice,
    MarketStatsFieldName::LastPrice,
    MarketStatsFieldName::Volume24h,
    MarketStatsFieldName::OpenInterest,
];

#[async_trait]
impl MarketStatsSource for HyperliquidExchange {
    fn capabilities(&self) -> MarketStatsCapabilities {
        let fields = [UnifiedMarketType::Perp, UnifiedMarketType::Spot]
            .into_iter()
            .map(|market_type| {
                let fields = FIELDS
                    .into_iter()
                    .map(|field| {
                        let (state, reason) = field_support(market_type, field);
                        (
                            field,
                            FeatureCapability {
                                state,
                                reason: reason.map(str::to_string),
                            },
                        )
                    })
                    .collect();
                (market_type, fields)
            })
            .collect();
        MarketStatsCapabilities::Supported(MarketStatsSupportedCapabilities {
            scope: MarketStatsScope {
                exchange: "hyperliquid".to_string(),
                params: json!({ "dex": "" }),
            },
            all_markets: MarketStatsAllMarketsCapability {
                types: vec![UnifiedMarketType::Perp],
                active_only: true,
            },
            selected_markets: MarketStatsSelectedMarketsCapability {
                types: vec![UnifiedMarketType::Perp, UnifiedMarketType::Spot],
                limit: 100,
            },
            fields,
            upstream_mode: "sharedPolling".to_string(),
            poll_interval_ms: 30_000,
            stale_after_ms: 90_000,
            ws: MarketStatsWsCapability {
                snapshot: true,
                delta: true,
                max_subscriptions_per_connection: 16,
            },
            funding_kinds: vec![FundingKind::CurrentUnclassified],
            rate_interval_ms: Some(3_600_000),
            payment_interval_ms: Some(3_600_000),
            limitations: vec![
                "primary-dex-only".to_string(),
                "rate-unit-decimal-fraction".to_string(),
                "receipt-time-freshness".to_string(),
            ],
        })
    }

    async fn fetch_market_stats(
        &self,
        _params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        // Scope is validated before this boundary. Selections never enter acquisition.
        let (primary, spot) = tokio::join!(
            self.cached_info(InfoKey::Primary),
            self.cached_info(InfoKey::Spot),
        );
        let fallback = if matches!(&spot.result, Err(ExchangeError::UpstreamRequest(_))) {
            self.last_good_spot.read().await.clone()
        } else {
            None
        };
        normalize(&primary, &spot, fallback.as_deref())
    }
}

fn field_support(
    market_type: UnifiedMarketType,
    field: MarketStatsFieldName,
) -> (CapabilityState, Option<&'static str>) {
    use MarketStatsFieldName::*;
    match (market_type, field) {
        (UnifiedMarketType::Spot, Funding | LastSettledFunding) => {
            (CapabilityState::NotApplicable, Some("non-perpetual-market"))
        }
        (UnifiedMarketType::Perp, Funding | MarkPrice | IndexPrice) => {
            (CapabilityState::Supported, None)
        }
        (UnifiedMarketType::Perp, Volume24h | OpenInterest) => {
            (CapabilityState::Unsupported, Some("units-unverified"))
        }
        _ => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
}

fn normalize(
    primary: &InfoObservation,
    spot: &InfoObservation,
    fallback_spot: Option<&Value>,
) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
    let perps = primary.result.as_ref().map_err(Clone::clone)?;
    let (universe, contexts) = primary_metadata(perps)?;
    let mut source_failures = Vec::new();
    let spot_value = match &spot.result {
        Ok(value) => Some(value.as_ref()),
        Err(error) => {
            source_failures.push(MarketStatsSourceFailure {
                source: SPOT_SOURCE.to_string(),
                reason: match error {
                    ExchangeError::UpstreamRequest(_) => "upstream-failure",
                    _ => "invalid-upstream-data",
                }
                .to_string(),
                message: error.to_string(),
            });
            if matches!(error, ExchangeError::UpstreamRequest(_)) {
                fallback_spot
            } else {
                None
            }
        }
    };
    let settlement = spot_value
        .map(|spot| resolve_settlement(perps, spot))
        .unwrap_or((None, None));
    if settlement.0.is_none() || settlement.1.is_none() {
        source_failures.push(MarketStatsSourceFailure {
            source: SPOT_SOURCE.to_string(),
            reason: "settlement-unresolved".to_string(),
            message: "current collateralToken has no unique valid spot token metadata".to_string(),
        });
    }
    if contexts.is_none() {
        source_failures.push(MarketStatsSourceFailure {
            source: PRIMARY_SOURCE.to_string(),
            reason: "context-mismatch".to_string(),
            message: "asset contexts must be an array aligned with the entire universe".to_string(),
        });
    }

    let mut rows = Vec::with_capacity(universe.len());
    for (index, item) in universe.iter().enumerate() {
        // Validate the whole native identity list before any positional join or display mapping.
        let market = map_perp_market(item, &settlement)?;
        let context = contexts.map(|contexts| &contexts[index]);
        let fields = market_fields(
            &market,
            context,
            contexts.is_some(),
            primary.received_timestamp,
        );
        rows.push(MarketStatsRow { market, fields });
    }

    let mut spot_enumeration_complete = false;
    if let Some(value) = spot_value {
        let metadata = spot_metadata(value);
        spot_enumeration_complete = spot.result.is_ok() && metadata.complete;
        if spot.result.is_ok() && !metadata.complete {
            source_failures.push(MarketStatsSourceFailure {
                source: SPOT_SOURCE.to_string(),
                reason: "invalid-upstream-data".to_string(),
                message: "spot metadata contains invalid or ambiguous tokens or pair identities"
                    .to_string(),
            });
        }
        rows.reserve(metadata.pairs.len());
        for pair in metadata.pairs {
            if let Some(market) = map_spot_market(pair, &metadata.token_map) {
                let fields = market_fields(&market, None, false, primary.received_timestamp);
                rows.push(MarketStatsRow { market, fields });
            } else {
                // The strict metadata validator rejects unrenderable names before reaching here.
                spot_enumeration_complete = false;
            }
        }
    }
    rows.sort_by(|left, right| {
        left.market
            .identity
            .as_ref()
            .map(|identity| &identity.market_id)
            .cmp(
                &right
                    .market
                    .identity
                    .as_ref()
                    .map(|identity| &identity.market_id),
            )
    });
    Ok(MarketStatsSourceSnapshot {
        rows,
        perp_catalog_known: true,
        perp_enumeration_complete: true,
        spot_enumeration_complete,
        contexts_valid: contexts.is_some(),
        received_at: Some(primary.received_at),
        field_received_at: Default::default(),
        next_poll_at: primary.next_poll_at,
        source_failures,
    })
}

fn primary_metadata(value: &Value) -> Result<(&[Value], Option<&[Value]>), ExchangeError> {
    let tuple = value
        .as_array()
        .filter(|tuple| tuple.len() == 2)
        .ok_or_else(|| {
            ExchangeError::UpstreamData(
                "metaAndAssetCtxs must contain exactly two elements".to_string(),
            )
        })?;
    let universe = tuple[0]
        .as_object()
        .and_then(|metadata| metadata.get("universe"))
        .and_then(Value::as_array)
        .filter(|universe| !universe.is_empty())
        .ok_or_else(|| {
            ExchangeError::UpstreamData(
                "primary metadata must contain a nonempty universe".to_string(),
            )
        })?;
    let mut names = HashSet::with_capacity(universe.len());
    for item in universe {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty());
        let Some(name) = name else {
            return Err(ExchangeError::UpstreamData(
                "primary metadata contains a missing or invalid native name".to_string(),
            ));
        };
        if name.contains(':') || !names.insert(name) {
            return Err(ExchangeError::UpstreamData(
                "primary metadata contains a DEX-qualified or duplicate native name".to_string(),
            ));
        }
        if item
            .get("isDelisted")
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(ExchangeError::UpstreamData(
                "primary metadata contains an invalid isDelisted flag".to_string(),
            ));
        }
    }
    let contexts = tuple[1]
        .as_array()
        .filter(|contexts| contexts.len() == universe.len())
        .map(Vec::as_slice);
    Ok((universe, contexts))
}

struct SpotMetadata<'a> {
    token_map: HashMap<u64, String>,
    pairs: Vec<&'a Value>,
    complete: bool,
}

fn spot_metadata(value: &Value) -> SpotMetadata<'_> {
    let mut metadata = SpotMetadata {
        token_map: HashMap::new(),
        pairs: Vec::new(),
        complete: true,
    };
    let Some(tokens) = value.get("tokens").and_then(Value::as_array) else {
        metadata.complete = false;
        return metadata;
    };
    if tokens.is_empty() {
        metadata.complete = false;
    }
    let mut seen_tokens = HashSet::with_capacity(tokens.len());
    for token in tokens {
        let Some(index) = token.get("index").and_then(Value::as_u64) else {
            metadata.complete = false;
            continue;
        };
        if !seen_tokens.insert(index) {
            metadata.token_map.remove(&index);
            metadata.complete = false;
            continue;
        }
        let name = token
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| name.bytes().any(|byte| byte.is_ascii_alphanumeric()));
        match name {
            Some(name) => {
                metadata.token_map.insert(index, name.to_string());
            }
            _ => metadata.complete = false,
        }
    }
    let Some(universe) = value.get("universe").and_then(Value::as_array) else {
        metadata.complete = false;
        return metadata;
    };
    let mut seen_pairs = HashSet::with_capacity(universe.len());
    let mut duplicate_pairs = HashSet::new();
    for pair in universe {
        if let Some(index) = pair.get("index").and_then(Value::as_u64) {
            if !seen_pairs.insert(index) {
                duplicate_pairs.insert(index);
            }
        }
    }
    for pair in universe {
        let valid_id = pair
            .get("index")
            .and_then(Value::as_u64)
            .is_some_and(|index| !duplicate_pairs.contains(&index));
        let valid_tokens = pair
            .get("tokens")
            .and_then(Value::as_array)
            .is_some_and(|tokens| {
                tokens.len() == 2
                    && tokens.iter().all(|index| {
                        index
                            .as_u64()
                            .is_some_and(|index| metadata.token_map.contains_key(&index))
                    })
            });
        let valid_active = pair.get("isDelisted").is_none_or(Value::is_boolean);
        if valid_id && valid_tokens && valid_active {
            metadata.pairs.push(pair);
        } else {
            metadata.complete = false;
        }
    }
    metadata
}

// Only complete, structurally valid token/catalog metadata may become transport-failure fallback.
pub(super) fn spot_metadata_complete(value: &Value) -> bool {
    spot_metadata(value).complete
}

fn market_fields(
    market: &UnifiedMarket,
    context: Option<&Value>,
    contexts_valid: bool,
    received_timestamp: u64,
) -> BTreeMap<MarketStatsFieldName, MarketStatsField> {
    FIELDS
        .into_iter()
        .map(|field| {
            let (support, reason) = field_support(market.market_type, field);
            let result = match support {
                CapabilityState::NotApplicable => {
                    fixed_field(MarketStatsFieldState::NotApplicable, reason)
                }
                CapabilityState::Unsupported => {
                    fixed_field(MarketStatsFieldState::Unsupported, reason)
                }
                CapabilityState::Supported if !market.active => {
                    fixed_field(MarketStatsFieldState::Unavailable, Some("inactive-market"))
                }
                CapabilityState::Supported => {
                    observed_field(market, field, context, contexts_valid, received_timestamp)
                }
            };
            (field, result)
        })
        .collect()
}

fn fixed_field(state: MarketStatsFieldState, reason: Option<&str>) -> MarketStatsField {
    MarketStatsField {
        state,
        value: None,
        reason: reason.map(str::to_string),
        exchange_timestamp: None,
        received_timestamp: None,
        source: None,
    }
}

fn observed_field(
    market: &UnifiedMarket,
    field: MarketStatsFieldName,
    context: Option<&Value>,
    contexts_valid: bool,
    received_timestamp: u64,
) -> MarketStatsField {
    let mut result = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some(
            if contexts_valid {
                "invalid-upstream-value"
            } else {
                "context-mismatch"
            }
            .to_string(),
        ),
        exchange_timestamp: None,
        received_timestamp: Some(received_timestamp),
        source: Some(PRIMARY_SOURCE.to_string()),
    };
    if !contexts_valid {
        return result;
    }
    let key = match field {
        MarketStatsFieldName::Funding => "funding",
        MarketStatsFieldName::MarkPrice => "markPx",
        MarketStatsFieldName::IndexPrice => "oraclePx",
        _ => return result,
    };
    let Some(raw) = context
        .and_then(|context| context.get(key))
        .and_then(decimal_string)
    else {
        return result;
    };
    let value = if field == MarketStatsFieldName::Funding {
        result.reason = None;
        MarketStatsValue::Funding(FundingValue::new(
            raw.to_string(),
            FundingRateUnit::DecimalFraction,
            FundingKind::CurrentUnclassified,
            Some(3_600_000),
            Some(3_600_000),
            None,
            None,
        ))
    } else {
        if raw.starts_with('-') || !raw.bytes().any(|byte| matches!(byte, b'1'..=b'9')) {
            return result;
        }
        // Price units follow native contract names, not sanitized display or settlement assets.
        let native_name = market
            .identity
            .as_ref()
            .map(|identity| identity.exchange_market_id.as_str());
        let Some(native_name) = native_name else {
            return result;
        };
        let quote = if matches!(native_name, "HYPE" | "PURR") {
            "USDC"
        } else {
            "USDT"
        };
        result.reason = None;
        MarketStatsValue::Price(PriceValue {
            amount: raw.to_string(),
            base_asset: native_name.to_string(),
            quote_asset: quote.to_string(),
        })
    };
    result.state = MarketStatsFieldState::Available;
    result.value = Some(value);
    result
}

// Validate the lexical decimal contract without floating-point conversion or canonicalization.
fn decimal_string(value: &Value) -> Option<&str> {
    let raw = value.as_str()?;
    let digits = raw.strip_prefix('-').unwrap_or(raw).as_bytes();
    let integer_end = digits
        .iter()
        .position(|byte| *byte == b'.')
        .unwrap_or(digits.len());
    if integer_end == 0 || !digits[..integer_end].iter().all(u8::is_ascii_digit) {
        return None;
    }
    if integer_end < digits.len() {
        let fractional = &digits[integer_end + 1..];
        if fractional.is_empty() || !fractional.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }
    Some(raw)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::time::Instant;

    use super::*;

    fn observation(result: Result<Value, ExchangeError>) -> InfoObservation {
        let received_at = Instant::now();
        InfoObservation {
            result: result.map(Arc::new),
            received_at,
            received_timestamp: 1_000,
            next_poll_at: received_at + Duration::from_secs(30),
        }
    }

    fn primary_fixture() -> Value {
        json!([
            {"collateralToken": 0, "universe": [
                {"name": "ETH"},
                {"name": "OLD", "isDelisted": true},
                {"name": "BTC"},
                {"name": "HYPE"}
            ]},
            [
                {"funding": "0", "markPx": "2000.01", "oraclePx": "1999.9"},
                {"funding": "0.03", "markPx": "9", "oraclePx": "8"},
                {"funding": "-0.0000125", "markPx": "60000.01", "oraclePx": "59999.9"},
                {"funding": "0.0000125", "markPx": "40.01", "oraclePx": "39.9"}
            ]
        ])
    }

    fn spot_fixture() -> Value {
        json!({
            "tokens": [
                {"index": 9, "name": "BTC", "tokenId": "btc-id"},
                {"index": 4, "name": "USDT", "tokenId": "usdt-id"},
                {"index": 0, "name": "USDC", "tokenId": "usdc-id"}
            ],
            "universe": [{"index": 0, "name": "BTC/USDC", "tokens": [9, 0]}]
        })
    }

    fn row<'a>(
        snapshot: &'a MarketStatsSourceSnapshot,
        market_type: UnifiedMarketType,
        native_id: &str,
    ) -> &'a MarketStatsRow {
        snapshot
            .rows
            .iter()
            .find(|row| {
                row.market.market_type == market_type
                    && row.market.identity.as_ref().unwrap().exchange_market_id == native_id
            })
            .unwrap()
    }

    fn funding(row: &MarketStatsRow) -> &FundingValue {
        match row.fields[&MarketStatsFieldName::Funding]
            .value
            .as_ref()
            .unwrap()
        {
            MarketStatsValue::Funding(value) => value,
            _ => panic!("expected funding value"),
        }
    }

    #[test]
    fn market_stats_positional_join_preserves_zero_sign_and_price_units() {
        let primary = observation(Ok(primary_fixture()));
        let spot = observation(Ok(spot_fixture()));
        let snapshot = normalize(&primary, &spot, None).unwrap();
        assert!(snapshot.perp_catalog_known && snapshot.perp_enumeration_complete);
        assert!(snapshot.spot_enumeration_complete && snapshot.contexts_valid);
        assert!(snapshot.source_failures.is_empty());
        let active: Vec<_> = snapshot
            .rows
            .iter()
            .filter(|row| row.market.market_type == UnifiedMarketType::Perp && row.market.active)
            .map(|row| {
                row.market
                    .identity
                    .as_ref()
                    .unwrap()
                    .exchange_market_id
                    .as_str()
            })
            .collect();
        assert_eq!(active, ["BTC", "ETH", "HYPE"]);
        assert_eq!(
            funding(row(&snapshot, UnifiedMarketType::Perp, "ETH")).rate,
            "0"
        );
        let btc = row(&snapshot, UnifiedMarketType::Perp, "BTC");
        assert_eq!(
            funding(btc),
            &FundingValue::new(
                "-0.0000125".to_string(),
                FundingRateUnit::DecimalFraction,
                FundingKind::CurrentUnclassified,
                Some(3_600_000),
                Some(3_600_000),
                None,
                None,
            )
        );
        assert_eq!(
            btc.fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
        assert_eq!(
            btc.fields[&MarketStatsFieldName::Funding].received_timestamp,
            Some(1_000)
        );
        for native in ["BTC", "HYPE"] {
            let row = row(&snapshot, UnifiedMarketType::Perp, native);
            let identity = row.market.identity.as_ref().unwrap();
            assert_eq!(identity.settle.as_deref(), Some("USDC"));
            assert_eq!(identity.settlement_asset_id.as_deref(), Some("usdc-id"));
            for field in [
                MarketStatsFieldName::MarkPrice,
                MarketStatsFieldName::IndexPrice,
            ] {
                let Some(MarketStatsValue::Price(price)) = &row.fields[&field].value else {
                    panic!("expected observed price");
                };
                assert_eq!(price.base_asset, native);
                assert_eq!(
                    price.quote_asset,
                    if native == "HYPE" { "USDC" } else { "USDT" }
                );
            }
        }
        let inactive = row(&snapshot, UnifiedMarketType::Perp, "OLD");
        for field in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::IndexPrice,
        ] {
            assert_eq!(
                inactive.fields[&field].state,
                MarketStatsFieldState::Unavailable
            );
            assert_eq!(
                inactive.fields[&field].reason.as_deref(),
                Some("inactive-market")
            );
            assert_eq!(inactive.fields[&field].value, None);
        }
        let spot = row(&snapshot, UnifiedMarketType::Spot, "0");
        assert_eq!(spot.market.symbol, btc.market.symbol);
        assert_ne!(
            spot.market.identity.as_ref().unwrap().market_id,
            btc.market.identity.as_ref().unwrap().market_id
        );
        for field in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::LastSettledFunding,
        ] {
            assert_eq!(
                spot.fields[&field].state,
                MarketStatsFieldState::NotApplicable
            );
            assert_eq!(spot.fields[&field].value, None);
            assert_eq!(spot.fields[&field].source, None);
            assert_eq!(spot.fields[&field].received_timestamp, None);
        }
        for field in [
            MarketStatsFieldName::Volume24h,
            MarketStatsFieldName::OpenInterest,
        ] {
            assert_eq!(btc.fields[&field].state, MarketStatsFieldState::Unsupported);
            assert_eq!(
                btc.fields[&field].reason.as_deref(),
                Some("units-unverified")
            );
            assert_eq!(btc.fields[&field].value, None);
        }
    }

    #[test]
    fn market_stats_native_identities_survive_display_collisions_and_reordering() {
        let mut primary = json!([
            {"collateralToken": 0, "universe": [{"name": "A-B"}, {"name": "AB"}, {"name": "kPEPE"}]},
            [
                {"funding": "-1", "markPx": "1", "oraclePx": "2"},
                {"funding": "2", "markPx": "3", "oraclePx": "4"},
                {"funding": "0", "markPx": "0.001", "oraclePx": "0.002"}
            ]
        ]);
        let spot = observation(Ok(spot_fixture()));
        let first = normalize(&observation(Ok(primary.clone())), &spot, None).unwrap();
        primary[0]["universe"].as_array_mut().unwrap().reverse();
        primary[1].as_array_mut().unwrap().reverse();
        let reordered = normalize(&observation(Ok(primary)), &spot, None).unwrap();
        assert_eq!(first.rows, reordered.rows);
        let punctuation = row(&first, UnifiedMarketType::Perp, "A-B");
        let plain = row(&first, UnifiedMarketType::Perp, "AB");
        assert_eq!(punctuation.market.symbol, plain.market.symbol);
        assert_eq!(
            punctuation.market.identity.as_ref().unwrap().market_id,
            r#"["hyperliquid","perp",null,"","A-B"]"#
        );
        assert_ne!(punctuation.market.identity, plain.market.identity);
        assert_eq!(funding(punctuation).rate, "-1");
        assert_eq!(funding(plain).rate, "2");
        let scaled = row(&first, UnifiedMarketType::Perp, "kPEPE");
        let Some(MarketStatsValue::Price(price)) =
            &scaled.fields[&MarketStatsFieldName::MarkPrice].value
        else {
            panic!("expected scaled contract price");
        };
        assert_eq!(price.base_asset, "kPEPE");
        assert_eq!(price.amount, "0.001");
    }

    #[test]
    fn market_stats_scalar_invalidations_do_not_poison_other_fields() {
        let mut primary = primary_fixture();
        primary[1][2] = json!({"funding": "NaN", "markPx": "60000.0100", "midPx": "50000"});
        primary[1][0] = Value::Null;
        let snapshot = normalize(
            &observation(Ok(primary)),
            &observation(Ok(spot_fixture())),
            None,
        )
        .unwrap();
        assert!(snapshot.contexts_valid && snapshot.perp_enumeration_complete);
        let btc = row(&snapshot, UnifiedMarketType::Perp, "BTC");
        for field in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::IndexPrice,
        ] {
            assert_eq!(btc.fields[&field].state, MarketStatsFieldState::Unavailable);
            assert_eq!(btc.fields[&field].value, None);
            assert_eq!(
                btc.fields[&field].reason.as_deref(),
                Some("invalid-upstream-value")
            );
        }
        let Some(MarketStatsValue::Price(price)) =
            &btc.fields[&MarketStatsFieldName::MarkPrice].value
        else {
            panic!("valid mark must survive another field's invalidation");
        };
        assert_eq!(price.amount, "60000.0100");
        for field in [
            MarketStatsFieldName::Funding,
            MarketStatsFieldName::MarkPrice,
            MarketStatsFieldName::IndexPrice,
        ] {
            assert_eq!(
                row(&snapshot, UnifiedMarketType::Perp, "ETH").fields[&field]
                    .reason
                    .as_deref(),
                Some("invalid-upstream-value")
            );
        }
    }

    #[test]
    fn market_stats_decimal_grammar_is_exact_and_prices_are_positive() {
        for raw in [
            "0",
            "-0",
            "-0.0000125",
            "001.2300",
            "9999999999999999999999999999999999999999.1",
        ] {
            assert_eq!(decimal_string(&json!(raw)), Some(raw));
        }
        for invalid in [
            json!(null),
            json!(0),
            json!(""),
            json!(" 1"),
            json!("1 "),
            json!("NaN"),
            json!("Infinity"),
            json!("1e-3"),
            json!("+1"),
            json!("-"),
            json!(".1"),
            json!("1."),
            json!("1.2.3"),
            json!("--1"),
            json!("１２"),
        ] {
            assert_eq!(decimal_string(&invalid), None, "accepted {invalid}");
        }
        let market = map_perp_market(&json!({"name": "BTC"}), &(None, None)).unwrap();
        for amount in ["0", "0.000", "-0", "-0.1"] {
            let field = observed_field(
                &market,
                MarketStatsFieldName::MarkPrice,
                Some(&json!({"markPx": amount})),
                true,
                10,
            );
            assert_eq!(
                field.state,
                MarketStatsFieldState::Unavailable,
                "accepted price {amount}"
            );
            assert_eq!(field.value, None);
        }
        let field = observed_field(
            &market,
            MarketStatsFieldName::MarkPrice,
            Some(&json!({"markPx": "000.0001"})),
            true,
            10,
        );
        assert_eq!(field.state, MarketStatsFieldState::Available);
        assert_eq!(
            field.value,
            Some(MarketStatsValue::Price(PriceValue {
                amount: "000.0001".to_string(),
                base_asset: "BTC".to_string(),
                quote_asset: "USDT".to_string(),
            }))
        );
    }

    #[test]
    fn market_stats_metadata_integrity_is_separate_from_context_alignment() {
        let spot = observation(Ok(spot_fixture()));
        for invalid in [
            json!([]),
            json!([{"universe": []}, []]),
            json!([{}, []]),
            json!([{"universe": [{"name": "BTC"}, {}]}, [{}, {}]]),
            json!([{"universe": [{"name": "BTC"}, {"name": "BTC"}]}, [{}, {}]]),
            json!([{"universe": [{"name": "dex:BTC"}]}, [{}]]),
            json!([{"universe": [{"name": ""}]}, [{}]]),
            json!([{"universe": [{"name": "BTC", "isDelisted": "false"}]}, [{}]]),
            json!([{"universe": [{"name": "BTC"}]}, [{}], {}]),
        ] {
            assert!(matches!(
                normalize(&observation(Ok(invalid)), &spot, None),
                Err(ExchangeError::UpstreamData(_))
            ));
        }
        for contexts in [json!([]), json!([{}]), json!({}), Value::Null] {
            let mut primary = primary_fixture();
            primary[1] = contexts;
            let snapshot = normalize(&observation(Ok(primary)), &spot, None).unwrap();
            assert!(snapshot.perp_catalog_known && snapshot.perp_enumeration_complete);
            assert!(!snapshot.contexts_valid);
            assert!(snapshot
                .source_failures
                .iter()
                .any(|failure| failure.reason == "context-mismatch"));
            assert_eq!(
                row(&snapshot, UnifiedMarketType::Perp, "BTC").fields
                    [&MarketStatsFieldName::Funding]
                    .reason
                    .as_deref(),
                Some("context-mismatch")
            );
            assert_eq!(
                row(&snapshot, UnifiedMarketType::Perp, "OLD").fields
                    [&MarketStatsFieldName::Funding]
                    .reason
                    .as_deref(),
                Some("inactive-market")
            );
        }
        let all_delisted = json!([{"collateralToken": 0, "universe": [{"name": "OLD", "isDelisted": true}]}, [{}]]);
        let snapshot = normalize(&observation(Ok(all_delisted)), &spot, None).unwrap();
        assert!(snapshot.perp_enumeration_complete && snapshot.contexts_valid);
        assert!(!snapshot
            .rows
            .iter()
            .any(|row| row.market.market_type == UnifiedMarketType::Perp && row.market.active));
    }

    #[test]
    fn market_stats_ambiguous_spot_identities_do_not_remove_valid_perps() {
        let mut spot = spot_fixture();
        spot["universe"] = json!([
            {"index": 0, "tokens": [9, 0]},
            {"index": 0, "tokens": [4, 0]},
            {"index": 1, "tokens": [4, 0]},
            {"index": 2, "tokens": [999, 0]},
            {"tokens": [9, 0]}
        ]);
        assert!(!spot_metadata_complete(&spot));
        let snapshot = normalize(
            &observation(Ok(primary_fixture())),
            &observation(Ok(spot)),
            None,
        )
        .unwrap();
        assert!(snapshot.perp_enumeration_complete);
        assert!(!snapshot.spot_enumeration_complete);
        let spots: Vec<_> = snapshot
            .rows
            .iter()
            .filter(|row| row.market.market_type == UnifiedMarketType::Spot)
            .collect();
        assert_eq!(spots.len(), 1);
        assert_eq!(
            spots[0]
                .market
                .identity
                .as_ref()
                .unwrap()
                .exchange_market_id,
            "1"
        );
        assert_eq!(
            row(&snapshot, UnifiedMarketType::Perp, "BTC").fields[&MarketStatsFieldName::Funding]
                .state,
            MarketStatsFieldState::Available
        );

        let mut duplicate_token = spot_fixture();
        let tokens = duplicate_token["tokens"].as_array_mut().unwrap();
        tokens.push(json!({"index": 9, "name": "FAKE", "tokenId": "fake-id"}));
        tokens.push(json!({"index": 9, "name": "BTC", "tokenId": "btc-id"}));
        assert!(!spot_metadata_complete(&duplicate_token));
        let snapshot = normalize(
            &observation(Ok(primary_fixture())),
            &observation(Ok(duplicate_token)),
            None,
        )
        .unwrap();
        assert!(!snapshot
            .rows
            .iter()
            .any(|row| row.market.market_type == UnifiedMarketType::Spot));
        assert_eq!(
            row(&snapshot, UnifiedMarketType::Perp, "BTC")
                .market
                .identity
                .as_ref()
                .unwrap()
                .settle
                .as_deref(),
            Some("USDC")
        );
    }

    #[test]
    fn market_stats_spot_identity_does_not_require_a_settlement_token_id() {
        let mut spot = spot_fixture();
        for token in spot["tokens"].as_array_mut().unwrap() {
            token.as_object_mut().unwrap().remove("tokenId");
        }
        assert!(spot_metadata_complete(&spot));
        let snapshot = normalize(
            &observation(Ok(primary_fixture())),
            &observation(Ok(spot)),
            None,
        )
        .unwrap();
        assert!(snapshot.spot_enumeration_complete);
        assert_eq!(
            row(&snapshot, UnifiedMarketType::Spot, "0").fields[&MarketStatsFieldName::Funding]
                .state,
            MarketStatsFieldState::NotApplicable
        );
        let btc = row(&snapshot, UnifiedMarketType::Perp, "BTC");
        assert_eq!(btc.market.identity.as_ref().unwrap().settle, None);
        assert_eq!(
            btc.market.identity.as_ref().unwrap().settlement_asset_id,
            None
        );
        assert_eq!(
            btc.fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::Available
        );
        assert!(snapshot
            .source_failures
            .iter()
            .any(|failure| failure.reason == "settlement-unresolved"));
    }

    #[test]
    fn market_stats_settlement_fallback_only_follows_transport_failure() {
        let good_spot = spot_fixture();
        assert!(spot_metadata_complete(&good_spot));
        let mut primary = primary_fixture();
        primary[0]["collateralToken"] = json!(4);
        let primary = observation(Ok(primary));
        let failed_spot = observation(Err(ExchangeError::UpstreamRequest("offline".to_string())));
        let snapshot = normalize(&primary, &failed_spot, Some(&good_spot)).unwrap();
        let identity = row(&snapshot, UnifiedMarketType::Perp, "BTC")
            .market
            .identity
            .as_ref()
            .unwrap();
        assert_eq!(identity.settle.as_deref(), Some("USDT"));
        assert_eq!(identity.settlement_asset_id.as_deref(), Some("usdt-id"));
        assert!(!snapshot.spot_enumeration_complete);
        assert_eq!(
            row(&snapshot, UnifiedMarketType::Spot, "0").fields[&MarketStatsFieldName::Funding]
                .state,
            MarketStatsFieldState::NotApplicable
        );
        assert!(snapshot
            .source_failures
            .iter()
            .any(|failure| failure.source == SPOT_SOURCE && failure.reason == "upstream-failure"));

        let mut duplicate = good_spot.clone();
        duplicate["tokens"]
            .as_array_mut()
            .unwrap()
            .push(json!({"index": 4, "name": "USDT", "tokenId": "other-id"}));
        let mut missing = good_spot.clone();
        missing["tokens"].as_array_mut().unwrap().remove(1);
        let mut invalid = good_spot.clone();
        invalid["tokens"][1]["tokenId"] = Value::Null;
        for spot_result in [
            Ok(duplicate),
            Ok(missing),
            Ok(invalid),
            Err(ExchangeError::UpstreamData("invalid JSON".to_string())),
        ] {
            let snapshot =
                normalize(&primary, &observation(spot_result), Some(&good_spot)).unwrap();
            let btc = row(&snapshot, UnifiedMarketType::Perp, "BTC");
            let identity = btc.market.identity.as_ref().unwrap();
            assert_eq!(identity.settle, None);
            assert_eq!(identity.settlement_asset_id, None);
            assert_eq!(
                btc.fields[&MarketStatsFieldName::Funding].state,
                MarketStatsFieldState::Available
            );
            assert!(snapshot
                .source_failures
                .iter()
                .any(|failure| failure.reason == "settlement-unresolved"));
        }
        let snapshot = normalize(&primary, &failed_spot, None).unwrap();
        assert_eq!(
            row(&snapshot, UnifiedMarketType::Perp, "BTC")
                .market
                .identity
                .as_ref()
                .unwrap()
                .settle,
            None
        );
        assert!(!snapshot.spot_enumeration_complete);
        let primary_failure =
            observation(Err(ExchangeError::UpstreamRequest("offline".to_string())));
        assert!(matches!(
            normalize(&primary_failure, &observation(Ok(good_spot)), None),
            Err(ExchangeError::UpstreamRequest(_))
        ));
    }
}
