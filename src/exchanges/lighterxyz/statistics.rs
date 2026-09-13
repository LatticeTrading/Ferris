use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::SystemTime,
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio::time::{timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::LighterExchange;
use crate::{
    exchanges::traits::{ExchangeError, MarketStatsSource},
    market_stats::MarketStatsSourceSnapshot,
    models::{
        CapabilityState, FeatureCapability, FetchMarketStatsParams, FundingKind, FundingValue,
        MarketStatsAllMarketsCapability, MarketStatsCapabilities, MarketStatsField,
        MarketStatsFieldName, MarketStatsFieldState, MarketStatsRow, MarketStatsScope,
        MarketStatsSelectedMarketsCapability, MarketStatsSourceFailure,
        MarketStatsSupportedCapabilities, MarketStatsValue, MarketStatsWsCapability, PriceValue,
        UnifiedMarketType,
    },
    ws_shared::{normalize_lighter_timestamp_ms, parse_u64_lossy},
};

const SOURCE: &str = "lighterxyz:market_stats";
const REQUIRED_NATIVE_FIELDS: [&str; 5] = [
    "current_funding_rate",
    "funding_rate",
    "mark_price",
    "index_price",
    "last_trade_price",
];
const FIELDS: [MarketStatsFieldName; 7] = [
    MarketStatsFieldName::Funding,
    MarketStatsFieldName::LastSettledFunding,
    MarketStatsFieldName::MarkPrice,
    MarketStatsFieldName::IndexPrice,
    MarketStatsFieldName::LastPrice,
    MarketStatsFieldName::Volume24h,
    MarketStatsFieldName::OpenInterest,
];

#[derive(Debug, Clone)]
pub(crate) struct NativeMetadata {
    pub(crate) symbol: String,
    pub(crate) active: bool,
    pub(crate) market_type: UnifiedMarketType,
    pub(crate) settlement_asset_id: Option<String>,
}

#[derive(Debug, Default)]
struct NativeStats {
    values: HashMap<u64, Map<String, Value>>,
    timestamps: HashMap<u64, u64>,
    field_timestamps: HashMap<u64, HashMap<String, u64>>,
    received_timestamps: HashMap<u64, HashMap<String, u64>>,
    received_at: Option<Instant>,
    complete: bool,
}

#[async_trait]
impl MarketStatsSource for LighterExchange {
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
                exchange: "lighterxyz".to_string(),
                params: json!({}),
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
            upstream_mode: "nativeWebSocket".to_string(),
            poll_interval_ms: 30_000,
            stale_after_ms: 90_000,
            ws: MarketStatsWsCapability {
                snapshot: true,
                delta: true,
                max_subscriptions_per_connection: 16,
            },
            funding_kinds: vec![FundingKind::Estimate, FundingKind::Settled],
            rate_interval_ms: None,
            payment_interval_ms: None,
            limitations: vec!["rate-basis-unverified".to_string()],
        })
    }

    async fn fetch_market_stats(
        &self,
        _params: FetchMarketStatsParams,
    ) -> Result<MarketStatsSourceSnapshot, ExchangeError> {
        let metadata = self.fetch_native_metadata().await?;
        let mut source_failures = Vec::new();
        let mut active_ids = HashSet::new();
        for (market_id, native) in &metadata {
            if native.market_type == UnifiedMarketType::Perp && native.active {
                active_ids.insert(*market_id);
            }
        }

        let native_stats = if active_ids.is_empty() {
            NativeStats {
                complete: true,
                ..NativeStats::default()
            }
        } else {
            self.fetch_native_stats(&active_ids).await?
        };
        if !native_stats.complete && !active_ids.is_empty() {
            source_failures.push(MarketStatsSourceFailure {
                source: SOURCE.to_string(),
                reason: if native_stats.values.is_empty() {
                    "ws-timeout"
                } else {
                    "incomplete-baseline"
                }
                .to_string(),
                message: format!(
                    "native market_stats baseline covered {} of {} active perpetual markets",
                    native_stats.values.len(),
                    active_ids.len()
                ),
            });
        }

        let mut rows = Vec::with_capacity(metadata.len());
        for (market_id, native) in metadata {
            let unified = super::native_market_to_unified(market_id, &native)?;
            let stats = native_stats.values.get(&market_id);
            let timestamp = native_stats.timestamps.get(&market_id).copied();
            let field_timestamps = native_stats.field_timestamps.get(&market_id);
            let received_timestamps = native_stats.received_timestamps.get(&market_id);
            let fields = build_fields_with_receipts(
                native.market_type,
                unified.active,
                &unified.base,
                &unified.quote,
                stats,
                timestamp,
                field_timestamps,
                received_timestamps,
                native_stats
                    .received_at
                    .map(wall_millis)
                    .unwrap_or_else(|| wall_millis(Instant::now())),
            );
            rows.push(MarketStatsRow {
                market: unified,
                fields,
            });
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
            spot_enumeration_complete: true,
            contexts_valid: true,
            received_at: native_stats.received_at,
            next_poll_at: Instant::now() + Duration::from_secs(30),
            source_failures,
        })
    }
}

impl LighterExchange {
    pub(crate) async fn fetch_native_metadata(
        &self,
    ) -> Result<HashMap<u64, NativeMetadata>, ExchangeError> {
        const REFRESH: Duration = Duration::from_secs(300);
        if let Some((fetched_at, metadata)) = self.native_metadata_cache.read().await.as_ref() {
            if fetched_at.elapsed() < REFRESH {
                return Ok(metadata.clone());
            }
        }
        let _guard = self.native_metadata_refresh_lock.lock().await;
        if let Some((fetched_at, metadata)) = self.native_metadata_cache.read().await.as_ref() {
            if fetched_at.elapsed() < REFRESH {
                return Ok(metadata.clone());
            }
        }
        let response = self
            .get_public("/api/v1/orderBookDetails", &[("filter", "all".to_string())])
            .await?;
        ensure_code_ok(&response, "orderBookDetails")?;
        let mut rows = Vec::new();
        for key in ["order_book_details", "spot_order_book_details"] {
            if let Some(values) = response.get(key).and_then(Value::as_array) {
                rows.extend(values.iter());
            }
        }
        if rows.is_empty() {
            return Err(ExchangeError::UpstreamData(
                "lighterxyz orderBookDetails missing market detail arrays".to_string(),
            ));
        }
        let mut result = HashMap::with_capacity(rows.len());
        for row in rows {
            let id = row
                .get("market_id")
                .and_then(parse_u64_lossy)
                .ok_or_else(|| {
                    ExchangeError::UpstreamData(
                        "lighterxyz orderBookDetails row missing numeric market_id".to_string(),
                    )
                })?;
            let market_type = match row.get("market_type").and_then(Value::as_str) {
                Some("perp") => UnifiedMarketType::Perp,
                Some("spot") => UnifiedMarketType::Spot,
                Some(other) => {
                    return Err(ExchangeError::UpstreamData(format!(
                        "lighterxyz orderBookDetails market {id} has unknown market_type `{other}`"
                    )))
                }
                None => {
                    return Err(ExchangeError::UpstreamData(format!(
                        "lighterxyz orderBookDetails market {id} missing market_type"
                    )))
                }
            };
            let status = row.get("status").and_then(Value::as_str).ok_or_else(|| {
                ExchangeError::UpstreamData(format!(
                    "lighterxyz orderBookDetails market {id} missing status"
                ))
            })?;
            let symbol = row
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ExchangeError::UpstreamData(format!(
                        "lighterxyz orderBookDetails market {id} missing symbol"
                    ))
                })?
                .trim()
                .to_string();
            if symbol.is_empty() {
                return Err(ExchangeError::UpstreamData(format!(
                    "lighterxyz orderBookDetails market {id} has empty symbol"
                )));
            }
            let settlement_asset_id = row
                .get("quote_asset_id")
                .and_then(parse_u64_lossy)
                .map(|value| value.to_string())
                .or_else(|| {
                    row.get("quote_asset_id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
            result.insert(
                id,
                NativeMetadata {
                    symbol,
                    active: status.eq_ignore_ascii_case("active"),
                    market_type,
                    settlement_asset_id,
                },
            );
        }
        *self.native_metadata_cache.write().await =
            Some((std::time::Instant::now(), result.clone()));
        Ok(result)
    }

    async fn fetch_native_stats(
        &self,
        active_ids: &HashSet<u64>,
    ) -> Result<NativeStats, ExchangeError> {
        let (mut socket, _) = connect_async(&self.stats_ws_url).await.map_err(|error| {
            ExchangeError::UpstreamRequest(format!("Lighter stats websocket: {error}"))
        })?;
        socket
            .send(Message::Text(
                json!({"type": "subscribe", "channel": "market_stats/all"})
                    .to_string()
                    .into(),
            ))
            .await
            .map_err(|error| {
                ExchangeError::UpstreamRequest(format!("Lighter stats subscribe: {error}"))
            })?;

        let deadline = Instant::now() + self.stats_ws_timeout;
        let mut stats = NativeStats::default();
        while !active_ids.iter().all(|id| {
            stats.values.get(id).is_some_and(|values| {
                REQUIRED_NATIVE_FIELDS
                    .iter()
                    .all(|field| values.contains_key(*field))
            })
        }) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let next = match timeout(remaining, socket.next()).await {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(error))) => {
                    if stats.values.is_empty() {
                        return Err(ExchangeError::UpstreamRequest(format!(
                            "Lighter stats websocket read: {error}"
                        )));
                    }
                    break;
                }
                Ok(None) | Err(_) => break,
            };
            match next {
                Message::Ping(payload) => {
                    socket.send(Message::Pong(payload)).await.map_err(|error| {
                        ExchangeError::UpstreamRequest(format!(
                            "Lighter stats websocket pong: {error}"
                        ))
                    })?;
                }
                Message::Text(text) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        if value.get("type").and_then(Value::as_str) == Some("ping") {
                            socket
                                .send(Message::Text(json!({"type":"pong"}).to_string().into()))
                                .await
                                .map_err(|error| {
                                    ExchangeError::UpstreamRequest(format!(
                                        "Lighter stats websocket pong: {error}"
                                    ))
                                })?;
                        }
                    }
                    merge_stats_message(&text, active_ids, &mut stats);
                }
                Message::Binary(bytes) => {
                    if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                        merge_stats_message(&text, active_ids, &mut stats);
                    }
                }
                Message::Close(_) => break,
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        stats.complete = active_ids.iter().all(|id| {
            stats.values.get(id).is_some_and(|values| {
                REQUIRED_NATIVE_FIELDS
                    .iter()
                    .all(|field| values.contains_key(*field))
            })
        });
        Ok(stats)
    }
}

fn merge_stats_message(text: &str, active_ids: &HashSet<u64>, stats: &mut NativeStats) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };
    if value.get("type").and_then(Value::as_str) != Some("update/market_stats") {
        return;
    }
    let timestamp = value
        .get("timestamp")
        .and_then(parse_u64_lossy)
        .map(normalize_lighter_timestamp_ms);
    let Some(markets) = value.get("market_stats").and_then(Value::as_object) else {
        return;
    };
    for (key, raw) in markets {
        let Some(id) = key
            .parse::<u64>()
            .ok()
            .or_else(|| raw.get("market_id").and_then(parse_u64_lossy))
        else {
            continue;
        };
        if !active_ids.contains(&id) {
            continue;
        }
        let Some(object) = raw.as_object() else {
            continue;
        };
        let receipt = wall_millis(Instant::now());
        let entry = stats.values.entry(id).or_default();
        let field_times = stats.field_timestamps.entry(id).or_default();
        let received_times = stats.received_timestamps.entry(id).or_default();
        for (field, value) in object {
            entry.insert(field.clone(), value.clone());
            if let Some(timestamp) = timestamp {
                field_times.insert(field.clone(), timestamp);
            }
            received_times.insert(field.clone(), receipt);
        }
        if let Some(timestamp) = timestamp {
            stats.timestamps.insert(id, timestamp);
        }
        stats.received_at.get_or_insert_with(Instant::now);
    }
}

fn build_fields_with_receipts(
    market_type: UnifiedMarketType,
    active: bool,
    base: &str,
    quote: &str,
    stats: Option<&Map<String, Value>>,
    timestamp: Option<u64>,
    field_timestamps: Option<&HashMap<String, u64>>,
    received_timestamps: Option<&HashMap<String, u64>>,
    fallback_received_at: u64,
) -> BTreeMap<MarketStatsFieldName, MarketStatsField> {
    FIELDS
        .into_iter()
        .map(|field| {
            let (support, reason) = field_support(market_type, field);
            let result = match support {
                CapabilityState::NotApplicable => {
                    fixed_field(MarketStatsFieldState::NotApplicable, reason)
                }
                CapabilityState::Unsupported => {
                    fixed_field(MarketStatsFieldState::Unsupported, reason)
                }
                CapabilityState::Supported if !active => {
                    fixed_field(MarketStatsFieldState::Unavailable, Some("inactive-market"))
                }
                CapabilityState::Supported => observed_field(
                    field,
                    base,
                    quote,
                    stats,
                    field_timestamps
                        .and_then(|times| times.get(native_field_name(field)).copied())
                        .or(timestamp),
                    received_timestamps
                        .and_then(|times| times.get(native_field_name(field)).copied())
                        .unwrap_or(fallback_received_at),
                ),
            };
            (field, result)
        })
        .collect()
}

fn native_field_name(field: MarketStatsFieldName) -> &'static str {
    match field {
        MarketStatsFieldName::Funding => "current_funding_rate",
        MarketStatsFieldName::LastSettledFunding => "funding_rate",
        MarketStatsFieldName::MarkPrice => "mark_price",
        MarketStatsFieldName::IndexPrice => "index_price",
        MarketStatsFieldName::LastPrice => "last_trade_price",
        MarketStatsFieldName::Volume24h => "daily_quote_token_volume",
        MarketStatsFieldName::OpenInterest => "open_interest",
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
        (
            UnifiedMarketType::Perp,
            Funding | LastSettledFunding | MarkPrice | IndexPrice | LastPrice,
        ) => (CapabilityState::Supported, None),
        (UnifiedMarketType::Perp, OpenInterest) => {
            (CapabilityState::Unsupported, Some("units-unverified"))
        }
        (UnifiedMarketType::Perp, Volume24h) => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
        _ => (
            CapabilityState::Unsupported,
            Some("adapter-not-implemented"),
        ),
    }
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
    field: MarketStatsFieldName,
    base: &str,
    quote: &str,
    stats: Option<&Map<String, Value>>,
    timestamp: Option<u64>,
    received_at: u64,
) -> MarketStatsField {
    let mut result = MarketStatsField {
        state: MarketStatsFieldState::Unavailable,
        value: None,
        reason: Some("invalid-upstream-value".to_string()),
        source: Some(SOURCE.to_string()),
        exchange_timestamp: timestamp,
        received_timestamp: Some(received_at),
    };
    let Some(stats) = stats else { return result };
    let value = match field {
        MarketStatsFieldName::Funding => {
            let Some(raw) = decimal(stats.get("current_funding_rate")) else {
                return result;
            };
            MarketStatsValue::Funding(FundingValue {
                rate: raw.to_string(),
                kind: FundingKind::Estimate,
                rate_interval_ms: None,
                payment_interval_ms: None,
                payment_timestamp: None,
                next_payment_timestamp: None,
            })
        }
        MarketStatsFieldName::LastSettledFunding => {
            let Some(raw) = decimal(stats.get("funding_rate")) else {
                return result;
            };
            let payment_timestamp = stats
                .get("funding_timestamp")
                .and_then(parse_u64_lossy)
                .map(normalize_lighter_timestamp_ms);
            MarketStatsValue::Funding(FundingValue {
                rate: raw.to_string(),
                kind: FundingKind::Settled,
                rate_interval_ms: None,
                payment_interval_ms: None,
                payment_timestamp,
                next_payment_timestamp: None,
            })
        }
        MarketStatsFieldName::MarkPrice => {
            match price_value(stats.get("mark_price"), base, quote) {
                Some(value) => value,
                None => return result,
            }
        }
        MarketStatsFieldName::IndexPrice => {
            match price_value(stats.get("index_price"), base, quote) {
                Some(value) => value,
                None => return result,
            }
        }
        MarketStatsFieldName::LastPrice => {
            match price_value(stats.get("last_trade_price"), base, quote) {
                Some(value) => value,
                None => return result,
            }
        }
        _ => return result,
    };
    result.state = MarketStatsFieldState::Available;
    result.value = Some(value);
    result.reason = if field == MarketStatsFieldName::Funding {
        Some("rate-basis-unverified".to_string())
    } else {
        None
    };
    result
}

fn price_value(raw: Option<&Value>, base: &str, quote: &str) -> Option<MarketStatsValue> {
    let raw = decimal(raw)?;
    if raw.starts_with('-') || decimal_is_zero(raw) {
        return None;
    }
    Some(MarketStatsValue::Price(PriceValue {
        amount: raw.to_string(),
        base_asset: base.to_string(),
        quote_asset: quote.to_string(),
    }))
}

fn decimal(value: Option<&Value>) -> Option<&str> {
    let raw = value?.as_str()?;
    if raw.starts_with('+') {
        return None;
    }
    let unsigned = raw.strip_prefix('-').unwrap_or(raw);
    let (integer, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(i, f)| (i, Some(f)));
    if integer.is_empty() || !integer.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if fraction.is_some_and(|fraction| {
        fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return None;
    }
    Some(raw)
}

fn decimal_is_zero(value: &str) -> bool {
    value
        .trim_start_matches(['+', '-'])
        .chars()
        .all(|character| character == '0' || character == '.')
}

fn ensure_code_ok(response: &Value, endpoint: &str) -> Result<(), ExchangeError> {
    let code = response
        .get("code")
        .and_then(parse_u64_lossy)
        .unwrap_or(200);
    if matches!(code, 0 | 200) {
        return Ok(());
    }
    Err(ExchangeError::UpstreamRequest(format!(
        "lighterxyz {endpoint} returned code {code}"
    )))
}

fn wall_millis(_receipt: Instant) -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use serde_json::json;

    use super::*;
    use crate::{
        exchanges::lighterxyz::LighterMarketCatalogService,
        market_stats::make_market_id,
        models::{MarketStatsCapabilities, MarketStatsValue},
    };

    fn exchange() -> LighterExchange {
        let catalog = Arc::new(
            LighterMarketCatalogService::new(
                1_000,
                "http://127.0.0.1:1/markets".to_string(),
                60_000,
            )
            .unwrap(),
        );
        LighterExchange::new("http://127.0.0.1:1".to_string(), 1_000, catalog).unwrap()
    }

    #[test]
    fn capabilities_advertise_native_lighter_contract() {
        let MarketStatsCapabilities::Supported(capabilities) = exchange().capabilities() else {
            panic!("Lighter statistics must be supported")
        };
        assert_eq!(capabilities.scope.exchange, "lighterxyz");
        assert_eq!(capabilities.scope.params, json!({}));
        assert_eq!(capabilities.upstream_mode, "nativeWebSocket");
        assert_eq!(capabilities.poll_interval_ms, 30_000);
        assert_eq!(capabilities.stale_after_ms, 90_000);
        assert_eq!(
            capabilities.funding_kinds,
            vec![FundingKind::Estimate, FundingKind::Settled]
        );
        assert_eq!(capabilities.payment_interval_ms, None);
        assert_eq!(
            capabilities.fields[&UnifiedMarketType::Spot][&MarketStatsFieldName::Funding].state,
            CapabilityState::NotApplicable
        );
        assert_eq!(
            capabilities.fields[&UnifiedMarketType::Perp][&MarketStatsFieldName::OpenInterest]
                .state,
            CapabilityState::Unsupported
        );
    }

    #[test]
    fn identities_keep_opaque_numeric_native_ids_for_perps_and_spot() {
        assert_eq!(
            make_market_id("lighterxyz", UnifiedMarketType::Perp, None, None, "86").unwrap(),
            r#"["lighterxyz","perp",null,null,"86"]"#
        );
        assert_eq!(
            make_market_id("lighterxyz", UnifiedMarketType::Spot, None, None, "2050").unwrap(),
            r#"["lighterxyz","spot",null,null,"2050"]"#
        );
    }

    #[test]
    fn sparse_bulk_updates_merge_estimate_settled_and_timestamp_fields() {
        let active = HashSet::from([86_u64]);
        let mut state = NativeStats::default();
        merge_stats_message(
            &json!({
                "type":"update/market_stats", "timestamp": 1_722_339_648,
                "market_stats": {"86": {
                    "market_id": 86, "current_funding_rate":"-0.00100",
                    "mark_price":"100.00", "index_price":"101.00"
                }}
            })
            .to_string(),
            &active,
            &mut state,
        );
        merge_stats_message(
            &json!({
                "type":"update/market_stats", "timestamp": 1_722_339_649_000_i64,
                "market_stats": {"86": {
                    "funding_rate":"0.00050", "funding_timestamp":1_722_339_600
                }}
            })
            .to_string(),
            &active,
            &mut state,
        );
        let fields = build_fields_with_receipts(
            UnifiedMarketType::Perp,
            true,
            "BTC",
            "USD",
            state.values.get(&86),
            state.timestamps.get(&86).copied(),
            state.field_timestamps.get(&86),
            state.received_timestamps.get(&86),
            1_000,
        );
        let estimate = fields.get(&MarketStatsFieldName::Funding).unwrap();
        let settled = fields
            .get(&MarketStatsFieldName::LastSettledFunding)
            .unwrap();
        assert_eq!(estimate.exchange_timestamp, Some(1_722_339_648_000));
        assert_eq!(estimate.source.as_deref(), Some(SOURCE));
        assert_eq!(
            estimate.value,
            Some(MarketStatsValue::Funding(FundingValue {
                rate: "-0.00100".to_string(),
                kind: FundingKind::Estimate,
                rate_interval_ms: None,
                payment_interval_ms: None,
                payment_timestamp: None,
                next_payment_timestamp: None,
            }))
        );
        assert_eq!(
            settled.value,
            Some(MarketStatsValue::Funding(FundingValue {
                rate: "0.00050".to_string(),
                kind: FundingKind::Settled,
                rate_interval_ms: None,
                payment_interval_ms: None,
                payment_timestamp: Some(1_722_339_600_000),
                next_payment_timestamp: None,
            }))
        );
        assert_eq!(state.values[&86].len(), 6);
    }

    #[test]
    fn prices_require_positive_exact_decimal_strings() {
        assert!(price_value(Some(&json!("100.00")), "BTC", "USD").is_some());
        for raw in ["0", "0.00", "-1.0", "+1.0", "1e2", "NaN", " 1"] {
            assert!(
                price_value(Some(&json!(raw)), "BTC", "USD").is_none(),
                "{raw}"
            );
        }
        assert!(decimal(Some(&json!("-0.000"))).is_some());
        assert!(decimal(Some(&json!(1.0))).is_none());
    }

    #[test]
    fn spot_funding_is_not_applicable_and_other_fields_are_unsupported() {
        let fields = build_fields_with_receipts(
            UnifiedMarketType::Spot,
            true,
            "LINK",
            "USDC",
            None,
            None,
            None,
            None,
            1_000,
        );
        assert_eq!(
            fields[&MarketStatsFieldName::Funding].state,
            MarketStatsFieldState::NotApplicable
        );
        assert_eq!(
            fields[&MarketStatsFieldName::LastSettledFunding].state,
            MarketStatsFieldState::NotApplicable
        );
        assert_eq!(
            fields[&MarketStatsFieldName::MarkPrice].state,
            MarketStatsFieldState::Unsupported
        );
        assert_eq!(
            fields[&MarketStatsFieldName::MarkPrice].reason.as_deref(),
            Some("adapter-not-implemented")
        );
    }
}
