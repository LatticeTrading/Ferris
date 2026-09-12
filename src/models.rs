use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchMarketsRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub include_inactive: bool,
}

#[derive(Debug, Clone)]
pub struct FetchMarketsParams {
    pub params: Value,
    pub include_inactive: bool,
}

impl FetchMarketsRequest {
    pub fn into_params(self) -> FetchMarketsParams {
        FetchMarketsParams {
            params: self.params,
            include_inactive: self.include_inactive,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FetchMarketStatsRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub market_ids: Option<Vec<String>>,
    pub fields: Option<Vec<MarketStatsFieldName>>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchMarketStatsParams {
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchTradesRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchTradesParams {
    pub symbol: String,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchTradesRequest {
    pub fn into_params(self) -> FetchTradesParams {
        FetchTradesParams {
            symbol: self.symbol,
            since: self.since,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchOhlcvRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub timeframe: Option<String>,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchOhlcvParams {
    pub symbol: String,
    pub timeframe: Option<String>,
    pub since: Option<u64>,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchOhlcvRequest {
    pub fn into_params(self) -> FetchOhlcvParams {
        FetchOhlcvParams {
            symbol: self.symbol,
            timeframe: self.timeframe,
            since: self.since,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchOrderBookRequest {
    #[serde(default = "default_exchange")]
    pub exchange: String,
    pub symbol: String,
    pub limit: Option<usize>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone)]
pub struct FetchOrderBookParams {
    pub symbol: String,
    pub limit: Option<usize>,
    pub params: Value,
}

impl FetchOrderBookRequest {
    pub fn into_params(self) -> FetchOrderBookParams {
        FetchOrderBookParams {
            symbol: self.symbol,
            limit: self.limit,
            params: self.params,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CcxtTrade {
    pub info: Value,
    pub amount: Option<f64>,
    pub datetime: Option<String>,
    pub id: Option<String>,
    pub order: Option<String>,
    pub price: Option<f64>,
    pub timestamp: Option<u64>,
    #[serde(rename = "type")]
    pub trade_type: Option<String>,
    pub side: Option<String>,
    pub symbol: Option<String>,
    pub taker_or_maker: Option<String>,
    pub cost: Option<f64>,
    pub fee: Option<CcxtFee>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CcxtFee {
    pub currency: Option<String>,
    pub cost: Option<f64>,
    pub rate: Option<f64>,
}

pub type CcxtOhlcv = (u64, f64, f64, f64, f64, f64);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CcxtOrderBook {
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
    pub datetime: Option<String>,
    pub timestamp: Option<u64>,
    pub nonce: Option<u64>,
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnifiedMarketType {
    Spot,
    Future,
    Perp,
    Option,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedMarketInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_symbol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedMarket {
    pub exchange: String,
    pub symbol: String,
    pub base: String,
    pub quote: String,
    #[serde(rename = "type")]
    pub market_type: UnifiedMarketType,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_order_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_size: Option<f64>,
    pub info: UnifiedMarketInfo,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub identity: Option<MarketIdentity>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchMarketsResponse {
    pub exchange: String,
    pub markets: Vec<UnifiedMarket>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketIdentity {
    pub market_id: String,
    pub exchange_market_id: String,
    pub category: Option<String>,
    pub dex: Option<String>,
    pub contract_type: Option<String>,
    pub settle: Option<String>,
    pub settlement_asset_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MarketStatsFieldName {
    Funding,
    IndexPrice,
    LastPrice,
    LastSettledFunding,
    MarkPrice,
    OpenInterest,
    Volume24h,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsScope {
    pub exchange: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsTopic {
    pub exchange: String,
    pub params: Value,
    pub market_ids: Option<Vec<String>>,
    pub fields: Vec<MarketStatsFieldName>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsSnapshot {
    pub timestamp: u64,
    pub scope: MarketStatsScope,
    pub markets: Vec<MarketStatsRow>,
    pub coverage: MarketStatsCoverage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsRow {
    #[serde(flatten)]
    pub market: UnifiedMarket,
    pub fields: BTreeMap<MarketStatsFieldName, MarketStatsField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsCoverage {
    pub expected_markets: Option<usize>,
    pub returned_markets: usize,
    pub enumeration_complete: bool,
    pub source_failures: Vec<MarketStatsSourceFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsSourceFailure {
    pub source: String,
    pub reason: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MarketStatsFieldState {
    Available,
    NotApplicable,
    Unsupported,
    Unavailable,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsField {
    pub state: MarketStatsFieldState,
    pub value: Option<MarketStatsValue>,
    pub reason: Option<String>,
    pub exchange_timestamp: Option<u64>,
    pub received_timestamp: Option<u64>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MarketStatsValue {
    Funding(FundingValue),
    Price(PriceValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FundingKind {
    Estimate,
    Settled,
    CurrentUnclassified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingValue {
    pub rate: String,
    pub kind: FundingKind,
    pub rate_interval_ms: Option<u64>,
    pub payment_interval_ms: Option<u64>,
    pub payment_timestamp: Option<u64>,
    pub next_payment_timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceValue {
    pub amount: String,
    pub base_asset: String,
    pub quote_asset: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitiesResponse {
    pub exchanges: Vec<ExchangeCapabilities>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeCapabilities {
    pub exchange: String,
    pub market_stats: MarketStatsCapabilities,
    pub funding_rate_history: FeatureCapability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CapabilityState {
    Supported,
    Unsupported,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureCapability {
    pub state: CapabilityState,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum MarketStatsCapabilities {
    Supported(MarketStatsSupportedCapabilities),
    Unsupported { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsSupportedCapabilities {
    pub scope: MarketStatsScope,
    pub all_markets: MarketStatsAllMarketsCapability,
    pub selected_markets: MarketStatsSelectedMarketsCapability,
    pub fields: BTreeMap<UnifiedMarketType, BTreeMap<MarketStatsFieldName, FeatureCapability>>,
    pub upstream_mode: String,
    pub poll_interval_ms: u64,
    pub stale_after_ms: u64,
    pub ws: MarketStatsWsCapability,
    pub funding_kinds: Vec<FundingKind>,
    pub rate_interval_ms: Option<u64>,
    pub payment_interval_ms: Option<u64>,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsAllMarketsCapability {
    pub types: Vec<UnifiedMarketType>,
    pub active_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsSelectedMarketsCapability {
    pub types: Vec<UnifiedMarketType>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketStatsWsCapability {
    pub snapshot: bool,
    pub delta: bool,
    pub max_subscriptions_per_connection: usize,
}

fn default_exchange() -> String {
    "hyperliquid".to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn market_stats_identity_is_flattened_with_explicit_nulls() {
        let mut market = UnifiedMarket {
            exchange: "hyperliquid".to_string(),
            symbol: "BTC/USDC".to_string(),
            base: "BTC".to_string(),
            quote: "USDC".to_string(),
            market_type: UnifiedMarketType::Perp,
            active: true,
            min_order_size: None,
            tick_size: None,
            contract_size: None,
            info: UnifiedMarketInfo::default(),
            identity: Some(MarketIdentity {
                market_id: r#"["hyperliquid","perp",null,"","BTC"]"#.to_string(),
                exchange_market_id: "BTC".to_string(),
                category: None,
                dex: Some(String::new()),
                contract_type: None,
                settle: None,
                settlement_asset_id: None,
            }),
        };
        let encoded = serde_json::to_value(&market).unwrap();
        assert_eq!(
            encoded,
            json!({
                "exchange": "hyperliquid", "symbol": "BTC/USDC", "base": "BTC",
                "quote": "USDC", "type": "perp", "active": true, "info": {},
                "marketId": "[\"hyperliquid\",\"perp\",null,\"\",\"BTC\"]",
                "exchangeMarketId": "BTC", "category": null, "dex": "",
                "contractType": null, "settle": null, "settlementAssetId": null,
            })
        );
        assert_eq!(
            serde_json::from_value::<UnifiedMarket>(encoded).unwrap(),
            market
        );

        market.identity = None;
        let encoded = serde_json::to_value(&market).unwrap();
        assert_eq!(
            encoded,
            json!({
                "exchange": "hyperliquid", "symbol": "BTC/USDC", "base": "BTC",
                "quote": "USDC", "type": "perp", "active": true, "info": {},
            })
        );
        assert_eq!(
            serde_json::from_value::<UnifiedMarket>(encoded).unwrap(),
            market
        );
    }

    #[test]
    fn market_stats_field_values_are_untagged_and_preserve_nullability() {
        let mut field = MarketStatsField {
            state: MarketStatsFieldState::Available,
            value: Some(MarketStatsValue::Funding(FundingValue {
                rate: "-0.000012500".to_string(),
                kind: FundingKind::CurrentUnclassified,
                rate_interval_ms: None,
                payment_interval_ms: Some(3_600_000),
                payment_timestamp: None,
                next_payment_timestamp: None,
            })),
            reason: Some("rate-basis-unverified".to_string()),
            exchange_timestamp: None,
            received_timestamp: Some(1_000),
            source: Some("hyperliquid:primary:metaAndAssetCtxs".to_string()),
        };
        let encoded = serde_json::to_value(&field).unwrap();
        assert_eq!(
            encoded,
            json!({
                "state": "available",
                "value": {
                    "rate": "-0.000012500", "kind": "currentUnclassified",
                    "rateIntervalMs": null, "paymentIntervalMs": 3600000,
                    "paymentTimestamp": null, "nextPaymentTimestamp": null,
                },
                "reason": "rate-basis-unverified", "exchangeTimestamp": null,
                "receivedTimestamp": 1000, "source": "hyperliquid:primary:metaAndAssetCtxs",
            })
        );
        assert_eq!(
            serde_json::from_value::<MarketStatsField>(encoded).unwrap(),
            field
        );

        field.state = MarketStatsFieldState::Unavailable;
        field.value = None;
        field.reason = None;
        field.received_timestamp = None;
        field.source = None;
        assert_eq!(
            serde_json::to_value(&field).unwrap(),
            json!({
                "state": "unavailable", "value": null, "reason": null,
                "exchangeTimestamp": null, "receivedTimestamp": null, "source": null,
            })
        );

        let price = MarketStatsValue::Price(PriceValue {
            amount: "65000.00".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
        });
        let encoded = serde_json::to_value(&price).unwrap();
        assert_eq!(
            encoded,
            json!({
                "amount": "65000.00", "baseAsset": "BTC", "quoteAsset": "USDT",
            })
        );
        assert_eq!(
            serde_json::from_value::<MarketStatsValue>(encoded).unwrap(),
            price
        );
    }

    #[test]
    fn market_stats_request_rejects_mistaken_symbol_and_unknown_fields() {
        let omitted: FetchMarketStatsRequest = serde_json::from_value(json!({})).unwrap();
        let nullable: FetchMarketStatsRequest = serde_json::from_value(json!({
            "marketIds": null, "fields": null, "params": null,
        }))
        .unwrap();
        assert_eq!(omitted, nullable);
        assert_eq!(nullable.exchange, "hyperliquid");
        assert_eq!(nullable.market_ids, None);
        assert_eq!(nullable.fields, None);

        for invalid in [
            json!({"symbol": "BTC/USDC"}),
            json!({"unexpected": true}),
            json!({"fields": ["fundng"]}),
            json!({"fields": "funding"}),
            json!({"marketIds": "BTC"}),
            json!({"marketIds": [1]}),
        ] {
            assert!(
                serde_json::from_value::<FetchMarketStatsRequest>(invalid.clone()).is_err(),
                "accepted invalid request: {invalid}",
            );
        }
    }
}
