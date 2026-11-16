//! Amount semantics at the owned-book boundary, never inside an upstream cache.
//! Native quantities remain the default. Base views are computed once per
//! publication and shared by all receivers; upstream checksum/delta state stays native.

use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::{exchanges::traits::ExchangeError, models::CcxtOrderBook};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BookAmountMode {
    #[default]
    Native,
    Base,
}

impl BookAmountMode {
    pub(crate) fn parse(params: &Value) -> Result<Self, ExchangeError> {
        if !params.is_null() && !params.is_object() {
            return Err(ExchangeError::BadSymbol(
                "params must be an object or null".into(),
            ));
        }
        match params.get("amountUnit") {
            None | Some(Value::Null) => Ok(Self::Native),
            Some(Value::String(s)) if s == "native" => Ok(Self::Native),
            Some(Value::String(s)) if s == "base" => Ok(Self::Base),
            _ => Err(ExchangeError::BadSymbol(
                "amountUnit must be `native` or `base`".into(),
            )),
        }
    }

    /// Native adapters' existing validators must still reject other parameters.
    pub(crate) fn without_selector(params: &Value) -> Value {
        let mut params = params.clone();
        if let Some(map) = params.as_object_mut() {
            map.remove("amountUnit");
        }
        params
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AmountUnit {
    Base,
    Contracts,
    Quote,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LiquidityKind {
    OrderBook,
    Indicative,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AmountConversionMethod {
    Identity,
    Multiply,
    MultiplyDivideByPrice,
    #[default]
    Unavailable,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AmountConversion {
    pub method: AmountConversionMethod,
    pub contract_size_applied: Option<f64>,
    pub contract_size_currency: Option<String>,
    pub source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BookMetadata {
    pub amount_unit: AmountUnit,
    pub amount_currency: Option<String>,
    pub source_amount_unit: AmountUnit,
    pub amount_conversion: AmountConversion,
    pub liquidity_kind: LiquidityKind,
    /// When a dynamic liquidity classification was observed in the catalog.
    pub liquidity_timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BaseRule {
    Identity,
    Contract { size: f64, inverse: bool },
}

/// Qualified by the adapter, not inferred from `contractSize` or symbol spelling.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BookPolicy {
    unit: AmountUnit,
    base: String,
    quote: String,
    rule: Option<BaseRule>,
    source: &'static str,
    reason: Option<&'static str>,
    liquidity: LiquidityKind,
    liquidity_timestamp: Option<u64>,
}

impl BookPolicy {
    pub(crate) fn base(base: &str, source: &'static str) -> Self {
        Self {
            unit: AmountUnit::Base,
            base: base.into(),
            quote: String::new(),
            rule: Some(BaseRule::Identity),
            source,
            reason: None,
            liquidity: LiquidityKind::OrderBook,
            liquidity_timestamp: None,
        }
    }

    pub(crate) fn unavailable(
        unit: AmountUnit,
        base: &str,
        source: &'static str,
        reason: &'static str,
    ) -> Self {
        Self {
            unit,
            rule: None,
            reason: Some(reason),
            ..Self::base(base, source)
        }
    }

    pub(crate) fn contracts(
        base: &str,
        quote: &str,
        size: Option<f64>,
        inverse: bool,
        source: &'static str,
    ) -> Self {
        let mut policy = Self::unavailable(
            AmountUnit::Contracts,
            base,
            source,
            "missing_or_invalid_contract_size",
        );
        policy.quote = quote.into();
        if let Some(size) = size.filter(|s| s.is_finite() && *s > 0.0) {
            policy.rule = Some(BaseRule::Contract { size, inverse });
            policy.reason = None;
        }
        policy
    }

    pub(crate) fn with_liquidity(mut self, kind: LiquidityKind, timestamp: u64) -> Self {
        self.liquidity = kind;
        self.liquidity_timestamp = Some(timestamp);
        self
    }

    pub(crate) fn same_definition(&self, other: &Self) -> bool {
        let mut other = other.clone();
        other.liquidity_timestamp = self.liquidity_timestamp;
        self == &other
    }

    pub(crate) fn validate_mode(&self, mode: BookAmountMode) -> Result<(), ExchangeError> {
        if mode == BookAmountMode::Base && self.rule.is_none() {
            return Err(ExchangeError::UnsupportedFeature(format!(
                "AMOUNT_NORMALIZATION_UNAVAILABLE: {} ({})",
                self.reason.unwrap_or("unqualified_amount_unit"),
                self.source,
            )));
        }
        Ok(())
    }

    fn metadata(&self, now: u64) -> BookMetadata {
        // RFQ status can change without changing the instrument's identity.
        // Do not keep advertising an old non-RFQ observation as executable depth.
        let liquidity_kind = if self
            .liquidity_timestamp
            .is_some_and(|ts| now < ts || now.saturating_sub(ts) >= 30_000)
        {
            LiquidityKind::Unknown
        } else {
            self.liquidity
        };
        BookMetadata {
            amount_unit: self.unit,
            amount_currency: match self.unit {
                AmountUnit::Base => Some(self.base.clone()),
                AmountUnit::Quote => Some(self.quote.clone()),
                _ => None,
            },
            source_amount_unit: self.unit,
            amount_conversion: AmountConversion {
                method: if self.unit == AmountUnit::Unknown {
                    AmountConversionMethod::Unavailable
                } else {
                    AmountConversionMethod::Identity
                },
                source: Some(self.source),
                reason: self.reason,
                ..Default::default()
            },
            liquidity_kind,
            liquidity_timestamp: self.liquidity_timestamp,
        }
    }
}

impl CcxtOrderBook {
    pub(crate) fn with_policy(mut self, policy: BookPolicy) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(u64::MAX, |d| d.as_millis() as u64);
        self.metadata = policy.metadata(now);
        self.amount_policy = Some(policy);
        self.base_view = Default::default();
        self
    }

    pub(crate) fn into_amount_mode(mut self, mode: BookAmountMode) -> Result<Self, ExchangeError> {
        if mode == BookAmountMode::Native || self.metadata.amount_unit == AmountUnit::Base {
            return Ok(self);
        }
        let policy = self.amount_policy.as_ref().ok_or_else(|| {
            ExchangeError::UnsupportedFeature(
                "AMOUNT_NORMALIZATION_UNAVAILABLE: unclassified_book".into(),
            )
        })?;
        policy.validate_mode(mode)?;
        let rule = policy.rule.as_ref().expect("validated conversion");
        let BaseRule::Contract { size, inverse } = rule else {
            return Err(ExchangeError::UpstreamData(
                "inconsistent book amount policy".into(),
            ));
        };
        for (price, amount) in self.bids.iter_mut().chain(self.asks.iter_mut()) {
            if !price.is_finite() || *price <= 0.0 || !amount.is_finite() || *amount < 0.0 {
                return Err(ExchangeError::UpstreamData(
                    "invalid book level for normalization".into(),
                ));
            }
            let base = if *inverse {
                *amount * *size / *price
            } else {
                *amount * *size
            };
            if !base.is_finite() || (base == 0.0 && *amount != 0.0) {
                return Err(ExchangeError::UpstreamData(
                    "book amount normalization overflow or underflow".into(),
                ));
            }
            *amount = base;
        }
        self.metadata.amount_unit = AmountUnit::Base;
        self.metadata.amount_currency = Some(policy.base.clone());
        self.metadata.amount_conversion = AmountConversion {
            method: if *inverse {
                AmountConversionMethod::MultiplyDivideByPrice
            } else {
                AmountConversionMethod::Multiply
            },
            contract_size_applied: Some(*size),
            contract_size_currency: Some(
                if *inverse {
                    &policy.quote
                } else {
                    &policy.base
                }
                .clone(),
            ),
            source: Some(policy.source),
            reason: None,
        };
        Ok(self)
    }

    pub(crate) fn amount_view(
        self: &Arc<Self>,
        mode: BookAmountMode,
    ) -> Result<Arc<Self>, ExchangeError> {
        if mode == BookAmountMode::Native || self.metadata.amount_unit == AmountUnit::Base {
            return Ok(Arc::clone(self));
        }
        self.base_view
            .get_or_init(|| {
                // Build a fresh owned view, without cloning a cached view or ever
                // mutating the native publication. OnceLock also shares failures.
                let view = Self {
                    bids: self.bids.clone(),
                    asks: self.asks.clone(),
                    datetime: self.datetime.clone(),
                    timestamp: self.timestamp,
                    nonce: self.nonce,
                    symbol: self.symbol.clone(),
                    metadata: self.metadata.clone(),
                    amount_policy: self.amount_policy.clone(),
                    base_view: Default::default(),
                };
                view.into_amount_mode(mode).map(Arc::new)
            })
            .clone()
    }
}

#[cfg(test)]
mod tests;
