use std::collections::HashMap;

use ccxt::Value;
use serde_json::Value as JsonValue;
use tokio::time::Instant;

use crate::{
    exchanges::ccxt::catalog::CatalogMarket,
    models::{UnifiedMarket, UnifiedMarketInfo, UnifiedMarketType},
};

use super::{acquisition::Acquired, Receipt};

pub(in crate::exchanges::ccxt) fn market(
    base: &str,
    quote: &str,
    product: UnifiedMarketType,
) -> UnifiedMarket {
    UnifiedMarket {
        exchange: "test".into(),
        symbol: format!("{base}/{quote}"),
        base: base.into(),
        quote: quote.into(),
        market_type: product,
        active: true,
        min_order_size: None,
        tick_size: None,
        contract_size: None,
        info: UnifiedMarketInfo {
            category: None,
            raw_symbol: None,
            exchange_symbol: None,
            is_rfq: None,
            is_off_hours: None,
        },
        identity: Some(crate::models::MarketIdentity {
            market_id: format!("test:{base}/{quote}"),
            exchange_market_id: base.to_string(),
            category: None,
            dex: None,
            contract_type: None,
            settle: None,
            settlement_asset_id: None,
        }),
    }
}

pub(in crate::exchanges::ccxt) fn entry(
    base: &str,
    quote: &str,
    product: UnifiedMarketType,
    inverse: Option<bool>,
    raw: JsonValue,
) -> CatalogMarket {
    CatalogMarket {
        market: market(base, quote, product),
        ccxt_symbol: format!("{base}/{quote}"),
        linear: inverse.map(|inverse| !inverse),
        inverse,
        raw,
    }
}

pub(in crate::exchanges::ccxt) fn sources(
    ticker: Option<&Value>,
    entry: &CatalogMarket,
    source: &'static str,
) -> Acquired {
    let mut data = Acquired::default();
    if let Some(ticker) = ticker {
        data.rows.insert(
            source,
            Ok(HashMap::from([(entry.ccxt_symbol.clone(), ticker.clone())])),
        );
        data.receipts.insert(
            source,
            Receipt {
                at: Instant::now(),
                wall: 5,
            },
        );
    }
    data
}
pub(in crate::exchanges::ccxt) fn ticker(info: JsonValue) -> Value {
    Value::from_json(&serde_json::json!({ "info": info }))
}
