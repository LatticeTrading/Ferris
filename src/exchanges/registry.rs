use std::{collections::HashMap, sync::Arc};

use super::traits::MarketDataExchange;

#[derive(Default)]
pub struct ExchangeRegistry {
    exchanges: HashMap<String, Arc<dyn MarketDataExchange>>,
}

impl ExchangeRegistry {
    pub fn new() -> Self {
        Self {
            exchanges: HashMap::new(),
        }
    }

    pub fn register(&mut self, exchange: Arc<dyn MarketDataExchange>) {
        self.exchanges
            .insert(exchange.id().to_ascii_lowercase(), exchange);
    }

    pub fn get(&self, exchange_id: &str) -> Option<Arc<dyn MarketDataExchange>> {
        self.exchanges
            .get(&exchange_id.to_ascii_lowercase())
            .cloned()
    }
}
