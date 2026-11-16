//! Qualification of stock book units. Catalog `contractSize` is deliberately
//! NOT a generic multiplier: several stock market parsers derive it from order
//! minimums or quote scaling, and book fields can already be base quantities.

use serde_json::Value;

use super::{CatalogMarket, Venue};
use crate::{
    models::UnifiedMarketType,
    orderbook::{AmountUnit, BookPolicy, LiquidityKind},
};

pub(super) fn policy(venue: Venue, market: &CatalogMarket, observed_at: u64) -> BookPolicy {
    use UnifiedMarketType::{Future, Option as OptionProduct, Perp, Spot};
    let base = market.market.base.as_str();
    let quote = market.market.quote.as_str();
    let info = &market.raw["info"];
    let product = market.market.market_type;
    let linear = market.linear == Some(true) && market.inverse != Some(true);
    let inverse = market.inverse == Some(true) && market.linear != Some(true);
    let unknown = || {
        BookPolicy::unavailable(
            AmountUnit::Unknown,
            base,
            "ccxt:book",
            "unqualified_product_or_contract_type",
        )
    };
    let result = if product == Spot {
        BookPolicy::base(base, "ccxt:spot_book")
    } else {
        match venue {
            Venue::Kucoin if matches!(product, Perp | Future) && (linear || inverse) => {
                // The native inverse multiplier is signed; magnitude is the
                // contract's quote face size, not a signed book amount.
                let multiplier = number(&info["multiplier"]).map(f64::abs);
                if info["isInverse"].as_bool() != Some(inverse) {
                    BookPolicy::unavailable(
                        AmountUnit::Contracts,
                        base,
                        "kucoin:info.multiplier",
                        "contradictory_contract_type",
                    )
                } else {
                    BookPolicy::contracts(
                        base,
                        quote,
                        multiplier,
                        inverse,
                        "kucoin:info.multiplier",
                    )
                }
            }
            Venue::Binance if product == OptionProduct => BookPolicy::contracts(
                base,
                quote,
                number(&info["unit"]),
                false,
                "binance:info.unit",
            ),
            Venue::Binance if matches!(product, Perp | Future) && inverse => BookPolicy::contracts(
                base,
                quote,
                number(&info["contractSize"]),
                true,
                "binance:info.contractSize",
            ),
            Venue::Binance | Venue::Bybit if matches!(product, Perp | Future) && linear => {
                BookPolicy::base(base, "ccxt:linear_base_quantity")
            }
            Venue::Bybit if product == OptionProduct => {
                BookPolicy::base(base, "bybit:option_underlying_quantity")
            }
            Venue::Bybit if matches!(product, Perp | Future) && inverse => {
                // Do not assume minOrderQty (stock contractSize) proves USD face
                // size, even when it happens to equal one on today's listings.
                BookPolicy::unavailable(
                    AmountUnit::Contracts,
                    base,
                    "bybit:inverse_book",
                    "unverified_inverse_contract_face_size",
                )
            }
            Venue::Bitfinex
            | Venue::Hyperliquid
            | Venue::Aster
            | Venue::Lighter
            | Venue::Apex
            | Venue::Extended
            | Venue::Nado
                if product == Perp && linear =>
            {
                BookPolicy::base(base, "ccxt:linear_base_quantity")
            }
            _ => unknown(),
        }
    };
    if venue == Venue::Extended {
        result.with_liquidity(
            match info["isRfq"].as_bool() {
                Some(true) => LiquidityKind::Indicative,
                Some(false) => LiquidityKind::OrderBook,
                None => LiquidityKind::Unknown,
            },
            observed_at,
        )
    } else {
        result
    }
}

fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|v| v.is_finite())
}

#[cfg(test)]
mod tests;
