use super::*;
use crate::{
    models::{CcxtOrderBook, UnifiedMarket, UnifiedMarketInfo},
    orderbook::BookAmountMode,
};
use serde_json::json;

fn market(product: UnifiedMarketType, inverse: bool, info: Value) -> CatalogMarket {
    CatalogMarket {
        market: UnifiedMarket {
            exchange: "test".into(),
            symbol: "BTC/USD".into(),
            base: "BTC".into(),
            quote: "USD".into(),
            market_type: product,
            active: true,
            min_order_size: Some(10.0),
            tick_size: Some(0.1),
            contract_size: Some(999.0),
            info: UnifiedMarketInfo::default(),
            identity: None,
        },
        ccxt_symbol: "BTC/USD:USD".into(),
        linear: Some(!inverse),
        inverse: Some(inverse),
        raw: json!({"contractSize":999.0,"info":info}),
    }
}
fn output(
    venue: Venue,
    m: &CatalogMarket,
    mode: BookAmountMode,
) -> Result<CcxtOrderBook, crate::exchanges::traits::ExchangeError> {
    CcxtOrderBook {
        bids: vec![(100.0, 2.0)],
        asks: vec![(200.0, 4.0)],
        ..Default::default()
    }
    .with_policy(policy(venue, m, 0))
    .into_amount_mode(mode)
}

#[test]
fn every_qualified_base_product_ignores_catalog_multipliers() {
    use UnifiedMarketType::*;
    for venue in Venue::ALL {
        let m = market(Spot, false, json!({}));
        for mode in [BookAmountMode::Native, BookAmountMode::Base] {
            assert_eq!(output(venue, &m, mode).unwrap().bids, vec![(100.0, 2.0)]);
        }
        if venue == Venue::Kucoin {
            continue;
        }
        let m = market(
            Perp,
            false,
            json!({"minOrderSize":0.001,"quote_multiplier":10000}),
        );
        let book = output(venue, &m, BookAmountMode::Base).unwrap();
        assert_eq!(book.bids[0].1, 2.0, "{venue:?}");
        assert_eq!(book.metadata.amount_unit, AmountUnit::Base);
        assert_eq!(book.metadata.amount_conversion.contract_size_applied, None);
    }
}

#[test]
fn contract_rules_use_native_provenance_and_each_level_price() {
    use UnifiedMarketType::*;
    for product in [Perp, Future] {
        for inverse in [false, true] {
            let m = market(
                product,
                inverse,
                json!({"multiplier": if inverse { -10.0 } else { 0.001 }, "isInverse":inverse}),
            );
            let native = output(Venue::Kucoin, &m, BookAmountMode::Native).unwrap();
            assert_eq!(native.metadata.amount_unit, AmountUnit::Contracts);
            assert_eq!(native.bids[0].1, 2.0);
            let base = output(Venue::Kucoin, &m, BookAmountMode::Base).unwrap();
            assert_eq!(base.bids[0].1, if inverse { 0.2 } else { 0.002 });
            assert_eq!(base.asks[0].1, if inverse { 0.2 } else { 0.004 });
        }
        let m = market(product, true, json!({"contractSize":"100"}));
        assert_eq!(
            output(Venue::Binance, &m, BookAmountMode::Base)
                .unwrap()
                .bids[0]
                .1,
            2.0
        );
    }
    let option = market(Option, false, json!({"unit":1000}));
    assert_eq!(
        output(Venue::Binance, &option, BookAmountMode::Base)
            .unwrap()
            .bids[0]
            .1,
        2000.0
    );
    assert_eq!(
        output(Venue::Bybit, &option, BookAmountMode::Base)
            .unwrap()
            .bids[0]
            .1,
        2.0
    );
    for unit in [Value::Null, json!(0), json!(-1), json!("NaN"), json!(false)] {
        assert!(output(
            Venue::Binance,
            &market(Option, false, json!({"unit":unit})),
            BookAmountMode::Base
        )
        .is_err());
    }
}

#[test]
fn ambiguous_or_missing_contract_metadata_never_uses_stock_defaults_or_minima() {
    use UnifiedMarketType::*;
    for venue in [Venue::Binance, Venue::Kucoin, Venue::Bybit] {
        let m = market(
            Perp,
            true,
            json!({"minOrderQty":"1", "lotSize":1, "isInverse":true}),
        );
        assert!(output(venue, &m, BookAmountMode::Base).is_err());
        assert!(output(venue, &m, BookAmountMode::Native).is_ok());
    }
    let mut m = market(Perp, true, json!({"multiplier":-1,"isInverse":false}));
    assert!(output(Venue::Kucoin, &m, BookAmountMode::Base).is_err());
    m.linear = Some(true);
    m.raw["info"]["isInverse"] = json!(true);
    assert!(output(Venue::Kucoin, &m, BookAmountMode::Base).is_err());
    assert!(output(
        Venue::Apex,
        &market(Option, false, json!({})),
        BookAmountMode::Base
    )
    .is_err());
}

#[test]
fn request_validation_is_book_only_and_identical_for_rest_and_live() {
    use crate::{
        exchanges::ccxt::{
            rest::{validate_params, Operation},
            stream::live_scope,
        },
        realtime::RealtimeChannel,
    };
    for venue in Venue::ALL {
        for value in [
            json!("base"),
            json!("native"),
            Value::Null,
            json!("quote"),
            json!(1),
        ] {
            let params = json!({"amountUnit":value});
            assert_eq!(
                validate_params(venue, Operation::Book, &params).is_ok(),
                live_scope(venue, RealtimeChannel::OrderBook, &params).is_ok()
            );
        }
        assert!(validate_params(venue, Operation::Trades, &json!({"amountUnit":"base"})).is_err());
        assert!(live_scope(
            venue,
            RealtimeChannel::Trades,
            &json!({"amountUnit":"base"})
        )
        .is_err());
    }
}
