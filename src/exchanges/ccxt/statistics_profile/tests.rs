use serde_json::json;

use super::{acquisition_params, live_statistics, selected_open_interest, Venue};
use crate::{
    config::Config,
    exchanges::ccxt::{CcxtExchange, CcxtService},
    exchanges::traits::MarketStatsSource,
};

#[test]
fn selected_oi_and_live_qualification_are_independent() {
    for venue in Venue::ALL {
        assert_eq!(
            selected_open_interest(venue).is_some(),
            venue == Venue::Binance
        );
        assert_eq!(live_statistics(venue).is_some(), venue == Venue::Lighter);
    }
    for venue in [Venue::Binance, Venue::Bybit] {
        let params = super::normalize_params(venue, &json!({"type":"future"})).unwrap();
        let perpetual = super::normalize_params(venue, &json!({})).unwrap();
        assert_eq!(
            acquisition_params(venue, &params),
            acquisition_params(venue, &perpetual)
        );
    }
    let spot = super::normalize_params(Venue::Lighter, &json!({"type":"spot"})).unwrap();
    assert_eq!(acquisition_params(Venue::Lighter, &spot), json!({}));
}

#[tokio::test]
async fn unqualified_venues_never_prepare_or_subscribe_statistics() {
    // A closed owner makes accidental catalog/preparation I/O fail, rather than
    // letting an erroneous live subscription silently succeed with a mock.
    let config = Config {
        host: "127.0.0.1".into(),
        port: 0,
        request_timeout_ms: 100,
        hyperliquid_base_url: "http://127.0.0.1:1".into(),
        extended_rest_base_url: "http://127.0.0.1:1/api/v1".into(),
        extended_ws_url: "ws://127.0.0.1:1".into(),
        lighter_rest_base_url: "http://127.0.0.1:1".into(),
        lighter_ws_url: "ws://127.0.0.1:1".into(),
        binance_base_url: "http://127.0.0.1:1".into(),
        bybit_base_url: "http://127.0.0.1:1".into(),
        aster_base_url: "http://127.0.0.1:1".into(),
        apex_rest_base_url: "http://127.0.0.1:1/api".into(),
        apex_ws_url: "ws://127.0.0.1:1".into(),
    };
    let service = CcxtService::start(&config).unwrap();
    service.shutdown().await.unwrap();
    for venue in Venue::ALL {
        let exchange = CcxtExchange::new(venue, service.clone());
        let result = exchange.subscribe_market_stats().await;
        if venue == Venue::Lighter {
            assert!(result.is_err(), "qualified feed must go through the owner");
        } else {
            assert!(
                result.unwrap().is_none(),
                "{} started live setup",
                venue.public_id()
            );
        }
    }
}
