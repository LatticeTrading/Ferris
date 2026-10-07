use super::*;
use crate::{
    config::Config,
    exchanges::ccxt::{
        catalog::Catalog,
        venue::{CatalogScope, ProviderConfig},
    },
};

fn snapshot(generation: u64, ids: &[u64]) -> Arc<CatalogSnapshot> {
    let markets = ids
        .iter()
        .map(|id| {
            ccxt::types::Market::from_value(value(json!({
                "id": id.to_string(), "symbol": format!("M{id}/USDC:USDC"),
                "base": format!("M{id}"), "quote":"USDC", "settle":"USDC",
                "type":"swap", "spot":false, "swap":true, "future":false, "option":false,
                "active":true, "contract":true, "linear":true, "inverse":false,
                "precision":{}, "limits":{}, "info":{}
            })))
        })
        .collect();
    Arc::new(CatalogSnapshot {
        venue: Venue::Lighter,
        scope: CatalogScope::Default,
        catalog: Catalog::from_markets(Venue::Lighter, markets, 4).unwrap(),
        timestamp: 0,
        generation,
    })
}

#[tokio::test]
async fn maintained_feed_refreshes_identity_without_freshening_held_rows() {
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
    let config = ProviderConfig::new(Venue::Lighter, &config).unwrap();
    let watch = Arc::new(CatalogWatch::new(snapshot(1, &[0])));
    let prepared = feed()
        .prepare(watch.load(), &config, watch.clone())
        .await
        .unwrap();
    let spec = prepared.spec;
    let mut provider = LiveProvider::new(&spec, None);
    let (feed, _receiver) = new_feed(spec.clone(), Arc::new(Notify::new()));
    let mut current = ActiveFeed::new(feed);
    refresh_statistics_catalog(&mut provider, &mut current, &spec);
    let LiveProvider::Lighter(core) = &mut provider else {
        unreachable!()
    };
    core.tickers = value(json!({
        "M0/USDC:USDC":{"info":{"market_id":0,"mark_price":"1"}},
        "M1/USDC:USDC":{"info":{"market_id":1,"mark_price":"2"}}
    }));
    assert_eq!(
        statistics_update(&provider, &mut current, &spec)
            .unwrap()
            .len(),
        1
    );
    assert!(statistics_update(&provider, &mut current, &spec)
        .unwrap()
        .is_empty());
    watch.publish(snapshot(2, &[0, 1]));
    refresh_statistics_catalog(&mut provider, &mut current, &spec);
    let rows = statistics_update(&provider, &mut current, &spec).unwrap();
    assert_eq!(rows.len(), 1, "catalog refresh replayed the old row");
    assert_eq!(rows[0].market_id, r#"["lighterxyz","perp",null,null,"1"]"#);
    current.invalidate();
    refresh_statistics_catalog(&mut provider, &mut current, &spec);
    assert_eq!(
        statistics_update(&provider, &mut current, &spec)
            .unwrap()
            .len(),
        2,
        "new epoch must clear held row marks"
    );
    provider.clear_feed(&spec);
    assert!(provider.cache(&spec).as_map().unwrap().is_empty());
}
