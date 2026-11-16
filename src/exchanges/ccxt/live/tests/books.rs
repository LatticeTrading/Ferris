use super::*;
use crate::{
    exchanges::ccxt::{catalog::Catalog, venue::CatalogScope},
    models::CcxtOrderBook,
    orderbook::LiquidityKind,
};

fn snapshot(generation: u64, rfq: Option<bool>) -> Arc<CatalogSnapshot> {
    let market = json!({
        "id":"BTC-USD", "symbol":"BTC/USDC:USDC", "base":"BTC", "quote":"USDC", "settle":"USDC",
        "type":"swap", "swap":true, "contract":true, "linear":true, "inverse":false,
        "active":true,"precision":{},"limits":{},"info":{"isRfq":rfq,"collateralAssetName":"USD"}
    });
    Arc::new(CatalogSnapshot {
        venue: Venue::Extended,
        scope: CatalogScope::Default,
        generation,
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        catalog: Catalog::from_markets(
            Venue::Extended,
            vec![ccxt::types::Market::from_value(value(market))],
            4,
        )
        .unwrap(),
    })
}

#[test]
fn book_watch_updates_semantics_without_seeding_stock_with_the_whole_catalog() {
    let watch = Arc::new(CatalogWatch::new(snapshot(1, Some(false))));
    let spec = Arc::new(LiveSpec {
        key: "test-book".into(),
        venue: Venue::Extended,
        scope: CatalogScope::Default,
        channel: LiveChannel::Client(RealtimeChannel::OrderBook),
        symbol: "BTC/USDC:USDC".into(),
        timeframe: None,
        depth: Some(1000),
        params: json!({}),
        market: watch.load().catalog.entries()[0].raw.clone(),
        url: "ws://127.0.0.1:1".into(),
        slots: None,
        hash: "orderbook:BTC/USDC:USDC".into(),
        catalog: None,
        book_catalog: Some(watch.clone()),
        config: super::catalog::provider_config(Venue::Extended),
        candle_cache_key: None,
    });
    // A book metadata watch must not activate the statistics provider path.
    assert!(spec.catalog.is_none());
    let (feed, _receiver) = new_feed(spec.clone(), Arc::new(Notify::new()));
    let mut current = ActiveFeed::new(feed);
    let first = book_policy(&mut current, &spec).unwrap();
    assert_eq!(
        CcxtOrderBook::default()
            .with_policy(first)
            .metadata
            .liquidity_kind,
        LiquidityKind::OrderBook
    );
    watch.publish(snapshot(2, Some(false)));
    assert!(book_policy(&mut current, &spec).is_ok());
    assert_eq!(current.catalog.as_ref().unwrap().generation, 2);
    watch.publish(snapshot(3, Some(true)));
    assert!(
        book_policy(&mut current, &spec).is_err(),
        "must not mix retained CLOB levels with RFQ quotes"
    );
    current.invalidate(); // the production owner reconnects and requests a fresh snapshot
    let rfq = book_policy(&mut current, &spec).unwrap();
    assert_eq!(
        CcxtOrderBook::default()
            .with_policy(rfq)
            .metadata
            .liquidity_kind,
        LiquidityKind::Indicative
    );
    watch.publish(snapshot(4, None));
    assert!(book_policy(&mut current, &spec).is_err());
    current.invalidate();
    let unknown = book_policy(&mut current, &spec).unwrap();
    assert_eq!(
        CcxtOrderBook::default()
            .with_policy(unknown)
            .metadata
            .liquidity_kind,
        LiquidityKind::Unknown
    );
}
