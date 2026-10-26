use crate::models::{
    CapabilityReason, DataCapability, ExchangeDataCapabilities, MarketScope, RealtimeCapabilities,
    RealtimeCapability, RealtimeMode, UnifiedMarketType,
};

const POLL_INTERVAL_MS: u64 = 30_000;

pub(super) fn for_venue(
    products: &'static [UnifiedMarketType],
    trades: bool,
    orderbook: bool,
    ohlcv: bool,
    marketstats_mode: RealtimeMode,
) -> ExchangeDataCapabilities {
    realtime(
        snapshots(products),
        products,
        trades,
        orderbook,
        ohlcv,
        marketstats_mode,
    )
}

pub(super) fn snapshots(products: &'static [UnifiedMarketType]) -> ExchangeDataCapabilities {
    ExchangeDataCapabilities {
        fetch_markets: DataCapability::supported(products.to_vec(), MarketScope::Catalog),
        fetch_tickers: DataCapability::unsupported(
            CapabilityReason::VenueDataUnavailable,
            "fetchTickers is not exposed by the native CCXT facade",
        ),
        fetch_trades: DataCapability::supported(products.to_vec(), MarketScope::MarketSpecific),
        fetch_ohlcv: DataCapability::supported(products.to_vec(), MarketScope::MarketSpecific),
        fetch_order_book: DataCapability::supported(products.to_vec(), MarketScope::MarketSpecific),
        fetch_market_stats: DataCapability::supported(products.to_vec(), MarketScope::AllMarkets),
        realtime: RealtimeCapabilities {
            trades: RealtimeCapability::unsupported(
                CapabilityReason::UpstreamWebsocketUnavailable,
                "realtime trades are not available from this adapter",
            ),
            orderbook: RealtimeCapability::unsupported(
                CapabilityReason::UpstreamWebsocketUnavailable,
                "realtime order books are not available from this adapter",
            ),
            ohlcv: RealtimeCapability::unsupported(
                CapabilityReason::UpstreamWebsocketUnavailable,
                "realtime OHLCV is not available from this adapter",
            ),
            marketstats: RealtimeCapability::supported(
                products.to_vec(),
                MarketScope::AllMarkets,
                RealtimeMode::Polling,
                Some(POLL_INTERVAL_MS),
            ),
        },
    }
}

pub(super) fn realtime(
    mut capabilities: ExchangeDataCapabilities,
    products: &'static [UnifiedMarketType],
    trades: bool,
    orderbook: bool,
    ohlcv: bool,
    marketstats_mode: RealtimeMode,
) -> ExchangeDataCapabilities {
    let supported = |enabled: bool, label: &'static str| {
        if enabled {
            RealtimeCapability::supported(
                products.to_vec(),
                MarketScope::MarketSpecific,
                RealtimeMode::Websocket,
                None,
            )
        } else {
            RealtimeCapability::unsupported(
                CapabilityReason::UpstreamWebsocketUnavailable,
                format!("realtime {label} are not available from this adapter"),
            )
        }
    };
    capabilities.realtime = RealtimeCapabilities {
        trades: supported(trades, "trades"),
        orderbook: supported(orderbook, "order books"),
        ohlcv: supported(ohlcv, "OHLCV"),
        marketstats: RealtimeCapability::supported(
            products.to_vec(),
            MarketScope::AllMarkets,
            marketstats_mode,
            Some(POLL_INTERVAL_MS),
        ),
    };
    capabilities
}
