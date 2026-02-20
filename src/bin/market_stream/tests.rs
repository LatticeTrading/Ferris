use super::{
    apply_bybit_orderbook_event, build_binance_ohlcv_ws_endpoint,
    build_binance_orderbook_ws_endpoint, build_bybit_ohlcv_ws_endpoint,
    build_bybit_orderbook_ws_endpoint, build_level_rows, build_ohlcv_chart_lines,
    compute_book_stats, format_depth_bar, infer_hyperliquid_coin_from_symbol,
    normalize_book_levels, parse_args, parse_binance_ohlcv_message,
    parse_binance_orderbook_message, parse_binance_trades_message, parse_bybit_ohlcv_message,
    parse_bybit_orderbook_message, parse_bybit_trades_message, parse_hyperliquid_orderbook_message,
    parse_hyperliquid_trades_message, resolve_binance_ws_symbol, resolve_bybit_ws_symbol,
    to_binance_ws_depth_levels, to_binance_ws_interval, to_bybit_ws_depth_levels,
    to_bybit_ws_interval, trade_key, BybitOrderBookEventType, Config, Mode, OhlcvRow,
    OrderBookSnapshot, ParseResult, TradeDeduper, TradeRow,
};

fn parse_run(args: &[&str]) -> Config {
    let args = args
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    match parse_args(&args).expect("parse should succeed") {
        ParseResult::Run(config) => config,
        ParseResult::Help => panic!("expected ParseResult::Run"),
    }
}

#[test]
fn parse_args_reads_trades_flags() {
    let config = parse_run(&[
        "trades",
        "--symbol",
        "ETH/USDC:USDC",
        "--limit",
        "40",
        "--iterations",
        "3",
    ]);

    assert_eq!(config.mode, Mode::Trades);
    assert_eq!(config.symbol, "ETH/USDC:USDC");
    assert_eq!(config.trade_limit, 40);
    assert_eq!(config.iterations, Some(3));
}

#[test]
fn parse_args_defaults_to_ws_mode() {
    let config = parse_run(&["trades"]);
    assert_eq!(config.mode, Mode::Trades);
}

#[test]
fn parse_args_reads_ohlcv_flags() {
    let config = parse_run(&[
        "ohlcv",
        "--exchange",
        "bybit",
        "--coin",
        "btc",
        "--timeframe",
        "5m",
        "--limit",
        "80",
        "--chart-height",
        "18",
    ]);

    assert_eq!(config.mode, Mode::Ohlcv);
    assert_eq!(config.exchange, "bybit");
    assert_eq!(config.coin.as_deref(), Some("btc"));
    assert_eq!(config.ohlcv_timeframe, "5m");
    assert_eq!(config.ohlcv_limit, 80);
    assert_eq!(config.ohlcv_chart_height, 18);
}

#[test]
fn parse_args_rejects_polling_flags() {
    let args = vec![
        "trades".to_string(),
        "--transport".to_string(),
        "poll".to_string(),
    ];
    let err = parse_args(&args).expect_err("poll transport flag should be rejected");
    assert!(err.contains("websocket-only"));
}

#[test]
fn parse_args_caps_orderbook_levels() {
    let config = parse_run(&["orderbook", "--levels", "999"]);
    assert_eq!(config.mode, Mode::OrderBook);
    assert_eq!(config.orderbook_levels, 20);
}

#[test]
fn parse_args_enforces_minimum_orderbook_levels() {
    let config = parse_run(&["orderbook", "--levels", "1"]);
    assert_eq!(config.orderbook_levels, 10);
}

#[test]
fn build_ohlcv_chart_lines_draws_candle_markers() {
    let candles = vec![
        OhlcvRow(1700000000000, 100.0, 102.0, 99.0, 101.5, 3.0),
        OhlcvRow(1700000060000, 101.5, 103.0, 100.0, 100.5, 2.5),
        OhlcvRow(1700000120000, 100.5, 101.0, 100.0, 100.5, 1.8),
    ];

    let lines = build_ohlcv_chart_lines(&candles, 10);
    let joined = lines.join("\n");

    assert!(joined.contains('#'));
    assert!(joined.contains('*'));
    assert!(joined.contains('='));
    assert!(joined.contains('|'));
}

#[test]
fn deduper_evicts_old_keys_when_capacity_hit() {
    let mut deduper = TradeDeduper::new(3);
    assert!(deduper.insert("a".to_string()));
    assert!(deduper.insert("b".to_string()));
    assert!(deduper.insert("c".to_string()));
    assert!(!deduper.insert("a".to_string()));

    assert!(deduper.insert("d".to_string()));
    assert!(deduper.insert("a".to_string()));
}

#[test]
fn trade_key_prefers_id_when_present() {
    let trade = TradeRow {
        id: Some("123".to_string()),
        timestamp: Some(10),
        datetime: None,
        side: Some("buy".to_string()),
        price: Some(1.1),
        amount: Some(0.2),
        cost: Some(0.22),
    };
    assert_eq!(trade_key(&trade), "id:123");
}

#[test]
fn compute_book_stats_uses_best_levels() {
    let book = OrderBookSnapshot {
        asks: vec![(101.0, 2.0), (102.0, 3.0)],
        bids: vec![(100.0, 2.5), (99.0, 4.0)],
        datetime: None,
        timestamp: None,
        symbol: Some("BTC/USDC:USDC".to_string()),
    };

    let stats = compute_book_stats(&book);
    assert_eq!(stats.best_bid, Some(100.0));
    assert_eq!(stats.best_ask, Some(101.0));
    assert_eq!(stats.spread, Some(1.0));
    assert_eq!(stats.mid, Some(100.5));
    assert_eq!(stats.spread_bps, Some((1.0 / 100.5) * 10_000.0));
}

#[test]
fn build_level_rows_tracks_cumulative_size() {
    let rows = build_level_rows(&[(10.0, 1.5), (11.0, 2.0), (12.0, 3.0)], 2);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].cumulative_size, 1.5);
    assert_eq!(rows[1].cumulative_size, 3.5);
}

#[test]
fn format_depth_bar_scales_to_max() {
    assert_eq!(format_depth_bar(0.0, 10.0, 5), "-----");
    assert_eq!(format_depth_bar(10.0, 10.0, 5), "#####");
    assert_eq!(format_depth_bar(5.0, 10.0, 5), "###--");
}

#[test]
fn normalize_book_levels_orders_best_first() {
    let book = OrderBookSnapshot {
        asks: vec![(101.5, 1.0), (100.5, 1.0), (102.0, 1.0)],
        bids: vec![(99.0, 1.0), (100.0, 1.0), (98.5, 1.0)],
        datetime: None,
        timestamp: None,
        symbol: None,
    };

    let (asks, bids) = normalize_book_levels(&book);
    assert_eq!(
        asks.iter().map(|(price, _)| *price).collect::<Vec<_>>(),
        vec![100.5, 101.5, 102.0]
    );
    assert_eq!(
        bids.iter().map(|(price, _)| *price).collect::<Vec<_>>(),
        vec![100.0, 99.0, 98.5]
    );
}

#[test]
fn parse_args_reads_ws_options() {
    let config = parse_run(&[
        "orderbook",
        "--ws-url",
        "wss://api.hyperliquid.xyz/ws",
        "--coin",
        "BTC",
    ]);

    assert_eq!(config.ws_url, "wss://api.hyperliquid.xyz/ws");
    assert_eq!(config.coin.as_deref(), Some("BTC"));
}

#[test]
fn parse_binance_ohlcv_message_maps_candle() {
    let payload = r#"{
        "e": "kline",
        "E": 1700000000100,
        "s": "BTCUSDT",
        "k": {
            "t": 1700000000000,
            "o": "100.0",
            "h": "105.0",
            "l": "99.0",
            "c": "102.5",
            "v": "12.34"
        }
    }"#;

    let parsed = parse_binance_ohlcv_message(payload);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].0, 1700000000000);
    assert_eq!(parsed[0].1, 100.0);
    assert_eq!(parsed[0].2, 105.0);
    assert_eq!(parsed[0].3, 99.0);
    assert_eq!(parsed[0].4, 102.5);
    assert_eq!(parsed[0].5, 12.34);
}

#[test]
fn parse_bybit_ohlcv_message_maps_candle() {
    let payload = r#"{
        "topic": "kline.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000100,
        "data": [
            {
                "start": 1700000000000,
                "end": 1700000059999,
                "interval": "1",
                "open": "100.0",
                "high": "105.0",
                "low": "99.0",
                "close": "102.5",
                "volume": "12.34",
                "confirm": false,
                "timestamp": 1700000000100
            }
        ]
    }"#;

    let parsed = parse_bybit_ohlcv_message(payload);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].0, 1700000000000);
    assert_eq!(parsed[0].1, 100.0);
    assert_eq!(parsed[0].2, 105.0);
    assert_eq!(parsed[0].3, 99.0);
    assert_eq!(parsed[0].4, 102.5);
    assert_eq!(parsed[0].5, 12.34);
}

#[test]
fn parse_hyperliquid_orderbook_message_maps_levels() {
    let payload = r#"{
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": 1700000000000,
            "levels": [
                [{"px":"100.0","sz":"1.5"},{"px":"99.0","sz":"2.0"}],
                [{"px":"101.0","sz":"1.0"},{"px":"102.0","sz":"3.0"}]
            ]
        }
    }"#;

    let parsed = parse_hyperliquid_orderbook_message(payload, "BTC/USDC:USDC", 10)
        .expect("orderbook message should parse");

    assert_eq!(parsed.bids[0], (100.0, 1.5));
    assert_eq!(parsed.asks[0], (101.0, 1.0));
    assert_eq!(parsed.symbol.as_deref(), Some("BTC/USDC:USDC"));
}

#[test]
fn parse_hyperliquid_trades_message_maps_trade_rows() {
    let payload = r#"{
        "channel": "trades",
        "data": [
            {"tid": 1, "time": 1700000000000, "side": "B", "px": "100.5", "sz": "0.2"}
        ]
    }"#;

    let parsed = parse_hyperliquid_trades_message(payload);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].id.as_deref(), Some("1"));
    assert_eq!(parsed[0].side.as_deref(), Some("buy"));
    assert_eq!(parsed[0].price, Some(100.5));
    assert_eq!(parsed[0].amount, Some(0.2));
}

#[test]
fn infer_hyperliquid_coin_from_symbol_uses_base_asset() {
    assert_eq!(
        infer_hyperliquid_coin_from_symbol("BTC/USDC:USDC"),
        Some("BTC".to_string())
    );
    assert_eq!(
        infer_hyperliquid_coin_from_symbol("@123"),
        Some("@123".to_string())
    );
}

#[test]
fn resolve_binance_ws_symbol_uses_coin_override() {
    let config = parse_run(&["trades", "--exchange", "binance", "--coin", "btc"]);
    assert_eq!(resolve_binance_ws_symbol(&config).unwrap(), "BTCUSDT");
}

#[test]
fn resolve_bybit_ws_symbol_uses_coin_override() {
    let config = parse_run(&["trades", "--exchange", "bybit", "--coin", "btc"]);
    assert_eq!(resolve_bybit_ws_symbol(&config).unwrap(), "BTCUSDT");

    let explicit_pair = parse_run(&["trades", "--exchange", "bybit", "--coin", "BTCUSDC"]);
    assert_eq!(resolve_bybit_ws_symbol(&explicit_pair).unwrap(), "BTCUSDC");
}

#[test]
fn parse_binance_trades_message_maps_trade_row() {
    let payload = r#"{
        "e": "trade",
        "E": 1700000000001,
        "T": 1700000000000,
        "s": "BTCUSDT",
        "t": 123456,
        "p": "100.5",
        "q": "0.25",
        "m": true
    }"#;

    let parsed = parse_binance_trades_message(payload);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].id.as_deref(), Some("123456"));
    assert_eq!(parsed[0].timestamp, Some(1700000000000));
    assert_eq!(parsed[0].side.as_deref(), Some("sell"));
    assert_eq!(parsed[0].price, Some(100.5));
    assert_eq!(parsed[0].amount, Some(0.25));
    assert_eq!(parsed[0].cost, Some(25.125));
}

#[test]
fn parse_bybit_trades_message_maps_trade_row() {
    let payload = r#"{
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000100,
        "data": [
            {
                "T": 1700000000000,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.25",
                "p": "100.5",
                "i": "abc-123",
                "BT": false,
                "RPI": false
            }
        ]
    }"#;

    let parsed = parse_bybit_trades_message(payload);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].id.as_deref(), Some("abc-123"));
    assert_eq!(parsed[0].timestamp, Some(1700000000000));
    assert_eq!(parsed[0].side.as_deref(), Some("buy"));
    assert_eq!(parsed[0].price, Some(100.5));
    assert_eq!(parsed[0].amount, Some(0.25));
    assert_eq!(parsed[0].cost, Some(25.125));
}

#[test]
fn parse_bybit_orderbook_message_maps_snapshot() {
    let payload = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000100,
        "data": {
            "s": "BTCUSDT",
            "b": [["100.0", "1.5"], ["99.5", "2.0"]],
            "a": [["100.5", "1.0"], ["101.0", "3.0"]],
            "u": 12345,
            "seq": 42
        }
    }"#;

    let event = parse_bybit_orderbook_message(payload).expect("message should parse");
    assert_eq!(event.event_type, BybitOrderBookEventType::Snapshot);
    assert_eq!(event.timestamp, Some(1700000000100));
    assert_eq!(event.bids, vec![(100.0, 1.5), (99.5, 2.0)]);
    assert_eq!(event.asks, vec![(100.5, 1.0), (101.0, 3.0)]);
}

#[test]
fn apply_bybit_orderbook_event_applies_delta_updates() {
    let snapshot_payload = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000100,
        "data": {
            "s": "BTCUSDT",
            "b": [["100.0", "1.5"], ["99.5", "2.0"]],
            "a": [["100.5", "1.0"], ["101.0", "3.0"]],
            "u": 12345
        }
    }"#;

    let delta_payload = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1700000000200,
        "data": {
            "s": "BTCUSDT",
            "b": [["100.0", "0.75"], ["98.0", "3.0"], ["99.5", "0"]],
            "a": [["100.5", "0"], ["100.6", "1.2"]],
            "u": 12346
        }
    }"#;

    let snapshot = parse_bybit_orderbook_message(snapshot_payload).expect("snapshot should parse");
    let delta = parse_bybit_orderbook_message(delta_payload).expect("delta should parse");

    let mut state = None;
    assert!(apply_bybit_orderbook_event(
        &mut state,
        snapshot,
        10,
        "BTC/USDT:USDT"
    ));
    assert!(apply_bybit_orderbook_event(
        &mut state,
        delta,
        10,
        "BTC/USDT:USDT"
    ));

    let book = state.expect("state should contain orderbook");
    assert_eq!(book.symbol.as_deref(), Some("BTC/USDT:USDT"));
    assert_eq!(book.timestamp, Some(1700000000200));
    assert_eq!(book.bids, vec![(100.0, 0.75), (98.0, 3.0)]);
    assert_eq!(book.asks, vec![(100.6, 1.2), (101.0, 3.0)]);
}

#[test]
fn parse_binance_orderbook_message_maps_levels() {
    let payload = r#"{
        "e": "depthUpdate",
        "E": 1700000000001,
        "T": 1700000000000,
        "s": "BTCUSDT",
        "b": [["100.0", "1.5"], ["99.5", "2.0"]],
        "a": [["100.5", "1.0"], ["101.0", "3.0"]]
    }"#;

    let parsed = parse_binance_orderbook_message(payload, "BTC/USDT:USDT", 10)
        .expect("orderbook message should parse");
    assert_eq!(parsed.timestamp, Some(1700000000000));
    assert_eq!(parsed.bids[0], (100.0, 1.5));
    assert_eq!(parsed.asks[0], (100.5, 1.0));
    assert_eq!(parsed.symbol.as_deref(), Some("BTC/USDT:USDT"));
}

#[test]
fn build_binance_orderbook_ws_endpoint_applies_depth_bucket() {
    let config = parse_run(&[
        "orderbook",
        "--exchange",
        "binance",
        "--symbol",
        "BTC/USDT:USDT",
        "--levels",
        "12",
    ]);

    let endpoint = build_binance_orderbook_ws_endpoint(&config).expect("endpoint should build");
    assert!(endpoint.ends_with("/btcusdt@depth20@100ms"));
    assert_eq!(to_binance_ws_depth_levels(10), 10);
    assert_eq!(to_binance_ws_depth_levels(20), 20);
}

#[test]
fn build_binance_ohlcv_ws_endpoint_uses_requested_interval() {
    let config = parse_run(&[
        "ohlcv",
        "--exchange",
        "binance",
        "--symbol",
        "BTC/USDT:USDT",
        "--timeframe",
        "5m",
    ]);

    let endpoint = build_binance_ohlcv_ws_endpoint(&config).expect("endpoint should build");
    assert!(endpoint.ends_with("/btcusdt@kline_5m"));
}

#[test]
fn build_bybit_orderbook_ws_endpoint_uses_default_linear_ws() {
    let config = parse_run(&["orderbook", "--exchange", "bybit", "--coin", "BTC"]);

    let endpoint = build_bybit_orderbook_ws_endpoint(&config).expect("endpoint should build");
    assert_eq!(endpoint, "wss://stream.bybit.com/v5/public/linear");
    assert_eq!(to_bybit_ws_depth_levels(1), 1);
    assert_eq!(to_bybit_ws_depth_levels(20), 50);
    assert_eq!(to_bybit_ws_depth_levels(120), 200);
}

#[test]
fn build_bybit_ohlcv_ws_endpoint_uses_default_linear_ws() {
    let config = parse_run(&["ohlcv", "--exchange", "bybit", "--coin", "BTC"]);

    let endpoint = build_bybit_ohlcv_ws_endpoint(&config).expect("endpoint should build");
    assert_eq!(endpoint, "wss://stream.bybit.com/v5/public/linear");
}

#[test]
fn maps_ohlcv_timeframes_to_ws_intervals() {
    assert_eq!(to_binance_ws_interval("1m").unwrap(), "1m");
    assert_eq!(to_binance_ws_interval("1M").unwrap(), "1M");
    assert!(to_binance_ws_interval("10m").is_err());

    assert_eq!(to_bybit_ws_interval("1m").unwrap(), "1");
    assert_eq!(to_bybit_ws_interval("1h").unwrap(), "60");
    assert_eq!(to_bybit_ws_interval("1M").unwrap(), "M");
    assert!(to_bybit_ws_interval("2w").is_err());
}
