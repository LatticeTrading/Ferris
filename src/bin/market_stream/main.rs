use std::env;

mod cli;
mod constants;
mod control;
mod view;
mod ws_helpers;
mod ws_runtime;

#[cfg(test)]
mod tests;

use cli::print_usage;

pub(crate) use cli::{parse_args, Config, Mode, ParseResult};
#[cfg(test)]
pub(crate) use view::{
    build_level_rows, build_ohlcv_chart_lines, compute_book_stats, format_depth_bar,
    normalize_book_levels, trade_key, OhlcvRow, OrderBookSnapshot, TradeDeduper, TradeRow,
};
#[cfg(test)]
pub(crate) use ws_helpers::{
    apply_bybit_orderbook_event, build_binance_ohlcv_ws_endpoint,
    build_binance_orderbook_ws_endpoint, build_bybit_ohlcv_ws_endpoint,
    build_bybit_orderbook_ws_endpoint, infer_hyperliquid_coin_from_symbol,
    parse_binance_ohlcv_message, parse_binance_orderbook_message, parse_binance_trades_message,
    parse_bybit_ohlcv_message, parse_bybit_orderbook_message, parse_bybit_trades_message,
    parse_hyperliquid_orderbook_message, parse_hyperliquid_trades_message,
    resolve_binance_ws_symbol, resolve_bybit_ws_symbol, to_binance_ws_depth_levels,
    to_binance_ws_interval, to_bybit_ws_depth_levels, to_bybit_ws_interval,
    BybitOrderBookEventType,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let parse_result = match parse_args(&args) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("error: {err}\n");
            print_usage();
            std::process::exit(2);
        }
    };

    match parse_result {
        ParseResult::Help => {
            print_usage();
            Ok(())
        }
        ParseResult::Run(config) => run_stream(config).await,
    }
}

async fn run_stream(config: Config) -> anyhow::Result<()> {
    let ws_url = ws_helpers::resolved_ws_base_url(&config);

    println!(
        "mode={} exchange={} symbol={} ws_url={} coin={}",
        config.mode.as_str(),
        config.exchange,
        config.symbol,
        ws_url,
        config.coin.as_deref().unwrap_or("(auto)")
    );
    if config.mode == Mode::Ohlcv {
        println!(
            "ohlcv_timeframe={} ohlcv_limit={} chart_height={}",
            config.ohlcv_timeframe, config.ohlcv_limit, config.ohlcv_chart_height
        );
    }

    if let Some(duration_secs) = config.duration_secs {
        println!("will stop after {duration_secs}s");
    }
    if let Some(iterations) = config.iterations {
        println!("will stop after {iterations} updates");
    }
    println!("press Ctrl+C to stop\n");

    match config.mode {
        Mode::Trades => ws_runtime::run_trades_stream_ws(&config).await,
        Mode::OrderBook => ws_runtime::run_orderbook_stream_ws(&config).await,
        Mode::Ohlcv => ws_runtime::run_ohlcv_stream_ws(&config).await,
    }
}
