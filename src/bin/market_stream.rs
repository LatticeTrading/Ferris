use std::{
    collections::{HashSet, VecDeque},
    env,
    fmt::Write as FmtWrite,
    io::{self, IsTerminal, Stdout, Write},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use chrono::{SecondsFormat, Utc};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    style::Print,
    terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand, QueueableCommand,
};
use futures_util::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{sleep, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8787";
const DEFAULT_EXCHANGE: &str = "hyperliquid";
const DEFAULT_SYMBOL: &str = "BTC/USDC:USDC";
const DEFAULT_POLL_MS: u64 = 800;
const DEFAULT_TIMEOUT_SECS: u64 = 20;
const DEFAULT_TRADE_LIMIT: usize = 25;
const DEFAULT_ORDERBOOK_LEVELS: usize = 10;
const MIN_ORDERBOOK_LEVELS: usize = 10;
const DEFAULT_ORDERBOOK_RENDER_MS: u64 = 33;
const DEFAULT_HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const DEFAULT_BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";
const DEFAULT_BYBIT_LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const MAX_ORDERBOOK_LEVELS: usize = 20;
const TRADE_DEDUP_CAPACITY: usize = 20_000;

const USAGE: &str = r#"Usage:
  cargo run --bin market_stream -- <mode> [options]

Modes:
  trades       Print only newly seen trades on each update
  orderbook    Render a lightweight in-terminal order book snapshot

Common options:
  --base-url <url>         Backend base URL (default: http://127.0.0.1:8787)
  --exchange <id>          Exchange id (default: hyperliquid)
  --symbol <symbol>        Market symbol (default: BTC/USDC:USDC)
  --transport <mode>       Data transport: poll|ws (default: ws)
  --ws-url <url>           Websocket URL for ws transport (default depends on exchange)
  --coin <coin>            Coin override for ws transport (Hyperliquid coin, or Binance/Bybit base asset)
  --poll-ms <ms>           Poll interval in milliseconds (default: 800)
  --timeout-secs <secs>    Per-request timeout in seconds (default: 20)
  --duration-secs <secs>   Stop after this duration
  --iterations <count>     Stop after this many iterations

Mode-specific options:
  trades:
    --limit <count>        fetchTrades limit (default: 25)

  orderbook:
    --levels <count>       Depth levels to display/request (default: 10, min: 10, max: 20)
    --render-ms <ms>       UI render interval in milliseconds (default: 33)

Examples:
  cargo run --bin market_stream -- trades --symbol BTC/USDC:USDC --poll-ms 500
  cargo run --bin market_stream -- orderbook --levels 12 --poll-ms 900
  cargo run --bin market_stream -- orderbook --transport ws --coin BTC --render-ms 16
  cargo run --bin market_stream -- orderbook --exchange binance --transport ws --coin BTC
  cargo run --bin market_stream -- orderbook --exchange bybit --transport ws --coin BTC
  cargo run --bin market_stream -- trades --exchange bybit --transport ws --coin BTC
  cargo run --bin market_stream -- trades --duration-secs 20
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Trades,
    OrderBook,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::OrderBook => "orderbook",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Poll,
    Ws,
}

impl Transport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::Ws => "ws",
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    transport: Transport,
    base_url: String,
    ws_url: String,
    coin: Option<String>,
    exchange: String,
    symbol: String,
    poll_ms: u64,
    timeout_secs: u64,
    duration_secs: Option<u64>,
    iterations: Option<u64>,
    trade_limit: usize,
    orderbook_levels: usize,
    orderbook_render_ms: u64,
}

enum ParseResult {
    Help,
    Run(Config),
}

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

fn print_usage() {
    println!("{USAGE}");
}

fn parse_args(args: &[String]) -> Result<ParseResult, String> {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(ParseResult::Help);
    }

    let mode = parse_mode(&args[0])?;

    let mut config = Config {
        mode,
        transport: Transport::Ws,
        base_url: DEFAULT_BASE_URL.to_string(),
        ws_url: DEFAULT_HYPERLIQUID_WS_URL.to_string(),
        coin: None,
        exchange: DEFAULT_EXCHANGE.to_string(),
        symbol: DEFAULT_SYMBOL.to_string(),
        poll_ms: DEFAULT_POLL_MS,
        timeout_secs: DEFAULT_TIMEOUT_SECS,
        duration_secs: None,
        iterations: None,
        trade_limit: DEFAULT_TRADE_LIMIT,
        orderbook_levels: DEFAULT_ORDERBOOK_LEVELS,
        orderbook_render_ms: DEFAULT_ORDERBOOK_RENDER_MS,
    };

    let mut index = 1usize;
    while index < args.len() {
        let flag = &args[index];
        let value = |i: &mut usize| -> Result<String, String> {
            let next = *i + 1;
            if next >= args.len() {
                return Err(format!("missing value for `{flag}`"));
            }
            *i = next;
            Ok(args[*i].clone())
        };

        match flag.as_str() {
            "--base-url" => {
                config.base_url = value(&mut index)?;
            }
            "--transport" => {
                config.transport = parse_transport(&value(&mut index)?)?;
            }
            "--ws-url" => {
                config.ws_url = value(&mut index)?;
            }
            "--coin" => {
                config.coin = Some(value(&mut index)?);
            }
            "--exchange" => {
                config.exchange = value(&mut index)?;
            }
            "--symbol" => {
                config.symbol = value(&mut index)?;
            }
            "--poll-ms" => {
                config.poll_ms = parse_u64_gt_zero("--poll-ms", &value(&mut index)?)?;
            }
            "--timeout-secs" => {
                config.timeout_secs = parse_u64_gt_zero("--timeout-secs", &value(&mut index)?)?;
            }
            "--duration-secs" => {
                config.duration_secs =
                    Some(parse_u64_gt_zero("--duration-secs", &value(&mut index)?)?);
            }
            "--iterations" => {
                config.iterations = Some(parse_u64_gt_zero("--iterations", &value(&mut index)?)?);
            }
            "--limit" => {
                config.trade_limit = parse_usize_gt_zero("--limit", &value(&mut index)?)?;
            }
            "--levels" => {
                let levels = parse_usize_gt_zero("--levels", &value(&mut index)?)?;
                config.orderbook_levels = levels.clamp(MIN_ORDERBOOK_LEVELS, MAX_ORDERBOOK_LEVELS);
            }
            "--render-ms" => {
                config.orderbook_render_ms = parse_u64_gt_zero("--render-ms", &value(&mut index)?)?;
            }
            _ => {
                return Err(format!("unknown argument `{flag}`"));
            }
        }

        index += 1;
    }

    if config.base_url.trim().is_empty() {
        return Err("`--base-url` cannot be empty".to_string());
    }
    if config.ws_url.trim().is_empty() {
        return Err("`--ws-url` cannot be empty".to_string());
    }
    if config.exchange.trim().is_empty() {
        return Err("`--exchange` cannot be empty".to_string());
    }
    if config.symbol.trim().is_empty() {
        return Err("`--symbol` cannot be empty".to_string());
    }
    if let Some(coin) = config.coin.as_deref() {
        if coin.trim().is_empty() {
            return Err("`--coin` cannot be empty".to_string());
        }
    }

    Ok(ParseResult::Run(config))
}

fn parse_mode(value: &str) -> Result<Mode, String> {
    match value {
        "trades" => Ok(Mode::Trades),
        "orderbook" => Ok(Mode::OrderBook),
        _ => Err(format!(
            "invalid mode `{value}` (expected `trades` or `orderbook`)"
        )),
    }
}

fn parse_transport(value: &str) -> Result<Transport, String> {
    match value {
        "poll" => Ok(Transport::Poll),
        "ws" => Ok(Transport::Ws),
        _ => Err(format!(
            "invalid transport `{value}` (expected `poll` or `ws`)"
        )),
    }
}

fn parse_u64_gt_zero(field: &str, value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("`{field}` expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{field}` must be greater than 0"));
    }
    Ok(parsed)
}

fn parse_usize_gt_zero(field: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("`{field}` expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{field}` must be greater than 0"));
    }
    Ok(parsed)
}

async fn run_stream(config: Config) -> anyhow::Result<()> {
    let client = BackendClient::new(config.base_url.clone(), config.timeout_secs)?;

    println!(
        "mode={} transport={} base_url={} exchange={} symbol={} poll_ms={} timeout_secs={} render_ms={}",
        config.mode.as_str(),
        config.transport.as_str(),
        config.base_url,
        config.exchange,
        config.symbol,
        config.poll_ms,
        config.timeout_secs,
        config.orderbook_render_ms,
    );
    if config.transport == Transport::Ws {
        let ws_url = resolved_ws_base_url(&config);
        println!(
            "ws_url={} coin={}",
            ws_url,
            config.coin.as_deref().unwrap_or("(auto)")
        );
    }

    if let Some(duration_secs) = config.duration_secs {
        println!("will stop after {duration_secs}s");
    }
    if let Some(iterations) = config.iterations {
        println!("will stop after {iterations} iterations");
    }
    println!("press Ctrl+C to stop\n");

    match config.mode {
        Mode::Trades => run_trades_stream(&client, &config).await,
        Mode::OrderBook => run_orderbook_stream(&client, &config).await,
    }
}

async fn run_trades_stream(client: &BackendClient, config: &Config) -> anyhow::Result<()> {
    if config.transport == Transport::Ws {
        match config.exchange.as_str() {
            "hyperliquid" | "binance" | "bybit" => return run_trades_stream_ws(config).await,
            other => {
                eprintln!(
                    "ws transport for exchange={other} is not wired yet; falling back to poll transport"
                );
            }
        }
    }

    run_trades_stream_poll(client, config).await
}

async fn run_trades_stream_poll(client: &BackendClient, config: &Config) -> anyhow::Result<()> {
    let payload = json!({
        "exchange": config.exchange,
        "symbol": config.symbol,
        "limit": config.trade_limit,
        "params": {},
    });

    let mut iteration = 0u64;
    let started_at = Instant::now();
    let mut deduper = TradeDeduper::new(TRADE_DEDUP_CAPACITY.max(config.trade_limit * 10));

    loop {
        if should_stop(started_at, iteration, config) {
            println!("stopped: reached configured stop condition");
            return Ok(());
        }

        match client
            .post_json::<Vec<TradeRow>>("/v1/fetchTrades", &payload)
            .await
        {
            Ok(rows) => {
                let mut fresh = Vec::new();
                for trade in rows.iter().rev() {
                    let key = trade_key(trade);
                    if deduper.insert(key) {
                        fresh.push(trade);
                    }
                }

                if fresh.is_empty() {
                    println!("[{}] no new trades", iteration + 1);
                } else {
                    for trade in fresh {
                        println!("{}", format_trade_line(trade));
                    }
                }
            }
            Err(err) => {
                eprintln!("[{}] fetchTrades error: {err:#}", iteration + 1);
            }
        }

        iteration += 1;
        if !wait_for_next_poll(config.poll_ms).await {
            println!("stopped: received Ctrl+C");
            return Ok(());
        }
    }
}

async fn run_orderbook_stream(client: &BackendClient, config: &Config) -> anyhow::Result<()> {
    if config.transport == Transport::Ws {
        match config.exchange.as_str() {
            "hyperliquid" | "binance" | "bybit" => return run_orderbook_stream_ws(config).await,
            other => {
                eprintln!(
                    "ws transport for exchange={other} is not wired yet; falling back to poll transport"
                );
            }
        }
    }

    run_orderbook_stream_poll(client, config).await
}

async fn run_orderbook_stream_poll(client: &BackendClient, config: &Config) -> anyhow::Result<()> {
    let payload = json!({
        "exchange": config.exchange,
        "symbol": config.symbol,
        "limit": config.orderbook_levels,
        "params": {},
    });

    let mut poll_iteration = 0u64;
    let started_at = Instant::now();
    let mut renderer = OrderBookRenderer::new()?;

    let mut state = match client
        .post_json::<OrderBookSnapshot>("/v1/fetchOrderBook", &payload)
        .await
    {
        Ok(book) => OrderBookState::Book(book),
        Err(err) => OrderBookState::Error(format!("{err:#}")),
    };
    poll_iteration += 1;

    let first_frame = build_orderbook_state_frame(
        &state,
        config.orderbook_levels,
        poll_iteration,
        started_at.elapsed(),
        &config.symbol,
    );
    renderer.render(&first_frame)?;

    let mut poll_interval = tokio::time::interval(Duration::from_millis(config.poll_ms));
    poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    poll_interval.tick().await;

    let mut render_interval =
        tokio::time::interval(Duration::from_millis(config.orderbook_render_ms));
    render_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    render_interval.tick().await;

    let stop_reason = loop {
        if should_stop(started_at, poll_iteration, config) {
            break "reached configured stop condition";
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break "received Ctrl+C";
            }
            _ = poll_interval.tick() => {
                state = match client
                    .post_json::<OrderBookSnapshot>("/v1/fetchOrderBook", &payload)
                    .await
                {
                    Ok(book) => OrderBookState::Book(book),
                    Err(err) => OrderBookState::Error(format!("{err:#}")),
                };
                poll_iteration += 1;

                let frame = build_orderbook_state_frame(
                    &state,
                    config.orderbook_levels,
                    poll_iteration,
                    started_at.elapsed(),
                    &config.symbol,
                );
                renderer.render(&frame)?;
            }
            _ = render_interval.tick() => {
                let frame = build_orderbook_state_frame(
                    &state,
                    config.orderbook_levels,
                    poll_iteration,
                    started_at.elapsed(),
                    &config.symbol,
                );
                renderer.render(&frame)?;
            }
        }
    };

    drop(renderer);
    println!("stopped: {stop_reason}");
    Ok(())
}

async fn run_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    match config.exchange.as_str() {
        "hyperliquid" => run_hyperliquid_trades_stream_ws(config).await,
        "binance" => run_binance_trades_stream_ws(config).await,
        "bybit" => run_bybit_trades_stream_ws(config).await,
        other => bail!(
            "ws transport for trades supports exchange=hyperliquid, exchange=binance, or exchange=bybit (got `{other}`)"
        ),
    }
}

async fn run_hyperliquid_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    ensure_hyperliquid_ws(config)?;
    let coin = resolve_hyperliquid_coin(config);
    let ws_url = resolved_ws_base_url(config);

    let (mut stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect websocket {ws_url}"))?;

    send_hyperliquid_subscription(&mut stream, json!({ "type": "trades", "coin": coin })).await?;

    let mut deduper = TradeDeduper::new(TRADE_DEDUP_CAPACITY.max(config.trade_limit * 10));
    let mut iteration = 0u64;
    let started_at = Instant::now();

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("stopped: received Ctrl+C");
                return Ok(());
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    println!("stopped: reached configured stop condition");
                    return Ok(());
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    println!("stopped: websocket closed by peer");
                    return Ok(());
                };

                let message = message.context("websocket read error")?;

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        println!("stopped: websocket closed by peer");
                        return Ok(());
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };
                        let trades = parse_hyperliquid_trades_message(&text);
                        if trades.is_empty() {
                            continue;
                        }

                        for trade in trades.iter().rev() {
                            let key = trade_key(trade);
                            if deduper.insert(key) {
                                println!("{}", format_trade_line(trade));
                            }
                        }

                        iteration += 1;
                    }
                }
            }
        }
    }
}

async fn run_binance_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_binance_trade_ws_endpoint(config)?;

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    let mut deduper = TradeDeduper::new(TRADE_DEDUP_CAPACITY.max(config.trade_limit * 10));
    let mut iteration = 0u64;
    let started_at = Instant::now();

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("stopped: received Ctrl+C");
                return Ok(());
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    println!("stopped: reached configured stop condition");
                    return Ok(());
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    println!("stopped: websocket closed by peer");
                    return Ok(());
                };

                let message = message.context("websocket read error")?;

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        println!("stopped: websocket closed by peer");
                        return Ok(());
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };

                        let trades = parse_binance_trades_message(&text);
                        if trades.is_empty() {
                            continue;
                        }

                        for trade in trades.iter().rev() {
                            let key = trade_key(trade);
                            if deduper.insert(key) {
                                println!("{}", format_trade_line(trade));
                            }
                        }

                        iteration += 1;
                    }
                }
            }
        }
    }
}

async fn run_bybit_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_bybit_trade_ws_endpoint(config)?;
    let symbol = resolve_bybit_ws_symbol(config)?;

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    send_bybit_trade_subscription(&mut stream, &symbol).await?;

    let mut deduper = TradeDeduper::new(TRADE_DEDUP_CAPACITY.max(config.trade_limit * 10));
    let mut iteration = 0u64;
    let started_at = Instant::now();

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("stopped: received Ctrl+C");
                return Ok(());
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    println!("stopped: reached configured stop condition");
                    return Ok(());
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    println!("stopped: websocket closed by peer");
                    return Ok(());
                };

                let message = message.context("websocket read error")?;

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        println!("stopped: websocket closed by peer");
                        return Ok(());
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };

                        let trades = parse_bybit_trades_message(&text);
                        if trades.is_empty() {
                            continue;
                        }

                        for trade in trades.iter().rev() {
                            let key = trade_key(trade);
                            if deduper.insert(key) {
                                println!("{}", format_trade_line(trade));
                            }
                        }

                        iteration += 1;
                    }
                }
            }
        }
    }
}

async fn run_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    match config.exchange.as_str() {
        "hyperliquid" => run_hyperliquid_orderbook_stream_ws(config).await,
        "binance" => run_binance_orderbook_stream_ws(config).await,
        "bybit" => run_bybit_orderbook_stream_ws(config).await,
        other => bail!(
            "ws transport for orderbook supports exchange=hyperliquid, exchange=binance, or exchange=bybit (got `{other}`)"
        ),
    }
}

async fn run_hyperliquid_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    ensure_hyperliquid_ws(config)?;
    let coin = resolve_hyperliquid_coin(config);
    let ws_url = resolved_ws_base_url(config);

    let (mut stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect websocket {ws_url}"))?;

    send_hyperliquid_subscription(&mut stream, json!({ "type": "l2Book", "coin": coin })).await?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;

    let waiting_frame = build_orderbook_error_frame(
        iteration,
        started_at.elapsed(),
        &config.symbol,
        "waiting for websocket orderbook updates",
    );
    renderer.render(&waiting_frame)?;

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    let stop_reason = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break "received Ctrl+C";
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    break "reached configured stop condition";
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    break "websocket closed by peer";
                };

                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        let frame = build_orderbook_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &config.symbol,
                            &format!("websocket read error: {err}"),
                        );
                        renderer.render(&frame)?;
                        continue;
                    }
                };

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        break "websocket closed by peer";
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };
                        let Some(book) = parse_hyperliquid_orderbook_message(
                            &text,
                            &config.symbol,
                            config.orderbook_levels,
                        ) else {
                            continue;
                        };

                        iteration += 1;
                        let frame = build_orderbook_frame(
                            &book,
                            config.orderbook_levels,
                            iteration,
                            started_at.elapsed(),
                        );
                        renderer.render(&frame)?;
                    }
                }
            }
        }
    };

    drop(renderer);
    println!("stopped: {stop_reason}");
    Ok(())
}

async fn run_binance_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_binance_orderbook_ws_endpoint(config)?;

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;

    let waiting_frame = build_orderbook_error_frame(
        iteration,
        started_at.elapsed(),
        &config.symbol,
        "waiting for websocket orderbook updates",
    );
    renderer.render(&waiting_frame)?;

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    let stop_reason = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break "received Ctrl+C";
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    break "reached configured stop condition";
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    break "websocket closed by peer";
                };

                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        let frame = build_orderbook_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &config.symbol,
                            &format!("websocket read error: {err}"),
                        );
                        renderer.render(&frame)?;
                        continue;
                    }
                };

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        break "websocket closed by peer";
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };

                        let Some(book) = parse_binance_orderbook_message(
                            &text,
                            &config.symbol,
                            config.orderbook_levels,
                        ) else {
                            continue;
                        };

                        iteration += 1;
                        let frame = build_orderbook_frame(
                            &book,
                            config.orderbook_levels,
                            iteration,
                            started_at.elapsed(),
                        );
                        renderer.render(&frame)?;
                    }
                }
            }
        }
    };

    drop(renderer);
    println!("stopped: {stop_reason}");
    Ok(())
}

async fn run_bybit_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_bybit_orderbook_ws_endpoint(config)?;
    let symbol = resolve_bybit_ws_symbol(config)?;
    let depth = to_bybit_ws_depth_levels(config.orderbook_levels);

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    send_bybit_orderbook_subscription(&mut stream, &symbol, depth).await?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;
    let mut state: Option<OrderBookSnapshot> = None;

    let waiting_frame = build_orderbook_error_frame(
        iteration,
        started_at.elapsed(),
        &config.symbol,
        "waiting for websocket orderbook updates",
    );
    renderer.render(&waiting_frame)?;

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    let stop_reason = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break "received Ctrl+C";
            }
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    break "reached configured stop condition";
                }
            }
            message = stream.next() => {
                let Some(message) = message else {
                    break "websocket closed by peer";
                };

                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        let frame = build_orderbook_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &config.symbol,
                            &format!("websocket read error: {err}"),
                        );
                        renderer.render(&frame)?;
                        continue;
                    }
                };

                match message {
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => {
                        break "websocket closed by peer";
                    }
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };

                        let Some(event) = parse_bybit_orderbook_message(&text) else {
                            continue;
                        };

                        if !apply_bybit_orderbook_event(
                            &mut state,
                            event,
                            config.orderbook_levels,
                            &config.symbol,
                        ) {
                            continue;
                        }

                        if let Some(book) = state.as_ref() {
                            iteration += 1;
                            let frame = build_orderbook_frame(
                                book,
                                config.orderbook_levels,
                                iteration,
                                started_at.elapsed(),
                            );
                            renderer.render(&frame)?;
                        }
                    }
                }
            }
        }
    };

    drop(renderer);
    println!("stopped: {stop_reason}");
    Ok(())
}

fn ensure_hyperliquid_ws(config: &Config) -> anyhow::Result<()> {
    if config.exchange != "hyperliquid" {
        bail!(
            "ws transport is currently supported only for exchange=hyperliquid (got `{}`)",
            config.exchange
        );
    }

    Ok(())
}

fn resolved_ws_base_url(config: &Config) -> String {
    let configured = config.ws_url.trim();
    if configured.is_empty() {
        return DEFAULT_HYPERLIQUID_WS_URL.to_string();
    }

    if configured != DEFAULT_HYPERLIQUID_WS_URL {
        return configured.to_string();
    }

    match config.exchange.as_str() {
        "binance" => DEFAULT_BINANCE_FUTURES_WS_URL.to_string(),
        "bybit" => DEFAULT_BYBIT_LINEAR_WS_URL.to_string(),
        _ => DEFAULT_HYPERLIQUID_WS_URL.to_string(),
    }
}

fn build_binance_trade_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    let symbol = resolve_binance_ws_symbol(config)?;
    let stream_name = format!("{}@trade", symbol.to_ascii_lowercase());
    Ok(format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        stream_name
    ))
}

fn build_bybit_trade_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    if base_url.trim().is_empty() {
        bail!("invalid Bybit websocket endpoint");
    }

    Ok(base_url.trim_end_matches('/').to_string())
}

fn build_bybit_orderbook_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    build_bybit_trade_ws_endpoint(config)
}

fn build_binance_orderbook_ws_endpoint(config: &Config) -> anyhow::Result<String> {
    let base_url = resolved_ws_base_url(config);
    let symbol = resolve_binance_ws_symbol(config)?;
    let depth_levels = to_binance_ws_depth_levels(config.orderbook_levels);
    let stream_name = format!(
        "{}@depth{}@100ms",
        symbol.to_ascii_lowercase(),
        depth_levels
    );
    Ok(format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        stream_name
    ))
}

fn to_binance_ws_depth_levels(levels: usize) -> usize {
    if levels <= 5 {
        5
    } else if levels <= 10 {
        10
    } else {
        20
    }
}

fn to_bybit_ws_depth_levels(levels: usize) -> usize {
    if levels <= 1 {
        1
    } else if levels <= 50 {
        50
    } else if levels <= 200 {
        200
    } else {
        1_000
    }
}

fn resolve_binance_ws_symbol(config: &Config) -> anyhow::Result<String> {
    if let Some(coin) = config.coin.as_deref() {
        let base = coin
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();

        if base.is_empty() {
            bail!("`--coin` cannot be empty for exchange=binance");
        }

        return Ok(format!("{base}USDT"));
    }

    let core = config
        .symbol
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();

    let market_symbol = if core.contains('/') {
        let mut parts = core.split('/');
        let base = parts.next().unwrap_or_default().trim();
        let quote = parts.next().unwrap_or_default().trim();
        if base.is_empty() || quote.is_empty() || parts.next().is_some() {
            bail!("invalid Binance symbol `{}`", config.symbol);
        }
        format!("{base}{quote}")
    } else {
        core.chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
    };

    if market_symbol.len() < 6 {
        bail!("invalid Binance symbol `{}`", config.symbol);
    }

    Ok(market_symbol)
}

fn resolve_bybit_ws_symbol(config: &Config) -> anyhow::Result<String> {
    if let Some(coin) = config.coin.as_deref() {
        let asset = coin
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();

        if asset.is_empty() {
            bail!("`--coin` cannot be empty for exchange=bybit");
        }

        if has_bybit_quote_suffix(&asset) {
            return Ok(asset);
        }

        return Ok(format!("{asset}USDT"));
    }

    let core = config
        .symbol
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();

    let market_symbol = if core.contains('/') {
        let mut parts = core.split('/');
        let base = parts.next().unwrap_or_default().trim();
        let quote = parts.next().unwrap_or_default().trim();
        if base.is_empty() || quote.is_empty() || parts.next().is_some() {
            bail!("invalid Bybit symbol `{}`", config.symbol);
        }
        format!("{base}{quote}")
    } else {
        core.chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .collect::<String>()
    };

    if market_symbol.len() < 6 {
        bail!("invalid Bybit symbol `{}`", config.symbol);
    }

    Ok(market_symbol)
}

fn has_bybit_quote_suffix(symbol: &str) -> bool {
    ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"]
        .iter()
        .any(|quote| symbol.len() > quote.len() && symbol.ends_with(quote))
}

fn resolve_hyperliquid_coin(config: &Config) -> String {
    if let Some(coin) = config.coin.as_deref() {
        return coin.trim().to_string();
    }

    infer_hyperliquid_coin_from_symbol(&config.symbol)
        .unwrap_or_else(|| config.symbol.trim().to_string())
}

fn infer_hyperliquid_coin_from_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with('@') {
        return Some(trimmed.to_string());
    }

    trimmed
        .split('/')
        .next()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
}

type HyperliquidWsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn send_hyperliquid_subscription(
    stream: &mut HyperliquidWsStream,
    subscription: Value,
) -> anyhow::Result<()> {
    let payload = json!({
        "method": "subscribe",
        "subscription": subscription,
    });

    stream
        .send(Message::Text(payload.to_string().into()))
        .await
        .context("failed to send websocket subscription")
}

async fn send_bybit_trade_subscription(
    stream: &mut HyperliquidWsStream,
    symbol: &str,
) -> anyhow::Result<()> {
    let topic = format!("publicTrade.{symbol}");
    send_bybit_subscription(stream, &topic, "trade").await
}

async fn send_bybit_orderbook_subscription(
    stream: &mut HyperliquidWsStream,
    symbol: &str,
    depth: usize,
) -> anyhow::Result<()> {
    let topic = format!("orderbook.{depth}.{symbol}");
    send_bybit_subscription(stream, &topic, "orderbook").await
}

async fn send_bybit_subscription(
    stream: &mut HyperliquidWsStream,
    topic: &str,
    channel: &str,
) -> anyhow::Result<()> {
    let payload = json!({
        "op": "subscribe",
        "args": [topic],
    });

    stream
        .send(Message::Text(payload.to_string().into()))
        .await
        .with_context(|| format!("failed to send Bybit {channel} websocket subscription"))
}

fn ws_message_text(message: Message) -> anyhow::Result<Option<String>> {
    match message {
        Message::Text(text) => Ok(Some(text.to_string())),
        Message::Binary(bytes) => {
            let text = String::from_utf8(bytes.to_vec())
                .context("unable to decode websocket binary payload")?;
            Ok(Some(text))
        }
        Message::Pong(_) | Message::Frame(_) => Ok(None),
        Message::Ping(_) | Message::Close(_) => Ok(None),
    }
}

fn parse_hyperliquid_orderbook_message(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<OrderBookSnapshot> {
    let value = serde_json::from_str::<Value>(payload).ok()?;
    if value.get("channel").and_then(Value::as_str)? != "l2Book" {
        return None;
    }

    let data = value.get("data")?;
    let timestamp = parse_u64_lossy(data.get("time")?);
    let datetime = timestamp.and_then(iso8601_millis);

    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }

    let bids = parse_hyperliquid_levels(levels.first()?, levels_limit, true);
    let asks = parse_hyperliquid_levels(levels.get(1)?, levels_limit, false);

    Some(OrderBookSnapshot {
        asks,
        bids,
        datetime,
        timestamp,
        symbol: Some(symbol.to_string()),
    })
}

fn parse_hyperliquid_levels(levels: &Value, limit: usize, descending: bool) -> Vec<(f64, f64)> {
    let Some(levels) = levels.as_array() else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let Some(px) = level.get("px").and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(sz) = level.get("sz").and_then(parse_f64_lossy) else {
            continue;
        };
        parsed.push((px, sz));
    }

    if descending {
        parsed.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        parsed.sort_by(|left, right| left.0.total_cmp(&right.0));
    }

    if parsed.len() > limit {
        parsed.truncate(limit);
    }

    parsed
}

fn parse_hyperliquid_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    if value
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        != "trades"
    {
        return Vec::new();
    }

    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row.get("time").and_then(parse_u64_lossy);
        let price = row.get("px").and_then(parse_f64_lossy);
        let amount = row.get("sz").and_then(parse_f64_lossy);

        let side = row
            .get("side")
            .and_then(Value::as_str)
            .map(|value| match value {
                "B" | "buy" | "BUY" => "buy".to_string(),
                "A" | "sell" | "SELL" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        let id = row.get("tid").and_then(stringify_json_value);

        parsed.push(TradeRow {
            id,
            timestamp,
            datetime: timestamp.and_then(iso8601_millis),
            side,
            price,
            amount,
            cost: match (price, amount) {
                (Some(price), Some(amount)) => Some(price * amount),
                _ => None,
            },
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

fn parse_binance_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let data = value.get("data").unwrap_or(&value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or_default();
    if event != "trade" && event != "aggTrade" {
        return Vec::new();
    }

    let timestamp = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy);
    let price = data.get("p").and_then(parse_f64_lossy);
    let amount = data.get("q").and_then(parse_f64_lossy);
    let id = data
        .get("t")
        .or_else(|| data.get("a"))
        .and_then(stringify_json_value);

    let side = data
        .get("m")
        .and_then(Value::as_bool)
        .map(|is_buyer_maker| {
            if is_buyer_maker {
                "sell".to_string()
            } else {
                "buy".to_string()
            }
        });

    let cost = match (price, amount) {
        (Some(price), Some(amount)) => Some(price * amount),
        _ => None,
    };

    vec![TradeRow {
        id,
        timestamp,
        datetime: timestamp.and_then(iso8601_millis),
        side,
        price,
        amount,
        cost,
    }]
}

fn parse_bybit_trades_message(payload: &str) -> Vec<TradeRow> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };

    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !topic.starts_with("publicTrade.") {
        return Vec::new();
    }

    let Some(rows) = value.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row.get("T").and_then(parse_u64_lossy);
        let price = row.get("p").and_then(parse_f64_lossy);
        let amount = row.get("v").and_then(parse_f64_lossy);
        let id = row.get("i").and_then(stringify_json_value);
        let side = row
            .get("S")
            .and_then(Value::as_str)
            .map(|value| match value {
                "Buy" | "BUY" | "buy" => "buy".to_string(),
                "Sell" | "SELL" | "sell" => "sell".to_string(),
                other => other.to_ascii_lowercase(),
            });

        let cost = match (price, amount) {
            (Some(price), Some(amount)) => Some(price * amount),
            _ => None,
        };

        parsed.push(TradeRow {
            id,
            timestamp,
            datetime: timestamp.and_then(iso8601_millis),
            side,
            price,
            amount,
            cost,
        });
    }

    parsed.sort_by_key(|trade| trade.timestamp.unwrap_or_default());
    parsed
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BybitOrderBookEventType {
    Snapshot,
    Delta,
}

#[derive(Debug)]
struct BybitOrderBookEvent {
    event_type: BybitOrderBookEventType,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    timestamp: Option<u64>,
}

fn parse_bybit_orderbook_message(payload: &str) -> Option<BybitOrderBookEvent> {
    let value = serde_json::from_str::<Value>(payload).ok()?;

    let topic = value.get("topic")?.as_str()?;
    if !topic.starts_with("orderbook.") {
        return None;
    }

    let event_type = match value.get("type").and_then(Value::as_str)? {
        "snapshot" => BybitOrderBookEventType::Snapshot,
        "delta" => BybitOrderBookEventType::Delta,
        _ => return None,
    };

    let data = value.get("data")?;
    let bids = parse_bybit_orderbook_side(data.get("b"));
    let asks = parse_bybit_orderbook_side(data.get("a"));

    let timestamp = value
        .get("ts")
        .and_then(parse_u64_lossy)
        .or_else(|| data.get("cts").and_then(parse_u64_lossy));

    Some(BybitOrderBookEvent {
        event_type,
        bids,
        asks,
        timestamp,
    })
}

fn parse_bybit_orderbook_side(value: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut output = Vec::with_capacity(levels.len());
    for row in levels {
        let Some(row) = row.as_array() else {
            continue;
        };
        if row.len() < 2 {
            continue;
        }

        let Some(price) = row.first().and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(size) = row.get(1).and_then(parse_f64_lossy) else {
            continue;
        };

        output.push((price, size));
    }

    output
}

fn apply_bybit_orderbook_event(
    state: &mut Option<OrderBookSnapshot>,
    event: BybitOrderBookEvent,
    levels_limit: usize,
    default_symbol: &str,
) -> bool {
    let symbol = default_symbol.to_string();
    let datetime = event.timestamp.and_then(iso8601_millis);

    match event.event_type {
        BybitOrderBookEventType::Snapshot => {
            let mut bids = event.bids;
            let mut asks = event.asks;

            normalize_bybit_book_side(&mut bids, true, levels_limit);
            normalize_bybit_book_side(&mut asks, false, levels_limit);

            *state = Some(OrderBookSnapshot {
                asks,
                bids,
                datetime,
                timestamp: event.timestamp,
                symbol: Some(symbol),
            });
            true
        }
        BybitOrderBookEventType::Delta => {
            let Some(book) = state.as_mut() else {
                return false;
            };

            apply_bybit_level_updates(&mut book.bids, &event.bids, true);
            apply_bybit_level_updates(&mut book.asks, &event.asks, false);

            normalize_bybit_book_side(&mut book.bids, true, levels_limit);
            normalize_bybit_book_side(&mut book.asks, false, levels_limit);

            book.timestamp = event.timestamp;
            book.datetime = datetime;
            book.symbol = Some(symbol);
            true
        }
    }
}

fn apply_bybit_level_updates(
    levels: &mut Vec<(f64, f64)>,
    updates: &[(f64, f64)],
    descending: bool,
) {
    for (price, size) in updates {
        let existing_index = levels
            .iter()
            .position(|(existing_price, _)| bybit_price_eq(*existing_price, *price));

        if *size <= 0.0 {
            if let Some(index) = existing_index {
                levels.remove(index);
            }
            continue;
        }

        if let Some(index) = existing_index {
            levels[index].1 = *size;
        } else {
            levels.push((*price, *size));
        }
    }

    sort_bybit_book_side(levels, descending);
}

fn normalize_bybit_book_side(levels: &mut Vec<(f64, f64)>, descending: bool, levels_limit: usize) {
    levels.retain(|(_, size)| *size > 0.0);
    sort_bybit_book_side(levels, descending);
    if levels.len() > levels_limit {
        levels.truncate(levels_limit);
    }
}

fn sort_bybit_book_side(levels: &mut [(f64, f64)], descending: bool) {
    if descending {
        levels.sort_by(|left, right| right.0.total_cmp(&left.0));
    } else {
        levels.sort_by(|left, right| left.0.total_cmp(&right.0));
    }
}

fn bybit_price_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

fn parse_binance_orderbook_message(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<OrderBookSnapshot> {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return None;
    };

    let data = value.get("data").unwrap_or(&value);
    let event = data.get("e").and_then(Value::as_str).unwrap_or_default();
    if event != "depthUpdate" {
        return None;
    }

    let timestamp = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy);
    let datetime = timestamp.and_then(iso8601_millis);

    let mut bids = parse_binance_orderbook_side(data.get("b"), levels_limit);
    let mut asks = parse_binance_orderbook_side(data.get("a"), levels_limit);

    bids.sort_by(|left, right| right.0.total_cmp(&left.0));
    asks.sort_by(|left, right| left.0.total_cmp(&right.0));

    if bids.len() > levels_limit {
        bids.truncate(levels_limit);
    }
    if asks.len() > levels_limit {
        asks.truncate(levels_limit);
    }

    Some(OrderBookSnapshot {
        asks,
        bids,
        datetime,
        timestamp,
        symbol: Some(symbol.to_string()),
    })
}

fn parse_binance_orderbook_side(value: Option<&Value>, limit: usize) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut output = Vec::with_capacity(levels.len().min(limit));

    for row in levels.iter().take(limit) {
        let Some(row) = row.as_array() else {
            continue;
        };
        if row.len() < 2 {
            continue;
        }

        let Some(price) = row.first().and_then(parse_f64_lossy) else {
            continue;
        };
        let Some(size) = row.get(1).and_then(parse_f64_lossy) else {
            continue;
        };
        output.push((price, size));
    }

    output
}

fn parse_u64_lossy(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_f64_lossy(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn stringify_json_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn should_stop(started_at: Instant, iteration: u64, config: &Config) -> bool {
    if let Some(max_iterations) = config.iterations {
        if iteration >= max_iterations {
            return true;
        }
    }

    if let Some(duration_secs) = config.duration_secs {
        if started_at.elapsed() >= Duration::from_secs(duration_secs) {
            return true;
        }
    }

    false
}

async fn wait_for_next_poll(poll_ms: u64) -> bool {
    tokio::select! {
        _ = sleep(Duration::from_millis(poll_ms)) => true,
        _ = tokio::signal::ctrl_c() => false,
    }
}

struct OrderBookRenderer {
    stdout: Stdout,
    previous_line_count: usize,
    interactive: bool,
}

impl OrderBookRenderer {
    fn new() -> anyhow::Result<Self> {
        let interactive = io::stdout().is_terminal();
        let mut stdout = io::stdout();

        if interactive {
            stdout
                .execute(EnterAlternateScreen)
                .context("failed to enter alternate terminal screen")?;
            stdout
                .execute(Hide)
                .context("failed to hide terminal cursor")?;
            stdout
                .execute(Clear(ClearType::All))
                .context("failed to clear terminal screen")?;
            stdout
                .execute(MoveTo(0, 0))
                .context("failed to move terminal cursor")?;
            stdout.flush().context("failed to flush terminal output")?;
        }

        Ok(Self {
            stdout,
            previous_line_count: 0,
            interactive,
        })
    }

    fn render(&mut self, frame: &str) -> anyhow::Result<()> {
        if !self.interactive {
            println!("{frame}");
            return Ok(());
        }

        let lines: Vec<&str> = frame.lines().collect();

        for (index, line) in lines.iter().enumerate() {
            let Ok(row) = u16::try_from(index) else {
                break;
            };
            self.stdout
                .queue(MoveTo(0, row))
                .context("failed to move cursor during frame render")?;
            self.stdout
                .queue(Clear(ClearType::CurrentLine))
                .context("failed to clear line during frame render")?;
            self.stdout
                .queue(Print(*line))
                .context("failed to print frame line")?;
        }

        for index in lines.len()..self.previous_line_count {
            let Ok(row) = u16::try_from(index) else {
                break;
            };
            self.stdout
                .queue(MoveTo(0, row))
                .context("failed to move cursor during line cleanup")?;
            self.stdout
                .queue(Clear(ClearType::CurrentLine))
                .context("failed to clear stale frame line")?;
        }

        self.stdout
            .flush()
            .context("failed to flush terminal frame")?;
        self.previous_line_count = lines.len();

        Ok(())
    }
}

impl Drop for OrderBookRenderer {
    fn drop(&mut self) {
        if !self.interactive {
            return;
        }

        let _ = self.stdout.execute(Show);
        let _ = self.stdout.execute(LeaveAlternateScreen);
        let _ = self.stdout.flush();
    }
}

struct BackendClient {
    client: reqwest::Client,
    base_url: String,
}

impl BackendClient {
    fn new(base_url: String, timeout_secs: u64) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        payload: &impl Serialize,
    ) -> anyhow::Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(&url)
            .json(payload)
            .send()
            .await
            .with_context(|| format!("request failed for {url}"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .with_context(|| format!("failed to read response body for {url}"))?;

        if !status.is_success() {
            bail!("request to {url} failed with status={status}; body={body}");
        }

        serde_json::from_str::<T>(&body)
            .with_context(|| format!("failed to parse JSON response from {url}; body={body}"))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeRow {
    id: Option<String>,
    timestamp: Option<u64>,
    datetime: Option<String>,
    side: Option<String>,
    price: Option<f64>,
    amount: Option<f64>,
    cost: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderBookSnapshot {
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
    datetime: Option<String>,
    timestamp: Option<u64>,
    symbol: Option<String>,
}

enum OrderBookState {
    Book(OrderBookSnapshot),
    Error(String),
}

#[derive(Debug)]
struct BookStats {
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread: Option<f64>,
    mid: Option<f64>,
    spread_bps: Option<f64>,
}

fn compute_book_stats(book: &OrderBookSnapshot) -> BookStats {
    let best_bid = book.bids.first().map(|(price, _)| *price);
    let best_ask = book.asks.first().map(|(price, _)| *price);
    let spread = match (best_ask, best_bid) {
        (Some(ask), Some(bid)) if ask >= bid => Some(ask - bid),
        _ => None,
    };
    let mid = match (best_ask, best_bid) {
        (Some(ask), Some(bid)) => Some((ask + bid) / 2.0),
        _ => None,
    };
    let spread_bps = match (spread, mid) {
        (Some(spread), Some(mid)) if mid > 0.0 => Some((spread / mid) * 10_000.0),
        _ => None,
    };

    BookStats {
        best_bid,
        best_ask,
        spread,
        mid,
        spread_bps,
    }
}

#[derive(Debug, Clone)]
struct BookLevelRow {
    price: f64,
    size: f64,
    cumulative_size: f64,
}

fn build_level_rows(levels: &[(f64, f64)], depth: usize) -> Vec<BookLevelRow> {
    let mut rows = Vec::with_capacity(depth.min(levels.len()));
    let mut cumulative = 0.0;

    for (price, size) in levels.iter().take(depth) {
        cumulative += size.max(0.0);
        rows.push(BookLevelRow {
            price: *price,
            size: *size,
            cumulative_size: cumulative,
        });
    }

    rows
}

fn build_orderbook_frame(
    book: &OrderBookSnapshot,
    levels: usize,
    iteration: u64,
    elapsed: Duration,
) -> String {
    let stats = compute_book_stats(book);
    let (asks_best_first, bids_best_first) = normalize_book_levels(book);
    let ask_rows = build_level_rows(&asks_best_first, levels);
    let bid_rows = build_level_rows(&bids_best_first, levels);
    let max_cumulative = ask_rows
        .last()
        .map(|row| row.cumulative_size)
        .unwrap_or(0.0)
        .max(
            bid_rows
                .last()
                .map(|row| row.cumulative_size)
                .unwrap_or(0.0),
        );

    let top_ask_size = ask_rows.iter().map(|row| row.size).sum::<f64>();
    let top_bid_size = bid_rows.iter().map(|row| row.size).sum::<f64>();
    let top_imbalance = if top_ask_size + top_bid_size > 0.0 {
        Some(top_bid_size / (top_bid_size + top_ask_size))
    } else {
        None
    };

    let timestamp = book
        .datetime
        .clone()
        .or_else(|| book.timestamp.and_then(iso8601_millis))
        .unwrap_or_else(|| "unknown".to_string());

    let mut output = String::new();
    let _ = writeln!(
        output,
        "orderbook stream | update={} | elapsed={}s | symbol={}",
        iteration,
        elapsed.as_secs(),
        book.symbol.as_deref().unwrap_or("unknown")
    );
    let _ = writeln!(output, "timestamp: {timestamp}");
    let _ = writeln!(
        output,
        "best_bid={} best_ask={} spread={} mid={}",
        format_f64(stats.best_bid),
        format_f64(stats.best_ask),
        format_f64(stats.spread),
        format_f64(stats.mid)
    );
    let _ = writeln!(
        output,
        "spread_bps={} top_bid_size={} top_ask_size={} top_imbalance={}",
        format_f64(stats.spread_bps),
        format_f64(Some(top_bid_size)),
        format_f64(Some(top_ask_size)),
        format_pct(top_imbalance)
    );
    let _ = writeln!(output);

    let _ = writeln!(output, "price          size        cum_size    depth");
    let asks_far_to_near = ask_rows.iter().rev().collect::<Vec<_>>();
    for slot in 0..levels {
        let row = asks_far_to_near.get(slot).copied();
        let _ = writeln!(output, "{}", format_book_row(row, max_cumulative));
    }

    let _ = writeln!(output, "------------------------ MID --------------------");

    for slot in 0..levels {
        let row = bid_rows.get(slot);
        let _ = writeln!(output, "{}", format_book_row(row, max_cumulative));
    }

    output
}

fn build_orderbook_state_frame(
    state: &OrderBookState,
    levels: usize,
    iteration: u64,
    elapsed: Duration,
    default_symbol: &str,
) -> String {
    match state {
        OrderBookState::Book(book) => build_orderbook_frame(book, levels, iteration, elapsed),
        OrderBookState::Error(error) => {
            build_orderbook_error_frame(iteration, elapsed, default_symbol, error)
        }
    }
}

fn build_orderbook_error_frame(
    iteration: u64,
    elapsed: Duration,
    symbol: &str,
    error: &str,
) -> String {
    let mut output = String::new();
    let compact_error = error.replace('\n', " | ");
    let _ = writeln!(
        output,
        "orderbook stream | update={} | elapsed={}s | symbol={}",
        iteration,
        elapsed.as_secs(),
        symbol
    );
    let _ = writeln!(output, "error fetching order book");
    let _ = writeln!(output, "{compact_error}");
    output
}

fn normalize_book_levels(book: &OrderBookSnapshot) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    let mut asks = book.asks.clone();
    let mut bids = book.bids.clone();

    asks.sort_by(|left, right| left.0.total_cmp(&right.0));
    bids.sort_by(|left, right| right.0.total_cmp(&left.0));

    (asks, bids)
}

fn format_book_row(row: Option<&BookLevelRow>, max_cumulative: f64) -> String {
    let Some(row) = row else {
        return format!("{:>12} {:>12} {:>12} {:<8}", "-", "-", "-", "-");
    };

    let depth_bar = format_depth_bar(row.cumulative_size, max_cumulative, 8);
    format!(
        "{:>12} {:>12} {:>12} {:<8}",
        format_price(row.price),
        format_f64(Some(row.size)),
        format_f64(Some(row.cumulative_size)),
        depth_bar,
    )
}

fn format_depth_bar(value: f64, max_value: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    if max_value <= 0.0 || value <= 0.0 {
        return "-".repeat(width);
    }

    let ratio = (value / max_value).clamp(0.0, 1.0);
    let filled = ((ratio * width as f64).round() as usize).min(width);
    format!("{}{}", "#".repeat(filled), "-".repeat(width - filled))
}

fn format_price(value: f64) -> String {
    if value.abs() >= 10_000.0 {
        return format!("{value:.2}");
    }
    if value.abs() >= 100.0 {
        return format!("{value:.4}");
    }
    format!("{value:.6}")
}

fn format_pct(value: Option<f64>) -> String {
    value
        .map(|number| format!("{:.2}%", number * 100.0))
        .unwrap_or_else(|| "-".to_string())
}

fn format_trade_line(trade: &TradeRow) -> String {
    let time = trade
        .datetime
        .clone()
        .or_else(|| trade.timestamp.and_then(iso8601_millis))
        .unwrap_or_else(|| "unknown-time".to_string());
    let side = trade
        .side
        .as_deref()
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let id = trade.id.as_deref().unwrap_or("-");

    format!(
        "{} side={:<5} price={:<14} amount={:<14} cost={:<14} id={}",
        time,
        side,
        format_f64(trade.price),
        format_f64(trade.amount),
        format_f64(trade.cost),
        id
    )
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn format_f64(value: Option<f64>) -> String {
    value
        .map(|number| format!("{number:.6}"))
        .unwrap_or_else(|| "-".to_string())
}

fn trade_key(trade: &TradeRow) -> String {
    if let Some(id) = trade
        .id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        return format!("id:{id}");
    }

    let timestamp = trade
        .timestamp
        .map_or_else(|| "-".to_string(), |value| value.to_string());
    let side = trade.side.clone().unwrap_or_else(|| "-".to_string());
    let price = format_f64(trade.price);
    let amount = format_f64(trade.amount);
    format!("ts:{timestamp}|side:{side}|price:{price}|amount:{amount}")
}

#[derive(Debug)]
struct TradeDeduper {
    set: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl TradeDeduper {
    fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn insert(&mut self, key: String) -> bool {
        if self.set.contains(&key) {
            return false;
        }

        self.set.insert(key.clone());
        self.order.push_back(key);

        while self.order.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.set.remove(&evicted);
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_bybit_orderbook_event, build_binance_orderbook_ws_endpoint,
        build_bybit_orderbook_ws_endpoint, build_level_rows, compute_book_stats, format_depth_bar,
        infer_hyperliquid_coin_from_symbol, normalize_book_levels, parse_args,
        parse_binance_orderbook_message, parse_binance_trades_message,
        parse_bybit_orderbook_message, parse_bybit_trades_message,
        parse_hyperliquid_orderbook_message, parse_hyperliquid_trades_message,
        resolve_binance_ws_symbol, resolve_bybit_ws_symbol, to_binance_ws_depth_levels,
        to_bybit_ws_depth_levels, trade_key, BybitOrderBookEventType, Config, Mode,
        OrderBookSnapshot, ParseResult, TradeDeduper, TradeRow, Transport,
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
            "--base-url",
            "http://127.0.0.1:8787",
            "--symbol",
            "ETH/USDC:USDC",
            "--poll-ms",
            "400",
            "--limit",
            "40",
            "--iterations",
            "3",
        ]);

        assert_eq!(config.mode, Mode::Trades);
        assert_eq!(config.base_url, "http://127.0.0.1:8787");
        assert_eq!(config.symbol, "ETH/USDC:USDC");
        assert_eq!(config.poll_ms, 400);
        assert_eq!(config.trade_limit, 40);
        assert_eq!(config.iterations, Some(3));
    }

    #[test]
    fn parse_args_defaults_to_ws_transport() {
        let config = parse_run(&["trades"]);
        assert_eq!(config.transport, Transport::Ws);
    }

    #[test]
    fn parse_args_caps_orderbook_levels() {
        let config = parse_run(&["orderbook", "--levels", "999", "--render-ms", "16"]);
        assert_eq!(config.mode, Mode::OrderBook);
        assert_eq!(config.orderbook_levels, 20);
        assert_eq!(config.orderbook_render_ms, 16);
    }

    #[test]
    fn parse_args_enforces_minimum_orderbook_levels() {
        let config = parse_run(&["orderbook", "--levels", "1"]);
        assert_eq!(config.orderbook_levels, 10);
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
    fn parse_args_reads_ws_transport_options() {
        let config = parse_run(&[
            "orderbook",
            "--transport",
            "ws",
            "--ws-url",
            "wss://api.hyperliquid.xyz/ws",
            "--coin",
            "BTC",
        ]);

        assert_eq!(config.transport, Transport::Ws);
        assert_eq!(config.ws_url, "wss://api.hyperliquid.xyz/ws");
        assert_eq!(config.coin.as_deref(), Some("BTC"));
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

        let snapshot =
            parse_bybit_orderbook_message(snapshot_payload).expect("snapshot should parse");
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
            "--transport",
            "ws",
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
    fn build_bybit_orderbook_ws_endpoint_uses_default_linear_ws() {
        let config = parse_run(&[
            "orderbook",
            "--exchange",
            "bybit",
            "--transport",
            "ws",
            "--coin",
            "BTC",
        ]);

        let endpoint = build_bybit_orderbook_ws_endpoint(&config).expect("endpoint should build");
        assert_eq!(endpoint, "wss://stream.bybit.com/v5/public/linear");
        assert_eq!(to_bybit_ws_depth_levels(1), 1);
        assert_eq!(to_bybit_ws_depth_levels(20), 50);
        assert_eq!(to_bybit_ws_depth_levels(120), 200);
    }
}
