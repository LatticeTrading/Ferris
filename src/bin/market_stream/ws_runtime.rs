use std::time::{Duration, Instant};

use crate::{
    cli::Config,
    constants::{BINANCE_SNAPSHOT_TIMEOUT_MS, TRADE_DEDUP_CAPACITY},
    control::should_stop,
    view::{
        build_ohlcv_error_frame, build_ohlcv_frame, build_orderbook_error_frame,
        build_orderbook_frame, format_trade_line, trade_key, OhlcvRow, OrderBookRenderer,
        OrderBookSnapshot, TradeDeduper,
    },
    ws_helpers::{
        apply_bybit_orderbook_event, build_aster_ohlcv_ws_endpoint,
        build_aster_orderbook_ws_endpoint, build_aster_trade_ws_endpoint,
        build_binance_ohlcv_ws_endpoint, build_binance_orderbook_ws_endpoint,
        build_binance_trade_ws_endpoint, build_bybit_ohlcv_ws_endpoint,
        build_bybit_orderbook_ws_endpoint, build_bybit_trade_ws_endpoint, ensure_hyperliquid_ws,
        parse_binance_ohlcv_message, parse_binance_orderbook_message, parse_binance_trades_message,
        parse_bybit_ohlcv_message, parse_bybit_orderbook_message, parse_bybit_trades_message,
        parse_hyperliquid_orderbook_message, parse_hyperliquid_trades_message,
        resolve_aster_ws_symbol, resolve_binance_ws_symbol, resolve_bybit_ws_symbol,
        resolve_hyperliquid_coin, resolved_ws_base_url, send_bybit_ohlcv_subscription,
        send_bybit_orderbook_subscription, send_bybit_trade_subscription,
        send_hyperliquid_subscription, to_bybit_ws_depth_levels, to_bybit_ws_interval,
        ws_message_text,
    },
};
use anyhow::{bail, Context};
use ferris_market_data_backend::{
    binance_orderbook::{
        parse_binance_depth_event, BinanceEventOutcome, BinanceOrderBookSync,
        BinanceSnapshotOutcome, OrderBookSnapshotProvider, BINANCE_PARTIAL_DEPTH_THRESHOLD,
    },
    bybit_full_orderbook::{
        fetch_bybit_full_snapshot, parse_bybit_full_delta, BufferDeltaOutcome, BybitFullBookSync,
        SnapshotSyncOutcome, BYBIT_FULL_DEPTH_THRESHOLD, BYBIT_REST_BASE_URL,
    },
    exchanges::{aster::AsterExchange, binance::BinanceExchange},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::{connect_async, tungstenite::Message};
pub(crate) async fn run_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    match config.exchange.as_str() {
        "hyperliquid" => run_hyperliquid_trades_stream_ws(config).await,
        "binance" | "aster" => run_binance_trades_stream_ws(config).await,
        "bybit" => run_bybit_trades_stream_ws(config).await,
        other => bail!(
            "ws transport for trades supports exchange=hyperliquid, exchange=binance, exchange=aster, or exchange=bybit (got `{other}`)"
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
                        if should_stop(started_at, iteration, config) {
                            println!("stopped: reached configured stop condition");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

async fn run_binance_trades_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = if config.exchange == "aster" {
        build_aster_trade_ws_endpoint(config)?
    } else {
        build_binance_trade_ws_endpoint(config)?
    };

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
                        if should_stop(started_at, iteration, config) {
                            println!("stopped: reached configured stop condition");
                            return Ok(());
                        }
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
                        if should_stop(started_at, iteration, config) {
                            println!("stopped: reached configured stop condition");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

pub(crate) async fn run_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    match config.exchange.as_str() {
        "hyperliquid" => run_hyperliquid_orderbook_stream_ws(config).await,
        "binance" | "aster" => run_binance_orderbook_stream_ws(config).await,
        "bybit" => run_bybit_orderbook_stream_ws(config).await,
        other => bail!("ws transport for orderbook supports exchange=hyperliquid, exchange=binance, exchange=aster, or exchange=bybit (got `{other}`)"),
    }
}

pub(crate) async fn run_ohlcv_stream_ws(config: &Config) -> anyhow::Result<()> {
    match config.exchange.as_str() {
        "binance" | "aster" => run_binance_ohlcv_stream_ws(config).await,
        "bybit" => run_bybit_ohlcv_stream_ws(config).await,
        other => bail!("ws transport for ohlcv currently supports exchange=binance, exchange=aster, or exchange=bybit (got `{other}`)"),
    }
}

async fn run_binance_ohlcv_stream_ws(config: &Config) -> anyhow::Result<()> {
    let (ws_endpoint, display_symbol) = if config.exchange == "aster" {
        (
            build_aster_ohlcv_ws_endpoint(config)?,
            resolve_aster_ws_symbol(config)?,
        )
    } else {
        (
            build_binance_ohlcv_ws_endpoint(config)?,
            resolve_binance_ws_symbol(config)?,
        )
    };

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;
    let mut candles: Vec<OhlcvRow> = Vec::new();

    let waiting_frame = build_ohlcv_error_frame(
        iteration,
        started_at.elapsed(),
        &display_symbol,
        &config.ohlcv_timeframe,
        "waiting for websocket candle updates",
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
                        let frame = build_ohlcv_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &display_symbol,
                            &config.ohlcv_timeframe,
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

                        let updates = parse_binance_ohlcv_message(&text);
                        if updates.is_empty() {
                            continue;
                        }

                        apply_ohlcv_updates(&mut candles, &updates, config.ohlcv_limit);
                        iteration += 1;

                        let frame = build_ohlcv_frame(
                            &candles,
                            iteration,
                            started_at.elapsed(),
                            &display_symbol,
                            &config.ohlcv_timeframe,
                            config.ohlcv_chart_height,
                        );
                        renderer.render(&frame)?;
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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

async fn run_bybit_ohlcv_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_bybit_ohlcv_ws_endpoint(config)?;
    let symbol = resolve_bybit_ws_symbol(config)?;
    let interval = to_bybit_ws_interval(&config.ohlcv_timeframe)?;

    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;

    send_bybit_ohlcv_subscription(&mut stream, &symbol, interval).await?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;
    let mut candles: Vec<OhlcvRow> = Vec::new();

    let waiting_frame = build_ohlcv_error_frame(
        iteration,
        started_at.elapsed(),
        &symbol,
        &config.ohlcv_timeframe,
        "waiting for websocket candle updates",
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
                        let frame = build_ohlcv_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &symbol,
                            &config.ohlcv_timeframe,
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

                        let updates = parse_bybit_ohlcv_message(&text);
                        if updates.is_empty() {
                            continue;
                        }

                        apply_ohlcv_updates(&mut candles, &updates, config.ohlcv_limit);
                        iteration += 1;

                        let frame = build_ohlcv_frame(
                            &candles,
                            iteration,
                            started_at.elapsed(),
                            &symbol,
                            &config.ohlcv_timeframe,
                            config.ohlcv_chart_height,
                        );
                        renderer.render(&frame)?;
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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

fn apply_ohlcv_updates(state: &mut Vec<OhlcvRow>, updates: &[OhlcvRow], limit: usize) {
    for candle in updates {
        if let Some(existing) = state
            .iter_mut()
            .find(|existing| existing.timestamp() == candle.timestamp())
        {
            *existing = *candle;
        } else {
            state.push(*candle);
        }
    }

    state.sort_by_key(|candle| candle.timestamp());
    let max_candles = limit.max(1);
    if state.len() > max_candles {
        let overflow = state.len() - max_candles;
        state.drain(0..overflow);
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
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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

async fn run_binance_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    if config.exchange != "aster" && config.orderbook_levels <= BINANCE_PARTIAL_DEPTH_THRESHOLD {
        return run_binance_partial_orderbook_stream_ws(config).await;
    }

    use std::{future::Future, pin::Pin, sync::Arc};

    type SnapshotFuture = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ferris_market_data_backend::binance_orderbook::BinanceDepthSnapshot,
                        String,
                    >,
                > + Send,
        >,
    >;

    let (ws_endpoint, market_symbol) = if config.exchange == "aster" {
        (
            build_aster_orderbook_ws_endpoint(config)?,
            resolve_aster_ws_symbol(config)?,
        )
    } else {
        (
            build_binance_orderbook_ws_endpoint(config)?,
            resolve_binance_ws_symbol(config)?,
        )
    };
    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;
    let provider: Arc<dyn OrderBookSnapshotProvider> = if config.exchange == "aster" {
        Arc::new(AsterExchange::new(BINANCE_SNAPSHOT_TIMEOUT_MS)?)
    } else {
        Arc::new(BinanceExchange::new(BINANCE_SNAPSHOT_TIMEOUT_MS)?)
    };
    let mut snapshot_request: Option<SnapshotFuture> = Some({
        let provider = provider.clone();
        let market_symbol = market_symbol.clone();
        Box::pin(async move { provider.fetch_order_book_snapshot(&market_symbol).await })
    });
    let mut sync = BinanceOrderBookSync::new();
    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;
    renderer.render(&build_orderbook_error_frame(
        iteration,
        started_at.elapsed(),
        &config.symbol,
        "synchronizing full-depth orderbook",
    ))?;

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    let stop_reason = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break "received Ctrl+C",
            _ = stop_check.tick() => {
                if should_stop(started_at, iteration, config) {
                    break "reached configured stop condition";
                }
            }
            snapshot = async {
                match snapshot_request.as_mut() {
                    Some(request) => Some(request.await),
                    None => std::future::pending::<Option<Result<ferris_market_data_backend::binance_orderbook::BinanceDepthSnapshot, String>>>().await,
                }
            } => {
                snapshot_request = None;
                let snapshot = snapshot
                    .expect("snapshot branch only yields an active request")
                    .map_err(anyhow::Error::msg)?;
                match sync.apply_snapshot(snapshot) {
                    BinanceSnapshotOutcome::Synchronized => {
                        render_binance_deep_book(&mut renderer, &sync, config, &mut iteration, started_at)?;
                    }
                    BinanceSnapshotOutcome::NeedMoreEvents => {}
                    BinanceSnapshotOutcome::RefetchSnapshot => {
                        let snapshot_provider = provider.clone();
                        let snapshot_symbol = market_symbol.clone();
                        snapshot_request = Some(Box::pin(async move {
                            snapshot_provider
                                .fetch_order_book_snapshot(&snapshot_symbol)
                                .await
                        }));
                    }
                }
            }
            message = stream.next() => {
                let Some(message) = message else { break "websocket closed by peer"; };
                let message = message.context("websocket read error")?;
                match message {
                    Message::Ping(payload) => {
                        stream.send(Message::Pong(payload)).await.context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => break "websocket closed by peer",
                    message => {
                        let Some(text) = ws_message_text(message)? else { continue; };
                        let event = parse_binance_depth_event(&text)
                            .map_err(anyhow::Error::msg)?;
                        let Some(event) = event else { continue; };
                        match sync.push_event(event) {
                            BinanceEventOutcome::Updated => {
                                render_binance_deep_book(&mut renderer, &sync, config, &mut iteration, started_at)?;
                            }
                            BinanceEventOutcome::Buffered => {
                                match sync.try_pending_snapshot() {
                                    BinanceSnapshotOutcome::Synchronized => {
                                        render_binance_deep_book(&mut renderer, &sync, config, &mut iteration, started_at)?;
                                    }
                                    BinanceSnapshotOutcome::RefetchSnapshot if snapshot_request.is_none() => {
                                        let snapshot_provider = provider.clone();
                                        let snapshot_symbol = market_symbol.clone();
                                        snapshot_request = Some(Box::pin(async move {
                                            snapshot_provider
                                                .fetch_order_book_snapshot(&snapshot_symbol)
                                                .await
                                        }));
                                    }
                                    BinanceSnapshotOutcome::NeedMoreEvents
                                    | BinanceSnapshotOutcome::RefetchSnapshot => {}
                                }
                            }
                            BinanceEventOutcome::NeedsSnapshot if snapshot_request.is_none() => {
                                let snapshot_provider = provider.clone();
                                let snapshot_symbol = market_symbol.clone();
                                snapshot_request = Some(Box::pin(async move {
                                    snapshot_provider
                                        .fetch_order_book_snapshot(&snapshot_symbol)
                                        .await
                                }));
                            }
                            BinanceEventOutcome::NeedsSnapshot | BinanceEventOutcome::Ignored => {}
                        }
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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

fn render_binance_deep_book(
    renderer: &mut OrderBookRenderer,
    sync: &BinanceOrderBookSync,
    config: &Config,
    iteration: &mut u64,
    started_at: Instant,
) -> anyhow::Result<()> {
    let Some(book) = sync.to_ccxt(&config.symbol, config.orderbook_levels) else {
        return Ok(());
    };
    let book = OrderBookSnapshot {
        asks: book.asks,
        bids: book.bids,
        datetime: book.datetime,
        timestamp: book.timestamp,
        symbol: book.symbol,
    };
    *iteration += 1;
    renderer.render(&build_orderbook_frame(
        &book,
        config.orderbook_levels,
        *iteration,
        started_at.elapsed(),
    ))?;
    Ok(())
}

async fn run_binance_partial_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
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
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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

async fn run_bybit_orderbook_stream_ws(config: &Config) -> anyhow::Result<()> {
    let ws_endpoint = build_bybit_orderbook_ws_endpoint(config)?;
    let symbol = resolve_bybit_ws_symbol(config)?;
    let is_full = config.orderbook_levels > BYBIT_FULL_DEPTH_THRESHOLD;
    let depth = if is_full {
        config.orderbook_levels
    } else {
        to_bybit_ws_depth_levels(config.orderbook_levels)
    };
    let (mut stream, _) = connect_async(&ws_endpoint)
        .await
        .with_context(|| format!("failed to connect websocket {ws_endpoint}"))?;
    send_bybit_orderbook_subscription(&mut stream, &symbol, depth).await?;

    let mut renderer = OrderBookRenderer::new()?;
    let started_at = Instant::now();
    let mut iteration = 0u64;
    let mut limited_state: Option<OrderBookSnapshot> = None;
    let mut full_sync = BybitFullBookSync::new();
    let http_client = reqwest::Client::new();
    renderer.render(&build_orderbook_error_frame(
        iteration,
        started_at.elapsed(),
        &config.symbol,
        if is_full {
            "synchronizing Bybit full-depth orderbook"
        } else {
            "waiting for websocket orderbook updates"
        },
    ))?;

    let mut stop_check = tokio::time::interval(Duration::from_millis(50));
    stop_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    stop_check.tick().await;

    let stop_reason = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break "received Ctrl+C",
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
                        renderer.render(&build_orderbook_error_frame(
                            iteration,
                            started_at.elapsed(),
                            &config.symbol,
                            &format!("websocket read error: {err}"),
                        ))?;
                        continue;
                    }
                };

                match message {
                    Message::Ping(payload) => {
                        stream.send(Message::Pong(payload)).await.context("failed to reply to websocket ping")?;
                    }
                    Message::Close(_) => break "websocket closed by peer",
                    _ => {
                        let Some(text) = ws_message_text(message)? else {
                            continue;
                        };

                        let book = if is_full {
                            let Some(delta) = parse_bybit_full_delta(&text).map_err(anyhow::Error::msg)? else {
                                continue;
                            };
                            match full_sync.push_delta(delta) {
                                BufferDeltaOutcome::Restart => {
                                    bail!("Bybit full orderbook update sequence requires a reconnect and resync");
                                }
                                BufferDeltaOutcome::Ignored => continue,
                                BufferDeltaOutcome::Updated => full_sync.to_ccxt(&config.symbol, config.orderbook_levels),
                                BufferDeltaOutcome::Buffered => {
                                    let mut sync_outcome = full_sync.try_pending_snapshot();
                                    while matches!(sync_outcome, SnapshotSyncOutcome::RefetchSnapshot)
                                        || full_sync.needs_snapshot()
                                    {
                                        let snapshot = fetch_bybit_full_snapshot(
                                            &http_client,
                                            BYBIT_REST_BASE_URL,
                                            "linear",
                                            &symbol,
                                        )
                                        .await
                                        .map_err(anyhow::Error::msg)?;
                                        sync_outcome = full_sync.apply_snapshot(snapshot);
                                    }
                                    matches!(sync_outcome, SnapshotSyncOutcome::Synchronized)
                                        .then(|| full_sync.to_ccxt(&config.symbol, config.orderbook_levels))
                                        .flatten()
                                }
                            }
                            .map(|book| OrderBookSnapshot {
                                asks: book.asks,
                                bids: book.bids,
                                datetime: book.datetime,
                                timestamp: book.timestamp,
                                symbol: book.symbol,
                            })
                        } else {
                            let Some(event) = parse_bybit_orderbook_message(&text) else {
                                continue;
                            };
                            if !apply_bybit_orderbook_event(
                                &mut limited_state,
                                event,
                                config.orderbook_levels,
                                &config.symbol,
                            ) {
                                continue;
                            }
                            limited_state.clone()
                        };

                        let Some(book) = book else {
                            continue;
                        };
                        iteration += 1;
                        renderer.render(&build_orderbook_frame(
                            &book,
                            config.orderbook_levels,
                            iteration,
                            started_at.elapsed(),
                        ))?;
                        if should_stop(started_at, iteration, config) {
                            break "reached configured stop condition";
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
