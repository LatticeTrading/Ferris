use std::{
    collections::{HashSet, VecDeque},
    fmt::Write as FmtWrite,
    io::{self, IsTerminal, Stdout, Write},
    time::Duration,
};

use anyhow::Context;
use chrono::{SecondsFormat, Utc};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    style::Print,
    terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand, QueueableCommand,
};
use serde::Deserialize;

pub(crate) struct OrderBookRenderer {
    stdout: Stdout,
    previous_line_count: usize,
    interactive: bool,
}

impl OrderBookRenderer {
    pub(crate) fn new() -> anyhow::Result<Self> {
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

    pub(crate) fn render(&mut self, frame: &str) -> anyhow::Result<()> {
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeRow {
    pub(crate) id: Option<String>,
    pub(crate) timestamp: Option<u64>,
    pub(crate) datetime: Option<String>,
    pub(crate) side: Option<String>,
    pub(crate) price: Option<f64>,
    pub(crate) amount: Option<f64>,
    pub(crate) cost: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderBookSnapshot {
    pub(crate) asks: Vec<(f64, f64)>,
    pub(crate) bids: Vec<(f64, f64)>,
    pub(crate) datetime: Option<String>,
    pub(crate) timestamp: Option<u64>,
    pub(crate) symbol: Option<String>,
}

#[derive(Debug)]
pub(crate) struct BookStats {
    pub(crate) best_bid: Option<f64>,
    pub(crate) best_ask: Option<f64>,
    pub(crate) spread: Option<f64>,
    pub(crate) mid: Option<f64>,
    pub(crate) spread_bps: Option<f64>,
}

pub(crate) fn compute_book_stats(book: &OrderBookSnapshot) -> BookStats {
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
pub(crate) struct BookLevelRow {
    pub(crate) price: f64,
    pub(crate) size: f64,
    pub(crate) cumulative_size: f64,
}

pub(crate) fn build_level_rows(levels: &[(f64, f64)], depth: usize) -> Vec<BookLevelRow> {
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

pub(crate) fn build_orderbook_frame(
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

pub(crate) fn build_orderbook_error_frame(
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

pub(crate) fn normalize_book_levels(
    book: &OrderBookSnapshot,
) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
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

pub(crate) fn format_depth_bar(value: f64, max_value: f64, width: usize) -> String {
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

pub(crate) fn format_trade_line(trade: &TradeRow) -> String {
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

pub(crate) fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn format_f64(value: Option<f64>) -> String {
    value
        .map(|number| format!("{number:.6}"))
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn trade_key(trade: &TradeRow) -> String {
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
pub(crate) struct TradeDeduper {
    set: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl TradeDeduper {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    pub(crate) fn insert(&mut self, key: String) -> bool {
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
