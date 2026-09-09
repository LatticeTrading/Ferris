use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;

use crate::models::CcxtOrderBook;

pub const BINANCE_PARTIAL_DEPTH_THRESHOLD: usize = 20;
pub const BINANCE_MAX_ORDERBOOK_LEVELS: usize = 1_000;
pub const BINANCE_FUTURES_WS_BASE_URL: &str = "wss://fstream.binance.com/ws";

pub fn to_binance_ws_depth_levels(levels: usize) -> usize {
    if levels <= 5 {
        5
    } else if levels <= 10 {
        10
    } else {
        20
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceOrderBookStreamKind {
    Partial { depth: usize },
    Diff,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceOrderBookStream {
    pub ws_endpoint: String,
    pub kind: BinanceOrderBookStreamKind,
    pub display_levels: usize,
}

pub fn resolve_binance_orderbook_stream(
    base_url: &str,
    market_symbol: &str,
    requested_levels: usize,
) -> BinanceOrderBookStream {
    let display_levels = requested_levels.clamp(1, BINANCE_MAX_ORDERBOOK_LEVELS);
    let base_url = base_url.trim_end_matches('/');
    let market_symbol = market_symbol.to_ascii_lowercase();

    if display_levels <= BINANCE_PARTIAL_DEPTH_THRESHOLD {
        let depth = to_binance_ws_depth_levels(display_levels);
        BinanceOrderBookStream {
            ws_endpoint: format!("{base_url}/{market_symbol}@depth{depth}@100ms"),
            kind: BinanceOrderBookStreamKind::Partial { depth },
            display_levels,
        }
    } else {
        BinanceOrderBookStream {
            ws_endpoint: format!("{base_url}/{market_symbol}@depth@100ms"),
            kind: BinanceOrderBookStreamKind::Diff,
            display_levels,
        }
    }
}

pub fn parse_binance_partial_orderbook(
    payload: &str,
    symbol: &str,
    levels_limit: usize,
) -> Option<CcxtOrderBook> {
    let value = serde_json::from_str::<Value>(payload).ok()?;
    let data = value.get("data").unwrap_or(&value);
    if data.get("e").and_then(Value::as_str) != Some("depthUpdate") {
        return None;
    }

    let timestamp = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(parse_u64_lossy);
    let mut bids = parse_partial_side(data.get("b"), levels_limit);
    let mut asks = parse_partial_side(data.get("a"), levels_limit);
    bids.sort_by(|left, right| right.0.total_cmp(&left.0));
    asks.sort_by(|left, right| left.0.total_cmp(&right.0));
    bids.truncate(levels_limit);
    asks.truncate(levels_limit);

    Some(CcxtOrderBook {
        asks,
        bids,
        datetime: timestamp.and_then(iso8601_millis),
        timestamp,
        nonce: None,
        symbol: Some(symbol.to_string()),
    })
}

fn parse_partial_side(value: Option<&Value>, limit: usize) -> Vec<(f64, f64)> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };

    levels
        .iter()
        .take(limit)
        .filter_map(|row| {
            let row = row.as_array()?;
            Some((
                row.first().and_then(parse_f64_lossy)?,
                row.get(1).and_then(parse_f64_lossy)?,
            ))
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct BinancePrice {
    canonical: String,
    integer_len: usize,
    value: f64,
}

impl BinancePrice {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let bytes = raw.as_bytes();
        if bytes.is_empty() || bytes.iter().filter(|byte| **byte == b'.').count() > 1 {
            return Err(format!("invalid Binance decimal price `{raw}`"));
        }
        if bytes
            .iter()
            .any(|byte| !byte.is_ascii_digit() && *byte != b'.')
        {
            return Err(format!("invalid Binance decimal price `{raw}`"));
        }

        let decimal = bytes.iter().position(|byte| *byte == b'.');
        let integer_end = decimal.unwrap_or(bytes.len());
        if integer_end == 0 || decimal.is_some_and(|index| index + 1 == bytes.len()) {
            return Err(format!("invalid Binance decimal price `{raw}`"));
        }

        let integer = raw[..integer_end].trim_start_matches('0');
        let integer = if integer.is_empty() { "0" } else { integer };
        let fraction = decimal
            .map(|index| raw[index + 1..].trim_end_matches('0'))
            .unwrap_or_default();
        let canonical = if fraction.is_empty() {
            integer.to_string()
        } else {
            format!("{integer}.{fraction}")
        };
        let value = raw
            .parse::<f64>()
            .map_err(|_| format!("invalid Binance decimal price `{raw}`"))?;
        if !value.is_finite() || value <= 0.0 {
            return Err(format!(
                "Binance price must be finite and positive, got `{raw}`"
            ));
        }

        Ok(Self {
            integer_len: integer.len(),
            canonical,
            value,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn as_f64(&self) -> f64 {
        self.value
    }

    fn numeric_cmp(&self, other: &Self) -> Ordering {
        self.integer_len
            .cmp(&other.integer_len)
            .then_with(|| {
                self.canonical.as_bytes()[..self.integer_len]
                    .cmp(&other.canonical.as_bytes()[..other.integer_len])
            })
            .then_with(|| {
                let left = self
                    .canonical
                    .as_bytes()
                    .get(self.integer_len + 1..)
                    .unwrap_or_default();
                let right = other
                    .canonical
                    .as_bytes()
                    .get(other.integer_len + 1..)
                    .unwrap_or_default();
                let max_len = left.len().max(right.len());
                for index in 0..max_len {
                    match left
                        .get(index)
                        .copied()
                        .unwrap_or(b'0')
                        .cmp(&right.get(index).copied().unwrap_or(b'0'))
                    {
                        Ordering::Equal => {}
                        ordering => return ordering,
                    }
                }
                Ordering::Equal
            })
    }
}

impl PartialEq for BinancePrice {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Eq for BinancePrice {}

impl PartialOrd for BinancePrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BinancePrice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.numeric_cmp(other)
    }
}

#[derive(Clone, Debug)]
pub struct BinanceDepthLevel {
    pub price: BinancePrice,
    pub quantity: f64,
}

impl BinanceDepthLevel {
    pub fn parse(price: &str, quantity: &str) -> Result<Self, String> {
        Ok(Self {
            price: BinancePrice::parse(price)?,
            quantity: parse_quantity(quantity)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BinanceDepthEvent {
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub previous_final_update_id: u64,
    pub bids: Vec<BinanceDepthLevel>,
    pub asks: Vec<BinanceDepthLevel>,
    pub timestamp: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BinanceDepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<BinanceDepthLevel>,
    pub asks: Vec<BinanceDepthLevel>,
    pub timestamp: Option<u64>,
}

pub fn parse_binance_depth_event(payload: &str) -> Result<Option<BinanceDepthEvent>, String> {
    let value = serde_json::from_str::<Value>(payload)
        .map_err(|error| format!("invalid Binance depth JSON: {error}"))?;
    let data = value.get("data").unwrap_or(&value);
    if data.get("e").and_then(Value::as_str) != Some("depthUpdate") {
        return Ok(None);
    }

    let first_update_id = required_numeric_u64(data, "U")?;
    let final_update_id = required_numeric_u64(data, "u")?;
    let previous_final_update_id = required_numeric_u64(data, "pu")?;
    if first_update_id > final_update_id {
        return Err(format!(
            "Binance depth event has U={first_update_id} greater than u={final_update_id}"
        ));
    }

    Ok(Some(BinanceDepthEvent {
        first_update_id,
        final_update_id,
        previous_final_update_id,
        bids: parse_depth_side(data.get("b"), "bid")?,
        asks: parse_depth_side(data.get("a"), "ask")?,
        timestamp: data
            .get("T")
            .or_else(|| data.get("E"))
            .and_then(Value::as_u64),
    }))
}

fn required_numeric_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("Binance depth event missing numeric `{field}`"))
}

fn parse_depth_side(value: Option<&Value>, side: &str) -> Result<Vec<BinanceDepthLevel>, String> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Binance depth event `{side}` side must be an array"))?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let row = row
            .as_array()
            .ok_or_else(|| format!("Binance depth {side} level must be an array"))?;
        if row.len() < 2 {
            return Err(format!(
                "Binance depth {side} level must contain price and quantity"
            ));
        }
        let price = row[0]
            .as_str()
            .ok_or_else(|| format!("Binance depth {side} price must be a decimal string"))?;
        let quantity = row[1]
            .as_str()
            .ok_or_else(|| format!("Binance depth {side} quantity must be a decimal string"))?;
        levels.push(BinanceDepthLevel::parse(price, quantity)?);
    }
    Ok(levels)
}

fn parse_quantity(raw: &str) -> Result<f64, String> {
    let bytes = raw.as_bytes();
    if bytes.is_empty()
        || bytes.iter().filter(|byte| **byte == b'.').count() > 1
        || bytes
            .iter()
            .any(|byte| !byte.is_ascii_digit() && *byte != b'.')
    {
        return Err(format!("invalid Binance quantity `{raw}`"));
    }
    let decimal = bytes.iter().position(|byte| *byte == b'.');
    if decimal == Some(0) || decimal.is_some_and(|index| index + 1 == bytes.len()) {
        return Err(format!("invalid Binance quantity `{raw}`"));
    }
    let quantity = raw
        .parse::<f64>()
        .map_err(|_| format!("invalid Binance quantity `{raw}`"))?;
    if !quantity.is_finite() || quantity < 0.0 {
        return Err(format!(
            "Binance quantity must be finite and non-negative, got `{raw}`"
        ));
    }
    Ok(quantity)
}

#[async_trait]
pub trait OrderBookSnapshotProvider: Send + Sync {
    async fn fetch_order_book_snapshot(
        &self,
        market_symbol: &str,
    ) -> Result<BinanceDepthSnapshot, String>;
}

#[derive(Clone, Debug)]
struct BinanceLiveBook {
    bids: BTreeMap<BinancePrice, f64>,
    asks: BTreeMap<BinancePrice, f64>,
    final_update_id: u64,
    timestamp: Option<u64>,
}

impl BinanceLiveBook {
    fn from_snapshot(snapshot: BinanceDepthSnapshot) -> Self {
        Self {
            bids: snapshot
                .bids
                .into_iter()
                .filter(|level| level.quantity > 0.0)
                .map(|level| (level.price, level.quantity))
                .collect(),
            asks: snapshot
                .asks
                .into_iter()
                .filter(|level| level.quantity > 0.0)
                .map(|level| (level.price, level.quantity))
                .collect(),
            final_update_id: snapshot.last_update_id,
            timestamp: snapshot.timestamp,
        }
    }

    fn apply(&mut self, event: BinanceDepthEvent) {
        apply_depth_levels(&mut self.bids, event.bids);
        apply_depth_levels(&mut self.asks, event.asks);
        self.final_update_id = event.final_update_id;
        self.timestamp = event.timestamp;
    }

    fn to_ccxt(&self, symbol: &str, levels_limit: usize) -> CcxtOrderBook {
        CcxtOrderBook {
            asks: self
                .asks
                .iter()
                .take(levels_limit)
                .map(|(price, quantity)| (price.as_f64(), *quantity))
                .collect(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(levels_limit)
                .map(|(price, quantity)| (price.as_f64(), *quantity))
                .collect(),
            datetime: self.timestamp.and_then(iso8601_millis),
            timestamp: self.timestamp,
            nonce: Some(self.final_update_id),
            symbol: Some(symbol.to_string()),
        }
    }
}

fn apply_depth_levels(book: &mut BTreeMap<BinancePrice, f64>, updates: Vec<BinanceDepthLevel>) {
    for update in updates {
        if update.quantity == 0.0 {
            book.remove(&update.price);
        } else {
            book.insert(update.price, update.quantity);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceEventOutcome {
    Buffered,
    Updated,
    Ignored,
    NeedsSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceSnapshotOutcome {
    Synchronized,
    NeedMoreEvents,
    RefetchSnapshot,
}

#[derive(Debug, Default)]
pub struct BinanceOrderBookSync {
    buffered: VecDeque<BinanceDepthEvent>,
    pending_snapshot: Option<BinanceDepthSnapshot>,
    book: Option<BinanceLiveBook>,
}

impl BinanceOrderBookSync {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn needs_snapshot(&self) -> bool {
        self.book.is_none() && self.pending_snapshot.is_none()
    }

    pub fn try_pending_snapshot(&mut self) -> BinanceSnapshotOutcome {
        let Some(snapshot) = self.pending_snapshot.take() else {
            return BinanceSnapshotOutcome::NeedMoreEvents;
        };
        self.apply_snapshot(snapshot)
    }

    pub fn push_event(&mut self, event: BinanceDepthEvent) -> BinanceEventOutcome {
        if let Some(book) = self.book.as_mut() {
            if event.final_update_id <= book.final_update_id {
                return BinanceEventOutcome::Ignored;
            }
            if event.previous_final_update_id != book.final_update_id {
                self.book = None;
                self.pending_snapshot = None;
                self.buffered.clear();
                self.buffered.push_back(event);
                return BinanceEventOutcome::NeedsSnapshot;
            }
            book.apply(event);
            return BinanceEventOutcome::Updated;
        }

        if let Some(previous) = self.buffered.back() {
            if event.final_update_id <= previous.final_update_id {
                return BinanceEventOutcome::Ignored;
            }
            if event.previous_final_update_id != previous.final_update_id {
                self.buffered.clear();
                self.pending_snapshot = None;
                self.buffered.push_back(event);
                return BinanceEventOutcome::NeedsSnapshot;
            }
        }

        self.buffered.push_back(event);
        BinanceEventOutcome::Buffered
    }

    pub fn apply_snapshot(&mut self, snapshot: BinanceDepthSnapshot) -> BinanceSnapshotOutcome {
        while self
            .buffered
            .front()
            .is_some_and(|event| event.final_update_id < snapshot.last_update_id)
        {
            self.buffered.pop_front();
        }

        let Some(first) = self.buffered.front() else {
            self.pending_snapshot = Some(snapshot);
            return BinanceSnapshotOutcome::NeedMoreEvents;
        };
        if first.first_update_id > snapshot.last_update_id {
            self.pending_snapshot = None;
            return BinanceSnapshotOutcome::RefetchSnapshot;
        }

        let bridge = self
            .buffered
            .pop_front()
            .expect("front was checked immediately above");
        let mut candidate = BinanceLiveBook::from_snapshot(snapshot);
        candidate.apply(bridge);

        while let Some(event) = self.buffered.pop_front() {
            if event.final_update_id <= candidate.final_update_id {
                continue;
            }
            if event.previous_final_update_id != candidate.final_update_id {
                let mut retained = VecDeque::new();
                retained.push_back(event);
                retained.append(&mut self.buffered);
                self.buffered = retained;
                self.book = None;
                self.pending_snapshot = None;
                return BinanceSnapshotOutcome::RefetchSnapshot;
            }
            candidate.apply(event);
        }

        self.pending_snapshot = None;
        self.book = Some(candidate);
        BinanceSnapshotOutcome::Synchronized
    }

    pub fn to_ccxt(&self, symbol: &str, levels_limit: usize) -> Option<CcxtOrderBook> {
        self.book
            .as_ref()
            .map(|book| book.to_ccxt(symbol, levels_limit))
    }
}

fn parse_u64_lossy(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
}

fn parse_f64_lossy(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn level(price: &str, quantity: &str) -> BinanceDepthLevel {
        BinanceDepthLevel::parse(price, quantity).expect("level should parse")
    }

    fn event(
        first_update_id: u64,
        final_update_id: u64,
        previous_final_update_id: u64,
        bids: Vec<BinanceDepthLevel>,
        asks: Vec<BinanceDepthLevel>,
    ) -> BinanceDepthEvent {
        BinanceDepthEvent {
            first_update_id,
            final_update_id,
            previous_final_update_id,
            bids,
            asks,
            timestamp: Some(final_update_id),
        }
    }

    fn snapshot(last_update_id: u64) -> BinanceDepthSnapshot {
        BinanceDepthSnapshot {
            last_update_id,
            bids: vec![level("100.00", "1"), level("99", "2"), level("98", "3")],
            asks: vec![level("101", "1"), level("102", "2")],
            timestamp: Some(last_update_id),
        }
    }

    #[test]
    fn buckets_partial_depths() {
        for (requested, expected) in [(1, 5), (5, 5), (6, 10), (10, 10), (11, 20), (20, 20)] {
            assert_eq!(to_binance_ws_depth_levels(requested), expected);
        }
    }

    #[test]
    fn resolves_partial_and_diff_endpoints() {
        let partial = resolve_binance_orderbook_stream("wss://example/ws/", "BTCUSDT", 20);
        assert_eq!(
            partial.kind,
            BinanceOrderBookStreamKind::Partial { depth: 20 }
        );
        assert!(partial.ws_endpoint.ends_with("/btcusdt@depth20@100ms"));

        for requested in [21, 50, 1_000] {
            let deep = resolve_binance_orderbook_stream("wss://example/ws", "BTCUSDT", requested);
            assert_eq!(deep.kind, BinanceOrderBookStreamKind::Diff);
            assert!(deep.ws_endpoint.ends_with("/btcusdt@depth@100ms"));
            assert!(!deep.ws_endpoint.contains("depth1000"));
            assert_eq!(deep.display_levels, requested);
        }
    }

    #[test]
    fn parses_raw_and_combined_partial_snapshots_without_nonce() {
        let data = json!({
            "e": "depthUpdate",
            "E": 10,
            "T": 11,
            "b": [["99", "1"], ["100", "2"]],
            "a": [["102", "1"], ["101", "2"]]
        });
        for payload in [data.to_string(), json!({"data": data}).to_string()] {
            let book = parse_binance_partial_orderbook(&payload, "BTC/USDT:USDT", 2)
                .expect("partial book should parse");
            assert_eq!(book.bids, vec![(100.0, 2.0), (99.0, 1.0)]);
            assert_eq!(book.asks, vec![(101.0, 2.0), (102.0, 1.0)]);
            assert_eq!(book.timestamp, Some(11));
            assert_eq!(book.nonce, None);
        }
    }

    #[test]
    fn parses_raw_and_combined_diff_events_strictly() {
        let data = json!({
            "e": "depthUpdate",
            "E": 10,
            "T": 11,
            "U": 100,
            "u": 101,
            "pu": 99,
            "b": [["100.00", "1.5"]],
            "a": [["101", "0"]]
        });
        for payload in [
            data.to_string(),
            json!({"stream": "x", "data": data}).to_string(),
        ] {
            let parsed = parse_binance_depth_event(&payload)
                .expect("event should be valid")
                .expect("event should be recognized");
            assert_eq!(parsed.first_update_id, 100);
            assert_eq!(parsed.final_update_id, 101);
            assert_eq!(parsed.previous_final_update_id, 99);
            assert_eq!(parsed.timestamp, Some(11));
            assert_eq!(parsed.bids[0].price.as_str(), "100");
            assert_eq!(parsed.asks[0].quantity, 0.0);
        }
        assert!(parse_binance_depth_event(r#"{"e":"ping"}"#)
            .expect("non-depth message is valid")
            .is_none());
    }

    #[test]
    fn rejects_invalid_diff_fields_and_levels() {
        let valid = json!({
            "e": "depthUpdate", "U": 10, "u": 11, "pu": 9,
            "b": [["100", "1"]], "a": []
        });
        for field in ["U", "u", "pu"] {
            let mut missing = valid.clone();
            missing.as_object_mut().expect("object").remove(field);
            assert!(parse_binance_depth_event(&missing.to_string()).is_err());
            let mut string_id = valid.clone();
            string_id[field] = json!("10");
            assert!(parse_binance_depth_event(&string_id.to_string()).is_err());
        }
        let mut reversed = valid.clone();
        reversed["U"] = json!(12);
        assert!(parse_binance_depth_event(&reversed.to_string()).is_err());
        for bad_bids in [
            json!(["not-a-row"]),
            json!([["100"]]),
            json!([["-100", "1"]]),
            json!([["100", "-1"]]),
            json!([["100", "NaN"]]),
        ] {
            let mut malformed = valid.clone();
            malformed["b"] = bad_bids;
            assert!(parse_binance_depth_event(&malformed.to_string()).is_err());
        }
    }

    #[test]
    fn exact_prices_canonicalize_and_order_numerically() {
        let one = BinancePrice::parse("00100.00").expect("price should parse");
        let same = BinancePrice::parse("100.0").expect("price should parse");
        let higher = BinancePrice::parse("100.0000000000001").expect("price should parse");
        assert_eq!(one, same);
        assert_eq!(one.as_str(), "100");
        assert!(one < higher);
        for invalid in ["", ".1", "1.", "-1", "+1", "1e2", "0", "0.00", "NaN"] {
            assert!(
                BinancePrice::parse(invalid).is_err(),
                "accepted `{invalid}`"
            );
        }
    }

    #[test]
    fn waits_for_bridge_and_never_publishes_snapshot_only() {
        let mut sync = BinanceOrderBookSync::new();
        assert_eq!(
            sync.apply_snapshot(snapshot(10)),
            BinanceSnapshotOutcome::NeedMoreEvents
        );
        assert!(sync.to_ccxt("BTC/USDT:USDT", 10).is_none());
        assert!(!sync.needs_snapshot());
        assert_eq!(
            sync.push_event(event(10, 11, 9, vec![], vec![])),
            BinanceEventOutcome::Buffered
        );
        assert_eq!(
            sync.try_pending_snapshot(),
            BinanceSnapshotOutcome::Synchronized
        );
        assert_eq!(sync.to_ccxt("BTC/USDT:USDT", 10).unwrap().nonce, Some(11));
    }

    #[test]
    fn discards_stale_events_and_applies_buffered_chain() {
        let mut sync = BinanceOrderBookSync::new();
        sync.push_event(event(8, 9, 7, vec![level("97", "4")], vec![]));
        sync.push_event(event(9, 10, 9, vec![level("100", "2")], vec![]));
        sync.push_event(event(11, 11, 10, vec![level("99", "0")], vec![]));
        assert_eq!(
            sync.apply_snapshot(snapshot(10)),
            BinanceSnapshotOutcome::Synchronized
        );
        let book = sync
            .to_ccxt("BTC/USDT:USDT", 10)
            .expect("book should exist");
        assert_eq!(book.bids, vec![(100.0, 2.0), (98.0, 3.0)]);
        assert_eq!(book.nonce, Some(11));
    }

    #[test]
    fn stale_snapshot_requires_refetch() {
        let mut sync = BinanceOrderBookSync::new();
        sync.push_event(event(12, 13, 11, vec![], vec![]));
        assert_eq!(
            sync.apply_snapshot(snapshot(10)),
            BinanceSnapshotOutcome::RefetchSnapshot
        );
        assert!(sync.to_ccxt("BTC/USDT:USDT", 10).is_none());
    }

    #[test]
    fn live_events_ignore_stale_invalidate_gaps_and_delete_levels() {
        let mut sync = BinanceOrderBookSync::new();
        sync.push_event(event(10, 10, 9, vec![], vec![]));
        assert_eq!(
            sync.apply_snapshot(snapshot(10)),
            BinanceSnapshotOutcome::Synchronized
        );
        assert_eq!(
            sync.push_event(event(10, 10, 9, vec![], vec![])),
            BinanceEventOutcome::Ignored
        );
        assert_eq!(
            sync.push_event(event(11, 11, 10, vec![level("100.0", "0")], vec![])),
            BinanceEventOutcome::Updated
        );
        let book = sync.to_ccxt("BTC/USDT:USDT", 2).expect("book should exist");
        assert_eq!(book.bids, vec![(99.0, 2.0), (98.0, 3.0)]);
        assert_eq!(book.nonce, Some(11));

        assert_eq!(
            sync.push_event(event(13, 13, 12, vec![], vec![])),
            BinanceEventOutcome::NeedsSnapshot
        );
        assert!(sync.to_ccxt("BTC/USDT:USDT", 10).is_none());
        assert!(sync.needs_snapshot());
    }

    #[test]
    fn buffered_gap_retains_new_chain_and_refetches() {
        let mut sync = BinanceOrderBookSync::new();
        sync.push_event(event(10, 10, 9, vec![], vec![]));
        assert_eq!(
            sync.push_event(event(12, 12, 11, vec![], vec![])),
            BinanceEventOutcome::NeedsSnapshot
        );
        assert_eq!(
            sync.push_event(event(13, 13, 12, vec![], vec![])),
            BinanceEventOutcome::Buffered
        );
        assert_eq!(
            sync.apply_snapshot(snapshot(11)),
            BinanceSnapshotOutcome::RefetchSnapshot
        );
    }
}
