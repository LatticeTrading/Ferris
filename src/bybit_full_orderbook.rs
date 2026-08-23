use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};

use chrono::{SecondsFormat, Utc};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use crate::models::CcxtOrderBook;

pub const BYBIT_FULL_DEPTH_THRESHOLD: usize = 1_000;
pub const BYBIT_MAX_FULL_DEPTH: usize = 10_000;
pub const BYBIT_REST_BASE_URL: &str = "https://api.bybit.com";

#[derive(Clone, Debug)]
pub struct ExactPrice {
    raw: String,
    integer_start: usize,
    integer_end: usize,
    fraction_start: usize,
    fraction_end: usize,
    value: f64,
}

impl ExactPrice {
    fn parse(value: &Value) -> Result<Self, String> {
        let raw = match value {
            Value::String(value) => value.clone(),
            Value::Number(value) => value.to_string(),
            _ => return Err("price must be a decimal string or number".to_string()),
        };

        let bytes = raw.as_bytes();
        if bytes.is_empty() || bytes.iter().filter(|byte| **byte == b'.').count() > 1 {
            return Err(format!("invalid decimal price `{raw}`"));
        }
        if bytes
            .iter()
            .any(|byte| !byte.is_ascii_digit() && *byte != b'.')
        {
            return Err(format!("invalid decimal price `{raw}`"));
        }

        let decimal = bytes.iter().position(|byte| *byte == b'.');
        let integer_end = decimal.unwrap_or(bytes.len());
        if integer_end == 0 {
            return Err(format!("invalid decimal price `{raw}`"));
        }
        let fraction_start = decimal.map_or(bytes.len(), |index| index + 1);
        if decimal.is_some() && fraction_start == bytes.len() {
            return Err(format!("invalid decimal price `{raw}`"));
        }

        let mut integer_start = 0;
        while integer_start + 1 < integer_end && bytes[integer_start] == b'0' {
            integer_start += 1;
        }
        let mut fraction_end = bytes.len();
        while fraction_end > fraction_start && bytes[fraction_end - 1] == b'0' {
            fraction_end -= 1;
        }

        let value = raw
            .parse::<f64>()
            .map_err(|_| format!("invalid decimal price `{raw}`"))?;
        if !value.is_finite() || value <= 0.0 {
            return Err(format!("price must be finite and positive, got `{raw}`"));
        }

        Ok(Self {
            raw,
            integer_start,
            integer_end,
            fraction_start,
            fraction_end,
            value,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn as_f64(&self) -> f64 {
        self.value
    }

    fn numeric_cmp(&self, other: &Self) -> Ordering {
        let left_integer = &self.raw.as_bytes()[self.integer_start..self.integer_end];
        let right_integer = &other.raw.as_bytes()[other.integer_start..other.integer_end];

        left_integer
            .len()
            .cmp(&right_integer.len())
            .then_with(|| left_integer.cmp(right_integer))
            .then_with(|| {
                let left_fraction = &self.raw.as_bytes()[self.fraction_start..self.fraction_end];
                let right_fraction =
                    &other.raw.as_bytes()[other.fraction_start..other.fraction_end];
                let max_len = left_fraction.len().max(right_fraction.len());

                for index in 0..max_len {
                    let left = left_fraction.get(index).copied().unwrap_or(b'0');
                    let right = right_fraction.get(index).copied().unwrap_or(b'0');
                    match left.cmp(&right) {
                        Ordering::Equal => {}
                        ordering => return ordering,
                    }
                }

                Ordering::Equal
            })
    }
}

impl PartialEq for ExactPrice {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for ExactPrice {}

impl PartialOrd for ExactPrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExactPrice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.numeric_cmp(other)
            .then_with(|| self.raw.cmp(&other.raw))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<Value>,
}

pub async fn fetch_bybit_full_snapshot(
    http_client: &reqwest::Client,
    base_url: &str,
    category: &str,
    symbol: &str,
) -> Result<BybitFullSnapshot, String> {
    let endpoint = format!(
        "{}/v5/market/full_orderbook",
        base_url.trim_end_matches('/')
    );
    let response = http_client
        .get(&endpoint)
        .query(&[("category", category), ("symbol", symbol)])
        .send()
        .await
        .map_err(|error| format!("Bybit full snapshot request failed: {error}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("failed to read Bybit full snapshot response: {error}"))?;
    if status != StatusCode::OK {
        return Err(format!(
            "Bybit full snapshot returned status={status} body={}",
            truncate(&body, 240)
        ));
    }

    let envelope = serde_json::from_str::<BybitEnvelope>(&body).map_err(|error| {
        format!(
            "failed to parse Bybit full snapshot response: {error}; body={}",
            truncate(&body, 240)
        )
    })?;
    if envelope.ret_code != 0 {
        return Err(format!(
            "Bybit full snapshot rejected with code={}: {}",
            envelope.ret_code, envelope.ret_msg
        ));
    }

    BybitFullSnapshot::from_result(
        envelope
            .result
            .as_ref()
            .ok_or_else(|| "Bybit full snapshot response missing `result`".to_string())?,
    )
}

#[derive(Clone, Debug)]
pub struct BybitFullLevel {
    pub price: ExactPrice,
    pub size: f64,
}

#[derive(Clone, Debug)]
pub struct BybitFullSnapshot {
    pub bids: Vec<BybitFullLevel>,
    pub asks: Vec<BybitFullLevel>,
    pub timestamp: Option<u64>,
    pub update_id: u64,
    pub sequence: u64,
}

impl BybitFullSnapshot {
    pub fn from_result(result: &Value) -> Result<Self, String> {
        Ok(Self {
            bids: parse_levels(result.get("b"), "bid")?,
            asks: parse_levels(result.get("a"), "ask")?,
            timestamp: result.get("ts").and_then(parse_u64_lossy),
            update_id: required_u64(result, "u")?,
            sequence: required_u64(result, "seq")?,
        })
    }

    pub fn to_ccxt(&self, symbol: &str, levels_limit: usize) -> CcxtOrderBook {
        let mut bids = self
            .bids
            .iter()
            .map(|level| (level.price.as_f64(), level.size))
            .collect::<Vec<_>>();
        let mut asks = self
            .asks
            .iter()
            .map(|level| (level.price.as_f64(), level.size))
            .collect::<Vec<_>>();

        bids.sort_by(|left, right| right.0.total_cmp(&left.0));
        asks.sort_by(|left, right| left.0.total_cmp(&right.0));
        bids.truncate(levels_limit);
        asks.truncate(levels_limit);

        CcxtOrderBook {
            asks,
            bids,
            datetime: self.timestamp.and_then(iso8601_millis),
            timestamp: self.timestamp,
            nonce: Some(self.update_id),
            symbol: Some(symbol.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BybitFullDelta {
    pub bids: Vec<BybitFullLevel>,
    pub asks: Vec<BybitFullLevel>,
    pub timestamp: Option<u64>,
    pub update_id: u64,
    pub sequence: u64,
}

pub fn parse_bybit_full_delta(payload: &str) -> Result<Option<BybitFullDelta>, String> {
    let value = serde_json::from_str::<Value>(payload)
        .map_err(|error| format!("invalid Bybit full orderbook JSON: {error}"))?;

    let Some(topic) = value.get("topic").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !topic.starts_with("orderbook.full.") {
        return Ok(None);
    }
    if value.get("type").and_then(Value::as_str) != Some("delta") {
        return Err(format!(
            "Bybit full orderbook topic `{topic}` must contain a delta event"
        ));
    }

    let data = value
        .get("data")
        .ok_or_else(|| "Bybit full orderbook delta missing `data`".to_string())?;

    Ok(Some(BybitFullDelta {
        bids: parse_levels(data.get("b"), "bid")?,
        asks: parse_levels(data.get("a"), "ask")?,
        timestamp: value
            .get("ts")
            .and_then(parse_u64_lossy)
            .or_else(|| data.get("cts").and_then(parse_u64_lossy)),
        update_id: required_u64(data, "u")?,
        sequence: required_u64(data, "seq")?,
    }))
}

fn parse_levels(value: Option<&Value>, side: &str) -> Result<Vec<BybitFullLevel>, String> {
    let Some(rows) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let row = row
            .as_array()
            .ok_or_else(|| format!("Bybit full orderbook {side} level is not an array"))?;
        if row.len() < 2 {
            return Err(format!(
                "Bybit full orderbook {side} level must contain price and size"
            ));
        }

        let price = ExactPrice::parse(&row[0])?;
        let size = parse_f64_lossy(&row[1])
            .ok_or_else(|| format!("invalid Bybit full orderbook {side} size"))?;
        if !size.is_finite() || size < 0.0 {
            return Err(format!(
                "Bybit full orderbook {side} size must be finite and non-negative"
            ));
        }

        levels.push(BybitFullLevel { price, size });
    }

    Ok(levels)
}

#[derive(Clone, Debug)]
struct BybitFullBook {
    bids: BTreeMap<ExactPrice, f64>,
    asks: BTreeMap<ExactPrice, f64>,
    timestamp: Option<u64>,
    update_id: u64,
    sequence: u64,
}

impl BybitFullBook {
    fn from_snapshot(snapshot: BybitFullSnapshot) -> Self {
        let bids = snapshot
            .bids
            .into_iter()
            .filter(|level| level.size > 0.0)
            .map(|level| (level.price, level.size))
            .collect();
        let asks = snapshot
            .asks
            .into_iter()
            .filter(|level| level.size > 0.0)
            .map(|level| (level.price, level.size))
            .collect();

        Self {
            bids,
            asks,
            timestamp: snapshot.timestamp,
            update_id: snapshot.update_id,
            sequence: snapshot.sequence,
        }
    }

    fn apply(&mut self, delta: BybitFullDelta) {
        apply_levels(&mut self.bids, delta.bids);
        apply_levels(&mut self.asks, delta.asks);
        self.timestamp = delta.timestamp;
        self.update_id = delta.update_id;
        self.sequence = delta.sequence;
    }

    fn to_ccxt(&self, symbol: &str, levels_limit: usize) -> CcxtOrderBook {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(levels_limit)
            .map(|(price, size)| (price.as_f64(), *size))
            .collect();
        let asks = self
            .asks
            .iter()
            .take(levels_limit)
            .map(|(price, size)| (price.as_f64(), *size))
            .collect();

        CcxtOrderBook {
            asks,
            bids,
            datetime: self.timestamp.and_then(iso8601_millis),
            timestamp: self.timestamp,
            nonce: Some(self.update_id),
            symbol: Some(symbol.to_string()),
        }
    }
}

fn apply_levels(book: &mut BTreeMap<ExactPrice, f64>, updates: Vec<BybitFullLevel>) {
    for update in updates {
        if update.size == 0.0 {
            book.remove(&update.price);
        } else {
            book.insert(update.price, update.size);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferDeltaOutcome {
    Buffered,
    Updated,
    Ignored,
    Restart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotSyncOutcome {
    Synchronized,
    NeedMoreDeltas,
    RefetchSnapshot,
}

#[derive(Debug, Default)]
pub struct BybitFullBookSync {
    buffered: VecDeque<BybitFullDelta>,
    pending_snapshot: Option<BybitFullSnapshot>,
    book: Option<BybitFullBook>,
}

impl BybitFullBookSync {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_synchronized(&self) -> bool {
        self.book.is_some()
    }

    pub fn buffered_len(&self) -> usize {
        self.buffered.len()
    }
    pub fn needs_snapshot(&self) -> bool {
        self.book.is_none() && self.pending_snapshot.is_none()
    }

    pub fn try_pending_snapshot(&mut self) -> SnapshotSyncOutcome {
        let Some(snapshot) = self.pending_snapshot.take() else {
            return SnapshotSyncOutcome::NeedMoreDeltas;
        };
        self.apply_snapshot(snapshot)
    }

    pub fn push_delta(&mut self, delta: BybitFullDelta) -> BufferDeltaOutcome {
        if let Some(book) = self.book.as_mut() {
            if delta.update_id == 1 {
                return BufferDeltaOutcome::Restart;
            }
            if delta.sequence < book.sequence || delta.update_id <= book.update_id {
                return BufferDeltaOutcome::Ignored;
            }
            if delta.update_id != book.update_id.saturating_add(1) {
                return BufferDeltaOutcome::Restart;
            }

            book.apply(delta);
            return BufferDeltaOutcome::Updated;
        }

        if delta.update_id == 1 {
            self.buffered.clear();
            self.pending_snapshot = None;
        } else if let Some(previous) = self.buffered.back() {
            if delta.sequence < previous.sequence {
                return BufferDeltaOutcome::Ignored;
            }
            if delta.update_id != previous.update_id.saturating_add(1) {
                self.buffered.clear();
                self.pending_snapshot = None;
            }
        }

        self.buffered.push_back(delta);
        BufferDeltaOutcome::Buffered
    }

    pub fn apply_snapshot(&mut self, snapshot: BybitFullSnapshot) -> SnapshotSyncOutcome {
        let Some(first) = self.buffered.front() else {
            self.pending_snapshot = Some(snapshot);
            return SnapshotSyncOutcome::NeedMoreDeltas;
        };
        if snapshot.sequence < first.sequence {
            return SnapshotSyncOutcome::RefetchSnapshot;
        }

        while self
            .buffered
            .front()
            .is_some_and(|delta| delta.sequence < snapshot.sequence)
        {
            self.buffered.pop_front();
        }

        let Some(matching) = self.buffered.front() else {
            self.pending_snapshot = Some(snapshot);
            return SnapshotSyncOutcome::NeedMoreDeltas;
        };
        if matching.sequence > snapshot.sequence || matching.update_id != snapshot.update_id {
            return SnapshotSyncOutcome::RefetchSnapshot;
        }

        self.buffered.pop_front();
        let mut book = BybitFullBook::from_snapshot(snapshot);
        while let Some(delta) = self.buffered.pop_front() {
            if delta.sequence < book.sequence {
                continue;
            }
            if delta.update_id != book.update_id.saturating_add(1) {
                self.buffered.clear();
                self.book = None;
                return SnapshotSyncOutcome::RefetchSnapshot;
            }
            book.apply(delta);
        }
        self.pending_snapshot = None;
        self.book = Some(book);

        SnapshotSyncOutcome::Synchronized
    }

    pub fn to_ccxt(&self, symbol: &str, levels_limit: usize) -> Option<CcxtOrderBook> {
        self.book
            .as_ref()
            .map(|book| book.to_ccxt(symbol, levels_limit))
    }
}

fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(parse_u64_lossy)
        .ok_or_else(|| format!("Bybit full orderbook payload missing `{field}`"))
}

fn parse_u64_lossy(value: &Value) -> Option<u64> {
    match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn parse_f64_lossy(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn iso8601_millis(timestamp: u64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    value.chars().take(max_chars).collect::<String>() + "..."
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn delta(update_id: u64, sequence: u64, bids: Value, asks: Value) -> BybitFullDelta {
        BybitFullDelta {
            bids: parse_levels(Some(&bids), "bid").expect("bids should parse"),
            asks: parse_levels(Some(&asks), "ask").expect("asks should parse"),
            timestamp: Some(sequence),
            update_id,
            sequence,
        }
    }

    fn snapshot(update_id: u64, sequence: u64) -> BybitFullSnapshot {
        BybitFullSnapshot::from_result(&json!({
            "b": [["100.0", "1"], ["99.0", "2"], ["98.0", "3"]],
            "a": [["101.0", "1"], ["102.0", "2"]],
            "ts": "1700000000000",
            "u": update_id,
            "seq": sequence
        }))
        .expect("snapshot should parse")
    }

    #[test]
    fn parses_full_delta_without_websocket_snapshot() {
        let parsed = parse_bybit_full_delta(
            &json!({
                "topic": "orderbook.full.BTCUSDT",
                "type": "delta",
                "ts": 1700000000001_u64,
                "data": {
                    "s": "BTCUSDT",
                    "b": [["100.0", "2"]],
                    "a": [["101.0", "0"]],
                    "u": 11,
                    "seq": 101,
                    "cts": 1700000000000_u64
                }
            })
            .to_string(),
        )
        .expect("delta should be valid")
        .expect("delta should be recognized");

        assert_eq!(parsed.update_id, 11);
        assert_eq!(parsed.sequence, 101);
        assert_eq!(parsed.bids[0].price.as_str(), "100.0");
    }

    #[test]
    fn synchronizes_matching_snapshot_and_buffered_deltas() {
        let mut sync = BybitFullBookSync::new();
        assert_eq!(
            sync.push_delta(delta(10, 100, json!([["100.0", "1.5"]]), json!([]))),
            BufferDeltaOutcome::Buffered
        );
        sync.push_delta(delta(11, 102, json!([["99.0", "0"]]), json!([])));

        assert_eq!(
            sync.apply_snapshot(snapshot(10, 100)),
            SnapshotSyncOutcome::Synchronized
        );
        let book = sync.to_ccxt("BTC/USDT:USDT", 3).expect("book should exist");
        assert_eq!(book.bids, vec![(100.0, 1.0), (98.0, 3.0)]);
        assert_eq!(book.nonce, Some(11));
    }

    #[test]
    fn rejects_snapshot_older_than_first_buffered_delta() {
        let mut sync = BybitFullBookSync::new();
        sync.push_delta(delta(11, 101, json!([]), json!([])));

        assert_eq!(
            sync.apply_snapshot(snapshot(10, 100)),
            SnapshotSyncOutcome::RefetchSnapshot
        );
        assert!(!sync.is_synchronized());
    }

    #[test]
    fn waits_when_snapshot_is_ahead_then_syncs_on_matching_delta() {
        let mut sync = BybitFullBookSync::new();
        sync.push_delta(delta(10, 100, json!([]), json!([])));

        assert_eq!(
            sync.apply_snapshot(snapshot(11, 102)),
            SnapshotSyncOutcome::NeedMoreDeltas
        );
        assert!(!sync.needs_snapshot());
        assert_eq!(
            sync.push_delta(delta(11, 102, json!([]), json!([]))),
            BufferDeltaOutcome::Buffered
        );
        assert_eq!(
            sync.try_pending_snapshot(),
            SnapshotSyncOutcome::Synchronized
        );
        assert!(sync.is_synchronized());
    }

    #[test]
    fn live_update_gap_and_reset_require_restart() {
        let mut sync = BybitFullBookSync::new();
        sync.push_delta(delta(10, 100, json!([]), json!([])));
        assert_eq!(
            sync.apply_snapshot(snapshot(10, 100)),
            SnapshotSyncOutcome::Synchronized
        );

        assert_eq!(
            sync.push_delta(delta(12, 102, json!([]), json!([]))),
            BufferDeltaOutcome::Restart
        );
        assert_eq!(
            sync.push_delta(delta(1, 103, json!([]), json!([]))),
            BufferDeltaOutcome::Restart
        );
    }

    #[test]
    fn exact_price_identity_deletes_only_matching_string_and_promotes_depth() {
        let custom_snapshot = BybitFullSnapshot::from_result(&json!({
            "b": [
                ["100.0000000000001", "1"],
                ["100.0", "2"],
                ["99.0", "3"]
            ],
            "a": [],
            "u": 10,
            "seq": 100
        }))
        .expect("snapshot should parse");
        let mut sync = BybitFullBookSync::new();
        sync.push_delta(delta(10, 100, json!([]), json!([])));
        assert_eq!(
            sync.apply_snapshot(custom_snapshot),
            SnapshotSyncOutcome::Synchronized
        );
        assert_eq!(
            sync.push_delta(delta(11, 101, json!([["100.0", "0"]]), json!([]))),
            BufferDeltaOutcome::Updated
        );

        let book = sync.to_ccxt("BTC/USDT:USDT", 2).expect("book should exist");
        assert_eq!(book.bids, vec![(100.0000000000001, 1.0), (99.0, 3.0)]);
    }
}
