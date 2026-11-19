//! TxFlow wire parsing and realtime state machines. Prices and sizes are
//! decimal strings; they stay `rust_decimal::Decimal` (exact map keys) and are
//! converted to Ferris floats only at publication.
//!
//! Fixture provenance: mainnet captures of 2026-11-17 with
//! `x-api-version: 2026-08-16` (see `tests/fixtures/txflow/`).
use super::Market;
use crate::models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};
use tokio::{sync::Notify, time::Instant};

/// Decimal from a JSON string or number. Absent, `null` and `""` are `None`,
/// never zero. Anything else that is not a decimal is an error.
pub(super) fn dec(v: Option<&Value>) -> Result<Option<Decimal>, String> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) => Decimal::from_str(s)
            .map(Some)
            .map_err(|_| format!("invalid TxFlow decimal `{s}`")),
        Some(Value::Number(n)) => Decimal::from_str(&n.to_string())
            .or_else(|_| Decimal::from_scientific(&n.to_string()))
            .map(Some)
            .map_err(|_| format!("invalid TxFlow decimal `{n}`")),
        Some(other) => Err(format!("invalid TxFlow decimal `{other}`")),
    }
}

pub(super) fn req_dec(v: &Value, key: &str) -> Result<Decimal, String> {
    dec(v.get(key))?.ok_or_else(|| format!("missing TxFlow `{key}`"))
}

/// Numeric id that may arrive as `1` or `"1"`; ids are compared numerically.
pub(super) fn id_of(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub(super) fn uint(v: &Value, key: &str) -> Result<u64, String> {
    id_of(v.get(key)).ok_or_else(|| format!("invalid TxFlow integer `{key}`"))
}

pub(super) fn float(d: Decimal) -> Option<f64> {
    d.to_f64().filter(|v| v.is_finite())
}

pub(super) fn datetime(ms: u64) -> Option<String> {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|t| t.to_rfc3339())
}

/// Book side, best first. Snapshots (and REST books) must not contain zero
/// sizes; deltas use `sz == "0"` for removal. Ordering is strict.
pub(super) fn levels(
    v: Option<&Value>,
    bids: bool,
    allow_zero: bool,
) -> Result<Vec<(Decimal, Decimal)>, String> {
    let rows = v
        .and_then(Value::as_array)
        .ok_or("missing TxFlow book side")?;
    let mut out = Vec::with_capacity(rows.len());
    let mut last: Option<Decimal> = None;
    for row in rows {
        let px = req_dec(row, "px")?;
        let sz = req_dec(row, "sz")?;
        if px <= Decimal::ZERO || sz < Decimal::ZERO || (!allow_zero && sz.is_zero()) {
            return Err("invalid TxFlow book level".into());
        }
        if last.is_some_and(|prev| if bids { px >= prev } else { px <= prev }) {
            return Err("unordered TxFlow book levels".into());
        }
        last = Some(px);
        out.push((px, sz));
    }
    Ok(out)
}

pub(super) fn book_sides(data: &Value, allow_zero: bool) -> Result<BookSides, String> {
    let sides = data
        .get("levels")
        .and_then(Value::as_array)
        .filter(|l| l.len() == 2)
        .ok_or("TxFlow book `levels` must be [bids, asks]")?;
    Ok((
        levels(sides.first(), true, allow_zero)?,
        levels(sides.get(1), false, allow_zero)?,
    ))
}
pub(super) type BookSides = (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>);

pub(super) fn to_book(
    m: &Market,
    bids: impl Iterator<Item = (Decimal, Decimal)>,
    asks: impl Iterator<Item = (Decimal, Decimal)>,
    timestamp: Option<u64>,
    nonce: Option<u64>,
) -> Result<CcxtOrderBook, String> {
    let convert = |(p, s): (Decimal, Decimal)| {
        float(p)
            .zip(float(s))
            .ok_or_else(|| "non-finite TxFlow book level".to_string())
    };
    Ok(CcxtOrderBook {
        bids: bids.map(convert).collect::<Result<_, _>>()?,
        asks: asks.map(convert).collect::<Result<_, _>>()?,
        datetime: timestamp.and_then(datetime),
        timestamp,
        nonce,
        symbol: Some(m.symbol()),
        ..Default::default()
    }
    .with_policy(super::book_policy(m)))
}

/// `l2BookDepth` state: snapshot, then deltas only while `prevSeq` chains to
/// the last applied `seq`. The server also sends unsolicited snapshots
/// mid-stream (observed live, sometimes reusing the last delta's `seq`);
/// any snapshot replaces the book.
#[derive(Default)]
pub(super) struct BookState {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    seq: Option<u64>,
}
impl BookState {
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    #[cfg(test)]
    pub fn ready(&self) -> bool {
        self.seq.is_some()
    }
    /// `Ok(None)`: delta while awaiting a snapshot (ignored, not a gap).
    pub fn apply(&mut self, m: &Market, data: &Value) -> Result<Option<CcxtOrderBook>, String> {
        let result = self.apply_inner(m, data);
        if result.is_err() {
            self.reset(); // no delta can revive an invalid book
        }
        result
    }
    fn apply_inner(&mut self, m: &Market, data: &Value) -> Result<Option<CcxtOrderBook>, String> {
        let seq = uint(data, "seq")?;
        let prev = data.get("prevSeq").ok_or("missing TxFlow `prevSeq`")?;
        let time = id_of(data.get("time"));
        match data.get("type").and_then(Value::as_str) {
            Some("snapshot") => {
                if !prev.is_null() {
                    return Err("TxFlow book snapshot carries prevSeq".into());
                }
                let (bids, asks) = book_sides(data, false)?;
                self.bids = bids.into_iter().collect();
                self.asks = asks.into_iter().collect();
            }
            Some("delta") => {
                let Some(last) = self.seq else {
                    return Ok(None);
                };
                let prev = id_of(Some(prev)).ok_or("invalid TxFlow `prevSeq`")?;
                if prev != last || seq <= prev {
                    return Err(format!(
                        "TxFlow book sequence gap (have {last}, got prevSeq {prev} seq {seq}); fresh snapshot required"
                    ));
                }
                let (bids, asks) = book_sides(data, true)?;
                for (map, changes) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
                    for (px, sz) in changes {
                        if sz.is_zero() {
                            map.remove(&px);
                        } else {
                            map.insert(px, sz);
                        }
                    }
                }
            }
            _ => return Err("unknown TxFlow book frame type".into()),
        }
        if self.bids.len() + self.asks.len() > 20_000 {
            return Err("TxFlow book exceeds safety bound".into());
        }
        if self
            .bids
            .last_key_value()
            .zip(self.asks.first_key_value())
            .is_some_and(|((b, _), (a, _))| b >= a)
        {
            return Err("crossed TxFlow book".into());
        }
        self.seq = Some(seq);
        to_book(
            m,
            self.bids.iter().rev().map(|(p, s)| (*p, *s)),
            self.asks.iter().map(|(p, s)| (*p, *s)),
            time,
            Some(seq),
        )
        .map(Some)
    }
}

/// One OHLCV bar. `v` is base volume. WS payloads were observed as strings
/// while the docs say numbers; both are accepted.
pub(super) fn candle(v: &Value) -> Result<(String, String, CcxtOhlcv), String> {
    let t = uint(v, "t")?;
    let o = req_dec(v, "o")?;
    let h = req_dec(v, "h")?;
    let l = req_dec(v, "l")?;
    let c = req_dec(v, "c")?;
    let vol = dec(v.get("v"))?;
    if l <= Decimal::ZERO || l > h || o < l || o > h || c < l || c > h {
        return Err("invalid TxFlow candle range".into());
    }
    if vol.is_some_and(|v| v < Decimal::ZERO) {
        return Err("negative TxFlow candle volume".into());
    }
    let s = match v.get("s") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    };
    let i = v
        .get("i")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let f = |d| float(d).ok_or_else(|| "non-finite TxFlow candle".to_string());
    Ok((
        s,
        i,
        (t, f(o)?, f(h)?, f(l)?, f(c)?, vol.map(f).transpose()?),
    ))
}

/// Bounded oldest-first candle window keyed by open time: equal `t`
/// replaces, greater `t` appends (closing the previous bar), smaller is stale.
#[derive(Default)]
pub(super) struct CandleState {
    bars: BTreeMap<u64, CcxtOhlcv>,
}
impl CandleState {
    pub fn apply(&mut self, bar: CcxtOhlcv) -> Option<Vec<CcxtOhlcv>> {
        if self.bars.last_key_value().is_some_and(|(t, _)| bar.0 < *t) {
            return None;
        }
        self.bars.insert(bar.0, bar);
        while self.bars.len() > 1024 {
            self.bars.pop_first();
        }
        Some(self.bars.values().copied().collect())
    }
}

/// REST (`trades` info) and WS (`/ws trades`) rows share field names. Their
/// `tid`s are different id spaces for the same fill (verified live), and
/// `hash` is per transaction. Each source keeps its own native `tid`.
pub(super) fn trade(m: &Market, row: &Value) -> Result<CcxtTrade, String> {
    if let Some(id) = row.get("instrumentId") {
        if id_of(Some(id)) != Some(m.id) {
            return Err("TxFlow trade instrument mismatch".into());
        }
    }
    let px = req_dec(row, "px")?;
    let sz = req_dec(row, "sz")?;
    if px <= Decimal::ZERO || sz <= Decimal::ZERO {
        return Err("invalid TxFlow trade price/size".into());
    }
    let side = match row.get("side").and_then(Value::as_str) {
        Some("B") => "buy",
        Some("A") => "sell",
        _ => return Err("invalid TxFlow trade side".into()),
    };
    let time = uint(row, "time")?;
    let tid = row
        .get("tid")
        .and_then(|v| {
            v.as_u64()
                .map(|n| n.to_string())
                .or(v.as_str().map(Into::into))
        })
        .ok_or("missing TxFlow trade tid")?;
    let cost = px.checked_mul(sz).and_then(float);
    Ok(CcxtTrade {
        info: row.clone(),
        amount: float(sz),
        datetime: datetime(time),
        id: Some(tid),
        order: None,
        price: float(px),
        timestamp: Some(time),
        trade_type: None,
        side: Some(side.into()),
        symbol: Some(m.symbol()),
        // TxFlow `side` is the aggressor's side.
        taker_or_maker: Some("taker".into()),
        cost,
        fee: None,
    })
}

/// One `allAssetCtxs` row. Every price is optional: absent or empty means
/// unavailable, never zero.
#[derive(Debug, Clone)]
pub(super) struct Ctx {
    pub funding: Option<Decimal>,
    pub mark: Option<Decimal>,
    pub oracle: Option<Decimal>,
    pub mid: Option<Decimal>,
    pub last: Option<Decimal>,
    pub open_interest: Option<Decimal>,
    /// Parsed when present; never observed populated (D2 keeps volume unsupported).
    #[allow(dead_code)]
    pub day_ntl_vlm: Option<Decimal>,
    #[allow(dead_code)]
    pub day_base_vlm: Option<Decimal>,
    pub next_funding: Option<u64>,
    pub prev_funding: Option<u64>,
    pub block_time: Option<u64>,
    pub raw: Value,
    pub received: Instant,
    pub received_wall: u64,
}
pub(super) fn ctx(raw: &Value, received: Instant, received_wall: u64) -> Result<Ctx, String> {
    let positive = |key: &str| -> Result<Option<Decimal>, String> {
        Ok(dec(raw.get(key))?.filter(|d| *d > Decimal::ZERO))
    };
    Ok(Ctx {
        funding: dec(raw.get("funding"))?,
        mark: positive("markPx")?,
        oracle: positive("oraclePx")?,
        mid: positive("midPx")?,
        last: positive("lastPrice")?,
        open_interest: dec(raw.get("openInterest"))?.filter(|d| *d >= Decimal::ZERO),
        day_ntl_vlm: dec(raw.get("dayNtlVlm"))?,
        day_base_vlm: dec(raw.get("dayBaseVlm"))?,
        next_funding: id_of(raw.get("nextFundingTime")),
        prev_funding: id_of(raw.get("prevFundingTime")),
        block_time: id_of(raw.get("blockTime")),
        raw: raw.clone(),
        received,
        received_wall,
    })
}

/// Shared `allAssetCtxs` cache, written by the `/ws` session and read by
/// tickers and market stats. Rows merge by instrument id: a row missing from
/// a push is retained, not treated as a removed market.
/// Rows by instrument id, plus the receipt time of the latest frame.
pub(super) type CtxRows = (HashMap<u64, Arc<Ctx>>, Instant);

#[derive(Default)]
pub(super) struct CtxStore {
    state: parking_lot::Mutex<CtxState>,
    pub changed: Notify,
}
#[derive(Default)]
struct CtxState {
    rows: HashMap<u64, Arc<Ctx>>,
    last_frame: Option<Instant>,
    last_read: Option<Instant>,
    error: Option<String>,
}
impl CtxStore {
    pub fn merge(&self, data: &Value) -> Result<usize, String> {
        let rows = data
            .get("ctxs")
            .and_then(Value::as_array)
            .ok_or("TxFlow allAssetCtxs frame has no `ctxs`")?;
        let received = Instant::now();
        let wall = super::now_millis();
        let mut parsed = Vec::with_capacity(rows.len());
        let mut rejected = 0usize;
        for row in rows {
            let id = id_of(row.get("coin"));
            match (id, row.get("ctx").map(|c| ctx(c, received, wall))) {
                (Some(id), Some(Ok(c))) => parsed.push((id, Arc::new(c))),
                _ => rejected += 1,
            }
        }
        let mut state = self.state.lock();
        for (id, c) in parsed {
            state.rows.insert(id, c);
        }
        state.last_frame = Some(received);
        state.error = None;
        drop(state);
        self.changed.notify_waiters();
        Ok(rejected)
    }
    pub fn fail(&self, error: &str) {
        self.state.lock().error = Some(error.into());
    }
    pub fn touch(&self) {
        self.state.lock().last_read = Some(Instant::now());
    }
    pub fn idle_for(&self) -> Option<std::time::Duration> {
        self.state.lock().last_read.map(|t| t.elapsed())
    }
    /// Rows if a frame arrived within `fresh`; otherwise the last error.
    pub fn read(&self, fresh: std::time::Duration) -> Result<CtxRows, Option<String>> {
        let state = self.state.lock();
        match state.last_frame {
            Some(at) if at.elapsed() <= fresh => Ok((state.rows.clone(), at)),
            _ => Err(state.error.clone()),
        }
    }
}

pub(super) fn subscribe_frame(node: bool, subscribe: bool, subscription: Value) -> String {
    let method = if subscribe {
        "subscribe"
    } else {
        "unsubscribe"
    };
    if node {
        // `/ws` silently drops frames without the top-level `type`.
        let kind = subscription["type"].clone();
        json!({"method": method, "type": kind, "subscription": subscription}).to_string()
    } else {
        json!({"method": method, "subscription": subscription}).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market() -> Market {
        super::super::tests::btc()
    }

    fn frame(kind: &str, seq: u64, prev: Option<u64>, bids: Value, asks: Value) -> Value {
        json!({"coin":"BTC-USDC","id":1,"time":1_794_877_959_530_u64,"seq":seq,"prevSeq":prev,"type":kind,"levels":[bids,asks]})
    }

    #[test]
    fn decimals_accept_strings_and_numbers_but_never_coerce_empty_to_zero() {
        assert_eq!(
            dec(Some(&json!("65317.1"))).unwrap(),
            Some(Decimal::from_str("65317.1").unwrap())
        );
        assert_eq!(
            dec(Some(&json!(65317.1))).unwrap(),
            Some(Decimal::from_str("65317.1").unwrap())
        );
        assert_eq!(dec(Some(&json!(""))).unwrap(), None);
        assert_eq!(dec(None).unwrap(), None);
        assert!(dec(Some(&json!("abc"))).is_err());
        assert_eq!(id_of(Some(&json!("1"))), id_of(Some(&json!(1))));
    }

    #[test]
    fn book_snapshot_delta_removal_and_gap() {
        let m = market();
        let mut s = BookState::default();
        // Deltas before the first snapshot are ignored, not gaps.
        assert!(s
            .apply(&m, &frame("delta", 9, Some(8), json!([]), json!([])))
            .unwrap()
            .is_none());
        let snap = frame(
            "snapshot",
            10,
            None,
            json!([{"px":"100.0","sz":"1","n":1},{"px":"99","sz":"2","n":1}]),
            json!([{"px":"101","sz":"3","n":1},{"px":"102","sz":"4","n":2}]),
        );
        let book = s.apply(&m, &snap).unwrap().unwrap();
        assert_eq!(book.bids, vec![(100.0, 1.0), (99.0, 2.0)]);
        assert_eq!(book.nonce, Some(10));
        // Removal (sz "0") and insert; "100.00" is the same level as "100.0".
        let book = s
            .apply(
                &m,
                &frame(
                    "delta",
                    11,
                    Some(10),
                    json!([{"px":"100.00","sz":"0","n":0},{"px":"98","sz":"5","n":1}]),
                    json!([]),
                ),
            )
            .unwrap()
            .unwrap();
        assert_eq!(book.bids, vec![(99.0, 2.0), (98.0, 5.0)]);
        assert_eq!(book.asks.len(), 2);
        // Gap: prevSeq does not chain.
        let err = s
            .apply(&m, &frame("delta", 13, Some(12), json!([]), json!([])))
            .unwrap_err();
        assert!(err.contains("gap"), "{err}");
        assert!(!s.ready(), "a gap must clear state before further delivery");
        assert!(s
            .apply(&m, &frame("delta", 14, Some(13), json!([]), json!([])))
            .unwrap()
            .is_none());
        // A fresh snapshot (even reusing an old seq, as observed live) recovers.
        assert!(s.apply(&m, &snap).unwrap().is_some());
    }

    #[test]
    fn book_rejects_zero_in_snapshot_unordered_and_crossed() {
        let m = market();
        let mut s = BookState::default();
        assert!(s
            .apply(
                &m,
                &frame(
                    "snapshot",
                    1,
                    None,
                    json!([{"px":"1","sz":"0","n":0}]),
                    json!([])
                )
            )
            .is_err());
        assert!(s
            .apply(
                &m,
                &frame(
                    "snapshot",
                    1,
                    None,
                    json!([{"px":"1","sz":"1","n":1},{"px":"2","sz":"1","n":1}]),
                    json!([])
                )
            )
            .is_err());
        s.apply(
            &m,
            &frame(
                "snapshot",
                1,
                None,
                json!([{"px":"1","sz":"1","n":1}]),
                json!([{"px":"2","sz":"1","n":1}]),
            ),
        )
        .unwrap();
        assert!(s
            .apply(
                &m,
                &frame(
                    "delta",
                    2,
                    Some(1),
                    json!([{"px":"3","sz":"1","n":1}]),
                    json!([])
                )
            )
            .unwrap_err()
            .contains("crossed"));
    }

    #[test]
    fn candle_window_replaces_appends_and_ignores_stale() {
        let mut s = CandleState::default();
        let bar = |t, c| (t, 1.0, 2.0, 0.5, c, Some(1.0));
        assert_eq!(s.apply(bar(60_000, 1.0)).unwrap().len(), 1);
        assert_eq!(s.apply(bar(60_000, 1.5)).unwrap()[0].4, 1.5);
        assert_eq!(s.apply(bar(120_000, 1.2)).unwrap().len(), 2);
        assert!(s.apply(bar(60_000, 1.9)).is_none());
        // String (observed) and number (documented) payloads.
        let (s1, i, row) = candle(&json!({"t":1,"o":"65317.1","h":"65400","l":"65300","c":"65350","v":"0.0100","n":11,"s":"1","i":"1m"})).unwrap();
        assert_eq!(
            (s1.as_str(), i.as_str(), row.1, row.5),
            ("1", "1m", 65317.1, Some(0.01))
        );
        let (_, _, row) = candle(
            &json!({"t":1,"o":65317.1,"h":65400,"l":65300,"c":65350,"v":0.01,"s":"BTC","i":"1m"}),
        )
        .unwrap();
        assert_eq!(row.2, 65400.0);
        assert!(candle(&json!({"t":1,"o":"5","h":"4","l":"3","c":"4","v":"1"})).is_err());
    }

    #[test]
    fn ctx_rows_keep_absent_values_unavailable_and_merge_by_id() {
        let store = CtxStore::default();
        store
            .merge(&json!({"ctxs":[
                {"coin":"1","ctx":{"funding":"0.00125000","openInterest":"239.2232","oraclePx":"64687.8","markPx":"64652.3","midPx":"64660.4","impactPxs":["64660.3","64660.5"],"lastPrice":"64662.0","nextFundingTime":1794880800000_u64,"prevFundingTime":1794877200000_u64,"blockTime":1794877962378_u64}},
                {"coin":"2","ctx":{"funding":"0.001","openInterest":"5","oraclePx":"3000","markPx":"3001","lastPrice":""}}
            ]}))
            .unwrap();
        // A later partial push keeps id 2.
        store
            .merge(&json!({"ctxs":[{"coin":"1","ctx":{"markPx":"1"}}]}))
            .unwrap();
        let (rows, _) = store.read(std::time::Duration::from_secs(5)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[&2].last, None);
        assert_eq!(rows[&2].mid, None);
        assert_eq!(rows[&1].mark, Some(Decimal::ONE));
        assert_eq!(rows[&1].funding, None);
    }

    #[test]
    fn node_frames_carry_top_level_type() {
        let f: Value = serde_json::from_str(&subscribe_frame(
            true,
            true,
            json!({"type":"trades","coin":"1"}),
        ))
        .unwrap();
        assert_eq!(f["type"], "trades");
        let f: Value = serde_json::from_str(&subscribe_frame(
            false,
            false,
            json!({"type":"candle","coin":"1","interval":"1m"}),
        ))
        .unwrap();
        assert!(f.get("type").is_none());
        assert_eq!(f["method"], "unsubscribe");
    }
}
