//! Perpl market-data state machines. Integer native prices remain map keys;
//! conversion to Ferris floats happens only at publication. The API's `sn` is
//! NOT contiguous for books/trades/candles (only heartbeat/gas is contiguous).
use super::Market;
use crate::{
    models::{CcxtOhlcv, CcxtOrderBook, CcxtTrade},
    realtime::{RealtimeChannel, RealtimeUpdate},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

pub(super) struct Spec {
    pub market: Market,
    pub chain: u64,
    pub stream: String,
    pub channel: RealtimeChannel,
    pub resolution: Option<u64>,
}
#[derive(Default)]
pub(super) struct State {
    ready: bool,
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    candles: BTreeMap<u64, CcxtOhlcv>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    block: Option<u64>,
    nonce: Option<u64>,
}
pub(super) fn uint(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        .ok_or_else(|| format!("invalid Perpl unsigned integer `{key}`"))
}
fn optional(v: &Value, key: &str) -> Result<Option<u64>, String> {
    if v.get(key).is_some() {
        uint(v, key).map(Some)
    } else {
        Ok(None)
    }
}
fn rows<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>, String> {
    v.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing Perpl `{key}` array"))
}
fn scaled(n: u64, decimals: u32) -> f64 {
    n as f64 / 10_f64.powi(decimals as i32)
}
fn symbol(m: &Market) -> String {
    format!("{}/AUSD:AUSD", m.name)
}
fn identity(v: &Value, spec: &Spec) -> Result<(), String> {
    // Wire identity is the ACK-bound sid. Validate redundant IDs if provided;
    // never infer a market from a display symbol or broadcast across markets.
    for key in ["mkt", "market_id"] {
        if let Some(id) = optional(v, key)? {
            if id != spec.market.id {
                return Err("Perpl market ID mismatch".into());
            }
        }
    }
    Ok(())
}
impl State {
    pub fn apply(&mut self, spec: &Spec, v: &Value) -> Result<Option<RealtimeUpdate>, String> {
        let result = self.apply_inner(spec, v);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }
    fn apply_inner(&mut self, spec: &Spec, v: &Value) -> Result<Option<RealtimeUpdate>, String> {
        identity(v, spec)?;
        let mt = uint(v, "mt")?;
        match spec.channel {
            RealtimeChannel::OrderBook if matches!(mt, 15 | 16) => self.book(spec, v, mt == 15),
            RealtimeChannel::Trades if matches!(mt, 17 | 18) => self.trades(spec, v, mt == 17),
            RealtimeChannel::Ohlcv if matches!(mt, 11 | 12) => self.candles(spec, v, mt == 11),
            _ => Err("Perpl message type does not match subscription".into()),
        }
    }
    fn book(
        &mut self,
        spec: &Spec,
        v: &Value,
        snapshot: bool,
    ) -> Result<Option<RealtimeUpdate>, String> {
        if !snapshot && !self.ready {
            return Err("Perpl book delta before snapshot".into());
        }
        let at = v
            .get("at")
            .filter(|v| v.is_object())
            .ok_or("missing Perpl book provenance")?;
        let block = optional(at, "b")?;
        let timestamp = optional(at, "t")?;
        let nonce = optional(v, "sn")?;
        // No +1 check: blocks and non-heartbeat sequences may legitimately jump.
        if !snapshot
            && (block.zip(self.block).is_some_and(|(n, p)| n < p)
                || nonce.zip(self.nonce).is_some_and(|(n, p)| n < p))
        {
            return Err("Perpl book provenance moved backwards; fresh snapshot required".into());
        }
        let bids = validate_levels(rows(v, "bid")?, snapshot, true)?;
        let asks = validate_levels(rows(v, "ask")?, snapshot, false)?;
        if snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for (map, changes) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for (p, s, o) in changes {
                if o == 0 {
                    map.remove(&p);
                } else {
                    map.insert(p, s);
                }
            }
        }
        // Never truncate maintained state to a downstream depth. Bound memory
        // by failing/resyncing an implausibly large source, not by losing levels.
        if self.bids.len() + self.asks.len() > 100_000 {
            return Err("Perpl book exceeds safety bound".into());
        }
        if self
            .bids
            .last_key_value()
            .zip(self.asks.first_key_value())
            .is_some_and(|((b, _), (a, _))| b >= a)
        {
            return Err("crossed Perpl book".into());
        }
        self.ready = true;
        self.block = block.or(self.block);
        self.nonce = nonce.or(self.nonce);
        let m = &spec.market;
        let convert = |(p, s): (&u64, &u64)| {
            (
                scaled(*p, m.config.price_decimals),
                scaled(*s, m.config.size_decimals),
            )
        };
        Ok(Some(RealtimeUpdate::OrderBook(Arc::new(
            CcxtOrderBook {
                bids: self.bids.iter().rev().map(convert).collect(),
                asks: self.asks.iter().map(convert).collect(),
                nonce,
                timestamp,
                datetime: None,
                symbol: Some(symbol(m)),
                ..Default::default()
            }
            .with_policy(super::book_policy(m)),
        ))))
    }
    fn trades(
        &mut self,
        spec: &Spec,
        v: &Value,
        snapshot: bool,
    ) -> Result<Option<RealtimeUpdate>, String> {
        if !snapshot && !self.ready {
            return Err("Perpl trade update before snapshot".into());
        }
        let mut parsed = Vec::new();
        for row in rows(v, "d")? {
            identity(row, spec)?;
            let p = uint(row, "p")?;
            let s = uint(row, "s")?;
            if p == 0 || s == 0 {
                return Err("zero Perpl trade price/size".into());
            }
            let side = match uint(row, "sd")? {
                1 => "buy",
                2 => "sell",
                _ => return Err("invalid Perpl trade side".into()),
            };
            let at = row
                .get("at")
                .filter(|a| a.is_object())
                .ok_or("missing trade provenance")?;
            let timestamp = optional(at, "t")?;
            let b = optional(at, "b")?;
            let tx = optional(at, "tx")?;
            let l = optional(at, "l")?;
            let txid = at
                .get("txid")
                .map(|v| v.as_str().filter(|s| !s.is_empty()).ok_or("invalid txid"))
                .transpose()?;
            let id = if let Some(txid) = txid {
                Some(if let Some(l) = l {
                    json!([spec.chain, spec.market.id, txid, l]).to_string()
                } else {
                    json!([spec.chain, spec.market.id, txid]).to_string()
                })
            } else if let (Some(b), Some(tx), Some(l)) = (b, tx, l) {
                Some(json!([spec.chain, spec.market.id, b, tx, l]).to_string())
            } else {
                None
            };
            let price = scaled(p, spec.market.config.price_decimals);
            let amount = scaled(s, spec.market.config.size_decimals);
            let cost = price * amount;
            if !(price.is_finite()
                && price > 0.0
                && amount.is_finite()
                && amount > 0.0
                && cost.is_finite())
            {
                return Err("invalid scaled Perpl trade values".into());
            }
            parsed.push(CcxtTrade {
                info: json!({"chainId":spec.chain,"marketId":spec.market.id,"native":row}),
                amount: Some(amount),
                price: Some(price),
                side: Some(side.into()),
                timestamp,
                id,
                symbol: Some(symbol(&spec.market)),
                cost: Some(cost),
                datetime: timestamp
                    .and_then(|t| i64::try_from(t).ok())
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map(|t| t.to_rfc3339()),
                order: None,
                trade_type: None,
                taker_or_maker: None,
                fee: None,
            });
        }
        if snapshot {
            self.seen.clear();
            self.seen_order.clear();
        }
        parsed.retain(|t| {
            let Some(id) = &t.id else {
                return true;
            };
            if !self.seen.insert(id.clone()) {
                return false;
            }
            self.seen_order.push_back(id.clone());
            while self.seen_order.len() > 4096 {
                if let Some(id) = self.seen_order.pop_front() {
                    self.seen.remove(&id);
                }
            }
            true
        });
        self.ready = true;
        Ok(Some(RealtimeUpdate::Trades(Arc::new(parsed))))
    }
    fn candles(
        &mut self,
        spec: &Spec,
        v: &Value,
        snapshot: bool,
    ) -> Result<Option<RealtimeUpdate>, String> {
        if !snapshot && !self.ready {
            return Err("Perpl candle update before snapshot".into());
        }
        let resolution = uint(v, "r")?;
        if Some(resolution) != spec.resolution {
            return Err("Perpl candle resolution mismatch".into());
        }
        let parsed = rows(v, "d")?
            .iter()
            .map(|v| candle(v, &spec.market, resolution))
            .collect::<Result<Vec<_>, _>>()?;
        if parsed.windows(2).any(|rows| rows[0].0 >= rows[1].0) {
            return Err("unordered or duplicate Perpl candle timestamps".into());
        }
        if snapshot {
            self.candles.clear();
        }
        for row in parsed {
            self.candles.insert(row.0, row);
        }
        // Bounded oldest-first window; always retain the newest mutable candle.
        while self.candles.len() > 1024 {
            self.candles.pop_first();
        }
        self.ready = true;
        Ok(Some(RealtimeUpdate::Ohlcv(Arc::new(
            self.candles.values().copied().collect(),
        ))))
    }
}
fn validate_levels(
    rows: &[Value],
    snapshot: bool,
    bid: bool,
) -> Result<Vec<(u64, u64, u64)>, String> {
    let mut seen = HashSet::new();
    let mut last = None;
    rows.iter()
        .map(|v| {
            let p = uint(v, "p")?;
            let s = uint(v, "s")?;
            let o = uint(v, "o")?;
            if p == 0 || !seen.insert(p) || (o > 0 && s == 0) || (snapshot && o == 0) {
                return Err("invalid Perpl book level".into());
            }
            // o is authoritative for deletions; nonnegative s may be residual.
            if snapshot && last.is_some_and(|prev| if bid { p >= prev } else { p <= prev }) {
                return Err("unordered Perpl book snapshot".into());
            }
            last = Some(p);
            Ok((p, s, o))
        })
        .collect()
}
fn candle(v: &Value, m: &Market, resolution: u64) -> Result<CcxtOhlcv, String> {
    let t = uint(v, "t")?;
    let o = uint(v, "o")?;
    let h = uint(v, "h")?;
    let l = uint(v, "l")?;
    let c = uint(v, "c")?;
    let _trade_count = uint(v, "n")?;
    if t % (resolution
        .checked_mul(1000)
        .ok_or("Perpl candle resolution overflow")?)
        != 0
        || l == 0
        || l > h
        || o < l
        || o > h
        || c < l
        || c > h
    {
        return Err("invalid Perpl candle timestamp/OHLC range".into());
    }
    let volume = v
        .get("v")
        .ok_or("missing Perpl candle volume")?
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .map(|v| v / 10_f64.powi(m.collateral_decimals as i32))
        .filter(|v| v.is_finite())
        .ok_or("invalid Perpl candle volume".to_string())?;
    let d = m.config.price_decimals;
    let row = (
        t,
        scaled(o, d),
        scaled(h, d),
        scaled(l, d),
        scaled(c, d),
        Some(volume),
    );
    if ![row.1, row.2, row.3, row.4, row.5.unwrap()]
        .into_iter()
        .all(|v| v.is_finite())
    {
        return Err("non-finite scaled Perpl candle".into());
    }
    Ok(row)
}
