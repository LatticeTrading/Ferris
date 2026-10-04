use ccxt::types::Market;
use ccxt::TypedExchange;
use serde_json::{json, Value};

use crate::{config::Config, exchanges::traits::ExchangeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    Binance,
    Bybit,
    Hyperliquid,
    Lighter,
    Aster,
    Extended,
}

impl Venue {
    pub const ALL: [Self; 6] = [
        Self::Binance,
        Self::Bybit,
        Self::Hyperliquid,
        Self::Lighter,
        Self::Aster,
        Self::Extended,
    ];

    pub fn public_id(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
            Self::Lighter => "lighterxyz",
            Self::Aster => "aster",
            Self::Extended => "extended",
        }
    }

    pub fn ccxt_id(self) -> &'static str {
        match self {
            Self::Lighter => "lighter",
            other => other.public_id(),
        }
    }

    pub fn from_public_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|venue| venue.public_id() == id)
    }

    pub(super) fn scope(self, scope: CatalogScope) -> Result<CatalogScope, ExchangeError> {
        use CatalogScope::*;
        match (self, scope) {
            (Self::Binance, Default) => Ok(Linear),
            (Self::Binance, Spot | Linear | Inverse | Option) => Ok(scope),
            (Self::Bybit, _) => Ok(scope),
            (Self::Hyperliquid, Default | Spot | Linear) => Ok(scope),
            (Self::Lighter | Self::Aster, Default | Spot | Linear) => Ok(Default),
            (Self::Extended, Default | Linear) => Ok(Default),
            _ => Err(ExchangeError::BadSymbol(format!(
                "{} has no qualified {scope:?} catalog profile",
                self.public_id()
            ))),
        }
    }
}

/// Acquisition profiles, not client display filters. Aster and Lighter stock
/// loadMarkets always load spot and swap together and therefore share one profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CatalogScope {
    #[default]
    Default,
    Spot,
    Linear,
    Inverse,
    Option,
}

/// Only owned configuration crosses into an owner. No caller can inject a
/// mutable CCXT object, a transport implementation, or a raw dispatch command.
#[derive(Clone)]
pub(super) struct ProviderConfig {
    value: Value,
}

impl ProviderConfig {
    pub(super) fn new(venue: Venue, config: &Config) -> Result<Self, ExchangeError> {
        let timeout = i64::try_from(config.request_timeout_ms)
            .ok()
            .filter(|timeout| *timeout > 0)
            .ok_or_else(|| ExchangeError::Internal("invalid CCXT request timeout".into()))?;
        let api = match venue {
            Venue::Binance => {
                let base = config.binance_base_url.trim_end_matches('/');
                let mut urls = json!({
                    "fapiPublic": format!("{base}/fapi/v1"),
                    "fapiPrivate": format!("{base}/fapi/v1"),
                    "fapiPublicV2": format!("{base}/fapi/v2"),
                    "fapiPrivateV2": format!("{base}/fapi/v2"),
                    "fapiPublicV3": format!("{base}/fapi/v3"),
                    "fapiPrivateV3": format!("{base}/fapi/v3")
                });
                // A custom gateway serves all public products. With the default
                // USD-M host, leave the other stock product hosts intact.
                if base != "https://fapi.binance.com" {
                    urls["dapiPublic"] = json!(format!("{base}/dapi/v1"));
                    urls["eapiPublic"] = json!(format!("{base}/eapi/v1"));
                    urls["public"] = json!(format!("{base}/api/v3"));
                }
                urls
            }
            Venue::Bybit => json!({
                "spot": config.bybit_base_url,
                "futures": config.bybit_base_url,
                "v2": config.bybit_base_url,
                "public": config.bybit_base_url,
                "private": config.bybit_base_url
            }),
            Venue::Hyperliquid => json!({
                "public": config.hyperliquid_base_url,
                "private": config.hyperliquid_base_url
            }),
            Venue::Lighter => json!({
                "root": config.lighter_rest_base_url,
                "public": config.lighter_rest_base_url,
                "private": config.lighter_rest_base_url,
                "ws": config.lighter_ws_url
            }),
            Venue::Aster => {
                let base = config.aster_base_url.trim_end_matches('/');
                let mut urls = json!({
                    "fapiPublic": format!("{base}/fapi"),
                    "fapiPrivate": format!("{base}/fapi")
                });
                if base != "https://fapi.asterdex.com" {
                    urls["sapiPublic"] = json!(format!("{base}/api"));
                }
                urls
            }
            Venue::Extended => {
                // Ferris config includes /api/v1; stock signing adds /api/{version}.
                let base = config.extended_rest_base_url.trim_end_matches('/');
                let base = base.strip_suffix("/api/v1").ok_or_else(|| {
                    ExchangeError::Internal(
                        "EXTENDED_REST_BASE_URL must end in /api/v1 for stock CCXT signing".into(),
                    )
                })?;
                json!({"rest": base, "ws": config.extended_ws_url})
            }
        };
        let mut value = json!({
            "timeout": timeout,
            "enableRateLimit": true,
            "urls": {"api": api},
            "options": {"defaultType": "swap", "defaultSubType": "linear"}
        });
        if venue == Venue::Binance {
            value["options"]["fetchCurrencies"] = json!(false);
        }
        if venue == Venue::Hyperliquid {
            // Stock parse_currency writes through &self with no UnsafeCell.
            // Normal constructor configuration bypasses that optional loader;
            // stock spot metadata itself supplies the native token names.
            value["has"] = json!({"fetchCurrencies": false});
            let base = config.hyperliquid_base_url.trim_end_matches('/');
            let ws = if let Some(host) = base.strip_prefix("https://") {
                format!("wss://{host}/ws")
            } else if let Some(host) = base.strip_prefix("http://") {
                format!("ws://{host}/ws")
            } else {
                return Err(ExchangeError::Internal("invalid Hyperliquid URL".into()));
            };
            value["urls"]["api"]["ws"] = json!({"public": ws, "private": ws});
        }
        Ok(Self { value })
    }

    pub(super) fn for_scope(&self, venue: Venue, scope: CatalogScope) -> ccxt::Value {
        use CatalogScope::*;
        let mut value = self.value.clone();
        let types: std::option::Option<&[&str]> = match (venue, scope) {
            (Venue::Binance | Venue::Bybit, Spot) => Some(&["spot"]),
            (Venue::Binance | Venue::Bybit, Linear) => Some(&["linear"]),
            (Venue::Binance | Venue::Bybit, Inverse) => Some(&["inverse"]),
            (Venue::Binance | Venue::Bybit, Option) => Some(&["option"]),
            (Venue::Bybit, Default) => Some(&["linear", "inverse", "spot"]),
            (Venue::Hyperliquid, Default) => Some(&["swap", "spot"]),
            (Venue::Hyperliquid, Spot) => Some(&["spot"]),
            (Venue::Hyperliquid, Linear) => Some(&["swap"]),
            _ => None,
        };
        if let Some(types) = types {
            value["options"]["fetchMarkets"] = json!({"types": types});
        }
        if scope == Spot {
            value["options"]["defaultType"] = json!("spot");
        } else if scope == Option {
            value["options"]["defaultType"] = json!("option");
        }
        if scope == Inverse {
            value["options"]["defaultSubType"] = json!("inverse");
        } else if matches!(scope, Spot | Option) {
            // A linear default takes precedence over Binance's spot/option type.
            value["options"]["defaultSubType"] = json!(null);
        }
        ccxt::Value::from_json(&value)
    }
}

// These typed wrappers own their cores. This enum never leaves its OS thread.
// No derived Binance ID is used: USD-M is selected explicitly in the options.
pub(super) enum Provider {
    Binance(ccxt::Binance),
    Bybit(ccxt::Bybit),
    Hyperliquid(ccxt::Hyperliquid),
    Lighter(ccxt::Lighter),
    Aster(ccxt::Aster),
    Extended(ccxt::Extended),
}

macro_rules! dispatch {
    ($provider:expr, $exchange:ident => $body:expr) => {
        match $provider {
            Provider::Binance($exchange) => $body,
            Provider::Bybit($exchange) => $body,
            Provider::Hyperliquid($exchange) => $body,
            Provider::Lighter($exchange) => $body,
            Provider::Aster($exchange) => $body,
            Provider::Extended($exchange) => $body,
        }
    };
}

impl Provider {
    pub(super) fn new(venue: Venue, scope: CatalogScope, config: &ProviderConfig) -> Self {
        let config = Some(config.for_scope(venue, scope));
        match venue {
            Venue::Binance => Self::Binance(ccxt::Binance::new(config)),
            Venue::Bybit => Self::Bybit(ccxt::Bybit::new(config)),
            Venue::Hyperliquid => Self::Hyperliquid(ccxt::Hyperliquid::new(config)),
            Venue::Lighter => Self::Lighter(ccxt::Lighter::new(config)),
            Venue::Aster => Self::Aster(ccxt::Aster::new(config)),
            Venue::Extended => {
                let mut core = ccxt::exchanges::extended::ExtendedCore::new(config);
                // Extended rejects a missing User-Agent; 4.5.85 ignores its config key.
                core.exchange.userAgent = ccxt::Value::from("Ferris/1.0");
                Self::Extended(ccxt::Extended::from_core(core))
            }
        }
    }

    pub(super) async fn load_markets(&mut self, reload: bool) -> ccxt::Result<Vec<Market>> {
        // Stock's fallible inherent method preserves acquisition errors rather
        // than presenting a failed load as an empty metadata snapshot.
        dispatch!(self, exchange => exchange.try_load_markets(reload).await)
    }

    pub(super) fn precision_mode(&self) -> Result<i64, ExchangeError> {
        dispatch!(self, exchange => exchange.precisionMode.as_i64())
            .ok_or_else(|| ExchangeError::UpstreamData("missing CCXT precision mode".into()))
    }

    pub(super) fn supports(&self, capability: &str) -> bool {
        let value =
            dispatch!(self, exchange => ccxt::value::get_value_k(&exchange.has, capability));
        value.as_bool() == Some(true) || value.as_str() == Some("emulated")
    }

    pub(super) fn has_timeframe(&self, timeframe: &str) -> bool {
        dispatch!(self, exchange => exchange.timeframes.as_map().is_some_and(|map| map.contains_key(timeframe)))
    }

    pub(super) fn timeframe_alias(&self, alias: &str) -> Option<String> {
        dispatch!(self, exchange => {
            let timeframes = exchange.timeframes.as_map()?;
            let mut matches = timeframes.iter()
                .filter(|(_, value)| value.as_str() == Some(alias));
            let (timeframe, _) = matches.next()?;
            matches.next().is_none().then(|| timeframe.clone())
        })
    }

    pub(super) fn timeframe_millis(&self, timeframe: &str) -> Option<i64> {
        dispatch!(self, exchange => exchange.parse_timeframe(ccxt::Value::from(timeframe)))
            .as_i64()?
            .checked_mul(1_000)
            .filter(|duration| *duration > 0)
    }

    pub(super) async fn call(
        &mut self,
        method: &str,
        args: Vec<ccxt::Value>,
    ) -> ccxt::Result<ccxt::Value> {
        dispatch!(self, exchange => exchange.call_raw(method, args).await)
    }

    pub(super) async fn public_trades(
        &mut self,
        venue: Venue,
        market: &super::catalog::CatalogMarket,
        since: Option<i64>,
        limit: i64,
        params: ccxt::Params,
    ) -> Result<ccxt::Value, ExchangeError> {
        use ccxt::Value as V;
        let rows = match venue {
            // The unified Hyperliquid method is userFills, not public prints.
            Venue::Hyperliquid => {
                let coin = if market.market.market_type == crate::models::UnifiedMarketType::Perp {
                    market.raw.get("baseName")
                } else {
                    market.raw.get("id")
                }
                .and_then(Value::as_str)
                .ok_or_else(|| ExchangeError::UpstreamData("missing stock trade coin".into()))?;
                self.call(
                    "public_post_info",
                    vec![V::from_json(&json!({"type":"recentTrades", "coin":coin}))],
                )
                .await
                .map_err(|error| exchange_error(venue, error))?
            }
            // This pin ships the endpoint and parser, but no unified method.
            Venue::Lighter => {
                let result = self
                    .call(
                        "public_get_recent_trades",
                        vec![V::from_json(&json!({
                            "market_id": market.raw["id"], "limit": limit
                        }))],
                    )
                    .await
                    .map_err(|error| exchange_error(venue, error))?;
                ccxt::value::get_value_k(&result, "trades")
            }
            _ => {
                return self
                    .call(
                        "fetch_trades",
                        vec![
                            V::from(market.ccxt_symbol.as_str()),
                            since.map(V::Int).unwrap_or(V::Null),
                            V::Int(limit),
                            params.into_value_object(),
                        ],
                    )
                    .await
                    .map_err(|error| exchange_error(venue, error))
            }
        };
        let V::Arr(rows) = rows else {
            return Err(ExchangeError::UpstreamData(
                "stock public trades response is not an array".into(),
            ));
        };
        let raw_market = V::from_json(&market.raw);
        let mut parsed = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            // Stock venue parser owns side, units, fees and raw-info semantics.
            parsed.push(
                self.call("parse_trade", vec![row.clone(), raw_market.clone()])
                    .await
                    .map_err(|error| exchange_error(venue, error))?,
            );
        }
        Ok(V::Arr(std::sync::Arc::new(parsed)))
    }
}

pub(super) fn exchange_error(venue: Venue, error: ccxt::ExchangeError) -> ExchangeError {
    let message = format!("{}: {error}", venue.public_id());
    if error.is("BadSymbol") {
        ExchangeError::BadSymbol(message)
    } else if error.is("BadResponse") || error.is("ChecksumError") {
        ExchangeError::UpstreamData(message)
    } else if error.is("NotSupported") {
        // Every catalog profile is source-qualified; this is a dispatch defect,
        // not permission to change the supported venue set on a failed request.
        ExchangeError::Internal(message)
    } else {
        ExchangeError::UpstreamRequest(message)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        catalog::Catalog, live::LiveHub, owner::CatalogSnapshot, stream::prepare_live,
    };
    use super::*;
    use crate::realtime::{RealtimeChannel, RealtimeTopic, RealtimeUpdate};
    use futures_util::{SinkExt, StreamExt};
    use std::{sync::Arc, time::Duration};
    use tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot},
        time::timeout,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    async fn trade_owner_delivers_new_rows(venue: Venue) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (outgoing, mut input) = mpsc::channel::<Value>(4);
        let (ready, registered) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            let mut ready = Some(ready);
            loop {
                tokio::select! {
                    frame = socket.next() => match frame {
                        Some(Ok(Message::Text(text))) => {
                            let request: Value = serde_json::from_str(&text).unwrap();
                            let subscribed = request["op"] == "subscribe" || request["method"] == "SUBSCRIBE";
                            if subscribed {
                                let ack = if venue == Venue::Binance {
                                    json!({"id": request["id"], "result": null})
                                } else {
                                    json!({"op":"subscribe", "success":true, "req_id":request["req_id"]})
                                };
                                socket.send(Message::Text(ack.to_string().into())).await.unwrap();
                                if let Some(ready) = ready.take() { let _ = ready.send(()); }
                            }
                        },
                        Some(Ok(Message::Ping(data))) => { let _ = socket.send(Message::Pong(data)).await; },
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {},
                    },
                    frame = input.recv() => match frame {
                        Some(frame) => socket.send(Message::Text(frame.to_string().into())).await.unwrap(),
                        None => break,
                    },
                }
            }
        });
        let ws = if venue == Venue::Binance {
            json!({"future":url})
        } else {
            json!({"public":{"linear":url}})
        };
        let config = ProviderConfig {
            value: json!({
                "enableRateLimit":true, "urls":{"api":{"ws":ws}},
                "options":{"defaultType":"swap", "defaultSubType":"linear"}
            }),
        };
        let market = json!({
            "id":"BTCUSDT", "symbol":"BTC/USDT:USDT", "base":"BTC", "quote":"USDT", "settle":"USDT",
            "type":"swap", "spot":false, "swap":true, "future":false, "option":false,
            "active":true, "contract":true, "linear":true, "inverse":false, "contractSize":1,
            "precision":{"amount":0.001,"price":0.1}, "limits":{"amount":{"min":0.001}},
            "info":{"symbol":"BTCUSDT", "contractType":"PERPETUAL", "settleCoin":"USDT", "marginAsset":"USDT"}
        });
        let catalog = Arc::new(CatalogSnapshot {
            venue,
            scope: CatalogScope::Linear,
            timestamp: 1,
            generation: 1,
            catalog: Catalog::from_markets(
                venue,
                vec![Market::from_value(ccxt::Value::from_json(&market))],
                ccxt::runtime::TICK_SIZE,
            )
            .unwrap(),
        });
        let provider = Provider::new(venue, CatalogScope::Linear, &config);
        let prepared = prepare_live(
            venue,
            RealtimeChannel::Trades,
            RealtimeTopic {
                exchange: venue.public_id().into(),
                symbol: "BTCUSDT".into(),
                params: json!({}),
            },
            catalog,
            &provider,
            &config,
        )
        .await
        .unwrap();
        let hub = LiveHub::default();
        let mut subscription = hub.subscribe(prepared).await.unwrap();
        timeout(Duration::from_secs(5), registered)
            .await
            .unwrap()
            .unwrap();
        for id in [101, 102] {
            let time = 1_700_000_000_000u64 + id;
            let wire = if venue == Venue::Binance {
                json!({"e":"trade", "E":time, "T":time, "s":"BTCUSDT", "t":id, "p":"60000", "q":"0.2", "m":false})
            } else {
                json!({"topic":"publicTrade.BTCUSDT", "type":"snapshot", "ts":time,
                    "data":[{"T":time,"s":"BTCUSDT","S":"Buy","v":"0.2","p":"60000","i":id.to_string(),"BT":false}]})
            };
            outgoing.send(wire).await.unwrap();
            let update = timeout(Duration::from_secs(5), subscription.receiver.recv())
                .await
                .unwrap()
                .unwrap();
            let RealtimeUpdate::Trades(rows) = update else {
                panic!("expected a trade update");
            };
            assert_eq!(
                rows.iter()
                    .map(|trade| trade.id.as_deref())
                    .collect::<Vec<_>>(),
                [Some(id.to_string().as_str())]
            );
            assert_eq!(rows[0].symbol.as_deref(), Some("BTC/USDT:USDT"));
            assert_eq!(rows[0].price, Some(60_000.0));
            assert_eq!(rows[0].amount, Some(0.2));
            assert_eq!(rows[0].timestamp, Some(time));
        }
        assert!(
            timeout(Duration::from_millis(100), subscription.receiver.recv())
                .await
                .is_err()
        );
        drop(subscription);
        hub.shutdown().await.unwrap();
        timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn bybit_trade_results_reach_the_owner_without_cache_replay() {
        trade_owner_delivers_new_rows(Venue::Bybit).await;
    }

    #[tokio::test]
    async fn binance_trade_registration_fits_the_live_worker_stack() {
        trade_owner_delivers_new_rows(Venue::Binance).await;
    }
}
