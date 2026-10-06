use super::*;

pub(super) fn bybit_linear() -> JsonValue {
    json!({
        "id": "BTCUSDT",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "type": "swap",
        "spot": false,
        "swap": true,
        "future": false,
        "option": false,
        "active": true,
        "contract": true,
        "linear": true,
        "inverse": false,
        "contractSize": 1.0,
        "precision": {"amount": 0.001, "price": 0.1},
        "limits": {"amount": {"min": 0.001, "max": null}},
        "info": {
            "symbol": "BTCUSDT",
            "contractType": "LinearPerpetual",
            "status": "Trading",
            "settleCoin": "USDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT"
        }
    })
}

pub(super) fn bybit_inverse() -> JsonValue {
    json!({
        "id": "BTCUSD",
        "symbol": "BTC/USD:BTC",
        "base": "BTC",
        "quote": "USD",
        "settle": "BTC",
        "type": "swap",
        "spot": false,
        "swap": true,
        "future": false,
        "option": false,
        "active": true,
        "contract": true,
        "linear": false,
        "inverse": true,
        "contractSize": 1.0,
        "precision": {"amount": 0.001, "price": 0.5},
        "limits": {"amount": {"min": 0.001, "max": null}},
        "info": {
            "symbol": "BTCUSD",
            "contractType": "InversePerpetual",
            "status": "Trading",
            "settleCoin": "BTC",
            "baseCoin": "BTC",
            "quoteCoin": "USD"
        }
    })
}

pub(super) fn bybit_spot() -> JsonValue {
    json!({
        "id": "BTCUSDT",
        "symbol": "BTC/USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": null,
        "type": "spot",
        "spot": true,
        "swap": false,
        "future": false,
        "option": false,
        "active": true,
        "contract": false,
        "linear": null,
        "inverse": null,
        "precision": {"amount": 0.000001, "price": 0.01},
        "limits": {"amount": {"min": null, "max": null}},
        "info": {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "status": "Trading"
        }
    })
}

pub(super) fn bybit_option() -> JsonValue {
    json!({
        "id": "BTC-27FEB26-100000-C-USDT",
        "symbol": "BTC/USDT:USDT-260227-100000-C",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "type": "option",
        "spot": false,
        "swap": false,
        "future": false,
        "option": true,
        "active": true,
        "contract": true,
        "linear": null,
        "inverse": null,
        "contractSize": 0.01,
        "precision": {"amount": 0.01, "price": 5.0},
        "limits": {"amount": {"min": 0.01, "max": null}},
        "info": {
            "symbol": "BTC-27FEB26-100000-C-USDT",
            "contractType": "Option",
            "status": "Trading"
        }
    })
}

pub(super) fn binance_linear() -> JsonValue {
    json!({
        "id": "BTCUSDT",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "type": "swap",
        "spot": false,
        "swap": true,
        "future": false,
        "option": false,
        "active": true,
        "contract": true,
        "linear": true,
        "inverse": false,
        "contractSize": 1.0,
        "precision": {"amount": 0.001, "price": 0.1},
        "limits": {"amount": {"min": 0.001, "max": null}},
        "info": {
            "symbol": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "marginAsset": "USDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT"
        }
    })
}

pub(super) fn binance_spot() -> JsonValue {
    json!({
        "id": "BTCUSDT",
        "symbol": "BTC/USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": null,
        "type": "spot",
        "spot": true,
        "swap": false,
        "future": false,
        "option": false,
        "active": true,
        "contract": false,
        "linear": null,
        "inverse": null,
        "precision": {"amount": 0.000001, "price": 0.01},
        "limits": {"amount": {"min": null, "max": null}},
        "info": {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT"
        }
    })
}

pub(super) fn hyperliquid_perp(name: &str, id: &str, dex: Option<&str>) -> JsonValue {
    let mut info = json!({"name": name, "szDecimals": 4, "maxLeverage": 50});
    if let Some(dex) = dex {
        info["dex"] = json!(dex);
        info["collateralTokenName"] = json!("USDC");
    }
    json!({
        "id": id,
        "symbol": format!("{name}/USDC:USDC"),
        "base": name,
        "quote": "USDC",
        "settle": "USDC",
        "type": "swap",
        "spot": false,
        "swap": true,
        "future": false,
        "option": false,
        "active": true,
        "contract": true,
        "linear": true,
        "inverse": false,
        "contractSize": 1.0,
        "precision": {"amount": 0.0001, "price": 0.1},
        "limits": {"amount": {"min": null, "max": null}},
        "info": info
    })
}

pub(super) fn hyperliquid_spot() -> JsonValue {
    json!({
        "id": "PURR/USDC",
        "symbol": "PURR/USDC",
        "base": "PURR",
        "quote": "USDC",
        "settle": null,
        "type": "spot",
        "spot": true,
        "swap": false,
        "future": false,
        "option": false,
        "active": true,
        "contract": false,
        "linear": null,
        "inverse": null,
        "precision": {"amount": 1.0, "price": 0.001},
        "limits": {"amount": {"min": null, "max": null}},
        "info": {"name": "PURR/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}
    })
}
