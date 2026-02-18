#!/usr/bin/env python3

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def http_json(method, url, payload=None, timeout=15):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {url} -> HTTP {err.code}. Body: {body if body else '<empty>'}"
        ) from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"{method} {url} -> connection error: {err}") from err


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def wait_for_server(base_url, timeout, wait_seconds):
    deadline = time.time() + wait_seconds
    last_error = None

    while time.time() < deadline:
        try:
            status, body = http_json("GET", f"{base_url}/healthz", timeout=timeout)
            if status == 200 and body.get("status") == "ok":
                return
            last_error = f"GET {base_url}/healthz returned unexpected body: {body}"
        except RuntimeError as err:
            last_error = str(err)

        time.sleep(1)

    raise RuntimeError(
        "server did not become ready in time. "
        f"Last error: {last_error if last_error else 'unknown'}"
    )


def test_health(base_url, timeout):
    status, body = http_json("GET", f"{base_url}/healthz", timeout=timeout)
    assert_true(status == 200, f"healthz status expected 200, got {status}")
    assert_true(body.get("status") == "ok", f"healthz response invalid: {body}")
    print("ok  healthz")


def test_fetch_trades(base_url, exchange, symbol, timeout):
    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "limit": 5,
        "params": {},
    }
    status, body = http_json(
        "POST", f"{base_url}/v1/fetchTrades", payload, timeout=timeout
    )
    assert_true(status == 200, f"fetchTrades status expected 200, got {status}")
    assert_true(
        isinstance(body, list), f"fetchTrades must return list, got: {type(body)}"
    )
    assert_true(len(body) > 0, "fetchTrades returned no trades")

    first = body[0]
    for key in [
        "info",
        "amount",
        "datetime",
        "id",
        "order",
        "price",
        "timestamp",
        "type",
        "side",
        "symbol",
        "takerOrMaker",
        "cost",
        "fee",
    ]:
        assert_true(key in first, f"fetchTrades missing key: {key}")

    assert_true(
        first.get("symbol") == symbol,
        f"trade symbol mismatch: {first.get('symbol')} != {symbol}",
    )
    print(f"ok  fetchTrades ({len(body)} rows)")


def test_fetch_ohlcv(base_url, exchange, symbol, timeout):
    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": "1m",
        "limit": 3,
        "params": {},
    }
    status, body = http_json(
        "POST", f"{base_url}/v1/fetchOHLCV", payload, timeout=timeout
    )
    assert_true(status == 200, f"fetchOHLCV status expected 200, got {status}")
    assert_true(
        isinstance(body, list), f"fetchOHLCV must return list, got: {type(body)}"
    )
    assert_true(len(body) > 0, "fetchOHLCV returned no candles")

    first = body[0]
    assert_true(isinstance(first, list), "fetchOHLCV row must be list")
    assert_true(
        len(first) == 6, f"fetchOHLCV row must have 6 elements, got {len(first)}"
    )
    print(f"ok  fetchOHLCV ({len(body)} rows)")


def test_fetch_order_book(base_url, exchange, symbol, timeout):
    payload = {
        "exchange": exchange,
        "symbol": symbol,
        "limit": 2,
        "params": {},
    }
    status, body = http_json(
        "POST", f"{base_url}/v1/fetchOrderBook", payload, timeout=timeout
    )
    assert_true(status == 200, f"fetchOrderBook status expected 200, got {status}")
    assert_true(
        isinstance(body, dict), f"fetchOrderBook must return object, got: {type(body)}"
    )

    for key in ["asks", "bids", "datetime", "timestamp", "nonce", "symbol"]:
        assert_true(key in body, f"fetchOrderBook missing key: {key}")

    asks = body.get("asks", [])
    bids = body.get("bids", [])
    assert_true(isinstance(asks, list), "fetchOrderBook asks must be list")
    assert_true(isinstance(bids, list), "fetchOrderBook bids must be list")
    assert_true(len(asks) > 0, "fetchOrderBook asks cannot be empty")
    assert_true(len(bids) > 0, "fetchOrderBook bids cannot be empty")
    assert_true(
        body.get("symbol") == symbol,
        f"order book symbol mismatch: {body.get('symbol')} != {symbol}",
    )
    print("ok  fetchOrderBook")


def main():
    parser = argparse.ArgumentParser(
        description="Smoke test Ferris market-data endpoints against a running server."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--exchange", default="hyperliquid")
    parser.add_argument("--symbol", default="BTC/USDC:USDC")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=90,
        help="how long to wait for /healthz before failing",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    print(f"testing {base_url} exchange={args.exchange} symbol={args.symbol}")
    try:
        if args.wait_seconds > 0:
            print(f"waiting for server readiness (up to {args.wait_seconds}s)...")
            wait_for_server(base_url, args.timeout, args.wait_seconds)

        test_health(base_url, args.timeout)
        test_fetch_trades(base_url, args.exchange, args.symbol, args.timeout)
        test_fetch_ohlcv(base_url, args.exchange, args.symbol, args.timeout)
        test_fetch_order_book(base_url, args.exchange, args.symbol, args.timeout)
    except (AssertionError, RuntimeError, json.JSONDecodeError) as err:
        print(f"fail {err}", file=sys.stderr)
        print(
            "hint: if you see HTTP 404 on /healthz, you are likely hitting a different service/port.",
            file=sys.stderr,
        )
        return 1

    print("all endpoint checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
