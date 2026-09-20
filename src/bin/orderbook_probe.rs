use std::{
    env,
    error::Error,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_BACKEND_WS_URL: &str = "ws://127.0.0.1:8787/v1/ws";
const DEFAULT_BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws/btcusdt@depth@100ms";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeMode {
    Backend,
    BinanceDirect,
}

impl ProbeMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "backend" => Ok(Self::Backend),
            "binance-direct" | "binance" | "direct" => Ok(Self::BinanceDirect),
            other => Err(format!(
                "unsupported mode `{other}`; expected `backend` or `binance-direct`"
            )),
        }
    }
}

#[derive(Debug)]
struct Config {
    mode: ProbeMode,
    ws_url: String,
    exchange: String,
    symbol: String,
    levels: usize,
    seconds: u64,
    print_messages: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: ProbeMode::Backend,
            ws_url: DEFAULT_BACKEND_WS_URL.to_string(),
            exchange: "binance".to_string(),
            symbol: "BTC/USDT:USDT".to_string(),
            levels: 1_000,
            seconds: 20,
            print_messages: false,
        }
    }
}

#[derive(Debug)]
struct Stats {
    update_count: u64,
    interval_count: u64,
    min_interval: Option<Duration>,
    max_interval: Duration,
    total_interval: Duration,
    over_one_second: u64,
    over_three_seconds: u64,
}

impl Stats {
    fn record_interval(&mut self, delta: Duration) {
        self.interval_count += 1;
        self.total_interval += delta;
        self.max_interval = self.max_interval.max(delta);
        self.min_interval = Some(match self.min_interval {
            Some(current) => current.min(delta),
            None => delta,
        });
        if delta >= Duration::from_secs(1) {
            self.over_one_second += 1;
        }
        if delta >= Duration::from_secs(3) {
            self.over_three_seconds += 1;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_config(env::args().skip(1).collect())?;
    run_probe(config).await
}

fn parse_config(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    let mut config = Config::default();
    let mut index = 0;

    while index < args.len() {
        let key = args[index].as_str();
        match key {
            "--mode" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or("missing value for --mode")?
                    .to_string();
                config.mode = ProbeMode::parse(&value)?;
                if config.mode == ProbeMode::BinanceDirect
                    && config.ws_url == DEFAULT_BACKEND_WS_URL
                {
                    config.ws_url = DEFAULT_BINANCE_WS_URL.to_string();
                }
            }
            "--ws-url" => {
                index += 1;
                config.ws_url = args
                    .get(index)
                    .ok_or("missing value for --ws-url")?
                    .to_string();
            }
            "--exchange" => {
                index += 1;
                config.exchange = args
                    .get(index)
                    .ok_or("missing value for --exchange")?
                    .to_string();
            }
            "--symbol" => {
                index += 1;
                config.symbol = args
                    .get(index)
                    .ok_or("missing value for --symbol")?
                    .to_string();
            }
            "--levels" => {
                index += 1;
                let value = args.get(index).ok_or("missing value for --levels")?;
                config.levels = value
                    .parse::<usize>()
                    .map_err(|err| format!("invalid --levels `{value}`: {err}"))?;
            }
            "--seconds" => {
                index += 1;
                let value = args.get(index).ok_or("missing value for --seconds")?;
                config.seconds = value
                    .parse::<u64>()
                    .map_err(|err| format!("invalid --seconds `{value}`: {err}"))?;
            }
            "--print" => {
                config.print_messages = true;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(format!("unrecognized argument `{other}`").into());
            }
        }

        index += 1;
    }

    if config.levels == 0 {
        return Err("--levels must be greater than 0".into());
    }
    if config.seconds == 0 {
        return Err("--seconds must be greater than 0".into());
    }

    Ok(config)
}

fn print_help() {
    println!(
        "orderbook_probe\n\n\
Usage:\n\
  cargo run --bin orderbook_probe -- [options]\n\n\
Options:\n\
  --mode backend|binance-direct   Probe backend websocket or Binance directly (default: backend)\n\
  --ws-url <url>                  Websocket URL override\n\
  --exchange <id>                 Exchange for backend mode (default: binance)\n\
  --symbol <symbol>               Symbol for backend mode (default: BTC/USDT:USDT)\n\
  --levels <n>                    Requested levels for backend mode (default: 1000)\n\
  --seconds <n>                   Probe duration in seconds (default: 20)\n\
  --print                          Print every update payload header\n"
    );
}

async fn run_probe(config: Config) -> Result<(), Box<dyn Error>> {
    println!(
        "connecting mode={:?} ws_url={} seconds={} ...",
        config.mode, config.ws_url, config.seconds
    );

    let (mut socket, response) = connect_async(&config.ws_url).await?;
    println!("connected status={}", response.status());

    if config.mode == ProbeMode::Backend {
        let subscribe = json!({
            "op": "subscribe",
            "channel": "orderbook",
            "exchange": config.exchange,
            "symbol": config.symbol,
            "params": {
                "levels": config.levels
            }
        });
        socket
            .send(Message::Text(subscribe.to_string().into()))
            .await?;
        println!("sent subscribe request: {}", subscribe);
    }

    let end = Instant::now() + Duration::from_secs(config.seconds);
    let mut stats = Stats {
        update_count: 0,
        interval_count: 0,
        min_interval: None,
        max_interval: Duration::ZERO,
        total_interval: Duration::ZERO,
        over_one_second: 0,
        over_three_seconds: 0,
    };
    let mut last_update_at: Option<Instant> = None;
    let mut window_start = Instant::now();
    let mut window_updates = 0u64;

    while Instant::now() < end {
        let timeout = end.saturating_duration_since(Instant::now());
        let next = tokio::time::timeout(timeout, socket.next()).await;
        let message = match next {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(err))) => return Err(format!("websocket frame error: {err}").into()),
            Ok(None) => {
                println!("websocket closed by upstream");
                break;
            }
            Err(_) => break,
        };

        match message {
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Message::Text(text) => {
                let text = text.to_string();
                if !is_update_payload(config.mode, &text) {
                    if config.print_messages {
                        println!("non-update: {text}");
                    }
                    continue;
                }

                let now = Instant::now();
                stats.update_count += 1;
                window_updates += 1;

                if let Some(previous) = last_update_at {
                    stats.record_interval(now.saturating_duration_since(previous));
                }
                last_update_at = Some(now);

                if window_start.elapsed() >= Duration::from_secs(1) {
                    println!(
                        "updates in last {:.1}s: {}",
                        window_start.elapsed().as_secs_f64(),
                        window_updates
                    );
                    window_start = now;
                    window_updates = 0;
                }

                if config.print_messages {
                    let summary = summarize_payload(config.mode, &text);
                    println!("update #{:>5}: {summary}", stats.update_count);
                }
            }
            Message::Binary(binary) => {
                if let Ok(text) = String::from_utf8(binary.to_vec()) {
                    if !is_update_payload(config.mode, &text) {
                        continue;
                    }

                    let now = Instant::now();
                    stats.update_count += 1;
                    window_updates += 1;
                    if let Some(previous) = last_update_at {
                        stats.record_interval(now.saturating_duration_since(previous));
                    }
                    last_update_at = Some(now);
                }
            }
            Message::Close(frame) => {
                println!("received close frame: {:?}", frame);
                break;
            }
            _ => {}
        }
    }

    let avg_interval = if stats.interval_count == 0 {
        None
    } else {
        Some(stats.total_interval / stats.interval_count as u32)
    };

    println!("\nprobe complete");
    println!("updates: {}", stats.update_count);
    println!(
        "min/avg/max interval: {}/{}/{}",
        format_duration(stats.min_interval.unwrap_or(Duration::ZERO)),
        format_duration(avg_interval.unwrap_or(Duration::ZERO)),
        format_duration(stats.max_interval),
    );
    println!(
        "intervals >=1s: {} | >=3s: {}",
        stats.over_one_second, stats.over_three_seconds
    );

    Ok(())
}

fn is_update_payload(mode: ProbeMode, payload: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return false;
    };

    match mode {
        ProbeMode::Backend => value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind == "orderbook"),
        ProbeMode::BinanceDirect => {
            let data = value.get("data").unwrap_or(&value);
            data.get("e")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind == "depthUpdate")
        }
    }
}

fn summarize_payload(mode: ProbeMode, payload: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return "invalid-json".to_string();
    };

    match mode {
        ProbeMode::Backend => {
            let data = value.get("data").unwrap_or(&Value::Null);
            let nonce = data.get("nonce").and_then(Value::as_u64).unwrap_or(0);
            let bids = data
                .get("bids")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let asks = data
                .get("asks")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            format!("nonce={nonce} bids={bids} asks={asks}")
        }
        ProbeMode::BinanceDirect => {
            let data = value.get("data").unwrap_or(&value);
            let first = data.get("U").and_then(Value::as_u64).unwrap_or(0);
            let last = data.get("u").and_then(Value::as_u64).unwrap_or(0);
            format!("U={first} u={last}")
        }
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0ms".to_string();
    }
    format!("{:.1}ms", duration.as_secs_f64() * 1_000.0)
}
