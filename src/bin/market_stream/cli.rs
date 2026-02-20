use crate::constants::*;

const USAGE: &str = r#"Usage:
  cargo run --bin market_stream -- <mode> [options]

Modes:
  trades       Print newly seen trades from websocket streams
  orderbook    Render a live in-terminal orderbook from websocket streams

Common options:
  --exchange <id>          Exchange id (default: hyperliquid)
  --symbol <symbol>        Market symbol (default: BTC/USDC:USDC)
  --ws-url <url>           Websocket base URL override (default depends on exchange)
  --coin <coin>            Coin override for websocket subscriptions
  --duration-secs <secs>   Stop after this duration
  --iterations <count>     Stop after this many updates

Mode-specific options:
  trades:
    --limit <count>        Dedup buffer sizing hint (default: 25)

  orderbook:
    --levels <count>       Depth levels to display/request (default: 10, min: 10, max: 20)

Examples:
  cargo run --bin market_stream -- trades --exchange bybit --coin BTC
  cargo run --bin market_stream -- orderbook --exchange bybit --coin BTC
  cargo run --bin market_stream -- orderbook --exchange binance --symbol BTC/USDT:USDT

Notes:
  market_stream is websocket-only. Poll transport is intentionally removed.
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Trades,
    OrderBook,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::OrderBook => "orderbook",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) mode: Mode,
    pub(crate) ws_url: String,
    pub(crate) coin: Option<String>,
    pub(crate) exchange: String,
    pub(crate) symbol: String,
    pub(crate) duration_secs: Option<u64>,
    pub(crate) iterations: Option<u64>,
    pub(crate) trade_limit: usize,
    pub(crate) orderbook_levels: usize,
}

#[derive(Debug)]
pub(crate) enum ParseResult {
    Help,
    Run(Config),
}

pub(crate) fn print_usage() {
    println!("{USAGE}");
}

pub(crate) fn parse_args(args: &[String]) -> Result<ParseResult, String> {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(ParseResult::Help);
    }

    let mode = parse_mode(&args[0])?;

    let mut config = Config {
        mode,
        ws_url: DEFAULT_HYPERLIQUID_WS_URL.to_string(),
        coin: None,
        exchange: DEFAULT_EXCHANGE.to_string(),
        symbol: DEFAULT_SYMBOL.to_string(),
        duration_secs: None,
        iterations: None,
        trade_limit: DEFAULT_TRADE_LIMIT,
        orderbook_levels: DEFAULT_ORDERBOOK_LEVELS,
    };

    let mut index = 1usize;
    while index < args.len() {
        let flag = &args[index];
        let value = |i: &mut usize| -> Result<String, String> {
            let next = *i + 1;
            if next >= args.len() {
                return Err(format!("missing value for `{flag}`"));
            }
            *i = next;
            Ok(args[*i].clone())
        };

        match flag.as_str() {
            "--ws-url" => {
                config.ws_url = value(&mut index)?;
            }
            "--coin" => {
                config.coin = Some(value(&mut index)?);
            }
            "--exchange" => {
                config.exchange = value(&mut index)?;
            }
            "--symbol" => {
                config.symbol = value(&mut index)?;
            }
            "--duration-secs" => {
                config.duration_secs =
                    Some(parse_u64_gt_zero("--duration-secs", &value(&mut index)?)?);
            }
            "--iterations" => {
                config.iterations = Some(parse_u64_gt_zero("--iterations", &value(&mut index)?)?);
            }
            "--limit" => {
                config.trade_limit = parse_usize_gt_zero("--limit", &value(&mut index)?)?;
            }
            "--levels" => {
                let levels = parse_usize_gt_zero("--levels", &value(&mut index)?)?;
                config.orderbook_levels = levels.clamp(MIN_ORDERBOOK_LEVELS, MAX_ORDERBOOK_LEVELS);
            }
            "--transport" | "--poll-ms" | "--base-url" | "--timeout-secs" | "--render-ms"
            | "--timeframe" | "--chart-height" => {
                return Err(format!(
                    "`{flag}` is no longer supported; market_stream is websocket-only"
                ));
            }
            _ => {
                return Err(format!("unknown argument `{flag}`"));
            }
        }

        index += 1;
    }

    if config.ws_url.trim().is_empty() {
        return Err("`--ws-url` cannot be empty".to_string());
    }
    if config.exchange.trim().is_empty() {
        return Err("`--exchange` cannot be empty".to_string());
    }
    if config.symbol.trim().is_empty() {
        return Err("`--symbol` cannot be empty".to_string());
    }
    if let Some(coin) = config.coin.as_deref() {
        if coin.trim().is_empty() {
            return Err("`--coin` cannot be empty".to_string());
        }
    }

    Ok(ParseResult::Run(config))
}

fn parse_mode(value: &str) -> Result<Mode, String> {
    match value {
        "trades" => Ok(Mode::Trades),
        "orderbook" => Ok(Mode::OrderBook),
        _ => Err(format!(
            "invalid mode `{value}` (expected `trades` or `orderbook`)"
        )),
    }
}

fn parse_u64_gt_zero(field: &str, value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("`{field}` expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{field}` must be greater than 0"));
    }
    Ok(parsed)
}

fn parse_usize_gt_zero(field: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("`{field}` expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{field}` must be greater than 0"));
    }
    Ok(parsed)
}
