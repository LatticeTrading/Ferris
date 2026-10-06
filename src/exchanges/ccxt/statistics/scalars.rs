//! Scalar validation shared by REST observations and live patches.

use ccxt::{value::get_value_k, Value};

/// Raw lexical string from a stock `info` object, preserving native spelling.
pub(in crate::exchanges::ccxt) fn raw_lexical(info: &Value, key: &str) -> Option<String> {
    lexical(&get_value_k(info, key))
}

pub(in crate::exchanges::ccxt) fn lexical(value: &Value) -> Option<String> {
    match value {
        Value::Str(text) => decimal_string(text).map(str::to_string),
        Value::Int(number) => Some(number.to_string()),
        Value::Float(number) if number.is_finite() => Some(format_f64(*number)),
        _ => None,
    }
}

pub(in crate::exchanges::ccxt) fn lexical_f64(value: &str) -> Option<f64> {
    let parsed: f64 = value.parse().ok()?;
    parsed.is_finite().then_some(parsed)
}

/// A nullable numeric member. `Err(())` means present-but-invalid (non-finite,
/// non-numeric, or negative), which clears the whole outer field.
pub(in crate::exchanges::ccxt) fn nonneg_member(value: &Value) -> Result<Option<f64>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    let text = lexical(value).ok_or(())?;
    let number = lexical_f64(&text).ok_or(())?;
    if number < 0.0 {
        return Err(());
    }
    Ok(Some(number))
}

/// Same policy for a stock unified numeric member.
pub(in crate::exchanges::ccxt) fn nonneg_number(value: &Value) -> Result<Option<f64>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    let number = unified_number(value).ok_or(())?;
    if number < 0.0 {
        return Err(());
    }
    Ok(Some(number))
}

pub(in crate::exchanges::ccxt) fn unified_number(value: &Value) -> Option<f64> {
    match value {
        Value::Int(number) => Some(*number as f64),
        Value::Float(number) if number.is_finite() => Some(*number),
        Value::Str(text) => lexical_f64(text),
        _ => None,
    }
}

pub(in crate::exchanges::ccxt) fn positive_integer(value: &Value) -> Option<u64> {
    match value {
        Value::Int(number) if *number > 0 => Some(*number as u64),
        Value::Float(number)
            if number.is_finite()
                && *number > 0.0
                && number.fract() == 0.0
                && *number < u64::MAX as f64 =>
        {
            Some(*number as u64)
        }
        Value::Str(text) => text.parse::<u64>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

pub(in crate::exchanges::ccxt) fn exchange_time(info: &Value) -> Option<u64> {
    positive_integer(&get_value_k(info, "time"))
        .or_else(|| positive_integer(&get_value_k(info, "closeTime")))
}

pub(in crate::exchanges::ccxt) fn format_f64(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// Validate lexically, without floating-point conversion or changing precision.
pub(in crate::exchanges::ccxt) fn decimal_string(value: &str) -> Option<&str> {
    let digits = value.strip_prefix('-').unwrap_or(value).as_bytes();
    let integer_end = digits
        .iter()
        .position(|byte| *byte == b'.')
        .unwrap_or(digits.len());
    if integer_end == 0 || !digits[..integer_end].iter().all(u8::is_ascii_digit) {
        return None;
    }
    if integer_end < digits.len() {
        let fraction = &digits[integer_end + 1..];
        if fraction.is_empty() || !fraction.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }
    Some(value)
}
