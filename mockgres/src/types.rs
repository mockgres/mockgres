use crate::session::SessionTimeZone;
use time::{Date, Duration, Month, PrimitiveDateTime, Time};

#[allow(dead_code)] // reserved for upcoming binary wire support
const POSTGRES_EPOCH_OFFSET_DAYS: i32 = 10957; // days between 1970-01-01 and 2000-01-01
#[allow(dead_code)] // reserved for upcoming binary wire support
const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_i64 * 1_000_000; // seconds * 1e6

fn epoch_date() -> Date {
    Date::from_calendar_date(1970, Month::January, 1).expect("valid epoch date")
}
const DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]");
const TIMESTAMP_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;
const MICROS_PER_DAY: i64 = MICROS_PER_SECOND * SECONDS_PER_DAY;

pub fn parse_date_str(s: &str) -> Result<i32, String> {
    let date = Date::parse(s, DATE_FORMAT).map_err(|e| e.to_string())?;
    let duration = date - epoch_date();
    Ok(duration.whole_days() as i32)
}

pub fn format_date(days: i32) -> Result<String, String> {
    let date = epoch_date()
        .checked_add(Duration::days(days as i64))
        .ok_or_else(|| "date out of range".to_string())?;
    date.format(DATE_FORMAT).map_err(|e| e.to_string())
}

pub fn parse_timestamp_str(s: &str) -> Result<i64, String> {
    let normalized = s.replace('T', " ");
    let date_time =
        PrimitiveDateTime::parse(&normalized, TIMESTAMP_FORMAT).map_err(|e| e.to_string())?;
    let epoch_dt = PrimitiveDateTime::new(epoch_date(), Time::MIDNIGHT);
    let duration = date_time - epoch_dt;
    let micros = duration.whole_microseconds();
    let value: i64 = micros
        .try_into()
        .map_err(|_| "timestamp out of range".to_string())?;
    Ok(value)
}

pub fn format_timestamp(micros: i64) -> Result<String, String> {
    let duration = Duration::microseconds(micros);
    let epoch_dt = PrimitiveDateTime::new(epoch_date(), Time::MIDNIGHT);
    let dt = epoch_dt
        .checked_add(duration)
        .ok_or_else(|| "timestamp out of range".to_string())?;
    dt.format(TIMESTAMP_FORMAT).map_err(|e| e.to_string())
}

fn micro_offset(offset_seconds: i32) -> Result<i64, String> {
    (offset_seconds as i64)
        .checked_mul(MICROS_PER_SECOND)
        .ok_or_else(|| "timestamp out of range".to_string())
}

fn apply_offset(micros: i64, offset_seconds: i32, subtract: bool) -> Result<i64, String> {
    let offset = micro_offset(offset_seconds)?;
    if subtract {
        micros
            .checked_sub(offset)
            .ok_or_else(|| "timestamp out of range".to_string())
    } else {
        micros
            .checked_add(offset)
            .ok_or_else(|| "timestamp out of range".to_string())
    }
}

fn split_timestamptz_offset(input: &str) -> Result<(&str, Option<i32>), String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("invalid timestamptz value".to_string());
    }
    if trimmed.ends_with('Z') || trimmed.ends_with('z') {
        let dt = trimmed[..trimmed.len() - 1].trim_end();
        return Ok((dt, Some(0)));
    }
    if let Some((idx, offset_part)) = find_offset_suffix(trimmed) {
        let tz = SessionTimeZone::parse(offset_part)
            .map_err(|_| "invalid timestamptz offset".to_string())?;
        let dt = trimmed[..idx].trim_end();
        return Ok((dt, Some(tz.offset_seconds())));
    }
    Ok((trimmed, None))
}

fn find_offset_suffix(input: &str) -> Option<(usize, &str)> {
    for (idx, ch) in input.char_indices().rev() {
        if ch == '+' || ch == '-' {
            let candidate = &input[idx..];
            if looks_like_offset_suffix(candidate) {
                return Some((idx, candidate));
            }
        }
    }
    None
}

fn looks_like_offset_suffix(candidate: &str) -> bool {
    if candidate.len() < 2 {
        return false;
    }
    let bytes = candidate.as_bytes();
    if bytes[0] != b'+' && bytes[0] != b'-' {
        return false;
    }
    let body = &candidate[1..];
    if body.is_empty() {
        return false;
    }
    if let Some(colon_idx) = body.find(':') {
        if colon_idx == 0 || colon_idx > 2 {
            return false;
        }
        if body[colon_idx + 1..].contains(':') {
            return false;
        }
        let hour = &body[..colon_idx];
        let minute = &body[colon_idx + 1..];
        if minute.len() != 2 {
            return false;
        }
        hour.chars().all(|c| c.is_ascii_digit()) && minute.chars().all(|c| c.is_ascii_digit())
    } else {
        (1..=2).contains(&body.len()) && body.chars().all(|c| c.is_ascii_digit())
    }
}

pub fn parse_timestamptz_str(s: &str, session_tz: &SessionTimeZone) -> Result<i64, String> {
    let (dt_part, explicit_offset) = split_timestamptz_offset(s)?;
    let local_micros = parse_timestamp_str(dt_part)?;
    let offset_seconds = explicit_offset.unwrap_or_else(|| session_tz.offset_seconds());
    apply_offset(local_micros, offset_seconds, true)
}

pub fn format_timestamptz(micros: i64, session_tz: &SessionTimeZone) -> Result<String, String> {
    let local = apply_offset(micros, session_tz.offset_seconds(), false)?;
    let mut text = format_timestamp(local)?;
    let offset = session_tz.offset_string();
    text.push_str(&offset);
    Ok(text)
}

pub fn timestamp_to_timestamptz(
    local_micros: i64,
    session_tz: &SessionTimeZone,
) -> Result<i64, String> {
    apply_offset(local_micros, session_tz.offset_seconds(), true)
}

pub fn timestamptz_to_timestamp(
    utc_micros: i64,
    session_tz: &SessionTimeZone,
) -> Result<i64, String> {
    apply_offset(utc_micros, session_tz.offset_seconds(), false)
}

pub fn timestamptz_to_date_days(
    utc_micros: i64,
    session_tz: &SessionTimeZone,
) -> Result<i32, String> {
    let local = apply_offset(utc_micros, session_tz.offset_seconds(), false)?;
    timestamp_micros_to_date_days(local)
}

pub fn date_to_timestamptz(days: i32, session_tz: &SessionTimeZone) -> Result<i64, String> {
    let local = (days as i64)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| "timestamp out of range".to_string())?;
    apply_offset(local, session_tz.offset_seconds(), true)
}

pub fn timestamp_micros_to_date_days(micros: i64) -> Result<i32, String> {
    let duration = Duration::microseconds(micros);
    let epoch_dt = PrimitiveDateTime::new(epoch_date(), Time::MIDNIGHT);
    let dt = epoch_dt
        .checked_add(duration)
        .ok_or_else(|| "timestamp out of range".to_string())?;
    let days = (dt.date() - epoch_date()).whole_days();
    i32::try_from(days).map_err(|_| "date out of range".to_string())
}

pub fn parse_bytea_text(s: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = s.strip_prefix("\\x").or_else(|| s.strip_prefix("\\\\x")) {
        parse_hex_bytes(hex)
    } else {
        Ok(s.as_bytes().to_vec())
    }
}

pub fn format_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 2);
    out.push_str("\\x");
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

#[allow(dead_code)]
pub fn date_days_to_postgres(days: i32) -> i32 {
    days - POSTGRES_EPOCH_OFFSET_DAYS
}

#[allow(dead_code)]
pub fn postgres_days_to_date(days: i32) -> i32 {
    days + POSTGRES_EPOCH_OFFSET_DAYS
}

#[allow(dead_code)]
pub fn timestamp_to_postgres_micros(micros: i64) -> i64 {
    micros - POSTGRES_EPOCH_MICROS
}

#[allow(dead_code)]
pub fn postgres_micros_to_timestamp(micros: i64) -> i64 {
    micros + POSTGRES_EPOCH_MICROS
}

fn parse_hex_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("bytea hex string must have even length".into());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i]).ok_or_else(|| "invalid hex digit".to_string())?;
        let lo = hex_val(bytes[i + 1]).ok_or_else(|| "invalid hex digit".to_string())?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}
