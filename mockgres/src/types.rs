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

#[allow(dead_code)] // helper for binary DATE encoding when implemented
pub fn date_days_to_postgres(days: i32) -> i32 {
    days - POSTGRES_EPOCH_OFFSET_DAYS
}

#[allow(dead_code)] // helper for binary DATE decoding when implemented
pub fn postgres_days_to_date(days: i32) -> i32 {
    days + POSTGRES_EPOCH_OFFSET_DAYS
}

#[allow(dead_code)] // helper for binary TIMESTAMP encoding when implemented
pub fn timestamp_to_postgres_micros(micros: i64) -> i64 {
    micros - POSTGRES_EPOCH_MICROS
}

#[allow(dead_code)] // helper for binary TIMESTAMP decoding when implemented
pub fn postgres_micros_to_timestamp(micros: i64) -> i64 {
    micros + POSTGRES_EPOCH_MICROS
}

fn parse_hex_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
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
