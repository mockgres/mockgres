use super::expr_plan::ScalarExpr;
use crate::session::SessionTimeZone;
use crate::types::{
    date_to_timestamptz, format_timestamp, format_timestamptz, parse_bytea_text, parse_date_str,
    parse_timestamp_str, parse_timestamptz_str, timestamp_micros_to_date_days,
    timestamp_to_timestamptz, timestamptz_to_date_days, timestamptz_to_timestamp,
};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError};
use serde_json::Value as JsonValue;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct SqlError {
    pub code: &'static str,
    pub message: String,
}

impl SqlError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SqlError {}

pub fn fe(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg.into(),
    )))
}

pub fn fe_code(code: &'static str, msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.into(),
    )))
}

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Int2,
    Int4,
    Int8,
    Float8,
    Text,
    Name,
    BpChar(Option<usize>),
    Point,
    Path,
    Json,
    Jsonb,
    Bool,
    Date,
    Timestamp,
    Timestamptz,
    Bytea,
    Interval,
    Void,
}

impl DataType {
    pub fn to_pg(&self) -> Type {
        match self {
            DataType::Int2 => Type::INT2,
            DataType::Int4 => Type::INT4,
            DataType::Int8 => Type::INT8,
            DataType::Float8 => Type::FLOAT8,
            DataType::Text => Type::TEXT,
            DataType::Name => Type::NAME,
            DataType::BpChar(_) => Type::BPCHAR,
            DataType::Point => Type::POINT,
            DataType::Path => Type::PATH,
            DataType::Json => Type::JSON,
            DataType::Jsonb => Type::JSONB,
            DataType::Bool => Type::BOOL,
            DataType::Date => Type::DATE,
            DataType::Timestamp => Type::TIMESTAMP,
            DataType::Timestamptz => Type::TIMESTAMPTZ,
            DataType::Bytea => Type::BYTEA,
            DataType::Interval => Type::INTERVAL,
            DataType::Void => Type::VOID,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IdentitySpec {
    pub always: bool,
    pub start_with: i128,
    pub increment_by: i128,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<ScalarExpr>,
    pub identity: Option<IdentitySpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldOrigin {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub origin: Option<FieldOrigin>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PointValue {
    x_bits: u64,
    y_bits: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathValue {
    closed: bool,
    points: Vec<PointValue>,
}

impl PathValue {
    pub fn new(closed: bool, points: Vec<PointValue>) -> Self {
        Self { closed, points }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn points(&self) -> &[PointValue] {
        &self.points
    }
}

impl PointValue {
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            x_bits: x.to_bits(),
            y_bits: y.to_bits(),
        }
    }

    pub fn x(self) -> f64 {
        f64::from_bits(self.x_bits)
    }

    pub fn y(self) -> f64 {
        f64::from_bits(self.y_bits)
    }
}

impl Schema {
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }
    pub fn len(&self) -> usize {
        self.fields.len()
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int64(i64),
    Float64Bits(u64),
    Text(String),
    Point(PointValue),
    Path(PathValue),
    Bool(bool),
    Date(i32),
    TimestampMicros(i64),
    TimestamptzMicros(i64),
    Bytes(Vec<u8>),
    IntervalMicros(i64),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Int64(a), Int64(b)) => a == b,
            (Float64Bits(a), Float64Bits(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Point(a), Point(b)) => a == b,
            (Path(a), Path(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (TimestampMicros(a), TimestampMicros(b)) => a == b,
            (TimestamptzMicros(a), TimestamptzMicros(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (IntervalMicros(a), IntervalMicros(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Null => {}
            Int64(v) => v.hash(state),
            Float64Bits(v) => v.hash(state),
            Text(s) => s.hash(state),
            Point(point) => point.hash(state),
            Path(path) => path.hash(state),
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            TimestampMicros(t) => t.hash(state),
            TimestamptzMicros(t) => t.hash(state),
            Bytes(b) => b.hash(state),
            IntervalMicros(v) => v.hash(state),
        }
    }
}

impl Value {
    pub fn from_f64(f: f64) -> Self {
        Value::Float64Bits(f.to_bits())
    }
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float64Bits(b) = self {
            Some(f64::from_bits(*b))
        } else {
            None
        }
    }

    pub fn as_interval_micros(&self) -> Option<i64> {
        if let Value::IntervalMicros(v) = self {
            Some(*v)
        } else {
            None
        }
    }
}

pub fn cast_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    convert_value_to_type(val, target, tz, false)
}

pub fn parse_point_text(input: &str) -> Result<PointValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type point: \"{input}\""),
        )
    }

    let trimmed = input.trim();
    let coordinates = if let Some(inner) = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    {
        inner
    } else if trimmed.starts_with('(') || trimmed.ends_with(')') {
        return Err(invalid(input));
    } else {
        trimmed
    };
    let mut parts = coordinates.split(',');
    let x = parts.next().ok_or_else(|| invalid(input))?;
    let y = parts.next().ok_or_else(|| invalid(input))?;
    if parts.next().is_some() || x.trim().is_empty() || y.trim().is_empty() {
        return Err(invalid(input));
    }
    Ok(PointValue::new(
        parse_geometry_coordinate(x, input, "point")?,
        parse_geometry_coordinate(y, input, "point")?,
    ))
}

pub fn format_point_text(point: PointValue) -> String {
    format!(
        "({},{})",
        format_geometry_coordinate(point.x()),
        format_geometry_coordinate(point.y())
    )
}

pub fn parse_path_text(input: &str) -> Result<PathValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type path: \"{input}\""),
        )
    }

    fn matching_close(input: &str) -> Option<usize> {
        let mut depth = 0_usize;
        for (index, ch) in input.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_points(mut inner: &str, input: &str) -> Result<Vec<PointValue>, SqlError> {
        let mut points = Vec::new();
        loop {
            inner = inner.trim_start();
            let (x, y, rest, needs_separator, allows_end) =
                if let Some(rest) = inner.strip_prefix('(') {
                    let Some(close) = rest.find(')') else {
                        return Err(invalid(input));
                    };
                    let coordinates = &rest[..close];
                    if coordinates.contains(['(', ')', '[', ']']) {
                        return Err(invalid(input));
                    }
                    let mut pair = coordinates.split(',');
                    let x = pair.next().ok_or_else(|| invalid(input))?;
                    let y = pair.next().ok_or_else(|| invalid(input))?;
                    if pair.next().is_some() {
                        return Err(invalid(input));
                    }
                    (x, y, &rest[close + 1..], true, true)
                } else {
                    let Some((x, rest)) = inner.split_once(',') else {
                        return Err(invalid(input));
                    };
                    if let Some((y, rest)) = rest.split_once(',') {
                        (x, y, rest, false, false)
                    } else {
                        (x, rest, "", false, true)
                    }
                };
            if x.trim().is_empty() || y.trim().is_empty() {
                return Err(invalid(input));
            }
            points.push(PointValue::new(
                parse_geometry_coordinate(x, input, "path")?,
                parse_geometry_coordinate(y, input, "path")?,
            ));

            inner = rest.trim_start();
            if inner.is_empty() {
                return if allows_end {
                    Ok(points)
                } else {
                    Err(invalid(input))
                };
            }
            if needs_separator {
                let Some(rest) = inner.strip_prefix(',') else {
                    return Err(invalid(input));
                };
                inner = rest;
            }
            if inner.trim().is_empty() {
                return Err(invalid(input));
            }
        }
    }

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(invalid(input));
    }

    let (closed, inner) = if let Some(rest) = trimmed.strip_prefix('[') {
        let Some(inner) = rest.strip_suffix(']') else {
            return Err(invalid(input));
        };
        (false, inner)
    } else if trimmed.starts_with('(') {
        let Some(close) = matching_close(trimmed) else {
            return Err(invalid(input));
        };
        if close == trimmed.len() - 1 {
            (true, &trimmed[1..trimmed.len() - 1])
        } else {
            (true, trimmed)
        }
    } else {
        (true, trimmed)
    };

    let inner = inner.trim();
    if inner.is_empty() || inner.contains(['[', ']']) {
        return Err(invalid(input));
    }
    let points = parse_points(inner, input)?;
    Ok(PathValue::new(closed, points))
}

pub fn format_path_text(path: &PathValue) -> String {
    let points = path
        .points()
        .iter()
        .copied()
        .map(format_point_text)
        .collect::<Vec<_>>()
        .join(",");
    if path.is_closed() {
        format!("({points})")
    } else {
        format!("[{points}]")
    }
}

fn parse_geometry_coordinate(value: &str, input: &str, type_name: &str) -> Result<f64, SqlError> {
    let trimmed = value.trim();
    let normalized = match trimmed.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => "inf",
        "-inf" | "-infinity" => "-inf",
        "nan" | "+nan" | "-nan" => "NaN",
        _ => trimmed,
    };
    let coordinate = normalized.parse::<f64>().map_err(|_| {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type {type_name}: \"{input}\""),
        )
    })?;
    let explicitly_infinite = matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "inf" | "+inf" | "infinity" | "+infinity" | "-inf" | "-infinity"
    );
    if coordinate.is_infinite() && !explicitly_infinite {
        return Err(SqlError::new(
            "22003",
            format!("\"{trimmed}\" is out of range for type double precision"),
        ));
    }
    Ok(coordinate)
}

fn format_geometry_coordinate(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    let mut buffer = ryu::Buffer::new();
    let mut formatted = buffer.format(value).to_string();
    if formatted.ends_with(".0") {
        formatted.truncate(formatted.len() - 2);
    }
    if let Some(exponent) = formatted.find('e')
        && !matches!(formatted.as_bytes().get(exponent + 1), Some(b'+' | b'-'))
    {
        formatted.insert(exponent + 1, '+');
    }
    formatted
}

pub fn coerce_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    convert_value_to_type(val, target, tz, true)
}

fn convert_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
    assignment: bool,
) -> Result<Value, SqlError> {
    fn validate_json(input: &str, type_name: &str) -> Result<(), SqlError> {
        serde_json::from_str::<JsonValue>(input).map_err(|e| {
            SqlError::new(
                "22P02",
                format!("invalid input syntax for type {type_name}: {e}"),
            )
        })?;
        Ok(())
    }

    fn parse_integer_input(
        input: &str,
        min: i128,
        max: i128,
        type_name: &str,
    ) -> Result<i64, SqlError> {
        let parsed = input.trim().parse::<i128>().map_err(|_| {
            SqlError::new(
                "22P02",
                format!("invalid input syntax for type {type_name}: \"{input}\""),
            )
        })?;
        if parsed < min || parsed > max {
            return Err(SqlError::new(
                "22003",
                format!("value \"{input}\" is out of range for type {type_name}"),
            ));
        }
        Ok(parsed as i64)
    }

    fn coerce_bpchar(
        input: String,
        length: Option<usize>,
        assignment: bool,
    ) -> Result<Value, SqlError> {
        let Some(length) = length else {
            return Ok(Value::Text(input));
        };
        let mut chars = input.chars();
        let mut output: String = chars.by_ref().take(length).collect();
        if assignment && chars.any(|c| c != ' ') {
            return Err(SqlError::new(
                "22001",
                format!("value too long for type character({length})"),
            ));
        }
        let padding = length.saturating_sub(output.chars().count());
        output.extend(std::iter::repeat_n(' ', padding));
        Ok(Value::Text(output))
    }

    fn coerce_name(mut input: String) -> Value {
        const MAX_NAME_BYTES: usize = 63;

        if input.len() > MAX_NAME_BYTES {
            let mut end = MAX_NAME_BYTES;
            while !input.is_char_boundary(end) {
                end -= 1;
            }
            input.truncate(end);
        }
        Value::Text(input)
    }

    match (target, val) {
        (DataType::Int2, Value::Int64(v)) => {
            if v < i16::MIN as i64 || v > i16::MAX as i64 {
                return Err(SqlError::new("22003", "smallint out of range"));
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int2, Value::Text(s)) => {
            parse_integer_input(&s, i16::MIN as i128, i16::MAX as i128, "smallint")
                .map(Value::Int64)
        }
        (DataType::Int4, Value::Int64(v)) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(SqlError::new("22003", "value out of range for int4"));
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int4, Value::Text(s)) => {
            parse_integer_input(&s, i32::MIN as i128, i32::MAX as i128, "integer").map(Value::Int64)
        }
        (DataType::Int8, Value::Int64(v)) => Ok(Value::Int64(v)),
        (DataType::Int8, Value::Text(s)) => {
            parse_integer_input(&s, i64::MIN as i128, i64::MAX as i128, "bigint").map(Value::Int64)
        }
        (DataType::Float8, Value::Float64Bits(bits)) => Ok(Value::Float64Bits(bits)),
        (DataType::Float8, Value::Int64(v)) => Ok(Value::from_f64(v as f64)),
        (DataType::Float8, Value::Text(s)) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| SqlError::new("22P02", format!("invalid input for float8: {e}")))?;
            Ok(Value::from_f64(parsed))
        }
        (DataType::Interval, Value::IntervalMicros(v)) => Ok(Value::IntervalMicros(v)),
        (DataType::Interval, Value::Text(s)) => parse_interval_literal(&s)
            .map(Value::IntervalMicros)
            .map_err(|e| {
                SqlError::new(
                    "22007",
                    format!("invalid input syntax for type interval: {e}"),
                )
            }),
        (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
        (DataType::Text, Value::Bool(b)) => Ok(Value::Text(if b { "t" } else { "f" }.into())),
        (DataType::Text, Value::Int64(i)) => Ok(Value::Text(i.to_string())),
        (DataType::Text, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            Ok(Value::Text(f.to_string()))
        }
        (DataType::Text, Value::IntervalMicros(m)) => Ok(Value::Text(format_interval_micros(m))),
        (DataType::Text, Value::TimestampMicros(m)) => {
            let text = format_timestamp(m).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Text(text))
        }
        (DataType::Text, Value::TimestamptzMicros(m)) => {
            let text = format_timestamptz(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Text(text))
        }
        (DataType::Name, Value::Text(s)) => Ok(coerce_name(s)),
        (DataType::BpChar(length), Value::Text(s)) => coerce_bpchar(s, *length, assignment),
        (DataType::Point, Value::Point(point)) => Ok(Value::Point(point)),
        (DataType::Point, Value::Text(value)) => parse_point_text(&value).map(Value::Point),
        (DataType::Text, Value::Point(point)) => Ok(Value::Text(format_point_text(point))),
        (DataType::Path, Value::Path(path)) => Ok(Value::Path(path)),
        (DataType::Path, Value::Text(value)) => parse_path_text(&value).map(Value::Path),
        (DataType::Text, Value::Path(path)) => Ok(Value::Text(format_path_text(&path))),
        (DataType::Json, Value::Text(s)) => {
            validate_json(&s, "json")?;
            Ok(Value::Text(s))
        }
        (DataType::Json, Value::Bytes(b)) => {
            let s = String::from_utf8(b).map_err(|e| {
                SqlError::new("22P02", format!("invalid input syntax for type json: {e}"))
            })?;
            validate_json(&s, "json")?;
            Ok(Value::Text(s))
        }
        (DataType::Json, Value::Bool(b)) => {
            let text = if b { "true" } else { "false" }.to_string();
            Ok(Value::Text(text))
        }
        (DataType::Json, Value::Int64(i)) => {
            let text = i.to_string();
            validate_json(&text, "json")?;
            Ok(Value::Text(text))
        }
        (DataType::Json, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            let text = f.to_string();
            validate_json(&text, "json")?;
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Text(s)) => {
            validate_json(&s, "jsonb")?;
            Ok(Value::Text(s))
        }
        (DataType::Jsonb, Value::Bytes(b)) => {
            let s = String::from_utf8(b).map_err(|e| {
                SqlError::new("22P02", format!("invalid input syntax for type jsonb: {e}"))
            })?;
            validate_json(&s, "jsonb")?;
            Ok(Value::Text(s))
        }
        (DataType::Jsonb, Value::Bool(b)) => {
            let text = if b { "true" } else { "false" }.to_string();
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Int64(i)) => {
            let text = i.to_string();
            validate_json(&text, "jsonb")?;
            Ok(Value::Text(text))
        }
        (DataType::Jsonb, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            let text = f.to_string();
            validate_json(&text, "jsonb")?;
            Ok(Value::Text(text))
        }
        (DataType::Bool, Value::Bool(b)) => Ok(Value::Bool(b)),
        (DataType::Bool, Value::Text(s)) => {
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                other => Err(SqlError::new(
                    "22P02",
                    format!("invalid input for bool: {other}"),
                )),
            }
        }
        (DataType::Date, Value::Date(d)) => Ok(Value::Date(d)),
        (DataType::Date, Value::Text(s)) => {
            let days = parse_date_str(&s).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Date, Value::TimestampMicros(m)) => {
            let days = timestamp_micros_to_date_days(m).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Date, Value::TimestamptzMicros(m)) => {
            let days = timestamptz_to_date_days(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Timestamp, Value::TimestampMicros(m)) => Ok(Value::TimestampMicros(m)),
        (DataType::Timestamp, Value::Text(s)) => {
            let micros = parse_timestamp_str(&s).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::TimestampMicros(micros))
        }
        (DataType::Timestamp, Value::TimestamptzMicros(m)) => {
            let local = timestamptz_to_timestamp(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestampMicros(local))
        }
        (DataType::Bytea, Value::Bytes(bytes)) => Ok(Value::Bytes(bytes)),
        (DataType::Bytea, Value::Text(s)) => {
            let bytes = parse_bytea_text(&s).map_err(|e| SqlError::new("22001", e))?;
            Ok(Value::Bytes(bytes))
        }
        (DataType::Timestamptz, Value::TimestamptzMicros(m)) => Ok(Value::TimestamptzMicros(m)),
        (DataType::Timestamptz, Value::TimestampMicros(m)) => {
            let utc = timestamp_to_timestamptz(m, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestamptzMicros(utc))
        }
        (DataType::Timestamptz, Value::Date(days)) => {
            let utc = date_to_timestamptz(days, tz).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::TimestamptzMicros(utc))
        }
        (DataType::Timestamptz, Value::Text(s)) => {
            let micros = parse_timestamptz_str(&s, tz).map_err(|e| SqlError::new("22007", e))?;
            Ok(Value::TimestamptzMicros(micros))
        }
        (_, Value::Null) => Ok(Value::Null),
        (dt, got) => Err(SqlError::new(
            "42804",
            format!("type mismatch: expected {dt:?}, got {got:?}"),
        )),
    }
}

pub fn parse_interval_literal(input: &str) -> Result<i64, String> {
    let trimmed = input.trim().to_ascii_lowercase();
    let mut parts = trimmed.split_whitespace();
    let num_str = parts
        .next()
        .ok_or_else(|| "missing interval value".to_string())?;
    let unit = parts
        .next()
        .ok_or_else(|| "missing interval unit".to_string())?;
    if parts.next().is_some() {
        return Err("only single-unit intervals are supported".into());
    }
    let qty: f64 = num_str
        .parse()
        .map_err(|_| "interval value must be numeric".to_string())?;
    let micros_per_unit: f64 = match unit {
        "day" | "days" => 86_400_000_000f64,
        "hour" | "hours" => 3_600_000_000f64,
        "minute" | "minutes" | "min" | "mins" => 60_000_000f64,
        "second" | "seconds" | "sec" | "secs" => 1_000_000f64,
        "millisecond" | "milliseconds" | "msec" | "msecs" => 1_000f64,
        "microsecond" | "microseconds" | "usec" | "usecs" => 1f64,
        other => return Err(format!("unsupported interval unit: {other}")),
    };
    let micros = qty * micros_per_unit;
    Ok(micros.round() as i64)
}

pub fn format_interval_micros(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let mut remaining = micros.unsigned_abs();
    let days = remaining / 86_400_000_000;
    remaining -= days * 86_400_000_000;
    let hours = remaining / 3_600_000_000;
    remaining -= hours * 3_600_000_000;
    let minutes = remaining / 60_000_000;
    remaining -= minutes * 60_000_000;
    let seconds = remaining / 1_000_000;
    let micros_left = remaining - seconds * 1_000_000;
    if micros_left == 0 {
        format!(
            "{}{:02}:{:02}:{:02}{}",
            sign,
            days * 24 + hours,
            minutes,
            seconds,
            if days > 0 {
                format!(" ({} days)", days)
            } else {
                "".into()
            }
        )
    } else {
        format!(
            "{}{:02}:{:02}:{:02}.{:06}{}",
            sign,
            days * 24 + hours,
            minutes,
            seconds,
            micros_left,
            if days > 0 {
                format!(" ({} days)", days)
            } else {
                "".into()
            }
        )
    }
}
