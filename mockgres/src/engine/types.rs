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

mod coerce;
mod text;

pub use coerce::{cast_value_to_type, coerce_value_to_type};
pub use text::*;

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
    Varchar(Option<usize>),
    Name,
    BpChar(Option<usize>),
    PgChar,
    Point,
    Lseg,
    Line,
    Circle,
    Box,
    Tid,
    Oid,
    PgLsn,
    MacAddr,
    MacAddr8,
    Path,
    Json,
    Jsonb,
    Bool,
    Date,
    Time(Option<usize>),
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
            DataType::Varchar(_) => Type::VARCHAR,
            DataType::Name => Type::NAME,
            DataType::BpChar(_) => Type::BPCHAR,
            DataType::PgChar => Type::CHAR,
            DataType::Point => Type::POINT,
            DataType::Lseg => Type::LSEG,
            DataType::Line => Type::LINE,
            DataType::Circle => Type::CIRCLE,
            DataType::Box => Type::BOX,
            DataType::Tid => Type::TID,
            DataType::Oid => Type::OID,
            DataType::PgLsn => Type::PG_LSN,
            DataType::MacAddr => Type::MACADDR,
            DataType::MacAddr8 => Type::MACADDR8,
            DataType::Path => Type::PATH,
            DataType::Json => Type::JSON,
            DataType::Jsonb => Type::JSONB,
            DataType::Bool => Type::BOOL,
            DataType::Date => Type::DATE,
            DataType::Time(_) => Type::TIME,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LsegValue {
    start: PointValue,
    end: PointValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LineValue {
    a_bits: u64,
    b_bits: u64,
    c_bits: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CircleValue {
    center: PointValue,
    radius_bits: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BoxValue {
    high: PointValue,
    low: PointValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TidValue {
    block: u32,
    offset: u16,
}

impl TidValue {
    pub fn new(block: u32, offset: u16) -> Self {
        Self { block, offset }
    }

    pub fn block(self) -> u32 {
        self.block
    }

    pub fn offset(self) -> u16 {
        self.offset
    }
}

impl BoxValue {
    pub fn new(first: PointValue, second: PointValue) -> Self {
        Self {
            high: PointValue::new(first.x().max(second.x()), first.y().max(second.y())),
            low: PointValue::new(first.x().min(second.x()), first.y().min(second.y())),
        }
    }

    pub fn high(self) -> PointValue {
        self.high
    }

    pub fn low(self) -> PointValue {
        self.low
    }
}

impl CircleValue {
    pub fn new(center: PointValue, radius: f64) -> Self {
        Self {
            center,
            radius_bits: radius.to_bits(),
        }
    }

    pub fn center(self) -> PointValue {
        self.center
    }

    pub fn radius(self) -> f64 {
        f64::from_bits(self.radius_bits)
    }
}

impl LineValue {
    pub fn new(a: f64, b: f64, c: f64) -> Self {
        Self {
            a_bits: a.to_bits(),
            b_bits: b.to_bits(),
            c_bits: c.to_bits(),
        }
    }

    pub fn a(self) -> f64 {
        f64::from_bits(self.a_bits)
    }

    pub fn b(self) -> f64 {
        f64::from_bits(self.b_bits)
    }

    pub fn c(self) -> f64 {
        f64::from_bits(self.c_bits)
    }
}

impl LsegValue {
    pub fn new(start: PointValue, end: PointValue) -> Self {
        Self { start, end }
    }

    pub fn start(self) -> PointValue {
        self.start
    }

    pub fn end(self) -> PointValue {
        self.end
    }
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
    PgChar(u8),
    Point(PointValue),
    Lseg(LsegValue),
    Line(LineValue),
    Circle(CircleValue),
    Box(BoxValue),
    Tid(TidValue),
    Oid(u32),
    PgLsn(u64),
    MacAddr([u8; 6]),
    MacAddr8([u8; 8]),
    Path(PathValue),
    Bool(bool),
    Date(i32),
    TimeMicros(u64),
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
            (PgChar(a), PgChar(b)) => a == b,
            (Point(a), Point(b)) => a == b,
            (Lseg(a), Lseg(b)) => a == b,
            (Line(a), Line(b)) => a == b,
            (Circle(a), Circle(b)) => a == b,
            (Box(a), Box(b)) => a == b,
            (Tid(a), Tid(b)) => a == b,
            (Oid(a), Oid(b)) => a == b,
            (PgLsn(a), PgLsn(b)) => a == b,
            (MacAddr(a), MacAddr(b)) => a == b,
            (MacAddr8(a), MacAddr8(b)) => a == b,
            (Path(a), Path(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (TimeMicros(a), TimeMicros(b)) => a == b,
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
            PgChar(value) => value.hash(state),
            Point(point) => point.hash(state),
            Lseg(lseg) => lseg.hash(state),
            Line(line) => line.hash(state),
            Circle(circle) => circle.hash(state),
            Box(value) => value.hash(state),
            Tid(tid) => tid.hash(state),
            Oid(value) => value.hash(state),
            PgLsn(value) => value.hash(state),
            MacAddr(value) => value.hash(state),
            MacAddr8(value) => value.hash(state),
            Path(path) => path.hash(state),
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            TimeMicros(value) => value.hash(state),
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

#[cfg(test)]
mod geometric_tests {
    use super::*;

    #[test]
    fn parses_and_formats_lseg_variants() {
        for (input, expected) in [
            ("[(1,2),(3,4)]", "[(1,2),(3,4)]"),
            ("(0,0),(6,6)", "[(0,0),(6,6)]"),
            ("10,-10,-3,-4", "[(10,-10),(-3,-4)]"),
        ] {
            assert_eq!(format_lseg_text(parse_lseg_text(input).unwrap()), expected);
        }
        assert!(parse_lseg_text("[(1,2),(3,4)").is_err());
    }

    #[test]
    fn normalizes_lines_constructed_from_points() {
        let diagonal =
            line_from_points(PointValue::new(0.0, 0.0), PointValue::new(6.0, 6.0)).unwrap();
        assert_eq!(format_line_text(diagonal), "{1,-1,0}");
        let vertical =
            line_from_points(PointValue::new(3.0, 1.0), PointValue::new(3.0, 2.0)).unwrap();
        assert_eq!(format_line_text(vertical), "{-1,0,3}");
        assert!(line_from_points(PointValue::new(1.0, 1.0), PointValue::new(1.0, 1.0)).is_err());
    }

    #[test]
    fn parses_and_formats_circle_variants() {
        for input in ["<(5,1),3>", "((5,1),3)", "(5,1),3", "5,1,3"] {
            assert_eq!(
                format_circle_text(parse_circle_text(input).unwrap()),
                "<(5,1),3>"
            );
        }
        assert!(parse_circle_text("<(5,1),-3>").is_err());
        assert!(parse_circle_text("<(5,1),3").is_err());
    }
}
