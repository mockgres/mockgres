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

pub fn cast_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    convert_value_to_type(val, target, tz, false)
}

pub fn parse_pg_lsn_text(input: &str) -> Result<u64, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type pg_lsn: \"{input}\""),
        )
    }
    if input.trim() != input {
        return Err(invalid(input));
    }
    let (high, low) = input.split_once('/').ok_or_else(|| invalid(input))?;
    if high.is_empty() || low.is_empty() || high.contains('/') || low.contains('/') {
        return Err(invalid(input));
    }
    let high = u32::from_str_radix(high, 16).map_err(|_| invalid(input))?;
    let low = u32::from_str_radix(low, 16).map_err(|_| invalid(input))?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

pub fn parse_oid_text(input: &str) -> Result<u32, SqlError> {
    let trimmed = input.trim();
    let parsed = trimmed.parse::<i128>().map_err(|_| {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type oid: \"{input}\""),
        )
    })?;
    if parsed < i32::MIN as i128 || parsed > u32::MAX as i128 {
        return Err(SqlError::new(
            "22003",
            format!("value \"{input}\" is out of range for type oid"),
        ));
    }
    Ok(parsed as u32)
}

pub fn format_pg_lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & u64::from(u32::MAX))
}

pub fn parse_macaddr_text(input: &str) -> Result<[u8; 6], SqlError> {
    let trimmed = input.trim();
    let compact = if trimmed.len() == 12 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        trimmed.to_string()
    } else {
        let separator = if trimmed.contains(':') {
            ':'
        } else if trimmed.contains('-') {
            '-'
        } else if trimmed.contains('.') {
            '.'
        } else {
            '\0'
        };
        let groups = trimmed.split(separator).collect::<Vec<_>>();
        let valid = (matches!(separator, ':' | '-')
            && ((groups.len() == 6 && groups.iter().all(|group| group.len() == 2))
                || (groups.len() == 2 && groups.iter().all(|group| group.len() == 6))))
            || (matches!(separator, '.' | '-')
                && groups.len() == 3
                && groups.iter().all(|group| group.len() == 4));
        if !valid {
            return Err(invalid_macaddr(input, "macaddr"));
        }
        groups.concat()
    };
    parse_mac_bytes::<6>(&compact).map_err(|_| invalid_macaddr(input, "macaddr"))
}

pub fn parse_macaddr8_text(input: &str) -> Result<[u8; 8], SqlError> {
    let trimmed = input.trim();
    if let Ok(mac) = parse_macaddr_text(trimmed) {
        return Ok([mac[0], mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]]);
    }
    {
        let compact = trimmed.replace([':', '-', '.'], "");
        if compact.len() == 12
            && compact.bytes().all(|byte| byte.is_ascii_hexdigit())
            && ((trimmed.matches(':').count() == 2
                && trimmed.split(':').all(|group| group.len() == 4))
                || (trimmed.matches('-').count() == 2
                    && trimmed.split('-').all(|group| group.len() == 4)))
        {
            let mac =
                parse_mac_bytes::<6>(&compact).map_err(|_| invalid_macaddr(input, "macaddr8"))?;
            return Ok([mac[0], mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]]);
        }
    }
    let compact = trimmed.replace([':', '-', '.'], "");
    let shape_valid = (trimmed.len() == 23
        && (trimmed.matches(':').count() == 7 || trimmed.matches('-').count() == 7))
        || (trimmed.len() == 19 && trimmed.matches('.').count() == 3)
        || (trimmed.len() == 17 && trimmed.matches(':').count() == 1)
        || (trimmed.len() == 17 && trimmed.matches('-').count() == 1)
        || (trimmed.len() == 16 && !trimmed.contains([':', '-', '.']));
    if !shape_valid || compact.len() != 16 {
        return Err(invalid_macaddr(input, "macaddr8"));
    }
    parse_mac_bytes::<8>(&compact).map_err(|_| invalid_macaddr(input, "macaddr8"))
}

fn parse_mac_bytes<const N: usize>(input: &str) -> Result<[u8; N], ()> {
    if input.len() != N * 2 {
        return Err(());
    }
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&input[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    Ok(bytes)
}

fn invalid_macaddr(input: &str, type_name: &str) -> SqlError {
    SqlError::new(
        "22P02",
        format!("invalid input syntax for type {type_name}: \"{input}\""),
    )
}

pub fn format_macaddr<const N: usize>(value: &[u8; N]) -> String {
    value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

pub fn parse_time_text(input: &str, precision: Option<usize>) -> Result<u64, SqlError> {
    let tokens = input.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(invalid_time(input, "22007"));
    }
    let has_date = tokens.first().is_some_and(|token| token.contains('-'));
    let time_index = usize::from(has_date);
    let Some(time) = tokens.get(time_index) else {
        return Err(invalid_time(input, "22007"));
    };
    if tokens
        .iter()
        .skip(time_index + 1)
        .any(|token| token.contains('/') && !has_date)
    {
        return Err(invalid_time(input, "22007"));
    }
    let meridiem = tokens
        .iter()
        .skip(time_index + 1)
        .find(|token| token.eq_ignore_ascii_case("am") || token.eq_ignore_ascii_case("pm"));
    let parts = time.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return Err(invalid_time(input, "22007"));
    }
    let mut hour = parts[0]
        .parse::<u64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    let minute = parts[1]
        .parse::<u64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    let seconds = parts
        .get(2)
        .copied()
        .unwrap_or("0")
        .parse::<f64>()
        .map_err(|_| invalid_time(input, "22007"))?;
    if let Some(meridiem) = meridiem {
        if !(1..=12).contains(&hour) {
            return Err(range_time(input));
        }
        if meridiem.eq_ignore_ascii_case("pm") {
            if hour != 12 {
                hour += 12;
            }
        } else if hour == 12 {
            hour = 0;
        }
    }
    if minute >= 60 || !(0.0..60.01).contains(&seconds) || hour > 24 {
        return Err(range_time(input));
    }
    let mut micros =
        ((hour * 3600 + minute * 60) as f64 * 1_000_000.0 + seconds * 1_000_000.0).round() as u64;
    if let Some(precision) = precision {
        let precision = precision.min(6);
        let quantum = 10_u64.pow((6 - precision) as u32);
        micros = ((micros + quantum / 2) / quantum) * quantum;
    }
    const DAY: u64 = 86_400_000_000;
    if micros > DAY || (hour == 24 && (minute != 0 || seconds != 0.0)) {
        return Err(range_time(input));
    }
    Ok(micros)
}

fn invalid_time(input: &str, code: &'static str) -> SqlError {
    SqlError::new(
        code,
        format!("invalid input syntax for type time: \"{input}\""),
    )
}

fn range_time(input: &str) -> SqlError {
    SqlError::new(
        "22008",
        format!("date/time field value out of range: \"{input}\""),
    )
}

pub fn format_time(value: u64) -> String {
    if value == 86_400_000_000 {
        return "24:00:00".to_string();
    }
    let hours = value / 3_600_000_000;
    let minutes = (value / 60_000_000) % 60;
    let seconds = (value / 1_000_000) % 60;
    let micros = value % 1_000_000;
    if micros == 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        let fraction = format!("{micros:06}").trim_end_matches('0').to_string();
        format!("{hours:02}:{minutes:02}:{seconds:02}.{fraction}")
    }
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

pub fn parse_lseg_text(input: &str) -> Result<LsegValue, SqlError> {
    let path = parse_path_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type path", "type lseg", 1),
        )
    })?;
    let [start, end] = path.points() else {
        return Err(SqlError::new(
            "22P02",
            format!("invalid input syntax for type lseg: \"{input}\""),
        ));
    };
    Ok(LsegValue::new(*start, *end))
}

pub fn format_lseg_text(lseg: LsegValue) -> String {
    format!(
        "[{},{}]",
        format_point_text(lseg.start()),
        format_point_text(lseg.end())
    )
}

pub fn line_from_points(start: PointValue, end: PointValue) -> Result<LineValue, SqlError> {
    if start == end {
        return Err(SqlError::new(
            "22P02",
            "invalid line specification: must be two distinct points",
        ));
    }
    let dx = end.x() - start.x();
    let (mut a, b, mut c) = if dx != 0.0 {
        let a = (end.y() - start.y()) / dx;
        (a, -1.0, start.y() - a * start.x())
    } else {
        (-1.0, 0.0, start.x())
    };
    if a == -0.0 {
        a = 0.0;
    }
    if c == -0.0 {
        c = 0.0;
    }
    Ok(LineValue::new(a, b, c))
}

pub fn parse_line_text(input: &str) -> Result<LineValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type line: \"{input}\""),
        )
    }

    let trimmed = input.trim();
    if let Some(inner) = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    {
        let values = inner.split(',').collect::<Vec<_>>();
        if values.len() != 3 || values.iter().any(|value| value.trim().is_empty()) {
            return Err(invalid(input));
        }
        let a = parse_geometry_coordinate(values[0], input, "line")?;
        let b = parse_geometry_coordinate(values[1], input, "line")?;
        let c = parse_geometry_coordinate(values[2], input, "line")?;
        if a == 0.0 && b == 0.0 {
            return Err(SqlError::new(
                "22P02",
                "invalid line specification: A and B cannot both be zero",
            ));
        }
        return Ok(LineValue::new(a, b, c));
    }
    if trimmed.starts_with('{') || trimmed.ends_with('}') {
        return Err(invalid(input));
    }
    let lseg = parse_lseg_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type lseg", "type line", 1),
        )
    })?;
    line_from_points(lseg.start(), lseg.end())
}

pub fn format_line_text(line: LineValue) -> String {
    format!(
        "{{{},{},{}}}",
        format_geometry_coordinate(line.a()),
        format_geometry_coordinate(line.b()),
        format_geometry_coordinate(line.c())
    )
}

pub fn parse_circle_text(input: &str) -> Result<CircleValue, SqlError> {
    fn invalid(input: &str) -> SqlError {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type circle: \"{input}\""),
        )
    }

    let compact = input
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    let bracketless = if let Some(inner) = compact
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
    {
        inner
    } else if compact.starts_with("((") && compact.ends_with(')') {
        &compact[1..compact.len() - 1]
    } else {
        compact.as_str()
    };
    let (point, radius) = if let Some(inner) = bracketless.strip_prefix('(') {
        inner.split_once("),").ok_or_else(|| invalid(input))?
    } else if bracketless.contains(['(', ')', '<', '>']) {
        return Err(invalid(input));
    } else {
        let mut parts = bracketless.split(',');
        let x = parts.next().ok_or_else(|| invalid(input))?;
        let y = parts.next().ok_or_else(|| invalid(input))?;
        let radius = parts.next().ok_or_else(|| invalid(input))?;
        if parts.next().is_some() {
            return Err(invalid(input));
        }
        return parse_circle_components(&format!("{x},{y}"), radius, input);
    };
    parse_circle_components(point, radius, input)
}

fn parse_circle_components(
    point: &str,
    radius: &str,
    input: &str,
) -> Result<CircleValue, SqlError> {
    let invalid = || {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type circle: \"{input}\""),
        )
    };
    let center = parse_point_text(point).map_err(|_| invalid())?;
    let radius = parse_geometry_coordinate(radius, input, "circle")?;
    if radius < 0.0 {
        return Err(invalid());
    }
    Ok(CircleValue::new(center, radius))
}

pub fn format_circle_text(circle: CircleValue) -> String {
    format!(
        "<{},{}>",
        format_point_text(circle.center()),
        format_geometry_coordinate(circle.radius())
    )
}

pub fn parse_box_text(input: &str) -> Result<BoxValue, SqlError> {
    let lseg = parse_lseg_text(input).map_err(|error| {
        SqlError::new(
            error.code,
            error.message.replacen("type lseg", "type box", 1),
        )
    })?;
    Ok(BoxValue::new(lseg.start(), lseg.end()))
}

pub fn format_box_text(value: BoxValue) -> String {
    format!(
        "{},{}",
        format_point_text(value.high()),
        format_point_text(value.low())
    )
}

pub fn parse_tid_text(input: &str) -> Result<TidValue, SqlError> {
    let invalid = || {
        SqlError::new(
            "22P02",
            format!("invalid input syntax for type tid: \"{input}\""),
        )
    };
    let inner = input
        .trim()
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(invalid)?;
    let mut parts = inner.split(',');
    let block = parts
        .next()
        .ok_or_else(invalid)?
        .trim()
        .parse::<i64>()
        .map_err(|_| invalid())?;
    let offset = parts
        .next()
        .ok_or_else(invalid)?
        .trim()
        .parse::<i64>()
        .map_err(|_| invalid())?;
    if parts.next().is_some()
        || !(-1..=u32::MAX as i64).contains(&block)
        || !(0..=u16::MAX as i64).contains(&offset)
    {
        return Err(invalid());
    }
    Ok(TidValue::new(block as u32, offset as u16))
}

pub fn format_tid_text(tid: TidValue) -> String {
    format!("({},{})", tid.block(), tid.offset())
}

pub fn validate_varchar_input(input: &str, type_spec: &str) -> Result<(), SqlError> {
    let length = type_spec
        .strip_prefix("varchar(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| SqlError::new("42704", format!("type \"{type_spec}\" does not exist")))?;
    coerce_value_to_type(
        Value::Text(input.to_string()),
        &DataType::Varchar(Some(length)),
        &SessionTimeZone::Utc,
    )
    .map(|_| ())
}

pub fn validate_char_input(input: &str, type_spec: &str) -> Result<(), SqlError> {
    let length = type_spec
        .strip_prefix("char(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| SqlError::new("42704", format!("type \"{type_spec}\" does not exist")))?;
    coerce_value_to_type(
        Value::Text(input.to_string()),
        &DataType::BpChar(Some(length)),
        &SessionTimeZone::Utc,
    )
    .map(|_| ())
}

fn parse_pg_char(input: &str) -> Value {
    let byte = input
        .strip_prefix('\\')
        .filter(|digits| {
            digits.len() == 3 && digits.bytes().all(|byte| matches!(byte, b'0'..=b'7'))
        })
        .and_then(|digits| u8::from_str_radix(digits, 8).ok())
        .unwrap_or_else(|| input.as_bytes().first().copied().unwrap_or(0));
    Value::PgChar(byte)
}

pub fn format_pg_char(byte: u8) -> String {
    match byte {
        0 => String::new(),
        0x20..=0x7e => (byte as char).to_string(),
        _ => format!("\\{byte:03o}"),
    }
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

    fn coerce_varchar(
        input: String,
        length: Option<usize>,
        assignment: bool,
    ) -> Result<Value, SqlError> {
        let Some(length) = length else {
            return Ok(Value::Text(input));
        };
        let mut chars = input.chars();
        let output: String = chars.by_ref().take(length).collect();
        if assignment && chars.any(|character| character != ' ') {
            return Err(SqlError::new(
                "22001",
                format!("value too long for type character varying({length})"),
            ));
        }
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
        (DataType::Text, Value::PgChar(value)) => Ok(Value::Text(format_pg_char(value))),
        (DataType::Varchar(length), Value::Text(s)) => coerce_varchar(s, *length, assignment),
        (DataType::Varchar(length), Value::Int64(value)) => {
            coerce_varchar(value.to_string(), *length, assignment)
        }
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
        (DataType::BpChar(length), Value::Int64(value)) => {
            coerce_bpchar(value.to_string(), *length, assignment)
        }
        (DataType::PgChar, Value::PgChar(value)) => Ok(Value::PgChar(value)),
        (DataType::PgChar, Value::Text(value)) => Ok(parse_pg_char(&value)),
        (DataType::Point, Value::Point(point)) => Ok(Value::Point(point)),
        (DataType::Point, Value::Text(value)) => parse_point_text(&value).map(Value::Point),
        (DataType::Text, Value::Point(point)) => Ok(Value::Text(format_point_text(point))),
        (DataType::Lseg, Value::Lseg(lseg)) => Ok(Value::Lseg(lseg)),
        (DataType::Lseg, Value::Text(value)) => parse_lseg_text(&value).map(Value::Lseg),
        (DataType::Text, Value::Lseg(lseg)) => Ok(Value::Text(format_lseg_text(lseg))),
        (DataType::Line, Value::Line(line)) => Ok(Value::Line(line)),
        (DataType::Line, Value::Text(value)) => parse_line_text(&value).map(Value::Line),
        (DataType::Text, Value::Line(line)) => Ok(Value::Text(format_line_text(line))),
        (DataType::Circle, Value::Circle(circle)) => Ok(Value::Circle(circle)),
        (DataType::Circle, Value::Text(value)) => parse_circle_text(&value).map(Value::Circle),
        (DataType::Text, Value::Circle(circle)) => Ok(Value::Text(format_circle_text(circle))),
        (DataType::Box, Value::Box(value)) => Ok(Value::Box(value)),
        (DataType::Box, Value::Text(value)) => parse_box_text(&value).map(Value::Box),
        (DataType::Text, Value::Box(value)) => Ok(Value::Text(format_box_text(value))),
        (DataType::Tid, Value::Tid(tid)) => Ok(Value::Tid(tid)),
        (DataType::Tid, Value::Text(value)) => parse_tid_text(&value).map(Value::Tid),
        (DataType::Text, Value::Tid(tid)) => Ok(Value::Text(format_tid_text(tid))),
        (DataType::Oid, Value::Oid(value)) => Ok(Value::Oid(value)),
        (DataType::Oid, Value::Int64(value)) if value >= i32::MIN as i64 => {
            Ok(Value::Oid(value as u32))
        }
        (DataType::Oid, Value::Text(value)) => parse_oid_text(&value).map(Value::Oid),
        (DataType::Text, Value::Oid(value)) => Ok(Value::Text(value.to_string())),
        (DataType::PgLsn, Value::PgLsn(value)) => Ok(Value::PgLsn(value)),
        (DataType::PgLsn, Value::Text(value)) => parse_pg_lsn_text(&value).map(Value::PgLsn),
        (DataType::Text, Value::PgLsn(value)) => Ok(Value::Text(format_pg_lsn(value))),
        (DataType::MacAddr, Value::MacAddr(value)) => Ok(Value::MacAddr(value)),
        (DataType::MacAddr, Value::Text(value)) => parse_macaddr_text(&value).map(Value::MacAddr),
        (DataType::MacAddr, Value::MacAddr8(value)) if value[3..5] == [0xff, 0xfe] => {
            Ok(Value::MacAddr([
                value[0], value[1], value[2], value[5], value[6], value[7],
            ]))
        }
        (DataType::Text, Value::MacAddr(value)) => Ok(Value::Text(format_macaddr(&value))),
        (DataType::MacAddr8, Value::MacAddr8(value)) => Ok(Value::MacAddr8(value)),
        (DataType::MacAddr8, Value::MacAddr(value)) => Ok(Value::MacAddr8([
            value[0], value[1], value[2], 0xff, 0xfe, value[3], value[4], value[5],
        ])),
        (DataType::MacAddr8, Value::Text(value)) => {
            parse_macaddr8_text(&value).map(Value::MacAddr8)
        }
        (DataType::Text, Value::MacAddr8(value)) => Ok(Value::Text(format_macaddr(&value))),
        (DataType::Time(precision), Value::TimeMicros(value)) => {
            if let Some(precision) = precision {
                let quantum = 10_u64.pow((6 - (*precision).min(6)) as u32);
                Ok(Value::TimeMicros(
                    ((value + quantum / 2) / quantum) * quantum,
                ))
            } else {
                Ok(Value::TimeMicros(value))
            }
        }
        (DataType::Time(precision), Value::Text(value)) => {
            parse_time_text(&value, *precision).map(Value::TimeMicros)
        }
        (DataType::Text, Value::TimeMicros(value)) => Ok(Value::Text(format_time(value))),
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
