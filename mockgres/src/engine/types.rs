use super::expr_plan::ScalarExpr;
use crate::session::SessionTimeZone;
use crate::types::{
    date_to_timestamptz, format_timestamp, format_timestamptz, parse_bytea_text, parse_date_str,
    parse_timestamp_str, parse_timestamptz_str, timestamp_micros_to_date_days,
    timestamp_to_timestamptz, timestamptz_to_date_days, timestamptz_to_timestamp,
};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError};
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
    Int4,
    Int8,
    Float8,
    Text,
    Bool,
    Date,
    Timestamp,
    Timestamptz,
    Bytea,
}

impl DataType {
    pub fn to_pg(&self) -> Type {
        match self {
            DataType::Int4 => Type::INT4,
            DataType::Int8 => Type::INT8,
            DataType::Float8 => Type::FLOAT8,
            DataType::Text => Type::TEXT,
            DataType::Bool => Type::BOOL,
            DataType::Date => Type::DATE,
            DataType::Timestamp => Type::TIMESTAMP,
            DataType::Timestamptz => Type::TIMESTAMPTZ,
            DataType::Bytea => Type::BYTEA,
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
    Bool(bool),
    Date(i32),
    TimestampMicros(i64),
    TimestamptzMicros(i64),
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Int64(a), Int64(b)) => a == b,
            (Float64Bits(a), Float64Bits(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (TimestampMicros(a), TimestampMicros(b)) => a == b,
            (TimestamptzMicros(a), TimestamptzMicros(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
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
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            TimestampMicros(t) => t.hash(state),
            TimestamptzMicros(t) => t.hash(state),
            Bytes(b) => b.hash(state),
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
}

pub fn cast_value_to_type(
    val: Value,
    target: &DataType,
    tz: &SessionTimeZone,
) -> Result<Value, SqlError> {
    match (target, val) {
        (DataType::Int4, Value::Int64(v)) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(SqlError::new("22003", "value out of range for int4"));
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int4, Value::Text(s)) => {
            let parsed: i32 = s
                .parse()
                .map_err(|e| SqlError::new("22P02", format!("invalid input for int4: {e}")))?;
            Ok(Value::Int64(parsed as i64))
        }
        (DataType::Int8, Value::Int64(v)) => Ok(Value::Int64(v)),
        (DataType::Int8, Value::Text(s)) => {
            let parsed: i64 = s
                .parse()
                .map_err(|e| SqlError::new("22P02", format!("invalid input for int8: {e}")))?;
            Ok(Value::Int64(parsed))
        }
        (DataType::Float8, Value::Float64Bits(bits)) => Ok(Value::Float64Bits(bits)),
        (DataType::Float8, Value::Int64(v)) => Ok(Value::from_f64(v as f64)),
        (DataType::Float8, Value::Text(s)) => {
            let parsed: f64 = s
                .parse()
                .map_err(|e| SqlError::new("22P02", format!("invalid input for float8: {e}")))?;
            Ok(Value::from_f64(parsed))
        }
        (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
        (DataType::Text, Value::Bool(b)) => Ok(Value::Text(if b { "t" } else { "f" }.into())),
        (DataType::Text, Value::Int64(i)) => Ok(Value::Text(i.to_string())),
        (DataType::Text, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            Ok(Value::Text(f.to_string()))
        }
        (DataType::Text, Value::TimestampMicros(m)) => {
            let text = format_timestamp(m).map_err(|e| SqlError::new("22008", e))?;
            Ok(Value::Text(text))
        }
        (DataType::Text, Value::TimestamptzMicros(m)) => {
            let text = format_timestamptz(m, tz).map_err(|e| SqlError::new("22008", e))?;
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
