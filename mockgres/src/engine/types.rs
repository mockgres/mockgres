use pgwire::api::Type;
use pgwire::error::PgWireError;
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
    PgWireError::ApiError(Box::new(SqlError::new("XX000", msg.into())))
}

pub fn fe_code(code: &'static str, msg: impl Into<String>) -> PgWireError {
    PgWireError::ApiError(Box::new(SqlError::new(code, msg.into())))
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
            DataType::Bytea => Type::BYTEA,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
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
