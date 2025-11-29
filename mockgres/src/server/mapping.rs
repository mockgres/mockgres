use pgwire::api::Type;

use crate::engine::DataType;

pub fn map_pg_type_to_datatype(t: &Type) -> Option<DataType> {
    match *t {
        Type::INT4 => Some(DataType::Int4),
        Type::INT8 => Some(DataType::Int8),
        Type::FLOAT8 => Some(DataType::Float8),
        Type::TEXT | Type::VARCHAR => Some(DataType::Text),
        Type::BOOL => Some(DataType::Bool),
        Type::DATE => Some(DataType::Date),
        Type::TIMESTAMP => Some(DataType::Timestamp),
        Type::TIMESTAMPTZ => Some(DataType::Timestamptz),
        Type::BYTEA => Some(DataType::Bytea),
        Type::INTERVAL => Some(DataType::Interval),
        _ => None,
    }
}

pub fn map_datatype_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int4 => Type::INT4,
        DataType::Int8 => Type::INT8,
        DataType::Float8 => Type::FLOAT8,
        DataType::Text => Type::TEXT,
        DataType::Bool => Type::BOOL,
        DataType::Date => Type::DATE,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::Timestamptz => Type::TIMESTAMPTZ,
        DataType::Bytea => Type::BYTEA,
        DataType::Interval => Type::INTERVAL,
    }
}

pub fn lookup_show_value(name: &str) -> Option<String> {
    match name {
        "server_version" => Some("15.0".to_string()),
        _ => None,
    }
}
