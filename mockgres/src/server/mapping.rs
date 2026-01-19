use pgwire::api::Type;

use crate::engine::DataType;

pub fn map_pg_type_to_datatype(t: &Type) -> Option<DataType> {
    match *t {
        Type::INT4 => Some(DataType::Int4),
        Type::INT8 => Some(DataType::Int8),
        Type::FLOAT8 => Some(DataType::Float8),
        Type::TEXT | Type::VARCHAR => Some(DataType::Text),
        Type::JSON => Some(DataType::Json),
        Type::BOOL => Some(DataType::Bool),
        Type::DATE => Some(DataType::Date),
        Type::TIMESTAMP => Some(DataType::Timestamp),
        Type::TIMESTAMPTZ => Some(DataType::Timestamptz),
        Type::BYTEA => Some(DataType::Bytea),
        Type::INTERVAL => Some(DataType::Interval),
        Type::VOID => Some(DataType::Void),
        _ => None,
    }
}

pub fn map_datatype_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int4 => Type::INT4,
        DataType::Int8 => Type::INT8,
        DataType::Float8 => Type::FLOAT8,
        DataType::Text => Type::TEXT,
        DataType::Json => Type::JSON,
        DataType::Bool => Type::BOOL,
        DataType::Date => Type::DATE,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::Timestamptz => Type::TIMESTAMPTZ,
        DataType::Bytea => Type::BYTEA,
        DataType::Interval => Type::INTERVAL,
        DataType::Void => Type::VOID,
    }
}

pub fn lookup_show_value(name: &str) -> Option<String> {
    match name {
        "server_version" => Some("15.0".to_string()),
        "standard_conforming_strings" => Some("on".to_string()),
        _ => None,
    }
}

pub fn server_version_string() -> String {
    "PostgreSQL 15.0 on mockgres".to_string()
}
