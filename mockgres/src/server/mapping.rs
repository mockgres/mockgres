use pgwire::api::Type;

use crate::compat::{
    POSTGRES_COMPAT_VERSION, POSTGRES_COMPAT_VERSION_NUM,
    server_version_string as compat_server_version_string,
};
use crate::engine::DataType;

pub fn map_pg_type_to_datatype(t: &Type) -> Option<DataType> {
    match *t {
        Type::INT2 => Some(DataType::Int2),
        Type::INT4 => Some(DataType::Int4),
        Type::INT8 => Some(DataType::Int8),
        Type::FLOAT8 => Some(DataType::Float8),
        Type::TEXT | Type::VARCHAR => Some(DataType::Text),
        Type::BPCHAR => Some(DataType::BpChar(None)),
        Type::JSON => Some(DataType::Json),
        Type::JSONB => Some(DataType::Jsonb),
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
        DataType::Int2 => Type::INT2,
        DataType::Int4 => Type::INT4,
        DataType::Int8 => Type::INT8,
        DataType::Float8 => Type::FLOAT8,
        DataType::Text => Type::TEXT,
        DataType::BpChar(_) => Type::BPCHAR,
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

pub fn lookup_show_value(name: &str) -> Option<String> {
    match name {
        "server_version" => Some(POSTGRES_COMPAT_VERSION.to_string()),
        "server_version_num" => Some(POSTGRES_COMPAT_VERSION_NUM.to_string()),
        "standard_conforming_strings" => Some("on".to_string()),
        _ => None,
    }
}

pub fn server_version_string() -> String {
    compat_server_version_string()
}
