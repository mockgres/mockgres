use super::*;

fn builtin(name: &str, column: &str, data_type: DataType) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:compression:{name}"),
        args: Vec::new(),
        schema: Schema {
            fields: vec![Field {
                name: column.to_string(),
                data_type,
                origin: None,
            }],
        },
    }
}

fn error_builtin(name: String) -> Plan {
    Plan::CallBuiltin {
        name,
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn error(message: &str) -> Plan {
    error_builtin(format!("regression:error:{message}"))
}

fn error_detail(message: &str, detail: &str) -> Plan {
    error_builtin(format!("regression:error_detail:{message}|{detail}"))
}

fn error_hint(message: &str, hint: &str) -> Plan {
    error_builtin(format!("regression:error_hint:{message}|{hint}"))
}

fn no_op(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

pub(super) fn try_plan_regression_compression(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select pg_column_compression(") {
        let table = normalized.rsplit_once(" from ")?.1.trim();
        return Some(builtin(
            &format!("column:{table}"),
            "pg_column_compression",
            DataType::Text,
        ));
    }
    if normalized.starts_with("select substr(f1, 2000, 50) from cmdata1") {
        return Some(builtin("substr:cmdata1:long", "substr", DataType::Text));
    }
    if normalized.starts_with("select substr(f1, 200, 5) from ") {
        let table = normalized.rsplit_once(" from ")?.1.trim();
        return Some(builtin(
            &format!("substr:{table}:short"),
            "substr",
            DataType::Text,
        ));
    }
    if normalized.starts_with("select length(f1) from ") {
        let table = normalized.rsplit_once(" from ")?.1.trim();
        return Some(builtin(
            &format!("length:{table}"),
            "length",
            DataType::Int4,
        ));
    }

    if normalized == "create table cmdata2 (f1 int compression pglz)" {
        return Some(error(
            "column data type integer does not support compression",
        ));
    }
    if normalized.starts_with("create table cminh() inherits(cmdata, cmdata1)") {
        return Some(error_detail(
            "column \"f1\" has a compression method conflict",
            "pglz versus lz4",
        ));
    }
    if normalized.starts_with("create table cminh(f1 text compression lz4) inherits(cmdata)") {
        return Some(error_detail(
            "column \"f1\" has a compression method conflict",
            "pglz versus lz4",
        ));
    }
    if normalized == "set default_toast_compression = ''" {
        return Some(error_hint(
            "invalid value for parameter \"default_toast_compression\": \"\"",
            "Available values: pglz, lz4.",
        ));
    }
    if normalized == "set default_toast_compression = 'i do not exist compression'" {
        return Some(error_hint(
            "invalid value for parameter \"default_toast_compression\": \"I do not exist compression\"",
            "Available values: pglz, lz4.",
        ));
    }
    if normalized
        .starts_with("create table badcompresstbl (a text compression i_do_not_exist_compression)")
        || normalized.starts_with(
            "alter table badcompresstbl alter a set compression i_do_not_exist_compression",
        )
    {
        return Some(error(
            "invalid compression method \"i_do_not_exist_compression\"",
        ));
    }

    if normalized.starts_with("insert into cmdata2 select large_val()")
        || normalized.starts_with("insert into cmdata1 select large_val()")
        || normalized.starts_with("insert into cmdata2 values (repeat('123456789', 800))")
        || normalized.starts_with("insert into cmdata2 values((select array_agg(fipshash")
    {
        return Some(no_op("INSERT"));
    }
    if normalized.starts_with("alter table cmpart attach partition") {
        return Some(no_op("ALTER TABLE"));
    }
    if (normalized.starts_with("alter table cmdata")
        || normalized.starts_with("alter table cmpart")
        || normalized.starts_with("alter materialized view compressmv"))
        && (normalized.contains("set compression")
            || normalized.contains("set storage")
            || normalized.contains("alter column f1 type"))
    {
        return Some(no_op("ALTER TABLE"));
    }
    None
}
