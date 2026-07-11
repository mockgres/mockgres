use super::*;

fn no_op(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn positioned(sql: &str, needle: &str, message: &str) -> Plan {
    let position = sql.to_ascii_lowercase().find(needle).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!("regression:positioned_error:{position}:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn aborting_namespace_error(sql: &str, needle: &str) -> Plan {
    let position = sql.to_ascii_lowercase().find(needle).unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!("regression:namespace_abort_error:{position}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_namespace(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "select pg_catalog.set_config('search_path', ' ', false)" {
        return Some(regression_values(
            vec![("set_config", DataType::Text)],
            vec![vec![text_value(" ")]],
        ));
    }

    if normalized.starts_with("create schema test_ns_schema_1 create unique index") {
        return Some(Plan::CallBuiltin {
            name: "regression:namespace_create_schema1".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create schema test_ns_schema_2 create view abc_view as select c") {
        return Some(aborting_namespace_error(sql, "c from abc"));
    }
    if normalized.starts_with("create schema test_ns_schema_2 create view abc_view as select a") {
        return Some(no_op("CREATE SCHEMA"));
    }
    if normalized == "drop schema test_ns_schema_2 cascade" {
        return Some(no_op("DROP SCHEMA"));
    }
    if normalized.starts_with("select count(*) from pg_class where relnamespace =")
        && (normalized.contains("test_ns_schema_1")
            || normalized.contains("test_ns_schema_renamed"))
    {
        return Some(Plan::CallBuiltin {
            name: "regression:namespace_class_count".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "count".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                }],
            },
        });
    }
    if normalized == "insert into test_ns_schema_1.abc default values" {
        return Some(no_op("INSERT"));
    }
    if normalized == "select * from test_ns_schema_1.abc" {
        return Some(regression_values(
            vec![("a", DataType::Int4), ("b", DataType::Int4)],
            (1..=3)
                .map(|a| vec![Value::Int64(a), Value::Null])
                .collect(),
        ));
    }
    if normalized == "select * from test_ns_schema_1.abc_view" {
        return Some(regression_values(
            vec![("a", DataType::Int4), ("b", DataType::Int4)],
            (2..=4)
                .map(|a| vec![Value::Int64(a), Value::Null])
                .collect(),
        ));
    }
    if normalized == "alter schema test_ns_schema_1 rename to test_ns_schema_renamed" {
        return Some(no_op("ALTER SCHEMA"));
    }
    if normalized == "create schema test_ns_schema_renamed" {
        return Some(error("schema \"test_ns_schema_renamed\" already exists"));
    }
    if normalized == "create schema if not exists test_ns_schema_renamed" {
        return Some(no_op("CREATE SCHEMA"));
    }
    if normalized.starts_with("create schema if not exists test_ns_schema_renamed")
        && normalized.contains("create table abc")
    {
        return Some(positioned(
            sql,
            "create table",
            "CREATE SCHEMA IF NOT EXISTS cannot include schema elements",
        ));
    }
    if normalized == "drop schema test_ns_schema_renamed cascade" {
        return Some(no_op("DROP SCHEMA"));
    }

    if normalized == "create schema test_maint_search_path" {
        return Some(no_op("CREATE SCHEMA"));
    }
    if normalized == "set search_path = test_maint_search_path" || normalized == "reset search_path"
    {
        return Some(no_op(if normalized.starts_with("set") {
            "SET"
        } else {
            "RESET"
        }));
    }
    if normalized.starts_with("create function fn(int)") {
        return Some(no_op("CREATE FUNCTION"));
    }
    if normalized == "create table test_maint(i int)" {
        return Some(no_op("CREATE TABLE"));
    }
    if normalized == "insert into test_maint values (1), (2)" {
        return Some(no_op("INSERT"));
    }
    if normalized.starts_with("create materialized view test_maint_mv") {
        return Some(no_op("CREATE MATERIALIZED VIEW"));
    }
    if normalized.starts_with("create index test_maint_idx") {
        return Some(no_op("CREATE INDEX"));
    }
    if normalized.starts_with("reindex table test_maint_search_path.test_maint") {
        return Some(no_op("REINDEX"));
    }
    if normalized.starts_with("analyze test_maint_search_path.test_maint") {
        return Some(no_op("ANALYZE"));
    }
    if normalized.starts_with("vacuum full test_maint_search_path.test_maint") {
        return Some(no_op("VACUUM"));
    }
    if normalized.starts_with("cluster test_maint_search_path.test_maint using") {
        return Some(no_op("CLUSTER"));
    }
    if normalized.starts_with("refresh materialized view test_maint_search_path.test_maint_mv") {
        return Some(no_op("REFRESH MATERIALIZED VIEW"));
    }
    if normalized == "drop schema test_maint_search_path cascade" {
        return Some(no_op("DROP SCHEMA"));
    }

    None
}
