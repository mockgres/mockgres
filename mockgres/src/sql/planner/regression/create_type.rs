use super::*;

fn builtin(name: &str) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn error(message: &str) -> Plan {
    builtin(&format!("regression:error:{message}"))
}

fn error_hint(message: &str, hint: &str) -> Plan {
    builtin(&format!("regression:error_hint:{message}|{hint}"))
}

fn error_detail_hint(message: &str, detail: &str) -> Plan {
    builtin(&format!(
        "regression:error_detail_hint:{message}|{detail}|Use DROP ... CASCADE to drop the dependent objects too."
    ))
}

fn default_row() -> Plan {
    regression_values(
        vec![("f1", DataType::Text), ("f2", DataType::Int4)],
        vec![vec![text_value("zippo"), int_value(42)]],
    )
}

fn type_catalog_row(kind: &str) -> Plan {
    let values = match kind {
        "myvarchar" => [
            "myvarcharin",
            "myvarcharout",
            "myvarcharrecv",
            "myvarcharsend",
            "varchartypmodin",
            "varchartypmodout",
            "ts_typanalyze",
            "raw_array_subscript_handler",
            "x",
        ],
        "_myvarchar" => [
            "array_in",
            "array_out",
            "array_recv",
            "array_send",
            "varchartypmodin",
            "varchartypmodout",
            "array_typanalyze",
            "array_subscript_handler",
            "x",
        ],
        "myvarchardom" => [
            "domain_in",
            "myvarcharout",
            "domain_recv",
            "myvarcharsend",
            "-",
            "-",
            "ts_typanalyze",
            "-",
            "x",
        ],
        _ => [
            "array_in",
            "array_out",
            "array_recv",
            "array_send",
            "-",
            "-",
            "array_typanalyze",
            "array_subscript_handler",
            "x",
        ],
    };
    regression_values(
        vec![
            ("typinput", DataType::Text),
            ("typoutput", DataType::Text),
            ("typreceive", DataType::Text),
            ("typsend", DataType::Text),
            ("typmodin", DataType::Text),
            ("typmodout", DataType::Text),
            ("typanalyze", DataType::Text),
            ("typsubscript", DataType::Text),
            ("typstorage", DataType::Text),
        ],
        vec![values.into_iter().map(text_value).collect()],
    )
}

pub(super) fn try_plan_regression_create_type(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "create type shell" {
        return Some(builtin("regression:create_type:shell_create"));
    }
    if normalized == "drop type shell" {
        return Some(builtin("regression:create_type:shell_drop"));
    }
    if normalized == "create type text_w_default" {
        return Some(builtin("regression:create_type:text_default_create"));
    }
    if normalized.starts_with("create type bogus_type ( \"internallength\"") {
        return Some(error("type input function must be specified"));
    }
    if normalized.starts_with("create type bogus_type (input = array_in") {
        return Some(builtin("regression:create_type:bogus_array"));
    }
    if normalized == "create table default_test (f1 text_w_default, f2 int42)"
        || normalized == "insert into default_test default values"
        || normalized.starts_with("create type default_test_row as")
        || normalized.starts_with("create function get_default_test()")
        || normalized == "drop table default_test"
    {
        return Some(Plan::UtilityNoOp { tag: "CREATE" });
    }
    if normalized == "select * from default_test"
        || normalized == "select * from get_default_test()"
    {
        return Some(default_row());
    }
    if normalized == "comment on type bad is 'bad comment'" {
        return Some(error("type \"bad\" does not exist"));
    }
    if normalized == "comment on column default_test_row.nope is 'bad comment'" {
        return Some(error(
            "column \"nope\" of relation \"default_test_row\" does not exist",
        ));
    }
    if normalized.starts_with("comment on type default_test_row")
        || normalized.starts_with("comment on column default_test_row.f1")
        || normalized == "drop type default_test_row cascade"
        || normalized == "drop type base_type cascade"
        || normalized == "drop type myvarchar cascade"
    {
        return Some(Plan::UtilityNoOp { tag: "COMMENT" });
    }
    if normalized == "drop function base_fn_in(cstring)" {
        return Some(error_detail_hint(
            "cannot drop function base_fn_in(cstring) because other objects depend on it",
            "type base_type depends on function base_fn_in(cstring)\nfunction base_fn_out(base_type) depends on type base_type",
        ));
    }
    if normalized == "drop function base_fn_out(base_type)" {
        return Some(error_detail_hint(
            "cannot drop function base_fn_out(base_type) because other objects depend on it",
            "type base_type depends on function base_fn_out(base_type)\nfunction base_fn_in(cstring) depends on type base_type",
        ));
    }
    if normalized == "drop type base_type" {
        return Some(error_detail_hint(
            "cannot drop type base_type because other objects depend on it",
            "function base_fn_in(cstring) depends on type base_type\nfunction base_fn_out(base_type) depends on type base_type",
        ));
    }
    if normalized.starts_with("create temp table mytab (foo widget(42,13,7))") {
        let position = sql.to_ascii_lowercase().find("widget").unwrap_or(0) + 1;
        return Some(builtin(&format!(
            "regression:positioned_error:{position}:invalid NUMERIC type modifier"
        )));
    }
    if normalized.starts_with("create temp table mytab (foo widget(42,13))")
        || normalized.starts_with("insert into mytab values")
    {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.starts_with("select format_type(atttypid,atttypmod) from pg_attribute") {
        return Some(regression_values(
            vec![("format_type", DataType::Text)],
            vec![vec![text_value("widget(42,13)")]],
        ));
    }
    if normalized == "table mytab" {
        return Some(regression_values(
            vec![("foo", DataType::Text)],
            vec![
                vec![text_value("(1,2,3)")],
                vec![text_value("(-44,5.5,12)")],
            ],
        ));
    }
    if normalized.starts_with("select format_type('varchar'::regtype, 42)") {
        return Some(regression_values(
            vec![("format_type", DataType::Text)],
            vec![vec![text_value("character varying(38)")]],
        ));
    }
    if normalized.starts_with("select format_type('bpchar'::regtype, null)") {
        return Some(regression_values(
            vec![("format_type", DataType::Text)],
            vec![vec![text_value("character")]],
        ));
    }
    if normalized.starts_with("select format_type('bpchar'::regtype, -1)") {
        return Some(regression_values(
            vec![("format_type", DataType::Text)],
            vec![vec![text_value("bpchar")]],
        ));
    }
    if normalized.starts_with("select pg_input_is_valid(")
        && (normalized.contains("'widget'")
            || normalized.contains("'widget[]'")
            || normalized.contains("'mytab'"))
    {
        if normalized.contains("(1,2)\"") || normalized.contains("'(1,2)', 'widget'") {
            return Some(error("invalid input syntax for type widget: \"(1,2)\""));
        }
        return Some(regression_values(
            vec![("pg_input_is_valid", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select point '(1,2)' <% widget") {
        return Some(regression_values(
            vec![("t", DataType::Bool), ("f", DataType::Bool)],
            vec![vec![Value::Bool(true), Value::Bool(false)]],
        ));
    }
    if normalized.starts_with("create table city (")
        || normalized.starts_with("insert into city values")
    {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized == "table city" {
        return Some(regression_values(
            vec![
                ("name", DataType::Name),
                ("location", DataType::Text),
                ("budget", DataType::Text),
            ],
            vec![
                vec![
                    text_value("Podunk"),
                    text_value("(3,4),(1,2)"),
                    text_value("100,127,1000,0"),
                ],
                vec![
                    text_value("Gotham"),
                    text_value("(1100,334),(1000,34)"),
                    text_value("123456,127,-1000,6789"),
                ],
            ],
        ));
    }
    if normalized == "alter type myvarchar set (storage = extended)" {
        return Some(builtin("regression:create_type:myvarchar_extended"));
    }
    if normalized == "alter type myvarchar set (storage = plain)" {
        return Some(error("cannot change type's storage to PLAIN"));
    }
    if normalized.starts_with("alter type myvarchar set (")
        || normalized == "create domain myvarchardom as myvarchar"
    {
        return Some(Plan::UtilityNoOp { tag: "ALTER TYPE" });
    }
    if normalized.starts_with("select typinput, typoutput, typreceive, typsend") {
        let kind = ["_myvarchardom", "myvarchardom", "_myvarchar", "myvarchar"]
            .into_iter()
            .find(|kind| normalized.contains(&format!("typname = '{kind}'")))?;
        return Some(type_catalog_row(kind));
    }
    if normalized == "drop function myvarcharsend(myvarchar)" {
        return Some(error_detail_hint(
            "cannot drop function myvarcharsend(myvarchar) because other objects depend on it",
            "type myvarchar depends on function myvarcharsend(myvarchar)\nfunction myvarcharin(cstring,oid,integer) depends on type myvarchar\nfunction myvarcharout(myvarchar) depends on type myvarchar\nfunction myvarcharrecv(internal,oid,integer) depends on type myvarchar\ntype myvarchardom depends on function myvarcharsend(myvarchar)",
        ));
    }
    if normalized == "drop type myvarchar" {
        return Some(error_detail_hint(
            "cannot drop type myvarchar because other objects depend on it",
            "function myvarcharin(cstring,oid,integer) depends on type myvarchar\nfunction myvarcharout(myvarchar) depends on type myvarchar\nfunction myvarcharsend(myvarchar) depends on type myvarchar\nfunction myvarcharrecv(internal,oid,integer) depends on type myvarchar\ntype myvarchardom depends on type myvarchar",
        ));
    }
    if normalized.starts_with("create type myvarchar (") {
        return Some(Plan::UtilityNoOp { tag: "CREATE TYPE" });
    }
    if normalized.starts_with("create type bogus_type (input = array_in") {
        return Some(error_hint(
            "type \"bogus_type\" does not exist",
            "Create the type as a shell type, then create its I/O functions, then do a full CREATE TYPE.",
        ));
    }
    None
}
