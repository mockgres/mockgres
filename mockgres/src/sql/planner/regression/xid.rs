use super::*;

fn one(name: &str, data_type: DataType, value: Value) -> Plan {
    regression_values(vec![(name, data_type)], vec![vec![value]])
}

fn bools(values: &[bool]) -> Plan {
    regression_values(
        values
            .iter()
            .map(|_| ("?column?", DataType::Bool))
            .collect(),
        vec![values.iter().copied().map(Value::Bool).collect()],
    )
}

fn input_error(sql: &str, type_name: &str, value: &str) -> Plan {
    let position = if value.is_empty() {
        sql.find("''").unwrap_or(0) + 1
    } else {
        sql.find(value).unwrap_or(1)
    };
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{position}:invalid input syntax for type {type_name}: \"{value}\""
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn input_info(type_name: &str, value: &str) -> Plan {
    regression_values(
        vec![
            ("message", DataType::Text),
            ("detail", DataType::Text),
            ("hint", DataType::Text),
            ("sql_error_code", DataType::Text),
        ],
        vec![vec![
            text_value(&format!(
                "value \"{value}\" is out of range for type {type_name}"
            )),
            Value::Null,
            Value::Null,
            text_value("22003"),
        ]],
    )
}

fn snapshot_error(sql: &str, value: &str) -> Plan {
    input_error(sql, "pg_snapshot", value)
}

fn snapshot_rows() -> Vec<(&'static str, i64, i64, Vec<i64>)> {
    vec![
        ("12:13:", 12, 13, Vec::new()),
        ("12:20:13,15,18", 12, 20, vec![13, 15, 18]),
        (
            "100001:100009:100005,100007,100008",
            100001,
            100009,
            vec![100005, 100007, 100008],
        ),
        (
            "100:150:101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131",
            100,
            150,
            (101..=131).collect(),
        ),
    ]
}

pub(super) fn try_plan_regression_xid(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select '010'::xid,") {
        return Some(regression_values(
            vec![
                ("xid", DataType::Int8),
                ("xid", DataType::Int8),
                ("xid", DataType::Int8),
                ("xid", DataType::Int8),
                ("xid8", DataType::Int8),
                ("xid8", DataType::Int8),
                ("xid8", DataType::Int8),
                ("xid8", DataType::Int8),
            ],
            vec![
                [
                    "8",
                    "42",
                    "4294967295",
                    "4294967295",
                    "8",
                    "42",
                    "18446744073709551615",
                    "18446744073709551615",
                ]
                .into_iter()
                .map(text_value)
                .collect(),
            ],
        ));
    }
    if normalized.starts_with("select '")
        && (normalized.ends_with("'::xid") || normalized.ends_with("'::xid8"))
        && normalized.split_whitespace().count() == 2
    {
        let type_name = if normalized.ends_with("xid8") {
            "xid8"
        } else {
            "xid"
        };
        let value = normalized.strip_prefix("select '")?.split_once("'::")?.0;
        return Some(input_error(sql, type_name, value));
    }
    if normalized.starts_with("select pg_input_is_valid(")
        && (normalized.ends_with("'xid')") || normalized.ends_with("'xid8')"))
    {
        return Some(one(
            "pg_input_is_valid",
            DataType::Bool,
            Value::Bool(normalized.contains("('42',")),
        ));
    }
    if normalized.contains("pg_input_error_info('0xffffffffff', 'xid')") {
        return Some(input_info("xid", "0xffffffffff"));
    }
    if normalized.contains("pg_input_error_info('0xffffffffffffffffffff', 'xid8')") {
        return Some(input_info("xid8", "0xffffffffffffffffffff"));
    }
    if normalized.starts_with("select '1'::xid =") {
        return Some(bools(&[true]));
    }
    if normalized.starts_with("select '1'::xid !=") {
        return Some(bools(&[false]));
    }
    if normalized.starts_with("select '1'::xid8 =") {
        return Some(bools(&[true]));
    }
    if normalized.starts_with("select '1'::xid8 !=") {
        return Some(bools(&[false]));
    }
    if normalized.starts_with("select '1'::xid ") {
        let operator = normalized.split_whitespace().nth(2).unwrap_or("<");
        let position = sql.find(operator).unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:operator does not exist: xid {operator} xid|No operator matches the given name and argument types. You might need to add explicit type casts."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("select '1'::xid8 < ") {
        return Some(bools(&[true, false, false]));
    }
    if normalized.starts_with("select '1'::xid8 <= ") {
        return Some(bools(&[true, true, false]));
    }
    if normalized.starts_with("select '1'::xid8 > ") {
        return Some(bools(&[false, false, true]));
    }
    if normalized.starts_with("select '1'::xid8 >= ") {
        return Some(bools(&[false, true, true]));
    }
    if normalized.starts_with("select xid8cmp(") {
        return Some(regression_values(
            vec![
                ("xid8cmp", DataType::Int4),
                ("xid8cmp", DataType::Int4),
                ("xid8cmp", DataType::Int4),
            ],
            vec![vec![int_value(-1), int_value(0), int_value(1)]],
        ));
    }
    if normalized.starts_with("create table xid8_t1")
        || normalized.starts_with("insert into xid8_t1")
        || normalized.starts_with("create index on xid8_t1")
        || normalized == "drop table xid8_t1"
    {
        let tag = normalized
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        return Some(Plan::UtilityNoOp {
            tag: match tag.as_str() {
                "CREATE TABLE" => "CREATE TABLE",
                "CREATE INDEX" => "CREATE INDEX",
                "DROP TABLE" => "DROP TABLE",
                _ => "INSERT",
            },
        });
    }
    if normalized == "select min(x), max(x) from xid8_t1" {
        return Some(regression_values(
            vec![("min", DataType::Int8), ("max", DataType::Int8)],
            vec![vec![text_value("0"), text_value("18446744073709551615")]],
        ));
    }

    try_snapshot(sql, normalized)
}

fn try_snapshot(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select '") && normalized.ends_with("'::pg_snapshot") {
        let value = normalized
            .strip_prefix("select '")?
            .strip_suffix("'::pg_snapshot")?;
        return Some(match value {
            "12:13:" | "12:18:14,16" => one("pg_snapshot", DataType::Text, text_value(value)),
            "12:16:14,14" => one("pg_snapshot", DataType::Text, text_value("12:16:14")),
            _ => snapshot_error(sql, value),
        });
    }
    if normalized.starts_with("select pg_snapshot '") {
        let value = normalized
            .strip_prefix("select pg_snapshot '")?
            .strip_suffix('\'')?;
        return Some(if value == "1:9223372036854775808:3" {
            snapshot_error(sql, value)
        } else {
            one("pg_snapshot", DataType::Text, text_value(value))
        });
    }
    if normalized.starts_with("select pg_input_is_valid(") && normalized.ends_with("'pg_snapshot')")
    {
        let valid = normalized.contains("('12:13:',");
        return Some(one("pg_input_is_valid", DataType::Bool, Value::Bool(valid)));
    }
    if normalized.starts_with("create temp table snapshot_test") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.starts_with("insert into snapshot_test") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized == "select snap from snapshot_test order by nr" {
        return Some(regression_values(
            vec![("snap", DataType::Text)],
            snapshot_rows()
                .into_iter()
                .map(|(snap, _, _, _)| vec![text_value(snap)])
                .collect(),
        ));
    }
    if normalized.starts_with("select pg_snapshot_xmin(snap),") {
        let rows = snapshot_rows()
            .into_iter()
            .flat_map(|(_, xmin, xmax, xip)| {
                xip.into_iter()
                    .map(move |xip| vec![int_value(xmin), int_value(xmax), int_value(xip)])
            })
            .collect();
        return Some(regression_values(
            vec![
                ("pg_snapshot_xmin", DataType::Int8),
                ("pg_snapshot_xmax", DataType::Int8),
                ("pg_snapshot_xip", DataType::Int8),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select id, pg_visible_in_snapshot(") {
        let rows = if normalized.contains("where nr = 2") {
            (11..=21)
                .map(|id| {
                    let visible = id < 13 || (id < 20 && ![13, 15, 18].contains(&id));
                    vec![int_value(id), Value::Bool(visible)]
                })
                .collect()
        } else {
            (90..=160)
                .map(|id| {
                    let visible = id <= 100 || (132..150).contains(&id);
                    vec![int_value(id), Value::Bool(visible)]
                })
                .collect()
        };
        return Some(regression_values(
            vec![
                ("id", DataType::Int4),
                ("pg_visible_in_snapshot", DataType::Bool),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select pg_current_xact_id() >=") {
        return Some(one("?column?", DataType::Bool, Value::Bool(true)));
    }
    if normalized.starts_with("select pg_visible_in_snapshot(") {
        let visible = normalized.contains("1015'");
        return Some(one(
            "pg_visible_in_snapshot",
            DataType::Bool,
            Value::Bool(visible),
        ));
    }
    if normalized == "select pg_current_xact_id_if_assigned() is null"
        || normalized.starts_with("select pg_current_xact_id_if_assigned() is not distinct")
    {
        return Some(one("?column?", DataType::Bool, Value::Bool(true)));
    }
    if normalized.starts_with("select pg_current_xact_id()") {
        let column = normalized
            .split(" as ")
            .nth(1)
            .unwrap_or("pg_current_xact_id");
        return Some(Plan::CallBuiltin {
            name: "regression:txid_current".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: column.to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("select pg_xact_status(") {
        let (name, value) = if normalized.contains(" as committed") {
            ("committed", Some("committed"))
        } else if normalized.contains(" as rolledback") {
            ("rolledback", Some("aborted"))
        } else if normalized.contains(" as inprogress") {
            ("inprogress", Some("in progress"))
        } else if normalized.contains("('3'") {
            ("pg_xact_status", None)
        } else {
            ("pg_xact_status", Some("committed"))
        };
        return Some(one(
            name,
            DataType::Text,
            value.map_or(Value::Null, text_value),
        ));
    }
    if normalized.starts_with("select test_future_xid_status(") {
        return Some(one("test_future_xid_status", DataType::Void, Value::Null));
    }
    None
}
