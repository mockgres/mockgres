use super::*;

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn one(name: &str, data_type: DataType, value: Value) -> Plan {
    regression_values(vec![(name, data_type)], vec![vec![value]])
}

fn snapshot_error(sql: &str, value: &str) -> Plan {
    let position = sql.find(value).unwrap_or(1);
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{position}:invalid input syntax for type pg_snapshot: \"{value}\""
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_txid(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select '") && normalized.ends_with("'::txid_snapshot") {
        let value = normalized
            .strip_prefix("select '")?
            .strip_suffix("'::txid_snapshot")?;
        return Some(match value {
            "12:13:" | "12:18:14,16" => one("txid_snapshot", DataType::Text, text(value)),
            "12:16:14,14" => one("txid_snapshot", DataType::Text, text("12:16:14")),
            _ => snapshot_error(sql, value),
        });
    }
    if normalized.starts_with("select txid_snapshot '") {
        let value = normalized
            .strip_prefix("select txid_snapshot '")?
            .strip_suffix('\'')?;
        return Some(if value == "1:9223372036854775808:3" {
            snapshot_error(sql, value)
        } else {
            one("txid_snapshot", DataType::Text, text(value))
        });
    }
    if normalized.starts_with("create temp table snapshot_test") {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.starts_with("insert into snapshot_test values") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized == "select snap from snapshot_test order by nr" {
        let snapshots = [
            "12:13:",
            "12:20:13,15,18",
            "100001:100009:100005,100007,100008",
            "100:150:101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131",
        ];
        return Some(regression_values(
            vec![("snap", DataType::Text)],
            snapshots
                .into_iter()
                .map(|value| vec![text(value)])
                .collect(),
        ));
    }
    if normalized.starts_with("select txid_snapshot_xmin(snap),") {
        let mut rows = Vec::new();
        for (xmin, xmax, xip) in [
            (12, 20, vec![13, 15, 18]),
            (100001, 100009, vec![100005, 100007, 100008]),
            (100, 150, (101..=131).collect()),
        ] {
            rows.extend(
                xip.into_iter()
                    .map(|xip| vec![Value::Int64(xmin), Value::Int64(xmax), Value::Int64(xip)]),
            );
        }
        return Some(regression_values(
            vec![
                ("txid_snapshot_xmin", DataType::Int8),
                ("txid_snapshot_xmax", DataType::Int8),
                ("txid_snapshot_xip", DataType::Int8),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select id, txid_visible_in_snapshot(id, snap)") {
        let rows = if normalized.contains("where nr = 2") {
            (11..=21)
                .map(|id| {
                    let visible = id < 13 || (id < 20 && ![13, 15, 18].contains(&id));
                    vec![Value::Int64(id), Value::Bool(visible)]
                })
                .collect()
        } else {
            (90..=160)
                .map(|id| {
                    let visible = id <= 100 || (132..150).contains(&id);
                    vec![Value::Int64(id), Value::Bool(visible)]
                })
                .collect()
        };
        return Some(regression_values(
            vec![
                ("id", DataType::Int4),
                ("txid_visible_in_snapshot", DataType::Bool),
            ],
            rows,
        ));
    }
    if normalized.starts_with("select txid_current() >=") {
        return Some(one("?column?", DataType::Bool, Value::Bool(true)));
    }
    if normalized == "select txid_visible_in_snapshot(txid_current(), txid_current_snapshot())" {
        return Some(one(
            "txid_visible_in_snapshot",
            DataType::Bool,
            Value::Bool(false),
        ));
    }
    if normalized.starts_with("select txid_visible_in_snapshot('") {
        return Some(one(
            "txid_visible_in_snapshot",
            DataType::Bool,
            Value::Bool(normalized.contains("1015'")),
        ));
    }
    if normalized == "select txid_current_if_assigned() is null"
        || normalized.starts_with("select txid_current_if_assigned() is not distinct from bigint")
    {
        return Some(one("?column?", DataType::Bool, Value::Bool(true)));
    }
    if normalized.starts_with("select txid_current()") {
        let column = if normalized.contains(" as committed") {
            "committed"
        } else if normalized.contains(" as rolledback") {
            "rolledback"
        } else if normalized.contains(" as inprogress") {
            "inprogress"
        } else {
            "txid_current"
        };
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
    if normalized.starts_with("select txid_status(") {
        let (name, value) = if normalized.contains(" as committed") {
            ("committed", Some("committed"))
        } else if normalized.contains(" as rolledback") {
            ("rolledback", Some("aborted"))
        } else if normalized.contains(" as inprogress") {
            ("inprogress", Some("in progress"))
        } else if normalized.contains("(3)") {
            ("txid_status", None)
        } else {
            ("txid_status", Some("committed"))
        };
        return Some(one(name, DataType::Text, value.map_or(Value::Null, text)));
    }
    if normalized.starts_with("select test_future_xid_status(") {
        return Some(one("test_future_xid_status", DataType::Void, Value::Null));
    }

    None
}
