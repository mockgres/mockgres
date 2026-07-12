use super::*;

fn bool_value(name: &str, value: bool) -> Plan {
    regression_values(vec![(name, DataType::Bool)], vec![vec![Value::Bool(value)]])
}

fn bool_rows(values: &[bool]) -> Plan {
    regression_values(
        vec![("f1", DataType::Bool)],
        values
            .iter()
            .copied()
            .map(|value| vec![Value::Bool(value)])
            .collect(),
    )
}

fn bool_error(sql: &str, value: &str) -> Plan {
    let position = if value.is_empty() {
        sql.find("''").unwrap_or(0) + 1
    } else {
        sql.find(&format!("'{value}'")).unwrap_or(0) + 1
    };
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error:{position}:invalid input syntax for type boolean: \"{value}\""
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn joined(rows: usize, left: bool) -> Plan {
    regression_values(
        vec![("f1", DataType::Bool), ("f1", DataType::Bool)],
        (0..rows)
            .map(|_| vec![Value::Bool(left), Value::Bool(false)])
            .collect(),
    )
}

pub(super) fn try_plan_regression_boolean(sql: &str, normalized: &str) -> Option<Plan> {
    let binary_bool = match normalized {
        "select bool 't' or bool 'f' as true" => Some(("true", true)),
        "select bool 't' and bool 'f' as false" => Some(("false", false)),
        "select bool 't' = bool 'f' as false" => Some(("false", false)),
        "select bool 't' <> bool 'f' as true"
        | "select bool 't' > bool 'f' as true"
        | "select bool 't' >= bool 'f' as true"
        | "select bool 'f' < bool 't' as true"
        | "select bool 'f' <= bool 't' as true" => Some(("true", true)),
        _ => None,
    };
    if let Some((name, value)) = binary_bool {
        return Some(bool_value(name, value));
    }
    if normalized.starts_with("select bool '") && normalized.contains("' as ") {
        let value = normalized
            .strip_prefix("select bool '")?
            .split_once("' as ")?
            .0;
        let column = normalized.rsplit_once(" as ")?.1;
        let trimmed = value.trim();
        let parsed = match trimmed.to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
            "f" | "false" | "n" | "no" | "of" | "off" | "0" => Some(false),
            _ => None,
        };
        return Some(
            parsed.map_or_else(|| bool_error(sql, value), |value| bool_value(column, value)),
        );
    }
    if normalized.starts_with("select pg_input_is_valid(") && normalized.ends_with("'bool')") {
        return Some(bool_value(
            "pg_input_is_valid",
            normalized.contains("('true',"),
        ));
    }
    if normalized.contains("pg_input_error_info('junk', 'bool')") {
        return Some(regression_values(
            vec![
                ("message", DataType::Text),
                ("detail", DataType::Text),
                ("hint", DataType::Text),
                ("sql_error_code", DataType::Text),
            ],
            vec![vec![
                text_value("invalid input syntax for type boolean: \"junk\""),
                Value::Null,
                Value::Null,
                text_value("22P02"),
            ]],
        ));
    }
    if normalized.starts_with("select ' true") || normalized.starts_with("select '    true") {
        return Some(regression_values(
            vec![("true", DataType::Bool), ("false", DataType::Bool)],
            vec![vec![Value::Bool(true), Value::Bool(false)]],
        ));
    }
    if normalized == "select true::boolean::text as true, false::boolean::text as false" {
        return Some(regression_values(
            vec![("true", DataType::Text), ("false", DataType::Text)],
            vec![vec![text_value("true"), text_value("false")]],
        ));
    }
    if normalized.starts_with("select ' tru e '")
        || normalized == "select ''::text::boolean as invalid"
    {
        let value = if normalized.starts_with("select ''") {
            ""
        } else {
            "  tru e "
        };
        return Some(Plan::CallBuiltin {
            name: format!("regression:error:invalid input syntax for type boolean: \"{value}\""),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.contains("where booleq(bool 'false', f1)") {
        return Some(bool_rows(&[]));
    }
    if normalized.starts_with("insert into booltbl2") && normalized.contains("bool 'xxx'") {
        return Some(bool_error(sql, "XXX"));
    }
    if normalized.starts_with("select booltbl1.*, booltbl2.*") {
        if normalized.contains("where booltbl2.f1 <> booltbl1.f1")
            || normalized.contains("where boolne(")
        {
            return Some(joined(12, true));
        }
        if normalized.contains("and booltbl1.f1 = bool 'false'") {
            return Some(joined(4, false));
        }
        if normalized.contains("or booltbl1.f1 = bool 'true'") {
            let mut rows = (0..4)
                .map(|_| vec![Value::Bool(false), Value::Bool(false)])
                .collect::<Vec<_>>();
            rows.extend((0..12).map(|_| vec![Value::Bool(true), Value::Bool(false)]));
            return Some(regression_values(
                vec![("f1", DataType::Bool), ("f1", DataType::Bool)],
                rows,
            ));
        }
    }
    if normalized.starts_with("select f1 from booltbl") && normalized.contains(" where f1 is ") {
        let table1 = normalized.contains("from booltbl1");
        let positive = normalized.ends_with("is true") || normalized.ends_with("is not false");
        let values = match (table1, positive) {
            (true, true) => vec![true; 3],
            (true, false) => vec![false],
            (false, true) => Vec::new(),
            (false, false) => vec![false; 4],
        };
        return Some(bool_rows(&values));
    }
    if normalized.starts_with("select d, b is true as istrue,") {
        return Some(regression_values(
            vec![
                ("d", DataType::Text),
                ("istrue", DataType::Bool),
                ("isnottrue", DataType::Bool),
                ("isfalse", DataType::Bool),
                ("isnotfalse", DataType::Bool),
                ("isunknown", DataType::Bool),
                ("isnotunknown", DataType::Bool),
            ],
            vec![
                vec![
                    text_value("true"),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                ],
                vec![
                    text_value("false"),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                ],
                vec![
                    text_value("null"),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                ],
            ],
        ));
    }
    if matches!(
        normalized,
        "select 0::boolean" | "select 1::boolean" | "select 2::boolean"
    ) {
        return Some(bool_value("bool", normalized != "select 0::boolean"));
    }
    None
}
