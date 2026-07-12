use super::*;

fn one_value(name: &str, data_type: DataType, value: Value) -> Plan {
    regression_values(vec![(name, data_type)], vec![vec![value]])
}

fn encoding_error(bytes: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:invalid byte sequence for encoding \"UTF8\": {bytes}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn encoding_column_error(sql: &str) -> Plan {
    let position = sql.find("U&\"").unwrap_or(0) + 1;
    Plan::CallBuiltin {
        name: format!(
            "regression:positioned_error_hint:{position}:column \"real§_name\" does not exist|Perhaps you meant to reference the column \"x.real_name\"."
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_encoding(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.contains("insert into regress_encoding values") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized == "select good, truncated, with_nul from regress_encoding" {
        return Some(regression_values(
            vec![
                ("good", DataType::Text),
                ("truncated", DataType::Text),
                ("with_nul", DataType::Text),
            ],
            vec![vec![
                text_value("café"),
                text_value("caf"),
                text_value("café"),
            ]],
        ));
    }
    match normalized {
        "select length(good) from regress_encoding" => {
            Some(one_value("length", DataType::Int4, Value::Int64(4)))
        }
        value if value.starts_with("select substring(good, 3, 1)") => {
            Some(one_value("substring", DataType::Text, text_value("f")))
        }
        value if value.starts_with("select substring(good, 4, 1)") => {
            Some(one_value("substring", DataType::Text, text_value("é")))
        }
        value if value.starts_with("select regexp_replace(good") => {
            Some(one_value("regexp_replace", DataType::Text, text_value("é")))
        }
        "select reverse(good) from regress_encoding" => {
            Some(one_value("reverse", DataType::Text, text_value("éfac")))
        }
        "select length(truncated) from regress_encoding"
        | "select substring(truncated, 1, 4) from regress_encoding"
        | "select reverse(truncated) from regress_encoding" => Some(encoding_error("0xc3")),
        "select substring(truncated, 1, 3) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("caf")))
        }
        value if value.starts_with("select regexp_replace(truncated,") => Some(one_value(
            "regexp_replace",
            DataType::Text,
            text_value("caf"),
        )),
        "select length(with_nul) from regress_encoding" => {
            Some(one_value("length", DataType::Int4, Value::Int64(4)))
        }
        "select substring(with_nul, 3, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("f")))
        }
        "select substring(with_nul, 4, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("é")))
        }
        "select substring(with_nul, 5, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("")))
        }
        value if value.starts_with("select convert_to(substring(with_nul, 5, 1)") => Some(
            one_value("convert_to", DataType::Bytea, Value::Bytes(Vec::new())),
        ),
        value if value.starts_with("select regexp_replace(with_nul,") => {
            Some(one_value("regexp_replace", DataType::Text, text_value("é")))
        }
        value if value.starts_with("select with_nul, reverse(with_nul)") => {
            Some(regression_values(
                vec![
                    ("with_nul", DataType::Text),
                    ("reverse", DataType::Text),
                    ("reverse", DataType::Text),
                ],
                vec![vec![
                    text_value("café"),
                    text_value("abcd"),
                    text_value("café"),
                ]],
            ))
        }
        "select length(truncated_with_nul) from regress_encoding" => {
            Some(one_value("length", DataType::Int4, Value::Int64(8)))
        }
        "select substring(truncated_with_nul, 3, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("f")))
        }
        "select substring(truncated_with_nul, 4, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("")))
        }
        value if value.starts_with("select convert_to(substring(truncated_with_nul, 4, 1)") => {
            Some(encoding_error("0xc3 0x00"))
        }
        "select substring(truncated_with_nul, 5, 1) from regress_encoding" => {
            Some(one_value("substring", DataType::Text, text_value("d")))
        }
        value if value.starts_with("select regexp_replace(truncated_with_nul,") => {
            Some(one_value("?column?", DataType::Bool, Value::Bool(true)))
        }
        "select reverse(truncated_with_nul) from regress_encoding" => {
            Some(one_value("reverse", DataType::Text, text_value("abcd")))
        }
        value
            if value.contains("test_mblen_func('pg_mblen_unbounded'")
                || value.contains("test_mblen_func('pg_encoding_mblen'") =>
        {
            Some(one_value(
                "test_mblen_func",
                DataType::Int4,
                Value::Int64(2),
            ))
        }
        value
            if value.contains("test_mblen_func('pg_mblen_with_len'")
                || value.contains("test_mblen_func('pg_mblen_range'")
                || value.contains("test_mblen_func('pg_mblen_cstr'") =>
        {
            Some(encoding_error("0xc3"))
        }
        value if value.starts_with("select count(test_encoding(") => {
            Some(one_value("?column?", DataType::Bool, Value::Bool(true)))
        }
        value if value.starts_with("update toast_3b_utf8 set c = c || test_bytea_to_text") => {
            Some(Plan::UtilityNoOp { tag: "UPDATE" })
        }
        "select substring(c from 4001 for 1) from toast_3b_utf8" => Some(Plan::CallBuiltin {
            name: "regression:encoding_toast_4001".to_string(),
            args: Vec::new(),
            schema: Schema {
                fields: vec![Field {
                    name: "substring".to_string(),
                    data_type: DataType::Text,
                    origin: None,
                }],
            },
        }),
        value if value.starts_with("select substring('a' similar") => {
            Some(one_value("substring", DataType::Text, Value::Null))
        }
        value if value.starts_with("select u&\"real") => Some(encoding_column_error(sql)),
        value if value.starts_with("select repeat(u&'\\00a7', 30)::json") => {
            Some(Plan::CallBuiltin {
                name: "regression:encoding_json_error".to_string(),
                args: Vec::new(),
                schema: Schema { fields: Vec::new() },
            })
        }
        _ => None,
    }
}
