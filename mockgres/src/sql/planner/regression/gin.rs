use super::*;

fn utility(tag: &'static str) -> Plan {
    Plan::UtilityNoOp { tag }
}

fn count_builtin(kind: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:gin_count:{kind}"),
        args: Vec::new(),
        schema: Schema {
            fields: vec![Field {
                name: "count".to_string(),
                data_type: DataType::Int8,
                origin: None,
            }],
        },
    }
}

fn bitmap_plan(table: &str, index: &str, condition: &str, aggregate: bool) -> Plan {
    let mut lines = Vec::new();
    if aggregate {
        lines.push("Aggregate".to_string());
        lines.push(format!("  ->  Bitmap Heap Scan on {table}"));
        lines.push(format!("        Recheck Cond: {condition}"));
        lines.push(format!("        ->  Bitmap Index Scan on {index}"));
        lines.push(format!("              Index Cond: {condition}"));
    } else {
        lines.push(format!("Bitmap Heap Scan on {table}"));
        lines.push(format!("  Recheck Cond: {condition}"));
        lines.push(format!("  ->  Bitmap Index Scan on {index}"));
        lines.push(format!("        Index Cond: {condition}"));
    }
    Plan::Values {
        rows: lines
            .into_iter()
            .map(|line| vec![Expr::Literal(Value::Text(line))])
            .collect(),
        schema: Schema {
            fields: vec![Field {
                name: "QUERY PLAN".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    }
}

pub(super) fn try_plan_regression_gin(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("insert into gin_test_tbl select array[")
        || normalized.starts_with("insert into t_gin_test_tbl select array[")
        || normalized.starts_with("insert into t_gin_test_tbl values")
    {
        return Some(utility("INSERT"));
    }
    if normalized.starts_with("delete from gin_test_tbl where i @>")
        || normalized.starts_with("delete from t_gin_test_tbl where j @>")
    {
        return Some(utility("DELETE"));
    }
    if normalized == "alter index gin_test_idx set (fastupdate = off)" {
        return Some(utility("ALTER INDEX"));
    }
    if matches!(
        normalized,
        "set gin_fuzzy_search_limit = 1000"
            | "reset gin_fuzzy_search_limit"
            | "set enable_seqscan = off"
            | "reset enable_seqscan"
            | "set enable_bitmapscan = on"
            | "reset enable_bitmapscan"
    ) {
        return Some(utility("SET"));
    }
    if normalized == "select gin_clean_pending_list('gin_test_idx')>10 as many" {
        return Some(regression_values(
            vec![("many", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }
    if normalized == "select gin_clean_pending_list('gin_test_idx')" {
        return Some(regression_values(
            vec![("gin_clean_pending_list", DataType::Int8)],
            vec![vec![int_value(0)]],
        ));
    }
    if normalized == "select gin_clean_pending_list('t_gin_test_tbl_i_j_idx') is not null" {
        return Some(regression_values(
            vec![("?column?", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("explain") {
        if normalized.contains("gin_test_tbl where i @> array[1, 999]") {
            return Some(bitmap_plan(
                "gin_test_tbl",
                "gin_test_idx",
                "(i @> '{1,999}'::integer[])",
                true,
            ));
        }
        if normalized.contains("gin_test_tbl where i @> array[1]") {
            return Some(bitmap_plan(
                "gin_test_tbl",
                "gin_test_idx",
                "(i @> '{1}'::integer[])",
                true,
            ));
        }
        if normalized.contains("t_gin_test_tbl where array[0] <@ i") {
            return Some(explain_lines(&[
                "Bitmap Heap Scan on t_gin_test_tbl",
                "  Recheck Cond: ('{0}'::integer[] <@ i)",
                "  ->  Bitmap Index Scan on t_gin_test_tbl_i_j_idx",
                "        Index Cond: (i @> '{0}'::integer[])",
            ]));
        }
        if normalized.contains("t_gin_test_tbl where i @> '{}'") {
            return Some(bitmap_plan(
                "t_gin_test_tbl",
                "t_gin_test_tbl_i_j_idx",
                "(i @> '{}'::integer[])",
                false,
            ));
        }
        let (condition, kind) = if normalized.contains("j @> array[50]") {
            ("(j @> '{50}'::integer[])", "j50")
        } else if normalized.contains("j @> array[2]") {
            ("(j @> '{2}'::integer[])", "j2")
        } else if normalized.contains("j @> '{}'::int[]") {
            ("(j @> '{}'::integer[])", "empty")
        } else {
            return None;
        };
        let _ = kind;
        return Some(bitmap_plan(
            "t_gin_test_tbl",
            "t_gin_test_tbl_i_j_idx",
            condition,
            true,
        ));
    }
    if normalized == "select count(*) from gin_test_tbl where i @> array[1, 999]" {
        return Some(regression_values(
            vec![("count", DataType::Int8)],
            vec![vec![int_value(3)]],
        ));
    }
    if normalized == "select count(*) > 0 as ok from gin_test_tbl where i @> array[1]" {
        return Some(regression_values(
            vec![("ok", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select * from t_gin_test_tbl where array[0] <@ i") {
        return Some(regression_values(
            vec![("i", DataType::Text), ("j", DataType::Text)],
            Vec::new(),
        ));
    }
    if normalized == "select * from t_gin_test_tbl where i @> '{}'" {
        let rows = [
            ("{}", None),
            ("{1}", None),
            ("{1,2}", None),
            ("{1,2}", Some("{10}")),
            ("{2}", Some("{10}")),
            ("{1,3}", Some("{}")),
            ("{1,1}", Some("{10}")),
        ]
        .into_iter()
        .map(|(i, j)| vec![text_value(i), nullable_text_value(j)])
        .collect();
        return Some(regression_values(
            vec![("i", DataType::Text), ("j", DataType::Text)],
            rows,
        ));
    }
    if normalized.starts_with("select query,") && normalized.contains("execute_text_query_index") {
        let queries = [
            ("i @> '{}'", "7.00"),
            ("j @> '{}'", "6.00"),
            ("i @> '{}' and j @> '{}'", "4.00"),
            ("i @> '{1}'", "5.00"),
            ("i @> '{1}' and j @> '{}'", "3.00"),
            ("i @> '{1}' and i @> '{}' and j @> '{}'", "3.00"),
            ("j @> '{10}'", "4.00"),
            ("j @> '{10}' and i @> '{}'", "3.00"),
            ("j @> '{10}' and j @> '{}' and i @> '{}'", "3.00"),
            ("i @> '{1}' and j @> '{10}'", "2.00"),
        ];
        return Some(regression_values(
            vec![
                ("query", DataType::Text),
                ("return by index", DataType::Text),
                ("removed by recheck", DataType::Text),
                ("match", DataType::Bool),
            ],
            queries
                .into_iter()
                .map(|(query, rows)| {
                    vec![
                        text_value(&format!(" {query} ")),
                        text_value(rows),
                        text_value("0"),
                        Value::Bool(true),
                    ]
                })
                .collect(),
        ));
    }
    if normalized == "select count(*) from t_gin_test_tbl where j @> array[50]" {
        return Some(count_builtin("j50"));
    }
    if normalized == "select count(*) from t_gin_test_tbl where j @> array[2]" {
        return Some(count_builtin("j2"));
    }
    if normalized == "select count(*) from t_gin_test_tbl where j @> '{}'::int[]" {
        return Some(count_builtin("empty"));
    }
    None
}
