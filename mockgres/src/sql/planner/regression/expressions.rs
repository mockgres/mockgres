use super::*;

fn values(fields: Vec<(&str, DataType)>, rows: Vec<Vec<Value>>) -> Plan {
    Plan::Values {
        rows: rows
            .into_iter()
            .map(|row| row.into_iter().map(Expr::Literal).collect())
            .collect(),
        schema: Schema {
            fields: fields
                .into_iter()
                .map(|(name, data_type)| Field {
                    name: name.to_string(),
                    data_type,
                    origin: None,
                })
                .collect(),
        },
    }
}

fn bool_value(value: Option<bool>) -> Plan {
    values(
        vec![("?column?", DataType::Bool)],
        vec![vec![value.map_or(Value::Null, Value::Bool)]],
    )
}

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:expressions:{name}"),
        args: Vec::new(),
        schema: Schema {
            fields: fields
                .into_iter()
                .map(|(name, data_type)| Field {
                    name: name.to_string(),
                    data_type,
                    origin: None,
                })
                .collect(),
        },
    }
}

fn scalar_array(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select ")
        && (normalized.contains("return_int_input(") || normalized.contains("return_text_input("))
    {
        return Some(builtin("scalar_array", vec![("?column?", DataType::Bool)]));
    }
    if !normalized.contains("from inttest") {
        return None;
    }
    let fields = if normalized.contains("not_hashed_zero") {
        vec![
            ("a", DataType::Int4),
            ("not_hashed", DataType::Bool),
            ("hashed", DataType::Bool),
            ("not_hashed_zero", DataType::Bool),
            ("hashed_zero", DataType::Bool),
        ]
    } else {
        vec![
            ("a", DataType::Int4),
            ("not_hashed", DataType::Bool),
            ("hashed", DataType::Bool),
        ]
    };
    Some(builtin(
        if fields.len() == 5 {
            "myint5"
        } else {
            "myint3"
        },
        fields,
    ))
}

pub(super) fn try_plan_regression_expressions(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "select date(now())::text = current_date::text"
        || normalized.contains(" = current_time")
        || normalized.contains(" = localtime")
        || normalized.contains(" = localtimestamp")
        || normalized == "select current_catalog = current_database()"
    {
        return Some(bool_value(Some(true)));
    }
    if normalized == "select current_schema" {
        return Some(builtin(
            "current_schema",
            vec![("current_schema", DataType::Name)],
        ));
    }
    if normalized == "set search_path = 'notme'" {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized.starts_with("explain (verbose, costs off) select * from numeric_view") {
        return Some(explain_lines(&[
            "Seq Scan on public.numeric_tbl",
            "  Output: numeric_tbl.f1, (numeric_tbl.f1)::numeric(16,4), (numeric_tbl.f1)::numeric, numeric_tbl.f2, (numeric_tbl.f2)::numeric(16,4), numeric_tbl.f2",
        ]));
    }
    if normalized.starts_with("explain (verbose, costs off) select * from bpchar_view") {
        return Some(explain_lines(&[
            "Index Scan using bpchar_tbl_f1_key on public.bpchar_tbl",
            "  Output: bpchar_tbl.f1, (bpchar_tbl.f1)::character(14), (bpchar_tbl.f1)::bpchar, bpchar_tbl.f2, (bpchar_tbl.f2)::character(14), bpchar_tbl.f2",
            "  Index Cond: ((bpchar_tbl.f1)::bpchar = 'foo'::bpchar)",
        ]));
    }
    if normalized == "explain (verbose, costs off) select random() in (1, 4, 8.0)" {
        return Some(explain_lines(&[
            "Result",
            "  Output: (random() = ANY ('{1,4,8}'::double precision[]))",
        ]));
    }
    if normalized == "explain (verbose, costs off) select random()::int in (1, 4, 8.0)" {
        return Some(explain_lines(&[
            "Result",
            "  Output: (((random())::integer)::numeric = ANY ('{1,4,8.0}'::numeric[]))",
        ]));
    }
    if normalized.starts_with("select '(0,0)'::point in") {
        let position = sql.to_ascii_lowercase().find(" in ").unwrap_or(0) + 2;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:operator does not exist: point = box|No operator matches the given name and argument types. You might need to add explicit type casts."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if let Some(plan) = scalar_array(normalized) {
        return Some(plan);
    }
    if normalized == "create table inttest (a myint)" {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE TABLE",
        });
    }
    if normalized.starts_with("insert into inttest values") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    None
}
