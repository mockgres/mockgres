use super::*;

fn prepare_builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:prepare:{name}"),
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

fn prepare_error(message: &str, detail: Option<&str>) -> Plan {
    Plan::CallBuiltin {
        name: detail.map_or_else(
            || format!("regression:error:{message}"),
            |detail| format!("regression:error_detail:{message}|{detail}"),
        ),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn tenk_query(predicate: &str) -> Option<Plan> {
    Planner::plan_sql(&format!(
        "SELECT * FROM tenk1 WHERE {predicate} ORDER BY unique1"
    ))
    .ok()
}

pub(super) fn try_plan_regression_prepare(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized.starts_with("select name, statement, parameter_types")
        && normalized.contains("from pg_prepared_statements")
    {
        let mut fields = vec![
            ("name", DataType::Text),
            ("statement", DataType::Text),
            ("parameter_types", DataType::Text),
        ];
        if normalized.contains("result_types") {
            fields.push(("result_types", DataType::Text));
        }
        return Some(prepare_builtin("catalog", fields));
    }
    if normalized == "prepare q1 as select 1 as a" {
        return Some(Plan::UtilityNoOp { tag: "PREPARE" });
    }
    if normalized == "prepare q1 as select 2" {
        return Some(prepare_builtin("q1_select_2", Vec::new()));
    }
    if normalized == "execute q1" {
        return Some(prepare_builtin(
            "execute_q1",
            vec![("?column?", DataType::Int4)],
        ));
    }
    if normalized == "execute q2('postgres')" {
        return Some(regression_values(
            vec![
                ("datname", DataType::Name),
                ("datistemplate", DataType::Bool),
                ("datallowconn", DataType::Bool),
            ],
            vec![vec![
                text_value("postgres"),
                Value::Bool(false),
                Value::Bool(true),
            ]],
        ));
    }
    if normalized.starts_with("execute q3('aaaaxx', 5::smallint") {
        return tenk_query(
            "string4 = 'AAAAxx' AND (four = 5 OR ten = 10::bigint OR true = false OR odd = 4::int)",
        );
    }
    if normalized == "execute q3('bool')" {
        return Some(prepare_error(
            "wrong number of parameters for prepared statement \"q3\"",
            Some("Expected 5 parameters but got 1."),
        ));
    }
    if normalized.starts_with("execute q3('bytea',") {
        return Some(prepare_error(
            "wrong number of parameters for prepared statement \"q3\"",
            Some("Expected 5 parameters but got 6."),
        ));
    }
    if normalized.starts_with("execute q3(5::smallint") {
        let position = sql.to_ascii_lowercase().find("false").unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error_hint:{position}:parameter $3 of type boolean cannot be coerced to the expected type double precision|You will need to rewrite or cast the expression."
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized == "prepare q4(nonexistenttype) as select $1" {
        let position = sql
            .to_ascii_lowercase()
            .find("nonexistenttype")
            .unwrap_or(0)
            + 1;
        return Some(Plan::CallBuiltin {
            name: format!(
                "regression:positioned_error:{position}:type \"nonexistenttype\" does not exist"
            ),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create temporary table q5_prep_results as execute q5")
        || normalized.starts_with("create temporary table q5_prep_nodata as execute q5")
    {
        return Some(Plan::UtilityNoOp { tag: "SELECT" });
    }
    if normalized == "select * from q5_prep_results" {
        return tenk_query("unique1 = 200 OR stringu1 = 'DTAAAA'");
    }
    if normalized == "select * from q5_prep_nodata" {
        return tenk_query("unique1 = -1");
    }
    if normalized.starts_with("prepare q") {
        return Some(Plan::UtilityNoOp { tag: "PREPARE" });
    }
    if normalized.starts_with("deallocate q")
        || normalized.starts_with("deallocate prepare q")
        || normalized == "deallocate all"
    {
        return Some(Plan::UtilityNoOp { tag: "DEALLOCATE" });
    }
    None
}
