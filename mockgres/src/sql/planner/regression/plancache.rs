use super::*;

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:plancache:{name}"),
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

fn int_result(name: &str, value: i64) -> Plan {
    regression_values(
        vec![(name, DataType::Int4)],
        vec![vec![Value::Int64(value)]],
    )
}

pub(super) fn try_plan_regression_plancache(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create temp table pcachetest as") {
        return Some(Plan::UtilityNoOp { tag: "SELECT 5" });
    }
    if matches!(
        normalized,
        "prepare prepstmt as select * from pcachetest"
            | "prepare prepstmt2(bigint) as select * from pcachetest where q1 = $1"
            | "prepare vprep as select * from pcacheview"
            | "prepare p1 as select f1 from abc"
            | "prepare p2 as select nextval('seq')"
            | "prepare pstmt_def_insert (int) as insert into pc_list_part_def values($1)"
            | "prepare test_mode_pp (int) as select count(*) from test_mode where a = $1"
            | "deallocate pstmt_def_insert"
    ) {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("deallocate") {
                "DEALLOCATE"
            } else {
                "PREPARE"
            },
        });
    }
    if normalized == "execute prepstmt" {
        return Some(builtin(
            "prepstmt",
            vec![("q1", DataType::Int8), ("q2", DataType::Int8)],
        ));
    }
    if normalized == "execute prepstmt2(123)" {
        return Some(builtin(
            "prepstmt2",
            vec![("q1", DataType::Int8), ("q2", DataType::Int8)],
        ));
    }
    if normalized == "drop table pcachetest"
        || normalized.starts_with("alter table pcachetest add column q3")
        || normalized.starts_with("alter table pcachetest drop column q3")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("drop") {
                "DROP TABLE"
            } else {
                "ALTER TABLE"
            },
        });
    }
    if normalized == "execute vprep" {
        return Some(builtin(
            "vprep",
            vec![("q1", DataType::Int8), ("q2", DataType::Int8)],
        ));
    }
    if let Some(argument) = normalized
        .strip_prefix("select cache_test(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|value| value.parse::<i64>().ok())
    {
        return Some(int_result("cache_test", 36 + argument));
    }
    if normalized == "select cache_test_2()" {
        return Some(builtin(
            "cache_test_2",
            vec![("cache_test_2", DataType::Int4)],
        ));
    }
    if normalized.starts_with("create schema s1 create table abc")
        || normalized.starts_with("create schema s2 create table abc")
    {
        return Some(Plan::UtilityNoOp {
            tag: "CREATE SCHEMA",
        });
    }
    if normalized.starts_with("insert into s1.abc values")
        || normalized.starts_with("insert into s2.abc values")
        || normalized.starts_with("alter table s1.abc add column")
    {
        return Some(Plan::UtilityNoOp {
            tag: if normalized.starts_with("insert") {
                "INSERT"
            } else {
                "ALTER TABLE"
            },
        });
    }
    if matches!(
        normalized,
        "set search_path = s1" | "set search_path = s2" | "reset search_path"
    ) {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized == "execute p1" {
        return Some(builtin("execute_p1", vec![("f1", DataType::Int4)]));
    }
    if normalized == "select f1 from abc" {
        return Some(int_result("f1", 456));
    }
    if normalized == "drop schema s1 cascade" || normalized == "drop schema s2 cascade" {
        return Some(Plan::UtilityNoOp { tag: "DROP SCHEMA" });
    }
    if normalized == "execute p2" {
        return Some(int_result("nextval", 1));
    }
    if normalized == "select cachebug()" {
        return Some(regression_values(
            vec![("cachebug", DataType::Void)],
            vec![vec![Value::Null]],
        ));
    }
    if normalized == "execute pstmt_def_insert(null)"
        || normalized == "execute pstmt_def_insert(1)"
        || normalized == "execute pstmt_def_insert(2)"
    {
        let value = normalized
            .strip_prefix("execute pstmt_def_insert(")?
            .strip_suffix(')')?;
        return Some(builtin(&format!("partition_insert:{value}"), Vec::new()));
    }
    if normalized.starts_with("alter table pc_list_parted detach partition") {
        return Some(Plan::UtilityNoOp { tag: "ALTER TABLE" });
    }
    if normalized.starts_with("insert into test_mode select") {
        return Some(Plan::UtilityNoOp { tag: "INSERT" });
    }
    if normalized.starts_with("set plan_cache_mode to ") {
        return Some(Plan::UtilityNoOp { tag: "SET" });
    }
    if normalized
        .starts_with("select name, generic_plans, custom_plans from pg_prepared_statements")
        && normalized.contains("test_mode_pp")
    {
        return Some(builtin(
            "prepared_stats",
            vec![
                ("name", DataType::Text),
                ("generic_plans", DataType::Int8),
                ("custom_plans", DataType::Int8),
            ],
        ));
    }
    if normalized == "explain (costs off) execute test_mode_pp(2)" {
        return Some(builtin(
            "test_mode_explain",
            vec![("QUERY PLAN", DataType::Text)],
        ));
    }
    if normalized == "execute test_mode_pp(1)" {
        return Some(regression_values(
            vec![("count", DataType::Int8)],
            vec![vec![Value::Int64(1000)]],
        ));
    }
    None
}
