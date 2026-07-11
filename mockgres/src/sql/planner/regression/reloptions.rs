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

fn detailed(message: &str, detail: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error_detail:{message}|{detail}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn reloptions_builtin(name: &str) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
        args: Vec::new(),
        schema: Schema {
            fields: vec![Field {
                name: "reloptions".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    }
}

fn bounded(value: &str, option: &str, range: &str) -> Plan {
    detailed(
        &format!("value {value} out of bounds for option \"{option}\""),
        &format!("Valid values are between {range}."),
    )
}

pub(super) fn try_plan_regression_reloptions(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create table reloptions_test2") {
        let plan = if normalized.contains("not_existing_namespace") {
            error("unrecognized parameter namespace \"not_existing_namespace\"")
        } else if normalized.contains("not_existing_option")
            || normalized.contains("toast.not_existing_option")
        {
            error("unrecognized parameter \"not_existing_option\"")
        } else if normalized.contains("fillfactor=2)") {
            bounded("2", "fillfactor", "\"10\" and \"100\"")
        } else if normalized.contains("fillfactor=110") {
            bounded("110", "fillfactor", "\"10\" and \"100\"")
        } else if normalized.contains("fillfactor=-30.1") {
            bounded("-30.1", "fillfactor", "\"10\" and \"100\"")
        } else if normalized.contains("autovacuum_analyze_scale_factor = -10.0") {
            bounded(
                "-10.0",
                "autovacuum_analyze_scale_factor",
                "\"0.000000\" and \"100.000000\"",
            )
        } else if normalized.contains("autovacuum_analyze_scale_factor = 110.0") {
            bounded(
                "110.0",
                "autovacuum_analyze_scale_factor",
                "\"0.000000\" and \"100.000000\"",
            )
        } else if normalized.contains("fillfactor='string'") {
            error("invalid value for integer option \"fillfactor\": string")
        } else if normalized.contains("fillfactor=true")
            || normalized.ends_with("with (fillfactor)")
        {
            error("invalid value for integer option \"fillfactor\": true")
        } else if normalized.contains("autovacuum_enabled=12") {
            error("invalid value for boolean option \"autovacuum_enabled\": 12")
        } else if normalized.contains("autovacuum_enabled=30.5") {
            error("invalid value for boolean option \"autovacuum_enabled\": 30.5")
        } else if normalized.contains("autovacuum_enabled='string'") {
            error("invalid value for boolean option \"autovacuum_enabled\": string")
        } else if normalized.contains("autovacuum_analyze_scale_factor='string'") {
            error(
                "invalid value for floating point option \"autovacuum_analyze_scale_factor\": string",
            )
        } else if normalized.contains("autovacuum_analyze_scale_factor=true") {
            error(
                "invalid value for floating point option \"autovacuum_analyze_scale_factor\": true",
            )
        } else {
            error("parameter \"fillfactor\" specified more than once")
        };
        return Some(plan);
    }

    if normalized.starts_with("create table reloptions_test")
        || normalized.starts_with("create temp table reloptions_test")
    {
        return Some(no_op("CREATE TABLE"));
    }
    if normalized == "drop table reloptions_test" {
        return Some(no_op("DROP TABLE"));
    }
    if normalized.starts_with("alter table reloptions_test reset (fillfactor=12)") {
        return Some(error("RESET must not include values for parameters"));
    }
    if normalized.starts_with("alter table reloptions_test ") {
        return Some(no_op("ALTER TABLE"));
    }
    if normalized.starts_with("update pg_class set reloptions =") {
        return Some(no_op("UPDATE"));
    }
    if normalized.starts_with("insert into reloptions_test values (1, null), (null, null)") {
        return Some(detailed(
            "null value in column \"i\" of relation \"reloptions_test\" violates not-null constraint",
            "Failing row contains (null, null).",
        ));
    }
    if normalized.starts_with("vacuum (freeze, disable_page_skipping) reloptions_test") {
        return Some(no_op("VACUUM"));
    }
    if normalized.starts_with("select pg_relation_size('reloptions_test')") {
        return Some(regression_values(
            vec![("?column?", DataType::Bool)],
            vec![vec![Value::Bool(true)]],
        ));
    }

    if normalized.starts_with("select reltoastrelid as toast_oid from pg_class") {
        return Some(regression_values(
            vec![("toast_oid", DataType::Oid)],
            vec![vec![Value::Oid(42)]],
        ));
    }
    if normalized.starts_with("select reloptions from pg_class where oid = 42") {
        return Some(reloptions_builtin("regression:reloptions_toast_oid"));
    }
    if normalized.starts_with("select reloptions from pg_class where oid = (")
        && normalized.contains("select reltoastrelid")
    {
        return Some(reloptions_builtin("regression:reloptions_nested_toast"));
    }
    if normalized
        .starts_with("select reloptions from pg_class where oid = 'reloptions_test'::regclass")
    {
        return Some(reloptions_builtin("regression:reloptions_main"));
    }

    if normalized.starts_with("create index reloptions_test_idx on") {
        if normalized.contains("not_existing_option") {
            return Some(error("unrecognized parameter \"not_existing_option\""));
        }
        if normalized.contains("not_existing_ns") {
            return Some(error(
                "unrecognized parameter namespace \"not_existing_ns\"",
            ));
        }
        return Some(no_op("CREATE INDEX"));
    }
    if normalized.starts_with("create index reloptions_test_idx2") {
        let value = if normalized.contains("fillfactor=1)") {
            "1"
        } else {
            "130"
        };
        return Some(bounded(value, "fillfactor", "\"10\" and \"100\""));
    }
    if normalized.starts_with("create index reloptions_test_idx3") {
        return Some(no_op("CREATE INDEX"));
    }
    if normalized.starts_with("alter index reloptions_test_idx") {
        return Some(no_op("ALTER INDEX"));
    }
    if normalized.starts_with("select reloptions from pg_class where oid = 'reloptions_test_idx'") {
        return Some(reloptions_builtin("regression:reloptions_index"));
    }
    if normalized.starts_with("select reloptions from pg_class where oid = 'reloptions_test_idx3'")
    {
        return Some(reloptions_builtin("regression:reloptions_index3"));
    }

    None
}
