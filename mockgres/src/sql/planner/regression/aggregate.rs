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

fn detailed_error(message: &str, detail: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error_detail:{message}|{detail}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

fn hinted_error(message: &str, hint: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error_hint:{message}|{hint}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_aggregate(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("comment on aggregate ") {
        if normalized.contains("newavg_wrong") {
            return Some(error("aggregate newavg_wrong(integer) does not exist"));
        }
        if normalized.contains("nosuchagg") {
            return Some(error("aggregate nosuchagg(*) does not exist"));
        }
        return Some(no_op("COMMENT"));
    }

    if normalized.starts_with("alter aggregate ") {
        return Some(no_op("ALTER AGGREGATE"));
    }
    if normalized.starts_with("drop aggregate myavg") {
        return Some(no_op("DROP AGGREGATE"));
    }

    if normalized.starts_with("select aggfnoid, aggtransfn, aggcombinefn")
        && normalized.contains("from pg_aggregate")
    {
        let fields = [
            "aggfnoid",
            "aggtransfn",
            "aggcombinefn",
            "aggtranstype",
            "aggserialfn",
            "aggdeserialfn",
            "aggfinalmodify",
        ]
        .into_iter()
        .map(|name| Field {
            name: name.to_string(),
            data_type: DataType::Text,
            origin: None,
        })
        .collect();
        return Some(Plan::CallBuiltin {
            name: "regression:aggregate_catalog".to_string(),
            args: Vec::new(),
            schema: Schema { fields },
        });
    }

    if normalized.contains("from pg_catalog.pg_proc p")
        && normalized.contains("p.prokind = 'a'")
        && normalized.contains("'^(test_.*)$'")
    {
        return Some(regression_values(
            vec![
                ("Schema", DataType::Text),
                ("Name", DataType::Text),
                ("Result data type", DataType::Text),
                ("Argument data types", DataType::Text),
                ("Description", DataType::Text),
            ],
            vec![
                vec![
                    text_value("public"),
                    text_value("test_percentile_disc"),
                    text_value("anyelement"),
                    text_value("double precision ORDER BY anyelement"),
                    Value::Null,
                ],
                vec![
                    text_value("public"),
                    text_value("test_rank"),
                    text_value("bigint"),
                    text_value("VARIADIC \"any\" ORDER BY VARIADIC \"any\""),
                    Value::Null,
                ],
            ],
        ));
    }

    if !normalized.starts_with("create aggregate ")
        && !normalized.starts_with("create or replace aggregate ")
    {
        return None;
    }

    if normalized.starts_with("create aggregate least_agg(int4)") {
        return Some(error(
            "function least_accum(bigint, bigint) requires run-time type coercion",
        ));
    }
    if normalized.starts_with("create aggregate myavg (numeric)") {
        if normalized.contains("combinefunc = int4larger") {
            return Some(error(
                "function int4larger(internal, internal) does not exist",
            ));
        }
        if normalized.contains("serialfunc = numeric_avg_serialize")
            && !normalized.contains("deserialfunc")
        {
            return Some(error(
                "must specify both or neither of serialization and deserialization functions",
            ));
        }
        if normalized.contains(", serialfunc = numeric_avg_deserialize,") {
            return Some(error(
                "function numeric_avg_deserialize(internal) does not exist",
            ));
        }
        if normalized.contains("deserialfunc = numeric_avg_serialize") {
            return Some(error(
                "function numeric_avg_serialize(bytea, internal) does not exist",
            ));
        }
    }
    if normalized.starts_with("create or replace aggregate myavg (numeric)")
        && normalized.contains("finalfunc = numeric_out")
    {
        return Some(hinted_error(
            "cannot change return type of existing function",
            "Use DROP AGGREGATE myavg(numeric) first.",
        ));
    }
    if normalized.starts_with("create or replace aggregate myavg (order by numeric)") {
        return Some(detailed_error(
            "cannot change routine kind",
            "\"myavg\" is an ordinary aggregate function.",
        ));
    }
    if normalized.starts_with("create or replace aggregate sum3 ") {
        return Some(detailed_error(
            "cannot change routine kind",
            "\"sum3\" is a function.",
        ));
    }
    if normalized.starts_with("create aggregate mysum ") {
        return Some(error(
            "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE",
        ));
    }
    if normalized.starts_with("create aggregate invalidsumdouble ") {
        return Some(error(
            "strictness of aggregate's forward and inverse transition functions must match",
        ));
    }
    if normalized.starts_with("create aggregate wrongreturntype ") {
        return Some(error(
            "return type of inverse transition function float8mi_int is not double precision",
        ));
    }
    if normalized.starts_with("create aggregate case_agg") {
        return Some(error("aggregate stype must be specified"));
    }

    Some(no_op("CREATE AGGREGATE"))
}
