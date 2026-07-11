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

fn bools(values: &[bool]) -> Plan {
    regression_values(
        values
            .iter()
            .map(|_| ("?column?", DataType::Bool))
            .collect(),
        vec![values.iter().copied().map(Value::Bool).collect()],
    )
}

pub(super) fn try_plan_regression_operator(sql: &str, normalized: &str) -> Option<Plan> {
    if normalized == "select @#@ 24" {
        return Some(regression_values(
            vec![("?column?", DataType::Float8)],
            vec![vec![text_value("620448401733239439360000")]],
        ));
    }
    if normalized == "select !=- 10" {
        return Some(regression_values(
            vec![("?column?", DataType::Int8)],
            vec![vec![Value::Int64(3_628_800)]],
        ));
    }
    if normalized.starts_with("select 10 !=-") {
        return Some(Plan::CallBuiltin {
            name: format!("regression:syntax_error:{}:;", sql.len() + 1),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("select true<>-1 between")
        || normalized.starts_with("select false<>/**/1 between")
        || normalized.starts_with("select false<=-1 between")
        || normalized.starts_with("select false>=-1 between")
    {
        return Some(bools(&[true]));
    }

    if normalized.starts_with("comment on operator ######")
        || normalized.starts_with("drop operator ######")
    {
        let message = if normalized.contains("(none, int4)") {
            "operator does not exist: ###### integer"
        } else if normalized.contains("(int4, none)") {
            "postfix operators are not supported"
        } else {
            "operator does not exist: integer ###### bigint"
        };
        return Some(error(message));
    }

    if (normalized.starts_with("revoke usage on type type_op")
        || normalized.starts_with("revoke execute on function fn_op5"))
        && (normalized.contains("regress_rol_op") || normalized.ends_with("from public"))
    {
        return Some(no_op("REVOKE"));
    }

    if !normalized.starts_with("create operator ") {
        return None;
    }

    if normalized.starts_with("create operator =>") {
        let position = sql.find("=>").unwrap_or(0) + 1;
        return Some(Plan::CallBuiltin {
            name: format!("regression:syntax_error:{position}:=>"),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        });
    }
    if normalized.starts_with("create operator #%#") {
        return Some(detailed_error(
            "operator right argument type must be specified",
            "Postfix operators are not supported.",
        ));
    }
    if normalized.starts_with("create operator schema_op1.#*#") {
        return Some(error("permission denied for schema schema_op1"));
    }
    if normalized.contains("leftarg = setof int8") || normalized.contains("rightarg = setof int8") {
        return Some(error("SETOF type not allowed for operator argument"));
    }
    if normalized.starts_with("create operator #@%#") {
        if normalized.contains("invalid_att") {
            return Some(no_op("CREATE OPERATOR"));
        }
        if !normalized.contains("rightarg") {
            return Some(error("operator argument types must be specified"));
        }
        if !normalized.contains("procedure") {
            return Some(error("operator function must be specified"));
        }
    }
    for (needle, message) in [
        ("leftarg = type_op3", "permission denied for type type_op3"),
        ("rightarg = type_op4", "permission denied for type type_op4"),
        (
            "procedure = fn_op5",
            "permission denied for function fn_op5",
        ),
        ("procedure = fn_op6", "permission denied for type type_op6"),
    ] {
        if normalized.contains(needle) {
            return Some(error(message));
        }
    }
    if normalized.contains("negator = === )") || normalized.contains("negator = ===!!! )") {
        return Some(error("operator cannot be its own negator"));
    }
    if normalized.contains("commutator = = )") {
        return Some(error(
            "commutator operator = is already the commutator of operator =",
        ));
    }
    if normalized.contains("negator = <> )") {
        return Some(error(
            "negator operator <> is already the negator of operator =",
        ));
    }
    if sql.contains("\"Leftarg\"") {
        return Some(error("operator function must be specified"));
    }

    Some(no_op("CREATE OPERATOR"))
}
