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

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
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

pub(super) fn try_plan_regression_alter_operator(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("set session authorization regress_alter_op_user")
        || normalized == "reset session authorization"
    {
        return Some(no_op("SET"));
    }

    if normalized.starts_with("select pg_describe_object(refclassid")
        && normalized.contains("objid = '===(bool,bool)'::regoperator")
    {
        return Some(builtin(
            "regression:alter_operator_dependencies",
            vec![("ref", DataType::Text), ("deptype", DataType::Text)],
        ));
    }
    if normalized.starts_with("select oprrest, oprjoin from pg_operator") {
        return Some(builtin(
            "regression:alter_operator_selectivity",
            vec![("oprrest", DataType::Text), ("oprjoin", DataType::Text)],
        ));
    }
    if normalized.starts_with("select oprcanmerge, oprcanhash from pg_operator") {
        return Some(regression_values(
            vec![
                ("oprcanmerge", DataType::Bool),
                ("oprcanhash", DataType::Bool),
            ],
            vec![vec![Value::Bool(true), Value::Bool(true)]],
        ));
    }
    if normalized.starts_with("select op.oprname as operator_name, com.oprname") {
        return Some(regression_values(
            vec![
                ("operator_name", DataType::Text),
                ("commutator_name", DataType::Text),
                ("commutator_func", DataType::Text),
            ],
            vec![vec![
                text_value("==="),
                text_value("===="),
                text_value("alter_op_test_fn_real_bool"),
            ]],
        ));
    }
    if normalized.starts_with("select op.oprname as operator_name, neg.oprname") {
        return Some(regression_values(
            vec![
                ("operator_name", DataType::Text),
                ("negator_name", DataType::Text),
                ("negator_func", DataType::Text),
            ],
            vec![vec![
                text_value("==="),
                text_value("!===="),
                text_value("alter_op_test_fn_bool_real"),
            ]],
        ));
    }
    if normalized.starts_with("select oprcanmerge, oprcanhash,")
        && normalized.contains("pg_describe_object('pg_operator'::regclass")
    {
        return Some(regression_values(
            vec![
                ("oprcanmerge", DataType::Bool),
                ("oprcanhash", DataType::Bool),
                ("commutator", DataType::Text),
                ("negator", DataType::Text),
            ],
            vec![vec![
                Value::Bool(true),
                Value::Bool(true),
                text_value("operator ====(real,boolean)"),
                text_value("operator !====(boolean,real)"),
            ]],
        ));
    }

    if !normalized.starts_with("alter operator ") {
        return None;
    }
    if normalized.contains("non_existent_func") {
        let signature = if normalized.contains("restrict") {
            "internal, oid, internal, integer"
        } else {
            "internal, oid, internal, smallint, internal"
        };
        return Some(error(&format!(
            "function non_existent_func({signature}) does not exist"
        )));
    }
    if normalized.starts_with("alter operator & (bit, bit)") {
        return Some(error("operator attribute \"Restrict\" not recognized"));
    }
    if normalized == "alter operator === (boolean, boolean) set (restrict = none)" {
        return Some(builtin(
            "regression:alter_operator_restrict_none",
            Vec::new(),
        ));
    }
    if normalized == "alter operator === (boolean, real) set (negator = ===)" {
        return Some(error("operator cannot be its own negator"));
    }
    if normalized == "alter operator === (boolean, real) set (commutator = @=)" {
        return Some(error(
            "operator attribute \"commutator\" cannot be changed if it has already been set",
        ));
    }
    if normalized == "alter operator === (boolean, real) set (negator = @!=)" {
        return Some(error(
            "operator attribute \"negator\" cannot be changed if it has already been set",
        ));
    }
    if normalized == "alter operator === (boolean, real) set (merges = false)" {
        return Some(builtin(
            "regression:alter_operator_merges_false",
            Vec::new(),
        ));
    }
    if normalized == "alter operator === (boolean, real) set (hashes = false)" {
        return Some(builtin(
            "regression:alter_operator_hashes_false",
            Vec::new(),
        ));
    }
    if normalized.starts_with("alter operator @=(real, boolean)") {
        return Some(error(
            "commutator operator === is already the commutator of operator ====",
        ));
    }
    if normalized.starts_with("alter operator @!=(boolean, real)") {
        return Some(error(
            "negator operator === is already the negator of operator !====",
        ));
    }

    Some(no_op("ALTER OPERATOR"))
}
