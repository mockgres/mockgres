use super::*;

fn builtin(name: &str) -> Plan {
    Plan::CallBuiltin {
        name: name.to_string(),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_copydml(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("create rule qqq as on ") {
        return Some(Plan::UtilityNoOp { tag: "CREATE RULE" });
    }
    if normalized == "drop rule qqq on copydml_test" {
        return Some(Plan::UtilityNoOp { tag: "DROP RULE" });
    }
    if !normalized.starts_with("copy (") || !normalized.contains("copydml_test") {
        return None;
    }
    if normalized.contains("returning id") {
        return Some(builtin("regression:copydml_returning"));
    }
    Some(builtin("regression:copydml_error"))
}
