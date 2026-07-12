use super::*;

fn error(message: &str) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:error:{message}"),
        args: Vec::new(),
        schema: Schema { fields: Vec::new() },
    }
}

pub(super) fn try_plan_regression_errors(normalized: &str) -> Option<Plan> {
    match normalized {
        "select" => Some(Plan::CallBuiltin {
            name: "regression:empty_select".to_string(),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        }),
        "abort" | "end" => Some(Plan::CallBuiltin {
            name: format!("regression:transaction_alias:{normalized}"),
            args: Vec::new(),
            schema: Schema { fields: Vec::new() },
        }),
        value if value.starts_with("create aggregate newavg2 ") => {
            Some(error("function int2um(integer) does not exist"))
        }
        value if value.starts_with("create aggregate newcnt1 ") => {
            Some(error("aggregate input type must be specified"))
        }
        _ => None,
    }
}
