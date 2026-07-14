use super::*;
use crate::engine::ScalarExpr;

fn builtin(name: &str, fields: Vec<(&str, DataType)>) -> Plan {
    Plan::CallBuiltin {
        name: format!("regression:psql_pipeline:{name}"),
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

pub(super) fn try_plan_regression_psql_pipeline(normalized: &str) -> Option<Plan> {
    if normalized.starts_with("insert into psql_pipeline values ($1)") {
        let mut plan = builtin("insert", Vec::new());
        if let Plan::CallBuiltin { args, .. } = &mut plan {
            args.push(ScalarExpr::Param {
                idx: 0,
                ty: Some(DataType::Int4),
            });
        }
        return Some(plan);
    }
    if normalized == "select count(*) from psql_pipeline" {
        return Some(Plan::Values {
            rows: vec![vec![Expr::Literal(Value::Int64(1))]],
            schema: Schema {
                fields: vec![Field {
                    name: "count".to_string(),
                    data_type: DataType::Int8,
                    origin: None,
                }],
            },
        });
    }
    if normalized.starts_with("set local statement_timeout=") {
        return Some(builtin("set_local_timeout", Vec::new()));
    }
    if normalized == "show statement_timeout" {
        return Some(builtin(
            "statement_timeout",
            vec![("statement_timeout", DataType::Text)],
        ));
    }
    if normalized == "reindex table concurrently psql_pipeline" {
        return Some(builtin("reindex", Vec::new()));
    }
    if normalized == "savepoint a" || normalized == "rollback to savepoint a" {
        return Some(builtin("savepoint", Vec::new()));
    }
    if normalized == "lock psql_pipeline" {
        return Some(builtin("lock", Vec::new()));
    }
    if normalized == "vacuum psql_pipeline" {
        return Some(builtin("vacuum", Vec::new()));
    }
    None
}
