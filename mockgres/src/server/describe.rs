use pgwire::api::results::{FieldFormat, FieldInfo};

use crate::engine::Plan;
use crate::server::statement_plan::StatementPlan;

pub fn plan_fields_with_format(plan: &Plan, format: FieldFormat) -> Vec<FieldInfo> {
    plan.schema()
        .fields
        .iter()
        .map(|f| FieldInfo::new(f.name.clone(), None, None, f.data_type.to_pg(), format))
        .collect()
}

pub fn statement_plan_fields(statement: &StatementPlan, format: FieldFormat) -> Vec<FieldInfo> {
    match statement {
        StatementPlan::Single(plan) => plan_fields_with_format(plan, format),
        StatementPlan::Batch(_) => vec![],
    }
}
