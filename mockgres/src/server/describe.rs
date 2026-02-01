use pgwire::api::results::{FieldFormat, FieldInfo};

use crate::engine::Plan;

pub fn plan_fields(plan: &Plan) -> Vec<FieldInfo> {
    plan_fields_with_format(plan, FieldFormat::Text)
}

pub fn plan_fields_with_format(plan: &Plan, format: FieldFormat) -> Vec<FieldInfo> {
    plan.schema()
        .fields
        .iter()
        .map(|f| {
            FieldInfo::new(
                f.name.clone(),
                None,
                None,
                f.data_type.to_pg(),
                format,
            )
        })
        .collect()
}
