use crate::catalog::TableMeta;
use crate::engine::{
    Column, EvalContext, EvalMode, ScalarExpr, Value, cast_value_to_type,
    eval_scalar_expr_with_mode,
};

use super::sql_err;

pub(crate) fn eval_column_default(
    expr: &ScalarExpr,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
    ctx: &EvalContext,
) -> anyhow::Result<Value> {
    let value = eval_scalar_expr_with_mode(&[], expr, &[], ctx, EvalMode::ColumnDefault)
        .map_err(|e| sql_err("XX000", e.to_string()))?;
    coerce_value_for_column(value, col, idx, meta, ctx)
}

pub(crate) fn coerce_value_for_column(
    val: Value,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
    ctx: &EvalContext,
) -> anyhow::Result<Value> {
    if let Value::Null = val {
        if !col.nullable {
            return Err(sql_err("23502", format!("column {} is not null", col.name)));
        }
        if let Some(pk) = &meta.primary_key {
            if pk.columns.contains(&idx) {
                return Err(sql_err(
                    "23502",
                    format!("primary key column {} cannot be null", col.name),
                ));
            }
        }
        return Ok(Value::Null);
    }
    let coerced = cast_value_to_type(val, &col.data_type, &ctx.time_zone).map_err(|e| {
        sql_err(
            e.code,
            format!("column {} (index {}): {}", col.name, idx, e.message),
        )
    })?;
    Ok(coerced)
}
