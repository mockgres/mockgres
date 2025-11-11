use pgwire::error::PgWireResult;

use crate::db::CellInput;
use crate::engine::{EvalContext, InsertSource, Value, eval_scalar_expr};

pub fn evaluate_insert_source(
    src: &InsertSource,
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<CellInput> {
    match src {
        InsertSource::Default => Ok(CellInput::Default),
        InsertSource::Expr(expr) => {
            let value = eval_scalar_expr(&[], expr, params, ctx)?;
            Ok(CellInput::Value(value))
        }
    }
}
