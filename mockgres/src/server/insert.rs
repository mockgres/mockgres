use pgwire::error::PgWireResult;

use crate::db::CellInput;
use crate::engine::{InsertSource, Value, eval_scalar_expr};

pub fn evaluate_insert_source(src: &InsertSource, params: &[Value]) -> PgWireResult<CellInput> {
    match src {
        InsertSource::Default => Ok(CellInput::Default),
        InsertSource::Expr(expr) => {
            let value = eval_scalar_expr(&[], expr, params)?;
            Ok(CellInput::Value(value))
        }
    }
}
