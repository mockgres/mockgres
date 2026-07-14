use async_trait::async_trait;
use pgwire::error::PgWireResult;
use std::sync::Arc;

use crate::engine::{Expr, Schema, Value, fe};

use super::super::eval::{EvalContext, eval_scalar_expr};
use super::ExecNode;

pub struct ValuesExec {
    schema: Schema,
    rows: Vec<Vec<Value>>,
    idx: usize,
}

impl ValuesExec {
    pub fn new(schema: Schema, rows_expr: Vec<Vec<Expr>>) -> PgWireResult<Self> {
        Self::new_with_context(
            schema,
            rows_expr,
            Arc::new(Vec::new()),
            EvalContext::default(),
        )
    }

    pub fn new_with_context(
        schema: Schema,
        rows_expr: Vec<Vec<Expr>>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> PgWireResult<Self> {
        let mut rows = Vec::with_capacity(rows_expr.len());
        for r in rows_expr {
            let mut out = Vec::with_capacity(r.len());
            for e in r {
                out.push(eval_const(&e, &params, &ctx)?);
            }
            rows.push(out);
        }
        Ok(Self {
            schema,
            rows,
            idx: 0,
        })
    }

    pub fn from_values(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            rows,
            idx: 0,
        }
    }
}

#[async_trait]
impl ExecNode for ValuesExec {
    async fn open(&mut self) -> PgWireResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = std::mem::take(&mut self.rows[self.idx]);
        self.idx += 1;
        Ok(Some(row))
    }

    async fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn eval_const(e: &Expr, params: &[Value], ctx: &EvalContext) -> PgWireResult<Value> {
    match e {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(_) => Err(fe("column not allowed here")),
        Expr::Scalar(expr) => eval_scalar_expr(&[], expr, params, ctx),
    }
}
