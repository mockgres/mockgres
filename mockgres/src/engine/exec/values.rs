use async_trait::async_trait;
use pgwire::error::PgWireResult;

use crate::engine::{Expr, Schema, Value, fe};

use super::ExecNode;

pub struct ValuesExec {
    schema: Schema,
    rows: Vec<Vec<Value>>,
    idx: usize,
}

impl ValuesExec {
    pub fn new(schema: Schema, rows_expr: Vec<Vec<Expr>>) -> PgWireResult<Self> {
        let mut rows = Vec::with_capacity(rows_expr.len());
        for r in rows_expr {
            let mut out = Vec::with_capacity(r.len());
            for e in r {
                out.push(eval_const(&e)?);
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
        let row = self.rows[self.idx].clone();
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

fn eval_const(e: &Expr) -> PgWireResult<Value> {
    match e {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(_) => Err(fe("column not allowed here")),
    }
}
