use crate::storage::Row;
use pgwire::error::PgWireResult;
use std::sync::Arc;

use super::eval::{eval_bool_expr, eval_scalar_expr};
use super::{BoolExpr, Expr, ScalarExpr, Schema, SortKey, Value, fe};

pub trait ExecNode: Send {
    fn open(&mut self) -> PgWireResult<()>;
    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>>;
    fn close(&mut self) -> PgWireResult<()>;
    fn schema(&self) -> &Schema;
}

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
impl ExecNode for ValuesExec {
    fn open(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
    fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct ProjectExec {
    schema: Schema,
    input: Box<dyn ExecNode>,
    exprs: Vec<ScalarExpr>,
    params: Arc<Vec<Value>>,
}
impl ProjectExec {
    pub fn new(
        schema: Schema,
        input: Box<dyn ExecNode>,
        exprs_named: Vec<(ScalarExpr, String)>,
        params: Arc<Vec<Value>>,
    ) -> Self {
        let exprs = exprs_named.into_iter().map(|(e, _)| e).collect();
        Self {
            schema,
            input,
            exprs,
            params,
        }
    }
}
impl ExecNode for ProjectExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.input.open()
    }
    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if let Some(in_row) = self.input.next()? {
            let mut out = Vec::with_capacity(self.exprs.len());
            for e in &self.exprs {
                out.push(eval_scalar_expr(&in_row, e, &self.params)?);
            }
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }
    fn close(&mut self) -> PgWireResult<()> {
        self.input.close()
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct CountExec {
    schema: Schema,
    input: Box<dyn ExecNode>,
    produced: bool,
}

impl CountExec {
    pub fn new(schema: Schema, input: Box<dyn ExecNode>) -> Self {
        Self {
            schema,
            input,
            produced: false,
        }
    }
}

impl ExecNode for CountExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.input.open()
    }

    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if self.produced {
            return Ok(None);
        }
        let mut count: i64 = 0;
        while let Some(_) = self.input.next()? {
            count += 1;
        }
        self.produced = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    fn close(&mut self) -> PgWireResult<()> {
        self.input.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct SeqScanExec {
    schema: Schema,
    rows: Vec<Vec<Value>>,
    idx: usize,
}
impl SeqScanExec {
    // simple table scan that materializes all rows.
    pub fn new(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            rows,
            idx: 0,
        }
    }
}
impl ExecNode for SeqScanExec {
    fn open(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if self.idx >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.idx].clone();
        self.idx += 1;
        Ok(Some(row))
    }
    fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct NestedLoopJoinExec {
    schema: Schema,
    rows: Vec<Row>,
    pos: usize,
}

impl NestedLoopJoinExec {
    pub fn new(
        schema: Schema,
        mut left: Box<dyn ExecNode>,
        mut right: Box<dyn ExecNode>,
    ) -> PgWireResult<Self> {
        left.open()?;
        let mut left_rows = Vec::new();
        while let Some(row) = left.next()? {
            left_rows.push(row);
        }
        left.close()?;

        right.open()?;
        let mut right_rows = Vec::new();
        while let Some(row) = right.next()? {
            right_rows.push(row);
        }
        right.close()?;

        let mut rows = Vec::with_capacity(left_rows.len() * right_rows.len());
        if !left_rows.is_empty() && !right_rows.is_empty() {
            for l in &left_rows {
                for r in &right_rows {
                    let mut combined = l.clone();
                    combined.extend(r.clone());
                    rows.push(combined);
                }
            }
        }

        Ok(Self {
            schema,
            rows,
            pos: 0,
        })
    }
}

impl ExecNode for NestedLoopJoinExec {
    fn open(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }
    fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct FilterExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    expr: BoolExpr,
    params: Arc<Vec<Value>>,
}

impl FilterExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        expr: BoolExpr,
        params: Arc<Vec<Value>>,
    ) -> Self {
        Self {
            schema,
            child,
            expr,
            params,
        }
    }
}

impl ExecNode for FilterExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.child.open()
    }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        loop {
            match self.child.next()? {
                Some(row) => {
                    let pass = eval_bool_expr(&row, &self.expr, &self.params)?.unwrap_or(false);
                    if pass {
                        return Ok(Some(row));
                    }
                }
                None => return Ok(None),
            }
        }
    }
    fn close(&mut self) -> PgWireResult<()> {
        self.child.close()
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// order exec: materializes child rows, sorts them, then yields
pub struct OrderExec {
    schema: Schema,
    rows: Vec<(Row, Vec<Value>)>,
    pos: usize,
}

struct OrderKeySpec {
    kind: OrderKeyKind,
    asc: bool,
    nulls_first: bool,
}

enum OrderKeyKind {
    Column(usize),
    Expr(usize),
}

impl OrderExec {
    pub fn new(
        schema: Schema,
        mut child: Box<dyn ExecNode>,
        keys: Vec<SortKey>,
        params: Arc<Vec<Value>>,
    ) -> PgWireResult<Self> {
        child.open()?;

        let mut expr_specs = Vec::new();
        let mut resolved_keys = Vec::with_capacity(keys.len());
        for key in keys {
            match key {
                SortKey::ByIndex {
                    idx,
                    asc,
                    nulls_first,
                } => {
                    let nulls_first_eff = nulls_first.unwrap_or(!asc);
                    resolved_keys.push(OrderKeySpec {
                        kind: OrderKeyKind::Column(idx),
                        asc,
                        nulls_first: nulls_first_eff,
                    });
                }
                SortKey::ByName {
                    col,
                    asc,
                    nulls_first,
                } => {
                    let idx = schema
                        .fields
                        .iter()
                        .position(|f| f.name == col)
                        .ok_or_else(|| fe(format!("unknown column in order by: {}", col)))?;
                    let nulls_first_eff = nulls_first.unwrap_or(!asc);
                    resolved_keys.push(OrderKeySpec {
                        kind: OrderKeyKind::Column(idx),
                        asc,
                        nulls_first: nulls_first_eff,
                    });
                }
                SortKey::Expr {
                    expr,
                    asc,
                    nulls_first,
                } => {
                    let idx = expr_specs.len();
                    expr_specs.push(expr);
                    let nulls_first_eff = nulls_first.unwrap_or(!asc);
                    resolved_keys.push(OrderKeySpec {
                        kind: OrderKeyKind::Expr(idx),
                        asc,
                        nulls_first: nulls_first_eff,
                    });
                }
            }
        }

        let mut buf = Vec::new();
        while let Some(r) = child.next()? {
            let mut expr_vals = Vec::with_capacity(expr_specs.len());
            for expr in &expr_specs {
                expr_vals.push(eval_scalar_expr(&r, expr, &params)?);
            }
            buf.push((r, expr_vals));
        }
        child.close()?;

        buf.sort_by(|(row_a, exprs_a), (row_b, exprs_b)| {
            use std::cmp::Ordering;
            for spec in &resolved_keys {
                let ord = match spec.kind {
                    OrderKeyKind::Column(idx) => {
                        let av = row_a.get(idx);
                        let bv = row_b.get(idx);
                        order_values(av, bv, spec.asc, spec.nulls_first)
                    }
                    OrderKeyKind::Expr(idx) => {
                        let av = exprs_a.get(idx).map(|v| v);
                        let bv = exprs_b.get(idx).map(|v| v);
                        order_values(av, bv, spec.asc, spec.nulls_first)
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        Ok(Self {
            schema,
            rows: buf,
            pos: 0,
        })
    }
}

impl ExecNode for OrderExec {
    fn open(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].0.clone();
        self.pos += 1;
        Ok(Some(row))
    }
    fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// compares two values with explicit asc and nulls policy.
// nulls_first=true => nulls before non-nulls; false => nulls after
fn order_values(
    a: Option<&Value>,
    b: Option<&Value>,
    asc: bool,
    nulls_first: bool,
) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;

    // treat missing outer option like sql nulls
    match (a, b) {
        (None, None) => return Equal,
        (None, Some(_)) => return if nulls_first { Less } else { Greater },
        (Some(_), None) => return if nulls_first { Greater } else { Less },
        _ => {}
    }

    // safe unwrap after early returns
    match (a.unwrap(), b.unwrap()) {
        // sql nulls obey nulls_first; do not flip by asc
        (Value::Null, Value::Null) => Equal,
        (Value::Null, _) => {
            if nulls_first {
                Less
            } else {
                Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Greater
            } else {
                Less
            }
        }

        // integers
        (Value::Int64(x), Value::Int64(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        // floats (NaN > all in ascending); desc handled by reversing ord
        (Value::Float64Bits(bx), Value::Float64Bits(by)) => {
            let (x, y) = (f64::from_bits(*bx), f64::from_bits(*by));
            let ord = if x.is_nan() && y.is_nan() {
                Equal
            } else if x.is_nan() {
                Greater
            }
            // NaN > all
            else if y.is_nan() {
                Less
            } else if x < y {
                Less
            } else if x > y {
                Greater
            } else {
                Equal
            };
            if asc { ord } else { ord.reverse() }
        }

        // int vs float coercion
        (Value::Int64(x), Value::Float64Bits(by)) => {
            let y = f64::from_bits(*by);
            let ord = if y.is_nan() {
                Less
            } else {
                let xf = *x as f64;
                if xf < y {
                    Less
                } else if xf > y {
                    Greater
                } else {
                    Equal
                }
            };
            if asc { ord } else { ord.reverse() }
        }
        (Value::Float64Bits(bx), Value::Int64(y)) => {
            let x = f64::from_bits(*bx);
            let ord = if x.is_nan() {
                Greater
            } else {
                let yf = *y as f64;
                if x < yf {
                    Less
                } else if x > yf {
                    Greater
                } else {
                    Equal
                }
            };
            if asc { ord } else { ord.reverse() }
        }

        (Value::Text(x), Value::Text(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::Bool(x), Value::Bool(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::Date(x), Value::Date(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::TimestampMicros(x), Value::TimestampMicros(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::Bytes(x), Value::Bytes(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        _ => Equal,
    }
}

// limit exec: skips `offset` rows, then forwards up to `limit` rows (if provided)
pub struct LimitExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    offset: usize,
    skipped: usize,
    remaining: Option<usize>,
}

impl LimitExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        Self {
            schema,
            child,
            offset,
            skipped: 0,
            remaining: limit,
        }
    }
}

impl ExecNode for LimitExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.child.open()
    }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        while self.skipped < self.offset {
            match self.child.next()? {
                Some(_) => self.skipped += 1,
                None => return Ok(None),
            }
        }
        if let Some(rem) = &mut self.remaining {
            if *rem == 0 {
                return Ok(None);
            }
        }
        match self.child.next()? {
            Some(r) => {
                if let Some(rem) = &mut self.remaining {
                    *rem -= 1;
                }
                Ok(Some(r))
            }
            None => Ok(None),
        }
    }
    fn close(&mut self) -> PgWireResult<()> {
        self.child.close()
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
