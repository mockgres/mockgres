use crate::storage::Row;
use async_trait::async_trait;
use pgwire::error::PgWireResult;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::eval::{EvalContext, eval_bool_expr, eval_scalar_expr};
use super::{
    AggCall, AggFunc, BoolExpr, JoinType, ScalarExpr, Schema, SortKey, Value, WindowSpec, fe,
};
mod aggregate;
mod order;

mod values;

pub use aggregate::HashAggregateExec;
pub use order::OrderExec;
use order::{OrderKeySpec, compare_window_entries, resolve_order_keys};

pub use values::ValuesExec;

#[async_trait]
pub trait ExecNode: Send {
    async fn open(&mut self) -> PgWireResult<()>;
    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>>;
    async fn close(&mut self) -> PgWireResult<()>;
    fn schema(&self) -> &Schema;
}

pub struct ProjectExec {
    schema: Schema,
    input: Box<dyn ExecNode>,
    exprs: Vec<ScalarExpr>,
    params: Arc<Vec<Value>>,
    ctx: EvalContext,
}
impl ProjectExec {
    pub fn new(
        schema: Schema,
        input: Box<dyn ExecNode>,
        exprs_named: Vec<(ScalarExpr, String)>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> Self {
        let exprs = exprs_named.into_iter().map(|(e, _)| e).collect();
        Self {
            schema,
            input,
            exprs,
            params,
            ctx,
        }
    }
}
#[async_trait]
impl ExecNode for ProjectExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.input.open().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if let Some(in_row) = self.input.next().await? {
            let mut out = Vec::with_capacity(self.exprs.len());
            for e in &self.exprs {
                out.push(eval_scalar_expr(&in_row, e, &self.params, &self.ctx)?);
            }
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }
    async fn close(&mut self) -> PgWireResult<()> {
        self.input.close().await
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

pub struct WindowRowNumberExec {
    schema: Schema,
    child: Option<Box<dyn ExecNode>>,
    specs: Vec<ResolvedWindowSpec>,
    params: Arc<Vec<Value>>,
    ctx: EvalContext,
    rows: Vec<Row>,
    pos: usize,
    built: bool,
}

struct ResolvedWindowSpec {
    partition_by: Vec<ScalarExpr>,
    order_keys: Vec<OrderKeySpec>,
    order_exprs: Vec<ScalarExpr>,
}

impl WindowRowNumberExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        specs: Vec<(WindowSpec, String)>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> PgWireResult<Self> {
        let child_schema = child.schema().clone();
        let mut resolved = Vec::with_capacity(specs.len());
        for (spec, _) in specs {
            let (order_keys, order_exprs) = resolve_order_keys(&child_schema, spec.order_by)?;
            resolved.push(ResolvedWindowSpec {
                partition_by: spec.partition_by,
                order_keys,
                order_exprs,
            });
        }
        Ok(Self {
            schema,
            child: Some(child),
            specs: resolved,
            params,
            ctx,
            rows: Vec::new(),
            pos: 0,
            built: false,
        })
    }

    async fn ensure_built(&mut self) -> PgWireResult<()> {
        if self.built {
            return Ok(());
        }
        let mut child = self.child.take().expect("window child missing");
        child.open().await?;
        let mut rows = Vec::new();
        while let Some(row) = child.next().await? {
            rows.push(row);
        }
        child.close().await?;

        let base_width = rows
            .first()
            .map(|r| r.len())
            .unwrap_or_else(|| self.schema.fields.len().saturating_sub(self.specs.len()));
        for row in &mut rows {
            row.resize(base_width + self.specs.len(), Value::Null);
        }

        for (spec_idx, spec) in self.specs.iter().enumerate() {
            let mut partitions: HashMap<Vec<Value>, Vec<(usize, Vec<Value>)>> = HashMap::new();
            for (row_idx, row) in rows.iter().enumerate() {
                let mut partition_key = Vec::with_capacity(spec.partition_by.len());
                for expr in &spec.partition_by {
                    partition_key.push(eval_scalar_expr(row, expr, &self.params, &self.ctx)?);
                }
                let mut order_vals = Vec::with_capacity(spec.order_exprs.len());
                for expr in &spec.order_exprs {
                    order_vals.push(eval_scalar_expr(row, expr, &self.params, &self.ctx)?);
                }
                partitions
                    .entry(partition_key)
                    .or_default()
                    .push((row_idx, order_vals));
            }

            for entries in partitions.values_mut() {
                entries.sort_by(|(idx_a, exprs_a), (idx_b, exprs_b)| {
                    compare_window_entries(
                        &rows[*idx_a],
                        exprs_a,
                        &rows[*idx_b],
                        exprs_b,
                        &spec.order_keys,
                    )
                });
                for (rank, (row_idx, _)) in entries.iter().enumerate() {
                    rows[*row_idx][base_width + spec_idx] = Value::Int64((rank + 1) as i64);
                }
            }
        }

        self.rows = rows;
        self.pos = 0;
        self.built = true;
        Ok(())
    }
}

#[async_trait]
impl ExecNode for WindowRowNumberExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.ensure_built().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        if !self.built {
            self.ensure_built().await?;
        }
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }
    async fn close(&mut self) -> PgWireResult<()> {
        Ok(())
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
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

#[async_trait]
impl ExecNode for CountExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.input.open().await
    }

    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        if self.produced {
            return Ok(None);
        }
        let mut count: i64 = 0;
        while self.input.next().await?.is_some() {
            count += 1;
        }
        self.produced = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    async fn close(&mut self) -> PgWireResult<()> {
        self.input.close().await
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
#[async_trait]
impl ExecNode for SeqScanExec {
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

pub struct NestedLoopJoinExec {
    schema: Schema,
    left: Option<Box<dyn ExecNode>>,
    right: Option<Box<dyn ExecNode>>,
    rows: Vec<Row>,
    right_width: usize,
    join_type: JoinType,
    on: Option<BoolExpr>,
    params: Arc<Vec<Value>>,
    ctx: EvalContext,
    pos: usize,
    built: bool,
}

const MAX_NESTED_LOOP_CANDIDATES: usize = 1_000_000;

impl NestedLoopJoinExec {
    pub fn new(
        schema: Schema,
        left: Box<dyn ExecNode>,
        right: Box<dyn ExecNode>,
        join_type: JoinType,
        on: Option<BoolExpr>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> Self {
        let right_width = right.schema().fields.len();
        Self {
            schema,
            left: Some(left),
            right: Some(right),
            rows: Vec::new(),
            right_width,
            join_type,
            on,
            params,
            ctx,
            pos: 0,
            built: false,
        }
    }

    async fn ensure_materialized(&mut self) -> PgWireResult<()> {
        if self.built {
            return Ok(());
        }
        let mut left = self.left.take().expect("left exec missing");
        let mut right = self.right.take().expect("right exec missing");
        left.open().await?;
        let mut left_rows = Vec::new();
        while let Some(row) = left.next().await? {
            left_rows.push(row);
        }
        left.close().await?;

        right.open().await?;
        let mut right_rows = Vec::new();
        while let Some(row) = right.next().await? {
            right_rows.push(row);
        }
        right.close().await?;

        let candidate_count = left_rows.len().saturating_mul(right_rows.len());
        if candidate_count > MAX_NESTED_LOOP_CANDIDATES {
            return Err(crate::engine::fe_code(
                "54000",
                format!(
                    "nested-loop join would examine {candidate_count} row pairs; limit is {MAX_NESTED_LOOP_CANDIDATES}"
                ),
            ));
        }

        let mut rows = Vec::new();
        match self.join_type {
            JoinType::Inner => {
                rows.reserve(left_rows.len().saturating_mul(right_rows.len()));
                for l in &left_rows {
                    for r in &right_rows {
                        let mut combined = l.clone();
                        combined.extend(r.clone());
                        if let Some(on_expr) = &self.on {
                            let pass = eval_bool_expr(&combined, on_expr, &self.params, &self.ctx)?
                                .unwrap_or(false);
                            if !pass {
                                continue;
                            }
                        }
                        rows.push(combined);
                    }
                }
            }
            JoinType::Left => {
                let mut null_right = Vec::with_capacity(self.right_width);
                null_right.resize(self.right_width, Value::Null);
                let estimated = left_rows.len().saturating_mul(right_rows.len().max(1));
                rows.reserve(estimated);
                for l in &left_rows {
                    let mut matched = false;
                    if right_rows.is_empty() {
                        let mut combined = l.clone();
                        combined.extend(null_right.clone());
                        rows.push(combined);
                        continue;
                    }
                    for r in &right_rows {
                        let mut combined = l.clone();
                        combined.extend(r.clone());
                        if let Some(on_expr) = &self.on {
                            let pass = eval_bool_expr(&combined, on_expr, &self.params, &self.ctx)?
                                .unwrap_or(false);
                            if !pass {
                                continue;
                            }
                        }
                        matched = true;
                        rows.push(combined);
                    }
                    if !matched {
                        let mut combined = l.clone();
                        combined.extend(null_right.clone());
                        rows.push(combined);
                    }
                }
            }
        }

        self.rows = rows;
        self.built = true;
        Ok(())
    }
}

#[async_trait]
impl ExecNode for NestedLoopJoinExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.ensure_materialized().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        if !self.built {
            self.ensure_materialized().await?;
        }
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }
    async fn close(&mut self) -> PgWireResult<()> {
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
    ctx: EvalContext,
}

impl FilterExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        expr: BoolExpr,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> Self {
        Self {
            schema,
            child,
            expr,
            params,
            ctx,
        }
    }
}

#[async_trait]
impl ExecNode for FilterExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.child.open().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        loop {
            match self.child.next().await? {
                Some(row) => {
                    let pass =
                        eval_bool_expr(&row, &self.expr, &self.params, &self.ctx)?.unwrap_or(false);
                    if pass {
                        return Ok(Some(row));
                    }
                }
                None => return Ok(None),
            }
        }
    }
    async fn close(&mut self) -> PgWireResult<()> {
        self.child.close().await
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// order exec: materializes child rows, sorts them, then yields
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

#[async_trait]
impl ExecNode for LimitExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.child.open().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        while self.skipped < self.offset {
            match self.child.next().await? {
                Some(_) => self.skipped += 1,
                None => return Ok(None),
            }
        }
        if let Some(rem) = &mut self.remaining
            && *rem == 0
        {
            return Ok(None);
        }
        match self.child.next().await? {
            Some(r) => {
                if let Some(rem) = &mut self.remaining {
                    *rem -= 1;
                }
                Ok(Some(r))
            }
            None => Ok(None),
        }
    }
    async fn close(&mut self) -> PgWireResult<()> {
        self.child.close().await
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
