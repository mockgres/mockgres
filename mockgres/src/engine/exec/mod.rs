use crate::storage::Row;
use async_trait::async_trait;
use pgwire::error::PgWireResult;
use std::sync::Arc;

use super::eval::{EvalContext, eval_bool_expr, eval_scalar_expr};
use super::{AggCall, AggFunc, BoolExpr, JoinType, ScalarExpr, Schema, SortKey, Value, fe};

mod values;

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
        while let Some(_) = self.input.next().await? {
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
pub struct OrderExec {
    schema: Schema,
    child: Option<Box<dyn ExecNode>>,
    rows: Vec<(Row, Vec<Value>)>,
    pos: usize,
    resolved_keys: Vec<OrderKeySpec>,
    expr_specs: Vec<ScalarExpr>,
    params: Arc<Vec<Value>>,
    sorted: bool,
    ctx: EvalContext,
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
        child: Box<dyn ExecNode>,
        keys: Vec<SortKey>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> PgWireResult<Self> {
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

        Ok(Self {
            schema,
            child: Some(child),
            rows: Vec::new(),
            pos: 0,
            resolved_keys,
            expr_specs,
            params,
            sorted: false,
            ctx,
        })
    }

    async fn ensure_sorted(&mut self) -> PgWireResult<()> {
        if self.sorted {
            return Ok(());
        }
        let mut child = self.child.take().expect("order child missing");
        child.open().await?;

        let mut buf = Vec::new();
        while let Some(r) = child.next().await? {
            let mut expr_vals = Vec::with_capacity(self.expr_specs.len());
            for expr in &self.expr_specs {
                expr_vals.push(eval_scalar_expr(&r, expr, &self.params, &self.ctx)?);
            }
            buf.push((r, expr_vals));
        }
        child.close().await?;

        buf.sort_by(|(row_a, exprs_a), (row_b, exprs_b)| {
            use std::cmp::Ordering;
            for spec in &self.resolved_keys {
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

        self.rows = buf;
        self.sorted = true;
        Ok(())
    }
}

#[async_trait]
impl ExecNode for OrderExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.ensure_sorted().await
    }
    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        if !self.sorted {
            self.ensure_sorted().await?;
        }
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].0.clone();
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

// compares two values with explicit asc and nulls policy.
// nulls_first=true => nulls before non-nulls
// false => nulls after
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
        // sql nulls obey nulls_first
        // do not flip by asc
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

        // floats nan > all in ascending desc handled by reversing ord
        (Value::Float64Bits(bx), Value::Float64Bits(by)) => {
            let (x, y) = (f64::from_bits(*bx), f64::from_bits(*by));
            let ord = if x.is_nan() && y.is_nan() {
                Equal
            } else if x.is_nan() {
                Greater
            }
            // nan > all
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

fn compare_rows(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (av, bv) in a.iter().zip(b.iter()) {
        match (av, bv) {
            (Value::Null, Value::Null) => continue,
            (Value::Null, _) => return Ordering::Less,
            (_, Value::Null) => return Ordering::Greater,
            _ => {
                if let Some(ord) = super::eval::compare_values(av, bv) {
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
            }
        }
    }
    a.len().cmp(&b.len())
}

fn eval_numeric_add(a: Value, b: Value) -> PgWireResult<Value> {
    use Value::*;
    match (a, b) {
        (Int64(x), Int64(y)) => Ok(Int64(x + y)),
        (Float64Bits(bx), Float64Bits(by)) => {
            Ok(Value::from_f64(f64::from_bits(bx) + f64::from_bits(by)))
        }
        (Int64(x), Float64Bits(by)) => Ok(Value::from_f64((x as f64) + f64::from_bits(by))),
        (Float64Bits(bx), Int64(y)) => Ok(Value::from_f64(f64::from_bits(bx) + (y as f64))),
        (other_a, other_b) => Err(fe(format!(
            "numeric add unsupported for {other_a:?} + {other_b:?}"
        ))),
    }
}

fn eval_numeric_div_by_i64(sum: Value, denom: i64) -> PgWireResult<Value> {
    use Value::*;
    if denom == 0 {
        return Err(fe("division by zero in AVG"));
    }
    match sum {
        Int64(x) => Ok(Value::from_f64((x as f64) / (denom as f64))),
        Float64Bits(bits) => Ok(Value::from_f64(f64::from_bits(bits) / (denom as f64))),
        other => Err(fe(format!("AVG() unsupported for {other:?}"))),
    }
}

// skips num offset rows, then forwards up to num limit rows if provided
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
        if let Some(rem) = &mut self.remaining {
            if *rem == 0 {
                return Ok(None);
            }
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

pub struct HashAggregateExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    group_exprs: Vec<ScalarExpr>,
    agg_calls: Vec<AggCall>,
    params: Arc<Vec<Value>>,
    ctx: EvalContext,
    groups: Vec<Row>,
    pos: usize,
    built: bool,
}

#[derive(Clone)]
struct AggState {
    count: i64,
    sum: Option<Value>,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggState {
    fn new() -> Self {
        AggState {
            count: 0,
            sum: None,
            min: None,
            max: None,
        }
    }
}

impl HashAggregateExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        group_exprs_named: Vec<(ScalarExpr, String)>,
        agg_exprs_named: Vec<(AggCall, String)>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> Self {
        let group_exprs = group_exprs_named.into_iter().map(|(e, _)| e).collect();
        let agg_calls = agg_exprs_named.into_iter().map(|(a, _)| a).collect();
        Self {
            schema,
            child,
            group_exprs,
            agg_calls,
            params,
            ctx,
            groups: Vec::new(),
            pos: 0,
            built: false,
        }
    }

    async fn ensure_built(&mut self) -> PgWireResult<()> {
        if self.built {
            return Ok(());
        }

        use std::collections::HashMap;

        let mut map: HashMap<Vec<Value>, Vec<AggState>> = HashMap::new();

        self.child.open().await?;
        while let Some(row) = self.child.next().await? {
            let mut key = Vec::with_capacity(self.group_exprs.len());
            for expr in &self.group_exprs {
                let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                key.push(v);
            }

            let entry = map
                .entry(key)
                .or_insert_with(|| vec![AggState::new(); self.agg_calls.len()]);

            for (i, agg) in self.agg_calls.iter().enumerate() {
                let state = &mut entry[i];
                match agg.func {
                    AggFunc::Count => {
                        if let Some(expr) = &agg.expr {
                            let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                            if !matches!(v, Value::Null) {
                                state.count += 1;
                            }
                        } else {
                            state.count += 1;
                        }
                    }
                    AggFunc::Sum | AggFunc::Avg => {
                        let expr = agg
                            .expr
                            .as_ref()
                            .ok_or_else(|| fe("SUM/AVG require an expression"))?;
                        let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                        if matches!(v, Value::Null) {
                            continue;
                        }
                        state.count += 1;
                        state.sum = Some(match state.sum.take() {
                            None => v,
                            Some(prev) => eval_numeric_add(prev, v)?,
                        });
                    }
                    AggFunc::Min => {
                        let expr = agg
                            .expr
                            .as_ref()
                            .ok_or_else(|| fe("MIN requires an expression"))?;
                        let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                        if matches!(v, Value::Null) {
                            continue;
                        }
                        let replace = match &state.min {
                            None => true,
                            Some(prev) => matches!(
                                super::eval::compare_values(prev, &v),
                                Some(std::cmp::Ordering::Greater)
                            ),
                        };
                        if replace {
                            state.min = Some(v);
                        }
                    }
                    AggFunc::Max => {
                        let expr = agg
                            .expr
                            .as_ref()
                            .ok_or_else(|| fe("MAX requires an expression"))?;
                        let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                        if matches!(v, Value::Null) {
                            continue;
                        }
                        let replace = match &state.max {
                            None => true,
                            Some(prev) => matches!(
                                super::eval::compare_values(prev, &v),
                                Some(std::cmp::Ordering::Less)
                            ),
                        };
                        if replace {
                            state.max = Some(v);
                        }
                    }
                }
            }
        }
        self.child.close().await?;

        let mut groups = Vec::with_capacity(map.len());
        for (group_key, states) in map {
            let mut out = group_key;
            for (agg, state) in self.agg_calls.iter().zip(states.into_iter()) {
                let value = match agg.func {
                    AggFunc::Count => Value::Int64(state.count),
                    AggFunc::Sum => state.sum.unwrap_or(Value::Null),
                    AggFunc::Avg => {
                        if state.count == 0 {
                            Value::Null
                        } else if let Some(sum) = state.sum {
                            eval_numeric_div_by_i64(sum, state.count)?
                        } else {
                            Value::Null
                        }
                    }
                    AggFunc::Min => state.min.unwrap_or(Value::Null),
                    AggFunc::Max => state.max.unwrap_or(Value::Null),
                };
                out.push(value);
            }
            groups.push(out);
        }

        groups.sort_by(|a, b| compare_rows(a, b));

        self.groups = groups;
        self.pos = 0;
        self.built = true;
        Ok(())
    }
}

#[async_trait]
impl ExecNode for HashAggregateExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.ensure_built().await
    }

    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        if !self.built {
            self.ensure_built().await?;
        }
        if self.pos >= self.groups.len() {
            return Ok(None);
        }
        let row = self.groups[self.pos].clone();

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
