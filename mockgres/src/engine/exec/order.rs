use super::*;

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

#[derive(Clone)]
pub(super) struct OrderKeySpec {
    pub(super) kind: OrderKeyKind,
    pub(super) asc: bool,
    pub(super) nulls_first: bool,
}

#[derive(Clone)]
pub(super) enum OrderKeyKind {
    Column(usize),
    Expr(usize),
}

pub(super) fn resolve_order_keys(
    schema: &Schema,
    keys: Vec<SortKey>,
) -> PgWireResult<(Vec<OrderKeySpec>, Vec<ScalarExpr>)> {
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
    Ok((resolved_keys, expr_specs))
}

impl OrderExec {
    pub fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        keys: Vec<SortKey>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> PgWireResult<Self> {
        let (resolved_keys, expr_specs) = resolve_order_keys(&schema, keys)?;

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
                        let av = exprs_a.get(idx);
                        let bv = exprs_b.get(idx);
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

pub(super) fn compare_window_entries(
    row_a: &Row,
    exprs_a: &[Value],
    row_b: &Row,
    exprs_b: &[Value],
    keys: &[OrderKeySpec],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for spec in keys {
        let ord = match spec.kind {
            OrderKeyKind::Column(idx) => {
                let av = row_a.get(idx);
                let bv = row_b.get(idx);
                order_values(av, bv, spec.asc, spec.nulls_first)
            }
            OrderKeyKind::Expr(idx) => {
                let av = exprs_a.get(idx);
                let bv = exprs_b.get(idx);
                order_values(av, bv, spec.asc, spec.nulls_first)
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
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
            else if y.is_nan() || x < y {
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

        (Value::PgLsn(x), Value::PgLsn(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::MacAddr(x), Value::MacAddr(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        (Value::MacAddr8(x), Value::MacAddr8(y)) => {
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

        (Value::TimeMicros(x), Value::TimeMicros(y)) => {
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
