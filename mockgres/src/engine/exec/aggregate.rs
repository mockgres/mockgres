use super::*;

fn compare_rows(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (av, bv) in a.iter().zip(b.iter()) {
        match (av, bv) {
            (Value::Null, Value::Null) => continue,
            (Value::Null, _) => return Ordering::Less,
            (_, Value::Null) => return Ordering::Greater,
            _ => {
                if let Some(ord) = crate::engine::eval::compare_values(av, bv)
                    && ord != Ordering::Equal
                {
                    return ord;
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
    bool_and: Option<bool>,
    distinct_seen: Option<HashSet<Value>>,
}

impl AggState {
    fn new() -> Self {
        AggState {
            count: 0,
            sum: None,
            min: None,
            max: None,
            bool_and: None,
            distinct_seen: None,
        }
    }

    fn accepts_distinct_value(&mut self, distinct: bool, value: &Value) -> bool {
        if !distinct {
            return true;
        }
        self.distinct_seen
            .get_or_insert_with(HashSet::new)
            .insert(value.clone())
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
                                if !state.accepts_distinct_value(agg.distinct, &v) {
                                    continue;
                                }
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
                        if !state.accepts_distinct_value(agg.distinct, &v) {
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
                        if !state.accepts_distinct_value(agg.distinct, &v) {
                            continue;
                        }
                        let replace = match &state.min {
                            None => true,
                            Some(prev) => matches!(
                                crate::engine::eval::compare_values(prev, &v),
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
                        if !state.accepts_distinct_value(agg.distinct, &v) {
                            continue;
                        }
                        let replace = match &state.max {
                            None => true,
                            Some(prev) => matches!(
                                crate::engine::eval::compare_values(prev, &v),
                                Some(std::cmp::Ordering::Less)
                            ),
                        };
                        if replace {
                            state.max = Some(v);
                        }
                    }
                    AggFunc::BoolAnd => {
                        let expr = agg
                            .expr
                            .as_ref()
                            .ok_or_else(|| fe("BOOL_AND requires an expression"))?;
                        let v = eval_scalar_expr(&row, expr, &self.params, &self.ctx)?;
                        if matches!(v, Value::Null) {
                            continue;
                        }
                        if !state.accepts_distinct_value(agg.distinct, &v) {
                            continue;
                        }
                        match v {
                            Value::Bool(b) => {
                                state.bool_and = Some(state.bool_and.unwrap_or(true) && b);
                            }
                            other => {
                                return Err(fe(format!(
                                    "BOOL_AND expects boolean values, got {:?}",
                                    other
                                )));
                            }
                        }
                    }
                }
            }
        }
        self.child.close().await?;

        if map.is_empty() && self.group_exprs.is_empty() {
            map.insert(Vec::new(), vec![AggState::new(); self.agg_calls.len()]);
        }
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
                    AggFunc::BoolAnd => match state.bool_and {
                        Some(v) => Value::Bool(v),
                        None => Value::Null,
                    },
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
        let row = std::mem::take(&mut self.groups[self.pos]);

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
