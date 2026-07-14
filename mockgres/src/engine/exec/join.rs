use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pgwire::error::PgWireResult;

use super::ExecNode;
use crate::engine::{
    BoolExpr, CmpOp, DataType, EvalContext, JoinType, ScalarExpr, Schema, Value, eval_bool_expr,
    fe_code,
};
use crate::storage::Row;

const MAX_NESTED_LOOP_CANDIDATES: usize = 1_000_000;

pub struct JoinExec {
    schema: Schema,
    left: Option<Box<dyn ExecNode>>,
    right: Option<Box<dyn ExecNode>>,
    rows: Vec<Row>,
    left_width: usize,
    right_width: usize,
    join_type: JoinType,
    on: Option<BoolExpr>,
    params: Arc<Vec<Value>>,
    ctx: EvalContext,
    pos: usize,
    built: bool,
}

impl JoinExec {
    pub fn new(
        schema: Schema,
        left: Box<dyn ExecNode>,
        right: Box<dyn ExecNode>,
        join_type: JoinType,
        on: Option<BoolExpr>,
        params: Arc<Vec<Value>>,
        ctx: EvalContext,
    ) -> Self {
        let left_width = left.schema().fields.len();
        let right_width = right.schema().fields.len();
        Self {
            schema,
            left: Some(left),
            right: Some(right),
            rows: Vec::new(),
            left_width,
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

        let hash_keys = self.hash_keys(left.schema(), right.schema());
        self.rows = if hash_keys.is_empty() {
            self.nested_loop(&left_rows, &right_rows)?
        } else {
            self.hash_join(&left_rows, &right_rows, &hash_keys)?
        };
        self.built = true;
        Ok(())
    }

    fn hash_keys(&self, left_schema: &Schema, right_schema: &Schema) -> Vec<(usize, usize)> {
        let mut keys = Vec::new();
        if let Some(on) = &self.on {
            collect_hash_keys(on, self.left_width, left_schema, right_schema, &mut keys);
        }
        keys
    }

    fn hash_join(
        &self,
        left_rows: &[Row],
        right_rows: &[Row],
        keys: &[(usize, usize)],
    ) -> PgWireResult<Vec<Row>> {
        let mut right_by_key: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
        for right in right_rows {
            if let Some(key) = row_key(right, keys.iter().map(|(_, right)| *right)) {
                right_by_key.entry(key).or_default().push(right);
            }
        }

        let mut rows = Vec::with_capacity(left_rows.len());
        let null_right = vec![Value::Null; self.right_width];
        for left in left_rows {
            let mut matched = false;
            if let Some(key) = row_key(left, keys.iter().map(|(left, _)| *left))
                && let Some(candidates) = right_by_key.get(&key)
            {
                for right in candidates {
                    let combined = combine_rows(left, right);
                    if !self.passes(&combined)? {
                        continue;
                    }
                    matched = true;
                    rows.push(combined);
                }
            }
            if self.join_type == JoinType::Left && !matched {
                rows.push(combine_rows(left, &null_right));
            }
        }
        Ok(rows)
    }

    fn nested_loop(&self, left_rows: &[Row], right_rows: &[Row]) -> PgWireResult<Vec<Row>> {
        let candidate_count = left_rows.len().saturating_mul(right_rows.len());
        if candidate_count > MAX_NESTED_LOOP_CANDIDATES {
            return Err(fe_code(
                "54000",
                format!(
                    "nested-loop join would examine {candidate_count} row pairs; limit is {MAX_NESTED_LOOP_CANDIDATES}"
                ),
            ));
        }

        let mut rows = Vec::new();
        let null_right = vec![Value::Null; self.right_width];
        for left in left_rows {
            let mut matched = false;
            for right in right_rows {
                let combined = combine_rows(left, right);
                if !self.passes(&combined)? {
                    continue;
                }
                matched = true;
                rows.push(combined);
            }
            if self.join_type == JoinType::Left && !matched {
                rows.push(combine_rows(left, &null_right));
            }
        }
        Ok(rows)
    }

    fn passes(&self, row: &[Value]) -> PgWireResult<bool> {
        let Some(on) = &self.on else {
            return Ok(true);
        };
        Ok(eval_bool_expr(row, on, &self.params, &self.ctx)?.unwrap_or(false))
    }
}

#[async_trait]
impl ExecNode for JoinExec {
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
        let row = std::mem::take(&mut self.rows[self.pos]);
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

fn collect_hash_keys(
    expr: &BoolExpr,
    left_width: usize,
    left_schema: &Schema,
    right_schema: &Schema,
    out: &mut Vec<(usize, usize)>,
) {
    match expr {
        BoolExpr::Comparison {
            lhs: ScalarExpr::ColumnIdx(lhs),
            op: CmpOp::Eq,
            rhs: ScalarExpr::ColumnIdx(rhs),
        } => {
            let key = if *lhs < left_width && *rhs >= left_width {
                Some((*lhs, *rhs - left_width))
            } else if *rhs < left_width && *lhs >= left_width {
                Some((*rhs, *lhs - left_width))
            } else {
                None
            };
            if let Some((left, right)) = key
                && left_schema.fields.get(left).is_some_and(|left_field| {
                    right_schema.fields.get(right).is_some_and(|right_field| {
                        hash_types_compatible(&left_field.data_type, &right_field.data_type)
                    })
                })
                && !out.contains(&(left, right))
            {
                out.push((left, right));
            }
        }
        BoolExpr::And(exprs) => {
            for expr in exprs {
                collect_hash_keys(expr, left_width, left_schema, right_schema, out);
            }
        }
        _ => {}
    }
}

fn hash_types_compatible(left: &DataType, right: &DataType) -> bool {
    let integers = |data_type: &DataType| {
        matches!(data_type, DataType::Int2 | DataType::Int4 | DataType::Int8)
    };
    (integers(left) && integers(right))
        || (left == right && !matches!(left, DataType::Float8 | DataType::Circle))
}

fn row_key(columns: &[Value], indexes: impl Iterator<Item = usize>) -> Option<Vec<Value>> {
    let mut key = Vec::new();
    for index in indexes {
        let value = columns.get(index)?.clone();
        if matches!(value, Value::Null) {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

fn combine_rows(left: &[Value], right: &[Value]) -> Row {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}
