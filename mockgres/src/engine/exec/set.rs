use super::*;
use crate::engine::SetOpKind;

pub struct SetOpExec {
    schema: Schema,
    left: Option<Box<dyn ExecNode>>,
    right: Option<Box<dyn ExecNode>>,
    op: SetOpKind,
    all: bool,
    rows: Vec<Row>,
    pos: usize,
    built: bool,
}

impl SetOpExec {
    pub fn new(
        schema: Schema,
        left: Box<dyn ExecNode>,
        right: Box<dyn ExecNode>,
        op: SetOpKind,
        all: bool,
    ) -> Self {
        Self {
            schema,
            left: Some(left),
            right: Some(right),
            op,
            all,
            rows: Vec::new(),
            pos: 0,
            built: false,
        }
    }

    async fn ensure_built(&mut self) -> PgWireResult<()> {
        if self.built {
            return Ok(());
        }
        let left_rows =
            read_all(self.left.take().expect("set operation left child missing")).await?;
        let right_rows = read_all(
            self.right
                .take()
                .expect("set operation right child missing"),
        )
        .await?;
        self.rows = match self.op {
            SetOpKind::Union => union_rows(left_rows, right_rows, self.all),
            SetOpKind::Intersect => intersect_rows(left_rows, right_rows, self.all),
            SetOpKind::Except => except_rows(left_rows, right_rows, self.all),
        };
        self.built = true;
        Ok(())
    }
}

#[async_trait]
impl ExecNode for SetOpExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.ensure_built().await
    }

    async fn next(&mut self) -> PgWireResult<Option<Row>> {
        self.ensure_built().await?;
        if self.pos == self.rows.len() {
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

async fn read_all(mut input: Box<dyn ExecNode>) -> PgWireResult<Vec<Row>> {
    input.open().await?;
    let mut rows = Vec::new();
    while let Some(row) = input.next().await? {
        rows.push(row);
    }
    input.close().await?;
    Ok(rows)
}

fn union_rows(left: Vec<Row>, right: Vec<Row>, all: bool) -> Vec<Row> {
    if all {
        return left.into_iter().chain(right).collect();
    }
    let mut seen = HashSet::new();
    left.into_iter()
        .chain(right)
        .filter(|row| seen.insert(set_key(row)))
        .collect()
}

fn intersect_rows(left: Vec<Row>, right: Vec<Row>, all: bool) -> Vec<Row> {
    let mut remaining = row_counts(right);
    let mut emitted = HashSet::new();
    left.into_iter()
        .filter(|row| {
            let key = set_key(row);
            let Some(count) = remaining.get_mut(&key) else {
                return false;
            };
            if all {
                if *count == 0 {
                    return false;
                }
                *count -= 1;
                true
            } else {
                *count != 0 && emitted.insert(key)
            }
        })
        .collect()
}

fn except_rows(left: Vec<Row>, right: Vec<Row>, all: bool) -> Vec<Row> {
    let mut remaining = row_counts(right);
    let mut emitted = HashSet::new();
    left.into_iter()
        .filter(|row| {
            let key = set_key(row);
            if all {
                if let Some(count) = remaining.get_mut(&key)
                    && *count != 0
                {
                    *count -= 1;
                    return false;
                }
                true
            } else {
                !remaining.contains_key(&key) && emitted.insert(key)
            }
        })
        .collect()
}

fn row_counts(rows: Vec<Row>) -> HashMap<Row, usize> {
    let mut counts = HashMap::new();
    for row in rows {
        *counts.entry(set_key(&row)).or_insert(0) += 1;
    }
    counts
}

fn set_key(row: &Row) -> Row {
    row.iter().map(canonical_set_value).collect()
}

fn canonical_set_value(value: &Value) -> Value {
    match value {
        Value::Float64Bits(bits) => {
            let value = f64::from_bits(*bits);
            if value.is_nan() {
                Value::from_f64(f64::NAN)
            } else if value == 0.0 {
                Value::from_f64(0.0)
            } else {
                Value::Float64Bits(*bits)
            }
        }
        value => value.clone(),
    }
}
