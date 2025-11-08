use crate::storage::Row;
use crate::types::{format_bytea, format_date, format_timestamp};
use futures::{Stream, StreamExt, stream};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug)]
pub struct SqlError {
    pub code: &'static str,
    pub message: String,
}

impl SqlError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SqlError {}

pub fn fe(msg: impl Into<String>) -> PgWireError {
    PgWireError::ApiError(Box::new(SqlError::new("XX000", msg.into())))
}

pub fn fe_code(code: &'static str, msg: impl Into<String>) -> PgWireError {
    PgWireError::ApiError(Box::new(SqlError::new(code, msg.into())))
}

// ===== core types =====

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Int4,
    Int8,
    Float8,
    Text,
    Bool,
    Date,
    Timestamp,
    Bytea,
}

impl DataType {
    pub fn to_pg(&self) -> Type {
        match self {
            DataType::Int4 => Type::INT4,
            DataType::Int8 => Type::INT8,
            DataType::Float8 => Type::FLOAT8,
            DataType::Text => Type::TEXT,
            DataType::Bool => Type::BOOL,
            DataType::Date => Type::DATE,
            DataType::Timestamp => Type::TIMESTAMP,
            DataType::Bytea => Type::BYTEA,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub fields: Vec<Field>,
}
impl Schema {
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }
    pub fn len(&self) -> usize {
        self.fields.len()
    }
}

// Values used in rows
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int64(i64),
    Float64Bits(u64), // store floats as bits for Eq/Hash; encode/decode via f64::from_bits
    Text(String),
    Bool(bool),
    Date(i32),
    TimestampMicros(i64),
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Int64(a), Int64(b)) => a == b,
            (Float64Bits(a), Float64Bits(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (TimestampMicros(a), TimestampMicros(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Null => {}
            Int64(v) => v.hash(state),
            Float64Bits(v) => v.hash(state),
            Text(s) => s.hash(state),
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            TimestampMicros(t) => t.hash(state),
            Bytes(b) => b.hash(state),
        }
    }
}
impl Value {
    pub fn from_f64(f: f64) -> Self {
        Value::Float64Bits(f.to_bits())
    }
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Float64Bits(b) = self {
            Some(f64::from_bits(*b))
        } else {
            None
        }
    }
}

// ===== expressions & logical plan =====

// compare ops supported by filter
#[derive(Clone, Copy, Debug)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Literal(Value),
    Column(usize), // bound column position
}

#[derive(Clone, Debug)]
pub enum ScalarExpr {
    Literal(Value),
    Column(String),   // unbound name
    ColumnIdx(usize), // bound column position
    Param {
        idx: usize,
        ty: Option<DataType>,
    },
    BinaryOp {
        op: ScalarBinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    UnaryOp {
        op: ScalarUnaryOp,
        expr: Box<ScalarExpr>,
    },
    Func {
        func: ScalarFunc,
        args: Vec<ScalarExpr>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum ScalarBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
}

#[derive(Clone, Copy, Debug)]
pub enum ScalarUnaryOp {
    Negate,
}

#[derive(Clone, Copy, Debug)]
pub enum ScalarFunc {
    Coalesce,
    Upper,
    Lower,
    Length,
}

#[derive(Clone, Debug)]
pub enum BoolExpr {
    Literal(bool),
    Comparison {
        lhs: ScalarExpr,
        op: CmpOp,
        rhs: ScalarExpr,
    },
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
    IsNull {
        expr: ScalarExpr,
        negated: bool,
    },
}

#[derive(Clone, Debug)]
pub enum InsertSource {
    Expr(ScalarExpr),
    Default,
}

#[derive(Clone, Debug)]
pub enum Selection {
    Star,
    Columns(Vec<String>), // unbound names
}

#[derive(Clone, Debug)]
pub struct ObjName {
    pub schema: Option<String>,
    pub name: String,
}

// logical plan
#[derive(Clone, Debug)]
pub enum Plan {
    // select 42 path: values + projection of named exprs
    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },

    // parser output (unbound): table + selection (star or column names)
    UnboundSeqScan {
        table: ObjName,
        selection: Selection,
    },

    // wrappers (inherit child schema)
    Filter {
        input: Box<Plan>,
        expr: BoolExpr,
        project_prefix_len: Option<usize>,
    },
    Order {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        limit: usize,
    },

    // bound scan (executor-ready)
    SeqScan {
        table: ObjName,
        cols: Vec<(usize, Field)>,
        schema: Schema,
    },

    // projection over input with named exprs
    Projection {
        input: Box<Plan>,
        exprs: Vec<(ScalarExpr, String)>,
        schema: Schema,
    },

    // ddl/dml
    CreateTable {
        table: ObjName,
        cols: Vec<(String, DataType, bool, Option<Value>)>,
        pk: Option<Vec<String>>,
    },
    AlterTableAddColumn {
        table: ObjName,
        column: (String, DataType, bool, Option<Value>),
        if_not_exists: bool,
    },
    AlterTableDropColumn {
        table: ObjName,
        column: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: ObjName,
        columns: Vec<String>,
        if_not_exists: bool,
    },
    DropIndex {
        indexes: Vec<ObjName>,
        if_exists: bool,
    },
    ShowVariable {
        name: String,
        schema: Schema,
    },
    SetVariable {
        name: String,
        value: Option<String>,
    },
    InsertValues {
        table: ObjName,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<InsertSource>>,
    },
    Update {
        table: ObjName,
        sets: Vec<UpdateSet>,
        filter: Option<BoolExpr>,
    },
    Delete {
        table: ObjName,
        filter: Option<BoolExpr>,
    },
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
}

#[derive(Clone, Debug)]
pub enum UpdateSet {
    ByName(String, ScalarExpr),
    ByIndex(usize, ScalarExpr),
}

// order by key; name or ordinal; asc=true means ascending
#[derive(Clone, Debug)]
pub enum SortKey {
    ByName {
        col: String,
        asc: bool,
        nulls_first: Option<bool>,
    },
    ByIndex {
        idx: usize,
        asc: bool,
        nulls_first: Option<bool>,
    },
    Expr {
        expr: ScalarExpr,
        asc: bool,
        nulls_first: Option<bool>,
    },
}

impl Plan {
    pub fn schema(&self) -> &Schema {
        match self {
            Plan::Values { schema, .. }
            | Plan::SeqScan { schema, .. }
            | Plan::Projection { schema, .. } => schema,
            Plan::ShowVariable { schema, .. } => schema,

            // wrappers: same schema as child
            Plan::Filter { input, .. } | Plan::Order { input, .. } | Plan::Limit { input, .. } => {
                input.schema()
            }

            Plan::UnboundSeqScan { .. }
            | Plan::CreateTable { .. }
            | Plan::AlterTableAddColumn { .. }
            | Plan::AlterTableDropColumn { .. }
            | Plan::CreateIndex { .. }
            | Plan::DropIndex { .. }
            | Plan::SetVariable { .. }
            | Plan::InsertValues { .. }
            | Plan::Update { .. }
            | Plan::Delete { .. }
            | Plan::BeginTransaction
            | Plan::CommitTransaction
            | Plan::RollbackTransaction => {
                static EMPTY: Schema = Schema { fields: vec![] };
                &EMPTY
            }
        }
    }
}

// ===== exec nodes & bridge =====

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

// limit exec: forwards up to n rows from child
pub struct LimitExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    remaining: usize,
}

impl LimitExec {
    pub fn new(schema: Schema, child: Box<dyn ExecNode>, n: usize) -> Self {
        Self {
            schema,
            child,
            remaining: n,
        }
    }
}

impl ExecNode for LimitExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.child.open()
    }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        match self.child.next()? {
            Some(r) => {
                self.remaining -= 1;
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

pub fn eval_scalar_expr(row: &[Value], expr: &ScalarExpr, params: &[Value]) -> PgWireResult<Value> {
    match expr {
        ScalarExpr::Literal(v) => Ok(v.clone()),
        ScalarExpr::ColumnIdx(i) => row
            .get(*i)
            .cloned()
            .ok_or_else(|| fe("column index out of range")),
        ScalarExpr::Column(name) => Err(fe(format!("unbound column reference: {name}"))),
        ScalarExpr::Param { idx, .. } => params
            .get(*idx)
            .cloned()
            .ok_or_else(|| fe("parameter index out of range")),
        ScalarExpr::BinaryOp { op, left, right } => {
            let lv = eval_scalar_expr(row, left, params)?;
            let rv = eval_scalar_expr(row, right, params)?;
            eval_binary_op(*op, lv, rv)
        }
        ScalarExpr::UnaryOp { op, expr } => {
            let v = eval_scalar_expr(row, expr, params)?;
            eval_unary_op(*op, v)
        }
        ScalarExpr::Func { func, args } => {
            let mut evaluated = Vec::with_capacity(args.len());
            for arg in args {
                evaluated.push(eval_scalar_expr(row, arg, params)?);
            }
            eval_function(*func, evaluated)
        }
    }
}

fn eval_binary_op(op: ScalarBinaryOp, left: Value, right: Value) -> PgWireResult<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match op {
        ScalarBinaryOp::Add | ScalarBinaryOp::Sub | ScalarBinaryOp::Mul => {
            let (l_val, r_val, use_float) = coerce_numeric_pair(left, right)?;
            if !use_float {
                if let (NumericValue::Int(a), NumericValue::Int(b)) = (&l_val, &r_val) {
                    return Ok(match op {
                        ScalarBinaryOp::Add => Value::Int64(*a + *b),
                        ScalarBinaryOp::Sub => Value::Int64(*a - *b),
                        ScalarBinaryOp::Mul => Value::Int64(*a * *b),
                        _ => unreachable!(),
                    });
                }
            }
            let lf = l_val
                .to_f64()
                .ok_or_else(|| fe("numeric evaluation failed"))?;
            let rf = r_val
                .to_f64()
                .ok_or_else(|| fe("numeric evaluation failed"))?;
            let res = match op {
                ScalarBinaryOp::Add => lf + rf,
                ScalarBinaryOp::Sub => lf - rf,
                ScalarBinaryOp::Mul => lf * rf,
                _ => unreachable!(),
            };
            Ok(Value::from_f64(res))
        }
        ScalarBinaryOp::Div => {
            let right_is_zero = match &right {
                Value::Int64(0) => true,
                Value::Float64Bits(bits) => f64::from_bits(*bits) == 0.0,
                _ => false,
            };
            if right_is_zero {
                return Err(fe_code("22012", "division by zero"));
            }
            let (l, r, _) = coerce_numeric_pair(left, right)?;
            let lf = l
                .to_f64()
                .ok_or_else(|| fe("cannot convert lhs to float"))?;
            let rf = r
                .to_f64()
                .ok_or_else(|| fe("cannot convert rhs to float"))?;
            Ok(Value::from_f64(lf / rf))
        }
        ScalarBinaryOp::Concat => {
            let ltxt = value_to_text(left)?;
            let rtxt = value_to_text(right)?;
            Ok(match (ltxt, rtxt) {
                (Some(l), Some(r)) => Value::Text(format!("{l}{r}")),
                _ => Value::Null,
            })
        }
    }
}

fn eval_unary_op(op: ScalarUnaryOp, value: Value) -> PgWireResult<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match op {
        ScalarUnaryOp::Negate => match value {
            Value::Int64(v) => Ok(Value::Int64(-v)),
            Value::Float64Bits(bits) => {
                let f = f64::from_bits(bits);
                Ok(Value::from_f64(-f))
            }
            other => Err(fe(format!("cannot negate value {:?}", other))),
        },
    }
}

fn eval_function(func: ScalarFunc, args: Vec<Value>) -> PgWireResult<Value> {
    match func {
        ScalarFunc::Coalesce => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg);
                }
            }
            Ok(Value::Null)
        }
        ScalarFunc::Upper => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("upper() expects text")),
        },
        ScalarFunc::Lower => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("lower() expects text")),
        },
        ScalarFunc::Length => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int64(s.chars().count() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            Some(Value::Null) | None => Ok(Value::Null),
            Some(other) => Err(fe(format!("length() unsupported for {:?}", other))),
        },
    }
}

#[derive(Clone)]
enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    fn to_f64(&self) -> Option<f64> {
        match self {
            NumericValue::Int(i) => Some(*i as f64),
            NumericValue::Float(f) => Some(*f),
        }
    }
}

fn coerce_numeric_pair(
    left: Value,
    right: Value,
) -> PgWireResult<(NumericValue, NumericValue, bool)> {
    let l = value_to_numeric(left)?;
    let r = value_to_numeric(right)?;
    let use_float = matches!(l, NumericValue::Float(_)) || matches!(r, NumericValue::Float(_));
    Ok(if use_float {
        (
            NumericValue::Float(l.to_f64().unwrap()),
            NumericValue::Float(r.to_f64().unwrap()),
            true,
        )
    } else {
        (l, r, false)
    })
}

fn value_to_numeric(v: Value) -> PgWireResult<NumericValue> {
    match v {
        Value::Int64(i) => Ok(NumericValue::Int(i)),
        Value::Float64Bits(bits) => Ok(NumericValue::Float(f64::from_bits(bits))),
        other => Err(fe(format!("numeric value expected, got {:?}", other))),
    }
}

fn value_to_text(v: Value) -> PgWireResult<Option<String>> {
    Ok(match v {
        Value::Null => None,
        Value::Text(s) => Some(s),
        Value::Int64(i) => Some(i.to_string()),
        Value::Float64Bits(bits) => Some(f64::from_bits(bits).to_string()),
        Value::Bool(b) => Some(if b { "t" } else { "f" }.into()),
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into()),
        Value::Date(_) | Value::TimestampMicros(_) => {
            return Err(fe("text conversion not supported for date/timestamp"));
        }
    })
}

pub fn eval_bool_expr(
    row: &[Value],
    expr: &BoolExpr,
    params: &[Value],
) -> PgWireResult<Option<bool>> {
    use std::cmp::Ordering;
    Ok(match expr {
        BoolExpr::Literal(b) => Some(*b),
        BoolExpr::Comparison { lhs, op, rhs } => {
            let lv = eval_scalar_expr(row, lhs, params)?;
            let rv = eval_scalar_expr(row, rhs, params)?;
            let ord = compare_values(&lv, &rv);
            ord.map(|o| match op {
                CmpOp::Eq => o == Ordering::Equal,
                CmpOp::Neq => o != Ordering::Equal,
                CmpOp::Lt => o == Ordering::Less,
                CmpOp::Lte => o != Ordering::Greater,
                CmpOp::Gt => o == Ordering::Greater,
                CmpOp::Gte => o != Ordering::Less,
            })
        }
        BoolExpr::And(exprs) => {
            let mut saw_null = false;
            for e in exprs {
                match eval_bool_expr(row, e, params)? {
                    Some(true) => {}
                    Some(false) => return Ok(Some(false)),
                    None => saw_null = true,
                }
            }
            if saw_null { None } else { Some(true) }
        }
        BoolExpr::Or(exprs) => {
            let mut saw_null = false;
            for e in exprs {
                match eval_bool_expr(row, e, params)? {
                    Some(true) => return Ok(Some(true)),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }
            if saw_null { None } else { Some(false) }
        }
        BoolExpr::Not(inner) => match eval_bool_expr(row, inner, params)? {
            Some(v) => Some(!v),
            None => None,
        },
        BoolExpr::IsNull { expr, negated } => {
            let v = eval_scalar_expr(row, expr, params)?;
            match v {
                Value::Null => Some(!*negated),
                _ => Some(*negated),
            }
        }
    })
}

fn compare_values(lhs: &Value, rhs: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return None;
    }
    Some(match (lhs, rhs) {
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64Bits(ba), Value::Float64Bits(bb)) => {
            let (a, b) = (f64::from_bits(*ba), f64::from_bits(*bb));
            if a.is_nan() && b.is_nan() {
                Ordering::Equal
            } else if a.is_nan() {
                Ordering::Greater
            } else if b.is_nan() {
                Ordering::Less
            } else if a < b {
                Ordering::Less
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Int64(a), Value::Float64Bits(bb)) => {
            let (a, b) = (*a as f64, f64::from_bits(*bb));
            if b.is_nan() {
                Ordering::Less
            } else if a < b {
                Ordering::Less
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Float64Bits(ba), Value::Int64(bi)) => {
            let (a, b) = (f64::from_bits(*ba), *bi as f64);
            if a.is_nan() {
                Ordering::Greater
            } else if a < b {
                Ordering::Less
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::TimestampMicros(a), Value::TimestampMicros(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => return None,
    })
}

pub fn to_pgwire_stream(
    mut node: Box<dyn ExecNode>,
    fmt: FieldFormat,
) -> PgWireResult<(
    Arc<Vec<FieldInfo>>,
    impl Stream<Item = PgWireResult<DataRow>> + Send + 'static,
)> {
    node.open()?;
    let schema = node.schema().clone();
    let fields = Arc::new(
        schema
            .fields
            .iter()
            .map(|f| FieldInfo::new(f.name.clone(), None, None, f.data_type.to_pg(), fmt))
            .collect::<Vec<_>>(),
    );

    let s = stream::unfold(
        (node, fields.clone(), schema),
        move |(mut node, fields, schema)| async move {
            let next = node.next();
            match next {
                Ok(Some(vals)) => {
                    let mut enc = DataRowEncoder::new(fields.clone());
                    for (i, v) in vals.into_iter().enumerate() {
                        let dt = &schema.field(i).data_type;
                        // encode by declared column type; allow null for any type
                        let res = match (v, dt) {
                            (Value::Null, DataType::Int4) => enc.encode_field(&Option::<i32>::None),
                            (Value::Null, DataType::Int8) => enc.encode_field(&Option::<i64>::None),
                            (Value::Null, DataType::Float8) => {
                                enc.encode_field(&Option::<f64>::None)
                            }
                            (Value::Null, DataType::Text) => {
                                enc.encode_field(&Option::<String>::None)
                            }
                            (Value::Null, DataType::Bool) => {
                                enc.encode_field(&Option::<bool>::None)
                            }
                            (Value::Null, DataType::Date) => {
                                enc.encode_field(&Option::<String>::None)
                            }
                            (Value::Null, DataType::Timestamp) => {
                                enc.encode_field(&Option::<String>::None)
                            }
                            (Value::Null, DataType::Bytea) => {
                                enc.encode_field(&Option::<Vec<u8>>::None)
                            }

                            (Value::Int64(i), DataType::Int4) => enc.encode_field(&(i as i32)),
                            (Value::Int64(i), DataType::Int8) => enc.encode_field(&i),
                            (Value::Int64(i), DataType::Float8) => {
                                let f = i as f64;
                                enc.encode_field(&f)
                            }
                            (Value::Float64Bits(b), DataType::Float8) => {
                                let f = f64::from_bits(b);
                                enc.encode_field(&f)
                            }
                            (Value::Text(s), DataType::Text) => {
                                let owned = s;
                                enc.encode_field(&owned)
                            }
                            (Value::Bool(b), DataType::Bool) => enc.encode_field(&b),
                            (Value::Date(days), DataType::Date) => {
                                if fmt == FieldFormat::Binary {
                                    return Some((
                                        Err(fe("binary date format not supported yet")),
                                        (node, fields, schema),
                                    ));
                                }
                                let text = match format_date(days) {
                                    Ok(t) => t,
                                    Err(e) => return Some((Err(fe(e)), (node, fields, schema))),
                                };
                                enc.encode_field(&text)
                            }
                            (Value::TimestampMicros(micros), DataType::Timestamp) => {
                                if fmt == FieldFormat::Binary {
                                    return Some((
                                        Err(fe("binary timestamp format not supported yet")),
                                        (node, fields, schema),
                                    ));
                                }
                                let text = match format_timestamp(micros) {
                                    Ok(t) => t,
                                    Err(e) => return Some((Err(fe(e)), (node, fields, schema))),
                                };
                                enc.encode_field(&text)
                            }
                            (Value::Bytes(bytes), DataType::Bytea) => {
                                if fmt == FieldFormat::Binary {
                                    enc.encode_field_with_type_and_format(
                                        &bytes,
                                        &Type::BYTEA,
                                        FieldFormat::Binary,
                                    )
                                } else {
                                    let text = format_bytea(bytes.as_slice());
                                    enc.encode_field(&text)
                                }
                            }
                            _ => Err(PgWireError::ApiError("type mismatch".into())),
                        };
                        if let Err(e) = res {
                            return Some((Err(e), (node, fields, schema)));
                        }
                    }
                    match enc.finish() {
                        Ok(dr) => Some((Ok(dr), (node, fields, schema))),
                        Err(e) => Some((Err(e), (node, fields, schema))),
                    }
                }
                Ok(None) => {
                    let _ = node.close();
                    None
                }
                Err(e) => Some((Err(e), (node, fields, schema))),
            }
        },
    )
    .boxed();

    Ok((fields, s))
}
