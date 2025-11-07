use std::{fmt, sync::Arc};
use futures::{Stream, stream, StreamExt};
use pgwire::api::{Type};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use crate::storage::Row;

#[derive(Debug)]
struct SimpleError(String);
impl fmt::Display for SimpleError { fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result{f.write_str(&self.0)} }
impl std::error::Error for SimpleError {}
pub fn fe(msg: impl Into<String>) -> PgWireError { PgWireError::ApiError(Box::new(SimpleError(msg.into()))) }

// ===== core types =====

#[derive(Clone, Debug, PartialEq)]
pub enum DataType { Int4, Int8, Float8 }

impl DataType {
    pub fn to_pg(&self) -> Type {
        match self {
            DataType::Int4 => Type::INT4,
            DataType::Int8 => Type::INT8,
            DataType::Float8 => Type::FLOAT8,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field { pub name: String, pub data_type: DataType }

#[derive(Clone, Debug, PartialEq)]
pub struct Schema { pub fields: Vec<Field> }
impl Schema { pub fn field(&self,i:usize)->&Field{&self.fields[i]} pub fn len(&self)->usize{self.fields.len()} }

// Values used in rows
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int64(i64),
    Float64Bits(u64), // store floats as bits for Eq/Hash; encode/decode via f64::from_bits
}
impl Value {
    pub fn from_f64(f: f64) -> Self { Value::Float64Bits(f.to_bits()) }
    pub fn as_f64(&self) -> Option<f64> { if let Value::Float64Bits(b)=self { Some(f64::from_bits(*b)) } else { None } }
}

// ===== expressions & logical plan =====

// compare ops supported by filter
#[derive(Clone, Copy, Debug)]
pub enum CmpOp { Eq, Neq, Lt, Lte, Gt, Gte }


#[derive(Clone, Debug)]
pub enum Expr {
    Literal(Value),
    Column(usize),           // bound column position
}

#[derive(Clone, Debug)]
pub enum Selection {
    Star,
    Columns(Vec<String>),    // unbound names
}

#[derive(Clone, Debug)]
pub struct ObjName { pub schema: Option<String>, pub name: String }

// logical plan
#[derive(Clone, Debug)]
pub enum Plan {
    // select 42 path: values + projection of named exprs
    Values { rows: Vec<Vec<Expr>>, schema: Schema },

    // parser output (unbound): table + selection (star or column names)
    UnboundSeqScan { table: ObjName, selection: Selection },

    // wrappers (inherit child schema)
    Filter { input: Box<Plan>, pred: FilterPred, project_prefix_len: Option<usize> },
    Order  { input: Box<Plan>, keys: Vec<SortKey> },
    Limit  { input: Box<Plan>, limit: usize },

    // bound scan (executor-ready)
    SeqScan { table: ObjName, cols: Vec<(usize, Field)>, schema: Schema },

    // projection over input with named exprs
    Projection { input: Box<Plan>, exprs: Vec<(Expr, String)>, schema: Schema },

    // ddl/dml
    CreateTable { table: ObjName, cols: Vec<(String, DataType, bool)>, pk: Option<Vec<String>> },
    InsertValues { table: ObjName, rows: Vec<Vec<Expr>> },
}

// match simple "col op value" shape
#[derive(Clone, Debug)]
pub enum FilterPred {
    ByName { col: String, op: crate::engine::CmpOp, rhs: Value },
    ByIndex { idx: usize, op: crate::engine::CmpOp, rhs: Value },
}

// order by key; name or ordinal; asc=true means ascending
#[derive(Clone, Debug)]
pub enum SortKey {
    ByName { col: String, asc: bool, nulls_first: Option<bool> },
    ByIndex { idx: usize, asc: bool, nulls_first: Option<bool> },
}

impl Plan {
    pub fn schema(&self) -> &Schema {
        match self {
            Plan::Values{schema,..} |
            Plan::SeqScan{schema,..} |
            Plan::Projection{schema,..} => schema,

            // wrappers: same schema as child
            Plan::Filter{input,..} |
            Plan::Order {input,..} |
            Plan::Limit {input,..} => input.schema(),

            Plan::UnboundSeqScan{..} |
            Plan::CreateTable{..} |
            Plan::InsertValues{..} => {
                static EMPTY: Schema = Schema { fields: vec![] }; &EMPTY
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

pub struct ValuesExec { schema: Schema, rows: Vec<Vec<Value>>, idx: usize }
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
        Ok(Self { schema, rows, idx: 0 })
    }
}
impl ExecNode for ValuesExec {
    fn open(&mut self)->PgWireResult<()> { Ok(()) }
    fn next(&mut self)->PgWireResult<Option<Vec<Value>>> {
        if self.idx>=self.rows.len(){return Ok(None)}
        let row = self.rows[self.idx].clone(); self.idx+=1; Ok(Some(row))
    }
    fn close(&mut self)->PgWireResult<()> { Ok(()) }
    fn schema(&self)->&Schema { &self.schema }
}

pub struct ProjectExec { schema: Schema, input: Box<dyn ExecNode>, exprs: Vec<Expr> }
impl ProjectExec {
    pub fn new(schema: Schema, input: Box<dyn ExecNode>, exprs_named: Vec<(Expr,String)>) -> Self {
        let exprs = exprs_named.into_iter().map(|(e,_)| e).collect();
        Self{ schema, input, exprs }
    }
}
impl ExecNode for ProjectExec {
    fn open(&mut self)->PgWireResult<()> { self.input.open() }
    fn next(&mut self)->PgWireResult<Option<Vec<Value>>> {
        if let Some(in_row)=self.input.next()? {
            let mut out=Vec::with_capacity(self.exprs.len());
            for e in &self.exprs { out.push(eval(&in_row,e)?); }
            Ok(Some(out))
        } else { Ok(None) }
    }
    fn close(&mut self)->PgWireResult<()> { self.input.close() }
    fn schema(&self)->&Schema { &self.schema }
}

pub struct SeqScanExec { schema: Schema, rows: Vec<Vec<Value>>, idx: usize }
impl SeqScanExec {
    // simple table scan that materializes all rows.
    pub fn new(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self { schema, rows, idx: 0 }
    }
}
impl ExecNode for SeqScanExec {
    fn open(&mut self)->PgWireResult<()> { Ok(()) }
    fn next(&mut self)->PgWireResult<Option<Vec<Value>>> {
        if self.idx>=self.rows.len(){return Ok(None)}
        let row = self.rows[self.idx].clone();
        self.idx+=1; Ok(Some(row))
    }
    fn close(&mut self)->PgWireResult<()> { Ok(()) }
    fn schema(&self)->&Schema { &self.schema }
}

// filter exec: passes through rows where col <op> rhs holds

pub struct FilterExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    col_idx: usize,
    op: CmpOp,
    rhs: Value,
}

impl FilterExec {
    pub fn new(schema: Schema, child: Box<dyn ExecNode>, col_idx: usize, op: CmpOp, rhs: Value) -> Self {
        Self { schema, child, col_idx, op, rhs }
    }

    fn cmp(lhs: &Value, rhs: &Value, op: CmpOp) -> bool {
        use std::cmp::Ordering;
        // null semantics: any comparison with null is false
        if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
            return false;
        }

        let ord = match (lhs, rhs) {
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),

            (Value::Float64Bits(ba), Value::Float64Bits(bb)) => {
                let (a, b) = (f64::from_bits(*ba), f64::from_bits(*bb));
                if a.is_nan() && b.is_nan() { Ordering::Equal }
                else if a.is_nan() { Ordering::Greater }
                else if b.is_nan() { Ordering::Less }
                else if a < b { Ordering::Less }
                else if a > b { Ordering::Greater }
                else { Ordering::Equal }
            }

            (Value::Int64(a), Value::Float64Bits(bb)) => {
                let (a, b) = (*a as f64, f64::from_bits(*bb));
                if b.is_nan() { Ordering::Less } // int < NaN
                else if a < b { Ordering::Less }
                else if a > b { Ordering::Greater }
                else { Ordering::Equal }
            }

            (Value::Float64Bits(ba), Value::Int64(bi)) => {
                let (a, b) = (f64::from_bits(*ba), *bi as f64);
                if a.is_nan() { Ordering::Greater } // NaN > int
                else if a < b { Ordering::Less }
                else if a > b { Ordering::Greater }
                else { Ordering::Equal }
            }

            _ => return false, // unsupported types compare false
        };

        match op {
            CmpOp::Eq  => ord == Ordering::Equal,
            CmpOp::Neq => ord != Ordering::Equal,
            CmpOp::Lt  => ord == Ordering::Less,
            CmpOp::Lte => ord != Ordering::Greater,
            CmpOp::Gt  => ord == Ordering::Greater,
            CmpOp::Gte => ord != Ordering::Less,
        }
    }
}

impl ExecNode for FilterExec {
    fn open(&mut self) -> PgWireResult<()> { self.child.open() }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        loop {
            match self.child.next()? {
                Some(row) => {
                    let v = row.get(self.col_idx).ok_or_else(|| fe("filter index out of range"))?;
                    if Self::cmp(v, &self.rhs, self.op) {
                        return Ok(Some(row));
                    }
                    // else keep scanning
                }
                None => return Ok(None),
            }
        }
    }
    fn close(&mut self) -> PgWireResult<()> { self.child.close() }
    fn schema(&self) -> &Schema { &self.schema }
}

// order exec: materializes child rows, sorts them, then yields
pub struct OrderExec {
    schema: Schema,
    rows: Vec<Row>,
    pos: usize,
    keys: Vec<(usize, bool, bool)>,
}

impl OrderExec {
    pub fn new(schema: Schema, mut child: Box<dyn ExecNode>, keys: Vec<(usize, bool, Option<bool>)>) -> PgWireResult<Self> {
        child.open()?;
        let mut buf = Vec::new();
        while let Some(r) = child.next()? { buf.push(r); }
        child.close()?;

        // resolve nulls policy up front: postgres default => asc => nulls last, desc => nulls first
        let resolved_keys: Vec<(usize, bool, bool)> = keys.into_iter()
            .map(|(idx, asc, nulls_first_opt)| {
                let nulls_first_eff = nulls_first_opt.unwrap_or(!asc);
                (idx, asc, nulls_first_eff)
            })
            .collect();

        // stable sort by keys, honoring asc/desc and nulls_first per key
        buf.sort_by(|a, b| {
            use std::cmp::Ordering;
            for (idx, asc, nulls_first) in &resolved_keys {
                let av = a.get(*idx);
                let bv = b.get(*idx);
                let ord = order_values(av, bv, *asc, *nulls_first);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        Ok(Self { schema, rows: buf, pos: 0, keys: resolved_keys })
    }}

// compares two values with explicit asc and nulls policy.
// nulls_first=true => nulls before non-nulls; false => nulls after
fn order_values(a: Option<&Value>, b: Option<&Value>, asc: bool, nulls_first: bool) -> std::cmp::Ordering {
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
        (Value::Null, _)           => if nulls_first { Less } else { Greater },
        (_,           Value::Null) => if nulls_first { Greater } else { Less },

        // integers
        (Value::Int64(x), Value::Int64(y)) => {
            let ord = x.cmp(y);
            if asc { ord } else { ord.reverse() }
        }

        // floats (NaN > all in ascending); desc handled by reversing ord
        (Value::Float64Bits(bx), Value::Float64Bits(by)) => {
            let (x, y) = (f64::from_bits(*bx), f64::from_bits(*by));
            let ord = if x.is_nan() && y.is_nan() { Equal }
            else if x.is_nan() { Greater }   // NaN > all
            else if y.is_nan() { Less }
            else if x < y { Less }
            else if x > y { Greater }
            else { Equal };
            if asc { ord } else { ord.reverse() }
        }

        // int vs float coercion
        (Value::Int64(x), Value::Float64Bits(by)) => {
            let y = f64::from_bits(*by);
            let ord = if y.is_nan() { Less } else {
                let xf = *x as f64;
                if xf < y { Less } else if xf > y { Greater } else { Equal }
            };
            if asc { ord } else { ord.reverse() }
        }
        (Value::Float64Bits(bx), Value::Int64(y)) => {
            let x = f64::from_bits(*bx);
            let ord = if x.is_nan() { Greater } else {
                let yf = *y as f64;
                if x < yf { Less } else if x > yf { Greater } else { Equal }
            };
            if asc { ord } else { ord.reverse() }
        }
    }
}

impl ExecNode for OrderExec {
    fn open(&mut self) -> PgWireResult<()> { Ok(()) }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        if self.pos >= self.rows.len() { return Ok(None); }
        let r = self.rows[self.pos].clone();
        self.pos += 1;
        Ok(Some(r))
    }
    fn close(&mut self) -> PgWireResult<()> { Ok(()) }
    fn schema(&self) -> &Schema { &self.schema }
}

// limit exec: forwards up to n rows from child
pub struct LimitExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    remaining: usize,
}

impl LimitExec {
    pub fn new(schema: Schema, child: Box<dyn ExecNode>, n: usize) -> Self {
        Self { schema, child, remaining: n }
    }
}

impl ExecNode for LimitExec {
    fn open(&mut self) -> PgWireResult<()> { self.child.open() }
    fn next(&mut self) -> PgWireResult<Option<Row>> {
        if self.remaining == 0 { return Ok(None); }
        match self.child.next()? {
            Some(r) => { self.remaining -= 1; Ok(Some(r)) }
            None => Ok(None),
        }
    }
    fn close(&mut self) -> PgWireResult<()> { self.child.close() }
    fn schema(&self) -> &Schema { &self.schema }
}



fn eval_const(e: &Expr)->PgWireResult<Value> {
    match e {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(_) => Err(fe("column not allowed here")),
    }
}
fn eval(row:&[Value], e:&Expr)->PgWireResult<Value>{
    match e {
        Expr::Literal(v)=>Ok(v.clone()),
        Expr::Column(i)=>Ok(row[*i].clone()),
    }
}

pub fn to_pgwire_stream(
    mut node: Box<dyn ExecNode>,
    fmt: FieldFormat,
) -> PgWireResult<(Arc<Vec<FieldInfo>>, impl Stream<Item = PgWireResult<DataRow>> + Send + 'static)> {
    node.open()?;
    let schema = node.schema().clone();
    let fields = Arc::new(schema.fields.iter().map(|f| {
        FieldInfo::new(f.name.clone(), None, None, f.data_type.to_pg(), fmt)
    }).collect::<Vec<_>>());

    let s = stream::unfold((node, fields.clone(), schema), |(mut node, fields, schema)| async move {
        let next = node.next();
        match next {
            Ok(Some(vals)) => {
                let mut enc = DataRowEncoder::new(fields.clone());
                for (i, v) in vals.into_iter().enumerate() {
                    let dt = &schema.field(i).data_type;
                    // encode by declared column type; allow null for any type
                    let res = match (v, dt) {
                        (Value::Null, DataType::Int4)   => enc.encode_field(&Option::<i32>::None),
                        (Value::Null, DataType::Int8)   => enc.encode_field(&Option::<i64>::None),
                        (Value::Null, DataType::Float8) => enc.encode_field(&Option::<f64>::None),

                        (Value::Int64(i), DataType::Int4) => enc.encode_field(&(i as i32)),
                        (Value::Int64(i), DataType::Int8) => enc.encode_field(&i),
                        (Value::Int64(i), DataType::Float8) => {
                            let f = i as f64; enc.encode_field(&f)
                        }
                        (Value::Float64Bits(b), DataType::Float8) => {
                            let f = f64::from_bits(b); enc.encode_field(&f)
                        }
                        _ => Err(PgWireError::ApiError("type mismatch".into())),
                    };
                    if let Err(e) = res { return Some((Err(e), (node, fields, schema))); }
                }
                match enc.finish() {
                    Ok(dr) => Some((Ok(dr), (node, fields, schema))),
                    Err(e) => Some((Err(e), (node, fields, schema))),
                }
            }
            Ok(None) => { let _ = node.close(); None }
            Err(e) => Some((Err(e), (node, fields, schema))),
        }
    }).boxed();

    Ok((fields, s))
}
