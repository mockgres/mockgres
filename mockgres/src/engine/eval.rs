use crate::session::{Session, SessionTimeZone, now_utc_micros};
use crate::types::{
    date_days_to_postgres, format_bytea, format_date, format_timestamp, format_timestamptz,
    timestamp_micros_to_date_days, timestamp_to_postgres_micros,
};
use futures::{Stream, StreamExt, stream};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use std::sync::Arc;

use super::exec::ExecNode;
use super::{
    BoolExpr, CmpOp, DataType, ScalarBinaryOp, ScalarExpr, ScalarFunc, ScalarUnaryOp, Value,
    cast_value_to_type, fe, fe_code,
};

#[derive(Clone)]
pub struct StatementTimeContext {
    pub stmt_ts_utc_micros: i64,
    pub session_tz: SessionTimeZone,
}

impl StatementTimeContext {
    pub fn new(stmt_ts_utc_micros: i64, session_tz: SessionTimeZone) -> Self {
        Self {
            stmt_ts_utc_micros,
            session_tz,
        }
    }

    pub fn capture(session: &Session) -> Self {
        let tz = session.time_zone();
        let stmt_ts = session
            .statement_time_micros()
            .unwrap_or_else(now_utc_micros);
        Self::new(stmt_ts, tz)
    }
}

#[derive(Clone)]
pub struct EvalContext {
    pub time_zone: SessionTimeZone,
    pub statement_time: Option<StatementTimeContext>,
}

impl EvalContext {
    pub fn new(time_zone: SessionTimeZone) -> Self {
        Self {
            time_zone,
            statement_time: None,
        }
    }

    pub fn for_statement(session: &Session) -> Self {
        let tz = session.time_zone();
        let statement_time = StatementTimeContext::capture(session);
        Self {
            time_zone: tz,
            statement_time: Some(statement_time),
        }
    }

    pub fn with_statement_time(
        time_zone: SessionTimeZone,
        statement_time: StatementTimeContext,
    ) -> Self {
        Self {
            time_zone,
            statement_time: Some(statement_time),
        }
    }

    pub fn from_session(session: &Session) -> Self {
        let time_zone = session.time_zone();
        let statement_time = session
            .statement_time_micros()
            .map(|micros| StatementTimeContext::new(micros, time_zone.clone()));
        Self {
            time_zone,
            statement_time,
        }
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        let tz = SessionTimeZone::Utc;
        Self {
            time_zone: tz.clone(),
            statement_time: Some(StatementTimeContext::new(now_utc_micros(), tz)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvalMode {
    Normal,
    ColumnDefault,
}

pub fn eval_scalar_expr(
    row: &[Value],
    expr: &ScalarExpr,
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<Value> {
    eval_scalar_expr_with_mode(row, expr, params, ctx, EvalMode::Normal)
}

pub fn eval_scalar_expr_with_mode(
    row: &[Value],
    expr: &ScalarExpr,
    params: &[Value],
    ctx: &EvalContext,
    mode: EvalMode,
) -> PgWireResult<Value> {
    match expr {
        ScalarExpr::Literal(v) => Ok(v.clone()),
        ScalarExpr::ColumnIdx(i) => row
            .get(*i)
            .cloned()
            .ok_or_else(|| fe("column index out of range")),
        ScalarExpr::Column(colref) => Err(fe(format!("unbound column reference: {colref}"))),
        ScalarExpr::Param { idx, .. } => params
            .get(*idx)
            .cloned()
            .ok_or_else(|| fe("parameter index out of range")),
        ScalarExpr::BinaryOp { op, left, right } => {
            let lv = eval_scalar_expr_with_mode(row, left, params, ctx, mode)?;
            let rv = eval_scalar_expr_with_mode(row, right, params, ctx, mode)?;
            eval_binary_op(*op, lv, rv)
        }
        ScalarExpr::UnaryOp { op, expr } => {
            let v = eval_scalar_expr_with_mode(row, expr, params, ctx, mode)?;
            eval_unary_op(*op, v)
        }
        ScalarExpr::Func { func, args } => {
            let mut evaluated = Vec::with_capacity(args.len());
            for arg in args {
                evaluated.push(eval_scalar_expr_with_mode(row, arg, params, ctx, mode)?);
            }
            eval_function(*func, evaluated, ctx, mode)
        }
        ScalarExpr::Cast { expr, ty } => {
            let value = eval_scalar_expr_with_mode(row, expr, params, ctx, mode)?;
            if matches!(value, Value::Null) {
                Ok(Value::Null)
            } else {
                cast_value_to_type(value, ty, &ctx.time_zone)
                    .map_err(|e| fe_code(e.code, e.message.clone()))
            }
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

fn eval_function(
    func: ScalarFunc,
    args: Vec<Value>,
    ctx: &EvalContext,
    mode: EvalMode,
) -> PgWireResult<Value> {
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
        ScalarFunc::CurrentSchema | ScalarFunc::CurrentSchemas | ScalarFunc::CurrentDatabase => {
            Err(fe("context-dependent function evaluated without binding"))
        }
        ScalarFunc::Now | ScalarFunc::CurrentTimestamp | ScalarFunc::StatementTimestamp => {
            ensure_no_args(&func, &args)?;
            Ok(Value::TimestamptzMicros(statement_timestamp(ctx)?))
        }
        ScalarFunc::TransactionTimestamp => {
            ensure_no_args(&func, &args)?;
            Ok(Value::TimestamptzMicros(statement_timestamp(ctx)?))
        }
        ScalarFunc::ClockTimestamp => {
            ensure_no_args(&func, &args)?;
            if matches!(mode, EvalMode::ColumnDefault) {
                return Err(fe("clock_timestamp() is not allowed in column defaults"));
            }
            Ok(Value::TimestamptzMicros(now_utc_micros()))
        }
        ScalarFunc::CurrentDate => {
            ensure_no_args(&func, &args)?;
            let micros = statement_timestamp(ctx)?;
            let days = timestamp_micros_to_date_days(micros).map_err(fe)?;
            Ok(Value::Date(days))
        }
    }
}

fn ensure_no_args(func: &ScalarFunc, args: &[Value]) -> PgWireResult<()> {
    if !args.is_empty() {
        return Err(fe(format!("{func:?}() takes no arguments")));
    }
    Ok(())
}

fn statement_timestamp(ctx: &EvalContext) -> PgWireResult<i64> {
    ctx.statement_time
        .as_ref()
        .map(|t| t.stmt_ts_utc_micros)
        .ok_or_else(|| fe("statement timestamp is not available in this context"))
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
        Value::Date(_) | Value::TimestampMicros(_) | Value::TimestamptzMicros(_) => {
            return Err(fe("text conversion not supported for date/timestamp"));
        }
    })
}

pub fn eval_bool_expr(
    row: &[Value],
    expr: &BoolExpr,
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<Option<bool>> {
    use std::cmp::Ordering;
    Ok(match expr {
        BoolExpr::Literal(b) => Some(*b),
        BoolExpr::Comparison { lhs, op, rhs } => {
            let lv = eval_scalar_expr(row, lhs, params, ctx)?;
            let rv = eval_scalar_expr(row, rhs, params, ctx)?;
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
                match eval_bool_expr(row, e, params, ctx)? {
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
                match eval_bool_expr(row, e, params, ctx)? {
                    Some(true) => return Ok(Some(true)),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }
            if saw_null { None } else { Some(false) }
        }
        BoolExpr::Not(inner) => match eval_bool_expr(row, inner, params, ctx)? {
            Some(v) => Some(!v),
            None => None,
        },
        BoolExpr::IsNull { expr, negated } => {
            let v = eval_scalar_expr(row, expr, params, ctx)?;
            match v {
                Value::Null => Some(!*negated),
                _ => Some(*negated),
            }
        }
    })
}

pub(super) fn compare_values(lhs: &Value, rhs: &Value) -> Option<std::cmp::Ordering> {
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
        (Value::TimestamptzMicros(a), Value::TimestamptzMicros(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => return None,
    })
}

pub async fn to_pgwire_stream(
    mut node: Box<dyn ExecNode>,
    fmt: FieldFormat,
    ctx: EvalContext,
) -> PgWireResult<(
    Arc<Vec<FieldInfo>>,
    impl Stream<Item = PgWireResult<DataRow>> + Send + 'static,
)> {
    let ctx = Arc::new(ctx);
    node.open().await?;
    let schema = node.schema().clone();
    let fields = Arc::new(
        schema
            .fields
            .iter()
            .map(|f| FieldInfo::new(f.name.clone(), None, None, f.data_type.to_pg(), fmt))
            .collect::<Vec<_>>(),
    );
    let ctx_stream = ctx.clone();
    let s = stream::unfold(
        (node, fields.clone(), schema),
        move |(mut node, fields, schema)| {
            let ctx = ctx_stream.clone();
            async move {
                match node.next().await {
                    Ok(Some(vals)) => {
                        let mut enc = DataRowEncoder::new(fields.clone());
                        for (i, v) in vals.into_iter().enumerate() {
                            let dt = &schema.field(i).data_type;
                            let res = match (v, dt) {
                                (Value::Null, DataType::Int4) => {
                                    enc.encode_field(&Option::<i32>::None)
                                }
                                (Value::Null, DataType::Int8) => {
                                    enc.encode_field(&Option::<i64>::None)
                                }
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
                                (Value::Null, DataType::Timestamptz) => {
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
                                (Value::Text(s), DataType::Text) => enc.encode_field(&s),
                                (Value::Bool(b), DataType::Bool) => enc.encode_field(&b),
                                (Value::Date(days), DataType::Date) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_days = date_days_to_postgres(days);
                                        enc.encode_field(&pg_days)
                                    } else {
                                        let text = match format_date(days) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
                                }
                                (Value::TimestampMicros(micros), DataType::Timestamp) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_micros = timestamp_to_postgres_micros(micros);
                                        enc.encode_field(&pg_micros)
                                    } else {
                                        let text = match format_timestamp(micros) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
                                }
                                (Value::TimestamptzMicros(micros), DataType::Timestamptz) => {
                                    if fmt == FieldFormat::Binary {
                                        let pg_micros = timestamp_to_postgres_micros(micros);
                                        enc.encode_field(&pg_micros)
                                    } else {
                                        let text = match format_timestamptz(micros, &ctx.time_zone)
                                        {
                                            Ok(t) => t,
                                            Err(e) => {
                                                return Some((Err(fe(e)), (node, fields, schema)));
                                            }
                                        };
                                        enc.encode_field(&text)
                                    }
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
                    Ok(None) => match node.close().await {
                        Ok(()) => None,
                        Err(e) => Some((Err(e), (node, fields, schema))),
                    },
                    Err(e) => Some((Err(e), (node, fields, schema))),
                }
            }
        },
    )
    .boxed();

    Ok((fields, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{parse_date_str, parse_timestamp_str};

    fn lit_int(v: i64) -> ScalarExpr {
        ScalarExpr::Literal(Value::Int64(v))
    }

    fn lit_text(v: &str) -> ScalarExpr {
        ScalarExpr::Literal(Value::Text(v.to_string()))
    }

    fn lit_float(v: f64) -> ScalarExpr {
        ScalarExpr::Literal(Value::from_f64(v))
    }

    fn eval(expr: &ScalarExpr) -> Value {
        eval_scalar_expr(&[], expr, &[], &EvalContext::default()).unwrap()
    }

    fn eval_bool(expr: &BoolExpr) -> Option<bool> {
        eval_bool_expr(&[], expr, &[], &EvalContext::default()).unwrap()
    }

    #[test]
    fn evaluates_all_arithmetic_ops() {
        let add = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Add,
            left: Box::new(lit_int(2)),
            right: Box::new(lit_int(3)),
        };
        assert_eq!(eval(&add), Value::Int64(5));

        let sub = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Sub,
            left: Box::new(lit_float(7.5)),
            right: Box::new(lit_int(2)),
        };
        assert_eq!(eval(&sub).as_f64().unwrap(), 5.5);

        let mul = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Mul,
            left: Box::new(lit_int(4)),
            right: Box::new(lit_int(3)),
        };
        assert_eq!(eval(&mul), Value::Int64(12));

        let div = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Div,
            left: Box::new(lit_int(9)),
            right: Box::new(lit_int(2)),
        };
        assert_eq!(eval(&div).as_f64().unwrap(), 4.5);
    }

    #[test]
    fn evaluates_concat_and_unary_ops() {
        let concat = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Concat,
            left: Box::new(lit_text("hello")),
            right: Box::new(lit_int(5)),
        };
        assert_eq!(eval(&concat), Value::Text("hello5".into()));

        let negate = ScalarExpr::UnaryOp {
            op: ScalarUnaryOp::Negate,
            expr: Box::new(lit_float(1.5)),
        };
        assert_eq!(eval(&negate).as_f64().unwrap(), -1.5);
    }

    #[test]
    fn evaluates_scalar_functions() {
        let upper = ScalarExpr::Func {
            func: ScalarFunc::Upper,
            args: vec![lit_text("hi")],
        };
        assert_eq!(eval(&upper), Value::Text("HI".into()));

        let lower = ScalarExpr::Func {
            func: ScalarFunc::Lower,
            args: vec![lit_text("LOUD")],
        };
        assert_eq!(eval(&lower), Value::Text("loud".into()));

        let len_bytes = ScalarExpr::Func {
            func: ScalarFunc::Length,
            args: vec![ScalarExpr::Literal(Value::Bytes(b"abc".to_vec()))],
        };
        assert_eq!(eval(&len_bytes), Value::Int64(3));
    }

    #[test]
    fn evaluates_bool_exprs_with_null_semantics() {
        let comparison = BoolExpr::Comparison {
            lhs: lit_int(5),
            op: CmpOp::Gt,
            rhs: lit_int(3),
        };
        assert_eq!(eval_bool(&comparison), Some(true));

        let null_cmp = BoolExpr::Comparison {
            lhs: ScalarExpr::Literal(Value::Null),
            op: CmpOp::Eq,
            rhs: lit_int(1),
        };
        assert_eq!(eval_bool(&null_cmp), None);

        let is_null = BoolExpr::IsNull {
            expr: ScalarExpr::Literal(Value::Null),
            negated: false,
        };
        assert_eq!(eval_bool(&is_null), Some(true));
    }

    #[test]
    fn casts_text_to_temporal_types() {
        let date_expr = ScalarExpr::Cast {
            expr: Box::new(lit_text("2024-02-01")),
            ty: DataType::Date,
        };
        let expected_date = Value::Date(parse_date_str("2024-02-01").unwrap());
        assert_eq!(eval(&date_expr), expected_date);

        let ts_expr = ScalarExpr::Cast {
            expr: Box::new(lit_text("2024-02-01 12:34:56")),
            ty: DataType::Timestamp,
        };
        let expected_ts =
            Value::TimestampMicros(parse_timestamp_str("2024-02-01 12:34:56").unwrap());
        assert_eq!(eval(&ts_expr), expected_ts);
    }

    #[test]
    fn column_default_allows_current_timestamp() {
        let expr = ScalarExpr::Func {
            func: ScalarFunc::CurrentTimestamp,
            args: vec![],
        };
        let tz = SessionTimeZone::Utc;
        let ctx =
            EvalContext::with_statement_time(tz.clone(), StatementTimeContext::new(42, tz.clone()));
        let value = eval_scalar_expr_with_mode(&[], &expr, &[], &ctx, EvalMode::ColumnDefault)
            .expect("evaluates timestamp");
        assert_eq!(value, Value::TimestamptzMicros(42));
    }

    #[test]
    fn column_default_rejects_clock_timestamp() {
        let expr = ScalarExpr::Func {
            func: ScalarFunc::ClockTimestamp,
            args: vec![],
        };
        let tz = SessionTimeZone::Utc;
        let ctx =
            EvalContext::with_statement_time(tz.clone(), StatementTimeContext::new(42, tz.clone()));
        let err = eval_scalar_expr_with_mode(&[], &expr, &[], &ctx, EvalMode::ColumnDefault)
            .expect_err("clock_timestamp blocked");
        let msg = format!("{err}");
        assert!(
            msg.contains("clock_timestamp"),
            "unexpected error message: {msg}"
        );
    }
}
