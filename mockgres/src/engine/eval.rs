use crate::types::{format_bytea, format_date, format_timestamp};
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
        ScalarExpr::Cast { expr, ty } => {
            let value = eval_scalar_expr(row, expr, params)?;
            if matches!(value, Value::Null) {
                Ok(Value::Null)
            } else {
                cast_value_to_type(value, ty).map_err(|e| fe_code(e.code, e.message.clone()))
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
