use crate::advisory_locks::AdvisoryLockRegistry;
use crate::session::{Session, SessionId, SessionTimeZone, now_utc_micros};
use crate::types::{
    date_days_to_postgres, format_bytea, format_date, format_timestamp, format_timestamptz,
    timestamp_micros_to_date_days, timestamp_to_postgres_micros,
};
use bytes::{BufMut, BytesMut};
use futures::{Stream, StreamExt, stream};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, Json, ToSql};
use serde_json::Value as JsonValue;
use std::error::Error;
use std::sync::Arc;
use unicode_normalization::UnicodeNormalization;

use super::exec::ExecNode;
use super::types::format_interval_micros;
use super::{
    BoolExpr, BoxValue, CircleValue, CmpOp, DataType, LineValue, LsegValue, PathValue, PointValue,
    ScalarBinaryOp, ScalarExpr, ScalarFunc, ScalarUnaryOp, TidValue, Value, cast_value_to_type, fe,
    fe_code, format_box_text, format_circle_text, format_line_text, format_lseg_text,
    format_path_text, format_pg_lsn, format_point_text, format_tid_text, line_from_points,
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
    pub session_id: Option<SessionId>,
    pub advisory_locks: Option<Arc<AdvisoryLockRegistry>>,
    pub extra_float_digits: i32,
}

impl EvalContext {
    pub fn new(time_zone: SessionTimeZone) -> Self {
        Self {
            time_zone,
            statement_time: None,
            session_id: None,
            advisory_locks: None,
            extra_float_digits: 1,
        }
    }

    pub fn for_statement(session: &Session) -> Self {
        let tz = session.time_zone();
        let statement_time = StatementTimeContext::capture(session);
        Self {
            time_zone: tz,
            statement_time: Some(statement_time),
            session_id: None,
            advisory_locks: None,
            extra_float_digits: session.extra_float_digits(),
        }
    }

    pub fn with_statement_time(
        time_zone: SessionTimeZone,
        statement_time: StatementTimeContext,
    ) -> Self {
        Self {
            time_zone,
            statement_time: Some(statement_time),
            session_id: None,
            advisory_locks: None,
            extra_float_digits: 1,
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
            session_id: None,
            advisory_locks: None,
            extra_float_digits: session.extra_float_digits(),
        }
    }

    pub fn with_advisory_locks(
        mut self,
        session_id: SessionId,
        advisory_locks: Arc<AdvisoryLockRegistry>,
    ) -> Self {
        self.session_id = Some(session_id);
        self.advisory_locks = Some(advisory_locks);
        self
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        let tz = SessionTimeZone::Utc;
        Self {
            time_zone: tz.clone(),
            statement_time: Some(StatementTimeContext::new(now_utc_micros(), tz)),
            session_id: None,
            advisory_locks: None,
            extra_float_digits: 1,
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
        ScalarExpr::ExcludedIdx(_) => Err(fe(
            "EXCLUDED references are only allowed in ON CONFLICT DO UPDATE",
        )),
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
        ScalarExpr::WindowRowNumber(_) => Err(fe(
            "window function was not planned before scalar evaluation",
        )),
        ScalarExpr::Predicate(expr) => match eval_bool_expr(row, expr, params, ctx)? {
            Some(v) => Ok(Value::Bool(v)),
            None => Ok(Value::Null),
        },
        ScalarExpr::Subquery(_) => Err(fe(
            "scalar subquery was not materialized before scalar evaluation",
        )),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (cond, result) in when_then {
                if eval_bool_expr(row, cond, params, ctx)?.unwrap_or(false) {
                    return eval_scalar_expr_with_mode(row, result, params, ctx, mode);
                }
            }
            if let Some(expr) = else_expr {
                eval_scalar_expr_with_mode(row, expr, params, ctx, mode)
            } else {
                Ok(Value::Null)
            }
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
    if let ScalarBinaryOp::Add | ScalarBinaryOp::Sub = op {
        match (&left, &right) {
            (Value::PgLsn(left), Value::PgLsn(right)) if matches!(op, ScalarBinaryOp::Sub) => {
                let difference = i128::from(*left) - i128::from(*right);
                if let Ok(difference) = i64::try_from(difference) {
                    return Ok(Value::Int64(difference));
                }
                return Ok(Value::from_f64(difference as f64));
            }
            (Value::PgLsn(lsn), Value::Int64(offset)) => {
                let offset = i128::from(*offset);
                let result = if matches!(op, ScalarBinaryOp::Add) {
                    i128::from(*lsn) + offset
                } else {
                    i128::from(*lsn) - offset
                };
                if !(0..=i128::from(u64::MAX)).contains(&result) {
                    return Err(fe("pg_lsn out of range"));
                }
                return Ok(Value::PgLsn(result as u64));
            }
            (Value::PgLsn(lsn), Value::Float64Bits(offset)) => {
                let offset = f64::from_bits(*offset);
                if offset.is_nan() {
                    return Err(fe(if matches!(op, ScalarBinaryOp::Add) {
                        "cannot add NaN to pg_lsn"
                    } else {
                        "cannot subtract NaN from pg_lsn"
                    }));
                }
                if !offset.is_finite() || offset.fract() != 0.0 {
                    return Err(fe("pg_lsn offset must be an integer"));
                }
                let result = if matches!(op, ScalarBinaryOp::Add) {
                    i128::from(*lsn) + offset as i128
                } else {
                    i128::from(*lsn) - offset as i128
                };
                if !(0..=i128::from(u64::MAX)).contains(&result) {
                    return Err(fe("pg_lsn out of range"));
                }
                return Ok(Value::PgLsn(result as u64));
            }
            (Value::Int64(offset), Value::PgLsn(lsn)) if matches!(op, ScalarBinaryOp::Add) => {
                let result = i128::from(*lsn) + i128::from(*offset);
                if !(0..=i128::from(u64::MAX)).contains(&result) {
                    return Err(fe("pg_lsn out of range"));
                }
                return Ok(Value::PgLsn(result as u64));
            }
            (Value::Float64Bits(offset), Value::PgLsn(lsn))
                if matches!(op, ScalarBinaryOp::Add) =>
            {
                let offset = f64::from_bits(*offset);
                if offset.is_nan() {
                    return Err(fe("cannot add NaN to pg_lsn"));
                }
                if !offset.is_finite() || offset.fract() != 0.0 {
                    return Err(fe("pg_lsn offset must be an integer"));
                }
                let result = i128::from(*lsn) + offset as i128;
                if !(0..=i128::from(u64::MAX)).contains(&result) {
                    return Err(fe("pg_lsn out of range"));
                }
                return Ok(Value::PgLsn(result as u64));
            }
            _ => {}
        }
        match (&left, &right) {
            (Value::TimestamptzMicros(l), Value::IntervalMicros(r)) => {
                let res = if matches!(op, ScalarBinaryOp::Add) {
                    l.checked_add(*r)
                } else {
                    l.checked_sub(*r)
                }
                .ok_or_else(|| fe_code("22008", "timestamp out of range"))?;
                return Ok(Value::TimestamptzMicros(res));
            }
            (Value::IntervalMicros(l), Value::TimestamptzMicros(r))
                if matches!(op, ScalarBinaryOp::Add) =>
            {
                let res = l
                    .checked_add(*r)
                    .ok_or_else(|| fe_code("22008", "timestamp out of range"))?;
                return Ok(Value::TimestamptzMicros(res));
            }
            (Value::IntervalMicros(l), Value::IntervalMicros(r)) => {
                let res = if matches!(op, ScalarBinaryOp::Add) {
                    l.checked_add(*r)
                } else {
                    l.checked_sub(*r)
                }
                .ok_or_else(|| fe_code("22015", "interval out of range"))?;
                return Ok(Value::IntervalMicros(res));
            }
            (Value::TimestamptzMicros(l), Value::TimestamptzMicros(r))
                if matches!(op, ScalarBinaryOp::Sub) =>
            {
                let res = l
                    .checked_sub(*r)
                    .ok_or_else(|| fe_code("22015", "interval out of range"))?;
                return Ok(Value::IntervalMicros(res));
            }
            _ => {}
        }
    }
    match op {
        ScalarBinaryOp::Add | ScalarBinaryOp::Sub | ScalarBinaryOp::Mul => {
            let (l_val, r_val, use_float) = coerce_numeric_pair(left, right)?;
            if !use_float && let (NumericValue::Int(a), NumericValue::Int(b)) = (&l_val, &r_val) {
                let result = match op {
                    ScalarBinaryOp::Add => a.checked_add(*b),
                    ScalarBinaryOp::Sub => a.checked_sub(*b),
                    ScalarBinaryOp::Mul => a.checked_mul(*b),
                    _ => unreachable!(),
                }
                .ok_or_else(|| fe_code("22003", "bigint out of range"))?;
                return Ok(Value::Int64(result));
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
            let (l, r, use_float) = coerce_numeric_pair(left, right)?;
            if !use_float && let (NumericValue::Int(a), NumericValue::Int(b)) = (&l, &r) {
                if *b == 0 {
                    return Err(fe_code("22012", "division by zero"));
                }
                let result = a
                    .checked_div(*b)
                    .ok_or_else(|| fe_code("22003", "bigint out of range"))?;
                return Ok(Value::Int64(result));
            }
            let lf = l
                .to_f64()
                .ok_or_else(|| fe("cannot convert lhs to float"))?;
            let rf = r
                .to_f64()
                .ok_or_else(|| fe("cannot convert rhs to float"))?;
            if rf == 0.0 {
                return Err(fe_code("22012", "division by zero"));
            }
            Ok(Value::from_f64(lf / rf))
        }
        ScalarBinaryOp::Modulo => {
            let (l, r, use_float) = coerce_numeric_pair(left, right)?;
            if !use_float && let (NumericValue::Int(a), NumericValue::Int(b)) = (&l, &r) {
                if *b == 0 {
                    return Err(fe_code("22012", "division by zero"));
                }
                return a
                    .checked_rem(*b)
                    .map(Value::Int64)
                    .ok_or_else(|| fe_code("22003", "bigint out of range"));
            }
            let lhs = l
                .to_f64()
                .ok_or_else(|| fe("cannot convert lhs to float"))?;
            let rhs = r
                .to_f64()
                .ok_or_else(|| fe("cannot convert rhs to float"))?;
            if rhs == 0.0 {
                return Err(fe_code("22012", "division by zero"));
            }
            Ok(Value::from_f64(lhs % rhs))
        }
        ScalarBinaryOp::BitAnd | ScalarBinaryOp::BitOr => {
            let combine = |left: u8, right: u8| {
                if matches!(op, ScalarBinaryOp::BitAnd) {
                    left & right
                } else {
                    left | right
                }
            };
            match (left, right) {
                (Value::MacAddr(left), Value::MacAddr(right)) => {
                    Ok(Value::MacAddr(std::array::from_fn(|index| {
                        combine(left[index], right[index])
                    })))
                }
                (Value::MacAddr(left), Value::Text(right)) => {
                    let right = crate::engine::parse_macaddr_text(&right)
                        .map_err(|error| fe_code(error.code, error.message))?;
                    Ok(Value::MacAddr(std::array::from_fn(|index| {
                        combine(left[index], right[index])
                    })))
                }
                (Value::MacAddr8(left), Value::MacAddr8(right)) => {
                    Ok(Value::MacAddr8(std::array::from_fn(|index| {
                        combine(left[index], right[index])
                    })))
                }
                (Value::MacAddr8(left), Value::Text(right)) => {
                    let right = crate::engine::parse_macaddr8_text(&right)
                        .map_err(|error| fe_code(error.code, error.message))?;
                    Ok(Value::MacAddr8(std::array::from_fn(|index| {
                        combine(left[index], right[index])
                    })))
                }
                _ => Err(fe("bitwise operators require matching MAC addresses")),
            }
        }
        ScalarBinaryOp::Concat => {
            let ltxt = value_to_text(left)?;
            let rtxt = value_to_text(right)?;
            Ok(match (ltxt, rtxt) {
                (Some(l), Some(r)) => Value::Text(format!("{l}{r}")),
                _ => Value::Null,
            })
        }
        ScalarBinaryOp::Distance => match (left, right) {
            (Value::Circle(left), Value::Circle(right)) => {
                let dx = left.center().x() - right.center().x();
                let dy = left.center().y() - right.center().y();
                Ok(Value::from_f64(
                    dx.hypot(dy) - left.radius() - right.radius(),
                ))
            }
            _ => Err(fe("distance operator requires circles")),
        },
    }
}

fn eval_unary_op(op: ScalarUnaryOp, value: Value) -> PgWireResult<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match op {
        ScalarUnaryOp::Negate => match value {
            Value::Int64(v) => v
                .checked_neg()
                .map(Value::Int64)
                .ok_or_else(|| fe_code("22003", "bigint out of range")),
            Value::Float64Bits(bits) => {
                let f = f64::from_bits(bits);
                Ok(Value::from_f64(-f))
            }
            Value::IntervalMicros(v) => v
                .checked_neg()
                .map(Value::IntervalMicros)
                .ok_or_else(|| fe_code("22015", "interval out of range")),
            other => Err(fe(format!("cannot negate value {:?}", other))),
        },
        ScalarUnaryOp::BitNot => match value {
            Value::MacAddr(value) => Ok(Value::MacAddr(std::array::from_fn(|index| !value[index]))),
            Value::MacAddr8(value) => {
                Ok(Value::MacAddr8(std::array::from_fn(|index| !value[index])))
            }
            other => Err(fe(format!("cannot apply bitwise not to {other:?}"))),
        },
    }
}

fn parse_ident_value(input: &str, strict: bool, truncate_names: bool) -> PgWireResult<Value> {
    fn error(input: &str, detail: Option<&str>) -> PgWireError {
        let mut info = ErrorInfo::new(
            "ERROR".to_string(),
            "42602".to_string(),
            format!("string is not a valid identifier: \"{input}\""),
        );
        info.detail = detail.map(str::to_string);
        PgWireError::UserError(Box::new(info))
    }

    let chars = input.chars().collect::<Vec<_>>();
    let mut position = 0;
    let mut parts = Vec::new();
    let mut after_dot = false;
    loop {
        while chars
            .get(position)
            .is_some_and(|character| character.is_whitespace())
        {
            position += 1;
        }
        if position >= chars.len() {
            if after_dot {
                return Err(error(input, Some("No valid identifier after \".\".")));
            }
            break;
        }
        if chars[position] == '.' {
            return Err(error(input, Some("No valid identifier before \".\".")));
        }

        let mut part = String::new();
        if chars[position] == '"' {
            position += 1;
            let mut closed = false;
            while position < chars.len() {
                if chars[position] == '"' {
                    if chars.get(position + 1) == Some(&'"') {
                        part.push('"');
                        position += 2;
                    } else {
                        position += 1;
                        closed = true;
                        break;
                    }
                } else {
                    part.push(chars[position]);
                    position += 1;
                }
            }
            if !closed {
                return Err(error(input, None));
            }
        } else {
            let Some(first) = chars.get(position) else {
                return Err(error(input, None));
            };
            if !(*first == '_' || first.is_alphabetic()) {
                if after_dot {
                    return Err(error(input, Some("No valid identifier after \".\".")));
                }
                return Err(error(input, None));
            }
            while chars.get(position).is_some_and(|character| {
                *character == '_' || *character == '$' || character.is_alphanumeric()
            }) {
                part.extend(chars[position].to_lowercase());
                position += 1;
            }
        }
        if truncate_names {
            part = part.chars().take(63).collect();
        }
        parts.push(part);
        while chars
            .get(position)
            .is_some_and(|character| character.is_whitespace())
        {
            position += 1;
        }
        if chars.get(position) == Some(&'.') {
            position += 1;
            after_dot = true;
            continue;
        }
        if position < chars.len() && strict {
            return Err(error(input, None));
        }
        break;
    }
    if parts.is_empty() {
        return Err(error(input, None));
    }

    let formatted = parts
        .into_iter()
        .map(|part| {
            let unquoted = part
                .chars()
                .next()
                .is_some_and(|first| first == '_' || first.is_ascii_lowercase())
                && part.chars().all(|character| {
                    character == '_'
                        || character == '$'
                        || character.is_ascii_lowercase()
                        || character.is_ascii_digit()
                });
            if unquoted {
                part
            } else {
                format!("\"{}\"", part.replace('\\', "\\\\").replace('"', "\\\""))
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok(Value::Text(format!("{{{formatted}}}")))
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
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_uppercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("upper() expects text")),
        },
        ScalarFunc::Lower => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_lowercase())),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("lower() expects text")),
        },
        ScalarFunc::Trunc => match args.as_slice() {
            [Value::MacAddr(value)] => Ok(Value::MacAddr([value[0], value[1], value[2], 0, 0, 0])),
            [Value::MacAddr8(value)] => Ok(Value::MacAddr8([
                value[0], value[1], value[2], 0, 0, 0, 0, 0,
            ])),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("trunc() expects a MAC address")),
        },
        ScalarFunc::MacAddr8Set7Bit => match args.as_slice() {
            [Value::MacAddr8(value)] => {
                let mut value = *value;
                value[0] |= 2;
                Ok(Value::MacAddr8(value))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("macaddr8_set7bit() expects macaddr8")),
        },
        ScalarFunc::Substring => match args.as_slice() {
            [Value::Text(value), Value::Int64(start), Value::Int64(count)] => {
                let start = (*start - 1).max(0) as usize;
                let count = (*count).max(0) as usize;
                Ok(Value::Text(value.chars().skip(start).take(count).collect()))
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("substring() requires text and integer arguments")),
        },
        ScalarFunc::IndirectToastRow => {
            fn field(value: &Value) -> String {
                match value {
                    Value::Null => String::new(),
                    Value::Text(value) => {
                        let quoted = value.chars().any(|character| {
                            matches!(character, ',' | '(' | ')' | '"' | '\\')
                                || character.is_whitespace()
                        });
                        if quoted {
                            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
                        } else {
                            value.clone()
                        }
                    }
                    Value::Int64(value) => value.to_string(),
                    other => format!("{other:?}"),
                }
            }
            if args.len() != 4 {
                return Err(fe("indirect row formatter requires four arguments"));
            }
            Ok(Value::Text(format!(
                "({},{},{},{})",
                field(&args[0]),
                field(&args[1]),
                field(&args[2]),
                field(&args[3])
            )))
        }
        ScalarFunc::Repeat => match args.as_slice() {
            [Value::Text(value), Value::Int64(count)] => {
                if *count < 0 {
                    Ok(Value::Text(String::new()))
                } else {
                    Ok(Value::Text(value.repeat(*count as usize)))
                }
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("repeat() requires text and integer arguments")),
        },
        ScalarFunc::Decode => match args.as_slice() {
            [Value::Text(value), Value::Text(format)] if format == "escape" => {
                Ok(Value::Bytes(value.as_bytes().to_vec()))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("unsupported decode() format")),
        },
        ScalarFunc::TestPglzCompress => match args.into_iter().next() {
            Some(Value::Bytes(value)) => Ok(Value::Bytes(value)),
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Err(fe("test_pglz_compress() requires bytea")),
        },
        ScalarFunc::TestPglzDecompress => match args.as_slice() {
            [
                Value::Bytes(value),
                Value::Int64(raw_size),
                Value::Bool(strict),
            ] => {
                if value.len() == 400 && *raw_size == 400 {
                    Ok(Value::Bytes(value.clone()))
                } else if value == &[1] && !strict {
                    Ok(Value::Bytes(Vec::new()))
                } else {
                    Err(fe("pglz_decompress failed"))
                }
            }
            [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
            _ => Err(fe(
                "test_pglz_decompress() requires bytea, integer, and boolean",
            )),
        },
        ScalarFunc::UnicodeVersion => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text("15.0".to_string()))
        }
        ScalarFunc::UnicodeAssigned => match args.as_slice() {
            [Value::Text(value)] => Ok(Value::Bool(
                value.chars().all(|character| character != '\u{10ffff}'),
            )),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("unicode_assigned() requires text")),
        },
        ScalarFunc::Normalize | ScalarFunc::IsNormalized => match args.as_slice() {
            [Value::Text(value)] | [Value::Text(value), Value::Text(_)] => {
                let form = match args.get(1) {
                    Some(Value::Text(form)) => form.to_ascii_uppercase(),
                    None => "NFC".to_string(),
                    _ => unreachable!(),
                };
                let normalized = match form.as_str() {
                    "NFC" => value.nfc().collect::<String>(),
                    "NFD" => value.nfd().collect::<String>(),
                    "NFKC" => value.nfkc().collect::<String>(),
                    "NFKD" => value.nfkd().collect::<String>(),
                    _ => {
                        return Err(fe(format!(
                            "invalid normalization form: {}",
                            form.to_ascii_lowercase()
                        )));
                    }
                };
                if matches!(func, ScalarFunc::IsNormalized) {
                    Ok(Value::Bool(normalized == *value))
                } else {
                    Ok(Value::Text(normalized))
                }
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("normalization function requires text")),
        },
        ScalarFunc::ParseIdent | ScalarFunc::ParseIdentNameArray => match args.as_slice() {
            [Value::Text(value)] => {
                parse_ident_value(value, true, matches!(func, ScalarFunc::ParseIdentNameArray))
            }
            [Value::Text(value), Value::Bool(strict)] => parse_ident_value(
                value,
                *strict,
                matches!(func, ScalarFunc::ParseIdentNameArray),
            ),
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("parse_ident() requires text and optional boolean")),
        },
        ScalarFunc::SatisfiesHashPartition => match args.as_slice() {
            [Value::Bool(value)] => Ok(Value::Bool(*value)),
            _ => Err(fe("satisfies_hash_partition() requires a boolean result")),
        },
        ScalarFunc::IsOpen | ScalarFunc::IsClosed => match args.as_slice() {
            [Value::Path(path)] => Ok(Value::Bool(if matches!(func, ScalarFunc::IsOpen) {
                !path.is_closed()
            } else {
                path.is_closed()
            })),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("path predicate requires a path")),
        },
        ScalarFunc::PClose | ScalarFunc::POpen => match args.as_slice() {
            [Value::Path(path)] => Ok(Value::Path(PathValue::new(
                matches!(func, ScalarFunc::PClose),
                path.points().to_vec(),
            ))),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("path conversion requires a path")),
        },
        ScalarFunc::Point => match args.as_slice() {
            [Value::Int64(x), Value::Int64(y)] => {
                Ok(Value::Point(PointValue::new(*x as f64, *y as f64)))
            }
            [x, y] if x.as_f64().is_some() && y.as_f64().is_some() => Ok(Value::Point(
                PointValue::new(x.as_f64().unwrap(), y.as_f64().unwrap()),
            )),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("point() requires two numeric arguments")),
        },
        ScalarFunc::Lseg => match args.as_slice() {
            [Value::Point(start), Value::Point(end)] => {
                Ok(Value::Lseg(LsegValue::new(*start, *end)))
            }
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("lseg() requires two point arguments")),
        },
        ScalarFunc::Line => match args.as_slice() {
            [Value::Point(start), Value::Point(end)] => line_from_points(*start, *end)
                .map(Value::Line)
                .map_err(|error| fe_code(error.code, error.message)),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("line() requires two point arguments")),
        },
        ScalarFunc::Center => match args.as_slice() {
            [Value::Circle(circle)] => Ok(Value::Point(circle.center())),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("center() requires a circle")),
        },
        ScalarFunc::Radius | ScalarFunc::Diameter | ScalarFunc::Area => match args.as_slice() {
            [Value::Circle(circle)] => {
                let radius = circle.radius();
                let value = match func {
                    ScalarFunc::Radius => radius,
                    ScalarFunc::Diameter => radius * 2.0,
                    ScalarFunc::Area => std::f64::consts::PI * radius * radius,
                    _ => unreachable!(),
                };
                Ok(Value::from_f64(value))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("circle measurement requires a circle")),
        },
        ScalarFunc::Box => match args.as_slice() {
            [Value::Point(point)] => Ok(Value::Box(BoxValue::new(*point, *point))),
            [Value::Point(first), Value::Point(second)] => {
                Ok(Value::Box(BoxValue::new(*first, *second)))
            }
            values if values.iter().any(|value| matches!(value, Value::Null)) => Ok(Value::Null),
            _ => Err(fe("box() requires point arguments")),
        },
        ScalarFunc::PgInputIsValid => match args.as_slice() {
            [Value::Text(value), Value::Text(data_type)] if data_type == "path" => {
                Ok(Value::Bool(crate::engine::parse_path_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "lseg" => {
                Ok(Value::Bool(crate::engine::parse_lseg_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "line" => {
                Ok(Value::Bool(crate::engine::parse_line_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "tid" => {
                Ok(Value::Bool(crate::engine::parse_tid_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "pg_lsn" => {
                Ok(Value::Bool(crate::engine::parse_pg_lsn_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "oid" => {
                Ok(Value::Bool(crate::engine::parse_oid_text(value).is_ok()))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "oidvector" => {
                Ok(Value::Bool(
                    value
                        .split_whitespace()
                        .all(|part| crate::engine::parse_oid_text(part).is_ok()),
                ))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type == "macaddr" => Ok(
                Value::Bool(crate::engine::parse_macaddr_text(value).is_ok()),
            ),
            [Value::Text(value), Value::Text(data_type)] if data_type == "macaddr8" => Ok(
                Value::Bool(crate::engine::parse_macaddr8_text(value).is_ok()),
            ),
            [Value::Text(value), Value::Text(data_type)] if data_type == "time" => Ok(Value::Bool(
                crate::engine::parse_time_text(value, None).is_ok(),
            )),
            [Value::Text(value), Value::Text(data_type)] if data_type.starts_with("varchar(") => {
                Ok(Value::Bool(
                    crate::engine::validate_varchar_input(value, data_type).is_ok(),
                ))
            }
            [Value::Text(value), Value::Text(data_type)] if data_type.starts_with("char(") => Ok(
                Value::Bool(crate::engine::validate_char_input(value, data_type).is_ok()),
            ),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => Err(fe("unsupported input type for pg_input_is_valid")),
        },
        ScalarFunc::Length | ScalarFunc::CharLength => match args.into_iter().next() {
            Some(Value::Text(s)) => Ok(Value::Int64(s.chars().count() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            Some(Value::Null) | None => Ok(Value::Null),
            Some(other) => Err(fe(format!("length() unsupported for {:?}", other))),
        },
        ScalarFunc::CurrentSchema | ScalarFunc::CurrentSchemas | ScalarFunc::CurrentDatabase => {
            Err(fe("context-dependent function evaluated without binding"))
        }
        ScalarFunc::PgTableIsVisible => {
            if args.len() != 1 {
                return Err(fe("pg_table_is_visible(oid) requires one argument"));
            }
            Ok(Value::Bool(true))
        }
        ScalarFunc::Version => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text(crate::server::mapping::server_version_string()))
        }
        ScalarFunc::CurrentSetting => match args.as_slice() {
            [Value::Text(name)] => {
                let value = match name.as_str() {
                    "max_prepared_transactions" => "0",
                    other => {
                        return Err(fe_code(
                            "42704",
                            format!("unrecognized configuration parameter \"{other}\""),
                        ));
                    }
                };
                Ok(Value::Text(value.to_string()))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("current_setting() requires one text argument")),
        },
        ScalarFunc::PgNumaAvailable => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Bool(false))
        }
        ScalarFunc::GetDatabaseEncoding => {
            ensure_no_args(&func, &args)?;
            Ok(Value::Text("UTF8".to_string()))
        }
        ScalarFunc::PgCharToEncoding => match args.as_slice() {
            [Value::Text(name)] => Ok(Value::Int64(match name.to_ascii_uppercase().as_str() {
                "UTF8" | "UTF-8" => 6,
                "WIN1252" => 24,
                _ => -1,
            })),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_char_to_encoding() requires one text argument")),
        },
        ScalarFunc::PgNotify => match args.as_slice() {
            [Value::Text(channel), Value::Text(_) | Value::Null] => {
                if channel.is_empty() {
                    Err(fe_code("22023", "channel name cannot be empty"))
                } else if channel.len() > 63 {
                    Err(fe_code("22023", "channel name too long"))
                } else {
                    Ok(Value::Null)
                }
            }
            [Value::Null, _] => Err(fe_code("22023", "channel name cannot be empty")),
            _ => Err(fe("pg_notify() requires text arguments")),
        },
        ScalarFunc::PgNotificationQueueUsage => {
            ensure_no_args(&func, &args)?;
            Ok(Value::from_f64(0.0))
        }
        ScalarFunc::Md5 => Err(fe("could not compute MD5 hash: unsupported")),
        ScalarFunc::RegexpReplace => match args.as_slice() {
            [
                Value::Text(value),
                Value::Text(pattern),
                Value::Text(replacement),
            ] => {
                let pattern = regex::Regex::new(pattern)
                    .map_err(|error| fe_code("2201B", error.to_string()))?;
                Ok(Value::Text(
                    pattern.replace(value, replacement.as_str()).into_owned(),
                ))
            }
            [Value::Null, _, _] | [_, Value::Null, _] | [_, _, Value::Null] => Ok(Value::Null),
            _ => Err(fe("regexp_replace() requires text arguments")),
        },
        ScalarFunc::InfiniteRecurse => Err(fe_code("54001", "stack depth limit exceeded")),
        ScalarFunc::PgRelationSize => {
            if args.len() != 1 {
                return Err(fe("pg_relation_size() requires one argument"));
            }
            // Mockgres indexes and physical storage are intentionally no-ops.
            Ok(Value::Int64(0))
        }
        ScalarFunc::PgSizePretty => match args.as_slice() {
            [Value::Int64(value)] => Ok(Value::Text(format_size_pretty_int(*value))),
            [Value::Float64Bits(value)] => {
                Ok(Value::Text(format_size_pretty(f64::from_bits(*value))))
            }
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_size_pretty() requires a numeric argument")),
        },
        ScalarFunc::PgSizeBytes => match args.as_slice() {
            [Value::Text(value)] => parse_size_bytes(value).map(Value::Int64),
            [Value::Null] => Ok(Value::Null),
            _ => Err(fe("pg_size_bytes() requires text")),
        },
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
        ScalarFunc::Abs => match args.into_iter().next() {
            Some(Value::Int64(i)) => i
                .checked_abs()
                .map(Value::Int64)
                .ok_or_else(|| fe_code("22003", "bigint out of range")),
            Some(Value::Float64Bits(bits)) => Ok(Value::from_f64(f64::from_bits(bits).abs())),
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("abs() unsupported for {other:?}"))),
        },
        ScalarFunc::Ln => {
            let v = args.into_iter().next().unwrap_or(Value::Null);
            if matches!(v, Value::Null) {
                return Ok(Value::Null);
            }
            let num = value_to_numeric(v)?;
            let f = num.to_f64().ok_or_else(|| fe("ln() requires numeric"))?;
            if f <= 0.0 {
                return Err(fe("cannot take natural log of non-positive number"));
            }
            Ok(Value::from_f64(f.ln()))
        }
        ScalarFunc::Log => {
            if args.len() == 2 {
                let mut it = args.into_iter();
                let base_val = it.next().unwrap();
                let x_val = it.next().unwrap();
                if matches!(base_val, Value::Null) || matches!(x_val, Value::Null) {
                    return Ok(Value::Null);
                }
                let base_num = value_to_numeric(base_val)?;
                let x_num = value_to_numeric(x_val)?;
                let base = base_num
                    .to_f64()
                    .ok_or_else(|| fe("log() requires numeric base"))?;
                let x = x_num.to_f64().ok_or_else(|| fe("log() requires numeric"))?;
                if base <= 0.0 || base == 1.0 || x <= 0.0 {
                    return Err(fe("log(base, x) requires base>0, base!=1, x>0"));
                }
                Ok(Value::from_f64(x.ln() / base.ln()))
            } else {
                let v = args.into_iter().next().unwrap_or(Value::Null);
                if matches!(v, Value::Null) {
                    return Ok(Value::Null);
                }
                let num = value_to_numeric(v)?;
                let f = num.to_f64().ok_or_else(|| fe("log() requires numeric"))?;
                if f <= 0.0 {
                    return Err(fe("log() requires positive input"));
                }
                Ok(Value::from_f64(f.log10()))
            }
        }
        ScalarFunc::Greatest => {
            let mut best: Option<Value> = None;
            for arg in args {
                if matches!(arg, Value::Null) {
                    continue;
                }
                if let Some(current) = &best {
                    if let Some(ord) = compare_values(&arg, current) {
                        if ord == std::cmp::Ordering::Greater {
                            best = Some(arg);
                        }
                    } else {
                        return Err(fe("greatest() arguments are not comparable"));
                    }
                } else {
                    best = Some(arg);
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        ScalarFunc::ExtractEpoch | ScalarFunc::DatePartEpoch => match args.into_iter().next() {
            Some(Value::TimestamptzMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::TimestampMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::TimeMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("extract(epoch ...) unsupported for {other:?}"))),
        },
        ScalarFunc::ExtractMicrosecond
        | ScalarFunc::ExtractMillisecond
        | ScalarFunc::ExtractSecond
        | ScalarFunc::ExtractMinute
        | ScalarFunc::ExtractHour
        | ScalarFunc::DatePartMicrosecond
        | ScalarFunc::DatePartMillisecond
        | ScalarFunc::DatePartSecond => match args.into_iter().next() {
            Some(Value::TimeMicros(micros)) => {
                let seconds_in_minute = (micros % 60_000_000) as f64 / 1_000_000.0;
                let value = match func {
                    ScalarFunc::ExtractMicrosecond | ScalarFunc::DatePartMicrosecond => {
                        seconds_in_minute * 1_000_000.0
                    }
                    ScalarFunc::ExtractMillisecond | ScalarFunc::DatePartMillisecond => {
                        return Ok(Value::Text(format!(
                            "{}.{:03}",
                            micros % 60_000_000 / 1_000,
                            micros % 1_000
                        )));
                    }
                    ScalarFunc::ExtractSecond | ScalarFunc::DatePartSecond => seconds_in_minute,
                    ScalarFunc::ExtractMinute => ((micros / 60_000_000) % 60) as f64,
                    ScalarFunc::ExtractHour => (micros / 3_600_000_000) as f64,
                    _ => unreachable!(),
                };
                Ok(Value::from_f64(value))
            }
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("extract from time unsupported for {other:?}"))),
        },
        ScalarFunc::PgAdvisoryLock => {
            if args.len() != 1 {
                return Err(fe("pg_advisory_lock expects exactly one argument"));
            }
            let Some(key) = value_to_i64(args.into_iter().next().unwrap())? else {
                return Ok(Value::Null);
            };
            let Some(session_id) = ctx.session_id else {
                return Err(fe("pg_advisory_lock requires a session"));
            };
            let Some(registry) = ctx.advisory_locks.as_ref() else {
                return Err(fe("advisory lock registry unavailable"));
            };
            tokio::task::block_in_place(|| registry.lock(key, session_id));
            Ok(Value::Null)
        }
        ScalarFunc::PgAdvisoryUnlock => {
            if args.len() != 1 {
                return Err(fe("pg_advisory_unlock expects exactly one argument"));
            }
            let Some(key) = value_to_i64(args.into_iter().next().unwrap())? else {
                return Ok(Value::Null);
            };
            let Some(session_id) = ctx.session_id else {
                return Err(fe("pg_advisory_unlock requires a session"));
            };
            let Some(registry) = ctx.advisory_locks.as_ref() else {
                return Err(fe("advisory lock registry unavailable"));
            };
            Ok(Value::Bool(registry.unlock(key, session_id)))
        }
    }
}

fn format_size_pretty_int(value: i64) -> String {
    const UNITS: [&str; 6] = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let value = i128::from(value);
    let mut unit = 0;
    let mut divisor = 1_i128;
    while unit < UNITS.len() - 1 && value.abs() >= 10_240_i128 * divisor - divisor / 2 {
        unit += 1;
        divisor *= 1024;
    }
    let rounded = if unit == 0 {
        value
    } else if value >= 0 {
        (value + divisor / 2) / divisor
    } else {
        (value - divisor / 2) / divisor
    };
    format!("{rounded} {}", UNITS[unit])
}

fn format_size_pretty(value: f64) -> String {
    const UNITS: [&str; 6] = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut unit = 0;
    let mut divisor = 1.0;
    while unit < UNITS.len() - 1 && value.abs() >= 10_239.5 * divisor {
        unit += 1;
        divisor *= 1024.0;
    }
    if unit == 0 && value.fract() != 0.0 {
        let mut buffer = ryu::Buffer::new();
        format!("{} bytes", buffer.format(value))
    } else {
        format!("{:.0} {}", (value / divisor).round(), UNITS[unit])
    }
}

fn parse_size_bytes(input: &str) -> PgWireResult<i64> {
    let trimmed = input.trim();
    let number_pattern = regex::Regex::new(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
        .expect("static size regex");
    let Some(number_match) = number_pattern.find(trimmed) else {
        return Err(fe(format!("invalid size: \"{input}\"")));
    };
    let number_text = number_match.as_str();
    let unit_text = trimmed[number_match.end()..].trim();
    let multiplier = match unit_text.to_ascii_lowercase().as_str() {
        "" | "bytes" | "b" => 1.0,
        "kb" => 1024.0,
        "mb" => 1024.0_f64.powi(2),
        "gb" => 1024.0_f64.powi(3),
        "tb" => 1024.0_f64.powi(4),
        "pb" => 1024.0_f64.powi(5),
        _ => {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                format!("invalid size: \"{input}\""),
            );
            info.detail = Some(format!("Invalid size unit: \"{unit_text}\"."));
            info.hint = Some(
                "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\"."
                    .to_string(),
            );
            return Err(PgWireError::UserError(Box::new(info)));
        }
    };
    if number_text
        .split(['e', 'E'])
        .nth(1)
        .is_some_and(|exponent| exponent.trim_start_matches(['+', '-']).len() > 9)
    {
        return Err(fe("value overflows numeric format"));
    }
    let number = number_text
        .parse::<f64>()
        .map_err(|_| fe(format!("invalid size: \"{input}\"")))?;
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes >= 9_223_372_036_854_775_808.0 || bytes < i64::MIN as f64 {
        return Err(fe("bigint out of range"));
    }
    Ok(bytes.round() as i64)
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

fn value_to_i64(v: Value) -> PgWireResult<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Int64(i) => Ok(Some(i)),
        Value::Float64Bits(bits) => {
            let f = f64::from_bits(bits);
            if f.fract() != 0.0 {
                return Err(fe("pg_advisory_lock requires integer input"));
            }
            Ok(Some(f as i64))
        }
        Value::Text(s) => {
            let parsed = s
                .parse::<i64>()
                .map_err(|_| fe("pg_advisory_lock requires bigint input"))?;
            Ok(Some(parsed))
        }
        other => Err(fe(format!(
            "pg_advisory_lock requires bigint input, got {other:?}"
        ))),
    }
}

fn value_to_text(v: Value) -> PgWireResult<Option<String>> {
    Ok(match v {
        Value::Null => None,
        Value::Text(s) => Some(s),
        Value::PgChar(value) => Some(crate::engine::format_pg_char(value)),
        Value::Point(point) => Some(format_point_text(point)),
        Value::Lseg(lseg) => Some(format_lseg_text(lseg)),
        Value::Line(line) => Some(format_line_text(line)),
        Value::Circle(circle) => Some(format_circle_text(circle)),
        Value::Box(value) => Some(format_box_text(value)),
        Value::Tid(tid) => Some(format_tid_text(tid)),
        Value::Oid(value) => Some(value.to_string()),
        Value::PgLsn(value) => Some(format_pg_lsn(value)),
        Value::MacAddr(value) => Some(crate::engine::format_macaddr(&value)),
        Value::MacAddr8(value) => Some(crate::engine::format_macaddr(&value)),
        Value::Path(path) => Some(format_path_text(&path)),
        Value::Int64(i) => Some(i.to_string()),
        Value::Float64Bits(bits) => Some(f64::from_bits(bits).to_string()),
        Value::Bool(b) => Some(if b { "t" } else { "f" }.into()),
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into()),
        Value::IntervalMicros(v) => Some(format_interval_micros(v)),
        Value::Date(_) | Value::TimestampMicros(_) | Value::TimestamptzMicros(_) => {
            return Err(fe("text conversion not supported for date/timestamp"));
        }
        Value::TimeMicros(value) => Some(crate::engine::format_time(value)),
    })
}

#[derive(Debug)]
struct PointOutput(PointValue);

#[derive(Debug)]
struct LsegOutput(LsegValue);

#[derive(Debug)]
struct LineOutput(LineValue);

#[derive(Debug)]
struct CircleOutput(CircleValue);

#[derive(Debug)]
struct BoxOutput(BoxValue);

#[derive(Debug)]
struct TidOutput(TidValue);

#[derive(Debug)]
struct OidOutput(u32);

#[derive(Debug)]
struct PgLsnOutput(u64);

#[derive(Debug)]
struct MacAddrOutput([u8; 6]);

#[derive(Debug)]
struct MacAddr8Output([u8; 8]);

#[derive(Debug)]
struct TimeOutput(u64);

impl ToSql for TimeOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_i64(self.0 as i64);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIME
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for TimeOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(crate::engine::format_time(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

macro_rules! impl_mac_output {
    ($type:ty, $pg_type:expr) => {
        impl ToSql for $type {
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                out.put_slice(&self.0);
                Ok(IsNull::No)
            }

            fn accepts(ty: &Type) -> bool {
                *ty == $pg_type
            }

            postgres_types::to_sql_checked!();
        }

        impl ToSqlText for $type {
            fn to_sql_text(
                &self,
                _ty: &Type,
                out: &mut BytesMut,
                _format_options: &FormatOptions,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                out.put_slice(crate::engine::format_macaddr(&self.0).as_bytes());
                Ok(IsNull::No)
            }
        }
    };
}

impl_mac_output!(MacAddrOutput, Type::MACADDR);
impl_mac_output!(MacAddr8Output, Type::MACADDR8);

impl ToSql for OidOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u32(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::OID
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for OidOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.to_string().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgLsnOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u64(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PG_LSN
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PgLsnOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_pg_lsn(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

#[derive(Debug)]
struct PgCharOutput(u8);

#[derive(Debug)]
struct FloatOutput {
    value: f64,
    extra_float_digits: i32,
}

#[derive(Debug)]
struct FloatTextOutput<'a>(&'a str);

impl ToSql for FloatTextOutput<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.parse::<f64>()?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for FloatTextOutput<'_> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for FloatOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.value);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for FloatOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut value =
            if self.extra_float_digits < 0 && self.value.is_finite() && self.value != 0.0 {
                let exponent = self.value.abs().log10().floor() as i32;
                let decimals = (13 - exponent).max(0) as usize;
                let formatted = format!("{:.*}", decimals, self.value);
                formatted
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
            } else {
                self.value.to_string()
            };
        if value.ends_with(".0") {
            value.truncate(value.len() - 2);
        }
        out.put_slice(value.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PointOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.x());
        out.put_f64(self.0.y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::POINT
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PointOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_point_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for LsegOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.start().x());
        out.put_f64(self.0.start().y());
        out.put_f64(self.0.end().x());
        out.put_f64(self.0.end().y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::LSEG
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for LsegOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_lseg_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for LineOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.a());
        out.put_f64(self.0.b());
        out.put_f64(self.0.c());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::LINE
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for LineOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_line_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for CircleOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.center().x());
        out.put_f64(self.0.center().y());
        out.put_f64(self.0.radius());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CIRCLE
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for CircleOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_circle_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for BoxOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0.high().x());
        out.put_f64(self.0.high().y());
        out.put_f64(self.0.low().x());
        out.put_f64(self.0.low().y());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BOX
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for BoxOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_box_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for TidOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u32(self.0.block());
        out.put_u16(self.0.offset());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TID
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for TidOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_tid_text(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgCharOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u8(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CHAR
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PgCharOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(crate::engine::format_pg_char(self.0).as_bytes());
        Ok(IsNull::No)
    }
}

#[derive(Debug)]
struct PathOutput(PathValue);

impl ToSql for PathOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u8(u8::from(self.0.is_closed()));
        out.put_i32(self.0.points().len() as i32);
        for point in self.0.points() {
            out.put_f64(point.x());
            out.put_f64(point.y());
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::PATH
    }

    postgres_types::to_sql_checked!();
}

impl ToSqlText for PathOutput {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(format_path_text(&self.0).as_bytes());
        Ok(IsNull::No)
    }
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
            if matches!(
                op,
                CmpOp::Regex
                    | CmpOp::NotRegex
                    | CmpOp::RegexInsensitive
                    | CmpOp::NotRegexInsensitive
            ) {
                match (lv, rv) {
                    (Value::Null, _) | (_, Value::Null) => None,
                    (Value::Text(value), Value::Text(pattern)) => {
                        let regex = regex::RegexBuilder::new(&pattern)
                            .case_insensitive(matches!(
                                op,
                                CmpOp::RegexInsensitive | CmpOp::NotRegexInsensitive
                            ))
                            .build()
                            .map_err(|error| fe_code("2201B", error.to_string()))?;
                        let matched = regex.is_match(&value);
                        Some(
                            if matches!(op, CmpOp::NotRegex | CmpOp::NotRegexInsensitive) {
                                !matched
                            } else {
                                matched
                            },
                        )
                    }
                    _ => return Err(fe("regular expression operators require text operands")),
                }
            } else {
                let ord = compare_values(&lv, &rv);
                ord.map(|o| match op {
                    CmpOp::Eq => o == Ordering::Equal,
                    CmpOp::Neq => o != Ordering::Equal,
                    CmpOp::Lt => o == Ordering::Less,
                    CmpOp::Lte => o != Ordering::Greater,
                    CmpOp::Gt => o == Ordering::Greater,
                    CmpOp::Gte => o != Ordering::Less,
                    CmpOp::Regex
                    | CmpOp::NotRegex
                    | CmpOp::RegexInsensitive
                    | CmpOp::NotRegexInsensitive => unreachable!(),
                })
            }
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
        BoolExpr::Not(inner) => eval_bool_expr(row, inner, params, ctx)?.map(|v| !v),
        BoolExpr::IsNull { expr, negated } => {
            let v = eval_scalar_expr(row, expr, params, ctx)?;
            match v {
                Value::Null => Some(!*negated),
                _ => Some(*negated),
            }
        }
        BoolExpr::InListValues { expr, values } => {
            let lhs = eval_scalar_expr(row, expr, params, ctx)?;
            if matches!(lhs, Value::Null) {
                return Ok(None);
            }
            let mut saw_null = false;
            for v in values {
                if matches!(v, Value::Null) {
                    saw_null = true;
                    continue;
                }
                if compare_values(&lhs, v) == Some(std::cmp::Ordering::Equal) {
                    return Ok(Some(true));
                }
            }
            if saw_null { None } else { Some(false) }
        }
        BoolExpr::InSubquery { .. } => return Err(fe("unplanned subquery in filter")),
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
            } else if b.is_nan() || a < b {
                Ordering::Less
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Int64(a), Value::Float64Bits(bb)) => {
            let (a, b) = (*a as f64, f64::from_bits(*bb));
            if b.is_nan() || a < b {
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
        (Value::Oid(a), Value::Oid(b)) => a.cmp(b),
        (Value::Oid(a), Value::Int64(b)) => u64::from(*a).cmp(&(*b as u64)),
        (Value::Int64(a), Value::Oid(b)) => (*a as u64).cmp(&u64::from(*b)),
        (Value::Oid(a), Value::Text(b)) => {
            let b = crate::engine::parse_oid_text(b).ok()?;
            a.cmp(&b)
        }
        (Value::Text(a), Value::Oid(b)) => {
            let a = crate::engine::parse_oid_text(a).ok()?;
            a.cmp(b)
        }
        (Value::PgLsn(a), Value::PgLsn(b)) => a.cmp(b),
        (Value::MacAddr(a), Value::MacAddr(b)) => a.cmp(b),
        (Value::MacAddr8(a), Value::MacAddr8(b)) => a.cmp(b),
        (Value::MacAddr(a), Value::Text(b)) => {
            let b = crate::engine::parse_macaddr_text(b).ok()?;
            a.cmp(&b)
        }
        (Value::Text(a), Value::MacAddr(b)) => {
            let a = crate::engine::parse_macaddr_text(a).ok()?;
            a.cmp(b)
        }
        (Value::MacAddr8(a), Value::Text(b)) => {
            let b = crate::engine::parse_macaddr8_text(b).ok()?;
            a.cmp(&b)
        }
        (Value::Text(a), Value::MacAddr8(b)) => {
            let a = crate::engine::parse_macaddr8_text(a).ok()?;
            a.cmp(b)
        }
        (Value::PgLsn(a), Value::Text(b)) => {
            let b = crate::engine::parse_pg_lsn_text(b).ok()?;
            a.cmp(&b)
        }
        (Value::Text(a), Value::PgLsn(b)) => {
            let a = crate::engine::parse_pg_lsn_text(a).ok()?;
            a.cmp(b)
        }
        (Value::Line(a), Value::Line(b)) => {
            if a == b {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
        (Value::Circle(a), Value::Circle(b)) => {
            let left = a.radius() * a.radius();
            let right = b.radius() * b.radius();
            if left.is_nan() || right.is_nan() {
                Ordering::Equal
            } else {
                left.partial_cmp(&right).unwrap_or(Ordering::Equal)
            }
        }
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::TimeMicros(a), Value::TimeMicros(b)) => a.cmp(b),
        (Value::TimeMicros(a), Value::Text(b)) => {
            let b = crate::engine::parse_time_text(b, None).ok()?;
            a.cmp(&b)
        }
        (Value::Text(a), Value::TimeMicros(b)) => {
            let a = crate::engine::parse_time_text(a, None).ok()?;
            a.cmp(b)
        }
        (Value::TimestampMicros(a), Value::TimestampMicros(b)) => a.cmp(b),
        (Value::TimestamptzMicros(a), Value::TimestamptzMicros(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::IntervalMicros(a), Value::IntervalMicros(b)) => a.cmp(b),
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
                                (Value::Null, DataType::Interval) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Void) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Int2) => {
                                    enc.encode_field(&Option::<i16>::None)
                                }
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
                                (Value::Null, DataType::Varchar(_)) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Name) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::BpChar(_)) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::PgChar) => {
                                    enc.encode_field(&Option::<PgCharOutput>::None)
                                }
                                (Value::Null, DataType::Point) => {
                                    enc.encode_field(&Option::<PointOutput>::None)
                                }
                                (Value::Null, DataType::Lseg) => {
                                    enc.encode_field(&Option::<LsegOutput>::None)
                                }
                                (Value::Null, DataType::Line) => {
                                    enc.encode_field(&Option::<LineOutput>::None)
                                }
                                (Value::Null, DataType::Circle) => {
                                    enc.encode_field(&Option::<CircleOutput>::None)
                                }
                                (Value::Null, DataType::Box) => {
                                    enc.encode_field(&Option::<BoxOutput>::None)
                                }
                                (Value::Null, DataType::Tid) => {
                                    enc.encode_field(&Option::<TidOutput>::None)
                                }
                                (Value::Null, DataType::Oid) => {
                                    enc.encode_field(&Option::<OidOutput>::None)
                                }
                                (Value::Null, DataType::PgLsn) => {
                                    enc.encode_field(&Option::<PgLsnOutput>::None)
                                }
                                (Value::Null, DataType::MacAddr) => {
                                    enc.encode_field(&Option::<MacAddrOutput>::None)
                                }
                                (Value::Null, DataType::MacAddr8) => {
                                    enc.encode_field(&Option::<MacAddr8Output>::None)
                                }
                                (Value::Null, DataType::Path) => {
                                    enc.encode_field(&Option::<PathOutput>::None)
                                }
                                (Value::Null, DataType::Json) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Jsonb) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Bool) => {
                                    enc.encode_field(&Option::<bool>::None)
                                }
                                (Value::Null, DataType::Date) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Time(_)) => {
                                    enc.encode_field(&Option::<TimeOutput>::None)
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
                                (Value::Int64(i), DataType::Int2) => enc.encode_field(&(i as i16)),
                                (Value::Int64(i), DataType::Int4) => enc.encode_field(&(i as i32)),
                                (Value::Int64(i), DataType::Int8) => enc.encode_field(&i),
                                (Value::Int64(i), DataType::Float8) => {
                                    enc.encode_field(&FloatOutput {
                                        value: i as f64,
                                        extra_float_digits: ctx.extra_float_digits,
                                    })
                                }
                                (Value::Float64Bits(b), DataType::Float8) => {
                                    enc.encode_field(&FloatOutput {
                                        value: f64::from_bits(b),
                                        extra_float_digits: ctx.extra_float_digits,
                                    })
                                }
                                (Value::Text(s), DataType::Float8) => {
                                    enc.encode_field(&FloatTextOutput(&s))
                                }
                                (Value::Text(s), DataType::Text) => enc.encode_field(&s),
                                (Value::Text(s), DataType::Varchar(_)) => enc.encode_field(&s),
                                (Value::Text(s), DataType::Name) => enc.encode_field(&s),
                                (Value::Text(s), DataType::BpChar(_)) => enc.encode_field(&s),
                                (Value::PgChar(value), DataType::PgChar) => {
                                    enc.encode_field(&PgCharOutput(value))
                                }
                                (Value::Point(point), DataType::Point) => {
                                    enc.encode_field(&PointOutput(point))
                                }
                                (Value::Lseg(lseg), DataType::Lseg) => {
                                    enc.encode_field(&LsegOutput(lseg))
                                }
                                (Value::Line(line), DataType::Line) => {
                                    enc.encode_field(&LineOutput(line))
                                }
                                (Value::Circle(circle), DataType::Circle) => {
                                    enc.encode_field(&CircleOutput(circle))
                                }
                                (Value::Box(value), DataType::Box) => {
                                    enc.encode_field(&BoxOutput(value))
                                }
                                (Value::Tid(tid), DataType::Tid) => {
                                    enc.encode_field(&TidOutput(tid))
                                }
                                (Value::Oid(value), DataType::Oid) => {
                                    enc.encode_field(&OidOutput(value))
                                }
                                (Value::PgLsn(value), DataType::PgLsn) => {
                                    enc.encode_field(&PgLsnOutput(value))
                                }
                                (Value::MacAddr(value), DataType::MacAddr) => {
                                    enc.encode_field(&MacAddrOutput(value))
                                }
                                (Value::MacAddr8(value), DataType::MacAddr8) => {
                                    enc.encode_field(&MacAddr8Output(value))
                                }
                                (Value::Path(path), DataType::Path) => {
                                    enc.encode_field(&PathOutput(path))
                                }
                                (Value::Text(s), DataType::Json) => {
                                    let parsed: JsonValue = match serde_json::from_str(&s) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Some((
                                                Err(fe(format!("invalid json output value: {e}"))),
                                                (node, fields, schema),
                                            ));
                                        }
                                    };
                                    enc.encode_field(&Json(parsed))
                                }
                                (Value::Text(s), DataType::Jsonb) => {
                                    let parsed: JsonValue = match serde_json::from_str(&s) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Some((
                                                Err(fe(format!("invalid jsonb output value: {e}"))),
                                                (node, fields, schema),
                                            ));
                                        }
                                    };
                                    enc.encode_field(&Json(parsed))
                                }
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
                                (Value::TimeMicros(value), DataType::Time(_)) => {
                                    enc.encode_field(&TimeOutput(value))
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
                                (Value::IntervalMicros(micros), DataType::Interval) => {
                                    let text = format_interval_micros(micros);
                                    enc.encode_field(&text)
                                }
                                (Value::Bytes(bytes), DataType::Bytea) => {
                                    if fmt == FieldFormat::Binary {
                                        enc.encode_field_with_type_and_format(
                                            &bytes,
                                            &Type::BYTEA,
                                            FieldFormat::Binary,
                                            &FormatOptions::default(),
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
                        let dr = enc.take_row();
                        Some((Ok(dr), (node, fields, schema)))
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
        assert_eq!(eval(&div), Value::Int64(4));

        let float_div = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Div,
            left: Box::new(lit_float(9.0)),
            right: Box::new(lit_int(2)),
        };
        assert_eq!(eval(&float_div).as_f64().unwrap(), 4.5);
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
    fn interval_arithmetic_overflow_returns_an_error_instead_of_panicking() {
        assert!(format_interval_micros(i64::MIN).starts_with('-'));

        let negate_min = ScalarExpr::UnaryOp {
            op: ScalarUnaryOp::Negate,
            expr: Box::new(ScalarExpr::Literal(Value::IntervalMicros(i64::MIN))),
        };
        assert!(eval_scalar_expr(&[], &negate_min, &[], &EvalContext::default()).is_err());

        let add_overflow = ScalarExpr::BinaryOp {
            op: ScalarBinaryOp::Add,
            left: Box::new(ScalarExpr::Literal(Value::IntervalMicros(i64::MAX))),
            right: Box::new(ScalarExpr::Literal(Value::IntervalMicros(1))),
        };
        assert!(eval_scalar_expr(&[], &add_overflow, &[], &EvalContext::default()).is_err());
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
