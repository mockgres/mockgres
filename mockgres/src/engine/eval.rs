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

mod functions;
mod wire;

use functions::{NumericValue, coerce_numeric_pair, eval_function, value_to_text};
pub use wire::to_pgwire_stream;
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
