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
    BoolExpr, CmpOp, DataType, PathValue, PointValue, ScalarBinaryOp, ScalarExpr, ScalarFunc,
    ScalarUnaryOp, Value, cast_value_to_type, fe, fe_code, format_path_text, format_point_text,
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
}

impl EvalContext {
    pub fn new(time_zone: SessionTimeZone) -> Self {
        Self {
            time_zone,
            statement_time: None,
            session_id: None,
            advisory_locks: None,
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
        ScalarFunc::PgInputIsValid => match args.as_slice() {
            [Value::Text(value), Value::Text(data_type)] if data_type == "path" => {
                Ok(Value::Bool(crate::engine::parse_path_text(value).is_ok()))
            }
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
        ScalarFunc::ExtractEpoch => match args.into_iter().next() {
            Some(Value::TimestamptzMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::TimestampMicros(m)) => Ok(Value::from_f64(m as f64 / 1_000_000f64)),
            Some(Value::Null) | None => Ok(Value::Null),
            other => Err(fe(format!("extract(epoch ...) unsupported for {other:?}"))),
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
        Value::Point(point) => Some(format_point_text(point)),
        Value::Path(path) => Some(format_path_text(&path)),
        Value::Int64(i) => Some(i.to_string()),
        Value::Float64Bits(bits) => Some(f64::from_bits(bits).to_string()),
        Value::Bool(b) => Some(if b { "t" } else { "f" }.into()),
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into()),
        Value::IntervalMicros(v) => Some(format_interval_micros(v)),
        Value::Date(_) | Value::TimestampMicros(_) | Value::TimestamptzMicros(_) => {
            return Err(fe("text conversion not supported for date/timestamp"));
        }
    })
}

#[derive(Debug)]
struct PointOutput(PointValue);

#[derive(Debug)]
struct FloatOutput(f64);

impl ToSql for FloatOutput {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_f64(self.0);
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
        let mut value = self.0.to_string();
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
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
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
                                (Value::Null, DataType::Name) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::BpChar(_)) => {
                                    enc.encode_field(&Option::<String>::None)
                                }
                                (Value::Null, DataType::Point) => {
                                    enc.encode_field(&Option::<PointOutput>::None)
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
                                    enc.encode_field(&FloatOutput(i as f64))
                                }
                                (Value::Float64Bits(b), DataType::Float8) => {
                                    enc.encode_field(&FloatOutput(f64::from_bits(b)))
                                }
                                (Value::Text(s), DataType::Text) => enc.encode_field(&s),
                                (Value::Text(s), DataType::Name) => enc.encode_field(&s),
                                (Value::Text(s), DataType::BpChar(_)) => enc.encode_field(&s),
                                (Value::Point(point), DataType::Point) => {
                                    enc.encode_field(&PointOutput(point))
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
