use std::collections::{BTreeSet, HashMap};
use std::convert::TryInto;
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::error::PgWireResult;

use crate::engine::{BoolExpr, DataType, InsertSource, Plan, ScalarExpr, UpdateSet, Value, fe};
use crate::types::{parse_bytea_text, parse_date_str, parse_timestamp_str};

use super::mapping::{map_datatype_to_pg_type, map_pg_type_to_datatype};

pub fn plan_parameter_types(plan: &Plan) -> Vec<Type> {
    let mut indexes = BTreeSet::new();
    collect_param_indexes(plan, &mut indexes);
    if indexes.is_empty() {
        return vec![];
    }
    let mut hints = HashMap::new();
    collect_param_hints_from_plan(plan, &mut hints);
    indexes
        .into_iter()
        .map(|idx| {
            hints
                .get(&idx)
                .map(map_datatype_to_pg_type)
                .unwrap_or(Type::UNKNOWN)
        })
        .collect()
}

pub fn build_params_for_portal(
    plan: &Plan,
    portal: &pgwire::api::portal::Portal<Plan>,
) -> PgWireResult<Arc<Vec<Value>>> {
    let mut hints = HashMap::new();
    collect_param_hints_from_plan(plan, &mut hints);

    let mut values = Vec::with_capacity(portal.parameters.len());
    for (idx, raw) in portal.parameters.iter().enumerate() {
        let fmt = portal.parameter_format.format_for(idx);
        let ty_from_plan = hints.get(&idx).cloned();
        let ty_from_stmt = portal
            .statement
            .parameter_types
            .get(idx)
            .and_then(map_pg_type_to_datatype);
        let ty = ty_from_plan.or(ty_from_stmt);
        let val = decode_param_value(raw.as_ref().map(|b| b.as_ref()), fmt, ty)?;
        values.push(val);
    }
    Ok(Arc::new(values))
}

fn collect_param_hints_from_plan(plan: &Plan, out: &mut HashMap<usize, DataType>) {
    match plan {
        Plan::Filter { input, expr, .. } => {
            collect_param_hints_from_plan(input, out);
            collect_param_hints_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::Limit { input, .. } => {
            collect_param_hints_from_plan(input, out)
        }
        Plan::Projection { input, exprs, .. } => {
            collect_param_hints_from_plan(input, out);
            for (expr, _) in exprs {
                collect_param_hints_from_scalar(expr, out);
            }
        }
        Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right } => {
            collect_param_hints_from_plan(left, out);
            collect_param_hints_from_plan(right, out);
        }
        Plan::Update { sets, filter, .. } => {
            collect_param_hints_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
        }
        Plan::Delete { filter, .. } => {
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
        }
        Plan::InsertValues { rows, .. } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_hints_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::SeqScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::Values { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. }
        | Plan::BeginTransaction
        | Plan::CommitTransaction
        | Plan::RollbackTransaction => {}
    }
}

fn collect_param_hints_from_bool(expr: &BoolExpr, out: &mut HashMap<usize, DataType>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_param_hints_from_scalar(lhs, out);
            collect_param_hints_from_scalar(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_param_hints_from_bool(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_param_hints_from_bool(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_param_hints_from_scalar(expr, out),
    }
}

fn collect_param_hints_from_scalar(expr: &ScalarExpr, out: &mut HashMap<usize, DataType>) {
    match expr {
        ScalarExpr::Param { idx, ty } => {
            if let Some(dt) = ty {
                out.entry(*idx).or_insert(dt.clone());
            }
        }
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_param_hints_from_scalar(left, out);
            collect_param_hints_from_scalar(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_param_hints_from_scalar(expr, out),
        ScalarExpr::Cast { expr, .. } => collect_param_hints_from_scalar(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_param_hints_from_scalar(arg, out);
            }
        }
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(..) | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_hints_from_update_sets(sets: &[UpdateSet], out: &mut HashMap<usize, DataType>) {
    for set in sets {
        match set {
            UpdateSet::ByIndex(_, expr) | UpdateSet::ByName(_, expr) => {
                collect_param_hints_from_scalar(expr, out);
            }
        }
    }
}

fn collect_param_indexes(plan: &Plan, out: &mut BTreeSet<usize>) {
    match plan {
        Plan::Filter { input, expr, .. } => {
            collect_param_indexes(input, out);
            collect_param_indexes_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::Limit { input, .. } => collect_param_indexes(input, out),
        Plan::Projection { input, exprs, .. } => {
            collect_param_indexes(input, out);
            for (expr, _) in exprs {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
        Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right } => {
            collect_param_indexes(left, out);
            collect_param_indexes(right, out);
        }
        Plan::Update { sets, filter, .. } => {
            collect_param_indexes_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
        }
        Plan::Delete { filter, .. } => {
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
        }
        Plan::InsertValues { rows, .. } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_indexes_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::SeqScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::Values { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. }
        | Plan::BeginTransaction
        | Plan::CommitTransaction
        | Plan::RollbackTransaction => {}
    }
}

fn collect_param_indexes_from_bool(expr: &BoolExpr, out: &mut BTreeSet<usize>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_param_indexes_from_scalar(lhs, out);
            collect_param_indexes_from_scalar(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_param_indexes_from_bool(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_param_indexes_from_bool(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_param_indexes_from_scalar(expr, out),
    }
}

fn collect_param_indexes_from_scalar(expr: &ScalarExpr, out: &mut BTreeSet<usize>) {
    match expr {
        ScalarExpr::Param { idx, .. } => {
            out.insert(*idx);
        }
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_param_indexes_from_scalar(left, out);
            collect_param_indexes_from_scalar(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_param_indexes_from_scalar(expr, out),
        ScalarExpr::Cast { expr, .. } => collect_param_indexes_from_scalar(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_param_indexes_from_scalar(arg, out);
            }
        }
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(..) | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_indexes_from_update_sets(sets: &[UpdateSet], out: &mut BTreeSet<usize>) {
    for set in sets {
        match set {
            UpdateSet::ByIndex(_, expr) | UpdateSet::ByName(_, expr) => {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
    }
}

pub fn decode_param_value(
    raw: Option<&[u8]>,
    fmt: FieldFormat,
    ty: Option<DataType>,
) -> PgWireResult<Value> {
    if raw.is_none() {
        return Ok(Value::Null);
    }
    let bytes = raw.unwrap();
    let ty = ty.unwrap_or(DataType::Text);
    match fmt {
        FieldFormat::Text => parse_text_value(bytes, &ty),
        FieldFormat::Binary => parse_binary_value(bytes, &ty),
    }
}

fn parse_text_value(bytes: &[u8], ty: &DataType) -> PgWireResult<Value> {
    let s = std::str::from_utf8(bytes).map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
    match ty {
        DataType::Int4 => {
            let v: i32 = s.parse().map_err(|e| fe(format!("bad int4 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int8 => {
            let v: i64 = s.parse().map_err(|e| fe(format!("bad int8 param: {e}")))?;
            Ok(Value::Int64(v))
        }
        DataType::Float8 => {
            let v: f64 = s
                .parse()
                .map_err(|e| fe(format!("bad float8 param: {e}")))?;
            Ok(Value::from_f64(v))
        }
        DataType::Text => Ok(Value::Text(s.to_string())),
        DataType::Bool => {
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                other => Err(fe(format!("bad bool param: {other}"))),
            }
        }
        DataType::Date => {
            let days = parse_date_str(s).map_err(fe)?;
            Ok(Value::Date(days))
        }
        DataType::Timestamp => {
            let micros = parse_timestamp_str(s).map_err(fe)?;
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Bytea => {
            let bytes = parse_bytea_text(s).map_err(fe)?;
            Ok(Value::Bytes(bytes))
        }
    }
}

fn parse_binary_value(bytes: &[u8], ty: &DataType) -> PgWireResult<Value> {
    match ty {
        DataType::Int4 => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| fe("binary int4 must be 4 bytes"))?;
            Ok(Value::Int64(i32::from_be_bytes(arr) as i64))
        }
        DataType::Int8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary int8 must be 8 bytes"))?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        DataType::Float8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary float8 must be 8 bytes"))?;
            Ok(Value::Float64Bits(u64::from_be_bytes(arr)))
        }
        DataType::Bool => {
            if bytes.len() != 1 {
                return Err(fe("binary bool must be 1 byte"));
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        DataType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Bytea => Ok(Value::Bytes(bytes.to_vec())),
        DataType::Date | DataType::Timestamp => {
            Err(fe("binary parameters for date/timestamp not supported"))
        }
    }
}
