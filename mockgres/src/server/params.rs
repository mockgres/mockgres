use std::collections::{BTreeSet, HashMap};
use std::convert::TryInto;
use std::sync::Arc;

use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::error::PgWireResult;

use crate::engine::types::parse_interval_literal;
use crate::engine::{
    BoolExpr, CountExpr, DataType, Expr, InsertSource, Plan, ReturningClause, ReturningExpr,
    ScalarExpr, UpdateSet, Value, fe, fe_code,
};
use crate::session::SessionTimeZone;
use crate::types::{
    parse_bytea_text, parse_date_str, parse_timestamp_str, parse_timestamptz_str,
    postgres_days_to_date, postgres_micros_to_timestamp,
};

use super::mapping::{map_datatype_to_pg_type, map_pg_type_to_datatype};
use super::statement_plan::StatementPlan;

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

pub fn statement_plan_parameter_types(statement: &StatementPlan) -> Vec<Type> {
    match statement {
        StatementPlan::Single(plan) => plan_parameter_types(plan),
        StatementPlan::Batch(plans) => {
            let mut indexes = BTreeSet::new();
            let mut hints = HashMap::new();
            for plan in plans {
                collect_param_indexes(plan, &mut indexes);
                collect_param_hints_from_plan(plan, &mut hints);
            }
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
    }
}

pub fn build_params_for_portal<S>(
    plan: &Plan,
    portal: &pgwire::api::portal::Portal<S>,
    tz: &SessionTimeZone,
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
            .and_then(|ty| ty.as_ref().and_then(map_pg_type_to_datatype));
        let ty = ty_from_plan.or(ty_from_stmt);
        let val = decode_param_value(raw.as_ref().map(|b| b.as_ref()), fmt, ty, tz)?;
        values.push(val);
    }
    Ok(Arc::new(values))
}

fn collect_param_hints_from_plan(plan: &Plan, out: &mut HashMap<usize, DataType>) {
    match plan {
        Plan::With { ctes, body } => {
            for cte in ctes {
                collect_param_hints_from_plan(&cte.plan, out);
            }
            collect_param_hints_from_plan(body, out);
        }
        Plan::Filter { input, expr, .. } => {
            collect_param_hints_from_plan(input, out);
            collect_param_hints_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::LockRows { input, .. } => {
            collect_param_hints_from_plan(input, out)
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            collect_param_hints_from_plan(input, out);
            if let Some(expr) = limit {
                collect_param_hints_from_count_expr(expr, out);
            }
            collect_param_hints_from_count_expr(offset, out);
        }
        Plan::Projection { input, exprs, .. } => {
            collect_param_hints_from_plan(input, out);
            for (expr, _) in exprs {
                collect_param_hints_from_scalar(expr, out);
            }
        }
        Plan::WindowRowNumber { input, specs, .. } => {
            collect_param_hints_from_plan(input, out);
            for (spec, _) in specs {
                for expr in &spec.partition_by {
                    collect_param_hints_from_scalar(expr, out);
                }
                for key in &spec.order_by {
                    if let crate::engine::SortKey::Expr { expr, .. } = key {
                        collect_param_hints_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            ..
        } => {
            collect_param_hints_from_plan(input, out);
            for (expr, _) in group_exprs {
                collect_param_hints_from_scalar(expr, out);
            }
            for (agg, _) in agg_exprs {
                if let Some(expr) = &agg.expr {
                    collect_param_hints_from_scalar(expr, out);
                }
            }
        }
        Plan::CountRows { input, .. } => collect_param_hints_from_plan(input, out),
        Plan::Join {
            left, right, on, ..
        }
        | Plan::UnboundJoin {
            left, right, on, ..
        } => {
            collect_param_hints_from_plan(left, out);
            collect_param_hints_from_plan(right, out);
            if let Some(expr) = on {
                collect_param_hints_from_bool(expr, out);
            }
        }
        Plan::Update {
            sets,
            filter,
            returning,
            from,
            ..
        } => {
            collect_param_hints_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
            if let Some(plan) = from {
                collect_param_hints_from_plan(plan, out);
            }
            if let Some(clause) = returning {
                collect_param_hints_from_returning(clause, out);
            }
        }
        Plan::Delete {
            filter, returning, ..
        } => {
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
            if let Some(clause) = returning {
                collect_param_hints_from_returning(clause, out);
            }
        }
        Plan::InsertValues {
            rows,
            returning,
            on_conflict: _,
            ..
        } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_hints_from_scalar(expr, out);
                    }
                }
            }
            if let Some(clause) = returning {
                collect_param_hints_from_returning(clause, out);
            }
        }
        Plan::InsertSelect {
            select,
            returning,
            on_conflict: _,
            ..
        } => {
            collect_param_hints_from_plan(select, out);
            if let Some(clause) = returning {
                collect_param_hints_from_returning(clause, out);
            }
        }
        Plan::CreateTableAs { query, .. } => collect_param_hints_from_plan(query, out),
        Plan::Alias { input, .. } => collect_param_hints_from_plan(input, out),
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_param_hints_from_values_expr(expr, out);
                }
            }
        }
        Plan::Empty
        | Plan::UtilityNoOp { .. }
        | Plan::SeqScan { .. }
        | Plan::CteScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableSetNotNull { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintPrimaryKey { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableAddConstraintCheck { .. }
        | Plan::AlterTableDropConstraint { .. }
        | Plan::AlterTableRename { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::TruncateTable { .. }
        | Plan::CreateSchema { .. }
        | Plan::DropSchema { .. }
        | Plan::AlterSchemaRename { .. }
        | Plan::GrantSchema { .. }
        | Plan::CreateTablespace { .. }
        | Plan::DropTablespace { .. }
        | Plan::Vacuum { .. }
        | Plan::CopyFrom { .. }
        | Plan::CreateDatabase { .. }
        | Plan::DropDatabase { .. }
        | Plan::AlterDatabase { .. }
        | Plan::UnsupportedDbDDL { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. }
        | Plan::CallBuiltin { .. }
        | Plan::DeclareCursor { .. }
        | Plan::FetchCursor { .. }
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
        BoolExpr::InSubquery { expr, subplan } => {
            collect_param_hints_from_scalar(expr, out);
            collect_param_hints_from_plan(subplan, out);
        }
        BoolExpr::InListValues { expr, .. } => {
            collect_param_hints_from_scalar(expr, out);
        }
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
        ScalarExpr::WindowRowNumber(spec) => {
            for expr in &spec.partition_by {
                collect_param_hints_from_scalar(expr, out);
            }
            for key in &spec.order_by {
                if let crate::engine::SortKey::Expr { expr, .. } = key {
                    collect_param_hints_from_scalar(expr, out);
                }
            }
        }
        ScalarExpr::Predicate(expr) => collect_param_hints_from_bool(expr, out),
        ScalarExpr::Subquery(plan) => collect_param_hints_from_plan(plan, out),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (cond, result) in when_then {
                collect_param_hints_from_bool(cond, out);
                collect_param_hints_from_scalar(result, out);
            }
            if let Some(expr) = else_expr {
                collect_param_hints_from_scalar(expr, out);
            }
        }
        ScalarExpr::Column(..)
        | ScalarExpr::ColumnIdx(..)
        | ScalarExpr::ExcludedIdx(..)
        | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_hints_from_values_expr(expr: &Expr, out: &mut HashMap<usize, DataType>) {
    if let Expr::Scalar(expr) = expr {
        collect_param_hints_from_scalar(expr, out);
    }
}

fn collect_param_hints_from_count_expr(expr: &CountExpr, out: &mut HashMap<usize, DataType>) {
    if let CountExpr::Expr(scalar) = expr {
        collect_param_hints_from_scalar(scalar, out);
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

fn collect_param_hints_from_returning(
    clause: &ReturningClause,
    out: &mut HashMap<usize, DataType>,
) {
    for item in &clause.exprs {
        if let ReturningExpr::Expr { expr, .. } = item {
            collect_param_hints_from_scalar(expr, out);
        }
    }
}

fn collect_param_indexes(plan: &Plan, out: &mut BTreeSet<usize>) {
    match plan {
        Plan::With { ctes, body } => {
            for cte in ctes {
                collect_param_indexes(&cte.plan, out);
            }
            collect_param_indexes(body, out);
        }
        Plan::Filter { input, expr, .. } => {
            collect_param_indexes(input, out);
            collect_param_indexes_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::LockRows { input, .. } => {
            collect_param_indexes(input, out)
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            collect_param_indexes(input, out);
            if let Some(expr) = limit {
                collect_param_indexes_from_count_expr(expr, out);
            }
            collect_param_indexes_from_count_expr(offset, out);
        }
        Plan::Projection { input, exprs, .. } => {
            collect_param_indexes(input, out);
            for (expr, _) in exprs {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
        Plan::WindowRowNumber { input, specs, .. } => {
            collect_param_indexes(input, out);
            for (spec, _) in specs {
                for expr in &spec.partition_by {
                    collect_param_indexes_from_scalar(expr, out);
                }
                for key in &spec.order_by {
                    if let crate::engine::SortKey::Expr { expr, .. } = key {
                        collect_param_indexes_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            ..
        } => {
            collect_param_indexes(input, out);
            for (expr, _) in group_exprs {
                collect_param_indexes_from_scalar(expr, out);
            }
            for (agg, _) in agg_exprs {
                if let Some(expr) = &agg.expr {
                    collect_param_indexes_from_scalar(expr, out);
                }
            }
        }
        Plan::CountRows { input, .. } => collect_param_indexes(input, out),
        Plan::Join {
            left, right, on, ..
        }
        | Plan::UnboundJoin {
            left, right, on, ..
        } => {
            collect_param_indexes(left, out);
            collect_param_indexes(right, out);
            if let Some(expr) = on {
                collect_param_indexes_from_bool(expr, out);
            }
        }
        Plan::Update {
            sets,
            filter,
            returning,
            from,
            ..
        } => {
            collect_param_indexes_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
            if let Some(plan) = from {
                collect_param_indexes(plan, out);
            }
            if let Some(clause) = returning {
                collect_param_indexes_from_returning(clause, out);
            }
        }
        Plan::Delete {
            filter, returning, ..
        } => {
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
            if let Some(clause) = returning {
                collect_param_indexes_from_returning(clause, out);
            }
        }
        Plan::InsertValues {
            rows, returning, ..
        } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_indexes_from_scalar(expr, out);
                    }
                }
            }
            if let Some(clause) = returning {
                collect_param_indexes_from_returning(clause, out);
            }
        }
        Plan::InsertSelect {
            select, returning, ..
        } => {
            collect_param_indexes(select, out);
            if let Some(clause) = returning {
                collect_param_indexes_from_returning(clause, out);
            }
        }
        Plan::CreateTableAs { query, .. } => collect_param_indexes(query, out),
        Plan::Alias { input, .. } => collect_param_indexes(input, out),
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_param_indexes_from_values_expr(expr, out);
                }
            }
        }
        Plan::Empty
        | Plan::UtilityNoOp { .. }
        | Plan::SeqScan { .. }
        | Plan::CteScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableSetNotNull { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintPrimaryKey { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableAddConstraintCheck { .. }
        | Plan::AlterTableDropConstraint { .. }
        | Plan::AlterTableRename { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::TruncateTable { .. }
        | Plan::CreateSchema { .. }
        | Plan::DropSchema { .. }
        | Plan::AlterSchemaRename { .. }
        | Plan::GrantSchema { .. }
        | Plan::CreateTablespace { .. }
        | Plan::DropTablespace { .. }
        | Plan::Vacuum { .. }
        | Plan::CopyFrom { .. }
        | Plan::CreateDatabase { .. }
        | Plan::DropDatabase { .. }
        | Plan::AlterDatabase { .. }
        | Plan::UnsupportedDbDDL { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. }
        | Plan::CallBuiltin { .. }
        | Plan::DeclareCursor { .. }
        | Plan::FetchCursor { .. }
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
        BoolExpr::InSubquery { expr, subplan } => {
            collect_param_indexes_from_scalar(expr, out);
            collect_param_indexes(subplan, out);
        }
        BoolExpr::InListValues { expr, .. } => {
            collect_param_indexes_from_scalar(expr, out);
        }
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
        ScalarExpr::WindowRowNumber(spec) => {
            for expr in &spec.partition_by {
                collect_param_indexes_from_scalar(expr, out);
            }
            for key in &spec.order_by {
                if let crate::engine::SortKey::Expr { expr, .. } = key {
                    collect_param_indexes_from_scalar(expr, out);
                }
            }
        }
        ScalarExpr::Predicate(expr) => collect_param_indexes_from_bool(expr, out),
        ScalarExpr::Subquery(plan) => collect_param_indexes(plan, out),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (cond, result) in when_then {
                collect_param_indexes_from_bool(cond, out);
                collect_param_indexes_from_scalar(result, out);
            }
            if let Some(expr) = else_expr {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
        ScalarExpr::Column(..)
        | ScalarExpr::ColumnIdx(..)
        | ScalarExpr::ExcludedIdx(..)
        | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_indexes_from_values_expr(expr: &Expr, out: &mut BTreeSet<usize>) {
    if let Expr::Scalar(expr) = expr {
        collect_param_indexes_from_scalar(expr, out);
    }
}

fn collect_param_indexes_from_count_expr(expr: &CountExpr, out: &mut BTreeSet<usize>) {
    if let CountExpr::Expr(scalar) = expr {
        collect_param_indexes_from_scalar(scalar, out);
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

fn collect_param_indexes_from_returning(clause: &ReturningClause, out: &mut BTreeSet<usize>) {
    for item in &clause.exprs {
        if let ReturningExpr::Expr { expr, .. } = item {
            collect_param_indexes_from_scalar(expr, out);
        }
    }
}

pub fn decode_param_value(
    raw: Option<&[u8]>,
    fmt: FieldFormat,
    ty: Option<DataType>,
    tz: &SessionTimeZone,
) -> PgWireResult<Value> {
    if raw.is_none() {
        return Ok(Value::Null);
    }
    let bytes = raw.unwrap();
    let ty = ty.unwrap_or(DataType::Text);
    match fmt {
        FieldFormat::Text => parse_text_value(bytes, &ty, tz),
        FieldFormat::Binary => parse_binary_value(bytes, &ty, tz),
    }
}

fn parse_text_value(bytes: &[u8], ty: &DataType, tz: &SessionTimeZone) -> PgWireResult<Value> {
    let s = std::str::from_utf8(bytes).map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
    match ty {
        DataType::Int2 => {
            let v: i16 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int2 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int4 => {
            let v: i32 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int4 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int8 => {
            let v: i64 = s
                .trim()
                .parse()
                .map_err(|e| fe(format!("bad int8 param: {e}")))?;
            Ok(Value::Int64(v))
        }
        DataType::Float8 => {
            let v: f64 = s
                .parse()
                .map_err(|e| fe(format!("bad float8 param: {e}")))?;
            Ok(Value::from_f64(v))
        }
        DataType::Text => Ok(Value::Text(s.to_string())),
        DataType::Varchar(length) => crate::engine::coerce_value_to_type(
            Value::Text(s.to_string()),
            &DataType::Varchar(*length),
            tz,
        )
        .map_err(|error| fe_code(error.code, error.message)),
        DataType::Name => {
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::Name, tz)
                .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::BpChar(length) => {
            let value = crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::BpChar(*length),
                tz,
            )
            .map_err(|e| fe_code(e.code, e.message))?;
            Ok(value)
        }
        DataType::PgChar => {
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::PgChar, tz)
                .map_err(|error| fe_code(error.code, error.message))
        }
        DataType::Point => crate::engine::parse_point_text(s)
            .map(Value::Point)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Lseg => crate::engine::parse_lseg_text(s)
            .map(Value::Lseg)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Line => crate::engine::parse_line_text(s)
            .map(Value::Line)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Circle => crate::engine::parse_circle_text(s)
            .map(Value::Circle)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Box => crate::engine::parse_box_text(s)
            .map(Value::Box)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Tid => crate::engine::parse_tid_text(s)
            .map(Value::Tid)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Oid => crate::engine::parse_oid_text(s)
            .map(Value::Oid)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::PgLsn => crate::engine::parse_pg_lsn_text(s)
            .map(Value::PgLsn)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::MacAddr => crate::engine::parse_macaddr_text(s)
            .map(Value::MacAddr)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::MacAddr8 => crate::engine::parse_macaddr8_text(s)
            .map(Value::MacAddr8)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Path => crate::engine::parse_path_text(s)
            .map(Value::Path)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Json => Ok(Value::Text(s.to_string())),
        DataType::Jsonb => Ok(Value::Text(s.to_string())),
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
        DataType::Time(precision) => crate::engine::parse_time_text(s, *precision)
            .map(Value::TimeMicros)
            .map_err(|error| fe_code(error.code, error.message)),
        DataType::Timestamp => {
            let micros = parse_timestamp_str(s).map_err(fe)?;
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Timestamptz => {
            let micros = parse_timestamptz_str(s, tz).map_err(fe)?;
            Ok(Value::TimestamptzMicros(micros))
        }
        DataType::Bytea => {
            let bytes = parse_bytea_text(s).map_err(fe)?;
            Ok(Value::Bytes(bytes))
        }
        DataType::Interval => {
            let micros =
                parse_interval_literal(s).map_err(|e| fe(format!("bad interval param: {e}")))?;
            Ok(Value::IntervalMicros(micros))
        }
        DataType::Void => Ok(Value::Null),
    }
}

fn parse_binary_value(bytes: &[u8], ty: &DataType, tz: &SessionTimeZone) -> PgWireResult<Value> {
    match ty {
        DataType::Int2 => {
            let arr: [u8; 2] = bytes
                .try_into()
                .map_err(|_| fe("binary int2 must be 2 bytes"))?;
            Ok(Value::Int64(i16::from_be_bytes(arr) as i64))
        }
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
        DataType::Varchar(length) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|error| fe(format!("invalid utf8 parameter: {error}")))?;
            crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::Varchar(*length),
                tz,
            )
            .map_err(|error| fe_code(error.code, error.message))
        }
        DataType::Name => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            crate::engine::coerce_value_to_type(Value::Text(s.to_string()), &DataType::Name, tz)
                .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::BpChar(length) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            crate::engine::coerce_value_to_type(
                Value::Text(s.to_string()),
                &DataType::BpChar(*length),
                tz,
            )
            .map_err(|e| fe_code(e.code, e.message))
        }
        DataType::PgChar => {
            if bytes.len() != 1 {
                return Err(fe("binary char must be 1 byte"));
            }
            Ok(Value::PgChar(bytes[0]))
        }
        DataType::Point => {
            if bytes.len() != 16 {
                return Err(fe("binary point must be 16 bytes"));
            }
            let x = f64::from_be_bytes(bytes[..8].try_into().expect("point x width checked"));
            let y = f64::from_be_bytes(bytes[8..].try_into().expect("point y width checked"));
            Ok(Value::Point(crate::engine::PointValue::new(x, y)))
        }
        DataType::Lseg => {
            if bytes.len() != 32 {
                return Err(fe("binary lseg must be 32 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary lseg coordinate width checked"),
                )
            };
            Ok(Value::Lseg(crate::engine::LsegValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                crate::engine::PointValue::new(coordinate(16), coordinate(24)),
            )))
        }
        DataType::Line => {
            if bytes.len() != 24 {
                return Err(fe("binary line must be 24 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary line coordinate width checked"),
                )
            };
            Ok(Value::Line(crate::engine::LineValue::new(
                coordinate(0),
                coordinate(8),
                coordinate(16),
            )))
        }
        DataType::Circle => {
            if bytes.len() != 24 {
                return Err(fe("binary circle must be 24 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary circle coordinate width checked"),
                )
            };
            Ok(Value::Circle(crate::engine::CircleValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                coordinate(16),
            )))
        }
        DataType::Box => {
            if bytes.len() != 32 {
                return Err(fe("binary box must be 32 bytes"));
            }
            let coordinate = |offset: usize| {
                f64::from_be_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .expect("binary box coordinate width checked"),
                )
            };
            Ok(Value::Box(crate::engine::BoxValue::new(
                crate::engine::PointValue::new(coordinate(0), coordinate(8)),
                crate::engine::PointValue::new(coordinate(16), coordinate(24)),
            )))
        }
        DataType::Tid => {
            if bytes.len() != 6 {
                return Err(fe("binary tid must be 6 bytes"));
            }
            Ok(Value::Tid(crate::engine::TidValue::new(
                u32::from_be_bytes(
                    bytes[..4]
                        .try_into()
                        .expect("binary tid block width checked"),
                ),
                u16::from_be_bytes(
                    bytes[4..]
                        .try_into()
                        .expect("binary tid offset width checked"),
                ),
            )))
        }
        DataType::Oid => {
            if bytes.len() != 4 {
                return Err(fe("binary oid must be 4 bytes"));
            }
            Ok(Value::Oid(u32::from_be_bytes(
                bytes.try_into().expect("binary oid width checked"),
            )))
        }
        DataType::PgLsn => {
            if bytes.len() != 8 {
                return Err(fe("binary pg_lsn must be 8 bytes"));
            }
            Ok(Value::PgLsn(u64::from_be_bytes(
                bytes.try_into().expect("binary pg_lsn width checked"),
            )))
        }
        DataType::MacAddr => {
            if bytes.len() != 6 {
                return Err(fe("binary macaddr must be 6 bytes"));
            }
            Ok(Value::MacAddr(
                bytes.try_into().expect("binary macaddr width checked"),
            ))
        }
        DataType::MacAddr8 => {
            if bytes.len() != 8 {
                return Err(fe("binary macaddr8 must be 8 bytes"));
            }
            Ok(Value::MacAddr8(
                bytes.try_into().expect("binary macaddr8 width checked"),
            ))
        }
        DataType::Path => {
            if bytes.len() < 5 {
                return Err(fe("binary path must contain a header"));
            }
            let closed = match bytes[0] {
                0 => false,
                1 => true,
                _ => return Err(fe("binary path has an invalid closed flag")),
            };
            let point_count = i32::from_be_bytes(
                bytes[1..5]
                    .try_into()
                    .expect("binary path count width checked"),
            );
            if point_count <= 0 {
                return Err(fe("binary path must contain at least one point"));
            }
            let point_count = point_count as usize;
            let expected_len = point_count
                .checked_mul(16)
                .and_then(|coordinate_bytes| coordinate_bytes.checked_add(5))
                .ok_or_else(|| fe("binary path point count is too large"))?;
            if bytes.len() != expected_len {
                return Err(fe("binary path length does not match its point count"));
            }
            let points = bytes[5..]
                .chunks_exact(16)
                .map(|point| {
                    let x = f64::from_be_bytes(
                        point[..8].try_into().expect("binary path x width checked"),
                    );
                    let y = f64::from_be_bytes(
                        point[8..].try_into().expect("binary path y width checked"),
                    );
                    crate::engine::PointValue::new(x, y)
                })
                .collect();
            Ok(Value::Path(crate::engine::PathValue::new(closed, points)))
        }
        DataType::Json => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Jsonb => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Bytea => Ok(Value::Bytes(bytes.to_vec())),
        DataType::Date => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| fe("binary date must be 4 bytes"))?;
            let pg_days = i32::from_be_bytes(arr);
            let days = postgres_days_to_date(pg_days);
            Ok(Value::Date(days))
        }
        DataType::Time(_) => {
            if bytes.len() != 8 {
                return Err(fe("binary time must be 8 bytes"));
            }
            Ok(Value::TimeMicros(u64::from_be_bytes(
                bytes.try_into().expect("binary time width checked"),
            )))
        }
        DataType::Timestamp => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary timestamp must be 8 bytes"))?;
            let pg_micros = i64::from_be_bytes(arr);
            let micros = postgres_micros_to_timestamp(pg_micros);
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Timestamptz => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary timestamptz must be 8 bytes"))?;
            let pg_micros = i64::from_be_bytes(arr);
            let micros = postgres_micros_to_timestamp(pg_micros);
            Ok(Value::TimestamptzMicros(micros))
        }
        DataType::Interval => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            let micros =
                parse_interval_literal(s).map_err(|e| fe(format!("bad interval param: {e}")))?;
            Ok(Value::IntervalMicros(micros))
        }
        DataType::Void => Ok(Value::Null),
    }
}
