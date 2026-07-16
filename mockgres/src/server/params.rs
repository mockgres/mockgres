use std::collections::{BTreeSet, HashMap};
use std::convert::TryInto;
use std::sync::Arc;

use pgwire::api::results::FieldFormat;
use pgwire::api::{DEFAULT_NAME, Type};
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

mod decode;

pub use decode::decode_param_value;

pub fn plan_parameter_types(plan: &Plan) -> Vec<Type> {
    let mut indexes = BTreeSet::new();
    collect_param_indexes(plan, &mut indexes);
    if indexes.is_empty() {
        return vec![];
    }
    let mut hints = HashMap::new();
    collect_param_hints_from_plan(plan, &mut hints);
    let count = indexes.last().map_or(0, |idx| idx + 1);
    (0..count)
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
            let count = indexes.last().map_or(0, |idx| idx + 1);
            (0..count)
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
    let expected = plan_parameter_types(plan)
        .len()
        .max(portal.statement.parameter_types.len());
    let actual = portal.parameters.len();
    if actual != expected {
        let statement_name = if portal.statement.id == DEFAULT_NAME {
            ""
        } else {
            &portal.statement.id
        };
        return Err(fe(format!(
            "bind message supplies {actual} parameters, but prepared statement \"{}\" requires {expected}",
            statement_name
        )));
    }
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
        Plan::SetOperation { left, right, .. } => {
            collect_param_hints_from_plan(left, out);
            collect_param_hints_from_plan(right, out);
        }
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
        Plan::CallBuiltin { args, .. } => {
            for arg in args {
                collect_param_hints_from_scalar(arg, out);
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
        Plan::SetOperation { left, right, .. } => {
            collect_param_indexes(left, out);
            collect_param_indexes(right, out);
        }
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
        Plan::CallBuiltin { args, .. } => {
            for arg in args {
                collect_param_indexes_from_scalar(arg, out);
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
