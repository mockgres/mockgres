use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::executor::block_on;
use pgwire::error::PgWireResult;

use crate::catalog::SchemaName;
use crate::db::Db;
use crate::engine::{
    AggCall, AggFunc, BoolExpr, DbDdlKind, EvalContext, ExecNode, Expr, Plan, ScalarExpr, Schema,
    Value, ValuesExec, fe,
};
use crate::session::Session;
use crate::txn::{TransactionManager, TxId};

use super::exec::ddl::build_ddl_executor;
use super::exec::read::build_read_executor;
use super::exec::set_show::build_set_show_executor;
use super::exec::tx::{begin_transaction, commit_transaction, rollback_transaction};
use super::exec::write_dml::{
    build_delete_executor, build_insert_executor, build_insert_select_executor,
    build_update_executor,
};

type ExecResult = PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)>;

#[derive(Clone)]
struct MaterializedCte {
    schema: Schema,
    rows: Vec<Vec<Value>>,
}

pub(crate) fn schema_or_public(schema: &Option<SchemaName>) -> &str {
    schema.as_ref().map(|s| s.as_str()).unwrap_or("public")
}

pub(crate) fn assert_supported_aggs(aggs: &[(AggCall, String)]) {
    for (agg, _) in aggs {
        match agg.func {
            AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {}
        }
    }
}

pub fn command_tag(plan: &Plan) -> &'static str {
    match plan {
        Plan::With { body, .. } => command_tag(body),
        Plan::Empty => "EMPTY",
        Plan::Values { .. }
        | Plan::SeqScan { .. }
        | Plan::CteScan { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::UnboundJoin { .. }
        | Plan::Join { .. }
        | Plan::Alias { .. } => "SELECT",

        Plan::CreateTable { .. } => "CREATE TABLE",
        Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableAddConstraintCheck { .. }
        | Plan::AlterTableDropConstraint { .. } => "ALTER TABLE",
        Plan::CreateIndex { .. } => "CREATE INDEX",
        Plan::DropIndex { .. } => "DROP INDEX",
        Plan::DropTable { .. } => "DROP TABLE",
        Plan::TruncateTable { .. } => "TRUNCATE",
        Plan::CreateSchema { .. } => "CREATE SCHEMA",
        Plan::DropSchema { .. } => "DROP SCHEMA",
        Plan::AlterSchemaRename { .. } => "ALTER SCHEMA",
        Plan::CreateDatabase { .. } => "CREATE DATABASE",
        Plan::DropDatabase { .. } => "DROP DATABASE",
        Plan::AlterDatabase { .. } => "ALTER DATABASE",
        Plan::UnsupportedDbDDL { kind, .. } => match kind {
            DbDdlKind::Create => "CREATE DATABASE",
            DbDdlKind::Drop => "DROP DATABASE",
            DbDdlKind::Alter => "ALTER DATABASE",
        },
        Plan::ShowVariable { .. } => "SHOW",
        Plan::SetVariable { .. } => "SET",
        Plan::CallBuiltin { .. } => "SELECT",
        Plan::InsertValues { .. } => "INSERT",
        Plan::InsertSelect { .. } => "INSERT",
        Plan::Update { .. } => "UPDATE",
        Plan::Delete { .. } => "DELETE",
        Plan::UnboundSeqScan { .. } => "SELECT",
        Plan::BeginTransaction => "BEGIN",
        Plan::CommitTransaction => "COMMIT",
        Plan::RollbackTransaction => "ROLLBACK",
    }
}
pub fn build_executor(
    db: &Arc<parking_lot::RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    p: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> ExecResult {
    match p {
        Plan::With { ctes, body } => build_with_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            ctes,
            body,
            params,
            ctx,
        ),
        Plan::Empty => Err(fe("empty query")),
        Plan::Values { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::SeqScan { .. }
        | Plan::CteScan { .. }
        | Plan::LockRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::Join { .. }
        | Plan::Alias { .. } => {
            build_read_executor(db, txn_manager, session, snapshot_xid, p, params, ctx)
        }
        Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableAddConstraintCheck { .. }
        | Plan::AlterTableDropConstraint { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::TruncateTable { .. }
        | Plan::CreateSchema { .. }
        | Plan::DropSchema { .. }
        | Plan::AlterSchemaRename { .. }
        | Plan::UnsupportedDbDDL { .. }
        | Plan::CreateDatabase { .. }
        | Plan::DropDatabase { .. }
        | Plan::AlterDatabase { .. } => build_ddl_executor(db, session, p, ctx),

        Plan::InsertValues {
            table,
            columns,
            rows,
            override_system_value,
            on_conflict,
            returning,
            returning_schema,
        } => build_insert_executor(
            db,
            txn_manager,
            session,
            table,
            columns,
            rows,
            *override_system_value,
            on_conflict,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::InsertSelect {
            table,
            columns,
            select,
            override_system_value,
            on_conflict,
            returning,
            returning_schema,
        } => build_insert_select_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            table,
            columns,
            select,
            *override_system_value,
            on_conflict,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::Update {
            table,
            table_alias: _,
            sets,
            filter,
            from,
            from_schema,
            returning,
            returning_schema,
        } => build_update_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            table,
            sets,
            filter,
            from,
            from_schema,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema,
        } => build_delete_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            table,
            filter,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::ShowVariable { .. } | Plan::SetVariable { .. } => {
            build_set_show_executor(db, session, p, ctx)
        }

        Plan::BeginTransaction => {
            begin_transaction(session, txn_manager)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("BEGIN".into()),
                None,
            ))
        }
        Plan::CommitTransaction => {
            commit_transaction(session, txn_manager, db)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("COMMIT".into()),
                None,
            ))
        }
        Plan::RollbackTransaction => {
            rollback_transaction(session, txn_manager, db)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ROLLBACK".into()),
                None,
            ))
        }
        Plan::CallBuiltin { .. } => Err(fe("builtin execution not supported here")),
        Plan::UnboundSeqScan { .. } | Plan::UnboundJoin { .. } => {
            Err(fe("unbound plan; call binder first"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_with_executor(
    db: &Arc<parking_lot::RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    ctes: &[crate::engine::CommonTableExprPlan],
    body: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> ExecResult {
    let mut materialized = HashMap::new();
    for cte in ctes {
        let rewritten = rewrite_cte_refs(&cte.plan, &materialized)?;
        let (exec, _, _) = build_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            &rewritten,
            params.clone(),
            ctx,
        )?;
        let rows = collect_rows(exec)?;
        let schema = cte
            .schema
            .clone()
            .unwrap_or_else(|| rewritten.schema().clone());
        materialized.insert(cte.name.clone(), MaterializedCte { schema, rows });
    }
    let rewritten_body = rewrite_cte_refs(body, &materialized)?;
    build_executor(
        db,
        txn_manager,
        session,
        snapshot_xid,
        &rewritten_body,
        params,
        ctx,
    )
}

fn collect_rows(mut exec: Box<dyn ExecNode>) -> PgWireResult<Vec<Vec<Value>>> {
    block_on(exec.open())?;
    let mut rows = Vec::new();
    while let Some(row) = block_on(exec.next())? {
        rows.push(row);
    }
    block_on(exec.close())?;
    Ok(rows)
}

fn rewrite_cte_refs(plan: &Plan, ctes: &HashMap<String, MaterializedCte>) -> PgWireResult<Plan> {
    match plan {
        Plan::CteScan { name, cols, schema } => {
            let Some(materialized) = ctes.get(name) else {
                return Err(fe(format!("missing materialized CTE: {name}")));
            };
            let mut rows = Vec::with_capacity(materialized.rows.len());
            for row in &materialized.rows {
                let mut projected = Vec::with_capacity(cols.len());
                for (idx, _) in cols {
                    if *idx >= materialized.schema.fields.len() {
                        return Err(fe(format!("CTE column index out of bounds: {name}.{idx}")));
                    }
                    let value = row.get(*idx).cloned().ok_or_else(|| {
                        fe(format!("CTE column index out of bounds: {name}.{idx}"))
                    })?;
                    projected.push(Expr::Literal(value));
                }
                rows.push(projected);
            }
            Ok(Plan::Values {
                rows,
                schema: schema.clone(),
            })
        }
        Plan::With { ctes: inner, body } => {
            let shadowed: HashSet<String> = inner.iter().map(|cte| cte.name.clone()).collect();
            let filtered: HashMap<String, MaterializedCte> = ctes
                .iter()
                .filter(|(name, _)| !shadowed.contains(*name))
                .map(|(name, cte)| (name.clone(), cte.clone()))
                .collect();
            let mut rewritten_ctes = Vec::with_capacity(inner.len());
            for cte in inner {
                rewritten_ctes.push(crate::engine::CommonTableExprPlan {
                    name: cte.name.clone(),
                    plan: Box::new(rewrite_cte_refs(&cte.plan, &filtered)?),
                    output_columns: cte.output_columns.clone(),
                    schema: cte.schema.clone(),
                });
            }
            Ok(Plan::With {
                ctes: rewritten_ctes,
                body: Box::new(rewrite_cte_refs(body, &filtered)?),
            })
        }
        Plan::Alias {
            input,
            alias,
            schema,
        } => Ok(Plan::Alias {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            alias: alias.clone(),
            schema: schema.clone(),
        }),
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => Ok(Plan::Filter {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            expr: rewrite_bool_expr_cte_refs(expr, ctes)?,
            project_prefix_len: *project_prefix_len,
        }),
        Plan::Order { input, keys } => Ok(Plan::Order {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            keys: keys.clone(),
        }),
        Plan::Limit {
            input,
            limit,
            offset,
        } => Ok(Plan::Limit {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            limit: limit.clone(),
            offset: offset.clone(),
        }),
        Plan::UnboundJoin {
            left,
            right,
            join_type,
            on,
        } => Ok(Plan::UnboundJoin {
            left: Box::new(rewrite_cte_refs(left, ctes)?),
            right: Box::new(rewrite_cte_refs(right, ctes)?),
            join_type: *join_type,
            on: on
                .as_ref()
                .map(|expr| rewrite_bool_expr_cte_refs(expr, ctes))
                .transpose()?,
        }),
        Plan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => Ok(Plan::Join {
            left: Box::new(rewrite_cte_refs(left, ctes)?),
            right: Box::new(rewrite_cte_refs(right, ctes)?),
            on: on
                .as_ref()
                .map(|expr| rewrite_bool_expr_cte_refs(expr, ctes))
                .transpose()?,
            join_type: *join_type,
            schema: schema.clone(),
        }),
        Plan::LockRows {
            table,
            input,
            lock,
            row_id_idx,
            schema,
        } => Ok(Plan::LockRows {
            table: table.clone(),
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            lock: *lock,
            row_id_idx: *row_id_idx,
            schema: schema.clone(),
        }),
        Plan::Projection {
            input,
            exprs,
            schema,
        } => Ok(Plan::Projection {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            exprs: exprs.clone(),
            schema: schema.clone(),
        }),
        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            schema,
        } => Ok(Plan::Aggregate {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            group_exprs: group_exprs.clone(),
            agg_exprs: agg_exprs.clone(),
            schema: schema.clone(),
        }),
        Plan::CountRows { input, schema } => Ok(Plan::CountRows {
            input: Box::new(rewrite_cte_refs(input, ctes)?),
            schema: schema.clone(),
        }),
        Plan::InsertSelect {
            table,
            columns,
            select,
            override_system_value,
            on_conflict,
            returning,
            returning_schema,
        } => Ok(Plan::InsertSelect {
            table: table.clone(),
            columns: columns.clone(),
            select: Box::new(rewrite_cte_refs(select, ctes)?),
            override_system_value: *override_system_value,
            on_conflict: on_conflict.clone(),
            returning: returning.clone(),
            returning_schema: returning_schema.clone(),
        }),
        Plan::Update {
            table,
            table_alias,
            sets,
            filter,
            from,
            from_schema,
            returning,
            returning_schema,
        } => Ok(Plan::Update {
            table: table.clone(),
            table_alias: table_alias.clone(),
            sets: sets.clone(),
            filter: filter
                .as_ref()
                .map(|expr| rewrite_bool_expr_cte_refs(expr, ctes))
                .transpose()?,
            from: from
                .as_ref()
                .map(|plan| rewrite_cte_refs(plan, ctes).map(Box::new))
                .transpose()?,
            from_schema: from_schema.clone(),
            returning: returning.clone(),
            returning_schema: returning_schema.clone(),
        }),
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema,
        } => Ok(Plan::Delete {
            table: table.clone(),
            filter: filter
                .as_ref()
                .map(|expr| rewrite_bool_expr_cte_refs(expr, ctes))
                .transpose()?,
            returning: returning.clone(),
            returning_schema: returning_schema.clone(),
        }),
        _ => Ok(plan.clone()),
    }
}

fn rewrite_bool_expr_cte_refs(
    expr: &BoolExpr,
    ctes: &HashMap<String, MaterializedCte>,
) -> PgWireResult<BoolExpr> {
    Ok(match expr {
        BoolExpr::And(parts) => BoolExpr::And(
            parts
                .iter()
                .map(|part| rewrite_bool_expr_cte_refs(part, ctes))
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Or(parts) => BoolExpr::Or(
            parts
                .iter()
                .map(|part| rewrite_bool_expr_cte_refs(part, ctes))
                .collect::<PgWireResult<Vec<_>>>()?,
        ),
        BoolExpr::Not(inner) => BoolExpr::Not(Box::new(rewrite_bool_expr_cte_refs(inner, ctes)?)),
        BoolExpr::Comparison { lhs, op, rhs } => BoolExpr::Comparison {
            lhs: rewrite_scalar_expr_cte_refs(lhs, ctes)?,
            op: *op,
            rhs: rewrite_scalar_expr_cte_refs(rhs, ctes)?,
        },
        BoolExpr::IsNull { expr, negated } => BoolExpr::IsNull {
            expr: rewrite_scalar_expr_cte_refs(expr, ctes)?,
            negated: *negated,
        },
        BoolExpr::InSubquery { expr, subplan } => BoolExpr::InSubquery {
            expr: rewrite_scalar_expr_cte_refs(expr, ctes)?,
            subplan: Box::new(rewrite_cte_refs(subplan, ctes)?),
        },
        BoolExpr::InListValues { expr, values } => BoolExpr::InListValues {
            expr: rewrite_scalar_expr_cte_refs(expr, ctes)?,
            values: values.clone(),
        },
        BoolExpr::Literal(value) => BoolExpr::Literal(*value),
    })
}

fn rewrite_scalar_expr_cte_refs(
    expr: &ScalarExpr,
    ctes: &HashMap<String, MaterializedCte>,
) -> PgWireResult<ScalarExpr> {
    Ok(match expr {
        ScalarExpr::BinaryOp { op, left, right } => ScalarExpr::BinaryOp {
            op: *op,
            left: Box::new(rewrite_scalar_expr_cte_refs(left, ctes)?),
            right: Box::new(rewrite_scalar_expr_cte_refs(right, ctes)?),
        },
        ScalarExpr::UnaryOp { op, expr } => ScalarExpr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_scalar_expr_cte_refs(expr, ctes)?),
        },
        ScalarExpr::Cast { expr, ty } => ScalarExpr::Cast {
            expr: Box::new(rewrite_scalar_expr_cte_refs(expr, ctes)?),
            ty: ty.clone(),
        },
        ScalarExpr::Func { func, args } => ScalarExpr::Func {
            func: *func,
            args: args
                .iter()
                .map(|arg| rewrite_scalar_expr_cte_refs(arg, ctes))
                .collect::<PgWireResult<Vec<_>>>()?,
        },
        other => other.clone(),
    })
}
