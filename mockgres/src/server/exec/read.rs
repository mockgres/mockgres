use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{Db, LockOwner};
use crate::engine::{
    CountExec, CountExpr, EvalContext, ExecNode, FilterExec, HashAggregateExec, JoinExec, JoinType,
    LimitExec, OrderExec, Plan, ProjectExec, ScalarExpr, Schema, SeqScanExec, Value, ValuesExec,
    WindowRowNumberExec, coerce_value_to_type, eval_scalar_expr, fe,
};
use crate::server::errors::map_db_err;
use crate::session::Session;
use crate::txn::{TransactionManager, TxId, VisibilityContext};

use super::locks::{LockScope, wrap_with_lock_apply};
pub(crate) mod subquery;
use crate::server::exec_builder::{assert_supported_aggs, build_executor, schema_or_public};
use subquery::{materialize_in_subqueries, materialize_scalar_subqueries};

type ExecResult = PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)>;

pub fn build_read_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    plan: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> ExecResult {
    match plan {
        Plan::Values { rows, schema } => {
            let cnt = rows.len();
            Ok((
                Box::new(ValuesExec::new_with_context(
                    schema.clone(),
                    rows.clone(),
                    params.clone(),
                    ctx.clone(),
                )?),
                None,
                Some(cnt),
            ))
        }
        Plan::Projection {
            input,
            exprs,
            schema,
        } => {
            let (child, _tag, cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            let materialized_exprs = exprs
                .iter()
                .map(|(expr, name)| {
                    Ok((
                        materialize_scalar_subqueries(
                            expr,
                            db,
                            txn_manager,
                            session,
                            snapshot_xid,
                            params.clone(),
                            ctx,
                        )?,
                        name.clone(),
                    ))
                })
                .collect::<PgWireResult<Vec<_>>>()?;
            Ok((
                Box::new(ProjectExec::new(
                    schema.clone(),
                    child,
                    materialized_exprs,
                    params.clone(),
                    ctx.clone(),
                )),
                None,
                cnt,
            ))
        }
        Plan::WindowRowNumber {
            input,
            specs,
            schema,
        } => {
            let (child, _tag, cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            Ok((
                Box::new(WindowRowNumberExec::new(
                    schema.clone(),
                    child,
                    specs.clone(),
                    params.clone(),
                    ctx.clone(),
                )?),
                None,
                cnt,
            ))
        }
        Plan::Aggregate {
            input,
            group_exprs,
            agg_exprs,
            schema,
        } => {
            assert_supported_aggs(agg_exprs);
            let (child, _tag, _cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            Ok((
                Box::new(HashAggregateExec::new(
                    schema.clone(),
                    child,
                    group_exprs.clone(),
                    agg_exprs.clone(),
                    params.clone(),
                    ctx.clone(),
                )),
                None,
                None,
            ))
        }
        Plan::CountRows { input, schema } => {
            if let Plan::SeqScan {
                table, lock: None, ..
            } = input.as_ref()
            {
                let schema_name = schema_or_public(&table.schema);
                let current_tx = session.current_tx();
                let visibility =
                    VisibilityContext::new(txn_manager.as_ref(), snapshot_xid, current_tx);
                let count = db
                    .read()
                    .count_visible_rows(schema_name, &table.name, &visibility)
                    .map_err(map_db_err)?;
                let count =
                    i64::try_from(count).map_err(|_| fe("row count exceeds bigint range"))?;
                return Ok((
                    Box::new(ValuesExec::from_values(
                        schema.clone(),
                        vec![vec![Value::Int64(count)]],
                    )),
                    None,
                    Some(1),
                ));
            }
            let (child, _tag, _cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            Ok((
                Box::new(CountExec::new(schema.clone(), child)),
                None,
                Some(1),
            ))
        }
        Plan::SeqScan {
            table,
            cols,
            schema,
            lock,
        } => {
            let db_read = db.read();
            let schema_name = schema_or_public(&table.schema);
            let _tm = db_read
                .resolve_table(schema_name, &table.name)
                .map_err(map_db_err)?;
            let positions: Vec<usize> = cols.iter().map(|(i, _)| *i).collect();
            let current_tx = session.current_tx();
            let visibility = VisibilityContext::new(txn_manager.as_ref(), snapshot_xid, current_tx);
            let (mut rows, row_ids) = if positions.is_empty() && schema.fields.is_empty() {
                (vec![], Vec::new())
            } else {
                db_read
                    .scan_bound_positions(schema_name, &table.name, &positions, &visibility)
                    .map_err(map_db_err)?
            };
            drop(db_read);
            if lock.is_some() {
                for (row, row_id) in rows.iter_mut().zip(row_ids.iter()) {
                    row.push(Value::Int64(*row_id as i64));
                }
            }
            let cnt = rows.len();
            Ok((
                Box::new(SeqScanExec::new(schema.clone(), rows)),
                None,
                Some(cnt),
            ))
        }
        Plan::LockRows {
            input,
            lock,
            row_id_idx,
            schema,
            ..
        } => {
            let (child, _tag, _cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            let epoch = session
                .current_epoch()
                .ok_or_else(|| fe("FOR UPDATE requires an active transaction"))?;
            let owner = LockOwner::new(session.id(), epoch);
            let lock_handle = {
                let db_read = db.read();
                db_read.lock_handle()
            };
            let exec = wrap_with_lock_apply(
                schema.clone(),
                child,
                *lock,
                *row_id_idx,
                LockScope::new(owner, session.current_tx().is_none()),
                lock_handle,
                session.lock_timeout(),
            );
            Ok((exec, None, None))
        }
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => {
            let materialized_expr = materialize_in_subqueries(
                expr,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params.clone(),
                ctx,
            )?;
            let indexed_child = try_build_indexed_scan(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                &materialized_expr,
                &params,
                ctx,
            )?;
            let child = if let Some(child) = indexed_child {
                child
            } else {
                build_executor(
                    db,
                    txn_manager,
                    session,
                    snapshot_xid,
                    input,
                    params.clone(),
                    ctx,
                )?
                .0
            };
            let child_schema = child.schema().clone();
            let mut node: Box<dyn ExecNode> = Box::new(FilterExec::new(
                child_schema.clone(),
                child,
                materialized_expr,
                params.clone(),
                ctx.clone(),
            ));

            if let Some(n) = *project_prefix_len {
                if n == 0 {
                    return Ok((node, None, None));
                }
                let proj_fields = child_schema.fields[..n].to_vec();
                let proj_schema = Schema {
                    fields: proj_fields.clone(),
                };
                let exprs: Vec<(ScalarExpr, String)> = (0..n)
                    .map(|i| (ScalarExpr::ColumnIdx(i), proj_fields[i].name.clone()))
                    .collect();
                node = Box::new(ProjectExec::new(
                    proj_schema,
                    node,
                    exprs,
                    params.clone(),
                    ctx.clone(),
                ));
            }

            Ok((node, None, None))
        }
        Plan::Order { input, keys } => {
            let (child, _tag, cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            let schema = child.schema().clone();
            let exec = Box::new(OrderExec::new(
                schema,
                child,
                keys.clone(),
                params.clone(),
                ctx.clone(),
            )?);
            Ok((exec, None, cnt))
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let limit_val = limit
                .as_ref()
                .map(|expr| resolve_count_expr(expr, "limit", &params, ctx))
                .transpose()?;
            let offset_val = resolve_count_expr(offset, "offset", &params, ctx)?;
            let (child, _tag, cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
                ctx,
            )?;
            let remaining_after_offset = cnt.map(|c| c.saturating_sub(offset_val));
            let out_cnt = match (remaining_after_offset, limit_val) {
                (Some(c), Some(lim)) => Some(c.min(lim)),
                (Some(c), None) => Some(c),
                _ => None,
            };
            let schema = child.schema().clone();
            Ok((
                Box::new(LimitExec::new(schema, child, limit_val, offset_val)),
                None,
                out_cnt,
            ))
        }
        Plan::Alias { input, .. } => {
            build_executor(db, txn_manager, session, snapshot_xid, input, params, ctx)
        }
        Plan::Join {
            left,
            right,
            schema,
            on,
            join_type,
        } => {
            let (left_exec, _ltag, left_cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                left,
                params.clone(),
                ctx,
            )?;
            let (right_exec, _rtag, right_cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                right,
                params.clone(),
                ctx,
            )?;
            let out_cnt = match (join_type, on, left_cnt, right_cnt) {
                (JoinType::Inner, None, Some(lc), Some(rc)) => Some(lc.saturating_mul(rc)),
                (JoinType::Left, _, Some(lc), _) => Some(lc),
                _ => None,
            };
            Ok((
                Box::new(JoinExec::new(
                    schema.clone(),
                    left_exec,
                    right_exec,
                    *join_type,
                    on.clone(),
                    params.clone(),
                    ctx.clone(),
                )),
                None,
                out_cnt,
            ))
        }
        _ => Err(fe("unsupported plan for read executor")),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_build_indexed_scan(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    input: &Plan,
    filter: &crate::engine::BoolExpr,
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<Option<Box<dyn ExecNode>>> {
    let Plan::SeqScan {
        table,
        cols,
        schema,
        lock,
    } = input
    else {
        return Ok(None);
    };
    let mut logical_equalities = Vec::new();
    collect_index_equalities(filter, params, ctx, &mut logical_equalities);
    if logical_equalities.is_empty() {
        return Ok(None);
    }
    let equalities = logical_equalities
        .into_iter()
        .filter_map(|(logical_column, value)| {
            cols.get(logical_column)
                .and_then(|(physical_column, field)| {
                    coerce_value_to_type(value, &field.data_type, &ctx.time_zone)
                        .ok()
                        .map(|value| (*physical_column, value))
                })
        })
        .collect::<Vec<_>>();
    if equalities.is_empty() {
        return Ok(None);
    }

    let schema_name = schema_or_public(&table.schema);
    let positions = cols
        .iter()
        .map(|(position, _)| *position)
        .collect::<Vec<_>>();
    let current_tx = session.current_tx();
    let visibility = VisibilityContext::new(txn_manager.as_ref(), snapshot_xid, current_tx);
    let indexed = db
        .read()
        .scan_bound_positions_indexed(
            schema_name,
            &table.name,
            &positions,
            &equalities,
            &visibility,
        )
        .map_err(map_db_err)?;
    let Some((mut rows, row_ids)) = indexed else {
        return Ok(None);
    };
    if lock.is_some() {
        for (row, row_id) in rows.iter_mut().zip(row_ids) {
            row.push(Value::Int64(row_id as i64));
        }
    }
    Ok(Some(Box::new(SeqScanExec::new(schema.clone(), rows))))
}

fn collect_index_equalities(
    expr: &crate::engine::BoolExpr,
    params: &[Value],
    ctx: &EvalContext,
    out: &mut Vec<(usize, Value)>,
) {
    match expr {
        crate::engine::BoolExpr::Comparison {
            lhs,
            op: crate::engine::CmpOp::Eq,
            rhs,
        } => {
            if let ScalarExpr::ColumnIdx(column) = lhs
                && let Ok(value) = eval_scalar_expr(&[], rhs, params, ctx)
                && !matches!(value, Value::Null)
            {
                out.push((*column, value));
            } else if let ScalarExpr::ColumnIdx(column) = rhs
                && let Ok(value) = eval_scalar_expr(&[], lhs, params, ctx)
                && !matches!(value, Value::Null)
            {
                out.push((*column, value));
            }
        }
        crate::engine::BoolExpr::And(exprs) => {
            for expr in exprs {
                collect_index_equalities(expr, params, ctx, out);
            }
        }
        _ => {}
    }
}

fn resolve_count_expr(
    expr: &CountExpr,
    label: &str,
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<usize> {
    let value = match expr {
        CountExpr::Value(v) => Value::Int64(*v as i64),
        CountExpr::Expr(e) => eval_scalar_expr(&[], e, params, ctx)?,
    };

    match value {
        Value::Int64(v) => {
            if v < 0 {
                Err(fe(format!("{label} must be non-negative")))
            } else {
                Ok(v as usize)
            }
        }
        _ => Err(fe(format!("{label} must be integer"))),
    }
}
