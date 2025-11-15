use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{Db, LockOwner};
use crate::engine::{
    CountExec, EvalContext, ExecNode, FilterExec, HashAggregateExec, LimitExec, NestedLoopJoinExec,
    OrderExec, Plan, ProjectExec, ScalarExpr, Schema, SeqScanExec, Value, ValuesExec, fe,
};
use crate::server::errors::map_db_err;
use crate::session::Session;
use crate::txn::{TransactionManager, TxId, VisibilityContext};

use super::locks::wrap_with_lock_apply;
use crate::server::exec_builder::{assert_supported_aggs, build_executor, schema_or_public};

pub fn build_read_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    plan: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    match plan {
        Plan::Values { rows, schema } => {
            let cnt = rows.len();
            Ok((
                Box::new(ValuesExec::new(schema.clone(), rows.clone())?),
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
            Ok((
                Box::new(ProjectExec::new(
                    schema.clone(),
                    child,
                    exprs.clone(),
                    params.clone(),
                    ctx.clone(),
                )),
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
            let (mut rows, _cols, row_ids) = if positions.is_empty() && schema.fields.is_empty() {
                (vec![], vec![], Vec::new())
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
                owner,
                lock_handle,
            );
            Ok((exec, None, None))
        }
        Plan::Filter {
            input,
            expr,
            project_prefix_len,
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
            let child_schema = child.schema().clone();
            let mut node: Box<dyn ExecNode> = Box::new(FilterExec::new(
                child_schema.clone(),
                child,
                expr.clone(),
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
            let limit_val = *limit;
            let offset_val = *offset;
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
        Plan::Join {
            left,
            right,
            schema,
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
            let out_cnt = match (left_cnt, right_cnt) {
                (Some(lc), Some(rc)) => Some(lc.saturating_mul(rc)),
                _ => None,
            };
            Ok((
                Box::new(NestedLoopJoinExec::new(
                    schema.clone(),
                    left_exec,
                    right_exec,
                )),
                None,
                out_cnt,
            ))
        }
        _ => Err(fe("unsupported plan for read executor")),
    }
}
