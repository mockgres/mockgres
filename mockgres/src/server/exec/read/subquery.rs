use std::sync::Arc;

use futures::executor::block_on;
use pgwire::error::PgWireResult;

use crate::db::Db;
use crate::engine::{BoolExpr, Plan, Value};
use crate::server::exec_builder::build_executor;
use crate::session::Session;
use crate::txn::{TransactionManager, TxId};

use super::EvalContext;
use parking_lot::RwLock;

pub fn materialize_in_subqueries(
    expr: &BoolExpr,
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<BoolExpr> {
    match expr {
        BoolExpr::And(parts) => Ok(BoolExpr::And(
            parts
                .iter()
                .map(|p| {
                    materialize_in_subqueries(
                        p,
                        db,
                        txn_manager,
                        session,
                        snapshot_xid,
                        params.clone(),
                        ctx,
                    )
                })
                .collect::<PgWireResult<Vec<_>>>()?,
        )),
        BoolExpr::Or(parts) => Ok(BoolExpr::Or(
            parts
                .iter()
                .map(|p| {
                    materialize_in_subqueries(
                        p,
                        db,
                        txn_manager,
                        session,
                        snapshot_xid,
                        params.clone(),
                        ctx,
                    )
                })
                .collect::<PgWireResult<Vec<_>>>()?,
        )),
        BoolExpr::Not(inner) => Ok(BoolExpr::Not(Box::new(materialize_in_subqueries(
            inner,
            db,
            txn_manager,
            session,
            snapshot_xid,
            params,
            ctx,
        )?))),
        BoolExpr::Comparison { .. }
        | BoolExpr::Literal(_)
        | BoolExpr::IsNull { .. }
        | BoolExpr::InListValues { .. } => Ok(expr.clone()),
        BoolExpr::InSubquery { expr: lhs, subplan } => {
            let values = materialize_subquery_values(
                subplan,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?;
            Ok(BoolExpr::InListValues {
                expr: lhs.clone(),
                values,
            })
        }
    }
}

pub fn materialize_subquery_values(
    plan: &Plan,
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<Vec<Value>> {
    let (mut exec, _, _) =
        build_executor(db, txn_manager, session, snapshot_xid, plan, params, ctx)?;
    block_on(exec.open())?;
    let mut values = Vec::new();
    while let Some(row) = block_on(exec.next())? {
        if let Some(v) = row.into_iter().next() {
            values.push(v);
        }
    }
    block_on(exec.close())?;
    Ok(values)
}
