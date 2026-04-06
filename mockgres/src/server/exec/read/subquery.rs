use std::sync::Arc;

use futures::executor::block_on;
use pgwire::error::PgWireResult;

use crate::db::Db;
use crate::engine::{BoolExpr, Plan, ScalarExpr, Value, fe_code};
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
        BoolExpr::Comparison { lhs, op, rhs } => Ok(BoolExpr::Comparison {
            lhs: materialize_scalar_subqueries(
                lhs,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params.clone(),
                ctx,
            )?,
            op: *op,
            rhs: materialize_scalar_subqueries(
                rhs,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?,
        }),
        BoolExpr::Literal(_) => Ok(expr.clone()),
        BoolExpr::IsNull { expr, negated } => Ok(BoolExpr::IsNull {
            expr: materialize_scalar_subqueries(
                expr,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?,
            negated: *negated,
        }),
        BoolExpr::InListValues { expr, values } => Ok(BoolExpr::InListValues {
            expr: materialize_scalar_subqueries(
                expr,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?,
            values: values.clone(),
        }),
        BoolExpr::InSubquery { expr: lhs, subplan } => {
            let lhs = materialize_scalar_subqueries(
                lhs,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params.clone(),
                ctx,
            )?;
            let values = materialize_subquery_values(
                subplan,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?;
            Ok(BoolExpr::InListValues { expr: lhs, values })
        }
    }
}

pub fn materialize_scalar_subqueries(
    expr: &ScalarExpr,
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<ScalarExpr> {
    Ok(match expr {
        ScalarExpr::Literal(_)
        | ScalarExpr::Column(_)
        | ScalarExpr::ColumnIdx(_)
        | ScalarExpr::ExcludedIdx(_)
        | ScalarExpr::Param { .. } => expr.clone(),
        ScalarExpr::BinaryOp { op, left, right } => ScalarExpr::BinaryOp {
            op: *op,
            left: Box::new(materialize_scalar_subqueries(
                left,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params.clone(),
                ctx,
            )?),
            right: Box::new(materialize_scalar_subqueries(
                right,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?),
        },
        ScalarExpr::UnaryOp { op, expr } => ScalarExpr::UnaryOp {
            op: *op,
            expr: Box::new(materialize_scalar_subqueries(
                expr,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?),
        },
        ScalarExpr::Cast { expr, ty } => ScalarExpr::Cast {
            expr: Box::new(materialize_scalar_subqueries(
                expr,
                db,
                txn_manager,
                session,
                snapshot_xid,
                params,
                ctx,
            )?),
            ty: ty.clone(),
        },
        ScalarExpr::Func { func, args } => ScalarExpr::Func {
            func: *func,
            args: args
                .iter()
                .map(|arg| {
                    materialize_scalar_subqueries(
                        arg,
                        db,
                        txn_manager,
                        session,
                        snapshot_xid,
                        params.clone(),
                        ctx,
                    )
                })
                .collect::<PgWireResult<Vec<_>>>()?,
        },
        ScalarExpr::Predicate(expr) => ScalarExpr::Predicate(Box::new(materialize_in_subqueries(
            expr,
            db,
            txn_manager,
            session,
            snapshot_xid,
            params,
            ctx,
        )?)),
        ScalarExpr::Subquery(plan) => ScalarExpr::Literal(materialize_scalar_subquery_value(
            plan,
            db,
            txn_manager,
            session,
            snapshot_xid,
            params,
            ctx,
        )?),
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => ScalarExpr::Case {
            when_then: when_then
                .iter()
                .map(|(cond, result)| {
                    Ok((
                        materialize_in_subqueries(
                            cond,
                            db,
                            txn_manager,
                            session,
                            snapshot_xid,
                            params.clone(),
                            ctx,
                        )?,
                        materialize_scalar_subqueries(
                            result,
                            db,
                            txn_manager,
                            session,
                            snapshot_xid,
                            params.clone(),
                            ctx,
                        )?,
                    ))
                })
                .collect::<PgWireResult<Vec<_>>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|expr| {
                    materialize_scalar_subqueries(
                        expr,
                        db,
                        txn_manager,
                        session,
                        snapshot_xid,
                        params.clone(),
                        ctx,
                    )
                    .map(Box::new)
                })
                .transpose()?,
        },
    })
}

fn materialize_scalar_subquery_value(
    plan: &Plan,
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<Value> {
    let (mut exec, _, _) =
        build_executor(db, txn_manager, session, snapshot_xid, plan, params, ctx)?;
    block_on(exec.open())?;
    let mut value: Option<Value> = None;
    while let Some(row) = block_on(exec.next())? {
        if row.len() != 1 {
            block_on(exec.close())?;
            return Err(fe_code("42601", "subquery must return only one column"));
        }
        if value.is_some() {
            block_on(exec.close())?;
            return Err(fe_code(
                "21000",
                "more than one row returned by a subquery used as an expression",
            ));
        }
        value = row.first().cloned();
    }
    block_on(exec.close())?;
    Ok(value.unwrap_or(Value::Null))
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
