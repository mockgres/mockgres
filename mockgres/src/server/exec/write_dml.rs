use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{CellInput, Db, LockOwner, ResolvedOnConflictKind};
use crate::engine::{
    BoolExpr, EvalContext, ExecNode, InsertSource, ObjName, OnConflictAction, ReturningClause,
    ReturningExpr, Schema, UpdateSet, Value, ValuesExec, eval_scalar_expr, fe, fe_code,
};
use crate::server::errors::map_db_err;
use crate::server::exec_builder::schema_or_public;
use crate::server::insert::evaluate_insert_source;
use crate::session::{RowPointer, Session};
use crate::storage::Row;
use crate::txn::{TransactionManager, TxId, VisibilityContext};

use super::tx::{finish_writer_tx, writer_txid};

pub(crate) fn build_insert_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    table: &ObjName,
    columns: &Option<Vec<String>>,
    rows: &[Vec<InsertSource>],
    override_system_value: bool,
    on_conflict: &Option<OnConflictAction>,
    returning: &Option<ReturningClause>,
    returning_schema: &Option<Schema>,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    let schema_name = schema_or_public(&table.schema);
    let table_meta = {
        let db = db.read();
        db.resolve_table(schema_name, &table.name)
            .map_err(map_db_err)?
            .clone()
    };
    let resolved_conflict: Option<ResolvedOnConflictKind> = match on_conflict {
        None => None,
        Some(OnConflictAction::DoNothing { target }) => {
            let db_read = db.read();
            Some(ResolvedOnConflictKind::DoNothing(
                db_read
                    .resolve_on_conflict_target(&table_meta, target)
                    .map_err(map_db_err)?,
            ))
        }
        Some(OnConflictAction::DoUpdate {
            target,
            sets,
            where_clause,
        }) => {
            let db_read = db.read();
            Some(ResolvedOnConflictKind::DoUpdate {
                target: db_read
                    .resolve_on_conflict_target(&table_meta, target)
                    .map_err(map_db_err)?,
                sets: sets.clone(),
                where_clause: where_clause.clone(),
            })
        }
    };
    let column_map = if let Some(cols) = columns {
        let mut indexes = Vec::with_capacity(cols.len());
        for col in cols {
            let pos = table_meta
                .columns
                .iter()
                .position(|c| c.name == *col)
                .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
            if indexes.iter().any(|i| *i == pos) {
                return Err(fe_code("42701", format!("column {col} specified twice")));
            }
            indexes.push(pos);
        }
        Some(indexes)
    } else {
        None
    };
    let mut realized = Vec::with_capacity(rows.len());
    for row in rows {
        match &column_map {
            Some(cols) if row.len() != cols.len() => {
                return Err(fe_code(
                    "21P01",
                    format!(
                        "INSERT has {} target columns but {} expressions",
                        cols.len(),
                        row.len()
                    ),
                ));
            }
            None if row.len() != table_meta.columns.len() => {
                return Err(fe_code(
                    "21P01",
                    format!(
                        "INSERT expects {} expressions, got {}",
                        table_meta.columns.len(),
                        row.len()
                    ),
                ));
            }
            _ => {}
        }
        let mut full = vec![CellInput::Default; table_meta.columns.len()];
        match &column_map {
            Some(cols) => {
                for (idx, src) in row.iter().enumerate() {
                    let value = evaluate_insert_source(src, &params, ctx)?;
                    full[cols[idx]] = value;
                }
            }
            None => {
                for (idx, src) in row.iter().enumerate() {
                    full[idx] = evaluate_insert_source(src, &params, ctx)?;
                }
            }
        }
        realized.push(full);
    }
    let (txid, autocommit) = writer_txid(session, txn_manager);
    let (inserted, inserted_rows, inserted_ptrs, updated_rows, updated_ptrs): (
        usize,
        Vec<Row>,
        Vec<RowPointer>,
        Vec<Row>,
        Vec<RowPointer>,
    ) = {
        let mut db = db.write();
        match db.insert_full_rows(
            schema_name,
            &table.name,
            realized,
            override_system_value,
            txid,
            params.as_ref(),
            ctx,
            resolved_conflict.clone(),
        ) {
            Ok(res) => res,
            Err(e) => {
                finish_writer_tx(txn_manager, txid, autocommit, false);
                return Err(map_db_err(e));
            }
        }
    };
    if autocommit {
        finish_writer_tx(txn_manager, txid, true, true);
    } else {
        session.record_inserts(inserted_ptrs.clone());
        session.record_touched(updated_ptrs.clone());
    }
    let tag = format!("INSERT 0 {}", inserted);
    if let Some(clause) = returning.clone() {
        let schema = returning_schema
            .clone()
            .expect("returning schema missing for INSERT");
        let all_rows: Vec<Row> = inserted_rows
            .iter()
            .zip(inserted_ptrs.iter())
            .chain(updated_rows.iter().zip(updated_ptrs.iter()))
            .map(|(row, _)| row.clone())
            .collect();
        let rows = materialize_returning_rows(&clause, &all_rows, &params, ctx)?;
        Ok((
            Box::new(ValuesExec::from_values(schema, rows)),
            Some(tag),
            Some(inserted),
        ))
    } else {
        Ok((
            Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
            Some(tag),
            Some(inserted),
        ))
    }
}

pub(crate) fn build_update_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    table: &ObjName,
    sets: &[UpdateSet],
    filter: &Option<BoolExpr>,
    returning: &Option<ReturningClause>,
    returning_schema: &Option<Schema>,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    let assignments = sets
        .iter()
        .map(|set| match set {
            UpdateSet::ByIndex(idx, expr) => Ok((*idx, expr.clone())),
            UpdateSet::ByName(name, _) => Err(fe(format!("unbound assignment target: {name}"))),
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let schema_name = schema_or_public(&table.schema);
    let (txid, autocommit) = writer_txid(session, txn_manager);
    let current_tx = session.current_tx();
    let visibility = VisibilityContext::new(txn_manager.as_ref(), snapshot_xid, current_tx);
    let epoch = session
        .current_epoch()
        .ok_or_else(|| fe("missing transaction context for UPDATE"))?;
    let lock_owner = LockOwner::new(session.id(), epoch);
    let (count, updated_rows, inserted_ptrs, touched_ptrs): (
        usize,
        Vec<Row>,
        Vec<RowPointer>,
        Vec<RowPointer>,
    ) = {
        let mut db = db.write();
        match db.update_rows(
            schema_name,
            &table.name,
            &assignments,
            filter.as_ref(),
            &params,
            &visibility,
            txid,
            lock_owner,
            ctx,
        ) {
            Ok(res) => res,
            Err(e) => {
                finish_writer_tx(txn_manager, txid, autocommit, false);
                return Err(map_db_err(e));
            }
        }
    };
    if autocommit {
        finish_writer_tx(txn_manager, txid, true, true);
    } else {
        session.record_inserts(inserted_ptrs);
        session.record_touched(touched_ptrs);
    }
    let tag = format!("UPDATE {}", count);
    if let Some(clause) = returning.clone() {
        let schema = returning_schema
            .clone()
            .expect("returning schema missing for UPDATE");
        let rows = materialize_returning_rows(&clause, &updated_rows, &params, ctx)?;
        Ok((
            Box::new(ValuesExec::from_values(schema, rows)),
            Some(tag),
            None,
        ))
    } else {
        Ok((
            Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
            Some(tag),
            None,
        ))
    }
}

pub(crate) fn build_delete_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    table: &ObjName,
    filter: &Option<BoolExpr>,
    returning: &Option<ReturningClause>,
    returning_schema: &Option<Schema>,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    let schema_name = schema_or_public(&table.schema);
    let (txid, autocommit) = writer_txid(session, txn_manager);
    let current_tx = session.current_tx();
    let visibility = VisibilityContext::new(txn_manager.as_ref(), snapshot_xid, current_tx);
    let epoch = session
        .current_epoch()
        .ok_or_else(|| fe("missing transaction context for DELETE"))?;
    let lock_owner = LockOwner::new(session.id(), epoch);
    let (count, removed_rows, touched_ptrs): (usize, Vec<Row>, Vec<RowPointer>) = {
        let mut db = db.write();
        match db.delete_rows(
            schema_name,
            &table.name,
            filter.as_ref(),
            &params,
            &visibility,
            txid,
            lock_owner,
            ctx,
        ) {
            Ok(res) => res,
            Err(e) => {
                finish_writer_tx(txn_manager, txid, autocommit, false);
                return Err(map_db_err(e));
            }
        }
    };
    if autocommit {
        finish_writer_tx(txn_manager, txid, true, true);
    } else {
        session.record_touched(touched_ptrs);
    }
    let tag = format!("DELETE {}", count);
    if let Some(clause) = returning.clone() {
        let schema = returning_schema
            .clone()
            .expect("returning schema missing for DELETE");
        let rows = materialize_returning_rows(&clause, &removed_rows, &params, ctx)?;
        Ok((
            Box::new(ValuesExec::from_values(schema, rows)),
            Some(tag),
            None,
        ))
    } else {
        Ok((
            Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
            Some(tag),
            None,
        ))
    }
}

fn materialize_returning_rows(
    clause: &ReturningClause,
    rows: &[Row],
    params: &[Value],
    ctx: &EvalContext,
) -> PgWireResult<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut projected = Vec::with_capacity(clause.exprs.len());
        for item in &clause.exprs {
            match item {
                ReturningExpr::Expr { expr, .. } => {
                    let value = eval_scalar_expr(row, expr, params, ctx)?;
                    projected.push(value);
                }
                ReturningExpr::Star => {
                    return Err(fe("RETURNING * not bound"));
                }
            }
        }
        out.push(projected);
    }
    Ok(out)
}
