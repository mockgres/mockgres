use std::sync::Arc;

use pgwire::error::PgWireResult;

use crate::db::{Db, LockOwner};
use crate::engine::{
    CountExec, ExecNode, Expr, FilterExec, LimitExec, LockSpec, NestedLoopJoinExec, OrderExec,
    Plan, ProjectExec, ReturningClause, ReturningExpr, ScalarExpr, Schema, SeqScanExec, UpdateSet,
    Value, ValuesExec, eval_scalar_expr, fe, fe_code,
};
use crate::session::{RowPointer, Session};
use crate::storage::{Row, RowId};
use crate::txn::{TransactionManager, TxId, TxnStatus, VisibilityContext};

use super::errors::map_db_err;
use super::insert::evaluate_insert_source;

pub fn command_tag(plan: &Plan) -> &'static str {
    match plan {
        Plan::Values { .. }
        | Plan::SeqScan { .. }
        | Plan::Projection { .. }
        | Plan::CountRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::UnboundJoin { .. }
        | Plan::Join { .. } => "SELECT",

        Plan::CreateTable { .. } => "CREATE TABLE",
        Plan::AlterTableAddColumn { .. } | Plan::AlterTableDropColumn { .. } => "ALTER TABLE",
        Plan::CreateIndex { .. } => "CREATE INDEX",
        Plan::DropIndex { .. } => "DROP INDEX",
        Plan::DropTable { .. } => "DROP TABLE",
        Plan::ShowVariable { .. } => "SHOW",
        Plan::SetVariable { .. } => "SET",
        Plan::InsertValues { .. } => "INSERT",
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
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    match p {
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
            )?;
            Ok((
                Box::new(ProjectExec::new(
                    schema.clone(),
                    child,
                    exprs.clone(),
                    params.clone(),
                )),
                None,
                cnt,
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
            let schema_name = table.schema.as_deref().unwrap_or("public");
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
            table: _,
            input,
            lock,
            row_id_idx,
            schema,
        } => {
            let (child, _tag, _cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                input,
                params.clone(),
            )?;
            let epoch = session
                .current_epoch()
                .ok_or_else(|| fe("FOR UPDATE requires an active transaction"))?;
            let owner = LockOwner::new(session.id(), epoch);
            let exec =
                LockApplyExec::new(schema.clone(), child, *lock, *row_id_idx, owner, db.clone());
            Ok((Box::new(exec), None, None))
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
            )?;
            let child_schema = child.schema().clone();
            let mut node: Box<dyn ExecNode> = Box::new(FilterExec::new(
                child_schema.clone(),
                child,
                expr.clone(),
                params.clone(),
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
                node = Box::new(ProjectExec::new(proj_schema, node, exprs, params.clone()));
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
            )?;
            let schema = child.schema().clone();
            let exec = Box::new(OrderExec::new(schema, child, keys.clone(), params.clone())?);
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
            let (left_exec, _ltag, left_cnt) =
                build_executor(db, txn_manager, session, snapshot_xid, left, params.clone())?;
            let (right_exec, _rtag, right_cnt) = build_executor(
                db,
                txn_manager,
                session,
                snapshot_xid,
                right,
                params.clone(),
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
                )?),
                None,
                out_cnt,
            ))
        }

        Plan::CreateTable {
            table,
            cols,
            pk,
            foreign_keys,
        } => {
            let mut db = db.write();
            let schema_name = table.schema.as_deref().unwrap_or("public");
            db.create_table(
                schema_name,
                &table.name,
                cols.clone(),
                pk.clone(),
                foreign_keys.clone(),
            )
            .map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("CREATE TABLE".into()),
                None,
            ))
        }

        Plan::AlterTableAddColumn {
            table,
            column,
            if_not_exists,
        } => {
            let mut db = db.write();
            let schema_name = table.schema.as_deref().unwrap_or("public");
            db.alter_table_add_column(schema_name, &table.name, column.clone(), *if_not_exists)
                .map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ALTER TABLE".into()),
                None,
            ))
        }

        Plan::AlterTableDropColumn {
            table,
            column,
            if_exists,
        } => {
            let mut db = db.write();
            let schema_name = table.schema.as_deref().unwrap_or("public");
            db.alter_table_drop_column(schema_name, &table.name, column, *if_exists)
                .map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ALTER TABLE".into()),
                None,
            ))
        }

        Plan::CreateIndex {
            table,
            name,
            columns,
            if_not_exists,
        } => {
            let mut db = db.write();
            let schema_name = table.schema.as_deref().unwrap_or("public");
            db.create_index(
                schema_name,
                &table.name,
                name,
                columns.clone(),
                *if_not_exists,
            )
            .map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("CREATE INDEX".into()),
                None,
            ))
        }

        Plan::DropIndex { indexes, if_exists } => {
            let mut db = db.write();
            for idx in indexes {
                let schema_name = idx.schema.as_deref().unwrap_or("public");
                db.drop_index(schema_name, &idx.name, *if_exists)
                    .map_err(map_db_err)?;
            }
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP INDEX".into()),
                None,
            ))
        }
        Plan::DropTable { tables, if_exists } => {
            let mut db = db.write();
            for table in tables {
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.drop_table(schema_name, &table.name, *if_exists)
                    .map_err(map_db_err)?;
            }
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP TABLE".into()),
                None,
            ))
        }

        Plan::InsertValues {
            table,
            columns,
            rows,
            returning,
            returning_schema,
        } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let table_meta = {
                let db = db.read();
                db.resolve_table(schema_name, &table.name)
                    .map_err(map_db_err)?
                    .clone()
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
                let mut full = vec![crate::db::CellInput::Default; table_meta.columns.len()];
                match &column_map {
                    Some(cols) => {
                        for (idx, src) in row.iter().enumerate() {
                            let value = evaluate_insert_source(src, &params)?;
                            full[cols[idx]] = value;
                        }
                    }
                    None => {
                        for (idx, src) in row.iter().enumerate() {
                            full[idx] = evaluate_insert_source(src, &params)?;
                        }
                    }
                }
                realized.push(full);
            }
            let (txid, autocommit) = writer_txid(session, txn_manager);
            let (inserted, inserted_rows, inserted_ptrs): (usize, Vec<Row>, Vec<RowPointer>) = {
                let mut db = db.write();
                match db.insert_full_rows(schema_name, &table.name, realized, txid) {
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
            }
            let tag = format!("INSERT 0 {}", inserted);
            if let Some(clause) = returning.clone() {
                let schema = returning_schema
                    .clone()
                    .expect("returning schema missing for INSERT");
                let rows = materialize_returning_rows(&clause, &inserted_rows, &params)?;
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
        Plan::Update {
            table,
            sets,
            filter,
            returning,
            returning_schema,
        } => {
            let assignments = sets
                .iter()
                .map(|set| match set {
                    UpdateSet::ByIndex(idx, expr) => Ok((*idx, expr.clone())),
                    UpdateSet::ByName(name, _) => {
                        Err(fe(format!("unbound assignment target: {name}")))
                    }
                })
                .collect::<PgWireResult<Vec<_>>>()?;
            let schema_name = table.schema.as_deref().unwrap_or("public");
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
                let rows = materialize_returning_rows(&clause, &updated_rows, &params)?;
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
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema,
        } => {
            let schema_name = table.schema.as_deref().unwrap_or("public");
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
                let rows = materialize_returning_rows(&clause, &removed_rows, &params)?;
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
        Plan::ShowVariable { name, schema } => {
            let value = match super::mapping::lookup_show_value(name) {
                Some(v) => v,
                None => return Err(fe_code("0A000", format!("SHOW {} not supported", name))),
            };
            let rows = vec![vec![Expr::Literal(Value::Text(value))]];
            let exec = ValuesExec::new(schema.clone(), rows)?;
            Ok((Box::new(exec), Some("SHOW".into()), Some(1)))
        }
        Plan::SetVariable { name, .. } => {
            if name != "client_min_messages" {
                return Err(fe_code("0A000", format!("SET {} not supported", name)));
            }
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("SET".into()),
                None,
            ))
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
        Plan::UnboundSeqScan { .. } | Plan::UnboundJoin { .. } => {
            Err(fe("unbound plan; call binder first"))
        }
    }
}

fn materialize_returning_rows(
    clause: &ReturningClause,
    rows: &[Row],
    params: &[Value],
) -> PgWireResult<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut projected = Vec::with_capacity(clause.exprs.len());
        for item in &clause.exprs {
            match item {
                ReturningExpr::Expr { expr, .. } => {
                    let value = eval_scalar_expr(row, expr, params)?;
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

struct LockApplyExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    owner: LockOwner,
    db: Arc<parking_lot::RwLock<Db>>,
}

impl LockApplyExec {
    fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        lock_spec: LockSpec,
        row_id_idx: usize,
        owner: LockOwner,
        db: Arc<parking_lot::RwLock<Db>>,
    ) -> Self {
        Self {
            schema,
            child,
            lock_spec,
            row_id_idx,
            owner,
            db,
        }
    }
}

impl ExecNode for LockApplyExec {
    fn open(&mut self) -> PgWireResult<()> {
        self.child.open()
    }

    fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        loop {
            let Some(mut row) = self.child.next()? else {
                return Ok(None);
            };
            if row.len() <= self.row_id_idx {
                return Err(fe("row identifier column missing from plan output"));
            }
            let row_id_value = row.remove(self.row_id_idx);
            let Value::Int64(raw_id) = row_id_value else {
                return Err(fe("row identifier column has unexpected type"));
            };
            if raw_id < 0 {
                return Err(fe("row identifier cannot be negative"));
            }
            let row_id = raw_id as RowId;
            let acquired = {
                let db_read = self.db.read();
                db_read.lock_row(
                    self.lock_spec.target,
                    row_id,
                    self.owner,
                    self.lock_spec.skip_locked,
                )
            };
            match acquired {
                Ok(true) => return Ok(Some(row)),
                Ok(false) => continue,
                Err(e) => return Err(map_db_err(e)),
            }
        }
    }

    fn close(&mut self) -> PgWireResult<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn writer_txid(session: &Arc<Session>, txn_manager: &Arc<TransactionManager>) -> (TxId, bool) {
    if let Some(txid) = session.current_tx() {
        (txid, false)
    } else {
        let txid = txn_manager.allocate();
        txn_manager.set_status(txid, TxnStatus::InProgress);
        (txid, true)
    }
}

fn finish_writer_tx(
    txn_manager: &Arc<TransactionManager>,
    txid: TxId,
    autocommit: bool,
    success: bool,
) {
    if autocommit {
        let status = if success {
            TxnStatus::Committed
        } else {
            TxnStatus::Aborted
        };
        txn_manager.set_status(txid, status);
    }
}

fn begin_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
) -> PgWireResult<()> {
    if session.current_tx().is_some() {
        return Err(fe_code("25001", "transaction already in progress"));
    }
    let txid = txn_manager.allocate();
    txn_manager.set_status(txid, TxnStatus::InProgress);
    session.begin_transaction_epoch();
    session.set_current_tx(Some(txid));
    session.reset_changes();
    Ok(())
}

fn commit_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
    db: &Arc<parking_lot::RwLock<Db>>,
) -> PgWireResult<()> {
    let Some(txid) = session.current_tx() else {
        return Err(fe_code("25P01", "no transaction in progress"));
    };
    txn_manager.set_status(txid, TxnStatus::Committed);
    session.set_current_tx(None);
    session.reset_changes();
    if let Some(epoch) = session.end_transaction_epoch() {
        let db_read = db.read();
        db_read.release_locks(LockOwner::new(session.id(), epoch));
    }
    Ok(())
}

fn rollback_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
    db: &Arc<parking_lot::RwLock<Db>>,
) -> PgWireResult<()> {
    let Some(txid) = session.current_tx() else {
        return Err(fe_code("25P01", "no transaction in progress"));
    };
    let changes = session.take_changes();
    session.set_current_tx(None);
    {
        let mut db = db.write();
        db.rollback_transaction_changes(&changes, txid)
            .map_err(map_db_err)?;
    }
    txn_manager.set_status(txid, TxnStatus::Aborted);
    if let Some(epoch) = session.end_transaction_epoch() {
        let db_read = db.read();
        db_read.release_locks(LockOwner::new(session.id(), epoch));
    }
    Ok(())
}
