use std::sync::Arc;

use async_trait::async_trait;
use pgwire::error::{PgWireError, PgWireResult};

use crate::catalog::{SchemaId, SchemaName};
use crate::db::{Db, LockHandle, LockOwner};
use crate::engine::{
    AggCall, AggFunc, CountExec, DbDdlKind, EvalContext, ExecNode, Expr, FilterExec,
    HashAggregateExec, LimitExec, LockSpec, NestedLoopJoinExec, ObjName, OrderExec, Plan,
    ProjectExec, ReturningClause, ReturningExpr, ScalarExpr, Schema, SeqScanExec, UpdateSet, Value,
    ValuesExec, eval_scalar_expr, fe, fe_code,
};
use crate::session::{RowPointer, Session, SessionTimeZone, now_utc_micros};
use crate::storage::{Row, RowId};
use crate::txn::{TransactionManager, TxId, TxnStatus, VisibilityContext};

use super::errors::map_db_err;
use super::insert::evaluate_insert_source;

fn schema_or_public(schema: &Option<SchemaName>) -> &str {
    schema.as_ref().map(|s| s.as_str()).unwrap_or("public")
}

fn resolve_table_schema(
    db: &Db,
    session: &Session,
    table: &ObjName,
) -> PgWireResult<Option<String>> {
    if let Some(schema) = &table.schema {
        if db.catalog.schema_entry(schema.as_str()).is_none() {
            return Err(fe_code(
                "3F000",
                format!("schema \"{}\" does not exist", schema.as_str()),
            ));
        }
        if db.catalog.get_table(schema.as_str(), &table.name).is_some() {
            return Ok(Some(schema.as_str().to_string()));
        }
        return Ok(None);
    }
    let search_path = session.search_path();
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(&schema_id) {
            if entry.objects.contains_key(&table.name) {
                return Ok(Some(entry.name.as_str().to_string()));
            }
        }
    }
    Ok(None)
}

fn resolve_schema_for_create(
    db: &Db,
    session: &Session,
    explicit: Option<&SchemaName>,
) -> PgWireResult<String> {
    if let Some(schema) = explicit {
        if db.catalog.schema_entry(schema.as_str()).is_none() {
            return Err(fe_code(
                "3F000",
                format!("schema \"{}\" does not exist", schema.as_str()),
            ));
        }
        return Ok(schema.as_str().to_string());
    }
    let search_path = session.search_path();
    if let Some(first) = search_path.first() {
        if let Some(entry) = db.catalog.schemas.get(first) {
            return Ok(entry.name.as_str().to_string());
        } else {
            return Err(fe_code(
                "3F000",
                "first schema in search_path does not exist",
            ));
        }
    }
    if db.catalog.schema_entry("public").is_some() {
        return Ok("public".to_string());
    }
    Err(fe("no schema available in search_path"))
}

fn resolve_index_schema(
    db: &Db,
    session: &Session,
    index: &ObjName,
) -> PgWireResult<Option<String>> {
    if let Some(schema) = &index.schema {
        if db.catalog.schema_entry(schema.as_str()).is_none() {
            return Err(fe_code(
                "3F000",
                format!("schema \"{}\" does not exist", schema.as_str()),
            ));
        }
        return Ok(Some(schema.as_str().to_string()));
    }
    let search_path = session.search_path();
    for schema_id in search_path {
        if schema_contains_index(db, schema_id, &index.name) {
            if let Some(entry) = db.catalog.schemas.get(&schema_id) {
                return Ok(Some(entry.name.as_str().to_string()));
            }
        }
    }
    Ok(None)
}

fn schema_contains_index(db: &Db, schema_id: SchemaId, index_name: &str) -> bool {
    let Some(entry) = db.catalog.schemas.get(&schema_id) else {
        return false;
    };
    for table_id in entry.objects.values() {
        if let Some(meta) = db.catalog.tables_by_id.get(table_id) {
            if meta.indexes.iter().any(|idx| idx.name == index_name) {
                return true;
            }
        }
    }
    false
}

fn derive_unique_index_name(table: &str, columns: &[String]) -> String {
    let suffix = if columns.is_empty() {
        "key".to_string()
    } else {
        columns.join("_")
    };
    if suffix.is_empty() {
        format!("{}_key", table)
    } else {
        format!("{}_{}_key", table, suffix)
    }
}

fn assert_supported_aggs(aggs: &[(AggCall, String)]) {
    for (agg, _) in aggs {
        match agg.func {
            AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {}
        }
    }
}

pub fn command_tag(plan: &Plan) -> &'static str {
    match plan {
        Plan::Values { .. }
        | Plan::SeqScan { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::UnboundJoin { .. }
        | Plan::Join { .. } => "SELECT",

        Plan::CreateTable { .. } => "CREATE TABLE",
        Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableDropConstraint { .. } => "ALTER TABLE",
        Plan::CreateIndex { .. } => "CREATE INDEX",
        Plan::DropIndex { .. } => "DROP INDEX",
        Plan::DropTable { .. } => "DROP TABLE",
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
        Plan::InsertValues { .. } => "INSERT",
        Plan::Update { .. } => "UPDATE",
        Plan::Delete { .. } => "DELETE",
        Plan::UnboundSeqScan { .. } => "SELECT",
        Plan::BeginTransaction => "BEGIN",
        Plan::CommitTransaction => "COMMIT",
        Plan::RollbackTransaction => "ROLLBACK",
    }
}

fn unsupported_dbddl_error(kind: DbDdlKind) -> PgWireError {
    let message = match kind {
        DbDdlKind::Create => {
            "CREATE DATABASE is not supported in mockgres; start a new instance instead."
        }
        DbDdlKind::Drop => {
            "DROP DATABASE is not supported in mockgres; start a new instance instead."
        }
        DbDdlKind::Alter => {
            "ALTER DATABASE is not supported in mockgres; start a new instance instead."
        }
    };
    fe_code("0A000", message)
}

pub fn build_executor(
    db: &Arc<parking_lot::RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    p: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
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
            let exec = LockApplyExec::new(
                schema.clone(),
                child,
                *lock,
                *row_id_idx,
                owner,
                lock_handle,
            );
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

        Plan::CreateTable {
            table,
            cols,
            pk,
            foreign_keys,
            uniques,
        } => {
            let schema_name = {
                let db_read = db.read();
                resolve_schema_for_create(&db_read, session, table.schema.as_ref())?
            };
            let search_path = session.search_path();
            let mut db_write = db.write();
            db_write
                .create_table(
                    &schema_name,
                    &table.name,
                    cols.clone(),
                    pk.clone(),
                    foreign_keys.clone(),
                    &search_path,
                )
                .map_err(map_db_err)?;
            if !uniques.is_empty() {
                for u in &*uniques {
                    let idx_name = u
                        .name
                        .clone()
                        .unwrap_or_else(|| derive_unique_index_name(&table.name, &u.columns));
                    db_write
                        .create_index(
                            &schema_name,
                            &table.name,
                            &idx_name,
                            u.columns.clone(),
                            true,
                            true,
                        )
                        .map_err(map_db_err)?;
                }
            }
            drop(db_write);
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
            let schema_name = {
                let db_read = db.read();
                match resolve_table_schema(&db_read, session, table)? {
                    Some(name) => name,
                    None => return Err(fe_code("42P01", format!("no such table {}", table.name))),
                }
            };
            let mut db_write = db.write();
            db_write
                .alter_table_add_column(
                    &schema_name,
                    &table.name,
                    column.clone(),
                    *if_not_exists,
                    ctx,
                )
                .map_err(map_db_err)?;
            drop(db_write);
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
            let schema_name = {
                let db_read = db.read();
                match resolve_table_schema(&db_read, session, table)? {
                    Some(name) => name,
                    None => {
                        if *if_exists {
                            return Ok((
                                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                                Some("ALTER TABLE".into()),
                                None,
                            ));
                        }
                        return Err(fe_code("42P01", format!("no such table {}", table.name)));
                    }
                }
            };
            let mut db_write = db.write();
            db_write
                .alter_table_drop_column(&schema_name, &table.name, column, *if_exists)
                .map_err(map_db_err)?;
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ALTER TABLE".into()),
                None,
            ))
        }

        Plan::AlterTableAddConstraintUnique {
            table,
            name,
            columns,
        } => {
            let schema_name = {
                let db_read = db.read();
                match resolve_table_schema(&db_read, session, table)? {
                    Some(name) => name,
                    None => return Err(fe_code("42P01", format!("no such table {}", table.name))),
                }
            };
            let index_name = name
                .clone()
                .unwrap_or_else(|| derive_unique_index_name(&table.name, columns));
            let mut db_write = db.write();
            db_write
                .create_index(
                    &schema_name,
                    &table.name,
                    &index_name,
                    columns.clone(),
                    false,
                    true,
                )
                .map_err(map_db_err)?;
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ALTER TABLE".into()),
                None,
            ))
        }

        Plan::AlterTableDropConstraint {
            table,
            name,
            if_exists,
        } => {
            let schema_name = {
                let db_read = db.read();
                match resolve_table_schema(&db_read, session, table)? {
                    Some(name) => name,
                    None => {
                        if *if_exists {
                            return Ok((
                                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                                Some("ALTER TABLE".into()),
                                None,
                            ));
                        }
                        return Err(fe_code("42P01", format!("no such table {}", table.name)));
                    }
                }
            };
            let constraint_name = name.clone();
            {
                let db_read = db.read();
                let Some(meta) = db_read.catalog.get_table(&schema_name, &table.name) else {
                    if *if_exists {
                        return Ok((
                            Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                            Some("ALTER TABLE".into()),
                            None,
                        ));
                    }
                    return Err(fe_code("42P01", format!("no such table {}", table.name)));
                };
                let has_unique = meta
                    .indexes
                    .iter()
                    .any(|idx| idx.unique && idx.name == constraint_name);
                if !has_unique {
                    if *if_exists {
                        return Ok((
                            Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                            Some("ALTER TABLE".into()),
                            None,
                        ));
                    }
                    return Err(fe_code(
                        "42704",
                        format!(
                            "constraint {} on table {}.{} does not exist",
                            constraint_name, schema_name, table.name
                        ),
                    ));
                }
            }
            let mut db_write = db.write();
            db_write
                .drop_index(&schema_name, &constraint_name, *if_exists)
                .map_err(map_db_err)?;
            drop(db_write);
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
            is_unique,
        } => {
            let schema_name = {
                let db_read = db.read();
                match resolve_table_schema(&db_read, session, table)? {
                    Some(name) => name,
                    None => return Err(fe_code("42P01", format!("no such table {}", table.name))),
                }
            };
            let mut db_write = db.write();
            db_write
                .create_index(
                    &schema_name,
                    &table.name,
                    name,
                    columns.clone(),
                    *if_not_exists,
                    *is_unique,
                )
                .map_err(map_db_err)?;
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("CREATE INDEX".into()),
                None,
            ))
        }

        Plan::DropIndex { indexes, if_exists } => {
            let mut db_write = db.write();
            for idx in indexes {
                let schema_name = resolve_index_schema(&db_write, session, idx)?;
                match schema_name {
                    Some(schema) => {
                        db_write
                            .drop_index(&schema, &idx.name, *if_exists)
                            .map_err(map_db_err)?;
                    }
                    None => {
                        if !if_exists {
                            return Err(fe_code(
                                "42704",
                                format!("index {} does not exist", idx.name),
                            ));
                        }
                    }
                }
            }
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP INDEX".into()),
                None,
            ))
        }
        Plan::DropTable { tables, if_exists } => {
            let mut db_write = db.write();
            for table in tables {
                let schema_name = resolve_table_schema(&db_write, session, table)?;
                match schema_name {
                    Some(schema) => {
                        db_write
                            .drop_table(&schema, &table.name, *if_exists, false)
                            .map_err(map_db_err)?;
                    }
                    None => {
                        if !if_exists {
                            return Err(fe_code("42P01", format!("no such table {}", table.name)));
                        }
                    }
                }
            }
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP TABLE".into()),
                None,
            ))
        }
        Plan::CreateSchema {
            name,
            if_not_exists,
        } => {
            let mut db_write = db.write();
            db_write
                .create_schema(name.as_str(), *if_not_exists)
                .map_err(map_db_err)?;
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("CREATE SCHEMA".into()),
                None,
            ))
        }
        Plan::DropSchema {
            schemas,
            if_exists,
            cascade,
        } => {
            let mut db_write = db.write();
            for schema in schemas {
                db_write
                    .drop_schema(schema.as_str(), *cascade, *if_exists)
                    .map_err(map_db_err)?;
            }
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP SCHEMA".into()),
                None,
            ))
        }
        Plan::AlterSchemaRename { name, new_name } => {
            let mut db_write = db.write();
            db_write
                .rename_schema(name.as_str(), new_name.as_str())
                .map_err(map_db_err)?;
            drop(db_write);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ALTER SCHEMA".into()),
                None,
            ))
        }

        Plan::InsertValues {
            table,
            columns,
            rows,
            override_system_value,
            returning,
            returning_schema,
        } => {
            let override_system_value = *override_system_value;
            let schema_name = schema_or_public(&table.schema);
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
            let (inserted, inserted_rows, inserted_ptrs): (usize, Vec<Row>, Vec<RowPointer>) = {
                let mut db = db.write();
                match db.insert_full_rows(
                    schema_name,
                    &table.name,
                    realized,
                    override_system_value,
                    txid,
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
            }
            let tag = format!("INSERT 0 {}", inserted);
            if let Some(clause) = returning.clone() {
                let schema = returning_schema
                    .clone()
                    .expect("returning schema missing for INSERT");
                let rows = materialize_returning_rows(&clause, &inserted_rows, &params, ctx)?;
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
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema,
        } => {
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
        Plan::ShowVariable { name, schema } => {
            let value = if name == "search_path" {
                let ids = session.search_path();
                let parts = {
                    let db_read = db.read();
                    ids.into_iter()
                        .filter_map(|sid| db_read.catalog.schema_name(sid))
                        .map(|schema_name| schema_name.as_str().to_string())
                        .collect::<Vec<_>>()
                };
                parts.join(", ")
            } else if name == "timezone" || name == "time zone" {
                session.time_zone().display_value().to_string()
            } else {
                match super::mapping::lookup_show_value(name) {
                    Some(v) => v,
                    None => return Err(fe_code("0A000", format!("SHOW {} not supported", name))),
                }
            };
            let rows = vec![vec![Expr::Literal(Value::Text(value))]];
            let exec = ValuesExec::new(schema.clone(), rows)?;
            Ok((Box::new(exec), Some("SHOW".into()), Some(1)))
        }
        Plan::SetVariable { name, value } => match name.as_str() {
            "client_min_messages" => Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("SET".into()),
                None,
            )),
            "search_path" => {
                let schema_ids = {
                    let db_read = db.read();
                    match value {
                        Some(values) => {
                            if values.is_empty() {
                                return Err(fe("SET search_path requires at least one schema"));
                            }
                            let mut resolved = Vec::with_capacity(values.len());
                            for schema_name in values {
                                let id =
                                    db_read.catalog.schema_id(&schema_name).ok_or_else(|| {
                                        fe_code(
                                            "3F000",
                                            format!("schema \"{}\" does not exist", schema_name),
                                        )
                                    })?;
                                resolved.push(id);
                            }
                            resolved
                        }
                        None => {
                            let public_id = db_read
                                .catalog
                                .schema_id("public")
                                .ok_or_else(|| fe("public schema not found"))?;
                            vec![public_id]
                        }
                    }
                };
                session.set_search_path(schema_ids);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "timezone" => {
                let tz = match value {
                    Some(values) => {
                        if values.len() != 1 {
                            return Err(fe("SET TIME ZONE requires a single value"));
                        }
                        SessionTimeZone::parse(&values[0]).map_err(|e| fe_code("22023", e))?
                    }
                    None => SessionTimeZone::Utc,
                };
                session.set_time_zone(tz);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            _ => Err(fe_code("0A000", format!("SET {} not supported", name))),
        },
        Plan::UnsupportedDbDDL { kind, .. } => Err(unsupported_dbddl_error(*kind)),
        Plan::CreateDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Create)),
        Plan::DropDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Drop)),
        Plan::AlterDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Alter)),
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

struct LockApplyExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    owner: LockOwner,
    locks: LockHandle,
}

enum AcquireOutcome {
    Acquired,
    Skipped,
}

impl LockApplyExec {
    fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        lock_spec: LockSpec,
        row_id_idx: usize,
        owner: LockOwner,
        locks: LockHandle,
    ) -> Self {
        Self {
            schema,
            child,
            lock_spec,
            row_id_idx,
            owner,
            locks,
        }
    }
}

#[async_trait]
impl ExecNode for LockApplyExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.child.open().await
    }

    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        loop {
            let Some(mut row) = self.child.next().await? else {
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
            let acquire_result = if self.lock_spec.skip_locked {
                self.locks
                    .lock_row_skip_locked(self.lock_spec.target, row_id, self.owner)
                    .map(|acquired| {
                        if acquired {
                            AcquireOutcome::Acquired
                        } else {
                            AcquireOutcome::Skipped
                        }
                    })
            } else if self.lock_spec.nowait {
                self.locks
                    .lock_row_nowait(self.lock_spec.target, row_id, self.owner)
                    .map(|_| AcquireOutcome::Acquired)
            } else {
                self.locks
                    .lock_row_blocking(self.lock_spec.target, row_id, self.owner)
                    .await
                    .map(|_| AcquireOutcome::Acquired)
            };
            match acquire_result {
                Ok(AcquireOutcome::Acquired) => return Ok(Some(row)),
                Ok(AcquireOutcome::Skipped) => continue,
                Err(e) => return Err(map_db_err(e)),
            }
        }
    }

    async fn close(&mut self) -> PgWireResult<()> {
        self.child.close().await
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
    session.set_txn_start_micros(now_utc_micros());
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
    session.clear_txn_start_micros();
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
    session.clear_txn_start_micros();
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
