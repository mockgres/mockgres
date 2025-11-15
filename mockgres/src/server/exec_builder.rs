use std::sync::Arc;

use pgwire::error::{PgWireError, PgWireResult};

use crate::catalog::{SchemaId, SchemaName};
use crate::db::{Db, LockOwner};
use crate::engine::{
    AggCall, AggFunc, DbDdlKind, EvalContext, ExecNode, Expr, ObjName, Plan, ReturningClause,
    ReturningExpr, Schema, UpdateSet, Value, ValuesExec, eval_scalar_expr, fe, fe_code,
};
use crate::session::{RowPointer, Session, SessionTimeZone};
use crate::storage::Row;
use crate::txn::{TransactionManager, TxId, VisibilityContext};

use super::errors::map_db_err;
use super::exec::read::build_read_executor;
use super::exec::tx::{
    begin_transaction, commit_transaction, finish_writer_tx, rollback_transaction, writer_txid,
};
use super::insert::evaluate_insert_source;

pub(crate) fn schema_or_public(schema: &Option<SchemaName>) -> &str {
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

pub(crate) fn assert_supported_aggs(aggs: &[(AggCall, String)]) {
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
        Plan::Values { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::SeqScan { .. }
        | Plan::LockRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::Join { .. } => {
            return build_read_executor(db, txn_manager, session, snapshot_xid, p, params, ctx);
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
