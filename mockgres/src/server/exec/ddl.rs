use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::{PgWireError, PgWireResult};

use crate::catalog::{SchemaId, SchemaName};
use crate::db::Db;
use crate::engine::{
    DbDdlKind, EvalContext, ExecNode, ObjName, Plan, Schema, ValuesExec, fe, fe_code,
};
use crate::server::errors::map_db_err;
use crate::session::Session;

pub(crate) fn build_ddl_executor(
    db: &Arc<RwLock<Db>>,
    session: &Arc<Session>,
    plan: &Plan,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    match plan {
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
        Plan::UnsupportedDbDDL { kind, .. } => Err(unsupported_dbddl_error(*kind)),
        Plan::CreateDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Create)),
        Plan::DropDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Drop)),
        Plan::AlterDatabase { .. } => Err(unsupported_dbddl_error(DbDdlKind::Alter)),
        _ => unreachable!("non-DDL plan routed to build_ddl_executor"),
    }
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
