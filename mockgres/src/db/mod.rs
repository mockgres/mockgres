use crate::catalog::{Catalog, SchemaId, TableId, TableMeta};
use crate::engine::{
    BoolExpr, Column, DataType, EvalContext, IdentitySpec, ReferentialAction, ScalarExpr, SqlError,
    Value, eval_bool_expr, eval_scalar_expr,
};
use crate::session::{RowPointer, TxnChanges};
use crate::storage::{Row, RowId, RowKey, Table, VersionedRow};
use crate::txn::{TxId, VisibilityContext};
use std::collections::HashMap;
use std::sync::Arc;

mod coerce;
mod constraints;
mod create;
mod dml_delete;
mod dml_insert;
mod dml_update;
mod locks;
mod mvcc;
mod schema_ddl;
mod visibility;

use locks::LockRegistry;

pub(crate) use coerce::{coerce_value_for_column, eval_column_default};
pub(crate) use constraints::*;
pub use locks::{LockHandle, LockOwner};
pub(crate) use visibility::{
    row_key_to_row_id, select_visible_version, select_visible_version_idx, visible_row_clone,
};

#[derive(Clone, Debug)]
pub enum CellInput {
    Value(Value),
    Default,
}

fn sql_err(code: &'static str, msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(SqlError::new(code, msg.into()))
}

#[derive(Debug)]
pub struct Db {
    pub catalog: Catalog,
    pub tables: HashMap<TableId, Table>,
    pub next_rel_id: u32,
    locks: Arc<LockRegistry>,
}

impl Default for Db {
    fn default() -> Self {
        let mut db = Self {
            catalog: Catalog::default(),
            tables: HashMap::new(),
            next_rel_id: 1,
            locks: Arc::new(LockRegistry::new()),
        };
        db.init_builtin_catalog();
        db
    }
}
impl Db {
    pub fn release_locks(&self, owner: LockOwner) {
        self.locks.release_owner(owner);
    }

    pub fn lock_handle(&self) -> LockHandle {
        LockHandle::new(Arc::clone(&self.locks))
    }

    fn table_meta_by_id(&self, id: TableId) -> Option<&TableMeta> {
        self.catalog.tables_by_id.get(&id)
    }

    pub fn alter_table_add_column(
        &mut self,
        schema: &str,
        name: &str,
        column: (
            String,
            DataType,
            bool,
            Option<ScalarExpr>,
            Option<IdentitySpec>,
        ),
        if_not_exists: bool,
        ctx: &EvalContext,
    ) -> anyhow::Result<()> {
        let (col_name, data_type, nullable, default_expr, identity) = column;
        if identity.is_some() {
            return Err(sql_err(
                "0A000",
                "ALTER TABLE ADD COLUMN does not yet support IDENTITY columns",
            ));
        }
        let (table_id, column_exists, meta_snapshot) = {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
            let exists = meta.columns.iter().any(|c| c.name == col_name);
            (meta.id, exists, meta.clone())
        };
        if column_exists {
            if if_not_exists {
                return Ok(());
            }
            return Err(sql_err(
                "42701",
                format!("column {col_name} already exists"),
            ));
        }
        let new_col_index = meta_snapshot.columns.len();
        let temp_column = Column {
            name: col_name.clone(),
            data_type: data_type.clone(),
            nullable,
            default: None,
            identity: None,
        };
        let append_value = if let Some(expr) = &default_expr {
            eval_column_default(expr, &temp_column, new_col_index, &meta_snapshot, ctx)?
        } else if nullable {
            Value::Null
        } else {
            return Err(sql_err(
                "23502",
                format!("column {col_name} must have a default or allow NULLs"),
            ));
        };
        {
            let table = self.tables.get_mut(&table_id).ok_or_else(|| {
                sql_err("XX000", format!("missing storage for table id {table_id}"))
            })?;
            for versions in table.rows_by_key.values_mut() {
                for version in versions.iter_mut() {
                    version.data.push(append_value.clone());
                }
            }
            table.identities.push(None);
        }
        if self.catalog.schema_entry(schema).is_none() {
            return Err(sql_err("3F000", format!("no such schema {schema}")));
        }
        let table_meta = self
            .catalog
            .table_meta_mut(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
        table_meta.columns.push(Column {
            name: col_name,
            data_type,
            nullable,
            default: default_expr,
            identity: None,
        });
        Ok(())
    }

    pub fn alter_table_drop_column(
        &mut self,
        schema: &str,
        name: &str,
        column: &str,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let (table_id, drop_idx) = {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
            let Some(idx) = meta.columns.iter().position(|c| c.name == column) else {
                if if_exists {
                    return Ok(());
                } else {
                    return Err(sql_err("42703", format!("column {column} does not exist")));
                }
            };
            if idx != meta.columns.len() - 1 {
                return Err(sql_err(
                    "0A000",
                    format!("can only drop the last column ({column} is at position {idx})"),
                ));
            }
            if meta
                .primary_key
                .as_ref()
                .map(|pk| pk.columns.contains(&idx))
                .unwrap_or(false)
            {
                return Err(sql_err(
                    "2BP01",
                    format!("cannot drop primary key column {column}"),
                ));
            }
            (meta.id, idx)
        };
        {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
            if meta
                .foreign_keys
                .iter()
                .any(|fk| fk.local_columns.contains(&drop_idx))
            {
                return Err(sql_err(
                    "2BP01",
                    format!("cannot drop column {column} referenced by a foreign key"),
                ));
            }
        }
        let inbound = collect_inbound_foreign_keys(&self.catalog, schema, name);
        if inbound
            .iter()
            .any(|fk| fk.fk.referenced_columns.contains(&drop_idx))
        {
            return Err(sql_err(
                "2BP01",
                format!("cannot drop column {column} referenced by another table"),
            ));
        }
        if let Some(table) = self.tables.get_mut(&table_id) {
            for versions in table.rows_by_key.values_mut() {
                for version in versions.iter_mut() {
                    if version.data.len() != drop_idx + 1 {
                        return Err(sql_err(
                            "XX000",
                            format!("row length mismatch while dropping column {column}"),
                        ));
                    }
                    version.data.pop();
                }
            }
            table.identities.pop();
        } else {
            return Err(sql_err(
                "XX000",
                format!("missing storage for table id {table_id}"),
            ));
        }
        if self.catalog.schema_entry(schema).is_none() {
            return Err(sql_err("3F000", format!("no such schema {schema}")));
        }
        let table_meta = self
            .catalog
            .table_meta_mut(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
        table_meta.columns.pop();
        Ok(())
    }

    pub fn create_index(
        &mut self,
        schema: &str,
        table: &str,
        index_name: &str,
        columns: Vec<String>,
        if_not_exists: bool,
        is_unique: bool,
    ) -> anyhow::Result<()> {
        if columns.is_empty() {
            return Err(sql_err("0A000", "index must reference at least one column"));
        }
        if self.catalog.schema_entry(schema).is_none() {
            return Err(sql_err("3F000", format!("no such schema {schema}")));
        }
        let table_meta = self
            .catalog
            .table_meta_mut(schema, table)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{table}")))?;
        if table_meta.indexes.iter().any(|idx| idx.name == index_name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(sql_err(
                "42P07",
                format!("index {index_name} already exists"),
            ));
        }
        let mut col_positions = Vec::with_capacity(columns.len());
        for col_name in columns {
            let pos = table_meta
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| sql_err("42703", format!("unknown column in index: {col_name}")))?;
            col_positions.push(pos);
        }
        table_meta.indexes.push(crate::catalog::IndexMeta {
            name: index_name.to_string(),
            columns: col_positions,
            unique: is_unique,
        });
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        schema: &str,
        index_name: &str,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let Some(schema_id) = self.catalog.schema_id(schema) else {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("3F000", format!("no such schema {schema}")))
            };
        };
        let table_ids: Vec<TableId> = self
            .catalog
            .schemas
            .get(&schema_id)
            .map(|entry| entry.objects.values().copied().collect())
            .unwrap_or_default();
        let mut removed = false;
        let mut removed_table_id = None;
        for tid in table_ids {
            if let Some(table_meta) = self.catalog.get_table_mut_by_id(&tid) {
                if let Some(pos) = table_meta
                    .indexes
                    .iter()
                    .position(|idx| idx.name == index_name)
                {
                    table_meta.indexes.remove(pos);
                    removed = true;
                    removed_table_id = Some(tid);
                    break;
                }
            }
        }
        if let Some(tid) = removed_table_id {
            if let Some(table) = self.tables.get_mut(&tid) {
                table.unique_maps.remove(index_name);
            }
        }
        if removed || if_exists {
            Ok(())
        } else {
            Err(sql_err(
                "42704",
                format!("index {index_name} does not exist"),
            ))
        }
    }

    pub fn resolve_table(&self, schema: &str, name: &str) -> anyhow::Result<&TableMeta> {
        self.catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))
    }

    pub fn resolve_table_in_search_path(
        &self,
        search_path: &[SchemaId],
        name: &str,
    ) -> anyhow::Result<&TableMeta> {
        for schema_id in search_path {
            if let Some(schema_name) = self.catalog.schema_name(*schema_id) {
                if let Some(table) = self.catalog.get_table(schema_name.as_str(), name) {
                    return Ok(table);
                }
            }
        }
        Err(sql_err("42P01", format!("no such table {name}")))
    }

    pub fn scan_bound_positions(
        &self,
        schema: &str,
        name: &str,
        positions: &[usize],
        visibility: &VisibilityContext,
    ) -> anyhow::Result<(Vec<Row>, Vec<(usize, String)>, Vec<RowId>)> {
        let tm = self.resolve_table(schema, name)?;
        let table = self
            .tables
            .get(&tm.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", tm.id)))?;

        let mut out_rows = Vec::new();
        let mut row_ids = Vec::new();
        for (key, versions) in table.scan_all() {
            if let Some(version) = select_visible_version(versions, visibility) {
                out_rows.push(positions.iter().map(|i| version.data[*i].clone()).collect());
                let row_id = row_key_to_row_id(key)?;
                row_ids.push(row_id);
            }
        }
        let cols = positions
            .iter()
            .map(|i| (*i, tm.columns[*i].name.clone()))
            .collect();
        Ok((out_rows, cols, row_ids))
    }

    fn ensure_outbound_foreign_keys(
        &self,
        table_schema: &str,
        table_name: &str,
        meta: &TableMeta,
        row: &[Value],
    ) -> anyhow::Result<Vec<Option<Vec<Value>>>> {
        let mut keys = Vec::with_capacity(meta.foreign_keys.len());
        for fk in &meta.foreign_keys {
            let key = build_fk_parent_key(row, fk);
            if let Some(ref vals) = key {
                ensure_parent_exists(&self.tables, fk, table_schema, table_name, vals)?;
            }
            keys.push(key);
        }
        Ok(keys)
    }
}
