use crate::catalog::{Catalog, ForeignKeyMeta, TableId, TableMeta};
use crate::engine::{
    BoolExpr, Column, DataType, ForeignKeySpec, ScalarExpr, SqlError, Value, cast_value_to_type,
    eval_bool_expr, eval_scalar_expr,
};
use crate::session::{RowPointer, TxnChanges};
use crate::storage::{Row, RowKey, Table, VersionedRow};
use crate::txn::{TxId, VisibilityContext};
use std::collections::HashMap;

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
    pub next_tid: u64,
}

impl Default for Db {
    fn default() -> Self {
        let mut catalog = Catalog::default();
        catalog.ensure_schema("public");
        Self {
            catalog,
            tables: HashMap::new(),
            next_tid: 1,
        }
    }
}
impl Db {
    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<(String, DataType, bool, Option<Value>)>,
        pk_names: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKeySpec>,
    ) -> anyhow::Result<TableId> {
        self.catalog.ensure_schema(schema);
        if self.catalog.get_table(schema, name).is_some() {
            return Err(sql_err(
                "42P07",
                format!("table already exists: {schema}.{name}"),
            ));
        }

        let id = self.next_tid;
        self.next_tid += 1;

        let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
        for (n, t, nullable, default_raw) in cols.into_iter() {
            let mut default_value = None;
            if let Some(def) = default_raw {
                let cast = cast_value_to_type(def, &t)?;
                if matches!(cast, Value::Null) && !nullable {
                    return Err(sql_err(
                        "23502",
                        format!("default NULL not allowed for NOT NULL column {n}"),
                    ));
                }
                default_value = Some(cast);
            }
            columns.push(Column {
                name: n,
                data_type: t,
                nullable,
                default: default_value,
            });
        }

        // map pk names -> positions
        let pk: Option<Vec<usize>> = match pk_names {
            None => None,
            Some(ns) => {
                let mut pos = Vec::with_capacity(ns.len());
                for n in ns {
                    let i = columns
                        .iter()
                        .position(|c| c.name == n)
                        .ok_or_else(|| sql_err("42703", format!("unknown pk column: {n}")))?;
                    pos.push(i);
                }
                Some(pos)
            }
        };

        let fk_metas = self.build_foreign_keys(schema, name, &columns, foreign_keys)?;

        let tm = TableMeta {
            id,
            name: name.to_string(),
            columns: columns.clone(),
            pk,
            indexes: vec![],
            foreign_keys: fk_metas,
        };

        self.catalog
            .schemas
            .get_mut(schema)
            .expect("schema exists after ensure")
            .tables
            .insert(name.to_string(), tm);

        self.tables.insert(id, Table::default());
        Ok(id)
    }

    fn build_foreign_keys(
        &self,
        table_schema: &str,
        table_name: &str,
        columns: &[Column],
        specs: Vec<ForeignKeySpec>,
    ) -> anyhow::Result<Vec<ForeignKeyMeta>> {
        let mut metas = Vec::with_capacity(specs.len());
        for spec in specs {
            if spec.columns.is_empty() {
                return Err(sql_err(
                    "42830",
                    format!("foreign key on {table_schema}.{table_name} requires columns"),
                ));
            }
            let mut local_indexes = Vec::with_capacity(spec.columns.len());
            for col in &spec.columns {
                let idx = columns
                    .iter()
                    .position(|c| &c.name == col)
                    .ok_or_else(|| {
                        sql_err(
                            "42703",
                            format!(
                                "foreign key references unknown column {col} on {table_schema}.{table_name}"
                            ),
                        )
                    })?;
                local_indexes.push(idx);
            }

            let referenced_schema = spec
                .referenced_table
                .schema
                .clone()
                .unwrap_or_else(|| "public".to_string());
            let referenced_table = spec.referenced_table.name.clone();
            let ref_meta = self
                .catalog
                .get_table(&referenced_schema, &referenced_table)
                .ok_or_else(|| {
                    sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} for foreign key on {}.{} does not exist",
                            referenced_schema, referenced_table, table_schema, table_name
                        ),
                    )
                })?;

            let referenced_indexes: Vec<usize> = if let Some(ref_cols) = spec.referenced_columns {
                if ref_cols.len() != local_indexes.len() {
                    return Err(sql_err(
                        "42830",
                        format!(
                            "foreign key on {}.{} has {} columns but referenced list has {}",
                            table_schema,
                            table_name,
                            local_indexes.len(),
                            ref_cols.len()
                        ),
                    ));
                }
                let mut idxs = Vec::with_capacity(ref_cols.len());
                for rc in ref_cols {
                    let idx = ref_meta
                        .columns
                        .iter()
                        .position(|c| c.name == rc)
                        .ok_or_else(|| {
                            sql_err(
                                "42703",
                                format!(
                                    "referenced column {}.{} does not exist on {}.{}",
                                    rc, referenced_table, referenced_schema, referenced_table
                                ),
                            )
                        })?;
                    idxs.push(idx);
                }
                idxs
            } else {
                let Some(pk) = ref_meta.pk.as_ref() else {
                    return Err(sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} must have a primary key or explicit column list",
                            referenced_schema, referenced_table
                        ),
                    ));
                };
                if pk.len() != local_indexes.len() {
                    return Err(sql_err(
                        "42830",
                        format!(
                            "foreign key on {}.{} does not match referenced primary key column count",
                            table_schema, table_name
                        ),
                    ));
                }
                pk.clone()
            };

            for (local_idx, ref_idx) in local_indexes.iter().zip(referenced_indexes.iter()) {
                let local_type = &columns[*local_idx].data_type;
                let ref_type = &ref_meta.columns[*ref_idx].data_type;
                if local_type != ref_type {
                    return Err(sql_err(
                        "42804",
                        format!(
                            "foreign key column {} type {:?} does not match referenced column {} type {:?}",
                            columns[*local_idx].name,
                            local_type,
                            ref_meta.columns[*ref_idx].name,
                            ref_type
                        ),
                    ));
                }
            }

            metas.push(ForeignKeyMeta {
                columns: local_indexes,
                referenced_schema,
                referenced_table,
                referenced_columns: referenced_indexes,
            });
        }
        Ok(metas)
    }

    pub fn alter_table_add_column(
        &mut self,
        schema: &str,
        name: &str,
        column: (String, DataType, bool, Option<Value>),
        if_not_exists: bool,
    ) -> anyhow::Result<()> {
        let (col_name, data_type, nullable, default_raw) = column;
        let mut default_value = None;
        if let Some(def) = default_raw {
            let cast = cast_value_to_type(def, &data_type)?;
            if matches!(cast, Value::Null) && !nullable {
                return Err(sql_err(
                    "23502",
                    format!("default NULL not allowed for NOT NULL column {col_name}"),
                ));
            }
            default_value = Some(cast);
        } else if !nullable {
            return Err(sql_err(
                "23502",
                format!("column {col_name} must have a default or allow NULLs"),
            ));
        }
        let (table_id, column_exists) = {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
            let exists = meta.columns.iter().any(|c| c.name == col_name);
            (meta.id, exists)
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
        let append_value = default_value.clone().unwrap_or(Value::Null);
        {
            let table = self.tables.get_mut(&table_id).ok_or_else(|| {
                sql_err("XX000", format!("missing storage for table id {table_id}"))
            })?;
            for versions in table.rows_by_key.values_mut() {
                for version in versions.iter_mut() {
                    version.data.push(append_value.clone());
                }
            }
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
        table_meta.columns.push(Column {
            name: col_name,
            data_type,
            nullable,
            default: default_value,
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
                .pk
                .as_ref()
                .map(|pk| pk.contains(&idx))
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
                .any(|fk| fk.columns.contains(&drop_idx))
            {
                return Err(sql_err(
                    "2BP01",
                    format!("cannot drop column {column} referenced by a foreign key"),
                ));
            }
        }
        let inbound = self.collect_inbound_foreign_keys(schema, name);
        if inbound
            .iter()
            .any(|fk| fk.referenced_columns.contains(&drop_idx))
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
        } else {
            return Err(sql_err(
                "XX000",
                format!("missing storage for table id {table_id}"),
            ));
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
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
    ) -> anyhow::Result<()> {
        if columns.is_empty() {
            return Err(sql_err("0A000", "index must reference at least one column"));
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(table)
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
        });
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        schema: &str,
        index_name: &str,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let Some(schema_entry) = self.catalog.schemas.get_mut(schema) else {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("3F000", format!("no such schema {schema}")))
            };
        };
        let mut removed = false;
        for table_meta in schema_entry.tables.values_mut() {
            if let Some(pos) = table_meta
                .indexes
                .iter()
                .position(|idx| idx.name == index_name)
            {
                table_meta.indexes.remove(pos);
                removed = true;
                break;
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

    pub fn resolve_table_mut(
        &mut self,
        schema: &str,
        name: &str,
    ) -> anyhow::Result<(&TableMeta, &mut Table)> {
        let tm = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let t = self
            .tables
            .get_mut(&tm.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", tm.id)))?;
        Ok((self.catalog.get_table(schema, name).unwrap(), t))
    }

    pub fn insert_full_rows(
        &mut self,
        schema: &str,
        name: &str,
        mut rows: Vec<Vec<CellInput>>,
        txid: TxId,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
        let table_id = meta.id;
        let ncols = meta.columns.len();
        let mut realized: Vec<(usize, Row)> = Vec::with_capacity(rows.len());

        for (ridx, r) in rows.drain(..).enumerate() {
            // arity: number of values must match table columns
            if r.len() != ncols {
                return Err(sql_err(
                    "21P01",
                    format!(
                        "insert has wrong number of values at row {}: expected {}, got {}",
                        ridx + 1,
                        ncols,
                        r.len()
                    ),
                ));
            }

            // coerce and validate each cell to the target column type
            let mut out: Row = Vec::with_capacity(ncols);
            for (i, cell) in r.into_iter().enumerate() {
                let col = &meta.columns[i];
                let value = match cell {
                    CellInput::Value(v) => v,
                    CellInput::Default => col.default.clone().unwrap_or(Value::Null),
                };
                let coerced = coerce_value_for_column(value, col, i, meta)?;
                out.push(coerced);
            }
            self.ensure_outbound_foreign_keys(schema, name, meta, &out)?;
            realized.push((ridx, out));
        }

        let tab = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {table_id}")))?;
        let mut count = 0usize;
        let mut inserted_rows = Vec::with_capacity(realized.len());
        let mut inserted_ptrs = Vec::with_capacity(realized.len());

        for (_ridx, out) in realized.into_iter() {
            let row = out;
            // build the row key (pk or hidden rowid)
            let key = if let Some(pkpos) = &meta.pk {
                let mut vals = Vec::with_capacity(pkpos.len());
                for i in pkpos {
                    vals.push(row[*i].clone());
                }
                RowKey::Pk(vals)
            } else {
                // use per-table allocator for hidden rowid
                let id = tab.alloc_rowid();
                RowKey::Hidden(id)
            };

            // enforce pk uniqueness when present
            if tab.rows_by_key.contains_key(&key) {
                return Err(sql_err(
                    "23505",
                    "duplicate key value violates unique constraint",
                ));
            }

            // finally insert
            inserted_rows.push(row.clone());
            let version = VersionedRow {
                xmin: txid,
                xmax: None,
                data: row,
            };
            tab.rows_by_key.insert(key.clone(), vec![version]);
            inserted_ptrs.push(RowPointer { table_id, key });
            count += 1;
        }

        Ok((count, inserted_rows, inserted_ptrs))
    }

    pub fn scan_bound_positions(
        &self,
        schema: &str,
        name: &str,
        positions: &[usize],
        visibility: &VisibilityContext,
    ) -> anyhow::Result<(Vec<Row>, Vec<(usize, String)>)> {
        let tm = self.resolve_table(schema, name)?;
        let table = self
            .tables
            .get(&tm.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", tm.id)))?;

        let mut out_rows = Vec::new();
        for (_k, versions) in table.scan_all() {
            if let Some(version) = select_visible_version(versions, visibility) {
                out_rows.push(positions.iter().map(|i| version.data[*i].clone()).collect());
            }
        }
        let cols = positions
            .iter()
            .map(|i| (*i, tm.columns[*i].name.clone()))
            .collect();
        Ok((out_rows, cols))
    }

    pub fn update_rows(
        &mut self,
        schema: &str,
        name: &str,
        sets: &[(usize, ScalarExpr)],
        filter: Option<&BoolExpr>,
        params: &[Value],
        visibility: &VisibilityContext,
        txid: TxId,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>, Vec<RowPointer>)> {
        let (meta, table) = self.resolve_table_mut(schema, name)?;
        if sets.is_empty() {
            return Ok((0, Vec::new(), Vec::new(), Vec::new()));
        }
        if let Some(pkpos) = &meta.pk {
            for (idx, _) in sets {
                if pkpos.iter().any(|pos| pos == idx) {
                    return Err(sql_err(
                        "0A000",
                        "updating primary key columns is not supported",
                    ));
                }
            }
        }
        let mut count = 0usize;
        let mut updated_rows = Vec::new();
        let mut inserted_ptrs = Vec::new();
        let mut touched_ptrs = Vec::new();
        for (key, versions) in table.rows_by_key.iter_mut() {
            let Some(idx) = select_visible_version_idx(versions, visibility) else {
                continue;
            };
            let snapshot = versions[idx].data.clone();
            let passes = if let Some(expr) = filter {
                eval_bool_expr(&snapshot, expr, params)
                    .map_err(|e| sql_err("XX000", e.to_string()))?
                    .unwrap_or(false)
            } else {
                true
            };
            if !passes {
                continue;
            }
            let mut updated = snapshot.clone();
            for (idx, expr) in sets {
                let value = eval_scalar_expr(&snapshot, expr, params)
                    .map_err(|e| sql_err("XX000", e.to_string()))?;
                let coerced = coerce_value_for_column(value, &meta.columns[*idx], *idx, meta)?;
                updated[*idx] = coerced;
            }
            if versions[idx].xmax.is_some() {
                continue;
            }
            versions[idx].xmax = Some(txid);
            let new_version = VersionedRow {
                xmin: txid,
                xmax: None,
                data: updated.clone(),
            };
            versions.push(new_version);
            updated_rows.push(updated);
            touched_ptrs.push(RowPointer {
                table_id: meta.id,
                key: key.clone(),
            });
            inserted_ptrs.push(RowPointer {
                table_id: meta.id,
                key: key.clone(),
            });
            count += 1;
        }
        Ok((count, updated_rows, inserted_ptrs, touched_ptrs))
    }

    pub fn drop_table(&mut self, schema: &str, name: &str, if_exists: bool) -> anyhow::Result<()> {
        let table_exists = self.catalog.get_table(schema, name).is_some();
        if !table_exists {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("42P01", format!("no such table {schema}.{name}")))
            };
        }

        let inbound = self.collect_inbound_foreign_keys(schema, name);
        if let Some(fk) = inbound.first() {
            return Err(sql_err(
                "2BP01",
                format!(
                    "cannot drop table {}.{} because it is referenced by {}.{}",
                    schema, name, fk.schema, fk.table
                ),
            ));
        }

        let schema_entry = match self.catalog.schemas.get_mut(schema) {
            Some(entry) => entry,
            None => {
                return Err(sql_err("3F000", format!("no such schema {schema}")));
            }
        };
        match schema_entry.tables.remove(name) {
            Some(meta) => {
                if self.tables.remove(&meta.id).is_none() {
                    return Err(sql_err(
                        "XX000",
                        format!("missing storage for table id {}", meta.id),
                    ));
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn delete_rows(
        &mut self,
        schema: &str,
        name: &str,
        filter: Option<&BoolExpr>,
        params: &[Value],
        visibility: &VisibilityContext,
        txid: TxId,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>)> {
        let meta = self.resolve_table(schema, name)?;
        let table_id = meta.id;
        let mut to_remove: Vec<(RowKey, Row)> = Vec::new();
        {
            let table = self.tables.get(&table_id).ok_or_else(|| {
                sql_err(
                    "XX000",
                    format!("missing storage for table id {}", table_id),
                )
            })?;
            if let Some(expr) = filter {
                for (key, versions) in table.rows_by_key.iter() {
                    let Some(idx) = select_visible_version_idx(versions, visibility) else {
                        continue;
                    };
                    let row = &versions[idx].data;
                    let passes = eval_bool_expr(row, expr, params)
                        .map_err(|e| sql_err("XX000", e.to_string()))?
                        .unwrap_or(false);
                    if passes {
                        to_remove.push((key.clone(), row.clone()));
                    }
                }
            } else {
                for (key, versions) in table.rows_by_key.iter() {
                    if let Some(version) = select_visible_version(versions, visibility) {
                        to_remove.push((key.clone(), version.data.clone()));
                    }
                }
            }
        }
        let inbound = self.collect_inbound_foreign_keys(schema, name);
        if !inbound.is_empty() {
            for (_, row) in &to_remove {
                self.ensure_no_inbound_refs(schema, name, &inbound, row)?;
            }
        }
        let count = to_remove.len();
        let table = self.tables.get_mut(&table_id).ok_or_else(|| {
            sql_err(
                "XX000",
                format!("missing storage for table id {}", table_id),
            )
        })?;
        let mut removed_rows = Vec::with_capacity(count);
        let mut touched_ptrs = Vec::with_capacity(count);
        for (key, row) in to_remove {
            if let Some(versions) = table.rows_by_key.get_mut(&key) {
                if let Some(idx) = select_visible_version_idx(versions, visibility) {
                    versions[idx].xmax = Some(txid);
                    touched_ptrs.push(RowPointer {
                        table_id,
                        key: key.clone(),
                    });
                }
            }
            removed_rows.push(row);
        }
        Ok((count, removed_rows, touched_ptrs))
    }

    fn ensure_outbound_foreign_keys(
        &self,
        table_schema: &str,
        table_name: &str,
        meta: &TableMeta,
        row: &[Value],
    ) -> anyhow::Result<()> {
        for fk in &meta.foreign_keys {
            let mut values = Vec::with_capacity(fk.columns.len());
            let mut has_null = false;
            for idx in &fk.columns {
                let val = row.get(*idx).cloned().unwrap_or(Value::Null);
                if matches!(val, Value::Null) {
                    has_null = true;
                    break;
                }
                values.push(val);
            }
            if has_null {
                continue;
            }
            if !self.referenced_row_exists(
                &fk.referenced_schema,
                &fk.referenced_table,
                &fk.referenced_columns,
                &values,
            )? {
                return Err(sql_err(
                    "23503",
                    format!(
                        "insert on {}.{} violates foreign key referencing {}.{}",
                        table_schema, table_name, fk.referenced_schema, fk.referenced_table
                    ),
                ));
            }
        }
        Ok(())
    }

    fn referenced_row_exists(
        &self,
        schema: &str,
        table: &str,
        columns: &[usize],
        values: &[Value],
    ) -> anyhow::Result<bool> {
        let meta = self.catalog.get_table(schema, table).ok_or_else(|| {
            sql_err(
                "42830",
                format!("referenced table {}.{} missing", schema, table),
            )
        })?;
        let storage = self
            .tables
            .get(&meta.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", meta.id)))?;
        'outer: for versions in storage.rows_by_key.values() {
            let Some(row) = versions.last() else {
                continue;
            };
            for (col, val) in columns.iter().zip(values.iter()) {
                if row.data[*col] != *val {
                    continue 'outer;
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn collect_inbound_foreign_keys(&self, schema: &str, table: &str) -> Vec<InboundForeignKey> {
        let mut out = Vec::new();
        for (schema_name, schema_entry) in &self.catalog.schemas {
            for (table_name, meta) in &schema_entry.tables {
                for fk in &meta.foreign_keys {
                    if fk.referenced_schema == schema && fk.referenced_table == table {
                        out.push(InboundForeignKey {
                            schema: schema_name.clone(),
                            table: table_name.clone(),
                            table_id: meta.id,
                            columns: fk.columns.clone(),
                            referenced_columns: fk.referenced_columns.clone(),
                        });
                    }
                }
            }
        }
        out
    }

    fn ensure_no_inbound_refs(
        &self,
        schema: &str,
        table: &str,
        inbound: &[InboundForeignKey],
        parent_row: &[Value],
    ) -> anyhow::Result<()> {
        for fk in inbound {
            let child = match self.tables.get(&fk.table_id) {
                Some(t) => t,
                None => {
                    return Err(sql_err(
                        "XX000",
                        format!("missing storage for table id {}", fk.table_id),
                    ));
                }
            };
            for versions in child.rows_by_key.values() {
                let Some(row) = versions.last() else {
                    continue;
                };
                let mut matches = true;
                for (child_idx, parent_idx) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
                    if row.data[*child_idx] != parent_row[*parent_idx] {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    return Err(sql_err(
                        "23503",
                        format!(
                            "operation on {}.{} violates foreign key from {}.{}",
                            schema, table, fk.schema, fk.table
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn rollback_transaction_changes(
        &mut self,
        changes: &TxnChanges,
        txid: TxId,
    ) -> anyhow::Result<()> {
        self.rollback_inserts(&changes.inserted, txid)?;
        self.rollback_xmax(&changes.updated_old, txid)?;
        Ok(())
    }

    fn rollback_inserts(&mut self, ptrs: &[RowPointer], txid: TxId) -> anyhow::Result<()> {
        for ptr in ptrs {
            if let Some(table) = self.tables.get_mut(&ptr.table_id) {
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    versions.retain(|v| v.xmin != txid);
                    if versions.is_empty() {
                        table.rows_by_key.remove(&ptr.key);
                    }
                }
            }
        }
        Ok(())
    }

    fn rollback_xmax(&mut self, ptrs: &[RowPointer], txid: TxId) -> anyhow::Result<()> {
        for ptr in ptrs {
            if let Some(table) = self.tables.get_mut(&ptr.table_id) {
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    for version in versions.iter_mut() {
                        if version.xmax == Some(txid) {
                            version.xmax = None;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct InboundForeignKey {
    schema: String,
    table: String,
    table_id: TableId,
    columns: Vec<usize>,
    referenced_columns: Vec<usize>,
}

fn select_visible_version_idx(
    versions: &[VersionedRow],
    visibility: &VisibilityContext,
) -> Option<usize> {
    versions
        .iter()
        .enumerate()
        .rev()
        .find(|(_, version)| visibility.is_visible(version))
        .map(|(idx, _)| idx)
}

fn select_visible_version<'a>(
    versions: &'a [VersionedRow],
    visibility: &VisibilityContext,
) -> Option<&'a VersionedRow> {
    select_visible_version_idx(versions, visibility).map(|idx| &versions[idx])
}

fn coerce_value_for_column(
    val: Value,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
) -> anyhow::Result<Value> {
    if let Value::Null = val {
        if !col.nullable {
            return Err(sql_err("23502", format!("column {} is not null", col.name)));
        }
        if let Some(pkpos) = &meta.pk {
            if pkpos.contains(&idx) {
                return Err(sql_err(
                    "23502",
                    format!("primary key column {} cannot be null", col.name),
                ));
            }
        }
        return Ok(Value::Null);
    }
    let coerced = cast_value_to_type(val, &col.data_type).map_err(|e| {
        sql_err(
            e.code,
            format!("column {} (index {}): {}", col.name, idx, e.message),
        )
    })?;
    Ok(coerced)
}
