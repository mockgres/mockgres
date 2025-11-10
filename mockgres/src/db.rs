use crate::catalog::{Catalog, ColId, ForeignKeyMeta, PrimaryKeyMeta, TableId, TableMeta};
use crate::engine::{
    BoolExpr, Column, DataType, ForeignKeySpec, PrimaryKeySpec, ReferentialAction, ScalarExpr,
    SqlError, Value, cast_value_to_type, eval_bool_expr, eval_scalar_expr,
};
use crate::session::{RowPointer, SessionId, TxnChanges};
use crate::storage::{Row, RowId, RowKey, Table, VersionedRow};
use crate::txn::{TxId, VisibilityContext};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};

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
    locks: LockRegistry,
}

impl Default for Db {
    fn default() -> Self {
        let mut catalog = Catalog::default();
        catalog.ensure_schema("public");
        Self {
            catalog,
            tables: HashMap::new(),
            next_tid: 1,
            locks: LockRegistry::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LockOwner {
    pub session_id: SessionId,
    pub epoch: u64,
}

impl LockOwner {
    pub fn new(session_id: SessionId, epoch: u64) -> Self {
        Self { session_id, epoch }
    }
}

#[derive(Debug)]
struct LockRegistry {
    inner: Mutex<LockState>,
}

#[derive(Debug)]
struct LockState {
    holders: HashMap<(TableId, RowId), LockOwner>,
    owned: HashMap<LockOwner, HashSet<(TableId, RowId)>>,
}

impl LockRegistry {
    fn new() -> Self {
        Self {
            inner: Mutex::new(LockState {
                holders: HashMap::new(),
                owned: HashMap::new(),
            }),
        }
    }

    fn acquire(
        &self,
        key: (TableId, RowId),
        owner: LockOwner,
        skip_locked: bool,
    ) -> anyhow::Result<bool> {
        let mut state = self.inner.lock();
        if let Some(existing) = state.holders.get(&key) {
            if *existing == owner {
                return Ok(true);
            }
            if skip_locked {
                return Ok(false);
            }
            return Err(sql_err(
                "55P03",
                "could not obtain lock on target row (locked by another transaction)",
            ));
        }
        state.holders.insert(key, owner);
        state.owned.entry(owner).or_default().insert(key);
        Ok(true)
    }

    fn release_owner(&self, owner: LockOwner) {
        let mut state = self.inner.lock();
        if let Some(keys) = state.owned.remove(&owner) {
            for key in keys {
                if let Some(existing) = state.holders.get(&key) {
                    if *existing == owner {
                        state.holders.remove(&key);
                    }
                }
            }
        }
    }
}
impl Db {
    pub fn release_locks(&self, owner: LockOwner) {
        self.locks.release_owner(owner);
    }

    pub fn lock_row(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
        skip_locked: bool,
    ) -> anyhow::Result<bool> {
        self.locks.acquire((table_id, row_id), owner, skip_locked)
    }

    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<(String, DataType, bool, Option<ScalarExpr>)>,
        pk_spec: Option<PrimaryKeySpec>,
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
        for (n, t, nullable, default_expr) in cols.into_iter() {
            if matches!(
                default_expr.as_ref(),
                Some(ScalarExpr::Literal(Value::Null))
            ) && !nullable
            {
                return Err(sql_err(
                    "23502",
                    format!("default NULL not allowed for NOT NULL column {n}"),
                ));
            }
            columns.push(Column {
                name: n,
                data_type: t,
                nullable,
                default: default_expr,
            });
        }

        let pk_meta = self.build_primary_key_meta(name, &mut columns, pk_spec)?;

        let fk_metas =
            self.build_foreign_keys(schema, name, id, &columns, pk_meta.as_ref(), foreign_keys)?;

        let tm = TableMeta {
            id,
            name: name.to_string(),
            columns: columns.clone(),
            primary_key: pk_meta,
            indexes: vec![],
            foreign_keys: fk_metas,
        };

        let has_pk = tm.primary_key.is_some();

        self.catalog
            .schemas
            .get_mut(schema)
            .expect("schema exists after ensure")
            .tables
            .insert(name.to_string(), tm);

        self.tables.insert(id, Table::with_pk(has_pk));
        Ok(id)
    }

    fn table_meta_by_id(&self, id: TableId) -> Option<&TableMeta> {
        for schema in self.catalog.schemas.values() {
            for meta in schema.tables.values() {
                if meta.id == id {
                    return Some(meta);
                }
            }
        }
        None
    }

    fn build_primary_key_meta(
        &self,
        table_name: &str,
        columns: &mut [Column],
        pk_spec: Option<PrimaryKeySpec>,
    ) -> anyhow::Result<Option<PrimaryKeyMeta>> {
        let Some(spec) = pk_spec else {
            return Ok(None);
        };
        let PrimaryKeySpec {
            name,
            columns: pk_columns,
        } = spec;
        if pk_columns.is_empty() {
            return Err(sql_err(
                "42P16",
                format!("primary key on {table_name} must reference at least one column"),
            ));
        }
        let mut positions: Vec<ColId> = Vec::with_capacity(pk_columns.len());
        for col_name in pk_columns {
            let idx = columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| {
                    sql_err(
                        "42703",
                        format!("primary key column {col_name} does not exist"),
                    )
                })?;
            if positions.contains(&idx) {
                return Err(sql_err(
                    "42P16",
                    format!("column {col_name} referenced multiple times in primary key"),
                ));
            }
            positions.push(idx);
        }
        for idx in &positions {
            columns[*idx].nullable = false;
        }
        let name = name.unwrap_or_else(|| format!("{table_name}_pkey"));
        Ok(Some(PrimaryKeyMeta {
            name,
            columns: positions,
        }))
    }

    fn build_foreign_keys(
        &self,
        table_schema: &str,
        table_name: &str,
        table_id: TableId,
        columns: &[Column],
        self_pk: Option<&PrimaryKeyMeta>,
        specs: Vec<ForeignKeySpec>,
    ) -> anyhow::Result<Vec<ForeignKeyMeta>> {
        let mut metas = Vec::with_capacity(specs.len());
        for (spec_idx, spec) in specs.into_iter().enumerate() {
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
            let referenced_table_name = spec.referenced_table.name.clone();
            let self_reference =
                referenced_schema == table_schema && referenced_table_name == table_name;
            let mut build_meta = |parent_pk: &PrimaryKeyMeta,
                                  parent_columns: &[Column],
                                  referenced_table_id: TableId|
             -> anyhow::Result<()> {
                if parent_pk.columns.len() != local_indexes.len() {
                    return Err(sql_err(
                        "42830",
                        format!(
                            "foreign key on {}.{} has {} local columns but parent primary key has {}",
                            table_schema,
                            table_name,
                            local_indexes.len(),
                            parent_pk.columns.len()
                        ),
                    ));
                }
                let referenced_indexes = if let Some(ref_cols) = spec.referenced_columns.as_ref() {
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
                        let idx = parent_columns
                            .iter()
                            .position(|c| c.name == rc.as_str())
                            .ok_or_else(|| {
                                sql_err(
                                    "42703",
                                    format!(
                                        "referenced column {} does not exist on {}.{}",
                                        rc, referenced_schema, referenced_table_name
                                    ),
                                )
                            })?;
                        idxs.push(idx);
                    }
                    idxs
                } else {
                    parent_pk.columns.clone()
                };

                let ordered_local = self.align_local_to_parent_pk(
                    &local_indexes,
                    &referenced_indexes,
                    parent_pk,
                    table_schema,
                    table_name,
                    &referenced_schema,
                    &referenced_table_name,
                )?;

                for (local_idx, pk_idx) in ordered_local.iter().zip(parent_pk.columns.iter()) {
                    let local_type = &columns[*local_idx].data_type;
                    let ref_type = &parent_columns[*pk_idx].data_type;
                    if local_type != ref_type {
                        return Err(sql_err(
                            "42804",
                            format!(
                                "foreign key column {} type {:?} does not match referenced column {} type {:?}",
                                columns[*local_idx].name,
                                local_type,
                                parent_columns[*pk_idx].name,
                                ref_type
                            ),
                        ));
                    }
                }

                let fk_name = spec
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_fk{}", table_name, spec_idx + 1));

                metas.push(ForeignKeyMeta {
                    name: fk_name,
                    local_columns: ordered_local,
                    referenced_table: referenced_table_id,
                    referenced_schema: referenced_schema.clone(),
                    referenced_table_name: referenced_table_name.clone(),
                    referenced_columns: parent_pk.columns.clone(),
                    on_delete: spec.on_delete,
                });
                Ok(())
            };

            if self_reference {
                let parent_pk = self_pk.ok_or_else(|| {
                    sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} must have a primary key",
                            referenced_schema, referenced_table_name
                        ),
                    )
                })?;
                build_meta(parent_pk, columns, table_id)?;
            } else {
                let ref_meta = self
                    .catalog
                    .get_table(&referenced_schema, &referenced_table_name)
                    .ok_or_else(|| {
                        sql_err(
                            "42830",
                            format!(
                                "referenced table {}.{} for foreign key on {}.{} does not exist",
                                referenced_schema, referenced_table_name, table_schema, table_name
                            ),
                        )
                    })?;
                let parent_pk = ref_meta.primary_key.as_ref().ok_or_else(|| {
                    sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} must have a primary key",
                            referenced_schema, referenced_table_name
                        ),
                    )
                })?;
                build_meta(parent_pk, &ref_meta.columns, ref_meta.id)?;
            }
        }
        Ok(metas)
    }

    fn align_local_to_parent_pk(
        &self,
        local_indexes: &[ColId],
        referenced_indexes: &[ColId],
        parent_pk: &PrimaryKeyMeta,
        table_schema: &str,
        table_name: &str,
        referenced_schema: &str,
        referenced_table_name: &str,
    ) -> anyhow::Result<Vec<ColId>> {
        let mut ordered = Vec::with_capacity(parent_pk.columns.len());
        for pk_idx in &parent_pk.columns {
            let Some(pos) = referenced_indexes.iter().position(|idx| idx == pk_idx) else {
                return Err(sql_err(
                    "42830",
                    format!(
                        "foreign key on {}.{} must reference primary key columns of {}.{}",
                        table_schema, table_name, referenced_schema, referenced_table_name
                    ),
                ));
            };
            ordered.push(local_indexes[pos]);
        }
        Ok(ordered)
    }

    pub fn alter_table_add_column(
        &mut self,
        schema: &str,
        name: &str,
        column: (String, DataType, bool, Option<ScalarExpr>),
        if_not_exists: bool,
    ) -> anyhow::Result<()> {
        let (col_name, data_type, nullable, default_expr) = column;
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
        };
        let append_value = if let Some(expr) = &default_expr {
            eval_column_default(expr, &temp_column, new_col_index, &meta_snapshot)?
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
            default: default_expr,
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
        let inbound = self.collect_inbound_foreign_keys(schema, name);
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
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let table_id = meta.id;
        let ncols = meta.columns.len();
        let mut staged: Vec<(Row, Option<RowKey>, Vec<Option<Vec<Value>>>)> =
            Vec::with_capacity(rows.len());

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
                    CellInput::Default => {
                        if let Some(expr) = &col.default {
                            eval_column_default(expr, col, i, &meta)?
                        } else {
                            Value::Null
                        }
                    }
                };
                let coerced = coerce_value_for_column(value, col, i, &meta)?;
                out.push(coerced);
            }
            let pk_key = self.build_primary_key_row_key(&meta, &out)?;
            let fk_keys = self.ensure_outbound_foreign_keys(schema, name, &meta, &out)?;
            staged.push((out, pk_key, fk_keys));
        }

        let mut table = self
            .tables
            .remove(&table_id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {table_id}")))?;
        let result = (|| {
            let mut count = 0usize;
            let mut inserted_rows = Vec::with_capacity(staged.len());
            let mut inserted_ptrs = Vec::with_capacity(staged.len());

            for (row, pk_key, fk_keys) in staged.into_iter() {
                let row_id = table.alloc_rowid();
                if let Some(pk) = pk_key.clone() {
                    let constraint = meta
                        .primary_key
                        .as_ref()
                        .expect("pk metadata exists when pk_key is present");
                    let pk_map = table
                        .pk_map
                        .as_mut()
                        .expect("pk_map exists when table has primary key");
                    if pk_map.contains_key(&pk) {
                        return Err(sql_err(
                            "23505",
                            format!(
                                "duplicate key value violates unique constraint {}",
                                constraint.name
                            ),
                        ));
                    }
                    pk_map.insert(pk, row_id);
                }

                inserted_rows.push(row.clone());
                let version = VersionedRow {
                    xmin: txid,
                    xmax: None,
                    data: row,
                };
                let storage_key = RowKey::RowId(row_id);
                table.rows_by_key.insert(storage_key.clone(), vec![version]);
                self.add_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                inserted_ptrs.push(RowPointer {
                    table_id,
                    key: storage_key,
                });
                count += 1;
            }

            Ok((count, inserted_rows, inserted_ptrs))
        })();
        self.tables.insert(table_id, table);
        result
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

    pub fn update_rows(
        &mut self,
        schema: &str,
        name: &str,
        sets: &[(usize, ScalarExpr)],
        filter: Option<&BoolExpr>,
        params: &[Value],
        visibility: &VisibilityContext,
        txid: TxId,
        lock_owner: LockOwner,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let mut table = self
            .tables
            .remove(&meta.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", meta.id)))?;
        let result = (|| {
            if sets.is_empty() {
                return Ok((0, Vec::new(), Vec::new(), Vec::new()));
            }
            let pk_meta = meta.primary_key.as_ref();
            let pk_columns: HashSet<usize> = pk_meta
                .map(|pk| pk.columns.iter().copied().collect())
                .unwrap_or_default();
            let inbound = self.collect_inbound_foreign_keys(schema, name);
            let updates_pk =
                pk_meta.is_some() && sets.iter().any(|(idx, _)| pk_columns.contains(idx));
            if updates_pk && !inbound.is_empty() {
                return Err(sql_err(
                    "0A000",
                    "updating referenced primary key columns is not supported",
                ));
            }
            let mut fk_updates = vec![false; meta.foreign_keys.len()];
            for (idx, _) in sets {
                for (fk_idx, fk) in meta.foreign_keys.iter().enumerate() {
                    if fk.local_columns.contains(idx) {
                        fk_updates[fk_idx] = true;
                    }
                }
            }
            let fk_changed = fk_updates.iter().any(|f| *f);
            #[derive(Clone)]
            struct PendingUpdate {
                key: RowKey,
                updated: Row,
                old_pk: Option<RowKey>,
                new_pk: Option<RowKey>,
                old_fk: Vec<Option<Vec<Value>>>,
                new_fk: Vec<Option<Vec<Value>>>,
            }
            let mut pending: Vec<PendingUpdate> = Vec::new();
            for (key, versions) in table.rows_by_key.iter() {
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
                    let coerced = coerce_value_for_column(value, &meta.columns[*idx], *idx, &meta)?;
                    updated[*idx] = coerced;
                }
                let old_pk_key = self.build_primary_key_row_key(&meta, &snapshot)?;
                let new_pk_key = self.build_primary_key_row_key(&meta, &updated)?;
                let old_fk_keys: Vec<Option<Vec<Value>>> = meta
                    .foreign_keys
                    .iter()
                    .map(|fk| self.build_fk_parent_key(&snapshot, fk))
                    .collect();
                let new_fk_keys =
                    self.ensure_outbound_foreign_keys(schema, name, &meta, &updated)?;
                pending.push(PendingUpdate {
                    key: key.clone(),
                    updated,
                    old_pk: old_pk_key,
                    new_pk: new_pk_key,
                    old_fk: old_fk_keys,
                    new_fk: new_fk_keys,
                });
            }
            let mut count = 0usize;
            let mut updated_rows = Vec::with_capacity(pending.len());
            let mut inserted_ptrs = Vec::with_capacity(pending.len());
            let mut touched_ptrs = Vec::with_capacity(pending.len());
            for pending_update in pending {
                if let Some(versions) = table.rows_by_key.get_mut(&pending_update.key) {
                    let Some(idx) = select_visible_version_idx(versions, visibility) else {
                        continue;
                    };
                    let row_id = row_key_to_row_id(&pending_update.key)?;
                    self.lock_row(meta.id, row_id, lock_owner, false)?;
                    if versions[idx].xmax.is_some() {
                        continue;
                    }
                    versions[idx].xmax = Some(txid);
                    versions.push(VersionedRow {
                        xmin: txid,
                        xmax: None,
                        data: pending_update.updated.clone(),
                    });
                    updated_rows.push(pending_update.updated.clone());
                    if let Some(pk_meta) = pk_meta {
                        self.update_pk_map_for_row(
                            &mut table,
                            pk_meta,
                            pending_update.old_pk.as_ref(),
                            pending_update.new_pk.as_ref(),
                            row_id,
                        )?;
                    }
                    if fk_changed {
                        self.remove_fk_rev_entries(
                            &mut table,
                            &meta,
                            row_id,
                            &pending_update.old_fk,
                        );
                        self.add_fk_rev_entries(&mut table, &meta, row_id, &pending_update.new_fk);
                    }
                    touched_ptrs.push(RowPointer {
                        table_id: meta.id,
                        key: pending_update.key.clone(),
                    });
                    inserted_ptrs.push(RowPointer {
                        table_id: meta.id,
                        key: pending_update.key,
                    });
                    count += 1;
                }
            }
            Ok((count, updated_rows, inserted_ptrs, touched_ptrs))
        })();
        self.tables.insert(meta.id, table);
        result
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
        lock_owner: LockOwner,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let table_id = meta.id;
        let schema_name = schema.to_string();
        let table_name = name.to_string();
        let (seeds, removed_rows) = {
            let table = self.tables.get(&table_id).ok_or_else(|| {
                sql_err(
                    "XX000",
                    format!("missing storage for table id {}", table_id),
                )
            })?;
            let mut seeds = Vec::new();
            let mut removed_rows = Vec::new();
            for (key, versions) in table.rows_by_key.iter() {
                let Some(idx) = select_visible_version_idx(versions, visibility) else {
                    continue;
                };
                let row = versions[idx].data.clone();
                let passes = match filter {
                    Some(expr) => eval_bool_expr(&row, expr, params)
                        .map_err(|e| sql_err("XX000", e.to_string()))?
                        .unwrap_or(false),
                    None => true,
                };
                if !passes {
                    continue;
                }
                let pk_key = self.build_primary_key_row_key(&meta, &row)?;
                let fk_keys = meta
                    .foreign_keys
                    .iter()
                    .map(|fk| self.build_fk_parent_key(&row, fk))
                    .collect();
                let row_key = key.clone();
                let row_id = row_key_to_row_id(&row_key)?;
                seeds.push(DeleteTarget {
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                    meta: meta.clone(),
                    row_key,
                    row_id,
                    pk_key,
                    fk_keys,
                });
                removed_rows.push(row);
            }
            (seeds, removed_rows)
        };
        if seeds.is_empty() {
            return Ok((0, removed_rows, Vec::new()));
        }
        let plan = self.build_delete_plan(seeds, visibility)?;
        let touched_ptrs = self.apply_delete_plan(plan, visibility, txid, lock_owner)?;
        Ok((removed_rows.len(), removed_rows, touched_ptrs))
    }

    fn build_delete_plan(
        &self,
        initial: Vec<DeleteTarget>,
        visibility: &VisibilityContext,
    ) -> anyhow::Result<Vec<DeleteTarget>> {
        let mut plan = Vec::new();
        let mut stack: Vec<CascadeFrame> = initial
            .into_iter()
            .map(|target| CascadeFrame {
                target,
                expanded: false,
            })
            .collect();
        let mut visited: HashSet<(TableId, RowId)> = HashSet::new();
        while let Some(mut frame) = stack.pop() {
            let key = (frame.target.meta.id, frame.target.row_id);
            if frame.expanded {
                plan.push(frame.target);
                continue;
            }
            if !visited.insert(key) {
                continue;
            }
            frame.expanded = true;
            let inbound =
                self.collect_inbound_foreign_keys(&frame.target.schema, &frame.target.table);
            if let Some(RowKey::Primary(pk_vals)) = frame.target.pk_key.as_ref() {
                self.ensure_no_inbound_refs(
                    &frame.target.schema,
                    &frame.target.table,
                    frame.target.meta.id,
                    &inbound,
                    pk_vals,
                    visibility,
                )?;
                for fk in inbound {
                    if fk.fk.on_delete == ReferentialAction::Cascade {
                        let children = self.collect_cascade_children(&fk, pk_vals, visibility)?;
                        for child in children {
                            stack.push(CascadeFrame {
                                target: child,
                                expanded: false,
                            });
                        }
                    }
                }
            } else if !inbound.is_empty() {
                return Err(sql_err(
                    "42830",
                    format!(
                        "referenced table {}.{} must have a primary key",
                        frame.target.schema, frame.target.table
                    ),
                ));
            }
            stack.push(frame);
        }
        Ok(plan)
    }

    fn collect_cascade_children(
        &self,
        fk: &InboundForeignKey,
        parent_key: &[Value],
        visibility: &VisibilityContext,
    ) -> anyhow::Result<Vec<DeleteTarget>> {
        let child_table = self.tables.get(&fk.table_id).ok_or_else(|| {
            sql_err(
                "XX000",
                format!("missing storage for table id {}", fk.table_id),
            )
        })?;
        let child_meta = self
            .catalog
            .get_table(&fk.schema, &fk.table)
            .ok_or_else(|| sql_err("42P01", format!("no such table {}.{}", fk.schema, fk.table)))?
            .clone();
        let mut targets = Vec::new();
        let map_key = (fk.fk.referenced_table, parent_key.to_vec());
        if let Some(rows) = child_table.fk_rev.get(&map_key) {
            for row_id in rows {
                if let Some(row) = visible_row_clone(child_table, *row_id, visibility) {
                    let pk_key = self.build_primary_key_row_key(&child_meta, &row)?;
                    let fk_keys = child_meta
                        .foreign_keys
                        .iter()
                        .map(|fk_meta| self.build_fk_parent_key(&row, fk_meta))
                        .collect();
                    targets.push(DeleteTarget {
                        schema: fk.schema.clone(),
                        table: fk.table.clone(),
                        meta: child_meta.clone(),
                        row_key: RowKey::RowId(*row_id),
                        row_id: *row_id,
                        pk_key,
                        fk_keys,
                    });
                }
            }
        }
        Ok(targets)
    }

    fn apply_delete_plan(
        &mut self,
        plan: Vec<DeleteTarget>,
        visibility: &VisibilityContext,
        txid: TxId,
        lock_owner: LockOwner,
    ) -> anyhow::Result<Vec<RowPointer>> {
        let mut table_cache: HashMap<TableId, Table> = HashMap::new();
        let result = (|| {
            let mut touched_ptrs = Vec::with_capacity(plan.len());
            for target in plan {
                let table_entry = if let Some(tbl) = table_cache.get_mut(&target.meta.id) {
                    tbl
                } else {
                    let table = self.tables.remove(&target.meta.id).ok_or_else(|| {
                        sql_err(
                            "XX000",
                            format!("missing storage for table id {}", target.meta.id),
                        )
                    })?;
                    table_cache.insert(target.meta.id, table);
                    table_cache
                        .get_mut(&target.meta.id)
                        .expect("table cache entry exists")
                };
                if let Some(versions) = table_entry.rows_by_key.get_mut(&target.row_key) {
                    if let Some(idx) = select_visible_version_idx(versions, visibility) {
                        self.lock_row(target.meta.id, target.row_id, lock_owner, false)?;
                        if versions[idx].xmax.is_some() {
                            continue;
                        }
                        versions[idx].xmax = Some(txid);
                        touched_ptrs.push(RowPointer {
                            table_id: target.meta.id,
                            key: target.row_key.clone(),
                        });
                        if let Some(pk_key) = target.pk_key.as_ref() {
                            if let Some(pk_map) = table_entry.pk_map.as_mut() {
                                if let Some(existing) = pk_map.get(pk_key) {
                                    if *existing == target.row_id {
                                        pk_map.remove(pk_key);
                                    }
                                }
                            }
                        }
                        self.remove_fk_rev_entries(
                            table_entry,
                            &target.meta,
                            target.row_id,
                            &target.fk_keys,
                        );
                    }
                }
            }
            Ok(touched_ptrs)
        })();
        for (id, table) in table_cache {
            self.tables.insert(id, table);
        }
        result
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
            let key = self.build_fk_parent_key(row, fk);
            if let Some(ref vals) = key {
                self.ensure_parent_exists(fk, table_schema, table_name, vals)?;
            }
            keys.push(key);
        }
        Ok(keys)
    }

    fn build_primary_key_row_key(
        &self,
        meta: &TableMeta,
        row: &[Value],
    ) -> anyhow::Result<Option<RowKey>> {
        let Some(pk) = meta.primary_key.as_ref() else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(pk.columns.len());
        for idx in &pk.columns {
            let value = row.get(*idx).cloned().unwrap_or(Value::Null);
            if matches!(value, Value::Null) {
                return Err(sql_err(
                    "23502",
                    format!(
                        "primary key column {} cannot be null",
                        meta.columns[*idx].name
                    ),
                ));
            }
            values.push(value);
        }
        Ok(Some(RowKey::Primary(values)))
    }

    fn build_fk_parent_key(&self, row: &[Value], fk: &ForeignKeyMeta) -> Option<Vec<Value>> {
        let mut values = Vec::with_capacity(fk.local_columns.len());
        for idx in &fk.local_columns {
            let value = row.get(*idx).cloned().unwrap_or(Value::Null);
            if matches!(value, Value::Null) {
                return None;
            }
            values.push(value);
        }
        Some(values)
    }

    fn ensure_parent_exists(
        &self,
        fk: &ForeignKeyMeta,
        table_schema: &str,
        table_name: &str,
        parent_key: &[Value],
    ) -> anyhow::Result<()> {
        let parent_table = self.tables.get(&fk.referenced_table).ok_or_else(|| {
            sql_err(
                "XX000",
                format!(
                    "missing storage for referenced table id {}",
                    fk.referenced_table
                ),
            )
        })?;
        let Some(pk_map) = parent_table.pk_map.as_ref() else {
            return Err(sql_err(
                "42830",
                format!(
                    "referenced table {}.{} must have a primary key",
                    fk.referenced_schema, fk.referenced_table_name
                ),
            ));
        };
        let key = RowKey::Primary(parent_key.to_vec());
        if let Some(row_id) = pk_map.get(&key) {
            if let Some(versions) = parent_table.rows_by_key.get(&RowKey::RowId(*row_id)) {
                if versions.iter().rev().any(|version| version.xmax.is_none()) {
                    return Ok(());
                }
            }
        }
        Err(sql_err(
            "23503",
            format!(
                "insert on {}.{} violates foreign key {} referencing {}.{}",
                table_schema, table_name, fk.name, fk.referenced_schema, fk.referenced_table_name
            ),
        ))
    }

    fn add_fk_rev_entries(
        &self,
        table: &mut Table,
        meta: &TableMeta,
        row_id: RowId,
        keys: &[Option<Vec<Value>>],
    ) {
        for (fk, key) in meta.foreign_keys.iter().zip(keys.iter()) {
            if let Some(values) = key {
                let map_key = (fk.referenced_table, values.clone());
                table
                    .fk_rev
                    .entry(map_key)
                    .or_insert_with(HashSet::new)
                    .insert(row_id);
            }
        }
    }

    fn remove_fk_rev_entries(
        &self,
        table: &mut Table,
        meta: &TableMeta,
        row_id: RowId,
        keys: &[Option<Vec<Value>>],
    ) {
        for (fk, key) in meta.foreign_keys.iter().zip(keys.iter()) {
            if let Some(values) = key {
                let map_key = (fk.referenced_table, values.clone());
                if let Some(set) = table.fk_rev.get_mut(&map_key) {
                    set.remove(&row_id);
                    if set.is_empty() {
                        table.fk_rev.remove(&map_key);
                    }
                }
            }
        }
    }

    fn update_pk_map_for_row(
        &self,
        table: &mut Table,
        pk_meta: &PrimaryKeyMeta,
        old_key: Option<&RowKey>,
        new_key: Option<&RowKey>,
        row_id: RowId,
    ) -> anyhow::Result<()> {
        let Some(new_key) = new_key else {
            return Ok(());
        };
        let pk_map = table
            .pk_map
            .as_mut()
            .expect("pk_map missing for table with primary key");
        if let Some(existing) = pk_map.get(new_key) {
            if *existing != row_id {
                return Err(sql_err(
                    "23505",
                    format!(
                        "duplicate key value violates unique constraint {}",
                        pk_meta.name
                    ),
                ));
            }
        }
        if let Some(old) = old_key {
            if let Some(existing) = pk_map.get(old) {
                if *existing == row_id && old != new_key {
                    pk_map.remove(old);
                }
            }
        }
        pk_map.insert(new_key.clone(), row_id);
        Ok(())
    }

    fn collect_inbound_foreign_keys(&self, schema: &str, table: &str) -> Vec<InboundForeignKey> {
        let Some(parent_meta) = self.catalog.get_table(schema, table) else {
            return Vec::new();
        };
        let parent_id = parent_meta.id;
        let mut out = Vec::new();
        for (schema_name, schema_entry) in &self.catalog.schemas {
            for (table_name, meta) in &schema_entry.tables {
                for fk in &meta.foreign_keys {
                    if fk.referenced_table == parent_id {
                        out.push(InboundForeignKey {
                            schema: schema_name.clone(),
                            table: table_name.clone(),
                            table_id: meta.id,
                            fk: fk.clone(),
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
        parent_id: TableId,
        inbound: &[InboundForeignKey],
        parent_key: &[Value],
        visibility: &VisibilityContext,
    ) -> anyhow::Result<()> {
        let key_values = parent_key.to_vec();
        for fk in inbound {
            if fk.fk.on_delete != ReferentialAction::Restrict {
                continue;
            }
            let child = match self.tables.get(&fk.table_id) {
                Some(t) => t,
                None => {
                    return Err(sql_err(
                        "XX000",
                        format!("missing storage for table id {}", fk.table_id),
                    ));
                }
            };
            let map_key = (parent_id, key_values.clone());
            if let Some(rows) = child.fk_rev.get(&map_key) {
                if rows
                    .iter()
                    .any(|row_id| visible_row_clone(child, *row_id, visibility).is_some())
                {
                    return Err(sql_err(
                        "23503",
                        format!(
                            "operation on {}.{} violates foreign key {} on {}.{}",
                            schema, table, fk.fk.name, fk.schema, fk.table
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
            let Some(meta) = self.table_meta_by_id(ptr.table_id).cloned() else {
                continue;
            };
            if let Some(mut table) = self.tables.remove(&ptr.table_id) {
                let mut removed_rows = Vec::new();
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    versions.retain(|v| {
                        if v.xmin == txid {
                            removed_rows.push(v.data.clone());
                            false
                        } else {
                            true
                        }
                    });
                    if versions.is_empty() {
                        table.rows_by_key.remove(&ptr.key);
                    }
                }
                if !removed_rows.is_empty() {
                    let row_id = row_key_to_row_id(&ptr.key)?;
                    for row in removed_rows {
                        if let Some(pk_key) = self.build_primary_key_row_key(&meta, &row)? {
                            if let Some(pk_map) = table.pk_map.as_mut() {
                                if let Some(existing) = pk_map.get(&pk_key) {
                                    if *existing == row_id {
                                        pk_map.remove(&pk_key);
                                    }
                                }
                            }
                        }
                        let fk_keys = meta
                            .foreign_keys
                            .iter()
                            .map(|fk| self.build_fk_parent_key(&row, fk))
                            .collect::<Vec<_>>();
                        self.remove_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                    }
                }
                self.tables.insert(ptr.table_id, table);
            } else {
                return Err(sql_err(
                    "XX000",
                    format!("missing storage for table id {}", ptr.table_id),
                ));
            }
        }
        Ok(())
    }

    fn rollback_xmax(&mut self, ptrs: &[RowPointer], txid: TxId) -> anyhow::Result<()> {
        for ptr in ptrs {
            let Some(meta) = self.table_meta_by_id(ptr.table_id).cloned() else {
                continue;
            };
            if let Some(mut table) = self.tables.remove(&ptr.table_id) {
                let mut restored_rows = Vec::new();
                if let Some(versions) = table.rows_by_key.get_mut(&ptr.key) {
                    for version in versions.iter_mut() {
                        if version.xmax == Some(txid) {
                            version.xmax = None;
                            restored_rows.push(version.data.clone());
                        }
                    }
                }
                if !restored_rows.is_empty() {
                    let row_id = row_key_to_row_id(&ptr.key)?;
                    for row in restored_rows {
                        if let Some(pk_key) = self.build_primary_key_row_key(&meta, &row)? {
                            if let Some(pk_map) = table.pk_map.as_mut() {
                                pk_map.insert(pk_key, row_id);
                            }
                        }
                        let fk_keys = meta
                            .foreign_keys
                            .iter()
                            .map(|fk| self.build_fk_parent_key(&row, fk))
                            .collect::<Vec<_>>();
                        self.add_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                    }
                }
                self.tables.insert(ptr.table_id, table);
            } else {
                return Err(sql_err(
                    "XX000",
                    format!("missing storage for table id {}", ptr.table_id),
                ));
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
    fk: ForeignKeyMeta,
}

#[derive(Clone)]
struct DeleteTarget {
    schema: String,
    table: String,
    meta: TableMeta,
    row_key: RowKey,
    row_id: RowId,
    pk_key: Option<RowKey>,
    fk_keys: Vec<Option<Vec<Value>>>,
}

struct CascadeFrame {
    target: DeleteTarget,
    expanded: bool,
}

fn row_key_to_row_id(key: &RowKey) -> anyhow::Result<RowId> {
    if let RowKey::RowId(id) = key {
        Ok(*id)
    } else {
        Err(sql_err(
            "XX000",
            "expected physical RowId key but found primary key tuple",
        ))
    }
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

fn visible_row_clone(table: &Table, row_id: RowId, visibility: &VisibilityContext) -> Option<Row> {
    table
        .rows_by_key
        .get(&RowKey::RowId(row_id))
        .and_then(|versions| select_visible_version(versions, visibility))
        .map(|version| version.data.clone())
}

fn eval_column_default(
    expr: &ScalarExpr,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
) -> anyhow::Result<Value> {
    let value = eval_scalar_expr(&[], expr, &[]).map_err(|e| sql_err("XX000", e.to_string()))?;
    coerce_value_for_column(value, col, idx, meta)
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
        if let Some(pk) = &meta.primary_key {
            if pk.columns.contains(&idx) {
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
