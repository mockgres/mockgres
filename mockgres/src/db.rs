use crate::catalog::{
    Catalog, ColId, ForeignKeyMeta, PrimaryKeyMeta, SchemaId, SchemaName, TableId, TableMeta,
};
use crate::engine::{
    BoolExpr, Column, DataType, EvalContext, EvalMode, ForeignKeySpec, IdentitySpec,
    PrimaryKeySpec, ReferentialAction, ScalarExpr, SqlError, Value, cast_value_to_type,
    eval_bool_expr, eval_scalar_expr, eval_scalar_expr_with_mode,
};
use crate::session::{RowPointer, SessionId, TxnChanges};
use crate::storage::{IdentityRuntime, Row, RowId, RowKey, Table, VersionedRow};
use crate::txn::{TxId, VisibilityContext};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Notify;

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
    notify: Notify,
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
            notify: Notify::new(),
        }
    }

    fn acquire_skip_locked(&self, key: (TableId, RowId), owner: LockOwner) -> anyhow::Result<bool> {
        let mut state = self.inner.lock();
        if let Some(existing) = state.holders.get(&key) {
            if *existing == owner {
                return Ok(true);
            }
            return Ok(false);
        }
        state.holders.insert(key, owner);
        state.owned.entry(owner).or_default().insert(key);
        Ok(true)
    }

    fn acquire_nowait(&self, key: (TableId, RowId), owner: LockOwner) -> anyhow::Result<()> {
        let mut state = self.inner.lock();
        if let Some(existing) = state.holders.get(&key) {
            if *existing == owner {
                return Ok(());
            }
            return Err(sql_err(
                "55P03",
                "could not obtain lock on target row (NOWAIT)",
            ));
        }
        state.holders.insert(key, owner);
        state.owned.entry(owner).or_default().insert(key);
        Ok(())
    }

    async fn acquire_blocking(
        &self,
        key: (TableId, RowId),
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        loop {
            {
                let mut state = self.inner.lock();
                match state.holders.get(&key) {
                    Some(existing) if *existing == owner => return Ok(()),
                    Some(_) => {}
                    None => {
                        state.holders.insert(key, owner);
                        state.owned.entry(owner).or_default().insert(key);
                        return Ok(());
                    }
                }
            }
            self.notify.notified().await;
        }
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
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
pub struct LockHandle {
    inner: Arc<LockRegistry>,
}

impl LockHandle {
    fn new(inner: Arc<LockRegistry>) -> Self {
        Self { inner }
    }

    pub fn lock_row_skip_locked(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<bool> {
        self.inner.acquire_skip_locked((table_id, row_id), owner)
    }

    pub fn lock_row_nowait(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        self.inner.acquire_nowait((table_id, row_id), owner)
    }

    pub async fn lock_row_blocking(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        self.inner.acquire_blocking((table_id, row_id), owner).await
    }
}
impl Db {
    fn init_builtin_catalog(&mut self) {
        self.catalog.ensure_schema("public");
        self.catalog.ensure_schema("pg_catalog");
        self.create_table(
            "pg_catalog",
            "pg_namespace",
            vec![("nspname".to_string(), DataType::Text, false, None, None)],
            None,
            Vec::new(),
            &[],
        )
        .expect("create pg_catalog.pg_namespace");
    }

    fn alloc_table_id(&mut self, schema_id: SchemaId) -> TableId {
        let rel_id = self.next_rel_id;
        self.next_rel_id = self.next_rel_id.checked_add(1).expect("table id overflow");
        TableId { schema_id, rel_id }
    }

    pub fn release_locks(&self, owner: LockOwner) {
        self.locks.release_owner(owner);
    }

    pub fn lock_handle(&self) -> LockHandle {
        LockHandle::new(Arc::clone(&self.locks))
    }

    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<(
            String,
            DataType,
            bool,
            Option<ScalarExpr>,
            Option<IdentitySpec>,
        )>,
        pk_spec: Option<PrimaryKeySpec>,
        foreign_keys: Vec<ForeignKeySpec>,
        search_path: &[SchemaId],
    ) -> anyhow::Result<TableId> {
        let schema_id = self.catalog.ensure_schema(schema);
        if self.catalog.get_table(schema, name).is_some() {
            return Err(sql_err(
                "42P07",
                format!("table already exists: {schema}.{name}"),
            ));
        }

        let id = self.alloc_table_id(schema_id);

        let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
        let mut identities: Vec<Option<IdentityRuntime>> = Vec::with_capacity(cols.len());
        for (n, t, nullable, default_expr, identity) in cols.into_iter() {
            let effective_nullable = if identity.is_some() { false } else { nullable };
            if matches!(
                default_expr.as_ref(),
                Some(ScalarExpr::Literal(Value::Null))
            ) && !effective_nullable
            {
                return Err(sql_err(
                    "23502",
                    format!("default NULL not allowed for NOT NULL column {n}"),
                ));
            }
            if identity.is_some() && default_expr.is_some() {
                return Err(sql_err(
                    "0A000",
                    format!("identity column {n} cannot specify a DEFAULT"),
                ));
            }
            let runtime = identity.as_ref().map(|spec| IdentityRuntime {
                next_value: spec.start_with,
                increment_by: spec.increment_by,
            });
            columns.push(Column {
                name: n,
                data_type: t,
                nullable: effective_nullable,
                default: default_expr,
                identity,
            });
            identities.push(runtime);
        }

        let pk_meta = self.build_primary_key_meta(name, &mut columns, pk_spec)?;

        let fk_metas = self.build_foreign_keys(
            schema,
            name,
            id,
            &columns,
            pk_meta.as_ref(),
            foreign_keys,
            search_path,
        )?;

        let tm = TableMeta {
            id,
            schema: SchemaName::new(schema),
            name: name.to_string(),
            columns: columns.clone(),
            primary_key: pk_meta,
            indexes: vec![],
            foreign_keys: fk_metas,
        };

        let has_pk = tm.primary_key.is_some();

        self.catalog.insert_table(schema_id, name, tm);

        self.tables.insert(id, Table::with_pk(has_pk, identities));
        Ok(id)
    }

    fn table_meta_by_id(&self, id: TableId) -> Option<&TableMeta> {
        self.catalog.tables_by_id.get(&id)
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
        search_path: &[SchemaId],
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

            let referenced_table_name = spec.referenced_table.name.clone();
            let referenced_schema_hint = spec.referenced_table.schema.clone();
            let self_reference = referenced_table_name == table_name
                && match referenced_schema_hint.as_ref() {
                    Some(schema) => schema.as_str() == table_schema,
                    None => true,
                };
            let mut build_meta = |parent_pk: &PrimaryKeyMeta,
                                  parent_columns: &[Column],
                                  referenced_schema: &SchemaName,
                                  referenced_table_name: &str,
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
                                        rc,
                                        referenced_schema.as_str(),
                                        referenced_table_name
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
                    referenced_schema.as_str(),
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
                    referenced_table_name: referenced_table_name.to_string(),
                    referenced_columns: parent_pk.columns.clone(),
                    on_delete: spec.on_delete,
                });
                Ok(())
            };

            if self_reference {
                let self_schema = SchemaName::new(table_schema);
                let parent_pk = self_pk.ok_or_else(|| {
                    sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} must have a primary key",
                            self_schema.as_str(),
                            table_name
                        ),
                    )
                })?;
                build_meta(parent_pk, columns, &self_schema, table_name, table_id)?;
            } else {
                let (ref_meta, resolved_schema) = if let Some(schema) =
                    referenced_schema_hint.as_ref()
                {
                    let meta = self
                            .catalog
                            .get_table(schema.as_str(), &referenced_table_name)
                            .ok_or_else(|| {
                                sql_err(
                                    "42830",
                                    format!(
                                        "referenced table {}.{} for foreign key on {}.{} does not exist",
                                        schema.as_str(),
                                        referenced_table_name,
                                        table_schema,
                                        table_name
                                    ),
                                )
                            })?;
                    (meta, meta.schema.clone())
                } else {
                    let meta =
                        self.resolve_table_in_search_path(search_path, &referenced_table_name)?;
                    (meta, meta.schema.clone())
                };
                let parent_pk = ref_meta.primary_key.as_ref().ok_or_else(|| {
                    sql_err(
                        "42830",
                        format!(
                            "referenced table {}.{} must have a primary key",
                            resolved_schema.as_str(),
                            referenced_table_name
                        ),
                    )
                })?;
                build_meta(
                    parent_pk,
                    &ref_meta.columns,
                    &resolved_schema,
                    &referenced_table_name,
                    ref_meta.id,
                )?;
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

    pub fn insert_full_rows(
        &mut self,
        schema: &str,
        name: &str,
        rows: Vec<Vec<CellInput>>,
        override_system_value: bool,
        txid: TxId,
        ctx: &EvalContext,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let table_id = meta.id;
        let ncols = meta.columns.len();
        #[derive(Clone)]
        struct PendingInsert {
            row: Row,
            pk_key: Option<RowKey>,
            fk_keys: Vec<Option<Vec<Value>>>,
            unique_keys: Vec<(String, Option<Vec<Value>>)>,
        }
        let mut staged: Vec<PendingInsert> = Vec::with_capacity(rows.len());

        for (ridx, row) in rows.into_iter().enumerate() {
            if row.len() != ncols {
                return Err(sql_err(
                    "21P01",
                    format!(
                        "insert has wrong number of values at row {}: expected {}, got {}",
                        ridx + 1,
                        ncols,
                        row.len()
                    ),
                ));
            }
            let out = {
                let table_entry = self.tables.get_mut(&table_id).ok_or_else(|| {
                    sql_err("XX000", format!("missing storage for table id {table_id}"))
                })?;
                Self::materialize_insert_row(table_entry, &meta, row, override_system_value, ctx)?
            };
            let unique_keys = self.build_unique_index_values(&meta, &out);
            if !unique_keys.is_empty() {
                let table_ref = self.tables.get(&table_id).ok_or_else(|| {
                    sql_err("XX000", format!("missing storage for table id {table_id}"))
                })?;
                self.ensure_unique_constraints(&table_ref.unique_maps, &unique_keys, None)?;
            }
            let pk_key = self.build_primary_key_row_key(&meta, &out)?;
            let fk_keys = self.ensure_outbound_foreign_keys(schema, name, &meta, &out)?;
            staged.push(PendingInsert {
                row: out,
                pk_key,
                fk_keys,
                unique_keys,
            });
        }

        let mut table = self
            .tables
            .remove(&table_id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {table_id}")))?;
        let result = (|| {
            let mut count = 0usize;
            let mut inserted_rows = Vec::with_capacity(staged.len());
            let mut inserted_ptrs = Vec::with_capacity(staged.len());

            for pending_insert in staged.into_iter() {
                self.ensure_unique_constraints(
                    &table.unique_maps,
                    &pending_insert.unique_keys,
                    None,
                )?;
                let PendingInsert {
                    row,
                    pk_key,
                    fk_keys,
                    unique_keys,
                } = pending_insert;
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
                self.insert_unique_entries_owned(&mut table, unique_keys, row_id);
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

    fn materialize_insert_row(
        table: &mut Table,
        meta: &TableMeta,
        row: Vec<CellInput>,
        override_system_value: bool,
        ctx: &EvalContext,
    ) -> anyhow::Result<Row> {
        let mut out: Row = Vec::with_capacity(meta.columns.len());
        for (i, cell) in row.into_iter().enumerate() {
            let col = &meta.columns[i];
            let value = match (col.identity.as_ref(), cell) {
                (Some(spec), CellInput::Value(v)) => {
                    if spec.always && !override_system_value {
                        return Err(sql_err(
                            "428C9",
                            format!("cannot insert into identity column \"{}\"", col.name),
                        ));
                    }
                    v
                }
                (Some(_), CellInput::Default) => {
                    let runtime = table
                        .identities
                        .get_mut(i)
                        .and_then(|slot| slot.as_mut())
                        .ok_or_else(|| {
                            sql_err(
                                "XX000",
                                format!("missing identity state for column {}", col.name),
                            )
                        })?;
                    let current = runtime.next_value;
                    runtime.next_value = runtime
                        .next_value
                        .checked_add(runtime.increment_by)
                        .ok_or_else(|| {
                            sql_err(
                                "22003",
                                format!("identity column {} increment overflowed", col.name),
                            )
                        })?;
                    if current < i64::MIN as i128 || current > i64::MAX as i128 {
                        return Err(sql_err(
                            "22003",
                            format!(
                                "identity column {} produced value {} out of range",
                                col.name, current
                            ),
                        ));
                    }
                    Value::Int64(current as i64)
                }
                (None, CellInput::Value(v)) => v,
                (None, CellInput::Default) => {
                    if let Some(expr) = &col.default {
                        eval_column_default(expr, col, i, meta, ctx)?
                    } else {
                        Value::Null
                    }
                }
            };
            let coerced = coerce_value_for_column(value, col, i, meta, ctx)?;
            out.push(coerced);
        }
        Ok(out)
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
        ctx: &EvalContext,
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
        let lock_handle = self.lock_handle();
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
                old_unique: Vec<(String, Option<Vec<Value>>)>,
                new_unique: Vec<(String, Option<Vec<Value>>)>,
            }
            let mut pending: Vec<PendingUpdate> = Vec::new();
            for (key, versions) in table.rows_by_key.iter() {
                let Some(idx) = select_visible_version_idx(versions, visibility) else {
                    continue;
                };
                let snapshot = versions[idx].data.clone();
                let passes = if let Some(expr) = filter {
                    eval_bool_expr(&snapshot, expr, params, ctx)
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
                    let value = eval_scalar_expr(&snapshot, expr, params, ctx)
                        .map_err(|e| sql_err("XX000", e.to_string()))?;
                    let coerced =
                        coerce_value_for_column(value, &meta.columns[*idx], *idx, &meta, ctx)?;
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
                let old_unique = self.build_unique_index_values(&meta, &snapshot);
                let new_unique = self.build_unique_index_values(&meta, &updated);
                pending.push(PendingUpdate {
                    key: key.clone(),
                    updated,
                    old_pk: old_pk_key,
                    new_pk: new_pk_key,
                    old_fk: old_fk_keys,
                    new_fk: new_fk_keys,
                    old_unique,
                    new_unique,
                });
            }
            let mut count = 0usize;
            let mut updated_rows = Vec::with_capacity(pending.len());
            let mut inserted_ptrs = Vec::with_capacity(pending.len());
            let mut touched_ptrs = Vec::with_capacity(pending.len());
            for pending_update in pending {
                let PendingUpdate {
                    key,
                    updated,
                    old_pk,
                    new_pk,
                    old_fk,
                    new_fk,
                    old_unique,
                    new_unique,
                } = pending_update;
                let Some(_) = table
                    .rows_by_key
                    .get(&key)
                    .and_then(|versions| select_visible_version_idx(versions, visibility))
                else {
                    continue;
                };
                let row_id = row_key_to_row_id(&key)?;
                lock_handle.lock_row_nowait(meta.id, row_id, lock_owner)?;
                let Some(idx_check) = table
                    .rows_by_key
                    .get(&key)
                    .and_then(|versions| select_visible_version_idx(versions, visibility))
                else {
                    continue;
                };
                let still_visible = table
                    .rows_by_key
                    .get(&key)
                    .and_then(|versions| versions.get(idx_check))
                    .map(|version| version.xmax.is_none())
                    .unwrap_or(false);
                if !still_visible {
                    continue;
                }
                self.check_unique_updates(&table.unique_maps, &old_unique, &new_unique, row_id)?;
                if let Some(versions) = table.rows_by_key.get_mut(&key) {
                    let Some(idx) = select_visible_version_idx(versions, visibility) else {
                        continue;
                    };
                    if versions[idx].xmax.is_some() {
                        continue;
                    }
                    versions[idx].xmax = Some(txid);
                    versions.push(VersionedRow {
                        xmin: txid,
                        xmax: None,
                        data: updated.clone(),
                    });
                } else {
                    continue;
                }
                updated_rows.push(updated.clone());
                if let Some(pk_meta) = pk_meta {
                    self.update_pk_map_for_row(
                        &mut table,
                        pk_meta,
                        old_pk.as_ref(),
                        new_pk.as_ref(),
                        row_id,
                    )?;
                }
                if fk_changed {
                    self.remove_fk_rev_entries(&mut table, &meta, row_id, &old_fk);
                    self.add_fk_rev_entries(&mut table, &meta, row_id, &new_fk);
                }
                self.apply_unique_updates(&mut table, old_unique, new_unique, row_id);
                let touched_key = key.clone();
                touched_ptrs.push(RowPointer {
                    table_id: meta.id,
                    key: touched_key,
                });
                inserted_ptrs.push(RowPointer {
                    table_id: meta.id,
                    key,
                });
                count += 1;
            }
            Ok((count, updated_rows, inserted_ptrs, touched_ptrs))
        })();
        self.tables.insert(meta.id, table);
        result
    }

    pub fn drop_table(
        &mut self,
        schema: &str,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> anyhow::Result<()> {
        let table_exists = self.catalog.get_table(schema, name).is_some();
        if !table_exists {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("42P01", format!("no such table {schema}.{name}")))
            };
        }

        let inbound = self.collect_inbound_foreign_keys(schema, name);
        if cascade {
            self.drop_inbound_foreign_keys(&inbound)?;
        } else if let Some(fk) = inbound.first() {
            return Err(sql_err(
                "2BP01",
                format!(
                    "cannot drop table {}.{} because it is referenced by {}.{}",
                    schema, name, fk.schema, fk.table
                ),
            ));
        }

        let schema_id = match self.catalog.schema_id(schema) {
            Some(id) => id,
            None => {
                return Err(sql_err("3F000", format!("no such schema {schema}")));
            }
        };
        match self.catalog.remove_table(schema_id, name) {
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

    pub fn create_schema(&mut self, name: &str, if_not_exists: bool) -> anyhow::Result<()> {
        if self.catalog.schema_id(name).is_some() {
            return if if_not_exists {
                Ok(())
            } else {
                Err(sql_err(
                    "42P06",
                    format!("schema \"{}\" already exists", name),
                ))
            };
        }
        self.catalog.ensure_schema(name);
        Ok(())
    }

    pub fn drop_schema(
        &mut self,
        name: &str,
        cascade: bool,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let schema_id = match self.catalog.schema_id(name) {
            Some(id) => id,
            None => {
                return if if_exists {
                    Ok(())
                } else {
                    Err(sql_err(
                        "3F000",
                        format!("schema \"{}\" does not exist", name),
                    ))
                };
            }
        };
        let table_names: Vec<String> = self
            .catalog
            .schemas
            .get(&schema_id)
            .map(|entry| entry.objects.keys().cloned().collect())
            .unwrap_or_default();
        if !cascade && !table_names.is_empty() {
            return Err(sql_err(
                "2BP01",
                format!("cannot drop schema {} because it is not empty", name),
            ));
        }
        for table_name in table_names {
            self.drop_table(name, &table_name, false, cascade)?;
        }
        self.catalog.drop_schema_entry(schema_id);
        Ok(())
    }

    pub fn rename_schema(&mut self, old: &str, new: &str) -> anyhow::Result<()> {
        let schema_id = self
            .catalog
            .schema_id(old)
            .ok_or_else(|| sql_err("3F000", format!("schema \"{}\" does not exist", old)))?;
        if self.catalog.schema_id(new).is_some() {
            return Err(sql_err(
                "42P06",
                format!("schema \"{}\" already exists", new),
            ));
        }
        self.catalog
            .rename_schema_entry(schema_id, SchemaName::new(new));
        Ok(())
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
        ctx: &EvalContext,
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
                    Some(expr) => eval_bool_expr(&row, expr, params, ctx)
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
                let unique_keys = self.build_unique_index_values(&meta, &row);
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
                    unique_keys,
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
                    let unique_keys = self.build_unique_index_values(&child_meta, &row);
                    targets.push(DeleteTarget {
                        schema: fk.schema.clone(),
                        table: fk.table.clone(),
                        meta: child_meta.clone(),
                        row_key: RowKey::RowId(*row_id),
                        row_id: *row_id,
                        pk_key,
                        fk_keys,
                        unique_keys,
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
        let lock_handle = self.lock_handle();
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
                        lock_handle.lock_row_nowait(target.meta.id, target.row_id, lock_owner)?;
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
                        self.remove_unique_entries(table_entry, &target.unique_keys, target.row_id);
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

    fn build_unique_index_values(
        &self,
        meta: &TableMeta,
        row: &[Value],
    ) -> Vec<(String, Option<Vec<Value>>)> {
        let mut entries = Vec::new();
        for idx in meta.indexes.iter().filter(|idx| idx.unique) {
            let mut values = Vec::with_capacity(idx.columns.len());
            let mut enforce = true;
            for col_idx in &idx.columns {
                let value = row.get(*col_idx).cloned().unwrap_or(Value::Null);
                if matches!(value, Value::Null) {
                    enforce = false;
                    break;
                }
                values.push(value);
            }
            if enforce {
                entries.push((idx.name.clone(), Some(values)));
            } else {
                entries.push((idx.name.clone(), None));
            }
        }
        entries
    }

    fn ensure_unique_constraints(
        &self,
        unique_maps: &HashMap<String, HashMap<Vec<Value>, RowId>>,
        entries: &[(String, Option<Vec<Value>>)],
        row_id: Option<RowId>,
    ) -> anyhow::Result<()> {
        for (index_name, maybe_values) in entries {
            let Some(values) = maybe_values else {
                continue;
            };
            if let Some(map) = unique_maps.get(index_name) {
                if let Some(existing) = map.get(values) {
                    if row_id.map_or(true, |rid| *existing != rid) {
                        return Err(sql_err(
                            "23505",
                            format!(
                                "duplicate key value violates unique constraint {}",
                                index_name
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn check_unique_updates(
        &self,
        unique_maps: &HashMap<String, HashMap<Vec<Value>, RowId>>,
        old_entries: &[(String, Option<Vec<Value>>)],
        new_entries: &[(String, Option<Vec<Value>>)],
        row_id: RowId,
    ) -> anyhow::Result<()> {
        for ((index_name, old_vals), (_, new_vals)) in old_entries.iter().zip(new_entries.iter()) {
            let Some(new_values) = new_vals else {
                continue;
            };
            let changed = match old_vals {
                Some(old_values) => old_values != new_values,
                None => true,
            };
            if !changed {
                continue;
            }
            if let Some(map) = unique_maps.get(index_name) {
                if let Some(existing) = map.get(new_values) {
                    if *existing != row_id {
                        return Err(sql_err(
                            "23505",
                            format!(
                                "duplicate key value violates unique constraint {}",
                                index_name
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn insert_unique_entries_owned(
        &self,
        table: &mut Table,
        entries: Vec<(String, Option<Vec<Value>>)>,
        row_id: RowId,
    ) {
        for (index_name, maybe_values) in entries {
            if let Some(values) = maybe_values {
                table
                    .unique_maps
                    .entry(index_name)
                    .or_insert_with(HashMap::new)
                    .insert(values, row_id);
            }
        }
    }

    fn apply_unique_updates(
        &self,
        table: &mut Table,
        old_entries: Vec<(String, Option<Vec<Value>>)>,
        new_entries: Vec<(String, Option<Vec<Value>>)>,
        row_id: RowId,
    ) {
        for ((index_name, old_vals), (_, new_vals)) in
            old_entries.into_iter().zip(new_entries.into_iter())
        {
            if old_vals == new_vals {
                continue;
            }
            if let Some(values) = old_vals.as_ref() {
                self.remove_unique_entry(table, &index_name, values, row_id);
            }
            if let Some(values) = new_vals {
                table
                    .unique_maps
                    .entry(index_name)
                    .or_insert_with(HashMap::new)
                    .insert(values, row_id);
            }
        }
    }

    fn remove_unique_entries(
        &self,
        table: &mut Table,
        entries: &[(String, Option<Vec<Value>>)],
        row_id: RowId,
    ) {
        for (index_name, maybe_values) in entries {
            if let Some(values) = maybe_values {
                self.remove_unique_entry(table, index_name, values, row_id);
            }
        }
    }

    fn remove_unique_entry(
        &self,
        table: &mut Table,
        index_name: &str,
        values: &[Value],
        row_id: RowId,
    ) {
        let mut remove_entry = false;
        if let Some(map) = table.unique_maps.get_mut(index_name) {
            if let Some(existing) = map.get(values) {
                if *existing == row_id {
                    map.remove(values);
                }
            }
            if map.is_empty() {
                remove_entry = true;
            }
        }
        if remove_entry {
            table.unique_maps.remove(index_name);
        }
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
                    fk.referenced_schema.as_str(),
                    fk.referenced_table_name
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
                table_schema,
                table_name,
                fk.name,
                fk.referenced_schema.as_str(),
                fk.referenced_table_name
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
        for meta in self.catalog.tables_by_id.values() {
            for fk in &meta.foreign_keys {
                if fk.referenced_table == parent_id {
                    out.push(InboundForeignKey {
                        schema: meta.schema.as_str().to_string(),
                        table: meta.name.clone(),
                        table_id: meta.id,
                        fk: fk.clone(),
                    });
                }
            }
        }
        out
    }

    fn drop_inbound_foreign_keys(&mut self, inbound: &[InboundForeignKey]) -> anyhow::Result<()> {
        for fk in inbound {
            if let Some(meta) = self.catalog.get_table_mut_by_id(&fk.table_id) {
                meta.foreign_keys
                    .retain(|existing| existing.name != fk.fk.name);
            }
            if let Some(table) = self.tables.get_mut(&fk.table_id) {
                table
                    .fk_rev
                    .retain(|(parent_id, _), _| *parent_id != fk.fk.referenced_table);
            }
        }
        Ok(())
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
                        let unique_keys = self.build_unique_index_values(&meta, &row);
                        self.remove_unique_entries(&mut table, &unique_keys, row_id);
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
                        let unique_keys = self.build_unique_index_values(&meta, &row);
                        self.insert_unique_entries_owned(&mut table, unique_keys, row_id);
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
    unique_keys: Vec<(String, Option<Vec<Value>>)>,
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
    ctx: &EvalContext,
) -> anyhow::Result<Value> {
    let value = eval_scalar_expr_with_mode(&[], expr, &[], ctx, EvalMode::ColumnDefault)
        .map_err(|e| sql_err("XX000", e.to_string()))?;
    coerce_value_for_column(value, col, idx, meta, ctx)
}

fn coerce_value_for_column(
    val: Value,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
    ctx: &EvalContext,
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
    let coerced = cast_value_to_type(val, &col.data_type, &ctx.time_zone).map_err(|e| {
        sql_err(
            e.code,
            format!("column {} (index {}): {}", col.name, idx, e.message),
        )
    })?;
    Ok(coerced)
}
