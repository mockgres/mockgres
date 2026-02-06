use crate::catalog::{
    ColId, ForeignKeyMeta, PrimaryKeyMeta, SchemaId, SchemaName, TableId, TableMeta,
};
use crate::engine::{
    Column, DataType, EvalContext, ForeignKeySpec, IdentitySpec, PrimaryKeySpec, ScalarExpr, Value,
};
use crate::session::SessionTimeZone;
use crate::storage::{IdentityRuntime, Table};
use crate::txn::SYSTEM_TXID;

use super::pg_type::init_pg_type;
use super::{Db, sql_err};

type ColumnSpec = (
    String,
    DataType,
    bool,
    Option<ScalarExpr>,
    Option<IdentitySpec>,
);

impl Db {
    pub(super) fn init_builtin_catalog(&mut self) {
        let public_id = self.catalog.ensure_schema("public");
        let pg_catalog_id = self.catalog.ensure_schema("pg_catalog");
        let info_schema_id = self.catalog.ensure_schema("information_schema");
        let ns_table = self
            .create_table(
                "pg_catalog",
                "pg_namespace",
                vec![
                    ("oid".to_string(), DataType::Int4, false, None, None),
                    ("nspname".to_string(), DataType::Text, false, None, None),
                ],
                None,
                Vec::new(),
                &[],
            )
            .expect("create pg_catalog.pg_namespace");
        let pg_class_table = self
            .create_table(
                "pg_catalog",
                "pg_class",
                vec![
                    ("oid".to_string(), DataType::Int4, false, None, None),
                    ("relname".to_string(), DataType::Text, false, None, None),
                    (
                        "relnamespace".to_string(),
                        DataType::Int4,
                        false,
                        None,
                        None,
                    ),
                    ("relkind".to_string(), DataType::Text, false, None, None),
                ],
                None,
                Vec::new(),
                &[],
            )
            .expect("create pg_catalog.pg_class");
        self.insert_pg_namespace_row(public_id, "public");
        self.insert_pg_namespace_row(pg_catalog_id, "pg_catalog");
        self.insert_pg_namespace_row(info_schema_id, "information_schema");
        self.insert_pg_class_row(ns_table, "pg_namespace", pg_catalog_id, "r");
        self.insert_pg_class_row(pg_class_table, "pg_class", pg_catalog_id, "r");
        init_pg_type(self);
        let _ = self
            .create_table(
                "information_schema",
                "tables",
                vec![
                    (
                        "table_schema".to_string(),
                        DataType::Text,
                        false,
                        None,
                        None,
                    ),
                    ("table_name".to_string(), DataType::Text, false, None, None),
                    ("table_type".to_string(), DataType::Text, false, None, None),
                ],
                None,
                Vec::new(),
                &[],
            )
            .expect("create information_schema.tables");
    }

    pub(super) fn alloc_table_id(&mut self, schema_id: SchemaId) -> TableId {
        let rel_id = self.next_rel_id;
        self.next_rel_id = self.next_rel_id.checked_add(1).expect("table id overflow");
        TableId { schema_id, rel_id }
    }

    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<ColumnSpec>,
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
            check_constraints: vec![],
        };

        let has_pk = tm.primary_key.is_some();

        self.catalog.insert_table(schema_id, name, tm);

        self.tables.insert(id, Table::with_pk(has_pk, identities));
        if self.catalog.get_table("pg_catalog", "pg_class").is_some()
            && !(schema == "pg_catalog" && name == "pg_class")
        {
            self.insert_pg_class_row(id, name, schema_id, "r");
        }
        self.insert_information_schema_table_row(schema, name, "BASE TABLE");
        Ok(id)
    }

    pub fn alter_table_add_foreign_key(
        &mut self,
        schema: &str,
        table: &str,
        spec: ForeignKeySpec,
        search_path: &[SchemaId],
    ) -> anyhow::Result<()> {
        let (table_id, meta_snapshot) = {
            let meta = self
                .catalog
                .get_table(schema, table)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{table}")))?;
            (meta.id, meta.clone())
        };

        let mut metas = self.build_foreign_keys(
            schema,
            table,
            table_id,
            &meta_snapshot.columns,
            meta_snapshot.primary_key.as_ref(),
            vec![spec],
            search_path,
        )?;
        let fk_meta = metas
            .pop()
            .ok_or_else(|| sql_err("XX000", "missing foreign key metadata"))?;

        {
            let meta = self
                .catalog
                .get_table(schema, table)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{table}")))?;
            if meta.foreign_keys.iter().any(|fk| fk.name == fk_meta.name) {
                return Err(sql_err(
                    "42710",
                    format!(
                        "constraint {} for table {}.{} already exists",
                        fk_meta.name, schema, table
                    ),
                ));
            }
        }

        let child_table = self.tables.remove(&table_id).ok_or_else(|| {
            sql_err(
                "XX000",
                format!("missing storage for table id {}", table_id),
            )
        })?;
        let mut child_table = child_table;
        let mut to_add = Vec::new();
        for (key, versions) in child_table.rows_by_key.iter() {
            let Some(row) = versions
                .last()
                .filter(|v| v.xmax.is_none())
                .map(|v| &v.data)
            else {
                continue;
            };
            let Some(parent_key) = super::build_fk_parent_key(row, &fk_meta) else {
                continue;
            };
            super::ensure_parent_exists(
                &self.tables,
                Some((&table_id, &child_table)),
                &fk_meta,
                schema,
                table,
                &parent_key,
            )?;
            let row_id = super::row_key_to_row_id(key)?;
            to_add.push((row_id, parent_key));
        }
        for (row_id, parent_key) in to_add {
            child_table
                .fk_rev
                .entry((fk_meta.referenced_table, parent_key))
                .or_default()
                .insert(row_id);
        }
        self.tables.insert(table_id, child_table);

        let meta = self
            .catalog
            .get_table_mut_by_id(&table_id)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{table}")))?;
        meta.foreign_keys.push(fk_meta);
        Ok(())
    }

    pub(crate) fn insert_pg_namespace_row(&mut self, schema_id: SchemaId, name: &str) {
        if self
            .catalog
            .get_table("pg_catalog", "pg_namespace")
            .is_none()
        {
            return;
        }
        let ctx = EvalContext::new(SessionTimeZone::Utc);
        let rows = vec![vec![
            super::CellInput::Value(Value::Int64(schema_id as i64)),
            super::CellInput::Value(Value::Text(name.to_string())),
        ]];
        let _ = self.insert_full_rows(
            "pg_catalog",
            "pg_namespace",
            rows,
            false,
            SYSTEM_TXID,
            &[],
            &ctx,
            None,
        );
    }

    pub(crate) fn insert_pg_class_row(
        &mut self,
        table_id: TableId,
        relname: &str,
        relnamespace: SchemaId,
        relkind: &str,
    ) {
        if self.catalog.get_table("pg_catalog", "pg_class").is_none() {
            return;
        }
        let ctx = EvalContext::new(SessionTimeZone::Utc);
        let rows = vec![vec![
            super::CellInput::Value(Value::Int64(table_id.rel_id as i64)),
            super::CellInput::Value(Value::Text(relname.to_string())),
            super::CellInput::Value(Value::Int64(relnamespace as i64)),
            super::CellInput::Value(Value::Text(relkind.to_string())),
        ]];
        let _ = self.insert_full_rows(
            "pg_catalog",
            "pg_class",
            rows,
            false,
            SYSTEM_TXID,
            &[],
            &ctx,
            None,
        );
    }

    pub(crate) fn insert_information_schema_table_row(
        &mut self,
        schema: &str,
        name: &str,
        table_type: &str,
    ) {
        if self
            .catalog
            .get_table("information_schema", "tables")
            .is_none()
        {
            return;
        }
        let ctx = EvalContext::new(SessionTimeZone::Utc);
        let rows = vec![vec![
            super::CellInput::Value(Value::Text(schema.to_string())),
            super::CellInput::Value(Value::Text(name.to_string())),
            super::CellInput::Value(Value::Text(table_type.to_string())),
        ]];
        let _ = self.insert_full_rows(
            "information_schema",
            "tables",
            rows,
            false,
            SYSTEM_TXID,
            &[],
            &ctx,
            None,
        );
    }

    pub(crate) fn remove_pg_class_row(&mut self, schema_id: SchemaId, relname: &str) {
        let Some(table_id) = self.catalog.table_id("pg_catalog", "pg_class") else {
            return;
        };
        if let Some(table) = self.tables.get_mut(&table_id) {
            let mut to_remove = Vec::new();
            for (k, versions) in table.rows_by_key.iter() {
                if let Some(row) = versions.last()
                    && matches!(row.data.get(1), Some(Value::Text(n)) if n == relname)
                    && matches!(row.data.get(2), Some(Value::Int64(ns)) if *ns == schema_id as i64)
                {
                    to_remove.push(k.clone());
                }
            }
            for k in to_remove {
                table.rows_by_key.remove(&k);
            }
        }
    }

    pub(crate) fn remove_information_schema_table_row(&mut self, schema: &str, name: &str) {
        let Some(table_id) = self.catalog.table_id("information_schema", "tables") else {
            return;
        };
        if let Some(table) = self.tables.get_mut(&table_id) {
            let mut to_remove = Vec::new();
            for (k, versions) in table.rows_by_key.iter() {
                if let Some(row) = versions.last()
                    && matches!(row.data.get(0), Some(Value::Text(s)) if s == schema)
                    && matches!(row.data.get(1), Some(Value::Text(n)) if n == name)
                {
                    to_remove.push(k.clone());
                }
            }
            for k in to_remove {
                table.rows_by_key.remove(&k);
            }
        }
    }

    pub(crate) fn remove_pg_namespace_row(&mut self, schema_id: SchemaId) {
        let Some(table_id) = self.catalog.table_id("pg_catalog", "pg_namespace") else {
            return;
        };
        if let Some(table) = self.tables.get_mut(&table_id) {
            let mut to_remove = Vec::new();
            for (k, versions) in table.rows_by_key.iter() {
                if let Some(row) = versions.last()
                    && matches!(row.data.first(), Some(Value::Int64(ns)) if *ns == schema_id as i64)
                {
                    to_remove.push(k.clone());
                }
            }
            for k in to_remove {
                table.rows_by_key.remove(&k);
            }
        }
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
        let mut positions: Vec<_> = Vec::with_capacity(pk_columns.len());
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

    #[allow(clippy::too_many_arguments)]
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
            let choose_parent_key = |meta: &TableMeta,
                                     ref_cols: &Option<Vec<String>>|
             -> anyhow::Result<Vec<ColId>> {
                if let Some(names) = ref_cols {
                    let names_match = |candidate: &[ColId]| -> bool {
                        if candidate.len() != names.len() {
                            return false;
                        }
                        candidate.iter().zip(names.iter()).all(|(idx, name)| {
                            meta.columns
                                .get(*idx)
                                .map(|c| c.name.as_str() == name)
                                .unwrap_or(false)
                        })
                    };
                    if let Some(pk) = meta.primary_key.as_ref()
                        && names_match(&pk.columns)
                    {
                        return Ok(pk.columns.clone());
                    }
                    for idx in meta.indexes.iter().filter(|i| i.unique) {
                        if names_match(&idx.columns) {
                            return Ok(idx.columns.clone());
                        }
                    }
                    return Err(sql_err(
                        "42830",
                        format!(
                            "foreign key on {}.{} must reference primary key or unique constraint of {}.{}",
                            table_schema,
                            table_name,
                            meta.schema.as_str(),
                            meta.name
                        ),
                    ));
                }
                if let Some(pk) = meta.primary_key.as_ref() {
                    return Ok(pk.columns.clone());
                }
                if let Some(idx) = meta.indexes.iter().find(|i| i.unique) {
                    return Ok(idx.columns.clone());
                }
                Err(sql_err(
                    "42830",
                    format!(
                        "referenced table {}.{} must have a primary key or unique constraint",
                        meta.schema.as_str(),
                        meta.name
                    ),
                ))
            };
            let mut build_meta = |parent_unique: &[ColId],
                                  parent_columns: &[Column],
                                  referenced_schema: &SchemaName,
                                  referenced_table_name: &str,
                                  referenced_table_id: TableId|
             -> anyhow::Result<()> {
                if parent_unique.len() != local_indexes.len() {
                    return Err(sql_err(
                        "42830",
                        format!(
                            "foreign key on {}.{} has {} local columns but referenced key has {}",
                            table_schema,
                            table_name,
                            local_indexes.len(),
                            parent_unique.len()
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
                    parent_unique.to_vec()
                };

                let ordered_local = self.align_local_to_parent_pk(
                    &local_indexes,
                    &referenced_indexes,
                    parent_unique,
                    table_schema,
                    table_name,
                    referenced_schema.as_str(),
                    referenced_table_name,
                )?;

                for (local_idx, pk_idx) in ordered_local.iter().zip(parent_unique.iter()) {
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

                let fk_name = spec.name.clone().unwrap_or_else(|| {
                    derive_foreign_key_constraint_name(table_name, &spec.columns, spec_idx + 1)
                });

                metas.push(ForeignKeyMeta {
                    name: fk_name,
                    local_columns: ordered_local,
                    referenced_table: referenced_table_id,
                    referenced_schema: referenced_schema.clone(),
                    referenced_table_name: referenced_table_name.to_string(),
                    referenced_columns: parent_unique.to_vec(),
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
                let parent_key = if let Some(ref_cols) = spec.referenced_columns.as_ref() {
                    if ref_cols.len() != parent_pk.columns.len() {
                        return Err(sql_err(
                            "42830",
                            format!(
                                "foreign key on {}.{} has {} columns but referenced list has {}",
                                table_schema,
                                table_name,
                                spec.columns.len(),
                                ref_cols.len()
                            ),
                        ));
                    }
                    parent_pk.columns.clone()
                } else {
                    parent_pk.columns.clone()
                };
                build_meta(&parent_key, columns, &self_schema, table_name, table_id)?;
            } else {
                let (ref_meta, resolved_schema) = if let Some(schema) = referenced_schema_hint {
                    let meta = self.catalog.get_table(schema.as_str(), &referenced_table_name).ok_or_else(|| {
                        sql_err(
                            "42P01",
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
                let parent_key = choose_parent_key(ref_meta, &spec.referenced_columns)?;
                build_meta(
                    &parent_key,
                    &ref_meta.columns,
                    &resolved_schema,
                    &referenced_table_name,
                    ref_meta.id,
                )?;
            }
        }
        Ok(metas)
    }

    #[allow(clippy::too_many_arguments)]
    fn align_local_to_parent_pk(
        &self,
        local_indexes: &[ColId],
        referenced_indexes: &[ColId],
        parent_key: &[ColId],
        table_schema: &str,
        table_name: &str,
        referenced_schema: &str,
        referenced_table_name: &str,
    ) -> anyhow::Result<Vec<ColId>> {
        let mut ordered = Vec::with_capacity(parent_key.len());
        for pk_idx in parent_key {
            let Some(pos) = referenced_indexes.iter().position(|idx| idx == pk_idx) else {
                return Err(sql_err(
                    "42830",
                    format!(
                        "foreign key on {}.{} must reference primary or unique key columns of {}.{}",
                        table_schema, table_name, referenced_schema, referenced_table_name
                    ),
                ));
            };
            ordered.push(local_indexes[pos]);
        }
        Ok(ordered)
    }
}

fn derive_foreign_key_constraint_name(
    table_name: &str,
    column_names: &[String],
    sequence: usize,
) -> String {
    if !column_names.is_empty() {
        let mut name = table_name.to_string();
        for col in column_names {
            name.push('_');
            name.push_str(col);
        }
        name.push_str("_fkey");
        return name;
    }
    format!("{table_name}_fk{sequence}")
}
