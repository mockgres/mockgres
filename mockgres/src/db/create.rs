use crate::catalog::{
    ColId, ForeignKeyMeta, PrimaryKeyMeta, SchemaId, SchemaName, TableId, TableMeta,
};
use crate::engine::{
    Column, DataType, ForeignKeySpec, IdentitySpec, PrimaryKeySpec, ScalarExpr, Value,
};
use crate::storage::{IdentityRuntime, Table};

use super::{Db, sql_err};

impl Db {
    pub(super) fn init_builtin_catalog(&mut self) {
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

    pub(super) fn alloc_table_id(&mut self, schema_id: SchemaId) -> TableId {
        let rel_id = self.next_rel_id;
        self.next_rel_id = self.next_rel_id.checked_add(1).expect("table id overflow");
        TableId { schema_id, rel_id }
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
}
