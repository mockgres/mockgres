use super::*;

#[derive(Clone)]
struct PendingInsert {
    row: Row,
    pk_key: Option<RowKey>,
    unique_keys: Vec<(String, Option<Vec<Value>>)>,
}

impl Db {
    pub(crate) fn insert_full_rows(
        &mut self,
        schema: &str,
        name: &str,
        rows: Vec<Vec<CellInput>>,
        override_system_value: bool,
        txid: TxId,
        ctx: &EvalContext,
        on_conflict: Option<ResolvedOnConflictTarget>,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let table_id = meta.id;
        let ncols = meta.columns.len();
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
            let unique_keys = build_unique_index_values(&meta, &out);
            let pk_key = build_primary_key_row_key(&meta, &out)?;
            staged.push(PendingInsert {
                row: out,
                pk_key,
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
                if let Some(conflict) = detect_conflict(&meta, &table, &pending_insert) {
                    if let Some(ref target) = on_conflict {
                        if conflict_matches_target(&conflict, target) {
                            continue;
                        }
                    }
                    let index_name = conflict.index_name();
                    return Err(sql_err(
                        "23505",
                        format!(
                            "duplicate key value violates unique constraint {}",
                            index_name
                        ),
                    ));
                }
                let PendingInsert {
                    row,
                    pk_key,
                    unique_keys,
                } = pending_insert;
                let fk_keys = self.ensure_outbound_foreign_keys(
                    schema,
                    name,
                    &meta,
                    &row,
                    Some((&table_id, &table)),
                )?;
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
                add_fk_rev_entries(&mut table, &meta, row_id, &fk_keys);
                insert_unique_entries_owned(&mut table, unique_keys, row_id);
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
}

#[derive(Clone, Debug)]
enum InsertConflict {
    PrimaryKey(String),
    UniqueIndex(String),
}

impl InsertConflict {
    fn index_name(&self) -> &str {
        match self {
            InsertConflict::PrimaryKey(name) | InsertConflict::UniqueIndex(name) => name.as_str(),
        }
    }
}

fn detect_conflict(
    meta: &TableMeta,
    table: &Table,
    pending: &PendingInsert,
) -> Option<InsertConflict> {
    if let Some(pk_key) = pending.pk_key.as_ref() {
        if table
            .pk_map
            .as_ref()
            .and_then(|pk_map| pk_map.get(pk_key))
            .is_some()
        {
            let constraint = meta
                .primary_key
                .as_ref()
                .expect("pk metadata exists when pk_key is present");
            return Some(InsertConflict::PrimaryKey(constraint.name.clone()));
        }
    }
    for (index_name, maybe_values) in &pending.unique_keys {
        let Some(values) = maybe_values else {
            continue;
        };
        if let Some(map) = table.unique_maps.get(index_name) {
            if map.get(values).is_some() {
                return Some(InsertConflict::UniqueIndex(index_name.clone()));
            }
        }
    }
    None
}

fn conflict_matches_target(conflict: &InsertConflict, target: &ResolvedOnConflictTarget) -> bool {
    match target {
        ResolvedOnConflictTarget::AnyConstraint => true,
        ResolvedOnConflictTarget::UniqueIndex { index_name } => {
            matches!(conflict, InsertConflict::UniqueIndex(name) if name == index_name)
        }
        ResolvedOnConflictTarget::Constraint { index_name } => match conflict {
            InsertConflict::PrimaryKey(name) | InsertConflict::UniqueIndex(name) => {
                name == index_name
            }
        },
    }
}
