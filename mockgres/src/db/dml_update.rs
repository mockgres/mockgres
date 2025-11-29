use std::collections::HashSet;

use super::*;

impl Db {
    pub fn update_rows(
        &mut self,
        schema: &str,
        name: &str,
        sets: &[(usize, ScalarExpr)],
        filter: Option<&BoolExpr>,
        source_rows: Option<&[Row]>,
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
            let inbound = collect_inbound_foreign_keys(&self.catalog, schema, name);
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
                return_row: Row,
            }
            let mut pending: Vec<PendingUpdate> = Vec::new();
            for (key, versions) in table.rows_by_key.iter() {
                let Some(idx) = select_visible_version_idx(versions, visibility) else {
                    continue;
                };
                let snapshot = versions[idx].data.clone();
                let mut chosen_source: Option<Row> = None;
                let passes = if let Some(src_rows) = source_rows {
                    for src in src_rows.iter() {
                        let mut combined = snapshot.clone();
                        combined.extend(src.clone());
                        let ok = if let Some(expr) = filter {
                            eval_bool_expr(&combined, expr, params, ctx)
                                .map_err(|e| sql_err("XX000", e.to_string()))?
                                .unwrap_or(false)
                        } else {
                            true
                        };
                        if ok {
                            chosen_source = Some(src.clone());
                            break;
                        }
                    }
                    chosen_source.is_some()
                } else if let Some(expr) = filter {
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
                let eval_row = if let Some(src) = &chosen_source {
                    let mut combined = snapshot.clone();
                    combined.extend(src.clone());
                    combined
                } else {
                    snapshot.clone()
                };
                for (idx, expr) in sets {
                    let value = eval_scalar_expr(&eval_row, expr, params, ctx)
                        .map_err(|e| sql_err("XX000", e.to_string()))?;
                    let coerced =
                        coerce_value_for_column(value, &meta.columns[*idx], *idx, &meta, ctx)?;
                    updated[*idx] = coerced;
                }
                let mut return_row = updated.clone();
                if let Some(src) = chosen_source {
                    return_row.extend(src);
                }
                let old_pk_key = build_primary_key_row_key(&meta, &snapshot)?;
                let new_pk_key = build_primary_key_row_key(&meta, &updated)?;
                let old_fk_keys: Vec<Option<Vec<Value>>> = meta
                    .foreign_keys
                    .iter()
                    .map(|fk| build_fk_parent_key(&snapshot, fk))
                    .collect();
                let new_fk_keys = self.ensure_outbound_foreign_keys(
                    schema,
                    name,
                    &meta,
                    &updated,
                    Some((&meta.id, &table)),
                )?;
                let old_unique = build_unique_index_values(&meta, &snapshot);
                let new_unique = build_unique_index_values(&meta, &updated);
                pending.push(PendingUpdate {
                    key: key.clone(),
                    updated,
                    old_pk: old_pk_key,
                    new_pk: new_pk_key,
                    old_fk: old_fk_keys,
                    new_fk: new_fk_keys,
                    old_unique,
                    new_unique,
                    return_row,
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
                    return_row,
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
                let Some(versions) = table.rows_by_key.get_mut(&key) else {
                    continue;
                };
                let Some(idx) = select_visible_version_idx(versions, visibility) else {
                    continue;
                };
                if versions[idx].xmax.is_some() {
                    continue;
                }
                check_unique_updates(&table.unique_maps, &old_unique, &new_unique, row_id)?;
                versions[idx].xmax = Some(txid);
                versions.push(VersionedRow {
                    xmin: txid,
                    xmax: None,
                    data: updated.clone(),
                });
                updated_rows.push(return_row.clone());
                if let Some(pk_meta) = pk_meta {
                    update_pk_map_for_row(
                        &mut table,
                        pk_meta,
                        old_pk.as_ref(),
                        new_pk.as_ref(),
                        row_id,
                    )?;
                }
                if fk_changed {
                    remove_fk_rev_entries(&mut table, &meta, row_id, &old_fk);
                    add_fk_rev_entries(&mut table, &meta, row_id, &new_fk);
                }
                apply_unique_updates(&mut table, old_unique, new_unique, row_id);
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
}
