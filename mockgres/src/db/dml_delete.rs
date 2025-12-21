use std::collections::{HashMap, HashSet};

use super::*;

impl Db {
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
                let pk_key = build_primary_key_row_key(&meta, &row)?;
                let fk_keys = meta
                    .foreign_keys
                    .iter()
                    .map(|fk| build_fk_parent_key(&row, fk))
                    .collect();
                let unique_keys = build_unique_index_values(&meta, &row);
                let row_key = key.clone();
                let row_id = row_key_to_row_id(&row_key)?;
                seeds.push(DeleteTarget {
                    schema: schema_name.clone(),
                    table: table_name.clone(),
                    meta: meta.clone(),
                    row_key,
                    row_id,
                    row: row.clone(),
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
            let inbound = collect_inbound_foreign_keys(
                &self.catalog,
                &frame.target.schema,
                &frame.target.table,
            );
            ensure_no_inbound_refs(
                &self.tables,
                &frame.target.schema,
                &frame.target.table,
                frame.target.meta.id,
                &inbound,
                &frame.target.row,
                visibility,
            )?;
            for fk in inbound {
                if fk.fk.on_delete == ReferentialAction::Cascade {
                    let Some(parent_key) = build_referenced_parent_key(&frame.target.row, &fk.fk)
                    else {
                        continue;
                    };
                    let children = self.collect_cascade_children(&fk, &parent_key, visibility)?;
                    for child in children {
                        stack.push(CascadeFrame {
                            target: child,
                            expanded: false,
                        });
                    }
                }
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
                    let pk_key = build_primary_key_row_key(&child_meta, &row)?;
                    let fk_keys = child_meta
                        .foreign_keys
                        .iter()
                        .map(|fk_meta| build_fk_parent_key(&row, fk_meta))
                        .collect();
                    let unique_keys = build_unique_index_values(&child_meta, &row);
                    targets.push(DeleteTarget {
                        schema: fk.schema.clone(),
                        table: fk.table.clone(),
                        meta: child_meta.clone(),
                        row_key: RowKey::RowId(*row_id),
                        row_id: *row_id,
                        row: row.clone(),
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
                    table_cache.get_mut(&target.meta.id).unwrap()
                };
                let row_id = target.row_id;
                lock_handle.lock_row_nowait(target.meta.id, row_id, lock_owner)?;
                if let Some(versions) = table_entry.rows_by_key.get_mut(&target.row_key) {
                    let Some(idx) = select_visible_version_idx(versions, visibility) else {
                        continue;
                    };
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
                    remove_fk_rev_entries(
                        table_entry,
                        &target.meta,
                        target.row_id,
                        &target.fk_keys,
                    );
                    remove_unique_entries(table_entry, &target.unique_keys, target.row_id);
                }
            }
            Ok(touched_ptrs)
        })();
        for (id, table) in table_cache {
            self.tables.insert(id, table);
        }
        result
    }
}
