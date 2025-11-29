use super::*;
use std::collections::HashSet;

#[derive(Clone)]
struct PendingInsert {
    row: Row,
    pk_key: Option<RowKey>,
    unique_keys: Vec<(String, Option<Vec<Value>>)>,
}

#[derive(Clone)]
enum InsertOrUpdateOp {
    Insert(PendingInsert),
    UpdateOnConflict {
        existing_rowid: RowId,
        pending_insert: PendingInsert,
    },
}

impl Db {
    pub(crate) fn insert_full_rows(
        &mut self,
        schema: &str,
        name: &str,
        rows: Vec<Vec<CellInput>>,
        override_system_value: bool,
        txid: TxId,
        params: &[Value],
        ctx: &EvalContext,
        on_conflict: Option<ResolvedOnConflictKind>,
    ) -> anyhow::Result<(usize, Vec<Row>, Vec<RowPointer>, Vec<Row>, Vec<RowPointer>)> {
        let meta = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let table_id = meta.id;
        let ncols = meta.columns.len();
        let mut pending_ops: Vec<InsertOrUpdateOp> = Vec::with_capacity(rows.len());

        enum ConflictDirective {
            None,
            DoNothing(ResolvedOnConflictTarget),
            DoUpdate {
                target: ResolvedOnConflictTarget,
                sets: Vec<UpdateSet>,
                where_clause: Option<BoolExpr>,
            },
        }

        let conflict_directive = match on_conflict {
            Some(ResolvedOnConflictKind::DoNothing(target)) => ConflictDirective::DoNothing(target),
            Some(ResolvedOnConflictKind::DoUpdate {
                target,
                sets,
                where_clause,
            }) => ConflictDirective::DoUpdate {
                target,
                sets,
                where_clause,
            },
            None => ConflictDirective::None,
        };

        let pk_meta = meta.primary_key.as_ref();
        let (inbound, updates_pk, fk_changed) = match &conflict_directive {
            ConflictDirective::DoUpdate { sets, .. } => {
                let pk_cols: HashSet<usize> = pk_meta
                    .map(|pk| pk.columns.iter().copied().collect())
                    .unwrap_or_default();
                let updates_pk = pk_meta.is_some()
                    && sets.iter().any(
                        |set| matches!(set, UpdateSet::ByIndex(idx, _) if pk_cols.contains(idx)),
                    );
                let inbound = if updates_pk {
                    collect_inbound_foreign_keys(&self.catalog, schema, name)
                } else {
                    Vec::new()
                };
                let mut fk_updates = vec![false; meta.foreign_keys.len()];
                for idx in sets.iter().filter_map(|set| {
                    if let UpdateSet::ByIndex(idx, _) = set {
                        Some(*idx)
                    } else {
                        None
                    }
                }) {
                    for (fk_idx, fk) in meta.foreign_keys.iter().enumerate() {
                        if fk.local_columns.contains(&idx) {
                            fk_updates[fk_idx] = true;
                        }
                    }
                }
                let fk_changed = fk_updates.iter().any(|f| *f);
                (inbound, updates_pk, fk_changed)
            }
            _ => (Vec::new(), false, false),
        };

        if updates_pk && !inbound.is_empty() {
            return Err(sql_err(
                "0A000",
                "updating referenced primary key columns is not supported",
            ));
        }

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
            let pk_key = build_primary_key_row_key(&meta, &out)?;
            let unique_keys = build_unique_index_values(&meta, &out);
            let pending_insert = PendingInsert {
                row: out,
                pk_key: pk_key.clone(),
                unique_keys,
            };

            let mut conflict: Option<InsertConflict> = None;
            if let Some(pk_key) = pk_key.as_ref() {
                if let Some(existing_rowid) = self.tables[&table_id]
                    .pk_map
                    .as_ref()
                    .and_then(|map| map.get(pk_key))
                {
                    let constraint = meta
                        .primary_key
                        .as_ref()
                        .expect("pk metadata exists when pk_key is present");
                    conflict = Some(InsertConflict::PrimaryKey {
                        name: constraint.name.clone(),
                        row_id: *existing_rowid,
                    });
                }
            }
            if conflict.is_none() {
                if let Some(existing) = find_unique_violation(
                    &self.tables[&table_id].unique_maps,
                    &pending_insert.unique_keys,
                    None,
                ) {
                    conflict = Some(InsertConflict::UniqueIndex(existing));
                }
            }

            if let Some(conflict) = conflict {
                match &conflict_directive {
                    ConflictDirective::DoNothing(target) => {
                        if conflict_matches_target(&conflict, target) {
                            continue;
                        }
                    }
                    ConflictDirective::DoUpdate { target, .. } => {
                        if conflict_matches_target(&conflict, target) {
                            pending_ops.push(InsertOrUpdateOp::UpdateOnConflict {
                                existing_rowid: conflict.row_id(),
                                pending_insert,
                            });
                            continue;
                        }
                    }
                    ConflictDirective::None => {}
                }
                return Err(sql_err(
                    "23505",
                    format!(
                        "duplicate key value violates unique constraint {}",
                        conflict.index_name()
                    ),
                ));
            }

            pending_ops.push(InsertOrUpdateOp::Insert(pending_insert));
        }

        let mut table = self
            .tables
            .remove(&table_id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {table_id}")))?;
        let result = (|| {
            let mut count = 0usize;
            let mut inserted_rows = Vec::with_capacity(pending_ops.len());
            let mut inserted_ptrs = Vec::with_capacity(pending_ops.len());
            let mut updated_rows = Vec::new();
            let mut updated_ptrs = Vec::new();

            let apply_conflict_update = |existing_rowid: RowId,
                                         pending_insert: PendingInsert,
                                         sets: &[UpdateSet],
                                         where_clause: &Option<BoolExpr>,
                                         table: &mut Table,
                                         updated_rows: &mut Vec<Row>,
                                         updated_ptrs: &mut Vec<RowPointer>|
             -> anyhow::Result<bool> {
                let key = RowKey::RowId(existing_rowid);
                let current = {
                    let Some(versions) = table.rows_by_key.get(&key) else {
                        return Ok(false);
                    };
                    let Some(idx) = versions.iter().rposition(|v| v.xmax.is_none()) else {
                        return Ok(false);
                    };
                    versions[idx].data.clone()
                };

                let excluded_row = &pending_insert.row;
                if let Some(pred) = where_clause {
                    let rewritten_pred = substitute_excluded_in_bool(pred, excluded_row, &meta)?;
                    let passes = eval_bool_expr(&current, &rewritten_pred, params, ctx)
                        .map_err(|e| sql_err("XX000", e.to_string()))?
                        .unwrap_or(false);
                    if !passes {
                        return Ok(false);
                    }
                }

                let mut updated = current.clone();
                for set in sets {
                    let (idx, expr) = match set {
                        UpdateSet::ByIndex(idx, expr) => (*idx, expr),
                        UpdateSet::ByName(_, _) => {
                            unreachable!("update sets must be bound to column indexes")
                        }
                    };
                    let rewritten_expr = substitute_excluded_in_scalar(expr, excluded_row, &meta)?;
                    let value = eval_scalar_expr(&current, &rewritten_expr, params, ctx)
                        .map_err(|e| sql_err("XX000", e.to_string()))?;
                    let coerced =
                        coerce_value_for_column(value, &meta.columns[idx], idx, &meta, ctx)?;
                    updated[idx] = coerced;
                }

                let old_pk = build_primary_key_row_key(&meta, &current)?;
                let new_pk = build_primary_key_row_key(&meta, &updated)?;
                let old_fk: Vec<Option<Vec<Value>>> = meta
                    .foreign_keys
                    .iter()
                    .map(|fk| build_fk_parent_key(&current, fk))
                    .collect();
                let new_fk = self.ensure_outbound_foreign_keys(
                    schema,
                    name,
                    &meta,
                    &updated,
                    Some((&table_id, table)),
                )?;
                let old_unique = build_unique_index_values(&meta, &current);
                let new_unique = build_unique_index_values(&meta, &updated);

                check_unique_updates(&table.unique_maps, &old_unique, &new_unique, existing_rowid)?;

                let Some(versions) = table.rows_by_key.get_mut(&key) else {
                    return Ok(false);
                };
                let Some(current_idx) = versions.iter().rposition(|v| v.xmax.is_none()) else {
                    return Ok(false);
                };
                versions[current_idx].xmax = Some(txid);
                versions.push(VersionedRow {
                    xmin: txid,
                    xmax: None,
                    data: updated.clone(),
                });
                updated_rows.push(updated.clone());

                if let Some(pk_meta) = pk_meta {
                    update_pk_map_for_row(
                        table,
                        pk_meta,
                        old_pk.as_ref(),
                        new_pk.as_ref(),
                        existing_rowid,
                    )?;
                }

                if fk_changed {
                    remove_fk_rev_entries(table, &meta, existing_rowid, &old_fk);
                    add_fk_rev_entries(table, &meta, existing_rowid, &new_fk);
                }

                apply_unique_updates(table, old_unique, new_unique, existing_rowid);

                updated_ptrs.push(RowPointer { table_id, key });
                Ok(true)
            };

            for op in pending_ops.into_iter() {
                match op {
                    InsertOrUpdateOp::Insert(pending_insert) => {
                        if let Some(conflict) = detect_conflict(&meta, &table, &pending_insert) {
                            match &conflict_directive {
                                ConflictDirective::DoNothing(target) => {
                                    if conflict_matches_target(&conflict, target) {
                                        continue;
                                    }
                                }
                                ConflictDirective::DoUpdate {
                                    target,
                                    sets,
                                    where_clause,
                                } => {
                                    if conflict_matches_target(&conflict, target) {
                                        if apply_conflict_update(
                                            conflict.row_id(),
                                            pending_insert,
                                            sets,
                                            where_clause,
                                            &mut table,
                                            &mut updated_rows,
                                            &mut updated_ptrs,
                                        )? {
                                            count += 1;
                                        }
                                        continue;
                                    }
                                }
                                ConflictDirective::None => {}
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
                            let pk_map = table
                                .pk_map
                                .as_mut()
                                .expect("pk_map exists when table has primary key");
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
                    InsertOrUpdateOp::UpdateOnConflict {
                        existing_rowid,
                        pending_insert,
                    } => {
                        if let ConflictDirective::DoUpdate {
                            sets, where_clause, ..
                        } = &conflict_directive
                        {
                            if apply_conflict_update(
                                existing_rowid,
                                pending_insert,
                                sets,
                                where_clause,
                                &mut table,
                                &mut updated_rows,
                                &mut updated_ptrs,
                            )? {
                                count += 1;
                            }
                        }
                    }
                }
            }

            Ok((
                count,
                inserted_rows,
                inserted_ptrs,
                updated_rows,
                updated_ptrs,
            ))
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
    PrimaryKey { name: String, row_id: RowId },
    UniqueIndex(ConflictInfo),
}

impl InsertConflict {
    fn index_name(&self) -> &str {
        match self {
            InsertConflict::PrimaryKey { name, .. } => name.as_str(),
            InsertConflict::UniqueIndex(info) => info.index_name.as_str(),
        }
    }

    fn row_id(&self) -> RowId {
        match self {
            InsertConflict::PrimaryKey { row_id, .. } => *row_id,
            InsertConflict::UniqueIndex(info) => info.existing_rowid,
        }
    }
}

fn detect_conflict(
    meta: &TableMeta,
    table: &Table,
    pending: &PendingInsert,
) -> Option<InsertConflict> {
    if let Some(pk_key) = pending.pk_key.as_ref() {
        if let Some(existing) = table.pk_map.as_ref().and_then(|pk_map| pk_map.get(pk_key)) {
            let constraint = meta
                .primary_key
                .as_ref()
                .expect("pk metadata exists when pk_key is present");
            return Some(InsertConflict::PrimaryKey {
                name: constraint.name.clone(),
                row_id: *existing,
            });
        }
    }
    if let Some(conflict) = find_unique_violation(&table.unique_maps, &pending.unique_keys, None) {
        return Some(InsertConflict::UniqueIndex(conflict));
    }
    None
}

fn conflict_matches_target(conflict: &InsertConflict, target: &ResolvedOnConflictTarget) -> bool {
    match target {
        ResolvedOnConflictTarget::AnyConstraint => true,
        ResolvedOnConflictTarget::UniqueIndex { index_name } => {
            matches!(conflict, InsertConflict::UniqueIndex(info) if &info.index_name == index_name)
        }
        ResolvedOnConflictTarget::Constraint { index_name } => match conflict {
            InsertConflict::PrimaryKey { name, .. } => name == index_name,
            InsertConflict::UniqueIndex(info) => &info.index_name == index_name,
        },
    }
}

fn substitute_excluded_in_scalar(
    expr: &ScalarExpr,
    excluded: &[Value],
    meta: &TableMeta,
) -> anyhow::Result<ScalarExpr> {
    Ok(match expr {
        ScalarExpr::ExcludedIdx(i) => excluded
            .get(*i)
            .cloned()
            .map(ScalarExpr::Literal)
            .ok_or_else(|| {
                sql_err(
                    "XX000",
                    format!("EXCLUDED index {i} out of range for {}", meta.name),
                )
            })?,
        ScalarExpr::BinaryOp { op, left, right } => ScalarExpr::BinaryOp {
            op: *op,
            left: Box::new(substitute_excluded_in_scalar(left, excluded, meta)?),
            right: Box::new(substitute_excluded_in_scalar(right, excluded, meta)?),
        },
        ScalarExpr::UnaryOp { op, expr } => ScalarExpr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_excluded_in_scalar(expr, excluded, meta)?),
        },
        ScalarExpr::Func { func, args } => ScalarExpr::Func {
            func: *func,
            args: args
                .iter()
                .map(|a| substitute_excluded_in_scalar(a, excluded, meta))
                .collect::<anyhow::Result<Vec<_>>>()?,
        },
        ScalarExpr::Cast { expr, ty } => ScalarExpr::Cast {
            expr: Box::new(substitute_excluded_in_scalar(expr, excluded, meta)?),
            ty: ty.clone(),
        },
        other => other.clone(),
    })
}

fn substitute_excluded_in_bool(
    expr: &BoolExpr,
    excluded: &[Value],
    meta: &TableMeta,
) -> anyhow::Result<BoolExpr> {
    Ok(match expr {
        BoolExpr::Literal(v) => BoolExpr::Literal(*v),
        BoolExpr::Comparison { lhs, op, rhs } => BoolExpr::Comparison {
            lhs: substitute_excluded_in_scalar(lhs, excluded, meta)?,
            op: *op,
            rhs: substitute_excluded_in_scalar(rhs, excluded, meta)?,
        },
        BoolExpr::And(exprs) => BoolExpr::And(
            exprs
                .iter()
                .map(|e| substitute_excluded_in_bool(e, excluded, meta))
                .collect::<anyhow::Result<Vec<_>>>()?,
        ),
        BoolExpr::Or(exprs) => BoolExpr::Or(
            exprs
                .iter()
                .map(|e| substitute_excluded_in_bool(e, excluded, meta))
                .collect::<anyhow::Result<Vec<_>>>()?,
        ),
        BoolExpr::Not(inner) => BoolExpr::Not(Box::new(substitute_excluded_in_bool(
            inner, excluded, meta,
        )?)),
        BoolExpr::IsNull { expr, negated } => BoolExpr::IsNull {
            expr: substitute_excluded_in_scalar(expr, excluded, meta)?,
            negated: *negated,
        },
        BoolExpr::InSubquery { .. } | BoolExpr::InListValues { .. } => {
            return Err(sql_err(
                "0A000",
                "subqueries are not supported in this context",
            ));
        }
    })
}
