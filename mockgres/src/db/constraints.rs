use std::collections::{HashMap, HashSet};

use crate::catalog::{Catalog, ForeignKeyMeta, PrimaryKeyMeta, TableId, TableMeta};
use crate::engine::{ReferentialAction, Value};
use crate::storage::{Row, RowId, RowKey, Table};
use crate::txn::VisibilityContext;

use super::sql_err;
use super::visibility::visible_row_clone;

#[derive(Clone, Debug)]
pub struct ConflictInfo {
    pub index_name: String,
    pub existing_rowid: RowId,
}

#[derive(Clone)]
pub(crate) struct InboundForeignKey {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) table_id: TableId,
    pub(crate) fk: ForeignKeyMeta,
}

#[derive(Clone)]
pub(crate) struct DeleteTarget {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) meta: TableMeta,
    pub(crate) row_key: RowKey,
    pub(crate) row_id: RowId,
    pub(crate) row: Row,
    pub(crate) pk_key: Option<RowKey>,
    pub(crate) fk_keys: Vec<Option<Vec<Value>>>,
    pub(crate) unique_keys: Vec<(String, Option<Vec<Value>>)>,
}

pub(crate) struct CascadeFrame {
    pub(crate) target: DeleteTarget,
    pub(crate) expanded: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum ResolvedOnConflictTarget {
    // ON CONFLICT DO NOTHING with no explicit target
    AnyConstraint,
    // ON CONFLICT (col1, col2, ...)
    UniqueIndex { index_name: String },
    // ON CONFLICT ON CONSTRAINT constraint_name
    Constraint { index_name: String },
}

pub(crate) fn build_primary_key_row_key(
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

pub(crate) fn build_fk_parent_key(row: &[Value], fk: &ForeignKeyMeta) -> Option<Vec<Value>> {
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

pub(crate) fn build_unique_index_values(
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

pub(crate) fn find_unique_violation(
    unique_maps: &HashMap<String, HashMap<Vec<Value>, RowId>>,
    entries: &[(String, Option<Vec<Value>>)],
    row_id: Option<RowId>,
) -> Option<ConflictInfo> {
    for (index_name, maybe_values) in entries {
        let Some(values) = maybe_values else {
            continue;
        };
        if let Some(map) = unique_maps.get(index_name) {
            if let Some(existing) = map.get(values) {
                if row_id.map_or(true, |rid| *existing != rid) {
                    return Some(ConflictInfo {
                        index_name: index_name.clone(),
                        existing_rowid: *existing,
                    });
                }
            }
        }
    }
    None
}

#[allow(dead_code)]
pub(crate) fn ensure_unique_constraints(
    unique_maps: &HashMap<String, HashMap<Vec<Value>, RowId>>,
    entries: &[(String, Option<Vec<Value>>)],
    row_id: Option<RowId>,
) -> anyhow::Result<()> {
    if let Some(conflict) = find_unique_violation(unique_maps, entries, row_id) {
        return Err(sql_err(
            "23505",
            format!(
                "duplicate key value violates unique constraint {}",
                conflict.index_name
            ),
        ));
    }
    Ok(())
}

pub(crate) fn check_unique_updates(
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

pub(crate) fn insert_unique_entries_owned(
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

pub(crate) fn apply_unique_updates(
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
            remove_unique_entry(table, &index_name, values, row_id);
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

pub(crate) fn remove_unique_entries(
    table: &mut Table,
    entries: &[(String, Option<Vec<Value>>)],
    row_id: RowId,
) {
    for (index_name, maybe_values) in entries {
        if let Some(values) = maybe_values {
            remove_unique_entry(table, index_name, values, row_id);
        }
    }
}

pub(crate) fn remove_unique_entry(
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

pub(crate) fn ensure_parent_exists(
    tables: &HashMap<TableId, Table>,
    current_table: Option<(&TableId, &Table)>,
    fk: &ForeignKeyMeta,
    table_schema: &str,
    table_name: &str,
    parent_key: &[Value],
) -> anyhow::Result<()> {
    let parent_table = tables
        .get(&fk.referenced_table)
        .or_else(|| {
            current_table.and_then(|(id, table)| {
                if *id == fk.referenced_table {
                    Some(table)
                } else {
                    None
                }
            })
        })
        .ok_or_else(|| {
            sql_err(
                "XX000",
                format!(
                    "missing storage for referenced table id {}",
                    fk.referenced_table
                ),
            )
        })?;
    for versions in parent_table.rows_by_key.values() {
        if versions
            .iter()
            .rev()
            .filter(|version| version.xmax.is_none())
            .any(|version| referenced_key_matches(&version.data, fk, parent_key))
        {
            return Ok(());
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

pub(crate) fn build_referenced_parent_key(
    row: &[Value],
    fk: &ForeignKeyMeta,
) -> Option<Vec<Value>> {
    let mut out = Vec::with_capacity(fk.referenced_columns.len());
    for idx in &fk.referenced_columns {
        let value = row.get(*idx).cloned().unwrap_or(Value::Null);
        if matches!(value, Value::Null) {
            return None;
        }
        out.push(value);
    }
    Some(out)
}

fn referenced_key_matches(row: &[Value], fk: &ForeignKeyMeta, key: &[Value]) -> bool {
    if fk.referenced_columns.len() != key.len() {
        return false;
    }
    fk.referenced_columns
        .iter()
        .zip(key.iter())
        .all(|(idx, expected)| row.get(*idx).is_some_and(|v| v == expected))
}

pub(crate) fn update_pk_map_for_row(
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

pub(crate) fn collect_inbound_foreign_keys(
    catalog: &Catalog,
    schema: &str,
    table: &str,
) -> Vec<InboundForeignKey> {
    let Some(parent_meta) = catalog.get_table(schema, table) else {
        return Vec::new();
    };
    let parent_id = parent_meta.id;
    let mut out = Vec::new();
    for meta in catalog.tables_by_id.values() {
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

pub(crate) fn drop_inbound_foreign_keys(
    catalog: &mut Catalog,
    tables: &mut HashMap<TableId, Table>,
    inbound: &[InboundForeignKey],
) -> anyhow::Result<()> {
    for fk in inbound {
        if let Some(meta) = catalog.get_table_mut_by_id(&fk.table_id) {
            meta.foreign_keys
                .retain(|existing| existing.name != fk.fk.name);
        }
        if let Some(table) = tables.get_mut(&fk.table_id) {
            table
                .fk_rev
                .retain(|(parent_id, _), _| *parent_id != fk.fk.referenced_table);
        }
    }
    Ok(())
}

pub(crate) fn ensure_no_inbound_refs(
    tables: &HashMap<TableId, Table>,
    schema: &str,
    table: &str,
    parent_id: TableId,
    inbound: &[InboundForeignKey],
    parent_row: &[Value],
    visibility: &VisibilityContext,
) -> anyhow::Result<()> {
    for fk in inbound {
        if fk.fk.on_delete != ReferentialAction::Restrict {
            continue;
        }
        let Some(key_values) = build_referenced_parent_key(parent_row, &fk.fk) else {
            continue;
        };
        let child = match tables.get(&fk.table_id) {
            Some(t) => t,
            None => {
                return Err(sql_err(
                    "XX000",
                    format!("missing storage for table id {}", fk.table_id),
                ));
            }
        };
        let map_key = (parent_id, key_values);
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

pub(crate) fn add_fk_rev_entries(
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

pub(crate) fn remove_fk_rev_entries(
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
