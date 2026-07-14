use std::collections::{HashMap, HashSet};

use crate::catalog::TableMeta;
use crate::engine::{
    BoolExpr, CmpOp, DataType, EvalContext, ScalarExpr, Value, coerce_value_to_type,
    eval_scalar_expr,
};
use crate::storage::{Row, RowId, RowKey, Table};
use crate::txn::VisibilityContext;

use super::{Db, select_visible_version, sql_err};

type IndexedScan = (Vec<Row>, Vec<RowId>);

fn hash_lookup_safe(data_type: &DataType) -> bool {
    !matches!(data_type, DataType::Float8 | DataType::Circle)
}

fn lookup_row_ids(
    table: &Table,
    meta: &TableMeta,
    equalities: &[(usize, Value)],
) -> Option<Vec<RowId>> {
    let equality_map: HashMap<usize, &Value> = equalities
        .iter()
        .map(|(column, value)| (*column, value))
        .collect();
    let columns = table
        .lookup_maps
        .keys()
        .filter(|columns| {
            columns.iter().all(|column| {
                equality_map.contains_key(column)
                    && meta
                        .columns
                        .get(*column)
                        .is_some_and(|column| hash_lookup_safe(&column.data_type))
            })
        })
        .max_by_key(|columns| columns.len())?;
    let key = columns
        .iter()
        .map(|column| (*equality_map[column]).clone())
        .collect::<Vec<_>>();
    let mut row_ids = table
        .lookup_maps
        .get(columns)
        .and_then(|index| index.get(&key))
        .map(|matches| matches.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    row_ids.sort_unstable();
    Some(row_ids)
}

pub(crate) fn indexed_filter_row_ids(
    table: &Table,
    meta: &TableMeta,
    filter: &BoolExpr,
    params: &[Value],
    ctx: &EvalContext,
) -> Option<Vec<RowId>> {
    let mut equalities = Vec::new();
    collect_filter_equalities(filter, params, ctx, &mut equalities);
    let equalities = equalities
        .into_iter()
        .filter_map(|(column, value)| {
            meta.columns.get(column).and_then(|column_meta| {
                coerce_value_to_type(value, &column_meta.data_type, &ctx.time_zone)
                    .ok()
                    .map(|value| (column, value))
            })
        })
        .collect::<Vec<_>>();
    lookup_row_ids(table, meta, &equalities)
}

fn collect_filter_equalities(
    expr: &BoolExpr,
    params: &[Value],
    ctx: &EvalContext,
    out: &mut Vec<(usize, Value)>,
) {
    match expr {
        BoolExpr::Comparison {
            lhs,
            op: CmpOp::Eq,
            rhs,
        } => {
            if let ScalarExpr::ColumnIdx(column) = lhs
                && let Ok(value) = eval_scalar_expr(&[], rhs, params, ctx)
                && !matches!(value, Value::Null)
            {
                out.push((*column, value));
            } else if let ScalarExpr::ColumnIdx(column) = rhs
                && let Ok(value) = eval_scalar_expr(&[], lhs, params, ctx)
                && !matches!(value, Value::Null)
            {
                out.push((*column, value));
            }
        }
        BoolExpr::And(exprs) => {
            for expr in exprs {
                collect_filter_equalities(expr, params, ctx, out);
            }
        }
        _ => {}
    }
}

fn indexed_prefixes(meta: &TableMeta) -> HashSet<Vec<usize>> {
    let mut prefixes = HashSet::new();
    if let Some(primary_key) = &meta.primary_key {
        for len in 1..=primary_key.columns.len() {
            prefixes.insert(primary_key.columns[..len].to_vec());
        }
    }
    for index in &meta.indexes {
        for len in 1..=index.columns.len() {
            prefixes.insert(index.columns[..len].to_vec());
        }
    }
    prefixes
}

pub(crate) fn add_lookup_entries(table: &mut Table, row_id: RowId, row: &[Value]) {
    for columns in &table.lookup_columns {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            let value = row.get(*column).cloned().unwrap_or(Value::Null);
            if matches!(value, Value::Null) {
                values.clear();
                break;
            }
            values.push(value);
        }
        if values.len() != columns.len() {
            continue;
        }
        table
            .lookup_maps
            .entry(columns.clone())
            .or_default()
            .entry(values)
            .or_default()
            .insert(row_id);
    }
}

pub(crate) fn rebuild_lookup_maps(table: &mut Table, meta: &TableMeta) -> anyhow::Result<()> {
    table.lookup_maps.clear();
    table.lookup_columns = indexed_prefixes(meta).into_iter().collect();
    table.lookup_columns.sort();
    let rows = table
        .rows_by_key
        .iter()
        .map(|(key, versions)| {
            let RowKey::RowId(row_id) = key else {
                return Err(sql_err(
                    "XX000",
                    "expected physical RowId key while rebuilding indexes",
                ));
            };
            Ok((
                *row_id,
                versions
                    .iter()
                    .map(|version| version.data.clone())
                    .collect::<Vec<_>>(),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (row_id, versions) in rows {
        for row in versions {
            add_lookup_entries(table, row_id, &row);
        }
    }
    Ok(())
}

impl Db {
    pub(crate) fn scan_bound_positions_indexed(
        &self,
        schema: &str,
        name: &str,
        positions: &[usize],
        equalities: &[(usize, Value)],
        visibility: &VisibilityContext,
    ) -> anyhow::Result<Option<IndexedScan>> {
        let meta = self.resolve_table(schema, name)?;
        let table = self
            .tables
            .get(&meta.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", meta.id)))?;
        let Some(row_ids) = lookup_row_ids(table, meta, equalities) else {
            return Ok(None);
        };

        let mut out_rows = Vec::with_capacity(row_ids.len());
        let mut visible_row_ids = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            let Some(version) = table
                .rows_by_key
                .get(&RowKey::RowId(row_id))
                .and_then(|versions| select_visible_version(versions, visibility))
            else {
                continue;
            };
            out_rows.push(
                positions
                    .iter()
                    .map(|position| version.data[*position].clone())
                    .collect(),
            );
            visible_row_ids.push(row_id);
        }
        Ok(Some((out_rows, visible_row_ids)))
    }
}
