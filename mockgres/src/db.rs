use crate::catalog::{Catalog, TableId, TableMeta};
use crate::engine::{
    BoolExpr, Column, DataType, ScalarExpr, Value, eval_bool_expr, eval_scalar_expr,
};
use crate::storage::{Row, RowKey, Table};
use crate::types::{parse_bytea_text, parse_date_str, parse_timestamp_str};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum CellInput {
    Value(Value),
    Default,
}

#[derive(Debug)]
pub struct Db {
    pub catalog: Catalog,
    pub tables: HashMap<TableId, Table>,
    pub next_tid: u64,
}

impl Default for Db {
    fn default() -> Self {
        let mut catalog = Catalog::default();
        catalog.ensure_schema("public");
        Self {
            catalog,
            tables: HashMap::new(),
            next_tid: 1,
        }
    }
}

impl Db {
    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<(String, DataType, bool, Option<Value>)>,
        pk_names: Option<Vec<String>>,
    ) -> anyhow::Result<TableId> {
        self.catalog.ensure_schema(schema);
        if self.catalog.get_table(schema, name).is_some() {
            anyhow::bail!("table already exists: {schema}.{name}");
        }

        let id = self.next_tid;
        self.next_tid += 1;

        let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
        for (n, t, nullable, default_raw) in cols.into_iter() {
            let mut default_value = None;
            if let Some(def) = default_raw {
                let cast =
                    cast_value_to_type(def, &t).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                if matches!(cast, Value::Null) && !nullable {
                    anyhow::bail!("default NULL not allowed for NOT NULL column {}", n);
                }
                default_value = Some(cast);
            }
            columns.push(Column {
                name: n,
                data_type: t,
                nullable,
                default: default_value,
            });
        }

        // map pk names -> positions
        let pk: Option<Vec<usize>> = match pk_names {
            None => None,
            Some(ns) => {
                let mut pos = Vec::with_capacity(ns.len());
                for n in ns {
                    let i = columns
                        .iter()
                        .position(|c| c.name == n)
                        .ok_or_else(|| anyhow::anyhow!("unknown pk column: {n}"))?;
                    pos.push(i);
                }
                Some(pos)
            }
        };

        let tm = TableMeta {
            id,
            name: name.to_string(),
            columns: columns.clone(),
            pk,
            indexes: vec![],
        };

        self.catalog
            .schemas
            .get_mut(schema)
            .expect("schema exists after ensure")
            .tables
            .insert(name.to_string(), tm);

        self.tables.insert(id, Table::default());
        Ok(id)
    }

    pub fn alter_table_add_column(
        &mut self,
        schema: &str,
        name: &str,
        column: (String, DataType, bool, Option<Value>),
        if_not_exists: bool,
    ) -> anyhow::Result<()> {
        let (col_name, data_type, nullable, default_raw) = column;
        let mut default_value = None;
        if let Some(def) = default_raw {
            let cast =
                cast_value_to_type(def, &data_type).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            if matches!(cast, Value::Null) && !nullable {
                anyhow::bail!("default NULL not allowed for NOT NULL column {}", col_name);
            }
            default_value = Some(cast);
        } else if !nullable {
            anyhow::bail!("column {} must have a default or allow NULLs", col_name);
        }
        let (table_id, column_exists) = {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))?;
            let exists = meta.columns.iter().any(|c| c.name == col_name);
            (meta.id, exists)
        };
        if column_exists {
            if if_not_exists {
                return Ok(());
            }
            anyhow::bail!("column {} already exists", col_name);
        }
        let append_value = default_value.clone().unwrap_or(Value::Null);
        {
            let table = self
                .tables
                .get_mut(&table_id)
                .ok_or_else(|| anyhow::anyhow!("missing storage for table id {}", table_id))?;
            for row in table.rows_by_key.values_mut() {
                row.push(append_value.clone());
            }
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| anyhow::anyhow!("no such schema {schema}"))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))?;
        table_meta.columns.push(Column {
            name: col_name,
            data_type,
            nullable,
            default: default_value,
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
                .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))?;
            let Some(idx) = meta.columns.iter().position(|c| c.name == column) else {
                if if_exists {
                    return Ok(());
                } else {
                    anyhow::bail!("column {} does not exist", column);
                }
            };
            if idx != meta.columns.len() - 1 {
                anyhow::bail!("can only drop the last column ({} is at position {})", column, idx);
            }
            if meta
                .pk
                .as_ref()
                .map(|pk| pk.contains(&idx))
                .unwrap_or(false)
            {
                anyhow::bail!("cannot drop primary key column {}", column);
            }
            (meta.id, idx)
        };
        if let Some(table) = self.tables.get_mut(&table_id) {
            for row in table.rows_by_key.values_mut() {
                if row.len() != drop_idx + 1 {
                    anyhow::bail!("row length mismatch while dropping column {}", column);
                }
                row.pop();
            }
        } else {
            anyhow::bail!("missing storage for table id {}", table_id);
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| anyhow::anyhow!("no such schema {schema}"))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))?;
        table_meta.columns.pop();
        Ok(())
    }

    pub fn resolve_table(&self, schema: &str, name: &str) -> anyhow::Result<&TableMeta> {
        self.catalog
            .get_table(schema, name)
            .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))
    }

    pub fn resolve_table_mut(
        &mut self,
        schema: &str,
        name: &str,
    ) -> anyhow::Result<(&TableMeta, &mut Table)> {
        let tm = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| anyhow::anyhow!("no such table {schema}.{name}"))?
            .clone();
        let t = self
            .tables
            .get_mut(&tm.id)
            .ok_or_else(|| anyhow::anyhow!("missing storage for table id {}", tm.id))?;
        Ok((self.catalog.get_table(schema, name).unwrap(), t))
    }

    pub fn insert_full_rows(
        &mut self,
        schema: &str,
        name: &str,
        mut rows: Vec<Vec<CellInput>>,
    ) -> anyhow::Result<usize> {
        let (meta, tab) = self.resolve_table_mut(schema, name)?;
        let ncols = meta.columns.len();
        let mut count = 0usize;

        for (ridx, r) in rows.drain(..).enumerate() {
            // arity: number of values must match table columns
            if r.len() != ncols {
                anyhow::bail!(
                    "insert has wrong number of values at row {}: expected {}, got {}",
                    ridx + 1,
                    ncols,
                    r.len()
                );
            }

            // coerce and validate each cell to the target column type
            let mut out: Row = Vec::with_capacity(ncols);
            for (i, cell) in r.into_iter().enumerate() {
                let col = &meta.columns[i];
                let value = match cell {
                    CellInput::Value(v) => v,
                    CellInput::Default => col.default.clone().unwrap_or(Value::Null),
                };
                let coerced = coerce_value_for_column(value, col, i, meta)?;
                out.push(coerced);
            }

            // build the row key (pk or hidden rowid)
            let key = if let Some(pkpos) = &meta.pk {
                let mut vals = Vec::with_capacity(pkpos.len());
                for i in pkpos {
                    vals.push(out[*i].clone());
                }
                RowKey::Pk(vals)
            } else {
                // use per-table allocator for hidden rowid
                let id = tab.alloc_rowid();
                RowKey::Hidden(id)
            };

            // enforce pk uniqueness when present
            if tab.rows_by_key.contains_key(&key) {
                anyhow::bail!("duplicate key value violates unique constraint");
            }

            // finally insert
            tab.insert(key, out);
            count += 1;
        }

        Ok(count)
    }

    pub fn scan_bound_positions(
        &self,
        schema: &str,
        name: &str,
        positions: &[usize],
    ) -> anyhow::Result<(Vec<Row>, Vec<(usize, String)>)> {
        let tm = self.resolve_table(schema, name)?;
        let table = self
            .tables
            .get(&tm.id)
            .ok_or_else(|| anyhow::anyhow!("missing storage for table id {}", tm.id))?;

        let mut out_rows = Vec::new();
        for (_k, r) in table.scan_all() {
            out_rows.push(positions.iter().map(|i| r[*i].clone()).collect());
        }
        let cols = positions
            .iter()
            .map(|i| (*i, tm.columns[*i].name.clone()))
            .collect();
        Ok((out_rows, cols))
    }

    pub fn update_rows(
        &mut self,
        schema: &str,
        name: &str,
        sets: &[(usize, ScalarExpr)],
        filter: Option<&BoolExpr>,
        params: &[Value],
    ) -> anyhow::Result<usize> {
        let (meta, table) = self.resolve_table_mut(schema, name)?;
        if sets.is_empty() {
            return Ok(0);
        }
        if let Some(pkpos) = &meta.pk {
            for (idx, _) in sets {
                if pkpos.iter().any(|pos| pos == idx) {
                    anyhow::bail!("updating primary key columns is not supported");
                }
            }
        }
        let mut count = 0usize;
        for row in table.rows_by_key.values_mut() {
            let passes = if let Some(expr) = filter {
                eval_bool_expr(row, expr, params)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?
                    .unwrap_or(false)
            } else {
                true
            };
            if !passes {
                continue;
            }
            let original = row.clone();
            let mut updated = original.clone();
            for (idx, expr) in sets {
                let value = eval_scalar_expr(&original, expr, params)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let coerced = coerce_value_for_column(value, &meta.columns[*idx], *idx, meta)?;
                updated[*idx] = coerced;
            }
            *row = updated;
            count += 1;
        }
        Ok(count)
    }

    pub fn delete_rows(
        &mut self,
        schema: &str,
        name: &str,
        filter: Option<&BoolExpr>,
        params: &[Value],
    ) -> anyhow::Result<usize> {
        let (_meta, table) = self.resolve_table_mut(schema, name)?;
        let mut to_remove = Vec::new();
        if let Some(expr) = filter {
            for (key, row) in table.rows_by_key.iter() {
                let passes = eval_bool_expr(row, expr, params)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?
                    .unwrap_or(false);
                if passes {
                    to_remove.push(key.clone());
                }
            }
        } else {
            to_remove.extend(table.rows_by_key.keys().cloned());
        }
        let count = to_remove.len();
        for key in to_remove {
            table.rows_by_key.remove(&key);
        }
        Ok(count)
    }
}

fn coerce_value_for_column(
    val: Value,
    col: &Column,
    idx: usize,
    meta: &TableMeta,
) -> anyhow::Result<Value> {
    if let Value::Null = val {
        if !col.nullable {
            anyhow::bail!("column {} is not null", col.name);
        }
        if let Some(pkpos) = &meta.pk {
            if pkpos.contains(&idx) {
                anyhow::bail!("primary key column {} cannot be null", col.name);
            }
        }
        return Ok(Value::Null);
    }
    let coerced = cast_value_to_type(val, &col.data_type)
        .map_err(|e| anyhow::anyhow!("column {} (index {}): {}", col.name, idx, e))?;
    Ok(coerced)
}

fn cast_value_to_type(val: Value, target: &DataType) -> anyhow::Result<Value> {
    match (target, val) {
        (DataType::Int4, Value::Int64(v)) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                anyhow::bail!("value out of range for int4");
            }
            Ok(Value::Int64(v))
        }
        (DataType::Int8, Value::Int64(v)) => Ok(Value::Int64(v)),
        (DataType::Float8, Value::Float64Bits(bits)) => Ok(Value::Float64Bits(bits)),
        (DataType::Float8, Value::Int64(v)) => Ok(Value::from_f64(v as f64)),
        (DataType::Text, Value::Text(s)) => Ok(Value::Text(s)),
        (DataType::Bool, Value::Bool(b)) => Ok(Value::Bool(b)),
        (DataType::Date, Value::Date(d)) => Ok(Value::Date(d)),
        (DataType::Date, Value::Text(s)) => {
            let days = parse_date_str(&s).map_err(|e| anyhow::anyhow!(e))?;
            Ok(Value::Date(days))
        }
        (DataType::Timestamp, Value::TimestampMicros(m)) => Ok(Value::TimestampMicros(m)),
        (DataType::Timestamp, Value::Text(s)) => {
            let micros = parse_timestamp_str(&s).map_err(|e| anyhow::anyhow!(e))?;
            Ok(Value::TimestampMicros(micros))
        }
        (DataType::Bytea, Value::Bytes(bytes)) => Ok(Value::Bytes(bytes)),
        (DataType::Bytea, Value::Text(s)) => {
            let bytes = parse_bytea_text(&s).map_err(|e| anyhow::anyhow!(e))?;
            Ok(Value::Bytes(bytes))
        }
        (DataType::Text, Value::Bool(b)) => Ok(Value::Text(if b { "t" } else { "f" }.into())),
        (DataType::Text, Value::Int64(i)) => Ok(Value::Text(i.to_string())),
        (DataType::Text, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            Ok(Value::Text(f.to_string()))
        }
        (dt, got) => anyhow::bail!("type mismatch: expected {:?}, got {:?}", dt, got),
    }
}
