use crate::catalog::{Catalog, TableId, TableMeta};
use crate::engine::{
    BoolExpr, Column, DataType, ScalarExpr, SqlError, Value, eval_bool_expr, eval_scalar_expr,
};
use crate::storage::{Row, RowKey, Table};
use crate::types::{parse_bytea_text, parse_date_str, parse_timestamp_str};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum CellInput {
    Value(Value),
    Default,
}

fn sql_err(code: &'static str, msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(SqlError::new(code, msg.into()))
}

#[derive(Clone, Debug)]
struct DbSnapshot {
    catalog: Catalog,
    tables: HashMap<TableId, Table>,
    next_tid: u64,
}

#[derive(Debug)]
pub struct Db {
    pub catalog: Catalog,
    pub tables: HashMap<TableId, Table>,
    pub next_tid: u64,
    txn_stack: Vec<DbSnapshot>,
}

impl Default for Db {
    fn default() -> Self {
        let mut catalog = Catalog::default();
        catalog.ensure_schema("public");
        Self {
            catalog,
            tables: HashMap::new(),
            next_tid: 1,
            txn_stack: Vec::new(),
        }
    }
}

impl Db {
    pub fn begin_transaction(&mut self) -> anyhow::Result<()> {
        if self.in_transaction() {
            return Err(sql_err("25001", "transaction already in progress"));
        }
        let snapshot = self.snapshot();
        self.txn_stack.push(snapshot);
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> anyhow::Result<()> {
        if self.txn_stack.pop().is_none() {
            return Err(sql_err("25P01", "no transaction in progress"));
        }
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> anyhow::Result<()> {
        let Some(snapshot) = self.txn_stack.pop() else {
            return Err(sql_err("25P01", "no transaction in progress"));
        };
        self.catalog = snapshot.catalog;
        self.tables = snapshot.tables;
        self.next_tid = snapshot.next_tid;
        Ok(())
    }

    pub fn in_transaction(&self) -> bool {
        !self.txn_stack.is_empty()
    }

    fn snapshot(&self) -> DbSnapshot {
        DbSnapshot {
            catalog: self.catalog.clone(),
            tables: self.tables.clone(),
            next_tid: self.next_tid,
        }
    }

    pub fn create_table(
        &mut self,
        schema: &str,
        name: &str,
        cols: Vec<(String, DataType, bool, Option<Value>)>,
        pk_names: Option<Vec<String>>,
    ) -> anyhow::Result<TableId> {
        self.catalog.ensure_schema(schema);
        if self.catalog.get_table(schema, name).is_some() {
            return Err(sql_err(
                "42P07",
                format!("table already exists: {schema}.{name}"),
            ));
        }

        let id = self.next_tid;
        self.next_tid += 1;

        let mut columns: Vec<Column> = Vec::with_capacity(cols.len());
        for (n, t, nullable, default_raw) in cols.into_iter() {
            let mut default_value = None;
            if let Some(def) = default_raw {
                let cast = cast_value_to_type(def, &t)?;
                if matches!(cast, Value::Null) && !nullable {
                    return Err(sql_err(
                        "23502",
                        format!("default NULL not allowed for NOT NULL column {n}"),
                    ));
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
                        .ok_or_else(|| sql_err("42703", format!("unknown pk column: {n}")))?;
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
            let cast = cast_value_to_type(def, &data_type)?;
            if matches!(cast, Value::Null) && !nullable {
                return Err(sql_err(
                    "23502",
                    format!("default NULL not allowed for NOT NULL column {col_name}"),
                ));
            }
            default_value = Some(cast);
        } else if !nullable {
            return Err(sql_err(
                "23502",
                format!("column {col_name} must have a default or allow NULLs"),
            ));
        }
        let (table_id, column_exists) = {
            let meta = self
                .catalog
                .get_table(schema, name)
                .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
            let exists = meta.columns.iter().any(|c| c.name == col_name);
            (meta.id, exists)
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
        let append_value = default_value.clone().unwrap_or(Value::Null);
        {
            let table = self.tables.get_mut(&table_id).ok_or_else(|| {
                sql_err("XX000", format!("missing storage for table id {table_id}"))
            })?;
            for row in table.rows_by_key.values_mut() {
                row.push(append_value.clone());
            }
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?;
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
                .pk
                .as_ref()
                .map(|pk| pk.contains(&idx))
                .unwrap_or(false)
            {
                return Err(sql_err(
                    "2BP01",
                    format!("cannot drop primary key column {column}"),
                ));
            }
            (meta.id, idx)
        };
        if let Some(table) = self.tables.get_mut(&table_id) {
            for row in table.rows_by_key.values_mut() {
                if row.len() != drop_idx + 1 {
                    return Err(sql_err(
                        "XX000",
                        format!("row length mismatch while dropping column {column}"),
                    ));
                }
                row.pop();
            }
        } else {
            return Err(sql_err(
                "XX000",
                format!("missing storage for table id {table_id}"),
            ));
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(name)
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
    ) -> anyhow::Result<()> {
        if columns.is_empty() {
            return Err(sql_err("0A000", "index must reference at least one column"));
        }
        let schema_entry = self
            .catalog
            .schemas
            .get_mut(schema)
            .ok_or_else(|| sql_err("3F000", format!("no such schema {schema}")))?;
        let table_meta = schema_entry
            .tables
            .get_mut(table)
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
        });
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        schema: &str,
        index_name: &str,
        if_exists: bool,
    ) -> anyhow::Result<()> {
        let Some(schema_entry) = self.catalog.schemas.get_mut(schema) else {
            return if if_exists {
                Ok(())
            } else {
                Err(sql_err("3F000", format!("no such schema {schema}")))
            };
        };
        let mut removed = false;
        for table_meta in schema_entry.tables.values_mut() {
            if let Some(pos) = table_meta
                .indexes
                .iter()
                .position(|idx| idx.name == index_name)
            {
                table_meta.indexes.remove(pos);
                removed = true;
                break;
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

    pub fn resolve_table_mut(
        &mut self,
        schema: &str,
        name: &str,
    ) -> anyhow::Result<(&TableMeta, &mut Table)> {
        let tm = self
            .catalog
            .get_table(schema, name)
            .ok_or_else(|| sql_err("42P01", format!("no such table {schema}.{name}")))?
            .clone();
        let t = self
            .tables
            .get_mut(&tm.id)
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", tm.id)))?;
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
                return Err(sql_err(
                    "21P01",
                    format!(
                        "insert has wrong number of values at row {}: expected {}, got {}",
                        ridx + 1,
                        ncols,
                        r.len()
                    ),
                ));
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
                return Err(sql_err(
                    "23505",
                    "duplicate key value violates unique constraint",
                ));
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
            .ok_or_else(|| sql_err("XX000", format!("missing storage for table id {}", tm.id)))?;

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
                    return Err(sql_err(
                        "0A000",
                        "updating primary key columns is not supported",
                    ));
                }
            }
        }
        let mut count = 0usize;
        for row in table.rows_by_key.values_mut() {
            let passes = if let Some(expr) = filter {
                eval_bool_expr(row, expr, params)
                    .map_err(|e| sql_err("XX000", e.to_string()))?
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
                    .map_err(|e| sql_err("XX000", e.to_string()))?;
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
                    .map_err(|e| sql_err("XX000", e.to_string()))?
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
            return Err(sql_err("23502", format!("column {} is not null", col.name)));
        }
        if let Some(pkpos) = &meta.pk {
            if pkpos.contains(&idx) {
                return Err(sql_err(
                    "23502",
                    format!("primary key column {} cannot be null", col.name),
                ));
            }
        }
        return Ok(Value::Null);
    }
    let coerced = cast_value_to_type(val, &col.data_type).map_err(|e| {
        if let Some(sql) = e.downcast_ref::<SqlError>() {
            sql_err(
                sql.code,
                format!("column {} (index {}): {}", col.name, idx, sql.message),
            )
        } else {
            sql_err(
                "42804",
                format!("column {} (index {}): {}", col.name, idx, e),
            )
        }
    })?;
    Ok(coerced)
}

fn cast_value_to_type(val: Value, target: &DataType) -> anyhow::Result<Value> {
    match (target, val) {
        (DataType::Int4, Value::Int64(v)) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                return Err(sql_err("22003", "value out of range for int4"));
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
            let days = parse_date_str(&s).map_err(|e| sql_err("22007", e))?;
            Ok(Value::Date(days))
        }
        (DataType::Timestamp, Value::TimestampMicros(m)) => Ok(Value::TimestampMicros(m)),
        (DataType::Timestamp, Value::Text(s)) => {
            let micros = parse_timestamp_str(&s).map_err(|e| sql_err("22007", e))?;
            Ok(Value::TimestampMicros(micros))
        }
        (DataType::Bytea, Value::Bytes(bytes)) => Ok(Value::Bytes(bytes)),
        (DataType::Bytea, Value::Text(s)) => {
            let bytes = parse_bytea_text(&s).map_err(|e| sql_err("22001", e))?;
            Ok(Value::Bytes(bytes))
        }
        (DataType::Text, Value::Bool(b)) => Ok(Value::Text(if b { "t" } else { "f" }.into())),
        (DataType::Text, Value::Int64(i)) => Ok(Value::Text(i.to_string())),
        (DataType::Text, Value::Float64Bits(bits)) => {
            let f = f64::from_bits(bits);
            Ok(Value::Text(f.to_string()))
        }
        (dt, got) => Err(sql_err(
            "42804",
            format!("type mismatch: expected {dt:?}, got {got:?}"),
        )),
    }
}
