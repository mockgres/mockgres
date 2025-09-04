use std::collections::HashMap;
use crate::catalog::{Catalog, TableMeta, TableId};
use crate::engine::{Column, DataType, Value};
use crate::storage::{Row, RowKey, Table};

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
        cols: Vec<(String, DataType, bool)>,
        pk_names: Option<Vec<String>>,
    ) -> anyhow::Result<TableId> {
        self.catalog.ensure_schema(schema);
        if self.catalog.get_table(schema, name).is_some() {
            anyhow::bail!("table already exists: {schema}.{name}");
        }

        let id = self.next_tid;
        self.next_tid += 1;

        let columns: Vec<Column> = cols
            .into_iter()
            .map(|(n, t, nullable)| Column {
                name: n,
                data_type: t,
                nullable,
            })
            .collect();

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
        mut rows: Vec<Row>,
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
            for (i, (val, col)) in r.into_iter().zip(meta.columns.iter()).enumerate() {
                // handle nulls first
                if let Value::Null = val {
                    // not null?
                    if !col.nullable {
                        anyhow::bail!("column {} is not null", col.name);
                    }
                    // pk columns cannot be null
                    if let Some(pkpos) = &meta.pk {
                        if pkpos.contains(&i) {
                            anyhow::bail!("primary key column {} cannot be null", col.name);
                        }
                    }
                    out.push(Value::Null);
                    continue;
                }

                let coerced = match (&col.data_type, val) {
                    // int4: keep as i64, but enforce i32 range
                    (DataType::Int4, Value::Int64(v)) => {
                        if v < i32::MIN as i64 || v > i32::MAX as i64 {
                            anyhow::bail!(
                                "value out of range for int4 at column {} (index {})",
                                col.name,
                                i
                            );
                        }
                        Value::Int64(v)
                    }
                    // int8: require an int64 value
                    (DataType::Int8, Value::Int64(v)) => Value::Int64(v),

                    // float8: accept float or int (promote int -> float)
                    (DataType::Float8, Value::Float64Bits(bits)) => Value::Float64Bits(bits),
                    (DataType::Float8, Value::Int64(_)) => anyhow::bail!("type mismatch at column {} (index {})", col.name, i),

                    // anything else is a mismatch for now (e.g., float into int)
                    (dt, got) => {
                        anyhow::bail!(
                            "type mismatch at column {} (index {}): expected {:?}, got {:?}",
                            col.name,
                            i,
                            dt,
                            got
                        )
                    }
                };

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
}
