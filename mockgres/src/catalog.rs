use std::collections::HashMap;
use crate::engine::{Column};

pub type SchemaName = String;
pub type TableName = String;
pub type TableId = u64;

#[derive(Clone, Debug)]
pub struct IndexMeta {
    pub name: String,
    pub columns: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub id: TableId,
    pub name: TableName,
    pub columns: Vec<Column>,
    pub pk: Option<Vec<usize>>,
    pub indexes: Vec<IndexMeta>,
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub tables: HashMap<TableName, TableMeta>,
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub schemas: HashMap<SchemaName, Schema>,
}

impl Catalog {
    pub fn ensure_schema(&mut self, s: &str) {
        self.schemas.entry(s.to_string()).or_default();
    }
    pub fn get_table(&self, schema: &str, name: &str) -> Option<&TableMeta> {
        self.schemas.get(schema).and_then(|sc| sc.tables.get(name))
    }
}
