use crate::engine::{Column, ReferentialAction};
use std::collections::HashMap;

pub type SchemaName = String;
pub type TableName = String;
pub type TableId = u64;
pub type ColId = usize;

#[derive(Clone, Debug)]
pub struct IndexMeta {
    pub name: String,
    pub columns: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct ForeignKeyMeta {
    pub name: String,
    pub local_columns: Vec<ColId>,
    pub referenced_table: TableId,
    pub referenced_schema: SchemaName,
    pub referenced_table_name: TableName,
    pub referenced_columns: Vec<ColId>,
    pub on_delete: ReferentialAction,
}

#[derive(Clone, Debug)]
pub struct PrimaryKeyMeta {
    pub name: String,
    pub columns: Vec<ColId>,
}

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub id: TableId,
    pub name: TableName,
    pub columns: Vec<Column>,
    pub primary_key: Option<PrimaryKeyMeta>,
    pub indexes: Vec<IndexMeta>,
    pub foreign_keys: Vec<ForeignKeyMeta>,
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
