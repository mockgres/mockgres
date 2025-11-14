use crate::engine::{Column, ReferentialAction};
use std::collections::HashMap;
use std::fmt;

pub type TableName = String;
pub type SchemaId = u32;
pub type ColId = usize;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaName(pub String);

impl SchemaName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    pub schema: Option<SchemaName>,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TableId {
    pub schema_id: SchemaId,
    pub rel_id: u32,
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.schema_id, self.rel_id)
    }
}

#[derive(Clone, Debug)]
pub struct IndexMeta {
    pub name: String,
    pub columns: Vec<usize>,
    pub unique: bool,
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
    pub schema: SchemaName,
    pub name: TableName,
    pub columns: Vec<Column>,
    pub primary_key: Option<PrimaryKeyMeta>,
    pub indexes: Vec<IndexMeta>,
    pub foreign_keys: Vec<ForeignKeyMeta>,
}

#[derive(Clone, Debug)]
pub struct SchemaEntry {
    pub id: SchemaId,
    pub name: SchemaName,
    pub objects: HashMap<String, TableId>,
}

#[derive(Clone, Debug)]
pub struct Catalog {
    pub schemas_by_name: HashMap<String, SchemaId>,
    pub schemas: HashMap<SchemaId, SchemaEntry>,
    pub tables_by_id: HashMap<TableId, TableMeta>,
    next_schema_id: SchemaId,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            schemas_by_name: HashMap::new(),
            schemas: HashMap::new(),
            tables_by_id: HashMap::new(),
            next_schema_id: 1,
        }
    }
}

impl Catalog {
    pub fn ensure_schema(&mut self, s: &str) -> SchemaId {
        if let Some(id) = self.schemas_by_name.get(s) {
            return *id;
        }
        let id = self.next_schema_id;
        self.next_schema_id = self
            .next_schema_id
            .checked_add(1)
            .expect("schema id overflow");
        let entry = SchemaEntry {
            id,
            name: SchemaName::new(s),
            objects: HashMap::new(),
        };
        self.schemas_by_name.insert(s.to_string(), id);
        self.schemas.insert(id, entry);
        id
    }

    pub fn schema_id(&self, name: &str) -> Option<SchemaId> {
        self.schemas_by_name.get(name).copied()
    }

    pub fn schema_name(&self, id: SchemaId) -> Option<&SchemaName> {
        self.schemas.get(&id).map(|entry| &entry.name)
    }

    pub fn schema_entry(&self, name: &str) -> Option<&SchemaEntry> {
        let id = self.schema_id(name)?;
        self.schemas.get(&id)
    }

    pub fn schema_entry_mut(&mut self, name: &str) -> Option<&mut SchemaEntry> {
        let id = self.schema_id(name)?;
        self.schemas.get_mut(&id)
    }

    pub fn table_id(&self, schema: &str, name: &str) -> Option<TableId> {
        let schema_id = self.schema_id(schema)?;
        self.schemas.get(&schema_id)?.objects.get(name).copied()
    }

    pub fn get_table(&self, schema: &str, name: &str) -> Option<&TableMeta> {
        let schema_id = self.schema_id(schema)?;
        let schema_entry = self.schemas.get(&schema_id)?;
        let table_id = schema_entry.objects.get(name)?;
        self.tables_by_id.get(table_id)
    }

    pub fn get_table_mut_by_id(&mut self, id: &TableId) -> Option<&mut TableMeta> {
        self.tables_by_id.get_mut(id)
    }

    pub fn table_meta_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableMeta> {
        let schema_id = self.schema_id(schema)?;
        let table_id = self.schemas.get(&schema_id)?.objects.get(name).copied()?;
        self.tables_by_id.get_mut(&table_id)
    }

    pub fn insert_table(&mut self, schema_id: SchemaId, name: &str, meta: TableMeta) {
        let entry = self
            .schemas
            .get_mut(&schema_id)
            .expect("schema must exist before inserting table");
        entry
            .objects
            .insert(name.to_string(), meta.id)
            .map(|existing| {
                panic!("table {name} already exists with id {:?}", existing);
            });
        self.tables_by_id.insert(meta.id, meta);
    }

    pub fn remove_table(&mut self, schema_id: SchemaId, name: &str) -> Option<TableMeta> {
        let entry = self.schemas.get_mut(&schema_id)?;
        let table_id = entry.objects.remove(name)?;
        self.tables_by_id.remove(&table_id)
    }

    pub fn drop_schema_entry(&mut self, schema_id: SchemaId) {
        if let Some(entry) = self.schemas.remove(&schema_id) {
            self.schemas_by_name.remove(entry.name.as_str());
        }
    }

    pub fn rename_schema_entry(&mut self, schema_id: SchemaId, new_name: SchemaName) {
        if let Some(entry) = self.schemas.get_mut(&schema_id) {
            self.schemas_by_name.remove(entry.name.as_str());
            entry.name = new_name.clone();
            self.schemas_by_name
                .insert(entry.name.as_str().to_string(), schema_id);
        }
    }
}
