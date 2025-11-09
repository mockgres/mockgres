use crate::engine::Value;
use crate::txn::TxId;
use std::collections::HashMap;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum RowKey {
    Hidden(u64),
    Pk(Vec<Value>),
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug)]
pub struct VersionedRow {
    pub xmin: TxId,
    pub xmax: Option<TxId>,
    pub data: Row,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub rows_by_key: HashMap<RowKey, Vec<VersionedRow>>,
    pub next_rowid: u64,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            rows_by_key: HashMap::new(),
            next_rowid: 1,
        }
    }
}

impl Table {
    pub fn insert(&mut self, k: RowKey, r: VersionedRow) {
        self.rows_by_key.insert(k, vec![r]);
    }
    pub fn scan_all(&self) -> impl Iterator<Item = (&RowKey, &Vec<VersionedRow>)> {
        self.rows_by_key.iter()
    }

    // simple per-table counter for hidden rowids
    pub fn alloc_rowid(&mut self) -> u64 {
        let id = self.next_rowid;
        self.next_rowid += 1;
        id
    }
}
