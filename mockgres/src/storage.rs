use crate::catalog::TableId;
use crate::engine::Value;
use crate::txn::TxId;
use std::collections::{HashMap, HashSet};

pub type RowId = u64;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum RowKey {
    RowId(RowId),
    Primary(Vec<Value>),
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug)]
pub struct VersionedRow {
    pub xmin: TxId,
    pub xmax: Option<TxId>,
    pub data: Row,
}

#[derive(Clone, Debug)]
pub struct IdentityRuntime {
    pub next_value: i128,
    pub increment_by: i128,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub rows_by_key: HashMap<RowKey, Vec<VersionedRow>>,
    pub next_rowid: RowId,
    pub pk_map: Option<HashMap<RowKey, RowId>>,
    pub fk_rev: HashMap<(TableId, Vec<Value>), HashSet<RowId>>,
    pub identities: Vec<Option<IdentityRuntime>>,
    // key = index name, value = map from column values to owning rowid
    pub unique_maps: HashMap<String, HashMap<Vec<Value>, RowId>>,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            rows_by_key: HashMap::new(),
            next_rowid: 1,
            pk_map: None,
            fk_rev: HashMap::new(),
            identities: Vec::new(),
            unique_maps: HashMap::new(),
        }
    }
}

impl Table {
    pub fn with_pk(has_pk: bool, identities: Vec<Option<IdentityRuntime>>) -> Self {
        let mut tbl = Self::default();
        if has_pk {
            tbl.pk_map = Some(HashMap::new());
        }
        tbl.identities = identities;
        tbl.unique_maps = HashMap::new();
        tbl
    }

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
