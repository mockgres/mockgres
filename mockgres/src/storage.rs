use crate::catalog::TableId;
use crate::engine::Value;
use crate::txn::TxId;
use std::collections::{HashMap, HashSet};
use std::slice;

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
    // Monotonic physical insertion order. Stale ids from rolled-back rows are harmless and
    // skipped during scans; retaining them avoids O(n) removals on rollback.
    pub row_order: Vec<RowId>,
    pub next_rowid: RowId,
    pub pk_map: Option<HashMap<RowKey, RowId>>,
    pub fk_rev: HashMap<(TableId, Vec<Value>), HashSet<RowId>>,
    pub identities: Vec<Option<IdentityRuntime>>,
    // key = index name, value = map from column values to owning rowid
    pub unique_maps: HashMap<String, HashMap<Vec<Value>, RowId>>,
    // Append-only lookup maps for every indexed column prefix. Keeping stale row ids lets
    // snapshots find older row versions; callers recheck visibility and predicates.
    pub lookup_maps: HashMap<Vec<usize>, HashMap<Vec<Value>, HashSet<RowId>>>,
    pub lookup_columns: Vec<Vec<usize>>,
}

pub enum CandidateRows<'a> {
    All(std::collections::hash_map::Iter<'a, RowKey, Vec<VersionedRow>>),
    Indexed {
        rows_by_key: &'a HashMap<RowKey, Vec<VersionedRow>>,
        row_ids: slice::Iter<'a, RowId>,
    },
}

impl<'a> Iterator for CandidateRows<'a> {
    type Item = (&'a RowKey, &'a Vec<VersionedRow>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            CandidateRows::All(rows) => rows.next(),
            CandidateRows::Indexed {
                rows_by_key,
                row_ids,
            } => row_ids.find_map(|row_id| rows_by_key.get_key_value(&RowKey::RowId(*row_id))),
        }
    }
}

impl Default for Table {
    fn default() -> Self {
        Self {
            rows_by_key: HashMap::new(),
            row_order: Vec::new(),
            next_rowid: 1,
            pk_map: None,
            fk_rev: HashMap::new(),
            identities: Vec::new(),
            unique_maps: HashMap::new(),
            lookup_maps: HashMap::new(),
            lookup_columns: Vec::new(),
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
        tbl.lookup_maps = HashMap::new();
        tbl.lookup_columns = Vec::new();
        tbl
    }

    pub fn insert(&mut self, k: RowKey, r: VersionedRow) {
        if let RowKey::RowId(row_id) = &k {
            self.row_order.push(*row_id);
        }
        self.rows_by_key.insert(k, vec![r]);
    }
    pub fn scan_all(&self) -> impl Iterator<Item = (&RowKey, &Vec<VersionedRow>)> {
        self.row_order
            .iter()
            .filter_map(|row_id| self.rows_by_key.get_key_value(&RowKey::RowId(*row_id)))
    }

    pub fn scan_candidates<'a>(&'a self, row_ids: Option<&'a [RowId]>) -> CandidateRows<'a> {
        match row_ids {
            Some(row_ids) => CandidateRows::Indexed {
                rows_by_key: &self.rows_by_key,
                row_ids: row_ids.iter(),
            },
            None => CandidateRows::All(self.rows_by_key.iter()),
        }
    }

    // simple per-table counter for hidden rowids
    pub fn alloc_rowid(&mut self) -> u64 {
        let id = self.next_rowid;
        self.next_rowid += 1;
        id
    }
}
