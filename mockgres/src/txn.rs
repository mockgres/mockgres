use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::storage::VersionedRow;

pub type TxId = u64;
pub const SYSTEM_TXID: TxId = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    InProgress,
    Committed,
    Aborted,
}

#[derive(Debug, Default)]
pub struct TransactionManager {
    next_xid: AtomicU64,
    // don't evict entries so tuple metadata never references an unknown xid
    statuses: DashMap<TxId, TxnStatus>,
}

impl TransactionManager {
    pub fn new() -> Self {
        let statuses = DashMap::new();
        statuses.insert(SYSTEM_TXID, TxnStatus::Committed);
        Self {
            next_xid: AtomicU64::new(0),
            statuses,
        }
    }

    pub fn allocate(&self) -> TxId {
        self.next_xid.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn snapshot_xid(&self) -> TxId {
        self.next_xid.load(Ordering::SeqCst)
    }

    pub fn set_status(&self, txid: TxId, status: TxnStatus) {
        self.statuses.insert(txid, status);
    }

    pub fn status(&self, txid: TxId) -> Option<TxnStatus> {
        self.statuses.get(&txid).map(|entry| *entry)
    }

    pub fn is_committed(&self, txid: TxId) -> bool {
        if txid == SYSTEM_TXID {
            return true;
        }
        // unknown txids are considered uncommitted
        matches!(self.status(txid), Some(TxnStatus::Committed))
    }
}

pub struct VisibilityContext<'a> {
    txn_manager: &'a TransactionManager,
    pub snapshot_xid: TxId,
    pub current_tx: Option<TxId>,
}

impl<'a> VisibilityContext<'a> {
    pub fn new(
        txn_manager: &'a TransactionManager,
        snapshot_xid: TxId,
        current_tx: Option<TxId>,
    ) -> Self {
        Self {
            txn_manager,
            snapshot_xid,
            current_tx,
        }
    }

    pub fn is_visible(&self, version: &VersionedRow) -> bool {
        if let Some(current) = self.current_tx {
            if version.xmin == current {
                return true;
            }
            if version.xmax == Some(current) {
                return false;
            }
        }

        if !self.txn_manager.is_committed(version.xmin) {
            return false;
        }
        if version.xmin > self.snapshot_xid {
            return false;
        }

        if let Some(xmax) = version.xmax {
            if let Some(current) = self.current_tx && xmax == current {
                return false;
            }
            if self.txn_manager.is_committed(xmax) && xmax <= self.snapshot_xid {
                return false;
            }
        }

        true
    }
}
