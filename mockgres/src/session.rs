use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};

use crate::catalog::{SchemaId, TableId};
use crate::storage::RowKey;
use crate::txn::TxId;

pub type SessionId = i32;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct RowPointer {
    pub table_id: TableId,
    pub key: RowKey,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct TxnChanges {
    pub inserted: Vec<RowPointer>,
    pub updated_old: Vec<RowPointer>,
}

#[derive(Debug)]
pub struct SessionState {
    pub current_tx: Option<TxId>,
    pub statement_xid: Option<TxId>,
    #[allow(dead_code)]
    pub changes: TxnChanges,
    pub next_epoch: u64,
    pub txn_epoch: Option<u64>,
    pub statement_epoch: Option<u64>,
    pub search_path: Vec<SchemaId>,
    pub current_database: Option<String>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            current_tx: None,
            statement_xid: None,
            changes: TxnChanges::default(),
            next_epoch: 1,
            txn_epoch: None,
            statement_epoch: None,
            search_path: Vec::new(),
            current_database: None,
        }
    }
}

#[derive(Debug)]
pub struct Session {
    id: SessionId,
    state: Mutex<SessionState>,
}

impl Session {
    pub fn new(id: SessionId) -> Self {
        Self {
            id,
            state: Mutex::new(SessionState::default()),
        }
    }

    pub fn id(&self) -> SessionId {
        self.id
    }

    #[allow(dead_code)]
    pub fn state(&self) -> MutexGuard<'_, SessionState> {
        self.state.lock()
    }

    pub fn set_statement_xid(&self, xid: TxId) {
        let mut guard = self.state.lock();
        guard.statement_xid = Some(xid);
    }

    #[allow(dead_code)]
    pub fn statement_xid(&self) -> Option<TxId> {
        self.state.lock().statement_xid
    }

    pub fn current_tx(&self) -> Option<TxId> {
        self.state.lock().current_tx
    }

    pub fn set_current_tx(&self, tx: Option<TxId>) {
        let mut guard = self.state.lock();
        guard.current_tx = tx;
    }

    pub fn reset_changes(&self) {
        let mut guard = self.state.lock();
        guard.changes = TxnChanges::default();
    }

    pub fn record_inserts(&self, mut ptrs: Vec<RowPointer>) {
        if ptrs.is_empty() {
            return;
        }
        let mut guard = self.state.lock();
        guard.changes.inserted.append(&mut ptrs);
    }

    pub fn record_touched(&self, mut ptrs: Vec<RowPointer>) {
        if ptrs.is_empty() {
            return;
        }
        let mut guard = self.state.lock();
        guard.changes.updated_old.append(&mut ptrs);
    }

    pub fn take_changes(&self) -> TxnChanges {
        let mut guard = self.state.lock();
        std::mem::take(&mut guard.changes)
    }

    pub fn enter_statement(&self) -> bool {
        let mut guard = self.state.lock();
        if guard.txn_epoch.is_some() {
            return false;
        }
        let epoch = guard.next_epoch;
        guard.next_epoch += 1;
        guard.statement_epoch = Some(epoch);
        true
    }

    pub fn exit_statement(&self) -> Option<u64> {
        let mut guard = self.state.lock();
        guard.statement_epoch.take()
    }

    pub fn current_epoch(&self) -> Option<u64> {
        let guard = self.state.lock();
        guard.txn_epoch.or(guard.statement_epoch)
    }

    pub fn begin_transaction_epoch(&self) -> u64 {
        let mut guard = self.state.lock();
        let epoch = guard.next_epoch;
        guard.next_epoch += 1;
        guard.txn_epoch = Some(epoch);
        epoch
    }

    pub fn end_transaction_epoch(&self) -> Option<u64> {
        let mut guard = self.state.lock();
        guard.txn_epoch.take()
    }

    pub fn search_path(&self) -> Vec<SchemaId> {
        self.state.lock().search_path.clone()
    }

    pub fn set_search_path(&self, path: Vec<SchemaId>) {
        let mut guard = self.state.lock();
        guard.search_path = path;
    }

    pub fn set_database_name(&self, name: String) {
        let mut guard = self.state.lock();
        guard.current_database = Some(name);
    }

    pub fn database_name(&self) -> Option<String> {
        self.state.lock().current_database.clone()
    }
}

#[derive(Debug, Default)]
pub struct SessionManager {
    next_id: AtomicI32,
    sessions: DashMap<SessionId, Arc<Session>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicI32::new(1),
            sessions: DashMap::new(),
        }
    }

    pub fn create_session(&self) -> Arc<Session> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let session = Arc::new(Session::new(id));
        self.sessions.insert(id, session.clone());
        session
    }

    pub fn get(&self, id: SessionId) -> Option<Arc<Session>> {
        self.sessions.get(&id).map(|entry| entry.clone())
    }

    #[allow(dead_code)]
    pub fn remove(&self, id: SessionId) {
        self.sessions.remove(&id);
    }
}
