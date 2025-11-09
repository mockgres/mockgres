use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};

use crate::catalog::TableId;
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

#[derive(Debug, Default)]
pub struct SessionState {
    pub current_tx: Option<TxId>,
    pub statement_xid: Option<TxId>,
    #[allow(dead_code)]
    pub changes: TxnChanges,
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
