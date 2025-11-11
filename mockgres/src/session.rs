use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};
use time::OffsetDateTime;

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

#[derive(Clone, Debug)]
pub enum SessionTimeZone {
    Utc,
    FixedOffset { seconds: i32, display: String },
}

impl Default for SessionTimeZone {
    fn default() -> Self {
        SessionTimeZone::Utc
    }
}

impl SessionTimeZone {
    pub fn parse(input: &str) -> Result<Self, String> {
        let trimmed = input.trim();
        if trimmed.eq_ignore_ascii_case("utc") || trimmed.eq_ignore_ascii_case("z") {
            return Ok(SessionTimeZone::Utc);
        }
        let mut chars = trimmed.chars();
        let Some(sign_char) = chars.next() else {
            return Err("invalid time zone value".to_string());
        };
        if sign_char != '+' && sign_char != '-' {
            return Err("invalid time zone offset".to_string());
        }
        let sign = if sign_char == '+' { 1 } else { -1 };
        let rest = chars.as_str();
        let (hour_part, minute_part) = if let Some(colon_idx) = rest.find(':') {
            (&rest[..colon_idx], Some(&rest[colon_idx + 1..]))
        } else {
            (rest, None)
        };
        if hour_part.is_empty() {
            return Err("invalid time zone hour".to_string());
        }
        let hours: i32 = hour_part
            .parse()
            .map_err(|_| "invalid time zone hour".to_string())?;
        if hours.abs() > 15 {
            return Err("time zone hour out of range".to_string());
        }
        let minutes: i32 = match minute_part {
            Some(part) if !part.is_empty() => part
                .parse()
                .map_err(|_| "invalid time zone minute".to_string())?,
            Some(_) => return Err("invalid time zone minute".to_string()),
            None => 0,
        };
        if minutes < 0 || minutes >= 60 {
            return Err("time zone minute out of range".to_string());
        }
        if hours == 15 && minutes > 0 {
            return Err("time zone offset out of range".to_string());
        }
        let seconds = sign * (hours * 3600 + minutes * 60);
        let display = format!(
            "{}{:02}:{:02}",
            if sign >= 0 { '+' } else { '-' },
            hours.abs(),
            minutes.abs()
        );
        Ok(SessionTimeZone::FixedOffset { seconds, display })
    }

    pub fn offset_seconds(&self) -> i32 {
        match self {
            SessionTimeZone::Utc => 0,
            SessionTimeZone::FixedOffset { seconds, .. } => *seconds,
        }
    }

    pub fn display_value(&self) -> &str {
        match self {
            SessionTimeZone::Utc => "UTC",
            SessionTimeZone::FixedOffset { display, .. } => display.as_str(),
        }
    }

    pub fn offset_string(&self) -> String {
        match self {
            SessionTimeZone::Utc => "+00:00".to_string(),
            SessionTimeZone::FixedOffset { display, .. } => display.clone(),
        }
    }
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
    pub statement_time_micros: Option<i64>,
    pub txn_start_micros: Option<i64>,
    pub time_zone: SessionTimeZone,
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
            statement_time_micros: None,
            txn_start_micros: None,
            time_zone: SessionTimeZone::default(),
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
        guard.statement_time_micros = None;
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

    pub fn set_time_zone(&self, tz: SessionTimeZone) {
        let mut guard = self.state.lock();
        guard.time_zone = tz;
    }

    pub fn time_zone(&self) -> SessionTimeZone {
        self.state.lock().time_zone.clone()
    }

    pub fn set_statement_time_micros(&self, micros: i64) {
        let mut guard = self.state.lock();
        guard.statement_time_micros = Some(micros);
    }

    pub fn statement_time_micros(&self) -> Option<i64> {
        self.state.lock().statement_time_micros
    }

    pub fn set_txn_start_micros(&self, micros: i64) {
        let mut guard = self.state.lock();
        guard.txn_start_micros = Some(micros);
    }

    pub fn txn_start_micros(&self) -> Option<i64> {
        self.state.lock().txn_start_micros
    }

    pub fn clear_txn_start_micros(&self) {
        let mut guard = self.state.lock();
        guard.txn_start_micros = None;
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

pub fn now_utc_micros() -> i64 {
    let now = OffsetDateTime::now_utc();
    (now.unix_timestamp_nanos() / 1_000) as i64
}
