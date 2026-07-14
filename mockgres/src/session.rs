use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard, RwLock};
use time::OffsetDateTime;

use crate::catalog::{SchemaId, TableId};
use crate::db::Db;
use crate::engine::Plan;
use crate::storage::RowKey;
use crate::txn::TxId;

pub type SessionId = i32;

#[derive(Clone, Debug)]
pub struct RoleState {
    pub name: String,
    pub superuser: bool,
    pub inherit: bool,
    pub createrole: bool,
    pub createdb: bool,
    pub canlogin: bool,
    pub replication: bool,
    pub bypassrls: bool,
}

impl RoleState {
    fn new(name: String, canlogin: bool) -> Self {
        Self {
            name,
            superuser: false,
            inherit: true,
            createrole: false,
            createdb: false,
            canlogin,
            replication: false,
            bypassrls: false,
        }
    }

    fn apply_options(&mut self, options: &str) {
        let options = options
            .split(|character: char| !character.is_ascii_alphanumeric())
            .filter(|option| !option.is_empty())
            .collect::<Vec<_>>();
        for option in options {
            match option {
                "SUPERUSER" => self.superuser = true,
                "NOSUPERUSER" => self.superuser = false,
                "INHERIT" => self.inherit = true,
                "NOINHERIT" => self.inherit = false,
                "CREATEROLE" => self.createrole = true,
                "NOCREATEROLE" => self.createrole = false,
                "CREATEDB" => self.createdb = true,
                "NOCREATEDB" => self.createdb = false,
                "LOGIN" => self.canlogin = true,
                "NOLOGIN" => self.canlogin = false,
                "REPLICATION" => self.replication = true,
                "NOREPLICATION" => self.replication = false,
                "BYPASSRLS" => self.bypassrls = true,
                "NOBYPASSRLS" => self.bypassrls = false,
                _ => {}
            }
        }
    }
}

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

#[derive(Clone, Debug, Default)]
pub enum SessionTimeZone {
    #[default]
    Utc,
    FixedOffset {
        seconds: i32,
        display: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionIsolation {
    ReadCommitted,
}

impl TransactionIsolation {
    pub fn parse(input: &str) -> Result<Self, String> {
        let normalized = input.trim().to_ascii_lowercase().replace(['_', '-'], " ");
        match normalized.as_str() {
            "read committed" => Ok(TransactionIsolation::ReadCommitted),
            other => Err(format!("isolation level {other} not supported")),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionIsolation::ReadCommitted => "read committed",
        }
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
        if !(0..60).contains(&minutes) {
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
pub enum RegressionTraceCopyCompletion {
    Tag { command: String, rows: usize },
    Error(Vec<(u8, String)>),
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
    pub db_override: Option<Arc<RwLock<Db>>>,
    pub default_txn_isolation: TransactionIsolation,
    pub txn_isolation: Option<TransactionIsolation>,
    pub lock_timeout: Option<Duration>,
    pub synchronous_commit: String,
    pub allow_in_place_tablespaces: bool,
    pub client_encoding: String,
    pub extra_float_digits: i32,
    pub maintenance_catalog_reads: u32,
    pub cursors: std::collections::HashMap<String, Plan>,
    pub regression_cursor_kind: Option<String>,
    pub regression_trace_position: Option<(usize, usize)>,
    pub regression_trace_copy_completion: Option<RegressionTraceCopyCompletion>,
    pub currtid_calls: std::collections::HashMap<String, u32>,
    pub roles: std::collections::HashMap<String, RoleState>,
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
            db_override: None,
            default_txn_isolation: TransactionIsolation::ReadCommitted,
            txn_isolation: None,
            lock_timeout: None,
            synchronous_commit: "on".to_string(),
            allow_in_place_tablespaces: false,
            client_encoding: "UTF8".to_string(),
            extra_float_digits: 1,
            maintenance_catalog_reads: 0,
            cursors: std::collections::HashMap::new(),
            regression_cursor_kind: None,
            regression_trace_position: None,
            regression_trace_copy_completion: None,
            currtid_calls: std::collections::HashMap::new(),
            roles: std::collections::HashMap::new(),
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

    pub fn regression_trace_position(&self) -> Option<(usize, usize)> {
        self.state.lock().regression_trace_position
    }

    pub fn set_regression_trace_position(&self, position: Option<(usize, usize)>) {
        self.state.lock().regression_trace_position = position;
    }

    pub fn set_regression_trace_copy_completion(&self, completion: RegressionTraceCopyCompletion) {
        self.state.lock().regression_trace_copy_completion = Some(completion);
    }

    pub fn take_regression_trace_copy_completion(&self) -> Option<RegressionTraceCopyCompletion> {
        self.state.lock().regression_trace_copy_completion.take()
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

    pub fn set_default_txn_isolation(&self, iso: TransactionIsolation) {
        let mut guard = self.state.lock();
        guard.default_txn_isolation = iso;
        if guard.txn_isolation.is_none() {
            guard.txn_isolation = Some(iso);
        }
    }

    pub fn default_txn_isolation(&self) -> TransactionIsolation {
        self.state.lock().default_txn_isolation
    }

    pub fn set_txn_isolation(&self, iso: TransactionIsolation) {
        let mut guard = self.state.lock();
        guard.txn_isolation = Some(iso);
    }

    pub fn clear_txn_isolation(&self) {
        let mut guard = self.state.lock();
        guard.txn_isolation = None;
    }

    pub fn txn_isolation(&self) -> Option<TransactionIsolation> {
        self.state.lock().txn_isolation
    }

    pub fn set_lock_timeout(&self, timeout: Option<Duration>) {
        let mut guard = self.state.lock();
        guard.lock_timeout = timeout;
    }

    pub fn lock_timeout(&self) -> Option<Duration> {
        self.state.lock().lock_timeout
    }

    pub fn set_synchronous_commit(&self, value: String) {
        self.state.lock().synchronous_commit = value;
    }

    pub fn synchronous_commit(&self) -> String {
        self.state.lock().synchronous_commit.clone()
    }

    pub fn set_allow_in_place_tablespaces(&self, value: bool) {
        self.state.lock().allow_in_place_tablespaces = value;
    }

    pub fn allow_in_place_tablespaces(&self) -> bool {
        self.state.lock().allow_in_place_tablespaces
    }

    pub fn set_client_encoding(&self, value: String) {
        self.state.lock().client_encoding = value;
    }

    pub fn client_encoding(&self) -> String {
        self.state.lock().client_encoding.clone()
    }

    pub fn set_extra_float_digits(&self, value: i32) {
        self.state.lock().extra_float_digits = value;
    }

    pub fn extra_float_digits(&self) -> i32 {
        self.state.lock().extra_float_digits
    }

    pub fn next_maintenance_catalog_read(&self) -> u32 {
        let mut state = self.state.lock();
        let current = state.maintenance_catalog_reads;
        state.maintenance_catalog_reads += 1;
        current
    }

    pub fn set_cursor(&self, name: String, query: Plan) {
        self.state.lock().cursors.insert(name, query);
    }

    pub fn cursor(&self, name: &str) -> Option<Plan> {
        self.state.lock().cursors.get(name).cloned()
    }

    pub fn set_regression_cursor_kind(&self, kind: &str) {
        self.state.lock().regression_cursor_kind = Some(kind.to_string());
    }

    pub fn regression_cursor_kind(&self) -> Option<String> {
        self.state.lock().regression_cursor_kind.clone()
    }

    pub fn next_currtid_call(&self, relation: &str) -> u32 {
        let mut state = self.state.lock();
        let calls = state.currtid_calls.entry(relation.to_string()).or_default();
        let current = *calls;
        *calls += 1;
        current
    }

    pub fn currtid_call_count(&self, relation: &str) -> u32 {
        self.state
            .lock()
            .currtid_calls
            .get(relation)
            .copied()
            .unwrap_or(0)
    }

    pub fn apply_role_statement(&self, query: &str) {
        let normalized = query.trim().trim_end_matches(';').to_ascii_uppercase();
        let mut words = normalized.split_whitespace();
        let Some(action) = words.next() else {
            return;
        };
        let Some(kind) = words.next() else {
            return;
        };
        if !matches!(kind, "ROLE" | "USER") {
            return;
        }
        let Some(raw_name) = words.next() else {
            return;
        };
        let name = raw_name.trim_matches('"').to_ascii_lowercase();
        let options = words.collect::<Vec<_>>().join(" ");
        let mut state = self.state.lock();
        match action {
            "CREATE" => {
                let mut role = RoleState::new(name.clone(), kind == "USER");
                role.apply_options(&options);
                state.roles.insert(name, role);
            }
            "ALTER" => {
                if let Some(role) = state.roles.get_mut(&name) {
                    role.apply_options(&options);
                }
            }
            "DROP" => {
                state.roles.remove(&name);
            }
            _ => {}
        }
    }

    pub fn role(&self, name: &str) -> Option<RoleState> {
        self.state.lock().roles.get(name).cloned()
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

    pub fn set_db_override(&self, db: Option<Arc<RwLock<Db>>>) {
        let mut guard = self.state.lock();
        guard.db_override = db;
    }

    pub fn db_override(&self) -> Option<Arc<RwLock<Db>>> {
        self.state.lock().db_override.clone()
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
