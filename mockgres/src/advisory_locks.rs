use std::collections::{HashMap, HashSet};

use parking_lot::{Condvar, Mutex};

use crate::session::SessionId;

#[derive(Default)]
struct AdvisoryLockState {
    locks: HashMap<i64, LockEntry>,
    held_by_session: HashMap<SessionId, HashSet<i64>>,
}

#[derive(Clone, Copy, Debug)]
struct LockEntry {
    session_id: SessionId,
    count: usize,
}

#[derive(Default)]
pub struct AdvisoryLockRegistry {
    inner: Mutex<AdvisoryLockState>,
    notify: Condvar,
}

impl AdvisoryLockRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self, key: i64, session_id: SessionId) {
        let mut guard = self.inner.lock();
        loop {
            match guard.locks.get_mut(&key) {
                None => {
                    guard
                        .locks
                        .insert(key, LockEntry { session_id, count: 1 });
                    guard
                        .held_by_session
                        .entry(session_id)
                        .or_default()
                        .insert(key);
                    return;
                }
                Some(entry) if entry.session_id == session_id => {
                    entry.count += 1;
                    guard
                        .held_by_session
                        .entry(session_id)
                        .or_default()
                        .insert(key);
                    return;
                }
                _ => {
                    self.notify.wait(&mut guard);
                }
            }
        }
    }

    pub fn unlock(&self, key: i64, session_id: SessionId) -> bool {
        let mut guard = self.inner.lock();
        match guard.locks.get_mut(&key) {
            Some(entry) if entry.session_id == session_id => {
                if entry.count > 1 {
                    entry.count -= 1;
                    true
                } else {
                    guard.locks.remove(&key);
                    if let Some(keys) = guard.held_by_session.get_mut(&session_id) {
                        keys.remove(&key);
                        if keys.is_empty() {
                            guard.held_by_session.remove(&session_id);
                        }
                    }
                    self.notify.notify_all();
                    true
                }
            }
            _ => false,
        }
    }

    pub fn release_session(&self, session_id: SessionId) {
        let mut guard = self.inner.lock();
        let Some(keys) = guard.held_by_session.remove(&session_id) else {
            return;
        };
        for key in keys {
            if let Some(entry) = guard.locks.get(&key)
                && entry.session_id == session_id
            {
                guard.locks.remove(&key);
            }
        }
        self.notify.notify_all();
    }
}
