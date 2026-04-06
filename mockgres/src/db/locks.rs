use crate::catalog::TableId;
use crate::session::SessionId;
use crate::storage::RowId;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{Instant, timeout};

use super::sql_err;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LockOwner {
    pub session_id: SessionId,
    pub epoch: u64,
}

impl LockOwner {
    pub fn new(session_id: SessionId, epoch: u64) -> Self {
        Self { session_id, epoch }
    }
}

#[derive(Debug)]
pub(super) struct LockRegistry {
    inner: Mutex<LockState>,
    notify: Notify,
}

#[derive(Debug)]
struct LockState {
    holders: HashMap<(TableId, RowId), LockOwner>,
    owned: HashMap<LockOwner, HashSet<(TableId, RowId)>>,
}

impl LockRegistry {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(LockState {
                holders: HashMap::new(),
                owned: HashMap::new(),
            }),
            notify: Notify::new(),
        }
    }

    fn acquire_skip_locked(&self, key: (TableId, RowId), owner: LockOwner) -> anyhow::Result<bool> {
        let mut state = self.inner.lock();
        if let Some(existing) = state.holders.get(&key) {
            if *existing == owner {
                return Ok(true);
            }
            return Ok(false);
        }
        state.holders.insert(key, owner);
        state.owned.entry(owner).or_default().insert(key);
        Ok(true)
    }

    fn acquire_nowait(&self, key: (TableId, RowId), owner: LockOwner) -> anyhow::Result<()> {
        let mut state = self.inner.lock();
        if let Some(existing) = state.holders.get(&key) {
            if *existing == owner {
                return Ok(());
            }
            return Err(sql_err(
                "55P03",
                "could not obtain lock on target row (NOWAIT)",
            ));
        }
        state.holders.insert(key, owner);
        state.owned.entry(owner).or_default().insert(key);
        Ok(())
    }

    async fn acquire_blocking(
        &self,
        key: (TableId, RowId),
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        loop {
            {
                let mut state = self.inner.lock();
                match state.holders.get(&key) {
                    Some(existing) if *existing == owner => return Ok(()),
                    Some(_) => {}
                    None => {
                        state.holders.insert(key, owner);
                        state.owned.entry(owner).or_default().insert(key);
                        return Ok(());
                    }
                }
            }
            self.notify.notified().await;
        }
    }

    async fn acquire_blocking_timeout(
        &self,
        key: (TableId, RowId),
        owner: LockOwner,
        timeout_dur: Duration,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout_dur;
        loop {
            {
                let mut state = self.inner.lock();
                match state.holders.get(&key) {
                    Some(existing) if *existing == owner => return Ok(()),
                    Some(_) => {}
                    None => {
                        state.holders.insert(key, owner);
                        state.owned.entry(owner).or_default().insert(key);
                        return Ok(());
                    }
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(sql_err("55P03", "canceling statement due to lock timeout"));
            }
            let remaining = deadline.duration_since(now);
            if timeout(remaining, self.notify.notified()).await.is_err() {
                return Err(sql_err("55P03", "canceling statement due to lock timeout"));
            }
        }
    }

    pub(super) fn release_owner(&self, owner: LockOwner) {
        let mut state = self.inner.lock();
        if let Some(keys) = state.owned.remove(&owner) {
            for key in keys {
                if let Some(existing) = state.holders.get(&key)
                    && *existing == owner
                {
                    state.holders.remove(&key);
                }
            }
        }
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
pub struct LockHandle {
    inner: Arc<LockRegistry>,
}

impl LockHandle {
    pub(super) fn new(inner: Arc<LockRegistry>) -> Self {
        Self { inner }
    }

    pub fn lock_row_skip_locked(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<bool> {
        self.inner.acquire_skip_locked((table_id, row_id), owner)
    }

    pub fn lock_row_nowait(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        self.inner.acquire_nowait((table_id, row_id), owner)
    }

    pub async fn lock_row_blocking(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
    ) -> anyhow::Result<()> {
        self.inner.acquire_blocking((table_id, row_id), owner).await
    }

    pub async fn lock_row_blocking_timeout(
        &self,
        table_id: TableId,
        row_id: RowId,
        owner: LockOwner,
        timeout_dur: Duration,
    ) -> anyhow::Result<()> {
        self.inner
            .acquire_blocking_timeout((table_id, row_id), owner, timeout_dur)
            .await
    }
}
