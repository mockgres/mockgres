use async_trait::async_trait;
use pgwire::error::PgWireResult;
use std::time::Duration;

use crate::db::{LockHandle, LockOwner};
use crate::engine::{ExecNode, LockSpec, Schema, Value, fe};
use crate::server::errors::map_db_err;
use crate::storage::RowId;

pub struct LockScope {
    owner: LockOwner,
    release_on_close: bool,
}

impl LockScope {
    pub fn new(owner: LockOwner, release_on_close: bool) -> Self {
        Self {
            owner,
            release_on_close,
        }
    }
}

pub fn wrap_with_lock_apply(
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    scope: LockScope,
    locks: LockHandle,
    lock_timeout: Option<Duration>,
) -> Box<dyn ExecNode> {
    Box::new(LockApplyExec::new(
        schema,
        child,
        lock_spec,
        row_id_idx,
        scope,
        locks,
        lock_timeout,
    ))
}

struct LockApplyExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    owner: LockOwner,
    locks: LockHandle,
    lock_timeout: Option<Duration>,
    release_on_close: bool,
    released: bool,
}

enum AcquireOutcome {
    Acquired,
    Skipped,
}

impl LockApplyExec {
    fn new(
        schema: Schema,
        child: Box<dyn ExecNode>,
        lock_spec: LockSpec,
        row_id_idx: usize,
        scope: LockScope,
        locks: LockHandle,
        lock_timeout: Option<Duration>,
    ) -> Self {
        Self {
            schema,
            child,
            lock_spec,
            row_id_idx,
            owner: scope.owner,
            locks,
            lock_timeout,
            release_on_close: scope.release_on_close,
            released: false,
        }
    }

    fn release_statement_locks(&mut self) {
        if self.release_on_close && !self.released {
            self.locks.release_owner(self.owner);
            self.released = true;
        }
    }
}

impl Drop for LockApplyExec {
    fn drop(&mut self) {
        self.release_statement_locks();
    }
}

#[async_trait]
impl ExecNode for LockApplyExec {
    async fn open(&mut self) -> PgWireResult<()> {
        self.child.open().await
    }

    async fn next(&mut self) -> PgWireResult<Option<Vec<Value>>> {
        loop {
            let Some(mut row) = self.child.next().await? else {
                return Ok(None);
            };
            if row.len() <= self.row_id_idx {
                return Err(fe("row identifier column missing from plan output"));
            }
            let row_id_value = row.remove(self.row_id_idx);
            let Value::Int64(raw_id) = row_id_value else {
                return Err(fe("row identifier column has unexpected type"));
            };
            if raw_id < 0 {
                return Err(fe("row identifier cannot be negative"));
            }
            let row_id = raw_id as RowId;
            let acquire_result = if self.lock_spec.skip_locked {
                self.locks
                    .lock_row_skip_locked(self.lock_spec.target, row_id, self.owner)
                    .map(|acquired| {
                        if acquired {
                            AcquireOutcome::Acquired
                        } else {
                            AcquireOutcome::Skipped
                        }
                    })
            } else if self.lock_spec.nowait {
                self.locks
                    .lock_row_nowait(self.lock_spec.target, row_id, self.owner)
                    .map(|_| AcquireOutcome::Acquired)
            } else {
                let result = if let Some(timeout) = self.lock_timeout {
                    self.locks
                        .lock_row_blocking_timeout(
                            self.lock_spec.target,
                            row_id,
                            self.owner,
                            timeout,
                        )
                        .await
                } else {
                    self.locks
                        .lock_row_blocking(self.lock_spec.target, row_id, self.owner)
                        .await
                };
                result.map(|_| AcquireOutcome::Acquired)
            };
            match acquire_result {
                Ok(AcquireOutcome::Acquired) => return Ok(Some(row)),
                Ok(AcquireOutcome::Skipped) => continue,
                Err(e) => return Err(map_db_err(e)),
            }
        }
    }

    async fn close(&mut self) -> PgWireResult<()> {
        let result = self.child.close().await;
        self.release_statement_locks();
        result
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
