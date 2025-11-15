use async_trait::async_trait;
use pgwire::error::PgWireResult;

use crate::db::{LockHandle, LockOwner};
use crate::engine::{ExecNode, LockSpec, Schema, Value, fe};
use crate::server::errors::map_db_err;
use crate::storage::RowId;

pub fn wrap_with_lock_apply(
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    owner: LockOwner,
    locks: LockHandle,
) -> Box<dyn ExecNode> {
    Box::new(LockApplyExec::new(
        schema, child, lock_spec, row_id_idx, owner, locks,
    ))
}

struct LockApplyExec {
    schema: Schema,
    child: Box<dyn ExecNode>,
    lock_spec: LockSpec,
    row_id_idx: usize,
    owner: LockOwner,
    locks: LockHandle,
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
        owner: LockOwner,
        locks: LockHandle,
    ) -> Self {
        Self {
            schema,
            child,
            lock_spec,
            row_id_idx,
            owner,
            locks,
        }
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
                self.locks
                    .lock_row_blocking(self.lock_spec.target, row_id, self.owner)
                    .await
                    .map(|_| AcquireOutcome::Acquired)
            };
            match acquire_result {
                Ok(AcquireOutcome::Acquired) => return Ok(Some(row)),
                Ok(AcquireOutcome::Skipped) => continue,
                Err(e) => return Err(map_db_err(e)),
            }
        }
    }

    async fn close(&mut self) -> PgWireResult<()> {
        self.child.close().await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
