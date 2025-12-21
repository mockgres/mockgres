use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{Db, LockOwner};
use crate::engine::fe_code;
use crate::server::errors::map_db_err;
use crate::session::{Session, now_utc_micros};
use crate::txn::{TransactionManager, TxId, TxnStatus};

pub(crate) fn writer_txid(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
) -> (TxId, bool) {
    if let Some(txid) = session.current_tx() {
        (txid, false)
    } else {
        let txid = txn_manager.allocate();
        txn_manager.set_status(txid, TxnStatus::InProgress);
        (txid, true)
    }
}

pub(crate) fn finish_writer_tx(
    txn_manager: &Arc<TransactionManager>,
    txid: TxId,
    autocommit: bool,
    success: bool,
) {
    if autocommit {
        let status = if success {
            TxnStatus::Committed
        } else {
            TxnStatus::Aborted
        };
        txn_manager.set_status(txid, status);
    }
}

pub(crate) fn begin_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
) -> PgWireResult<()> {
    if session.current_tx().is_some() {
        return Err(fe_code("25001", "transaction already in progress"));
    }
    let txid = txn_manager.allocate();
    txn_manager.set_status(txid, TxnStatus::InProgress);
    session.begin_transaction_epoch();
    session.set_current_tx(Some(txid));
    session.reset_changes();
    session.set_txn_start_micros(now_utc_micros());
    let iso = session.default_txn_isolation();
    session.set_txn_isolation(iso);
    Ok(())
}

pub(crate) fn commit_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
    db: &Arc<RwLock<Db>>,
) -> PgWireResult<()> {
    let Some(txid) = session.current_tx() else {
        return Err(fe_code("25P01", "no transaction in progress"));
    };
    txn_manager.set_status(txid, TxnStatus::Committed);
    session.set_current_tx(None);
    session.reset_changes();
    session.clear_txn_start_micros();
    session.clear_txn_isolation();
    if let Some(epoch) = session.end_transaction_epoch() {
        let db_read = db.read();
        db_read.release_locks(LockOwner::new(session.id(), epoch));
    }
    Ok(())
}

pub(crate) fn rollback_transaction(
    session: &Arc<Session>,
    txn_manager: &Arc<TransactionManager>,
    db: &Arc<RwLock<Db>>,
) -> PgWireResult<()> {
    let Some(txid) = session.current_tx() else {
        return Err(fe_code("25P01", "no transaction in progress"));
    };
    let changes = session.take_changes();
    session.set_current_tx(None);
    session.clear_txn_start_micros();
    session.clear_txn_isolation();
    {
        let mut db = db.write();
        db.rollback_transaction_changes(&changes, txid)
            .map_err(map_db_err)?;
    }
    txn_manager.set_status(txid, TxnStatus::Aborted);
    if let Some(epoch) = session.end_transaction_epoch() {
        let db_read = db.read();
        db_read.release_locks(LockOwner::new(session.id(), epoch));
    }
    Ok(())
}
