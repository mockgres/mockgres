use std::sync::Arc;

use pgwire::error::PgWireResult;

use crate::catalog::SchemaName;
use crate::db::Db;
use crate::engine::{
    AggCall, AggFunc, DbDdlKind, EvalContext, ExecNode, Plan, Schema, Value, ValuesExec, fe,
};
use crate::session::Session;
use crate::txn::{TransactionManager, TxId};

use super::exec::ddl::build_ddl_executor;
use super::exec::read::build_read_executor;
use super::exec::set_show::build_set_show_executor;
use super::exec::tx::{begin_transaction, commit_transaction, rollback_transaction};
use super::exec::write_dml::{build_delete_executor, build_insert_executor, build_update_executor};

pub(crate) fn schema_or_public(schema: &Option<SchemaName>) -> &str {
    schema.as_ref().map(|s| s.as_str()).unwrap_or("public")
}

pub(crate) fn assert_supported_aggs(aggs: &[(AggCall, String)]) {
    for (agg, _) in aggs {
        match agg.func {
            AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {}
        }
    }
}

pub fn command_tag(plan: &Plan) -> &'static str {
    match plan {
        Plan::Values { .. }
        | Plan::SeqScan { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::UnboundJoin { .. }
        | Plan::Join { .. }
        | Plan::Alias { .. } => "SELECT",

        Plan::CreateTable { .. } => "CREATE TABLE",
        Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableDropConstraint { .. } => "ALTER TABLE",
        Plan::CreateIndex { .. } => "CREATE INDEX",
        Plan::DropIndex { .. } => "DROP INDEX",
        Plan::DropTable { .. } => "DROP TABLE",
        Plan::CreateSchema { .. } => "CREATE SCHEMA",
        Plan::DropSchema { .. } => "DROP SCHEMA",
        Plan::AlterSchemaRename { .. } => "ALTER SCHEMA",
        Plan::CreateDatabase { .. } => "CREATE DATABASE",
        Plan::DropDatabase { .. } => "DROP DATABASE",
        Plan::AlterDatabase { .. } => "ALTER DATABASE",
        Plan::UnsupportedDbDDL { kind, .. } => match kind {
            DbDdlKind::Create => "CREATE DATABASE",
            DbDdlKind::Drop => "DROP DATABASE",
            DbDdlKind::Alter => "ALTER DATABASE",
        },
        Plan::ShowVariable { .. } => "SHOW",
        Plan::SetVariable { .. } => "SET",
        Plan::CallBuiltin { .. } => "SELECT",
        Plan::InsertValues { .. } => "INSERT",
        Plan::Update { .. } => "UPDATE",
        Plan::Delete { .. } => "DELETE",
        Plan::UnboundSeqScan { .. } => "SELECT",
        Plan::BeginTransaction => "BEGIN",
        Plan::CommitTransaction => "COMMIT",
        Plan::RollbackTransaction => "ROLLBACK",
    }
}
pub fn build_executor(
    db: &Arc<parking_lot::RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    p: &Plan,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
    match p {
        Plan::Values { .. }
        | Plan::Projection { .. }
        | Plan::Aggregate { .. }
        | Plan::CountRows { .. }
        | Plan::SeqScan { .. }
        | Plan::LockRows { .. }
        | Plan::Filter { .. }
        | Plan::Order { .. }
        | Plan::Limit { .. }
        | Plan::Join { .. }
        | Plan::Alias { .. } => {
            return build_read_executor(db, txn_manager, session, snapshot_xid, p, params, ctx);
        }
        Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::AlterTableAddConstraintUnique { .. }
        | Plan::AlterTableAddConstraintForeignKey { .. }
        | Plan::AlterTableDropConstraint { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::DropTable { .. }
        | Plan::CreateSchema { .. }
        | Plan::DropSchema { .. }
        | Plan::AlterSchemaRename { .. }
        | Plan::UnsupportedDbDDL { .. }
        | Plan::CreateDatabase { .. }
        | Plan::DropDatabase { .. }
        | Plan::AlterDatabase { .. } => {
            return build_ddl_executor(db, session, p, ctx);
        }

        Plan::InsertValues {
            table,
            columns,
            rows,
            override_system_value,
            on_conflict,
            returning,
            returning_schema,
        } => build_insert_executor(
            db,
            txn_manager,
            session,
            table,
            columns,
            rows,
            *override_system_value,
            on_conflict,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::Update {
            table,
            sets,
            filter,
            from,
            from_schema,
            returning,
            returning_schema,
        } => build_update_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            table,
            sets,
            filter,
            from,
            from_schema,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::Delete {
            table,
            filter,
            returning,
            returning_schema,
        } => build_delete_executor(
            db,
            txn_manager,
            session,
            snapshot_xid,
            table,
            filter,
            returning,
            returning_schema,
            params.clone(),
            ctx,
        ),
        Plan::ShowVariable { .. } | Plan::SetVariable { .. } => {
            return build_set_show_executor(db, session, p, ctx);
        }

        Plan::BeginTransaction => {
            begin_transaction(session, txn_manager)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("BEGIN".into()),
                None,
            ))
        }
        Plan::CommitTransaction => {
            commit_transaction(session, txn_manager, db)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("COMMIT".into()),
                None,
            ))
        }
        Plan::RollbackTransaction => {
            rollback_transaction(session, txn_manager, db)?;
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ROLLBACK".into()),
                None,
            ))
        }
        Plan::CallBuiltin { .. } => Err(fe("builtin execution not supported here")),
        Plan::UnboundSeqScan { .. } | Plan::UnboundJoin { .. } => {
            Err(fe("unbound plan; call binder first"))
        }
    }
}
