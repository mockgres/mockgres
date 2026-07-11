use std::sync::Arc;

use futures::executor::block_on;
use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::{CellInput, Db};
use crate::engine::{EvalContext, ExecNode, ObjName, Plan, Schema, Value, ValuesExec, fe_code};
use crate::server::errors::map_db_err;
use crate::session::Session;
use crate::txn::{TransactionManager, TxId};

use super::ddl::resolve_schema_for_create;
use super::tx::{finish_writer_tx, writer_txid};

type ExecResult = PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)>;

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_create_table_as_executor(
    db: &Arc<RwLock<Db>>,
    txn_manager: &Arc<TransactionManager>,
    session: &Arc<Session>,
    snapshot_xid: TxId,
    table: &ObjName,
    column_names: &[String],
    query: &Plan,
    with_data: bool,
    if_not_exists: bool,
    params: Arc<Vec<Value>>,
    ctx: &EvalContext,
) -> ExecResult {
    let schema_name = {
        let db = db.read();
        resolve_schema_for_create(&db, session, table.schema.as_ref())?
    };
    if db
        .read()
        .catalog
        .get_table(&schema_name, &table.name)
        .is_some()
    {
        if if_not_exists {
            return empty_result("CREATE TABLE AS", None);
        }
        return Err(fe_code(
            "42P07",
            format!("relation \"{}\" already exists", table.name),
        ));
    }

    let source_schema = query.schema();
    let columns = source_schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (
                column_names
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| field.name.clone()),
                field.data_type.clone(),
                true,
                None,
                None,
            )
        })
        .collect();

    if !with_data {
        let search_path = session.search_path();
        db.write()
            .create_table(
                &schema_name,
                &table.name,
                columns,
                None,
                Vec::new(),
                &search_path,
            )
            .map_err(map_db_err)?;
        return empty_result("CREATE TABLE AS", None);
    }

    let (mut executor, _, _) = crate::server::exec_builder::build_executor(
        db,
        txn_manager,
        session,
        snapshot_xid,
        query,
        params,
        ctx,
    )?;
    block_on(executor.open())?;
    let mut selected_rows = Vec::new();
    while let Some(row) = block_on(executor.next())? {
        selected_rows.push(row);
    }
    block_on(executor.close())?;

    let rows = selected_rows
        .into_iter()
        .map(|row| row.into_iter().map(CellInput::Value).collect())
        .collect();
    let search_path = session.search_path();
    let (txid, autocommit) = writer_txid(session, txn_manager);
    let result = {
        let mut db = db.write();
        if let Err(error) = db.create_table(
            &schema_name,
            &table.name,
            columns,
            None,
            Vec::new(),
            &search_path,
        ) {
            Err(error)
        } else {
            match db.insert_full_rows(&schema_name, &table.name, rows, false, txid, &[], ctx, None)
            {
                Ok(result) => Ok(result),
                Err(error) => {
                    let _ = db.drop_table(&schema_name, &table.name, false, true);
                    Err(error)
                }
            }
        }
    };
    let (inserted, _, inserted_ptrs, _, _) = match result {
        Ok(result) => result,
        Err(error) => {
            finish_writer_tx(txn_manager, txid, autocommit, false);
            return Err(map_db_err(error));
        }
    };
    if autocommit {
        finish_writer_tx(txn_manager, txid, true, true);
    } else {
        session.record_inserts(inserted_ptrs);
    }
    empty_result(&format!("SELECT {inserted}"), Some(inserted))
}

fn empty_result(tag: &str, row_count: Option<usize>) -> ExecResult {
    Ok((
        Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
        Some(tag.to_string()),
        row_count,
    ))
}
