use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::error::PgWireResult;

use crate::db::Db;
use crate::engine::{EvalContext, ExecNode, Expr, Plan, Schema, Value, ValuesExec, fe, fe_code};
use crate::server::mapping::lookup_show_value;
use crate::session::{Session, SessionTimeZone, TransactionIsolation};

type ExecResult = PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)>;

pub(crate) fn build_set_show_executor(
    db: &Arc<RwLock<Db>>,
    session: &Arc<Session>,
    plan: &Plan,
    _ctx: &EvalContext,
) -> ExecResult {
    match plan {
        Plan::ShowVariable { name, schema } => {
            let normalized = name.replace(' ', "_");
            let value = if normalized == "search_path" {
                let ids = session.search_path();
                let parts = {
                    let db_read = db.read();
                    ids.into_iter()
                        .filter_map(|sid| db_read.catalog.schema_name(sid))
                        .map(|schema_name| schema_name.as_str().to_string())
                        .collect::<Vec<_>>()
                };
                parts.join(", ")
            } else if normalized == "timezone" || normalized == "time_zone" {
                session.time_zone().display_value().to_string()
            } else if normalized == "transaction_isolation" {
                let iso = session
                    .txn_isolation()
                    .unwrap_or_else(|| session.default_txn_isolation());
                iso.as_str().to_string()
            } else if normalized == "default_transaction_isolation" {
                session.default_txn_isolation().as_str().to_string()
            } else {
                match lookup_show_value(&normalized) {
                    Some(v) => v,
                    None => return Err(fe_code("0A000", format!("SHOW {} not supported", name))),
                }
            };
            let rows = vec![vec![Expr::Literal(Value::Text(value))]];
            let exec = ValuesExec::new(schema.clone(), rows)?;
            Ok((Box::new(exec), Some("SHOW".into()), Some(1)))
        }
        Plan::SetVariable { name, value } => match name.as_str() {
            "client_min_messages" => Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("SET".into()),
                None,
            )),
            "search_path" => {
                let schema_ids = {
                    let db_read = db.read();
                    match value {
                        Some(values) => {
                            if values.is_empty() {
                                return Err(fe("SET search_path requires at least one schema"));
                            }
                            let mut resolved = Vec::with_capacity(values.len());
                            for schema_name in values {
                                let id = db_read.catalog.schema_id(schema_name).ok_or_else(|| {
                                    fe_code(
                                        "3F000",
                                        format!("schema \"{}\" does not exist", schema_name),
                                    )
                                })?;
                                resolved.push(id);
                            }
                            resolved
                        }
                        None => {
                            let public_id = db_read
                                .catalog
                                .schema_id("public")
                                .ok_or_else(|| fe("public schema not found"))?;
                            vec![public_id]
                        }
                    }
                };
                session.set_search_path(schema_ids);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "timezone" => {
                let tz = match value {
                    Some(values) => {
                        if values.len() != 1 {
                            return Err(fe("SET TIME ZONE requires a single value"));
                        }
                        SessionTimeZone::parse(&values[0]).map_err(|e| fe_code("22023", e))?
                    }
                    None => SessionTimeZone::Utc,
                };
                session.set_time_zone(tz);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "transaction_isolation" => {
                let iso = match value {
                    Some(values) => {
                        if values.len() != 1 {
                            return Err(fe("SET transaction_isolation requires a single value"));
                        }
                        TransactionIsolation::parse(&values[0]).map_err(|e| fe_code("0A000", e))?
                    }
                    None => TransactionIsolation::ReadCommitted,
                };
                if session.current_tx().is_some() {
                    session.set_txn_isolation(iso);
                } else {
                    session.set_default_txn_isolation(iso);
                }
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "default_transaction_isolation" => {
                let iso = match value {
                    Some(values) => {
                        if values.len() != 1 {
                            return Err(fe(
                                "SET default_transaction_isolation requires a single value",
                            ));
                        }
                        TransactionIsolation::parse(&values[0]).map_err(|e| fe_code("0A000", e))?
                    }
                    None => TransactionIsolation::ReadCommitted,
                };
                session.set_default_txn_isolation(iso);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            _ => Err(fe_code("0A000", format!("SET {} not supported", name))),
        },
        _ => unreachable!("non set/show plan routed to build_set_show_executor"),
    }
}
