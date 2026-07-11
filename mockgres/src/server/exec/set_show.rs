use std::sync::Arc;
use std::time::Duration;

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
            } else if normalized == "lock_timeout" {
                format_lock_timeout(session.lock_timeout())
            } else if normalized == "synchronous_commit" {
                session.synchronous_commit()
            } else if normalized == "allow_in_place_tablespaces" {
                if session.allow_in_place_tablespaces() {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
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
            "client_encoding" => {
                let encoding = value
                    .as_ref()
                    .and_then(|values| values.first())
                    .cloned()
                    .unwrap_or_else(|| "UTF8".to_string())
                    .to_ascii_uppercase();
                session.set_client_encoding(encoding);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "extra_float_digits" => {
                let digits = value
                    .as_ref()
                    .and_then(|values| values.first())
                    .ok_or_else(|| fe("SET extra_float_digits requires a value"))?
                    .parse::<i32>()
                    .map_err(|_| fe_code("22023", "invalid extra_float_digits value"))?;
                if !(-15..=3).contains(&digits) {
                    return Err(fe_code("22023", "extra_float_digits out of range"));
                }
                session.set_extra_float_digits(digits);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "client_min_messages"
            | "enable_seqscan"
            | "enable_indexscan"
            | "enable_indexonlyscan"
            | "enable_bitmapscan"
            | "work_mem"
            | "max_parallel_maintenance_workers"
            | "min_parallel_index_scan_size"
            | "role"
            | "geqo"
            | "geqo_threshold"
            | "parallel_setup_cost"
            | "parallel_tuple_cost"
            | "min_parallel_table_scan_size"
            | "max_parallel_workers_per_gather"
            | "default_toast_compression" => Ok((
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
                                let id =
                                    db_read.catalog.schema_id(schema_name).ok_or_else(|| {
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
            "synchronous_commit" => {
                let setting = parse_synchronous_commit(value)?;
                session.set_synchronous_commit(setting);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            "allow_in_place_tablespaces" => {
                let setting = parse_bool_setting(name, value, false)?;
                session.set_allow_in_place_tablespaces(setting);
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
            "lock_timeout" => {
                let timeout = parse_lock_timeout(value)?;
                session.set_lock_timeout(timeout);
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

fn parse_synchronous_commit(value: &Option<Vec<String>>) -> PgWireResult<String> {
    let Some(values) = value else {
        return Ok("on".to_string());
    };
    if values.len() != 1 {
        return Err(fe("SET synchronous_commit requires a single value"));
    }
    let normalized = values[0].trim().to_ascii_lowercase();
    match normalized.as_str() {
        "on" | "off" | "local" | "remote_write" | "remote_apply" => Ok(normalized),
        _ => Err(fe_code(
            "22023",
            format!(
                "invalid value for parameter \"synchronous_commit\": \"{}\"",
                values[0]
            ),
        )),
    }
}

fn parse_bool_setting(
    name: &str,
    value: &Option<Vec<String>>,
    default: bool,
) -> PgWireResult<bool> {
    let Some(values) = value else {
        return Ok(default);
    };
    if values.len() != 1 {
        return Err(fe(format!("SET {name} requires a single value")));
    }
    match values[0].trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err(fe_code(
            "22023",
            format!("invalid value for parameter \"{name}\": \"{}\"", values[0]),
        )),
    }
}

fn parse_lock_timeout(value: &Option<Vec<String>>) -> PgWireResult<Option<Duration>> {
    let Some(values) = value else {
        return Ok(None);
    };
    if values.len() != 1 {
        return Err(fe("SET lock_timeout requires a single value"));
    }

    let original = values[0].trim();
    if original.is_empty() {
        return Err(invalid_lock_timeout(""));
    }
    let normalized = original.to_ascii_lowercase();
    if normalized.starts_with('-') {
        return Err(invalid_lock_timeout(original));
    }

    let (raw_qty, raw_unit) = if normalized.chars().any(char::is_whitespace) {
        let mut parts = normalized.split_whitespace();
        let qty = parts.next().unwrap_or_default();
        let unit = parts.next().unwrap_or("ms");
        if parts.next().is_some() {
            return Err(invalid_lock_timeout(original));
        }
        (qty, unit)
    } else {
        let first_non_digit = normalized
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(normalized.len());
        if first_non_digit == 0 {
            return Err(invalid_lock_timeout(original));
        }
        let qty = &normalized[..first_non_digit];
        let unit = if first_non_digit == normalized.len() {
            "ms"
        } else {
            &normalized[first_non_digit..]
        };
        (qty, unit)
    };

    let quantity: u64 = raw_qty
        .parse()
        .map_err(|_| invalid_lock_timeout(original))?;
    let multiplier_ms: u64 = match raw_unit {
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        _ => return Err(invalid_lock_timeout(original)),
    };
    let timeout_ms = quantity
        .checked_mul(multiplier_ms)
        .ok_or_else(|| invalid_lock_timeout(original))?;
    if timeout_ms == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(timeout_ms)))
}

fn format_lock_timeout(timeout: Option<Duration>) -> String {
    let Some(timeout) = timeout else {
        return "0".to_string();
    };
    let timeout_ms = timeout.as_millis();
    if timeout_ms == 0 {
        return "0".to_string();
    }
    if timeout_ms % 1_000 == 0 {
        format!("{}s", timeout_ms / 1_000)
    } else {
        format!("{timeout_ms}ms")
    }
}

fn invalid_lock_timeout(value: &str) -> pgwire::error::PgWireError {
    fe_code(
        "22023",
        format!("invalid value for parameter \"lock_timeout\": \"{value}\""),
    )
}
