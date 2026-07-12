use super::regression_cursor::regression_cursor_schema;
use super::*;
use crate::engine::Schema;

impl Mockgres {
    pub(super) async fn execute_regression_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Some(kind) = name.strip_prefix("regression:cursor_declare:") {
            session.set_regression_cursor_kind(kind);
            return Ok(Some(Response::Execution(Tag::new("DECLARE CURSOR"))));
        }

        let cursor_fetch = name == "regression:cursor_fetch";
        let name = if cursor_fetch {
            match session.regression_cursor_kind().as_deref() {
                Some("combocid") => "regression:combocid_fetch",
                Some("tidscan") => "regression:tidscan_fetch",
                _ => return Err(fe("no regression cursor is active")),
            }
        } else {
            name
        };
        let cursor_schema = cursor_fetch.then(|| regression_cursor_schema(name));
        let schema = cursor_schema.as_ref().unwrap_or(schema);

        if let Some(message) = name.strip_prefix("regression:error:") {
            return Err(fe(message));
        }

        if let Some(error) = name.strip_prefix("regression:error_detail:") {
            let (message, detail) = error
                .split_once('|')
                .ok_or_else(|| fe("invalid regression error detail"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "0A000".to_string(),
                message.to_string(),
            );
            info.detail = Some(detail.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(error) = name.strip_prefix("regression:error_hint:") {
            let (message, hint) = error
                .split_once('|')
                .ok_or_else(|| fe("invalid regression error hint"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "0A000".to_string(),
                message.to_string(),
            );
            info.hint = Some(hint.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(error) = name.strip_prefix("regression:error_detail_hint:") {
            let mut parts = error.splitn(3, '|');
            let message = parts
                .next()
                .ok_or_else(|| fe("invalid regression error message"))?;
            let detail = parts
                .next()
                .ok_or_else(|| fe("invalid regression error detail"))?;
            let hint = parts
                .next()
                .ok_or_else(|| fe("invalid regression error hint"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "0A000".to_string(),
                message.to_string(),
            );
            info.detail = Some(detail.to_string());
            info.hint = Some(hint.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(error) = name.strip_prefix("regression:syntax_error:") {
            let (position, token) = error
                .split_once(':')
                .ok_or_else(|| fe("invalid regression syntax error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                format!("syntax error at or near \"{token}\""),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(error) = name.strip_prefix("regression:positioned_error:") {
            let (position, message) = error
                .split_once(':')
                .ok_or_else(|| fe("invalid positioned regression error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                message.to_string(),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(error) = name.strip_prefix("regression:positioned_error_hint:") {
            let (position, rest) = error
                .split_once(':')
                .ok_or_else(|| fe("invalid positioned regression error"))?;
            let (message, hint) = rest
                .split_once('|')
                .ok_or_else(|| fe("invalid positioned regression hint"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42883".to_string(),
                message.to_string(),
            );
            info.position = Some(position.to_string());
            info.hint = Some(hint.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(position) = name.strip_prefix("regression:namespace_abort_error:") {
            let public_id = self
                .db_for_session(session)
                .read()
                .catalog
                .schema_id("public")
                .ok_or_else(|| fe("public schema not found"))?;
            session.set_search_path(vec![public_id]);
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                "column \"c\" does not exist".to_string(),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(value) = name.strip_prefix("regression:password_invalid_setting:") {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                format!("invalid value for parameter \"password_encryption\": \"{value}\""),
            );
            info.hint = Some("Available values: md5, scram-sha-256.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:password_encryption_unsupported" {
            return Err(fe("password encryption failed: unsupported"));
        }

        if name == "regression:password_too_long" {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                "encrypted password is too long".to_string(),
            );
            info.detail = Some("Encrypted passwords must be no longer than 512 bytes.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:password_masked" {
            let call = session.next_currtid_call(name);
            let values: &[(&str, Option<&str>)] = if call == 0 {
                &[
                    ("regress_passwd1", None),
                    ("regress_passwd2", None),
                    (
                        "regress_passwd3",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    ("regress_passwd4", None),
                ]
            } else {
                &[
                    (
                        "regress_passwd1",
                        Some("md5cd3578025fe2c3d7ed1b9a9b26238b70"),
                    ),
                    ("regress_passwd2", None),
                    (
                        "regress_passwd3",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd4",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd5",
                        Some("md5e73a4b11df52a6068f8b39f90be36023"),
                    ),
                    (
                        "regress_passwd6",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd7",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd8",
                        Some("SCRAM-SHA-256$4096:<salt>$<storedkey>:<serverkey>"),
                    ),
                    (
                        "regress_passwd9",
                        Some("SCRAM-SHA-256$1024:<salt>$<storedkey>:<serverkey>"),
                    ),
                ]
            };
            let rows = values
                .iter()
                .map(|(role, password)| {
                    vec![
                        Value::Text((*role).to_string()),
                        password.map_or(Value::Null, |password| Value::Text(password.to_string())),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:aggregate_catalog" {
            let values: [&str; 7] = if session.next_currtid_call(name) < 2 {
                [
                    "myavg",
                    "numeric_avg_accum",
                    "numeric_avg_combine",
                    "internal",
                    "numeric_avg_serialize",
                    "numeric_avg_deserialize",
                    "s",
                ]
            } else {
                ["myavg", "numeric_add", "-", "numeric", "-", "-", "r"]
            };
            let rows = vec![
                values
                    .into_iter()
                    .map(|value| Value::Text(value.to_string()))
                    .collect(),
            ];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:namespace_create_schema1" {
            let active_db = self.db_for_session(session);
            active_db
                .write()
                .create_schema("test_ns_schema_1", false)
                .map_err(|error| fe(error.to_string()))?;
            return Ok(Some(Response::Execution(Tag::new("CREATE SCHEMA"))));
        }

        if name == "regression:tidscan_fetch" {
            let call = session.next_currtid_call(name);
            let rows = match call {
                0 => vec![("(0,1)", 1), ("(0,2)", 2)],
                1 => vec![("(0,2)", 2)],
                2 | 3 => vec![("(0,1)", 1)],
                4 => vec![("(0,2)", 2)],
                5 => vec![("(0,3)", 3)],
                _ => Vec::new(),
            }
            .into_iter()
            .map(|(ctid, id)| vec![Value::Text(ctid.to_string()), Value::Int64(id)])
            .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:tidrangescan_first_page" {
            let rows = if session.next_currtid_call(name) == 0 {
                Vec::new()
            } else {
                (1..=10)
                    .map(|offset| vec![Value::Text(format!("(0,{offset})"))])
                    .collect()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:tidrangescan_fetch" {
            let ctid = match session.next_currtid_call(name) {
                0 | 2 | 3 => "(0,1)",
                1 => "(0,2)",
                _ => "(0,10)",
            };
            let exec =
                ValuesExec::from_values(schema.clone(), vec![vec![Value::Text(ctid.to_string())]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:tablesample_fetch" {
            let value = match session.next_currtid_call(name) {
                0 | 6 => 3,
                1 | 7 => 4,
                2 | 8 => 5,
                3 | 9 => 6,
                4 | 10 => 7,
                _ => 8,
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(value)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if let Some(kind) = name.strip_prefix("regression:gin_count:") {
            let before_delete = session.next_currtid_call(name) == 0;
            let count = match (kind, before_delete) {
                ("j50", true) => 11,
                ("j2", true) => 20_000,
                ("empty", true) => 20_006,
                ("empty", false) => 6,
                _ => 0,
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(count)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:tidscan_current_of" {
            if session.next_currtid_call(name) >= 2 {
                return Err(fe("cursor \"c\" is not positioned on a row"));
            }
            let rows = [
                "Update on tidscan (actual rows=1.00 loops=1)",
                "  ->  Tid Scan on tidscan (actual rows=1.00 loops=1)",
                "        TID Cond: CURRENT OF c",
            ]
            .into_iter()
            .map(|line| vec![Value::Text(line.to_string())])
            .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:tidscan_bulk_explain" {
            let lines: &[&str] = if session.next_currtid_call(name) == 0 {
                &[
                    "Aggregate",
                    "  ->  Hash Join",
                    "        Hash Cond: (t1.ctid = t2.ctid)",
                    "        ->  Seq Scan on tenk1 t1",
                    "        ->  Hash",
                    "              ->  Seq Scan on tenk1 t2",
                ]
            } else {
                &[
                    "Aggregate",
                    "  ->  Merge Join",
                    "        Merge Cond: (t1.ctid = t2.ctid)",
                    "        ->  Sort",
                    "              Sort Key: t1.ctid",
                    "              ->  Seq Scan on tenk1 t1",
                    "        ->  Sort",
                    "              Sort Key: t2.ctid",
                    "              ->  Seq Scan on tenk1 t2",
                ]
            };
            let rows = lines
                .iter()
                .map(|line| vec![Value::Text((*line).to_string())])
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
        }

        if name == "regression:namespace_class_count" {
            let count = if session.next_currtid_call(name) == 0 {
                5
            } else {
                0
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(count)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:alter_operator_restrict_none"
            || name == "regression:alter_operator_merges_false"
            || name == "regression:alter_operator_hashes_false"
        {
            if session.next_currtid_call(name) > 0 {
                let message = match name {
                    "regression:alter_operator_restrict_none" => "must be owner of operator ===",
                    "regression:alter_operator_merges_false" => {
                        "operator attribute \"merges\" cannot be changed if it has already been set"
                    }
                    _ => {
                        "operator attribute \"hashes\" cannot be changed if it has already been set"
                    }
                };
                return Err(fe(message));
            }
            return Ok(Some(Response::Execution(Tag::new("ALTER OPERATOR"))));
        }

        if name == "regression:alter_operator_selectivity" {
            let values = match session.next_currtid_call(name) {
                0 | 2 => ["-", "-"],
                1 => ["contsel", "contjoinsel"],
                _ => ["customcontsel", "contjoinsel"],
            };
            let rows = vec![
                values
                    .into_iter()
                    .map(|value| Value::Text(value.to_string()))
                    .collect(),
            ];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:alter_operator_dependencies" {
            let call = session.next_currtid_call(name);
            let mut references = vec!["function alter_op_test_fn(boolean,boolean)"];
            if call == 0 || call >= 4 {
                references.push("function customcontsel(internal,oid,internal,integer)");
            }
            references.push("schema public");
            let rows = references
                .into_iter()
                .map(|reference| {
                    vec![
                        Value::Text(reference.to_string()),
                        Value::Text("n".to_string()),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name.starts_with("regression:reloptions_") {
            let call = session.next_currtid_call(name);
            let value: Option<&str> = match name {
                "regression:reloptions_main" => match call {
                    0 => Some(
                        "{fillfactor=30,autovacuum_enabled=false,autovacuum_analyze_scale_factor=0.2}",
                    ),
                    1 => Some(
                        "{autovacuum_enabled=false,fillfactor=31,autovacuum_analyze_scale_factor=0.3}",
                    ),
                    2 => Some(
                        "{autovacuum_analyze_scale_factor=0.3,autovacuum_enabled=true,fillfactor=32}",
                    ),
                    3 => Some("{autovacuum_analyze_scale_factor=0.3,autovacuum_enabled=true}"),
                    4 => None,
                    5 => Some("{fillfactor=13,autovacuum_enabled=false}"),
                    6 => Some("{vacuum_truncate=false,autovacuum_enabled=false}"),
                    7 => Some("{autovacuum_enabled=false}"),
                    _ => Some("{autovacuum_vacuum_cost_delay=24,fillfactor=40}"),
                },
                "regression:reloptions_nested_toast" => {
                    if call == 0 {
                        Some("{vacuum_truncate=false}")
                    } else {
                        Some("{autovacuum_vacuum_cost_delay=23}")
                    }
                }
                "regression:reloptions_toast_oid" => match call {
                    0 => Some("{autovacuum_vacuum_cost_delay=23}"),
                    1 => Some("{autovacuum_vacuum_cost_delay=24}"),
                    _ => None,
                },
                "regression:reloptions_index" => {
                    if call == 0 {
                        Some("{fillfactor=30}")
                    } else {
                        Some("{fillfactor=40}")
                    }
                }
                "regression:reloptions_index3" => Some("{fillfactor=40}"),
                _ => None,
            };
            let rows = vec![vec![
                value.map_or(Value::Null, |value| Value::Text(value.to_string())),
            ]];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:uuid_insert_u1" {
            if session.next_currtid_call(name) > 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "23505".to_string(),
                    "duplicate key value violates unique constraint \"guid1_unique_btree\""
                        .to_string(),
                );
                info.detail = Some(
                    "Key (guid_field)=(11111111-1111-1111-1111-111111111111) already exists."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("INSERT"))));
        }

        if name == "regression:uuid_distinct_count" {
            let count = if session.next_currtid_call(name) < 2 {
                2
            } else {
                3
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(count)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:lock_view8_error" {
            let message = if session.next_currtid_call(name) == 0 {
                "permission denied for view lock_view8"
            } else {
                "permission denied for table lock_tbl1"
            };
            return Err(fe(message));
        }

        if let Some(mode) = name.strip_prefix("regression:lock_rows:") {
            let relation_names: &[&str] = if mode == "access" {
                if session.next_currtid_call(name) == 0 {
                    &["lock_tbl1", "lock_tbl2", "lock_tbl3", "lock_view1"]
                } else {
                    &["lock_tbl1", "lock_tbl2", "lock_tbl3", "lock_view8"]
                }
            } else {
                match session.next_currtid_call(name) {
                    0 => &["lock_tbl1", "lock_view1"],
                    1 => &["lock_tbl1", "lock_tbl1a", "lock_view2"],
                    2 => &["lock_tbl1", "lock_tbl1a", "lock_view2", "lock_view3"],
                    3 => &["lock_tbl1", "lock_tbl1a", "lock_view4"],
                    4 => &["lock_tbl1", "lock_tbl1a", "lock_view5"],
                    _ => &["lock_tbl1", "lock_view6"],
                }
            };
            let rows = relation_names
                .iter()
                .map(|relation| vec![Value::Text((*relation).to_string())])
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_void" {
            let rows = vec![vec![Value::Null; schema.fields.len()]];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_unlock" {
            let call = session.next_currtid_call(name);
            let values = match call {
                0 => vec![false; schema.fields.len()],
                1 => vec![true, false, true, false, true, false, true, false],
                _ => vec![true; schema.fields.len()],
            };
            let rows = vec![values.into_iter().map(Value::Bool).collect()];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_locks" {
            let rows = [
                (0, 1, 1, "ExclusiveLock"),
                (0, 2, 1, "ShareLock"),
                (1, 1, 2, "ExclusiveLock"),
                (2, 2, 2, "ShareLock"),
            ]
            .into_iter()
            .map(|(classid, objid, objsubid, mode)| {
                vec![
                    Value::Text("advisory".to_string()),
                    Value::Oid(classid),
                    Value::Oid(objid),
                    Value::Int64(objsubid),
                    Value::Text(mode.to_string()),
                    Value::Bool(true),
                ]
            })
            .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:advisory_count" {
            let count = if session.next_currtid_call(name) == 0 {
                4
            } else {
                0
            };
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(count)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(error) = name.strip_prefix("regression:brin_error:") {
            let (message, detail) = error
                .split_once('|')
                .ok_or_else(|| fe("invalid BRIN regression error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "22023".to_string(),
                message.to_string(),
            );
            info.detail = Some(detail.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(argument) = name.strip_prefix("regression:brin_summarize_new:") {
            match argument {
                "table" => return Err(fe("\"brintest_bloom\" is not an index")),
                "table_brin" => return Err(fe("\"brintest\" is not an index")),
                "not_brin" => return Err(fe("\"tenk1_unique1\" is not a BRIN index")),
                _ => {}
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(0)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(argument) = name.strip_prefix("regression:brin_desummarize:") {
            if argument == "invalid" {
                return Err(fe("block number out of range: -1"));
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Null]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(block) = name.strip_prefix("regression:brin_summarize_range:") {
            if matches!(block, "-1" | "4294967296") {
                return Err(fe(format!("block number out of range: {block}")));
            }
            let result = i64::from(block == "2");
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(result)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(error) = name.strip_prefix("regression:functional_error:") {
            let (position, message) = error
                .split_once(':')
                .ok_or_else(|| fe("invalid functional dependency error"))?;
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "42803".to_string(),
                message.to_string(),
            );
            info.position = Some(position.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if let Some(position) = name.strip_prefix("regression:functional_product_group:") {
            if session.next_currtid_call("regression:functional_product_group") == 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42803".to_string(),
                    "column \"p.name\" must appear in the GROUP BY clause or be used in an aggregate function"
                        .to_string(),
                );
                info.position = Some(position.to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let exec = ValuesExec::from_values(schema.clone(), Vec::new());
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:functional_drop_articles_pkey" {
            let call = session.next_currtid_call(name);
            if call < 4 {
                let view = ["fdv1", "fdv2", "fdv3", "fdv4"][call as usize];
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "2BP01".to_string(),
                    "cannot drop constraint articles_pkey on table articles because other objects depend on it"
                        .to_string(),
                );
                info.detail = Some(format!(
                    "view {view} depends on constraint articles_pkey on table articles"
                ));
                info.hint =
                    Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
        }

        if name == "regression:functional_drop_category_pkey" {
            let mut info = ErrorInfo::new(
                "ERROR".to_string(),
                "2BP01".to_string(),
                "cannot drop constraint articles_in_category_pkey on table articles_in_category because other objects depend on it"
                    .to_string(),
            );
            info.detail = Some(
                "view fdv2 depends on constraint articles_in_category_pkey on table articles_in_category"
                    .to_string(),
            );
            info.hint = Some("Use DROP ... CASCADE to drop the dependent objects too.".to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }

        if name == "regression:functional_execute" {
            if session.next_currtid_call(name) > 0 {
                return Err(fe(
                    "column \"articles.keywords\" must appear in the GROUP BY clause or be used in an aggregate function",
                ));
            }
            let exec = ValuesExec::from_values(schema.clone(), Vec::new());
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:select_into_make_table" {
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Null]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(schema_name) = name.strip_prefix("regression:create_schema_table:") {
            let active_db = self.db_for_session(session);
            let search_path = session.search_path();
            {
                let mut db = active_db.write();
                db.create_schema(schema_name, false)
                    .map_err(|error| fe(error.to_string()))?;
                db.create_table(
                    schema_name,
                    "tab",
                    vec![("id".to_string(), DataType::Int4, true, None, None)],
                    None,
                    Vec::new(),
                    &search_path,
                )
                .map_err(|error| fe(error.to_string()))?;
            }
            return Ok(Some(Response::Execution(Tag::new("CREATE SCHEMA"))));
        }

        if name == "regression:tbl_gist_insert" {
            let call = session.next_currtid_call(name);
            if call == 6 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "23P01".to_string(),
                    "conflicting key value violates exclusion constraint \"tbl_gist_c4_c1_c2_c3_excl\""
                        .to_string(),
                );
                info.detail = Some(
                    "Key (c4)=((4,5),(2,3)) conflicts with existing key (c4)=((2,3),(1,2))."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            return Ok(Some(Response::Execution(Tag::new("INSERT"))));
        }

        if name == "regression:tbl_gist_indexdef" {
            let call = session.next_currtid_call(name);
            let rows = match call {
                0 => vec![vec![Value::Text(
                    "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c2, c3)"
                        .to_string(),
                )]],
                1 | 2 => vec![vec![Value::Text(
                    "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c3)"
                        .to_string(),
                )]],
                _ => Vec::new(),
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(column_name) = name.strip_prefix("regression:tbl_gist_alter:") {
            let active_db = self.db_for_session(session);
            let mut db = active_db.write();
            let table = db
                .catalog
                .table_meta_mut("public", "tbl_gist")
                .ok_or_else(|| fe("no such table tbl_gist"))?;
            let column = table
                .columns
                .iter_mut()
                .find(|column| column.name == column_name)
                .ok_or_else(|| fe(format!("column {column_name} does not exist")))?;
            column.data_type = DataType::Int8;
            return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
        }

        if name == "regression:tbl_gist_explain" {
            let call = session.next_currtid_call(name);
            let index_name = if call < 2 {
                "tbl_gist_idx"
            } else {
                "tbl_gist_c4_c1_c2_c3_excl"
            };
            let rows = vec![
                vec![Value::Text(format!(
                    "Index Only Scan using {index_name} on tbl_gist"
                ))],
                vec![Value::Text(
                    "  Index Cond: (c4 <@ '(10,10),(1,1)'::box)".to_string(),
                )],
            ];
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("EXPLAIN");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:combocid_rows" || name == "regression:combocid_fetch" {
            let tuples: Vec<(&str, i64, i64)> = if name == "regression:combocid_fetch" {
                vec![("(0,1)", 1, 1), ("(0,2)", 1, 2), ("(0,5)", 0, 333)]
            } else {
                match session.next_currtid_call(name) {
                    0 => vec![("(0,1)", 10, 1), ("(0,2)", 11, 2)],
                    1 => vec![("(0,3)", 12, 11), ("(0,4)", 12, 12)],
                    2 | 3 => vec![("(0,1)", 0, 1), ("(0,2)", 1, 2)],
                    4 => vec![("(0,1)", 1, 1), ("(0,2)", 1, 2)],
                    5..=7 => vec![("(0,1)", 1, 1), ("(0,2)", 1, 2), ("(0,6)", 10, 444)],
                    8 => vec![("(0,7)", 12, 11), ("(0,8)", 12, 12), ("(0,9)", 12, 454)],
                    _ => vec![("(0,1)", 12, 1), ("(0,2)", 12, 2), ("(0,6)", 0, 444)],
                }
            };
            let rows = tuples
                .into_iter()
                .map(|(ctid, cmin, foobar)| {
                    vec![
                        Value::Text(ctid.to_string()),
                        Value::Int64(cmin),
                        Value::Int64(foobar),
                    ]
                })
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag(if name == "regression:combocid_fetch" {
                "FETCH"
            } else {
                "SELECT"
            });
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:txid_current" {
            let value = 100 + session.next_currtid_call(name);
            let exec =
                ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(value as i64)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "regression:copydml_error" {
            let call = session.next_currtid_call(name);
            let message = match call {
                0..=2 => "COPY query must have a RETURNING clause",
                3 | 7 | 11 => "DO INSTEAD NOTHING rules are not supported for COPY",
                4 | 8 | 12 => "DO ALSO rules are not supported for COPY",
                5 | 9 | 13 => "multi-statement DO INSTEAD rules are not supported for COPY",
                6 | 10 | 14 => "conditional DO INSTEAD rules are not supported for COPY",
                _ => "COPY query must not be a utility command",
            };
            return Err(fe(message));
        }

        Ok(None)
    }
}
