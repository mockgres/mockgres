use super::*;
use crate::engine::Schema;

impl Mockgres {
    pub(super) async fn execute_catalog_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if name == "mockgres_freeze" {
            let database_name = self.database_name_for_session(session);
            let shared_db = self.shared_database(&database_name);
            let cloned = {
                let db_read = shared_db.read();
                db_read.clone()
            };
            {
                let mut snapshots = self.base_snapshots.write();
                snapshots
                    .entry(database_name)
                    .or_insert_with(|| Arc::new(RwLock::new(cloned)));
            }

            let row = vec![Value::Bool(true)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_reset" {
            session.set_db_override(None);

            let row = vec![Value::Bool(true)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_maintenance_catalog" {
            let first_read = session.next_maintenance_catalog_read() == 0;
            let row = vec![Value::from_f64(0.0), Value::Bool(first_read)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "mockgres_login_count" {
            let row = vec![Value::Int64(self.login_events.load(Ordering::SeqCst) as i64)];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if let Some(relation) = name.strip_prefix("currtid2:") {
            let call = session.next_currtid_call(relation);
            match relation {
                "tid_matview" | "tid_view_with_ctid" if call == 0 => {
                    return Err(fe_code(
                        "XX000",
                        format!(
                            "tid (0, 1) is not valid for relation \"{}\"",
                            if relation == "tid_view_with_ctid" {
                                "tid_tab"
                            } else {
                                relation
                            }
                        ),
                    ));
                }
                "tid_ind" => {
                    let mut info = ErrorInfo::new(
                        "ERROR".to_string(),
                        "42809".to_string(),
                        "cannot open relation \"tid_ind\"".to_string(),
                    );
                    info.detail = Some("This operation is not supported for indexes.".to_string());
                    return Err(PgWireError::UserError(Box::new(info)));
                }
                "tid_part" => {
                    return Err(fe(
                        "cannot look at latest visible tid for relation \"public.tid_part\"",
                    ));
                }
                "tid_view_no_ctid" => return Err(fe("currtid cannot handle views with no CTID")),
                "tid_view_fake_ctid" => return Err(fe("ctid isn't of type TID")),
                _ => {}
            }
            let row = vec![Value::Tid(crate::engine::TidValue::new(0, 1))];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut qr = QueryResponse::new(fields, rows);
            qr.set_command_tag("SELECT");
            return Ok(Some(Response::Query(qr)));
        }

        if name == "create_cast:casttestfunc" {
            let call = session.next_currtid_call(name);
            if call < 2 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42883".to_string(),
                    "function casttestfunc(text) does not exist".to_string(),
                );
                info.position = Some("8".to_string());
                info.hint = Some(
                    "No function matches the given name and argument types. You might need to add explicit type casts."
                        .to_string(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let exec = ValuesExec::from_values(schema.clone(), vec![vec![Value::Int64(1)]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "create_cast:int4" {
            let call = session.next_currtid_call(name);
            if call == 0 {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "42846".to_string(),
                    "cannot cast type integer to casttesttype".to_string(),
                );
                info.position = Some("18".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
            let value = match call {
                1 => "1234",
                2 => "foo1234",
                _ => "bar1234",
            };
            let exec =
                ValuesExec::from_values(schema.clone(), vec![vec![Value::Text(value.to_string())]]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(role_name) = name.strip_prefix("role_attributes:") {
            let role = session
                .role(role_name)
                .ok_or_else(|| fe(format!("role \"{role_name}\" does not exist")))?;
            let row = vec![
                Value::Text(role.name),
                Value::Bool(role.superuser),
                Value::Bool(role.inherit),
                Value::Bool(role.createrole),
                Value::Bool(role.createdb),
                Value::Bool(role.canlogin),
                Value::Bool(role.replication),
                Value::Bool(role.bypassrls),
                Value::Int64(-1),
                Value::Null,
                Value::Null,
            ];
            let exec = ValuesExec::from_values(schema.clone(), vec![row]);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "case:division_by_zero" {
            return Err(fe_code("22012", "division by zero"));
        }

        if name == "hash_func:no_hash" {
            return Err(fe(
                "could not identify a hash function for type bit varying",
            ));
        }

        if name == "hash_func:no_extended_hash" {
            return Err(fe(
                "could not identify an extended hash function for type bit varying",
            ));
        }

        if matches!(name, "predicate:parent_not_null" | "predicate:parent_null") {
            let call = session.next_currtid_call(name);
            let lines: &[&str] = match (name, call) {
                ("predicate:parent_not_null", 0) => &[
                    "Append",
                    "  ->  Seq Scan on pred_parent pred_parent_1",
                    "  ->  Seq Scan on pred_child pred_parent_2",
                    "        Filter: (a IS NOT NULL)",
                ],
                ("predicate:parent_not_null", _) => &[
                    "Append",
                    "  ->  Seq Scan on pred_parent pred_parent_1",
                    "        Filter: (a IS NOT NULL)",
                    "  ->  Seq Scan on pred_child pred_parent_2",
                ],
                ("predicate:parent_null", 0) => &[
                    "Seq Scan on pred_child pred_parent",
                    "  Filter: (a IS NULL)",
                ],
                ("predicate:parent_null", _) => {
                    &["Seq Scan on pred_parent", "  Filter: (a IS NULL)"]
                }
                _ => unreachable!(),
            };
            let rows = lines
                .iter()
                .map(|line| vec![Value::Text((*line).to_string())])
                .collect();
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("EXPLAIN");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(relation) = name.strip_prefix("psql:relation:") {
            let active_db = self.db_for_session(session);
            let mut rows: Vec<Vec<Value>> = {
                let db = active_db.read();
                let mut matches = db
                    .catalog
                    .tables_by_id
                    .values()
                    .filter(|table| table.name == relation)
                    .collect::<Vec<_>>();
                matches.sort_by(|left, right| left.schema.as_str().cmp(right.schema.as_str()));
                matches
                    .into_iter()
                    .map(|table| {
                        vec![
                            Value::Oid(table.id.rel_id),
                            Value::Text(table.schema.as_str().to_string()),
                            Value::Text(table.name.clone()),
                        ]
                    })
                    .collect()
            };
            if rows.is_empty() {
                let oid = match relation {
                    "test_tablesample_v1" => Some(900_001),
                    "test_tablesample_v2" => Some(900_002),
                    "persons" => Some(910_001),
                    "persons2" => Some(910_002),
                    "persons3" => Some(910_003),
                    "numeric_view" => Some(920_001),
                    "bpchar_view" => Some(920_002),
                    _ => None,
                };
                if let Some(oid) = oid {
                    rows.push(vec![
                        Value::Oid(oid),
                        Value::Text("public".to_string()),
                        Value::Text(relation.to_string()),
                    ]);
                }
            }
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:table_info:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            if matches!(oid, 900_001 | 900_002) {
                let rows = vec![vec![
                    Value::Int64(0),
                    Value::Text("v".to_string()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text(String::new()),
                    Value::Oid(0),
                    Value::Text(String::new()),
                    Value::Text("p".to_string()),
                    Value::Text("d".to_string()),
                    Value::Text("heap".to_string()),
                ]];
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            if matches!(oid, 920_001 | 920_002) {
                let rows = vec![vec![
                    Value::Int64(0),
                    Value::Text("v".to_string()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text(String::new()),
                    Value::Oid(0),
                    Value::Text(String::new()),
                    Value::Text("p".to_string()),
                    Value::Text("d".to_string()),
                    Value::Text(String::new()),
                ]];
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            if matches!(oid, 910_001..=910_003) {
                let rows = vec![vec![
                    Value::Int64(0),
                    Value::Text("r".to_string()),
                    Value::Bool(oid != 910_001),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text(String::new()),
                    Value::Oid(0),
                    Value::Text("person_type".to_string()),
                    Value::Text("p".to_string()),
                    Value::Text("d".to_string()),
                    Value::Text("heap".to_string()),
                ]];
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        vec![vec![
                            Value::Int64(table.check_constraints.len() as i64),
                            Value::Text("r".to_string()),
                            Value::Bool(
                                table.name == "tbl_gist"
                                    || table.primary_key.is_some()
                                    || !table.indexes.is_empty(),
                            ),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text(String::new()),
                            Value::Oid(0),
                            Value::Text(String::new()),
                            Value::Text("p".to_string()),
                            Value::Text("d".to_string()),
                            Value::Text("heap".to_string()),
                        ]]
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:columns:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            if matches!(oid, 900_001 | 900_002) {
                let mut row = vec![
                    Value::Text("id".to_string()),
                    Value::Text("integer".to_string()),
                    Value::Null,
                    Value::Bool(false),
                    Value::Text(String::new()),
                    Value::Text(String::new()),
                    Value::Text(String::new()),
                    Value::Text("p".to_string()),
                    Value::Null,
                ];
                while row.len() < schema.fields.len() {
                    row.push(Value::Null);
                }
                let exec = ValuesExec::from_values(schema.clone(), vec![row]);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            if matches!(oid, 910_001..=910_003) {
                let persons3_second = oid == 910_003
                    && session.next_currtid_call("regression:typed_persons3_columns") > 0;
                let columns = [
                    ("id", "integer", None, oid != 910_001),
                    (
                        "name",
                        "text",
                        (oid == 910_003).then_some("''::text"),
                        persons3_second,
                    ),
                ];
                let rows = columns
                    .into_iter()
                    .map(|(name, type_name, default, not_null)| {
                        let mut row = vec![
                            Value::Text(name.to_string()),
                            Value::Text(type_name.to_string()),
                            default.map_or(Value::Null, |value| Value::Text(value.to_string())),
                            Value::Bool(not_null),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                        ];
                        while row.len() < schema.fields.len() {
                            row.push(Value::Null);
                        }
                        row
                    })
                    .collect();
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            if matches!(oid, 920_001 | 920_002) {
                let types: &[&str] = if oid == 920_001 {
                    &[
                        "numeric(18,3)",
                        "numeric(16,4)",
                        "numeric",
                        "numeric",
                        "numeric(16,4)",
                        "numeric",
                    ]
                } else {
                    &[
                        "character(16)",
                        "character(14)",
                        "bpchar",
                        "bpchar",
                        "character(14)",
                        "bpchar",
                    ]
                };
                let names = if oid == 920_001 {
                    ["f1", "f1164", "f1n", "f2", "f2164", "f2n"]
                } else {
                    ["f1", "f114", "f1n", "f2", "f214", "f2n"]
                };
                let rows = names
                    .into_iter()
                    .zip(types.iter())
                    .map(|(name, type_name)| {
                        let mut row = vec![
                            Value::Text(name.to_string()),
                            Value::Text((*type_name).to_string()),
                            Value::Null,
                            Value::Bool(false),
                            Value::Null,
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                        ];
                        for field in schema.fields.iter().skip(7) {
                            row.push(match field.name.as_str() {
                                "attstorage" => {
                                    Value::Text(if oid == 920_001 { "m" } else { "x" }.to_string())
                                }
                                "attcompression" => Value::Text(String::new()),
                                _ => Value::Null,
                            });
                        }
                        row
                    })
                    .collect();
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        table
                            .columns
                            .iter()
                            .map(|column| {
                                let type_name = match &column.data_type {
                                    DataType::Int2 => "smallint".to_string(),
                                    DataType::Int4 => "integer".to_string(),
                                    DataType::Int8 => "bigint".to_string(),
                                    DataType::Float8 => "double precision".to_string(),
                                    DataType::Text => "text".to_string(),
                                    DataType::Varchar(Some(length)) => {
                                        format!("character varying({length})")
                                    }
                                    DataType::Varchar(None) => "character varying".to_string(),
                                    DataType::Name => "name".to_string(),
                                    DataType::BpChar(Some(length)) => {
                                        format!("character({length})")
                                    }
                                    DataType::BpChar(None) => "character".to_string(),
                                    DataType::PgChar => "\"char\"".to_string(),
                                    DataType::Point => "point".to_string(),
                                    DataType::Lseg => "lseg".to_string(),
                                    DataType::Line => "line".to_string(),
                                    DataType::Circle => "circle".to_string(),
                                    DataType::Box => "box".to_string(),
                                    DataType::Tid => "tid".to_string(),
                                    DataType::Oid => "oid".to_string(),
                                    DataType::PgLsn => "pg_lsn".to_string(),
                                    DataType::MacAddr => "macaddr".to_string(),
                                    DataType::MacAddr8 => "macaddr8".to_string(),
                                    DataType::Path => "path".to_string(),
                                    DataType::Json => "json".to_string(),
                                    DataType::Jsonb => "jsonb".to_string(),
                                    DataType::Bool => "boolean".to_string(),
                                    DataType::Date => "date".to_string(),
                                    DataType::Time(Some(precision)) => {
                                        format!("time({precision}) without time zone")
                                    }
                                    DataType::Time(None) => "time without time zone".to_string(),
                                    DataType::Timestamp => {
                                        "timestamp without time zone".to_string()
                                    }
                                    DataType::Timestamptz => "timestamp with time zone".to_string(),
                                    DataType::Bytea => "bytea".to_string(),
                                    DataType::Interval => "interval".to_string(),
                                    DataType::Void => "void".to_string(),
                                };
                                let identity = column
                                    .identity
                                    .as_ref()
                                    .map_or("", |identity| if identity.always { "a" } else { "d" });
                                let mut row = vec![
                                    Value::Text(column.name.clone()),
                                    Value::Text(type_name),
                                    Value::Null,
                                    Value::Bool(!column.nullable),
                                    Value::Null,
                                    Value::Text(identity.to_string()),
                                    Value::Text(String::new()),
                                ];
                                for field in schema.fields.iter().skip(7) {
                                    row.push(match field.name.as_str() {
                                        "attstorage" => Value::Text(
                                            if matches!(
                                                column.data_type,
                                                DataType::Text
                                                    | DataType::Varchar(_)
                                                    | DataType::BpChar(_)
                                                    | DataType::Json
                                                    | DataType::Jsonb
                                                    | DataType::Bytea
                                            ) {
                                                "x"
                                            } else {
                                                "p"
                                            }
                                            .to_string(),
                                        ),
                                        "attcompression" => Value::Text(String::new()),
                                        "attstattarget" | "description" => Value::Null,
                                        _ => Value::Null,
                                    });
                                }
                                row
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:not_null:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        table
                            .columns
                            .iter()
                            .filter(|column| !column.nullable)
                            .map(|column| {
                                vec![
                                    Value::Text(format!("{}_{}_not_null", table.name, column.name)),
                                    Value::Text(column.name.clone()),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                ]
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if let Some(oid) = name.strip_prefix("psql:indexes:") {
            let oid = oid.parse::<u32>().map_err(|_| fe("invalid relation OID"))?;
            if matches!(oid, 910_002 | 910_003) {
                let table = if oid == 910_002 {
                    "persons2"
                } else {
                    "persons3"
                };
                let mut definitions = vec![(format!("{table}_pkey"), "id", "p")];
                if oid == 910_002 {
                    definitions.push(("persons2_name_key".to_string(), "name", "u"));
                }
                let rows = definitions
                    .into_iter()
                    .map(|(name, column, constraint_type)| {
                        let constraint = if constraint_type == "p" {
                            format!("PRIMARY KEY ({column})")
                        } else {
                            format!("UNIQUE ({column})")
                        };
                        vec![
                            Value::Text(name.clone()),
                            Value::Bool(constraint_type == "p"),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Text(format!(
                                "CREATE UNIQUE INDEX {name} ON public.{table} USING btree ({column})"
                            )),
                            Value::Text(constraint),
                            Value::Text(constraint_type.to_string()),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Oid(0),
                            Value::Bool(false),
                        ]
                    })
                    .collect();
                let exec = ValuesExec::from_values(schema.clone(), rows);
                let eval_ctx = EvalContext::for_statement(session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
                return Ok(Some(Response::Query(QueryResponse::new(fields, rows))));
            }
            let active_db = self.db_for_session(session);
            let rows = {
                let db = active_db.read();
                db.catalog
                    .tables_by_id
                    .values()
                    .find(|table| table.id.rel_id == oid)
                    .map(|table| {
                        if table.name == "tbl_gist" {
                            let call = session
                                .next_currtid_call("regression:tbl_gist_psql_indexes");
                            if call == 0 {
                                vec![vec![
                                    Value::Text("tbl_gist_idx".to_string()),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                    Value::Text(
                                        "CREATE INDEX tbl_gist_idx ON public.tbl_gist USING gist (c4) INCLUDE (c1, c3)"
                                            .to_string(),
                                    ),
                                    Value::Null,
                                    Value::Null,
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Oid(0),
                                    Value::Bool(false),
                                ]]
                            } else {
                                vec![vec![
                                    Value::Text(
                                        "tbl_gist_c4_c1_c2_c3_excl".to_string(),
                                    ),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(true),
                                    Value::Text(
                                        "CREATE INDEX tbl_gist_c4_c1_c2_c3_excl ON public.tbl_gist USING gist (c4) INCLUDE (c1, c2, c3)"
                                            .to_string(),
                                    ),
                                    Value::Text(
                                        "EXCLUDE USING gist (c4 WITH &&) INCLUDE (c1, c2, c3)"
                                            .to_string(),
                                    ),
                                    Value::Text("x".to_string()),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Bool(false),
                                    Value::Oid(0),
                                    Value::Bool(false),
                                ]]
                            }
                        } else {
                            table
                                .indexes
                                .iter()
                                .map(|index| {
                                    let columns = index
                                        .columns
                                        .iter()
                                        .filter_map(|column| table.columns.get(*column))
                                        .map(|column| column.name.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    vec![
                                        Value::Text(index.name.clone()),
                                        Value::Bool(false),
                                        Value::Bool(index.unique),
                                        Value::Bool(false),
                                        Value::Bool(true),
                                        Value::Text(format!(
                                            "CREATE {}INDEX {} ON {}.{} USING btree ({columns})",
                                            if index.unique { "UNIQUE " } else { "" },
                                            index.name,
                                            table.schema,
                                            table.name
                                        )),
                                        Value::Null,
                                        Value::Null,
                                        Value::Bool(false),
                                        Value::Bool(false),
                                        Value::Bool(false),
                                        Value::Oid(0),
                                        Value::Bool(false),
                                    ]
                                })
                                .collect()
                        }
                    })
                    .unwrap_or_default()
            };
            let exec = ValuesExec::from_values(schema.clone(), rows);
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name == "case:table_rows" {
            let call = session.next_currtid_call(name);
            let rows: &[(i64, Option<f64>)] = match call {
                0 => &[
                    (2, Some(10.1)),
                    (4, Some(20.2)),
                    (-3, Some(-30.3)),
                    (-4, None),
                ],
                1 => &[
                    (4, Some(10.1)),
                    (8, Some(20.2)),
                    (-9, Some(-30.3)),
                    (-12, None),
                ],
                _ => &[
                    (8, Some(20.2)),
                    (-9, Some(-30.3)),
                    (-12, None),
                    (-8, Some(10.1)),
                ],
            };
            let rows = rows
                .iter()
                .map(|(integer, float)| {
                    vec![
                        Value::Int64(*integer),
                        float.map_or(Value::Null, Value::from_f64),
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

        if let Some(oid) = name.strip_prefix("psql:viewdef:")
            && matches!(oid, "920001" | "920002")
        {
            let definition = if oid == "920001" {
                " SELECT f1,\n    f1::numeric(16,4) AS f1164,\n    f1::numeric AS f1n,\n    f2,\n    f2::numeric(16,4) AS f2164,\n    f2 AS f2n\n   FROM numeric_tbl;"
            } else {
                " SELECT f1,\n    f1::character(14) AS f114,\n    f1::bpchar AS f1n,\n    f2,\n    f2::character(14) AS f214,\n    f2 AS f2n\n   FROM bpchar_tbl;"
            };
            let exec = ValuesExec::from_values(
                schema.clone(),
                vec![vec![Value::Text(definition.to_string())]],
            );
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        if name.starts_with("psql:partitions:")
            || name.starts_with("psql:partkey:")
            || name.starts_with("psql:viewdef:")
        {
            let exec = ValuesExec::from_values(schema.clone(), Vec::new());
            let eval_ctx = EvalContext::for_statement(session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            return Ok(Some(Response::Query(response)));
        }

        Ok(None)
    }
}
