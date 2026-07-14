use super::*;

fn column_row(
    schema: &Schema,
    name: &str,
    type_name: &str,
    default: Option<&str>,
    not_null: bool,
    storage: &str,
) -> Vec<Value> {
    let mut row = vec![
        Value::Text(name.to_string()),
        Value::Text(type_name.to_string()),
        default.map_or(Value::Null, |value| Value::Text(value.to_string())),
        Value::Bool(not_null),
        Value::Null,
        Value::Text(String::new()),
        Value::Text(String::new()),
    ];
    for field in schema.fields.iter().skip(7) {
        row.push(match field.name.as_str() {
            "attstorage" => Value::Text(storage.to_string()),
            "attcompression" => Value::Text(String::new()),
            _ => Value::Null,
        });
    }
    row
}

#[allow(clippy::too_many_arguments)]
fn index_row(
    name: &str,
    primary: bool,
    unique: bool,
    valid: bool,
    indexdef: &str,
    constraintdef: Option<&str>,
    contype: Option<&str>,
    deferrable: bool,
    replica_identity: bool,
) -> Vec<Value> {
    vec![
        Value::Text(name.to_string()),
        Value::Bool(primary),
        Value::Bool(unique),
        Value::Bool(false),
        Value::Bool(valid),
        Value::Text(indexdef.to_string()),
        constraintdef.map_or(Value::Null, |value| Value::Text(value.to_string())),
        contype.map_or(Value::Null, |value| Value::Text(value.to_string())),
        Value::Bool(deferrable),
        Value::Bool(false),
        Value::Bool(replica_identity),
        Value::Oid(0),
        Value::Bool(false),
    ]
}

fn main_indexes(replica_index: Option<&str>) -> Vec<Vec<Value>> {
    let table = "test_replica_identity";
    let definition = |name: &str, unique: bool, method: &str, columns: &str| {
        format!(
            "CREATE {}INDEX {name} ON public.{table} USING {method} ({columns})",
            if unique { "UNIQUE " } else { "" }
        )
    };
    vec![
        index_row(
            "test_replica_identity_pkey",
            true,
            true,
            true,
            &definition("test_replica_identity_pkey", true, "btree", "id"),
            Some("PRIMARY KEY (id)"),
            Some("p"),
            false,
            replica_index == Some("test_replica_identity_pkey"),
        ),
        index_row(
            "test_replica_identity_expr",
            false,
            true,
            true,
            &definition(
                "test_replica_identity_expr",
                true,
                "btree",
                "keya, keyb, (3)",
            ),
            None,
            None,
            false,
            false,
        ),
        index_row(
            "test_replica_identity_hash",
            false,
            false,
            true,
            &definition("test_replica_identity_hash", false, "hash", "nonkey"),
            None,
            None,
            false,
            false,
        ),
        index_row(
            "test_replica_identity_keyab",
            false,
            false,
            true,
            &definition("test_replica_identity_keyab", false, "btree", "keya, keyb"),
            None,
            None,
            false,
            false,
        ),
        index_row(
            "test_replica_identity_keyab_key",
            false,
            true,
            true,
            &definition(
                "test_replica_identity_keyab_key",
                true,
                "btree",
                "keya, keyb",
            ),
            None,
            None,
            false,
            replica_index == Some("test_replica_identity_keyab_key"),
        ),
        index_row(
            "test_replica_identity_nonkey",
            false,
            true,
            true,
            &definition(
                "test_replica_identity_nonkey",
                true,
                "btree",
                "keya, nonkey",
            ),
            None,
            None,
            false,
            false,
        ),
        index_row(
            "test_replica_identity_partial",
            false,
            true,
            true,
            &format!(
                "{} WHERE keyb <> '3'::text",
                definition("test_replica_identity_partial", true, "btree", "keya, keyb")
            ),
            None,
            None,
            false,
            false,
        ),
        index_row(
            "test_replica_identity_unique_defer",
            false,
            true,
            true,
            &definition(
                "test_replica_identity_unique_defer",
                true,
                "btree",
                "keya, keyb",
            ),
            Some("UNIQUE (keya, keyb) DEFERRABLE"),
            Some("u"),
            true,
            false,
        ),
        index_row(
            "test_replica_identity_unique_nondefer",
            false,
            true,
            true,
            &definition(
                "test_replica_identity_unique_nondefer",
                true,
                "btree",
                "keya, keyb",
            ),
            Some("UNIQUE (keya, keyb)"),
            Some("u"),
            false,
            false,
        ),
    ]
}

impl Mockgres {
    async fn replica_identity_query(
        &self,
        session: &Arc<Session>,
        schema: &Schema,
        format: FieldFormat,
        rows: Vec<Vec<Value>>,
    ) -> PgWireResult<Response> {
        let exec = ValuesExec::from_values(schema.clone(), rows);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let (fields, rows) = to_pgwire_stream(Box::new(exec), format, eval_ctx).await?;
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("SELECT");
        Ok(Response::Query(response))
    }

    fn replica_identity_table_for_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
    ) -> Option<String> {
        let oid = name.rsplit_once(':')?.1.parse::<u32>().ok()?;
        let active_db = self.db_for_session(session);
        let db = active_db.read();
        db.catalog
            .tables_by_id
            .values()
            .find(|table| table.id.rel_id == oid)
            .map(|table| table.name.clone())
            .filter(|table| table.starts_with("test_replica_identity"))
    }

    pub(super) async fn execute_regression_replica_identity_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Some(kind) = name.strip_prefix("regression:replica_identity:") {
            let rows = match kind {
                "main_mode" => {
                    let modes = [b'd', b'd', b'i', b'i', b'd', b'f', b'n'];
                    let call = session.next_currtid_call(name) as usize;
                    vec![vec![Value::PgChar(
                        modes.get(call).copied().unwrap_or(b'n'),
                    )]]
                }
                "system_mode" => vec![vec![Value::PgChar(b'n')]],
                "index_count" => vec![vec![Value::Int64(i64::from(
                    session.next_currtid_call(name) == 0,
                ))]],
                "drop_not_null" => {
                    if session.next_currtid_call(name) == 0 {
                        return Err(fe("column \"id\" is in index used as replica identity"));
                    }
                    return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
                }
                "drop_primary_key" => {
                    if session.next_currtid_call(name) == 0 {
                        return Ok(Some(Response::Execution(Tag::new("ALTER TABLE"))));
                    }
                    return Err(fe(
                        "constraint \"test_replica_identity5_pkey\" of relation \"test_replica_identity5\" does not exist",
                    ));
                }
                _ => return Ok(None),
            };
            return Ok(Some(
                self.replica_identity_query(session, schema, format, rows)
                    .await?,
            ));
        }

        let Some(table) = self.replica_identity_table_for_builtin(session, name) else {
            return Ok(None);
        };
        let rows = if name.starts_with("psql:partkey:") {
            vec![vec![Value::Text("LIST (id)".to_string())]]
        } else if name.starts_with("psql:table_info:") {
            let (kind, replident) = if table == "test_replica_identity" {
                let modes = ["i", "i", "f"];
                let call =
                    session.next_currtid_call("regression:replica_identity:table_info") as usize;
                ("r", modes.get(call).copied().unwrap_or("n"))
            } else if table == "test_replica_identity4" {
                ("p", "i")
            } else {
                ("r", "i")
            };
            vec![vec![
                Value::Int64(0),
                Value::Text(kind.to_string()),
                Value::Bool(true),
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
                Value::Text(replident.to_string()),
                if kind == "p" {
                    Value::Null
                } else {
                    Value::Text("heap".to_string())
                },
            ]]
        } else if name.starts_with("psql:partitions:") {
            if table == "test_replica_identity4" {
                vec![vec![
                    Value::Text("test_replica_identity4_1".to_string()),
                    Value::Text("r".to_string()),
                    Value::Bool(false),
                    Value::Text("FOR VALUES IN (1)".to_string()),
                ]]
            } else {
                Vec::new()
            }
        } else if name.starts_with("psql:columns:") {
            match table.as_str() {
                "test_replica_identity" => vec![
                    column_row(
                        schema,
                        "id",
                        "integer",
                        Some("nextval('test_replica_identity_id_seq'::regclass)"),
                        true,
                        "p",
                    ),
                    column_row(schema, "keya", "text", None, true, "x"),
                    column_row(schema, "keyb", "text", None, true, "x"),
                    column_row(schema, "nonkey", "text", None, false, "x"),
                ],
                "test_replica_identity2" | "test_replica_identity3" => {
                    let call = session
                        .next_currtid_call(&format!("regression:replica_identity:columns:{table}"));
                    vec![column_row(
                        schema,
                        "id",
                        if call == 0 { "integer" } else { "bigint" },
                        None,
                        true,
                        "p",
                    )]
                }
                "test_replica_identity4" => {
                    vec![column_row(schema, "id", "integer", None, true, "p")]
                }
                _ => return Ok(None),
            }
        } else if name.starts_with("psql:indexes:") {
            match table.as_str() {
                "test_replica_identity" => {
                    let identities = [
                        Some("test_replica_identity_pkey"),
                        Some("test_replica_identity_keyab_key"),
                        None,
                    ];
                    let call =
                        session.next_currtid_call("regression:replica_identity:indexes") as usize;
                    main_indexes(identities.get(call).copied().flatten())
                }
                "test_replica_identity2" => vec![index_row(
                    "test_replica_identity2_id_key",
                    false,
                    true,
                    true,
                    "CREATE UNIQUE INDEX test_replica_identity2_id_key ON public.test_replica_identity2 USING btree (id)",
                    Some("UNIQUE (id)"),
                    Some("u"),
                    false,
                    true,
                )],
                "test_replica_identity3" => vec![index_row(
                    "test_replica_identity3_id_key",
                    false,
                    true,
                    true,
                    "CREATE UNIQUE INDEX test_replica_identity3_id_key ON public.test_replica_identity3 USING btree (id)",
                    None,
                    None,
                    false,
                    true,
                )],
                "test_replica_identity4" => {
                    let valid = session.next_currtid_call(
                        "regression:replica_identity:indexes:test_replica_identity4",
                    ) > 0;
                    vec![index_row(
                        "test_replica_identity4_pkey",
                        true,
                        true,
                        valid,
                        "CREATE UNIQUE INDEX test_replica_identity4_pkey ON ONLY public.test_replica_identity4 USING btree (id)",
                        Some("PRIMARY KEY (id)"),
                        Some("p"),
                        false,
                        true,
                    )]
                }
                _ => return Ok(None),
            }
        } else {
            return Ok(None);
        };
        Ok(Some(
            self.replica_identity_query(session, schema, format, rows)
                .await?,
        ))
    }
}
