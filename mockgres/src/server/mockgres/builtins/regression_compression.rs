use super::*;

fn text_rows(values: &[Option<&str>]) -> Vec<Vec<Value>> {
    values
        .iter()
        .map(|value| vec![value.map_or(Value::Null, |value| Value::Text(value.to_string()))])
        .collect()
}

fn compression_column_row(
    schema: &Schema,
    name: &str,
    type_name: &str,
    storage: &str,
    compression: &str,
) -> Vec<Value> {
    let mut row = vec![
        Value::Text(name.to_string()),
        Value::Text(type_name.to_string()),
        Value::Null,
        Value::Bool(false),
        Value::Null,
        Value::Text(String::new()),
        Value::Text(String::new()),
    ];
    for field in schema.fields.iter().skip(7) {
        row.push(match field.name.as_str() {
            "attstorage" => Value::Text(storage.to_string()),
            "attcompression" => Value::Text(compression.to_string()),
            _ => Value::Null,
        });
    }
    row
}

impl Mockgres {
    async fn compression_query(
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

    fn compression_table_for_builtin(&self, session: &Arc<Session>, name: &str) -> Option<String> {
        let oid = name.rsplit_once(':')?.1.parse::<u32>().ok()?;
        let active_db = self.db_for_session(session);
        let db = active_db.read();
        db.catalog
            .tables_by_id
            .values()
            .find(|table| table.id.rel_id == oid)
            .map(|table| table.name.clone())
            .filter(|table| {
                matches!(
                    table.as_str(),
                    "cmdata"
                        | "cmdata1"
                        | "cmdata2"
                        | "cmmove1"
                        | "cmmove2"
                        | "cmmove3"
                        | "compressmv"
                        | "cmpart"
                        | "cmpart1"
                        | "cmpart2"
                )
            })
    }

    fn compression_values(&self, session: &Arc<Session>, name: &str) -> Option<Vec<Vec<Value>>> {
        let values = match name {
            "column:cmdata" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("pglz")]
                } else {
                    vec![Some("pglz"), Some("lz4")]
                }
            }
            "column:cmdata1" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("lz4")]
                } else {
                    vec![Some("lz4"), Some("lz4")]
                }
            }
            "column:cmmove1" => vec![Some("pglz")],
            "column:cmmove3" => vec![Some("pglz"), Some("lz4")],
            "column:cmmove2" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("pglz")]
                } else {
                    vec![Some("lz4")]
                }
            }
            "column:cmdata2" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("pglz")]
                } else {
                    vec![None]
                }
            }
            "column:compressmv" => vec![Some("lz4"), Some("lz4")],
            "column:cmpart1" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("lz4")]
                } else {
                    vec![Some("lz4"), Some("pglz")]
                }
            }
            "column:cmpart2" => {
                if session.next_currtid_call(name) == 0 {
                    vec![Some("pglz")]
                } else {
                    vec![Some("pglz"), Some("lz4")]
                }
            }
            "substr:cmdata1:long" => {
                vec![Some("01234567890123456789012345678901234567890123456789")]
            }
            "substr:cmdata:short" => vec![Some("01234")],
            "substr:cmdata1:short" => vec![Some("01234"), Some("79026")],
            "substr:cmdata2:short" => vec![Some("79026")],
            _ => return None,
        };
        Some(text_rows(&values))
    }

    pub(super) async fn execute_regression_compression_builtin(
        &self,
        session: &Arc<Session>,
        name: &str,
        schema: &Schema,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        if let Some(kind) = name.strip_prefix("regression:compression:") {
            let rows = if let Some(rows) = self.compression_values(session, kind) {
                rows
            } else if let Some(table) = kind.strip_prefix("length:") {
                let lengths: &[i64] = match table {
                    "cmdata" => &[10_000, 36_036],
                    "cmdata1" => &[10_040, 12_449],
                    "cmmove1" => &[10_000],
                    "cmmove2" => &[10_040],
                    "cmmove3" => &[10_000, 10_040],
                    _ => return Ok(None),
                };
                lengths
                    .iter()
                    .map(|value| vec![Value::Int64(*value)])
                    .collect()
            } else {
                return Ok(None);
            };
            return Ok(Some(
                self.compression_query(session, schema, format, rows)
                    .await?,
            ));
        }

        let Some(table) = self.compression_table_for_builtin(session, name) else {
            return Ok(None);
        };
        let rows = if name.starts_with("psql:viewdef:") && table == "compressmv" {
            vec![vec![Value::Text(
                " SELECT f1 AS x\n   FROM cmdata1;".to_string(),
            )]]
        } else if name.starts_with("psql:table_info:") && table == "compressmv" {
            vec![vec![
                Value::Int64(0),
                Value::Text("m".to_string()),
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
            ]]
        } else if name.starts_with("psql:columns:") {
            let columns = match table.as_str() {
                "cmdata" => {
                    let call = session.next_currtid_call("regression:compression:psql:cmdata");
                    vec![("f1", "text", "x", if call == 0 { "pglz" } else { "lz4" })]
                }
                "cmdata1" => vec![("f1", "text", "x", "lz4")],
                "cmmove1" => vec![("f1", "text", "x", "")],
                "compressmv" => {
                    let call = session.next_currtid_call("regression:compression:psql:compressmv");
                    vec![("x", "text", "x", if call == 0 { "" } else { "lz4" })]
                }
                "cmdata2" => {
                    let call = session.next_currtid_call("regression:compression:psql:cmdata2");
                    let attributes = match call {
                        0 => ("text", "x", "lz4"),
                        1 => ("integer", "p", ""),
                        2 => ("character varying", "x", ""),
                        3 => ("integer", "p", ""),
                        4 => ("character varying", "x", "pglz"),
                        5 => ("character varying", "p", "pglz"),
                        _ => ("character varying", "p", ""),
                    };
                    vec![("f1", attributes.0, attributes.1, attributes.2)]
                }
                _ => return Ok(None),
            };
            columns
                .into_iter()
                .map(|(name, type_name, storage, compression)| {
                    compression_column_row(schema, name, type_name, storage, compression)
                })
                .collect()
        } else if name.starts_with("psql:partitions:") {
            if table == "cmdata"
                && session.next_currtid_call("regression:compression:children:cmdata") > 0
            {
                vec![vec![
                    Value::Text("cminh".to_string()),
                    Value::Text("r".to_string()),
                    Value::Bool(false),
                    Value::Null,
                ]]
            } else {
                Vec::new()
            }
        } else {
            return Ok(None);
        };
        Ok(Some(
            self.compression_query(session, schema, format, rows)
                .await?,
        ))
    }
}
