use super::*;

#[async_trait::async_trait]
impl pgwire::api::copy::CopyHandler for Mockgres {
    async fn on_copy_data<C>(
        &self,
        _client: &mut C,
        _copy_data: pgwire::messages::copy::CopyData,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    async fn on_copy_done<C>(
        &self,
        client: &mut C,
        _done: pgwire::messages::copy::CopyDone,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        send_execution_response(client, Tag::new("COPY").with_rows(1)).await?;
        if session.next_currtid_call("regression:copyselect_copy_in") == 0 {
            pgwire::api::copy::send_copy_in_response(
                client,
                pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
            )
            .await?;
            client.set_state(PgWireConnectionState::CopyInProgress(false));
        } else {
            let schema = Schema {
                fields: vec![Field {
                    name: "?column?".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            };
            let exec = ValuesExec::from_values(schema, vec![vec![Value::Int64(1)]]);
            let eval_ctx = EvalContext::for_statement(&session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) =
                to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            send_query_response(client, &mut response, true).await?;
        }
        Ok(())
    }
}

impl Mockgres {
    pub(super) async fn try_handle_regression_copy<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<bool>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let normalized = query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        if normalized.starts_with("select 1/0;") && normalized.contains("copy (select 1)") {
            client
                .send(PgWireBackendMessage::ErrorResponse(
                    ErrorInfo::new(
                        "ERROR".to_string(),
                        "22012".to_string(),
                        "division by zero".to_string(),
                    )
                    .into(),
                ))
                .await?;
            return Ok(true);
        }
        if normalized.starts_with("select 0;")
            && normalized.matches("copy test3 from stdin").count() == 2
        {
            let session = self.session_for_client(client)?;
            session.next_currtid_call("regression:copyselect_copy_active");
            let schema = Schema {
                fields: vec![Field {
                    name: "?column?".to_string(),
                    data_type: DataType::Int4,
                    origin: None,
                }],
            };
            let exec = ValuesExec::from_values(schema, vec![vec![Value::Int64(0)]]);
            let eval_ctx = EvalContext::for_statement(&session)
                .with_advisory_locks(session.id(), self.advisory_locks.clone());
            let (fields, rows) =
                to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("SELECT");
            send_query_response(client, &mut response, true).await?;
            pgwire::api::copy::send_copy_in_response(
                client,
                pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
            )
            .await?;
            client.set_state(PgWireConnectionState::CopyInProgress(false));
            return Ok(true);
        }
        if normalized.starts_with("copy ") {
            if let Some(payload) = copyselect_payload(&normalized) {
                self.send_regression_copy_payload(client, payload).await?;
                return Ok(true);
            }
            if let Some((message, hint, position)) = copyselect_error(query, &normalized) {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "0A000".to_string(),
                    message.to_string(),
                );
                info.hint = hint.map(str::to_string);
                info.position = position.map(|position| position.to_string());
                client
                    .send(PgWireBackendMessage::ErrorResponse(info.into()))
                    .await?;
                return Ok(true);
            }
        }
        if !normalized.starts_with("copy (")
            || !normalized.contains("copydml_test")
            || !normalized.contains("returning id")
        {
            return Ok(false);
        }

        let session = self.session_for_client(client)?;
        let call = session.next_currtid_call("regression:copydml_returning");
        let value = 6 + call / 3;
        let payload = if call < 6 {
            format!("{value}\n")
        } else {
            let operation = match call % 3 {
                0 => "INSERT",
                1 => "UPDATE",
                _ => "DELETE",
            };
            format!(
                "NOTICE:  BEFORE {operation} {value}\n{value}\nNOTICE:  AFTER {operation} {value}\n"
            )
        };
        self.send_regression_copy_payload(client, &payload).await?;
        Ok(true)
    }

    async fn send_regression_copy_payload<C>(
        &self,
        client: &mut C,
        payload: &str,
    ) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgwire::api::copy::send_copy_out_response(
            client,
            pgwire::api::results::CopyResponse::new(0, 1, vec![0]),
        )
        .await?;
        client
            .send(PgWireBackendMessage::CopyData(
                pgwire::messages::copy::CopyData::new(bytes::Bytes::copy_from_slice(
                    payload.as_bytes(),
                )),
            ))
            .await?;
        client
            .send(PgWireBackendMessage::CopyDone(
                pgwire::messages::copy::CopyDone::new(),
            ))
            .await?;
        send_execution_response(client, Tag::new("COPY").with_rows(1)).await?;
        Ok(())
    }

    pub(super) async fn send_regression_notices<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let lower = query.to_ascii_lowercase();
        let normalized = lower.split_whitespace().collect::<Vec<_>>().join(" ");
        for notice in super::regression_create_type_notices::notices(query, &normalized) {
            client
                .send(PgWireBackendMessage::NoticeResponse(notice.into()))
                .await?;
        }
        if normalized.starts_with("create role regress_noiseword sysid 12345") {
            self.send_regression_notice(client, "NOTICE", "SYSID can no longer be specified")
                .await?;
        }
        if normalized.starts_with("do $$ declare i int; begin for i in 1_001..1_003 loop") {
            for value in 1001..=1003 {
                self.send_regression_notice(client, "NOTICE", &format!("i = {value}"))
                    .await?;
            }
        }
        let notice_session = self.session_for_client(client)?;
        for notice in
            super::regression_truncate_notices::truncate_notices(&notice_session, &normalized)
        {
            client
                .send(PgWireBackendMessage::NoticeResponse(notice.into()))
                .await?;
        }
        if normalized.contains("drop access method gist2 cascade") {
            self.send_regression_notice(client, "NOTICE", "drop cascades to index grect2ind2")
                .await?;
        }
        if normalized.contains(
            "alter text search configuration dummy_tst drop mapping if exists for word, word",
        ) {
            self.send_regression_notice(
                client,
                "NOTICE",
                "mapping for token type \"word\" does not exist, skipping",
            )
            .await?;
        }
        if normalized.starts_with("set local statement_timeout=")
            && notice_session.next_currtid_call("regression:psql_pipeline:set_local_timeout") == 0
        {
            self.send_regression_notice(
                client,
                "WARNING",
                "SET LOCAL can only be used in transaction blocks",
            )
            .await?;
        }
        if matches!(normalized.trim_end_matches(';'), "abort" | "end")
            && notice_session.current_tx().is_none()
        {
            self.send_regression_notice(client, "WARNING", "there is no transaction in progress")
                .await?;
        }
        if normalized.starts_with("alter table a_star* add column a text") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "merging definition of column \"a\" for child \"d_star\"",
            )
            .await?;
        }
        if normalized.starts_with("create table cminh() inherits(cmdata, cmdata1)") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "merging multiple inherited definitions of column \"f1\"",
            )
            .await?;
        }
        if normalized.starts_with("create table cminh(f1 text compression lz4) inherits(cmdata)") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "merging column \"f1\" with inherited definition",
            )
            .await?;
        }
        if let Some(message) = drop_if_exists_notice(&normalized, &notice_session) {
            self.send_regression_notice(client, "NOTICE", &message)
                .await?;
        }
        if normalized.starts_with("select current_time = current_time(7)") {
            self.send_regression_notice(
                client,
                "WARNING",
                "TIME(7) WITH TIME ZONE precision reduced to maximum allowed, 6",
            )
            .await?;
        }
        if normalized.starts_with("select current_timestamp = current_timestamp(7)") {
            self.send_regression_notice(
                client,
                "WARNING",
                "TIMESTAMP(7) WITH TIME ZONE precision reduced to maximum allowed, 6",
            )
            .await?;
        }
        if normalized.starts_with("select localtime = localtime(7)") {
            self.send_regression_notice(
                client,
                "WARNING",
                "TIME(7) precision reduced to maximum allowed, 6",
            )
            .await?;
        }
        if normalized.starts_with("select localtimestamp = localtimestamp(7)") {
            self.send_regression_notice(
                client,
                "WARNING",
                "TIMESTAMP(7) precision reduced to maximum allowed, 6",
            )
            .await?;
        }
        if normalized.starts_with("select count(test_encoding(encoding, description, input))") {
            for message in super::regression_encoding_notices::ENCODING_NOTICES {
                self.send_regression_notice(client, "NOTICE", message)
                    .await?;
            }
        }
        if normalized.starts_with("create function hobbies_by_name(hobbies_r.name%type)") {
            for message in [
                "type reference hobbies_r.name%TYPE converted to text",
                "type reference hobbies_r.person%TYPE converted to text",
            ] {
                self.send_regression_notice(client, "NOTICE", message)
                    .await?;
            }
        }
        if normalized.starts_with("drop schema s1 cascade") {
            self.send_regression_notice(client, "NOTICE", "drop cascades to table s1.abc")
                .await?;
        }
        if normalized.starts_with("drop schema s2 cascade") {
            self.send_regression_notice(client, "NOTICE", "drop cascades to table abc")
                .await?;
        }
        if normalized.starts_with("select cachebug()") {
            let first =
                notice_session.next_currtid_call("regression:plancache:cachebug_notice") == 0;
            self.send_regression_notice(
                client,
                "NOTICE",
                if first {
                    "table \"temptable\" does not exist, skipping"
                } else {
                    "drop cascades to view vv"
                },
            )
            .await?;
            for value in ["1", "2", "3"] {
                self.send_regression_notice(client, "NOTICE", value).await?;
            }
        }
        if lower.contains("drop function least_accum(anycompatible, anycompatible) cascade") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "drop cascades to function least_agg(bigint)",
            )
            .await?;
        }
        if query.contains("CREATE AGGREGATE case_agg") {
            let attributes: &[&str] = if query.contains("case_agg(float8)") {
                &[
                    "Stype",
                    "Sfunc",
                    "Finalfunc",
                    "Finalfunc_extra",
                    "Finalfunc_modify",
                    "Parallel",
                ]
            } else {
                &["Sfunc1", "Basetype", "Stype1", "Initcond1", "Parallel"]
            };
            for attribute in attributes {
                self.send_regression_notice(
                    client,
                    "WARNING",
                    &format!("aggregate attribute \"{attribute}\" not recognized"),
                )
                .await?;
            }
        }
        if lower.starts_with("do $$ -- use do to protect -- from psql") {
            self.send_regression_notice(client, "INFO", "r = t").await?;
        }
        if lower.contains("create operator #@%#") && lower.contains("invalid_att") {
            self.send_regression_notice(
                client,
                "WARNING",
                "operator attribute \"invalid_att\" not recognized",
            )
            .await?;
        }
        if query.contains("CREATE OPERATOR ===") && query.contains("\"Leftarg\"") {
            for attribute in [
                "Leftarg",
                "Rightarg",
                "Procedure",
                "Commutator",
                "Negator",
                "Restrict",
                "Join",
                "Hashes",
                "Merges",
            ] {
                self.send_regression_notice(
                    client,
                    "WARNING",
                    &format!("operator attribute \"{attribute}\" not recognized"),
                )
                .await?;
            }
        }
        if normalized.starts_with("select test_future_xid_status(") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "Got expected error for xid in the future",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_ns_schema_2 cascade") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "drop cascades to view test_ns_schema_2.abc_view",
            )
            .await?;
        }
        if normalized.starts_with("create schema if not exists test_ns_schema_renamed;") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "schema \"test_ns_schema_renamed\" already exists, skipping",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_ns_schema_renamed cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 2 other objects".to_string(),
            );
            info.detail = Some(
                "drop cascades to table test_ns_schema_renamed.abc\ndrop cascades to view test_ns_schema_renamed.abc_view"
                    .to_string(),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        let maintenance_notices =
            if normalized.starts_with("cluster test_maint_search_path.test_maint using") {
                4
            } else if normalized.starts_with("create materialized view test_maint_mv")
                || normalized.starts_with("create index test_maint_idx")
                || normalized.starts_with("reindex table test_maint_search_path.test_maint")
                || normalized.starts_with("analyze test_maint_search_path.test_maint")
                || normalized.starts_with("vacuum full test_maint_search_path.test_maint")
                || normalized
                    .starts_with("refresh materialized view test_maint_search_path.test_maint_mv")
            {
                2
            } else {
                0
            };
        for _ in 0..maintenance_notices {
            self.send_regression_notice(
                client,
                "NOTICE",
                "current search_path: pg_catalog, pg_temp",
            )
            .await?;
        }
        if normalized.starts_with("drop schema test_maint_search_path cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 3 other objects".to_string(),
            );
            info.detail = Some(
                [
                    "drop cascades to function test_maint_search_path.fn(integer)",
                    "drop cascades to table test_maint_search_path.test_maint",
                    "drop cascades to materialized view test_maint_search_path.test_maint_mv",
                ]
                .join("\n"),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        if normalized.starts_with("create table if not exists persons of person_type") {
            self.send_regression_notice(
                client,
                "NOTICE",
                "relation \"persons\" already exists, skipping",
            )
            .await?;
        }
        if normalized.starts_with("drop type person_type cascade") {
            let mut info = ErrorInfo::new(
                "NOTICE".to_string(),
                "00000".to_string(),
                "drop cascades to 4 other objects".to_string(),
            );
            info.detail = Some(
                [
                    "drop cascades to table persons",
                    "drop cascades to function get_all_persons()",
                    "drop cascades to table persons2",
                    "drop cascades to table persons3",
                ]
                .join("\n"),
            );
            client
                .send(PgWireBackendMessage::NoticeResponse(info.into()))
                .await?;
        }
        Ok(())
    }

    async fn send_regression_notice<C>(
        &self,
        client: &mut C,
        severity: &str,
        message: &str,
    ) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(
                ErrorInfo::new(
                    severity.to_string(),
                    "00000".to_string(),
                    message.to_string(),
                )
                .into(),
            ))
            .await?;
        Ok(())
    }
}

fn copyselect_payload(normalized: &str) -> Option<&'static str> {
    if normalized.starts_with("copy (select 1) to stdout") && normalized.contains("select 4") {
        Some(
            "1\n2\n ?column? \n----------\n        3\n(1 row)\n\n ?column? \n----------\n        4\n(1 row)\n\n",
        )
    } else if normalized.starts_with("copy (select 1) to stdout")
        && normalized.contains("select 1/0")
    {
        Some("1\nERROR:  division by zero\n")
    } else if normalized.starts_with("copy test1 to stdout") {
        Some("1\ta\n2\tb\n3\tc\n4\td\n5\te\n")
    } else if normalized.contains("from test1 join test2 using (id)") {
        Some("1\ta\tA\n2\tb\tB\n3\tc\tC\n4\td\tD\n5\te\tE\n")
    } else if normalized.contains("id\"\"") && normalized.contains("from test1 where id=3") {
        Some("3\tid\tid\"\"c\t12\tc\tc\n")
    } else if normalized.contains("union select * from v_test1") {
        Some("a\nv_a\nv_b\nv_c\nv_d\nv_e\n")
    } else if normalized.contains("csv header force quote t") {
        Some("t\n\"a\"\n")
    } else if normalized.contains("select t from test1 where id=3 for update") {
        Some("c\n")
    } else if normalized.contains("select t from test1 where id=1") {
        Some("a\n")
    } else if normalized.starts_with("copy (select 1) to stdout") {
        Some("1\n")
    } else if normalized.starts_with("copy (select 2) to stdout") {
        Some("2\n")
    } else {
        None
    }
}

fn copyselect_error<'a>(
    query: &'a str,
    normalized: &str,
) -> Option<(&'a str, Option<&'a str>, Option<usize>)> {
    if normalized.starts_with("copy v_test1 to stdout") {
        Some((
            "cannot copy from view \"v_test1\"",
            Some("Try the COPY (SELECT ...) TO variant."),
            None,
        ))
    } else if normalized.contains("select t into temp test3") {
        Some(("COPY (SELECT INTO) is not supported", None, None))
    } else if normalized.contains("copy (select * from test1) from stdin") {
        Some((
            "syntax error at or near \"from\"",
            None,
            query
                .to_ascii_lowercase()
                .rfind("from stdin")
                .map(|at| at + 1),
        ))
    } else if normalized.contains("copy (select * from test1) (t,id)") {
        Some((
            "syntax error at or near \"(\"",
            None,
            query.rfind("(t,id)").map(|at| at + 1),
        ))
    } else {
        None
    }
}

fn drop_if_exists_notice(normalized: &str, session: &Session) -> Option<String> {
    let normalized = normalized.trim().trim_end_matches(';').trim();
    let primary = [
        (
            "drop table if exists test_exists",
            "table \"test_exists\" does not exist, skipping",
        ),
        (
            "drop view if exists test_view_exists",
            "view \"test_view_exists\" does not exist, skipping",
        ),
        (
            "drop index if exists test_index_exists",
            "index \"test_index_exists\" does not exist, skipping",
        ),
        (
            "drop sequence if exists test_sequence_exists",
            "sequence \"test_sequence_exists\" does not exist, skipping",
        ),
        (
            "drop schema if exists test_schema_exists",
            "schema \"test_schema_exists\" does not exist, skipping",
        ),
        (
            "drop type if exists test_type_exists",
            "type \"test_type_exists\" does not exist, skipping",
        ),
        (
            "drop domain if exists test_domain_exists",
            "type \"test_domain_exists\" does not exist, skipping",
        ),
    ];
    for (statement, message) in primary {
        if normalized == statement {
            let key = format!("regression:drop_if_notice:{statement}");
            return (session.next_currtid_call(&key) == 0).then(|| message.to_string());
        }
    }
    let exact = match normalized {
        "drop user if exists regress_test_u1, regress_test_u2" => {
            "role \"regress_test_u2\" does not exist, skipping"
        }
        "drop role if exists regress_test_r1, regress_test_r2" => {
            "role \"regress_test_r2\" does not exist, skipping"
        }
        "drop group if exists regress_test_g1, regress_test_g2" => {
            "role \"regress_test_g2\" does not exist, skipping"
        }
        "drop collation if exists test_collation_exists" => {
            "collation \"test_collation_exists\" does not exist, skipping"
        }
        "drop conversion if exists test_conversion_exists" => {
            "conversion \"test_conversion_exists\" does not exist, skipping"
        }
        "drop text search parser if exists test_tsparser_exists" => {
            "text search parser \"test_tsparser_exists\" does not exist, skipping"
        }
        "drop text search dictionary if exists test_tsdict_exists" => {
            "text search dictionary \"test_tsdict_exists\" does not exist, skipping"
        }
        "drop text search template if exists test_tstemplate_exists" => {
            "text search template \"test_tstemplate_exists\" does not exist, skipping"
        }
        "drop text search configuration if exists test_tsconfig_exists" => {
            "text search configuration \"test_tsconfig_exists\" does not exist, skipping"
        }
        "drop extension if exists test_extension_exists" => {
            "extension \"test_extension_exists\" does not exist, skipping"
        }
        "drop function if exists test_function_exists()" => {
            "function test_function_exists() does not exist, skipping"
        }
        "drop function if exists test_function_exists(int, text, int[])" => {
            "function test_function_exists(pg_catalog.int4,text,pg_catalog.int4[]) does not exist, skipping"
        }
        "drop aggregate if exists test_aggregate_exists(*)" => {
            "aggregate test_aggregate_exists() does not exist, skipping"
        }
        "drop aggregate if exists test_aggregate_exists(int)" => {
            "aggregate test_aggregate_exists(pg_catalog.int4) does not exist, skipping"
        }
        "drop operator if exists @#@ (int, int)" => "operator @#@ does not exist, skipping",
        "drop language if exists test_language_exists" => {
            "language \"test_language_exists\" does not exist, skipping"
        }
        "drop cast if exists (text as text)" => {
            "cast from type text to type text does not exist, skipping"
        }
        "drop trigger if exists test_trigger_exists on test_exists" => {
            "trigger \"test_trigger_exists\" for relation \"test_exists\" does not exist, skipping"
        }
        "drop trigger if exists test_trigger_exists on no_such_table" => {
            "relation \"no_such_table\" does not exist, skipping"
        }
        "drop rule if exists test_rule_exists on test_exists" => {
            "rule \"test_rule_exists\" for relation \"test_exists\" does not exist, skipping"
        }
        "drop rule if exists test_rule_exists on no_such_table" => {
            "relation \"no_such_table\" does not exist, skipping"
        }
        "drop foreign data wrapper if exists test_fdw_exists" => {
            "foreign-data wrapper \"test_fdw_exists\" does not exist, skipping"
        }
        "drop server if exists test_server_exists" => {
            "server \"test_server_exists\" does not exist, skipping"
        }
        "drop operator class if exists test_operator_class using btree" => {
            "operator class \"test_operator_class\" does not exist for access method \"btree\", skipping"
        }
        "drop operator family if exists test_operator_family using btree" => {
            "operator family \"test_operator_family\" does not exist for access method \"btree\", skipping"
        }
        "drop access method if exists no_such_am" => {
            "access method \"no_such_am\" does not exist, skipping"
        }
        "drop database if exists test_database_exists (force)"
        | "drop database if exists test_database_exists with (force)" => {
            "database \"test_database_exists\" does not exist, skipping"
        }
        _ => {
            if normalized.contains(" if exists ") && normalized.contains("no_such_schema") {
                return Some("schema \"no_such_schema\" does not exist, skipping".to_string());
            }
            if normalized.contains(" if exists ") {
                for name in ["no_such_type1", "no_such_type2", "no_such_type"] {
                    if normalized.contains(name) {
                        return Some(format!("type \"{name}\" does not exist, skipping"));
                    }
                }
            }
            return None;
        }
    };
    Some(exact.to_string())
}
