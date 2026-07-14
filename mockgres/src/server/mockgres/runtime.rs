use super::*;

pub(super) struct StatementEpochGuard {
    session: Arc<Session>,
    db: Arc<RwLock<Db>>,
    active: bool,
}

impl StatementEpochGuard {
    pub(super) fn new(session: Arc<Session>, db: Arc<RwLock<Db>>) -> Self {
        let active = session.enter_statement();
        Self {
            session,
            db,
            active,
        }
    }
}

impl Drop for StatementEpochGuard {
    fn drop(&mut self) {
        if self.active
            && let Some(epoch) = self.session.exit_statement()
        {
            let db_read = self.db.read();
            db_read.release_locks(LockOwner::new(self.session.id(), epoch));
        }
    }
}

impl Mockgres {
    pub(super) fn response_kind(response: &Response) -> &'static str {
        match response {
            Response::EmptyQuery => "EmptyQuery",
            Response::Query(_) => "Query",
            Response::Execution(_) => "Execution",
            Response::TransactionStart(_) => "TransactionStart",
            Response::TransactionEnd(_) => "TransactionEnd",
            Response::Error(_) => "Error",
            Response::CopyIn(_) => "CopyIn",
            Response::CopyOut(_) => "CopyOut",
            Response::CopyBoth(_) => "CopyBoth",
        }
    }

    pub(super) async fn execute_statement_batch(
        &self,
        session: &Arc<Session>,
        plans: &[Plan],
        format: FieldFormat,
        params: Arc<Vec<Value>>,
    ) -> BatchExecution {
        let mut responses = Vec::with_capacity(plans.len());
        let mut metadata = Vec::with_capacity(plans.len());

        for (idx, plan) in plans.iter().enumerate() {
            match self
                .execute_one_statement(session, plan, format, |_bound, _ctx| Ok(params.clone()))
                .await
            {
                Ok(response) => {
                    let response = match Self::materialize_response(response).await {
                        Ok(response) => response,
                        Err(err) => {
                            metadata.push(BatchStatementMeta {
                                statement_index: idx + 1,
                                response_kind: "Error",
                                failed: true,
                            });
                            responses.push(Response::Error(Box::new(err.into())));
                            break;
                        }
                    };
                    metadata.push(BatchStatementMeta {
                        statement_index: idx + 1,
                        response_kind: Self::response_kind(&response),
                        failed: false,
                    });
                    responses.push(response);
                }
                Err(err) => {
                    metadata.push(BatchStatementMeta {
                        statement_index: idx + 1,
                        response_kind: "Error",
                        failed: true,
                    });
                    responses.push(Response::Error(Box::new(err.into())));
                    break;
                }
            }
        }

        BatchExecution {
            responses,
            metadata,
        }
    }

    pub(super) async fn materialize_response(response: Response) -> PgWireResult<Response> {
        let Response::Query(mut query) = response else {
            return Ok(response);
        };
        let command_tag = query.command_tag().to_string();
        let fields = query.row_schema();
        let mut rows = Vec::new();
        while let Some(row) = query.data_rows().next().await {
            rows.push(row?);
        }
        let row_stream = futures::stream::iter(rows.into_iter().map(Ok));
        let mut materialized = QueryResponse::new(fields, row_stream);
        materialized.set_command_tag(&command_tag);
        Ok(Response::Query(materialized))
    }

    pub(super) async fn execute_one_statement<PR>(
        &self,
        session: &Arc<Session>,
        plan: &Plan,
        format: FieldFormat,
        resolve_params: PR,
    ) -> PgWireResult<Response>
    where
        PR: FnOnce(&Plan, &EvalContext) -> PgWireResult<Arc<Vec<Value>>>,
    {
        if matches!(plan, Plan::Empty) {
            return Ok(Response::EmptyQuery);
        }
        if let Some(response) = self
            .execute_builtin_statement(session, plan, format)
            .await?
        {
            return Ok(response);
        }

        if matches!(plan, Plan::CreateTable { table, .. } if table.schema.as_ref().is_some_and(|schema| schema.as_str() == "pg_temp"))
            && session.db_override().is_none()
        {
            let database_name = self.database_name_for_session(session);
            let cloned = self.shared_database(&database_name).read().clone();
            let override_db = Arc::new(RwLock::new(cloned));
            let (temp_id, public_id) = {
                let mut db = override_db.write();
                db.create_schema("pg_temp", true)
                    .map_err(|error| fe(error.to_string()))?;
                (
                    db.catalog
                        .schema_id("pg_temp")
                        .expect("temporary schema created"),
                    db.catalog
                        .schema_id("public")
                        .expect("public schema exists"),
                )
            };
            session.set_db_override(Some(override_db));
            session.set_search_path(vec![temp_id, public_id]);
        }

        let active_db = self.db_for_session(session);
        let _stmt_guard = StatementEpochGuard::new(session.clone(), active_db.clone());
        let snapshot_xid = self.capture_statement_snapshot(session);
        let eval_ctx = EvalContext::for_statement(session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let bound = {
            let db = active_db.read();
            bind(&db, session, plan.clone())?
        };
        let params = resolve_params(&bound, &eval_ctx)?;

        let (exec, _tag, row_count) = build_executor(
            &active_db,
            &self.txn_manager,
            session,
            snapshot_xid,
            &bound,
            params,
            &eval_ctx,
        )?;
        if matches!(bound, Plan::BeginTransaction) {
            return Ok(Response::TransactionStart(Tag::new("BEGIN")));
        }
        if matches!(bound, Plan::CommitTransaction) {
            return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
        }
        if matches!(bound, Plan::RollbackTransaction) {
            return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
        }

        if exec.schema().fields.is_empty() {
            return Ok(Response::Execution(execution_tag(&bound, row_count)));
        }

        let (fields, rows) = to_pgwire_stream(exec, format, eval_ctx).await?;
        let mut qr = QueryResponse::new(fields, rows);
        qr.set_command_tag(&query_command_tag(&bound));
        Ok(Response::Query(qr))
    }

    pub(super) fn bind_statement_plans(
        &self,
        active_db: &Arc<RwLock<Db>>,
        session: &Session,
        statement: &StatementPlan,
    ) -> PgWireResult<Vec<Plan>> {
        let db = active_db.read();
        let plans = match statement {
            StatementPlan::Single(plan) => std::slice::from_ref(plan.as_ref()),
            StatementPlan::Batch(plans) => plans.as_slice(),
        };
        let mut out = Vec::with_capacity(plans.len());
        for plan in plans {
            let bound = bind(&db, session, plan.clone())?;
            out.push(bound);
        }
        Ok(out)
    }

    pub(super) fn fields_from_bound_plans(bound: &[Plan], format: FieldFormat) -> Vec<FieldInfo> {
        bound
            .iter()
            .find(|plan| !matches!(plan, Plan::Empty))
            .map(|plan| plan_fields_with_format(plan, format))
            .unwrap_or_default()
    }

    pub(super) fn db_for_session(&self, session: &Session) -> Arc<RwLock<Db>> {
        // 1. If this session already has its own copy, use that
        if let Some(override_db) = session.db_override() {
            return override_db;
        }

        let database_name = self.database_name_for_session(session);

        // 2. If a frozen base exists for this database, lazily clone it for this session
        if let Some(base) = self.base_snapshots.read().get(&database_name).cloned() {
            let cloned = {
                let base_read = base.read();
                base_read.clone()
            };
            let arc = Arc::new(RwLock::new(cloned));
            session.set_db_override(Some(arc.clone()));
            return arc;
        }

        // 3. Pre-freeze: use the database shared by all of its sessions
        self.shared_database(&database_name)
    }

    pub(super) fn database_name_for_session(&self, session: &Session) -> String {
        session
            .database_name()
            .unwrap_or_else(|| self.config.database_name.clone())
    }

    pub(super) fn shared_database(&self, name: &str) -> Arc<RwLock<Db>> {
        self.databases
            .read()
            .get(name)
            .cloned()
            .unwrap_or_else(|| panic!("session references missing database {name}"))
    }

    pub(super) fn session_for_client<C>(&self, client: &C) -> PgWireResult<Arc<Session>>
    where
        C: ClientInfo,
    {
        let (pid, _) = client.pid_and_secret_key();
        self.session_manager
            .get(pid)
            .ok_or_else(|| fe("session not initialized"))
    }

    pub(super) fn init_session<C>(
        &self,
        client: &mut C,
        database_name: &str,
        database: &Arc<RwLock<Db>>,
    ) -> Arc<Session>
    where
        C: ClientInfo,
    {
        let (pid, _) = client.pid_and_secret_key();
        if pid != 0
            && let Some(existing) = self.session_manager.get(pid)
        {
            return existing;
        }
        let session = self.session_manager.create_session();
        session.set_database_name(database_name.to_string());
        {
            let db_read = database.read();
            if let Some(public_id) = db_read.catalog.schema_id("public") {
                session.set_search_path(vec![public_id]);
            }
        }
        client.set_pid_and_secret_key(session.id(), SecretKey::I32(session.id()));
        session
    }

    pub(super) fn cleanup_session(&self, session_id: i32) {
        self.advisory_locks.release_session(session_id);
        self.session_manager.remove(session_id);
    }

    pub(super) fn capture_statement_snapshot(&self, session: &Arc<Session>) -> TxId {
        let snapshot = self.txn_manager.snapshot_xid();
        session.set_statement_xid(snapshot);
        session.set_statement_time_micros(now_utc_micros());
        snapshot
    }
}
pub mod pgwire_parser {
    use async_trait::async_trait;
    use pgwire::api::portal::Format;
    use pgwire::api::results::FieldInfo;
    use pgwire::api::{ClientInfo, Type};
    use pgwire::error::PgWireResult;

    use crate::server::describe::statement_plan_fields;
    use crate::server::params::statement_plan_parameter_types;
    use crate::server::statement_plan::StatementPlan;
    use crate::sql::Planner;

    #[derive(Clone, Default)]
    pub struct PgQueryParserAdapter;

    #[async_trait]
    impl pgwire::api::stmt::QueryParser for PgQueryParserAdapter {
        type Statement = StatementPlan;
        async fn parse_sql<C>(
            &self,
            _client: &C,
            sql: &str,
            types: &[Option<Type>],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            if types.is_empty() && sql.contains("$2") && !sql.contains("$1") {
                return Err(crate::engine::fe(
                    "could not determine data type of parameter $1",
                ));
            }
            let plan = Planner::plan_sql(sql)?;
            Ok(StatementPlan::Single(Box::new(plan)))
        }

        fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
            Ok(statement_plan_parameter_types(stmt))
        }

        fn get_result_schema(
            &self,
            stmt: &Self::Statement,
            column_format: Option<&Format>,
        ) -> PgWireResult<Vec<FieldInfo>> {
            let format = match column_format {
                Some(Format::UnifiedBinary) => pgwire::api::results::FieldFormat::Binary,
                _ => pgwire::api::results::FieldFormat::Text,
            };
            Ok(statement_plan_fields(stmt, format))
        }
    }
}
