use std::any::Any;
use std::fmt::Debug;
use std::io;
use std::ops::DerefMut;
use std::sync::Arc;

use futures::{Sink, SinkExt, StreamExt};
use parking_lot::RwLock;
use pgwire::api::{
    ClientInfo, ClientPortalStore, DEFAULT_NAME, ErrorHandler, NoopHandler, PgWireConnectionState,
    PgWireServerHandlers,
    auth::{StartupHandler, noop::NoopStartupHandler},
    cancel::CancelHandler,
    portal::PortalExecutionState,
    query::{ExtendedQueryHandler, SimpleQueryHandler},
    query::{send_execution_response, send_query_response},
    results::{
        DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
        Response, Tag,
    },
    store::PortalStore,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::NoData;
use pgwire::messages::extendedquery::Execute;
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::server::{negotiate_tls, process_error, process_message};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

use crate::advisory_locks::AdvisoryLockRegistry;
use crate::binder::bind;
use crate::db::{Db, LockOwner};
use crate::engine::exec::ValuesExec;
use crate::engine::{EvalContext, Plan, Value, fe, fe_code, to_pgwire_stream};
use crate::session::{Session, SessionManager, now_utc_micros};
use crate::sql::Planner;
use crate::txn::{TransactionManager, TxId};

use super::ServerConfig;
use super::describe::plan_fields_with_format;
use super::exec::tx::{begin_transaction, commit_transaction, rollback_transaction};
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, statement_plan_parameter_types};
use super::statement_plan::StatementPlan;

const STARTUP_TIMEOUT_MILLIS: u64 = 60_000;

#[allow(dead_code)]
#[derive(Debug)]
struct BatchStatementMeta {
    statement_index: usize,
    response_kind: &'static str,
    failed: bool,
}

#[derive(Debug)]
struct BatchExecution {
    responses: Vec<Response>,
    metadata: Vec<BatchStatementMeta>,
}

fn query_command_tag(plan: &Plan) -> String {
    match plan {
        Plan::InsertValues { .. } | Plan::InsertSelect { .. } => "INSERT 0".to_string(),
        Plan::Update { .. } => "UPDATE".to_string(),
        Plan::Delete { .. } => "DELETE".to_string(),
        _ => command_tag(plan).to_string(),
    }
}

fn execution_tag(plan: &Plan, row_count: Option<usize>) -> Tag {
    let mut tag = Tag::new(command_tag(plan));
    if matches!(plan, Plan::InsertValues { .. } | Plan::InsertSelect { .. }) {
        tag = tag.with_oid(0);
    }
    if let Some(rows) = row_count {
        tag = tag.with_rows(rows);
    }
    tag
}

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
    session_manager: Arc<SessionManager>,
    pub txn_manager: Arc<TransactionManager>,
    config: ServerConfig,
    base_snapshot: Arc<RwLock<Option<Arc<RwLock<Db>>>>>,
    advisory_locks: Arc<AdvisoryLockRegistry>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self::new_with_config(db, ServerConfig::default())
    }

    pub fn new_with_config(db: Arc<RwLock<Db>>, config: ServerConfig) -> Self {
        Self {
            db,
            session_manager: Arc::new(SessionManager::new()),
            txn_manager: Arc::new(TransactionManager::new()),
            config,
            base_snapshot: Arc::new(RwLock::new(None)),
            advisory_locks: Arc::new(AdvisoryLockRegistry::new()),
        }
    }

    pub fn with_config(config: ServerConfig) -> Self {
        Self::new_with_config(Arc::new(RwLock::new(Db::default())), config)
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _peer) = listener.accept().await?;
            let h = self.clone();
            tokio::spawn(async move {
                let _ = process_socket_with_terminate(socket, None, h).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl NoopStartupHandler for Mockgres {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(startup) = &message {
            let requested = startup
                .parameters
                .get("database")
                .cloned()
                .filter(|name| !name.is_empty());
            let effective = requested
                .clone()
                .unwrap_or_else(|| self.config.database_name.clone());
            if effective != self.config.database_name {
                return Err(fe_code(
                    "3D000",
                    format!("database \"{}\" does not exist", effective),
                ));
            }
        }
        let session = self.init_session(client);
        session.set_database_name(self.config.database_name.clone());
        Ok(())
    }
}

impl Default for Mockgres {
    fn default() -> Self {
        Self::with_config(ServerConfig::default())
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for Mockgres {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let plans = Planner::plan_sql_batch(query)?;
        let session = self.session_for_client(client)?;
        let non_empty = plans
            .iter()
            .filter(|plan| !matches!(plan, Plan::Empty))
            .count();
        let has_explicit_tx_control = plans.iter().any(|plan| {
            matches!(
                plan,
                Plan::BeginTransaction | Plan::CommitTransaction | Plan::RollbackTransaction
            )
        });
        let use_implicit_tx =
            session.current_tx().is_none() && non_empty > 1 && !has_explicit_tx_control;
        let implicit_db = if use_implicit_tx {
            Some(self.db_for_session(&session))
        } else {
            None
        };

        if use_implicit_tx {
            begin_transaction(&session, &self.txn_manager)?;
        }

        let execution = self
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        if use_implicit_tx {
            let failed = execution.metadata.iter().any(|meta| meta.failed);
            let db = implicit_db.expect("implicit tx db");
            if failed {
                rollback_transaction(&session, &self.txn_manager, &db)?;
            } else {
                commit_transaction(&session, &self.txn_manager, &db)?;
            }
        }

        Ok(execution.responses)
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for Mockgres {
    type Statement = StatementPlan;
    type QueryParser = pgwire_parser::PgQueryParserAdapter;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire_parser::PgQueryParserAdapter)
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let max_rows = message.max_rows as usize;

        let Some(portal) = client.portal_store().get_portal(portal_name) else {
            return Err(PgWireError::PortalNotFound(portal_name.to_owned()));
        };

        if !portal.statement.statement.is_multi_non_empty() {
            return self._on_execute(client, message).await;
        }
        if max_rows > 0 {
            return Err(fe(
                "portal suspension with max_rows is not supported for statement batches",
            ));
        }

        let mut transaction_status = client.transaction_status();
        client.set_state(PgWireConnectionState::QueryInProgress);

        let portal_state_lock = portal.state();
        let mut portal_state = portal_state_lock.lock().await;
        match portal_state.deref_mut() {
            PortalExecutionState::Initial => {
                let session = self.session_for_client(client)?;
                let fmt = match portal.result_column_format {
                    pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
                    _ => FieldFormat::Text,
                };
                let StatementPlan::Batch(plans) = &portal.statement.statement else {
                    unreachable!("checked multi-batch above")
                };
                for plan in plans {
                    let response = self
                        .execute_one_statement(&session, plan, fmt, |bound, eval_ctx| {
                            build_params_for_portal(bound, portal.as_ref(), &eval_ctx.time_zone)
                        })
                        .await;
                    let response = match response {
                        Ok(resp) => resp,
                        Err(err) => Response::Error(Box::new(err.into())),
                    };
                    match response {
                        Response::EmptyQuery => {}
                        Response::Query(mut results) => {
                            // Each statement in a batch can have a different shape.
                            send_query_response(client, &mut results, true).await?;
                        }
                        Response::Execution(tag) => {
                            send_execution_response(client, tag).await?;
                        }
                        Response::TransactionStart(tag) => {
                            send_execution_response(client, tag).await?;
                            transaction_status = transaction_status.to_in_transaction_state();
                        }
                        Response::TransactionEnd(tag) => {
                            send_execution_response(client, tag).await?;
                            transaction_status = transaction_status.to_idle_state();
                        }
                        Response::Error(err) => {
                            client
                                .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                                .await?;
                            transaction_status = transaction_status.to_error_state();
                            break;
                        }
                        Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                            return Err(fe(
                                "COPY is not supported for statement batch execution in extended mode",
                            ));
                        }
                    }
                }
                *portal_state = PortalExecutionState::Finished;
            }
            PortalExecutionState::Suspended(_) => {
                return Err(fe(
                    "portal suspension is not supported for statement batches",
                ));
            }
            PortalExecutionState::Finished => {
                client
                    .send(PgWireBackendMessage::NoData(NoData::new()))
                    .await?;
            }
        }

        client.set_state(PgWireConnectionState::ReadyForQuery);
        client.set_transaction_status(transaction_status);
        if portal_name == DEFAULT_NAME {
            client.portal_store().rm_portal(portal_name);
        }
        Ok(())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        let active_db = self.db_for_session(&session);
        let bound = self.bind_statement_plans(&active_db, &session, &target.statement)?;
        let params = statement_plan_parameter_types(&StatementPlan::from_plans(bound.clone()));
        let fields = Self::fields_from_bound_plans(&bound, FieldFormat::Text);
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.session_for_client(client)?;
        let active_db = self.db_for_session(&session);
        let bound = self.bind_statement_plans(&active_db, &session, &portal.statement.statement)?;
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };
        let fields = Self::fields_from_bound_plans(&bound, fmt);
        Ok(DescribePortalResponse::new(fields))
    }
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if portal.statement.statement.is_empty() {
            return Ok(Response::EmptyQuery);
        }
        let Some(statement) = portal.statement.statement.single_non_empty() else {
            return Err(fe(
                "extended multi-statement execute is not implemented yet",
            ));
        };
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };

        let session = self.session_for_client(client)?;
        self.execute_one_statement(&session, statement, fmt, |bound, eval_ctx| {
            build_params_for_portal(bound, portal, &eval_ctx.time_zone)
        })
        .await
    }
}

impl PgWireServerHandlers for Mockgres {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.clone())
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(self.clone())
    }
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(self.clone())
    }
    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        Arc::new(NoopHandler)
    }
    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NoopHandler)
    }
}

struct StatementEpochGuard {
    session: Arc<Session>,
    db: Arc<RwLock<Db>>,
    active: bool,
}

impl StatementEpochGuard {
    fn new(session: Arc<Session>, db: Arc<RwLock<Db>>) -> Self {
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
    fn response_kind(response: &Response) -> &'static str {
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

    async fn execute_statement_batch(
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

    async fn materialize_response(response: Response) -> PgWireResult<Response> {
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

    async fn execute_one_statement<PR>(
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

    async fn execute_builtin_statement(
        &self,
        session: &Arc<Session>,
        plan: &Plan,
        format: FieldFormat,
    ) -> PgWireResult<Option<Response>> {
        let Plan::CallBuiltin { name, schema, .. } = plan else {
            return Ok(None);
        };

        if name == "mockgres_freeze" {
            {
                let mut guard = self.base_snapshot.write();
                if guard.is_none() {
                    let cloned = {
                        let db_read = self.db.read();
                        db_read.clone()
                    };
                    *guard = Some(Arc::new(RwLock::new(cloned)));
                }
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

        Ok(None)
    }

    fn bind_statement_plans(
        &self,
        active_db: &Arc<RwLock<Db>>,
        session: &Session,
        statement: &StatementPlan,
    ) -> PgWireResult<Vec<Plan>> {
        let db = active_db.read();
        let plans = match statement {
            StatementPlan::Single(plan) => std::slice::from_ref(plan),
            StatementPlan::Batch(plans) => plans.as_slice(),
        };
        let mut out = Vec::with_capacity(plans.len());
        for plan in plans {
            let bound = bind(&db, session, plan.clone())?;
            out.push(bound);
        }
        Ok(out)
    }

    fn fields_from_bound_plans(bound: &[Plan], format: FieldFormat) -> Vec<FieldInfo> {
        bound
            .iter()
            .find(|plan| !matches!(plan, Plan::Empty))
            .map(|plan| plan_fields_with_format(plan, format))
            .unwrap_or_default()
    }

    fn db_for_session(&self, session: &Session) -> Arc<RwLock<Db>> {
        // 1. If this session already has its own copy, use that
        if let Some(override_db) = session.db_override() {
            return override_db;
        }

        // 2. If a frozen base exists, lazily clone it for this session
        if let Some(base) = self.base_snapshot.read().as_ref() {
            let cloned = {
                let base_read = base.read();
                base_read.clone()
            };
            let arc = Arc::new(RwLock::new(cloned));
            session.set_db_override(Some(arc.clone()));
            return arc;
        }

        // 3. Pre-freeze: use shared db
        self.db.clone()
    }

    fn session_for_client<C>(&self, client: &C) -> PgWireResult<Arc<Session>>
    where
        C: ClientInfo,
    {
        let (pid, _) = client.pid_and_secret_key();
        self.session_manager
            .get(pid)
            .ok_or_else(|| fe("session not initialized"))
    }

    fn init_session<C>(&self, client: &mut C) -> Arc<Session>
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
        {
            let db_read = self.db.read();
            if let Some(public_id) = db_read.catalog.schema_id("public") {
                session.set_search_path(vec![public_id]);
            }
        }
        client.set_pid_and_secret_key(session.id(), SecretKey::I32(session.id()));
        session
    }

    fn cleanup_session(&self, session_id: i32) {
        self.advisory_locks.release_session(session_id);
        self.session_manager.remove(session_id);
    }

    fn capture_statement_snapshot(&self, session: &Arc<Session>) -> TxId {
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
            _types: &[Option<Type>],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            let plan = Planner::plan_sql(sql)?;
            Ok(StatementPlan::Single(plan))
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

pub async fn process_socket_with_terminate<H>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    handlers: H,
) -> Result<(), io::Error>
where
    H: PgWireServerHandlers + Any,
{
    let startup_timeout = sleep(Duration::from_millis(STARTUP_TIMEOUT_MILLIS));
    tokio::pin!(startup_timeout);

    let socket = tokio::select! {
        _ = &mut startup_timeout => {
            return Ok(())
        },
        socket = negotiate_tls(tcp_socket, tls_acceptor) => {
            socket?
        }
    };
    let Some(mut socket) = socket else {
        return Ok(());
    };

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.simple_query_handler();
    let extended_query_handler = handlers.extended_query_handler();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    let socket = &mut socket;
    loop {
        let msg = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => None,
                msg = socket.next() => msg,
            }
        } else {
            socket.next().await
        };

        match msg {
            Some(Ok(PgWireFrontendMessage::Terminate(_))) => {
                socket.close().await?;
                break;
            }
            Some(Ok(msg)) => {
                let is_extended_query = match socket.state() {
                    PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                    _ => msg.is_extended_query(),
                };
                if let Err(mut e) = process_message(
                    msg,
                    socket,
                    startup_handler.clone(),
                    simple_query_handler.clone(),
                    extended_query_handler.clone(),
                    copy_handler.clone(),
                    cancel_handler.clone(),
                )
                .await
                {
                    error_handler.on_error(socket, &mut e);
                    process_error(socket, e, is_extended_query).await?;
                }
            }
            _ => break,
        }
    }

    let (pid, _) = socket.pid_and_secret_key();
    if pid != 0 {
        if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Mockgres>() {
            mockgres.cleanup_session(pid);
        } else if let Some(mockgres) = (&handlers as &dyn Any).downcast_ref::<Arc<Mockgres>>() {
            mockgres.cleanup_session(pid);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{BatchExecution, Mockgres, Response};
    use crate::db::Db;
    use crate::session::Session;
    use crate::sql::Planner;
    use parking_lot::RwLock;
    use pgwire::api::results::FieldFormat;

    fn test_session(server: &Mockgres) -> Arc<Session> {
        let session = Arc::new(Session::new(42));
        session.set_database_name(server.config().database_name.clone());
        let db = server.db.read();
        let public_id = db.catalog.schema_id("public").expect("public schema");
        drop(db);
        session.set_search_path(vec![public_id]);
        session
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_all_succeed() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_batch_ok(id int primary key); insert into t_batch_ok values (1); select id from t_batch_ok",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata[0].statement_index, 1);
        assert_eq!(metadata[0].response_kind, "Execution");
        assert!(!metadata[0].failed);
        assert_eq!(metadata[1].statement_index, 2);
        assert_eq!(metadata[1].response_kind, "Execution");
        assert!(!metadata[1].failed);
        assert_eq!(metadata[2].statement_index, 3);
        assert_eq!(metadata[2].response_kind, "Query");
        assert!(!metadata[2].failed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_stops_on_first_error() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_batch_err(id int primary key); insert into t_batch_err values (1); insert into t_batch_err values ('bad'); insert into t_batch_err values (2)",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(
            responses.len(),
            3,
            "execution should stop after the first failing statement"
        );
        assert_eq!(metadata.len(), 3);
        assert!(!metadata[0].failed);
        assert!(!metadata[1].failed);
        assert!(metadata[2].failed);
        assert_eq!(metadata[2].statement_index, 3);
        assert_eq!(metadata[2].response_kind, "Error");
        assert!(
            matches!(responses[2], Response::Error(_)),
            "failing statement should produce an in-order error response"
        );

        // Verify statement 4 did not run: inserting id=2 after the failed batch must succeed.
        let post_error_insert = Planner::plan_sql("insert into t_batch_err values (2)")
            .expect("plan post-error insert");
        let response = server
            .execute_one_statement(
                &session,
                &post_error_insert,
                FieldFormat::Text,
                |_bound, _ctx| Ok(Arc::new(Vec::new())),
            )
            .await
            .expect("post-error insert should succeed");
        assert!(matches!(response, Response::Execution(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_first_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "select mockgres_freeze(); create table t_builtin_first(id int primary key)",
        )
        .expect("plan batch");

        let BatchExecution {
            responses,
            metadata,
        } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 2);
        assert!(matches!(responses[0], Response::Query(_)));
        assert!(matches!(responses[1], Response::Execution(_)));
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].response_kind, "Query");
        assert_eq!(metadata[1].response_kind, "Execution");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_middle_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        let plans = Planner::plan_sql_batch(
            "create table t_builtin_mid(id int primary key); select mockgres_freeze(); insert into t_builtin_mid values (1)",
        )
        .expect("plan batch");

        let BatchExecution { responses, .. } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(responses[0], Response::Execution(_)));
        assert!(matches!(responses[1], Response::Query(_)));
        assert!(matches!(responses[2], Response::Execution(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_statement_batch_supports_builtin_last_position() {
        let server = Mockgres::default();
        let session = test_session(&server);
        session.set_db_override(Some(Arc::new(RwLock::new(Db::default()))));

        let plans = Planner::plan_sql_batch(
            "create table t_builtin_last(id int primary key); insert into t_builtin_last values (1); select mockgres_reset()",
        )
        .expect("plan batch");

        let BatchExecution { responses, .. } = server
            .execute_statement_batch(&session, &plans, FieldFormat::Text, Arc::new(Vec::new()))
            .await;

        assert_eq!(responses.len(), 3);
        assert!(matches!(responses[0], Response::Execution(_)));
        assert!(matches!(responses[1], Response::Execution(_)));
        assert!(matches!(responses[2], Response::Query(_)));
        assert!(
            session.db_override().is_none(),
            "mockgres_reset should clear session DB override in batch mode"
        );
    }
}
