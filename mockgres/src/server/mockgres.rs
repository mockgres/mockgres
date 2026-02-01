use std::any::Any;
use std::fmt::Debug;
use std::io;
use std::sync::Arc;

use futures::{Sink, SinkExt, StreamExt};
use parking_lot::RwLock;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers,
    auth::{StartupHandler, noop::NoopStartupHandler},
    cancel::CancelHandler,
    query::{ExtendedQueryHandler, SimpleQueryHandler},
    results::{
        DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
        Response, Tag,
    },
    PgWireConnectionState,
    store::PortalStore,
};
use pgwire::error::{PgWireError, PgWireResult};
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
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, plan_parameter_types};

const STARTUP_TIMEOUT_MILLIS: u64 = 60_000;

fn query_command_tag(plan: &Plan) -> String {
    match plan {
        Plan::InsertValues { .. } => "INSERT 0".to_string(),
        Plan::Update { .. } => "UPDATE".to_string(),
        Plan::Delete { .. } => "DELETE".to_string(),
        _ => command_tag(plan).to_string(),
    }
}

fn execution_tag(plan: &Plan, row_count: Option<usize>) -> Tag {
    let mut tag = Tag::new(command_tag(plan));
    if matches!(plan, Plan::InsertValues { .. }) {
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
        match Planner::plan_sql(query) {
            Ok(plan) => {
                if matches!(plan, Plan::Empty) {
                    return Ok(vec![Response::EmptyQuery]);
                }
                let session = self.session_for_client(client)?;
                if let Plan::CallBuiltin { name, schema, .. } = &plan {
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
                        let eval_ctx = EvalContext::for_statement(&session)
                            .with_advisory_locks(session.id(), self.advisory_locks.clone());
                        let (fields, rows) =
                            to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
                        let mut qr = QueryResponse::new(fields, rows);
                        qr.set_command_tag("SELECT");
                        return Ok(vec![Response::Query(qr)]);
                    }
                    if name == "mockgres_reset" {
                        session.set_db_override(None);
                        let row = vec![Value::Bool(true)];
                        let exec = ValuesExec::from_values(schema.clone(), vec![row]);
                        let eval_ctx = EvalContext::for_statement(&session)
                            .with_advisory_locks(session.id(), self.advisory_locks.clone());
                        let (fields, rows) =
                            to_pgwire_stream(Box::new(exec), FieldFormat::Text, eval_ctx).await?;
                        let mut qr = QueryResponse::new(fields, rows);
                        qr.set_command_tag("SELECT");
                        return Ok(vec![Response::Query(qr)]);
                    }
                }
                let active_db = self.db_for_session(&session);
                let params: Arc<Vec<Value>> = Arc::new(Vec::new());
                let _stmt_guard = StatementEpochGuard::new(session.clone(), active_db.clone());
                let snapshot_xid = self.capture_statement_snapshot(&session);
                let eval_ctx = EvalContext::for_statement(&session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                // bind (names -> positions) using catalog
                let bound = {
                    let db_read = active_db.read();
                    bind(&db_read, &session, plan.clone())?
                };
                let (exec, _tag, row_count) = build_executor(
                    &active_db,
                    &self.txn_manager,
                    &session,
                    snapshot_xid,
                    &bound,
                    params,
                    &eval_ctx,
                )?;
                if matches!(bound, Plan::BeginTransaction) {
                    return Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]);
                }
                if matches!(bound, Plan::CommitTransaction) {
                    return Ok(vec![Response::TransactionEnd(Tag::new("COMMIT"))]);
                }
                if matches!(bound, Plan::RollbackTransaction) {
                    return Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]);
                }

                if exec.schema().fields.is_empty() {
                    return Ok(vec![Response::Execution(execution_tag(&bound, row_count))]);
                }

                let (fields, rows) =
                    to_pgwire_stream(exec, FieldFormat::Text, eval_ctx.clone()).await?;
                let mut qr = QueryResponse::new(fields, rows);
                qr.set_command_tag(&query_command_tag(&bound));
                Ok(vec![Response::Query(qr)])
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl ExtendedQueryHandler for Mockgres {
    type Statement = Plan;
    type QueryParser = pgwire_parser::PgQueryParserAdapter;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire_parser::PgQueryParserAdapter)
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
        let bound = {
            let db = active_db.read();
            bind(&db, &session, target.statement.clone())?
        };
        let params = plan_parameter_types(&bound);
        let fields = plan_fields_with_format(&bound, FieldFormat::Text);
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
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };
        let fields = self.describe_plan(&session, &portal.statement.statement, fmt)?;
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
        if matches!(portal.statement.statement, Plan::Empty) {
            return Ok(Response::EmptyQuery);
        }
        let fmt = match portal.result_column_format {
            pgwire::api::portal::Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };

        let session = self.session_for_client(client)?;
        if let Plan::CallBuiltin { name, schema, .. } = &portal.statement.statement {
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
                let eval_ctx = EvalContext::for_statement(&session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), fmt, eval_ctx).await?;
                let mut qr = QueryResponse::new(fields, rows);
                qr.set_command_tag("SELECT");
                return Ok(Response::Query(qr));
            }

            if name == "mockgres_reset" {
                session.set_db_override(None);

                let row = vec![Value::Bool(true)];
                let exec = ValuesExec::from_values(schema.clone(), vec![row]);
                let eval_ctx = EvalContext::for_statement(&session)
                    .with_advisory_locks(session.id(), self.advisory_locks.clone());
                let (fields, rows) = to_pgwire_stream(Box::new(exec), fmt, eval_ctx).await?;
                let mut qr = QueryResponse::new(fields, rows);
                qr.set_command_tag("SELECT");
                return Ok(Response::Query(qr));
            }
        }
        let active_db = self.db_for_session(&session);
        let _stmt_guard = StatementEpochGuard::new(session.clone(), active_db.clone());
        let snapshot_xid = self.capture_statement_snapshot(&session);
        let eval_ctx = EvalContext::for_statement(&session)
            .with_advisory_locks(session.id(), self.advisory_locks.clone());
        let bound = {
            let db = active_db.read();
            bind(&db, &session, portal.statement.statement.clone())?
        };

        let params = build_params_for_portal(&bound, portal, &eval_ctx.time_zone)?;
        let (exec, _tag, row_count) = build_executor(
            &active_db,
            &self.txn_manager,
            &session,
            snapshot_xid,
            &bound,
            params.clone(),
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

        let (fields, rows) = to_pgwire_stream(exec, fmt, eval_ctx.clone()).await?;
        let mut qr = QueryResponse::new(fields, rows);
        qr.set_command_tag(&query_command_tag(&bound));
        Ok(Response::Query(qr))
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
        if self.active && let Some(epoch) = self.session.exit_statement() {
            let db_read = self.db.read();
            db_read.release_locks(LockOwner::new(self.session.id(), epoch));
        }
    }
}

impl Mockgres {
    fn describe_plan(
        &self,
        session: &Session,
        plan: &Plan,
        format: FieldFormat,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let active_db = self.db_for_session(session);
        let db = active_db.read();
        let bound = bind(&db, session, plan.clone())?;
        Ok(plan_fields_with_format(&bound, format))
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
        if pid != 0 && let Some(existing) = self.session_manager.get(pid) {
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
    use pgwire::api::{ClientInfo, Type};
    use pgwire::api::portal::Format;
    use pgwire::api::results::FieldInfo;
    use pgwire::error::PgWireResult;

    use crate::engine::Plan;
    use crate::sql::Planner;

    #[derive(Clone, Default)]
    pub struct PgQueryParserAdapter;

    #[async_trait]
    impl pgwire::api::stmt::QueryParser for PgQueryParserAdapter {
        type Statement = Plan;
        async fn parse_sql<C>(
            &self,
            _client: &C,
            sql: &str,
            _types: &[Option<Type>],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            Planner::plan_sql(sql)
        }

        fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
            Ok(crate::server::params::plan_parameter_types(stmt))
        }

        fn get_result_schema(
            &self,
            stmt: &Self::Statement,
            _column_format: Option<&Format>,
        ) -> PgWireResult<Vec<FieldInfo>> {
            Ok(crate::server::describe::plan_fields(stmt))
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
