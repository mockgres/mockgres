use std::fmt::Debug;
use std::sync::Arc;

use futures::Sink;
use parking_lot::RwLock;
use pgwire::api::portal::Format;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers,
    auth::{StartupHandler, noop::NoopStartupHandler},
    cancel::CancelHandler,
    query::{ExtendedQueryHandler, SimpleQueryHandler},
    results::{
        DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo, QueryResponse,
        Response,
    },
    store::PortalStore,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::binder::bind;
use crate::db::{Db, LockOwner};
use crate::engine::{Plan, Value, fe, to_pgwire_stream};
use crate::session::{Session, SessionManager};
use crate::sql::Planner;
use crate::txn::{TransactionManager, TxId};

use super::describe::plan_fields;
use super::exec_builder::{build_executor, command_tag};
use super::params::{build_params_for_portal, plan_parameter_types};

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
    session_manager: Arc<SessionManager>,
    pub txn_manager: Arc<TransactionManager>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self {
            db,
            session_manager: Arc::new(SessionManager::new()),
            txn_manager: Arc::new(TransactionManager::new()),
        }
    }

    pub async fn serve(self: Arc<Self>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _peer) = listener.accept().await?;
            let h = self.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(socket, None, h).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl NoopStartupHandler for Mockgres {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.init_session(client);
        Ok(())
    }
}

impl Default for Mockgres {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(Db::default())))
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for Mockgres {
    async fn do_query<'a, C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match Planner::plan_sql(query) {
            Ok(lp0) => {
                // bind (names -> positions) using catalog
                let bound = {
                    let db_read = self.db.read();
                    bind(&db_read, lp0)?
                };

                let params: Arc<Vec<Value>> = Arc::new(Vec::new());
                let session = self.session_for_client(client)?;
                let _stmt_guard = StatementEpochGuard::new(session.clone(), self.db.clone());
                let snapshot_xid = self.capture_statement_snapshot(&session);
                let (exec, tag, row_count) = build_executor(
                    &self.db,
                    &self.txn_manager,
                    &session,
                    snapshot_xid,
                    &bound,
                    params,
                )?;
                let (fields, rows) = to_pgwire_stream(exec, FieldFormat::Text).await?;
                let mut qr = QueryResponse::new(fields, rows);
                let tag_text = if let Some(t) = tag {
                    t
                } else if let Some(n) = row_count {
                    format!("{} {}", command_tag(&bound), n)
                } else {
                    command_tag(&bound).to_string()
                };
                qr.set_command_tag(&tag_text);
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
        Arc::new(pgwire_parser::PgQueryParserAdapter::default())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let bound = {
            let db = self.db.read();
            bind(&db, target.statement.clone())?
        };
        let params = plan_parameter_types(&bound);
        let fields = plan_fields(&bound);
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fields = self.describe_plan(&portal.statement.statement)?;
        Ok(DescribePortalResponse::new(fields))
    }
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let fmt = match portal.result_column_format {
            Format::UnifiedBinary => FieldFormat::Binary,
            _ => FieldFormat::Text,
        };

        let bound = {
            let db = self.db.read();
            bind(&db, portal.statement.statement.clone())?
        };

        let params = build_params_for_portal(&bound, portal)?;
        let session = self.session_for_client(client)?;
        let _stmt_guard = StatementEpochGuard::new(session.clone(), self.db.clone());
        let snapshot_xid = self.capture_statement_snapshot(&session);
        let (exec, tag, row_count) = build_executor(
            &self.db,
            &self.txn_manager,
            &session,
            snapshot_xid,
            &bound,
            params.clone(),
        )?;
        let (fields, rows) = to_pgwire_stream(exec, fmt).await?;
        let mut qr = QueryResponse::new(fields, rows);
        let tag_text = if let Some(t) = tag {
            t
        } else if let Some(n) = row_count {
            format!("{} {}", command_tag(&bound), n)
        } else {
            command_tag(&bound).to_string()
        };
        qr.set_command_tag(&tag_text);
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
        if self.active {
            if let Some(epoch) = self.session.exit_statement() {
                let db_read = self.db.read();
                db_read.release_locks(LockOwner::new(self.session.id(), epoch));
            }
        }
    }
}

impl Mockgres {
    fn describe_plan(&self, plan: &Plan) -> PgWireResult<Vec<FieldInfo>> {
        let db = self.db.read();
        let bound = bind(&db, plan.clone())?;
        Ok(plan_fields(&bound))
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
        if pid != 0 {
            if let Some(existing) = self.session_manager.get(pid) {
                return existing;
            }
        }
        let session = self.session_manager.create_session();
        client.set_pid_and_secret_key(session.id(), SecretKey::I32(session.id()));
        session
    }

    fn capture_statement_snapshot(&self, session: &Arc<Session>) -> TxId {
        let snapshot = self.txn_manager.snapshot_xid();
        session.set_statement_xid(snapshot);
        snapshot
    }
}

/// Pgwire adapter: parse -> our `Plan`
pub mod pgwire_parser {
    use async_trait::async_trait;
    use pgwire::api::{ClientInfo, Type};
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
            _types: &[Type],
        ) -> PgWireResult<Self::Statement>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            Planner::plan_sql(sql)
        }
    }
}
