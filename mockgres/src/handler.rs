use std::collections::{BTreeSet, HashMap};
use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;

use futures::Sink;
use parking_lot::RwLock;
use pgwire::api::portal::Format;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers, Type,
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
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::binder::bind;
use crate::db::{CellInput, Db};
use crate::engine::{
    BoolExpr, DataType, ExecNode, Expr, FilterExec, InsertSource, LimitExec, OrderExec, Plan,
    ProjectExec, ScalarExpr, Schema, SeqScanExec, SqlError, UpdateSet, Value, ValuesExec,
    eval_scalar_expr, fe, fe_code, to_pgwire_stream,
};
use crate::parser::Planner;
use crate::types::{parse_bytea_text, parse_date_str, parse_timestamp_str};

#[derive(Clone)]
pub struct Mockgres {
    pub db: Arc<RwLock<Db>>,
}

impl Mockgres {
    pub fn new(db: Arc<RwLock<Db>>) -> Self {
        Self { db }
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

    // default/fallback command tags used when the executor didn't provide one
    fn command_tag(plan: &Plan) -> &'static str {
        match plan {
            Plan::Values { .. }
            | Plan::SeqScan { .. }
            | Plan::Projection { .. }
            | Plan::Filter { .. }
            | Plan::Order { .. }
            | Plan::Limit { .. } => "SELECT 0",

            Plan::CreateTable { .. } => "CREATE TABLE",
            Plan::AlterTableAddColumn { .. } | Plan::AlterTableDropColumn { .. } => "ALTER TABLE",
            Plan::CreateIndex { .. } => "CREATE INDEX",
            Plan::DropIndex { .. } => "DROP INDEX",
            Plan::ShowVariable { .. } => "SHOW",
            Plan::SetVariable { .. } => "SET",
            Plan::InsertValues { .. } => "INSERT 0",
            Plan::Update { .. } => "UPDATE 0",
            Plan::Delete { .. } => "DELETE 0",
            Plan::UnboundSeqScan { .. } => "SELECT 0",
        }
    }

    // build a physical plan
    fn build_executor(
        &self,
        p: &Plan,
        params: Arc<Vec<Value>>,
    ) -> PgWireResult<(Box<dyn ExecNode>, Option<String>, Option<usize>)> {
        match p {
            Plan::Values { rows, schema } => {
                let cnt = rows.len();
                Ok((
                    Box::new(ValuesExec::new(schema.clone(), rows.clone())?),
                    None,
                    Some(cnt),
                ))
            }

            Plan::Projection {
                input,
                exprs,
                schema,
            } => {
                let (child, _tag, cnt) = self.build_executor(input, params.clone())?;
                Ok((
                    Box::new(ProjectExec::new(
                        schema.clone(),
                        child,
                        exprs.clone(),
                        params.clone(),
                    )),
                    None,
                    cnt,
                ))
            }

            Plan::SeqScan {
                table,
                cols,
                schema,
            } => {
                let db = self.db.read();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let _tm = db
                    .resolve_table(schema_name, &table.name)
                    .map_err(map_db_err)?;
                let positions: Vec<usize> = cols.iter().map(|(i, _)| *i).collect();
                let (rows, _) = if positions.is_empty() && schema.fields.is_empty() {
                    (vec![], vec![])
                } else {
                    db.scan_bound_positions(schema_name, &table.name, &positions)
                        .map_err(map_db_err)?
                };
                drop(db);
                let cnt = rows.len();
                Ok((
                    Box::new(SeqScanExec::new(schema.clone(), rows)),
                    None,
                    Some(cnt),
                ))
            }

            // wrappers
            Plan::Filter {
                input,
                expr,
                project_prefix_len,
            } => {
                let (child, _tag, _cnt) = self.build_executor(input, params.clone())?;
                let child_schema = child.schema().clone();
                let mut node: Box<dyn ExecNode> = Box::new(FilterExec::new(
                    child_schema.clone(),
                    child,
                    expr.clone(),
                    params.clone(),
                ));

                // if parser widened selection for where, drop the extra columns here
                if let Some(n) = *project_prefix_len {
                    // project first n fields from the child schema
                    let proj_fields = child_schema.fields[..n].to_vec();
                    let proj_schema = Schema {
                        fields: proj_fields.clone(),
                    };
                    let exprs: Vec<(ScalarExpr, String)> = (0..n)
                        .map(|i| (ScalarExpr::ColumnIdx(i), proj_fields[i].name.clone()))
                        .collect();
                    node = Box::new(ProjectExec::new(proj_schema, node, exprs, params.clone()));
                }

                Ok((node, None, None))
            }

            Plan::Order { input, keys } => {
                let (child, _tag, cnt) = self.build_executor(input, params.clone())?;
                let schema = child.schema().clone();
                let exec = Box::new(OrderExec::new(schema, child, keys.clone(), params.clone())?);
                Ok((exec, None, cnt))
            }

            Plan::Limit { input, limit } => {
                let (child, _tag, cnt) = self.build_executor(input, params.clone())?;
                let out_cnt = cnt.map(|c| c.min(*limit));
                let schema = child.schema().clone();
                Ok((
                    Box::new(LimitExec::new(schema, child, *limit)),
                    None,
                    out_cnt,
                ))
            }

            Plan::CreateTable { table, cols, pk } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.create_table(schema_name, &table.name, cols.clone(), pk.clone())
                    .map_err(map_db_err)?;
                drop(db);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("CREATE TABLE".into()),
                    None,
                ))
            }

            Plan::AlterTableAddColumn {
                table,
                column,
                if_not_exists,
            } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.alter_table_add_column(schema_name, &table.name, column.clone(), *if_not_exists)
                    .map_err(map_db_err)?;
                drop(db);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("ALTER TABLE".into()),
                    None,
                ))
            }

            Plan::AlterTableDropColumn {
                table,
                column,
                if_exists,
            } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.alter_table_drop_column(schema_name, &table.name, column, *if_exists)
                    .map_err(map_db_err)?;
                drop(db);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("ALTER TABLE".into()),
                    None,
                ))
            }

            Plan::CreateIndex {
                table,
                name,
                columns,
                if_not_exists,
            } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.create_index(
                    schema_name,
                    &table.name,
                    name,
                    columns.clone(),
                    *if_not_exists,
                )
                .map_err(map_db_err)?;
                drop(db);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("CREATE INDEX".into()),
                    None,
                ))
            }

            Plan::DropIndex { indexes, if_exists } => {
                let mut db = self.db.write();
                for idx in indexes {
                    let schema_name = idx.schema.as_deref().unwrap_or("public");
                    db.drop_index(schema_name, &idx.name, *if_exists)
                        .map_err(map_db_err)?;
                }
                drop(db);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("DROP INDEX".into()),
                    None,
                ))
            }

            Plan::InsertValues {
                table,
                columns,
                rows,
            } => {
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let table_meta = {
                    let db = self.db.read();
                    db.resolve_table(schema_name, &table.name)
                        .map_err(map_db_err)?
                        .clone()
                };
                let column_map = if let Some(cols) = columns {
                    let mut indexes = Vec::with_capacity(cols.len());
                    for col in cols {
                        let pos = table_meta
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                        if indexes.iter().any(|i| *i == pos) {
                            return Err(fe_code("42701", format!("column {col} specified twice")));
                        }
                        indexes.push(pos);
                    }
                    Some(indexes)
                } else {
                    None
                };
                let mut realized = Vec::with_capacity(rows.len());
                for row in rows {
                    match &column_map {
                        Some(cols) if row.len() != cols.len() => {
                            return Err(fe_code(
                                "21P01",
                                format!(
                                    "INSERT has {} target columns but {} expressions",
                                    cols.len(),
                                    row.len()
                                ),
                            ));
                        }
                        None if row.len() != table_meta.columns.len() => {
                            return Err(fe_code(
                                "21P01",
                                format!(
                                    "INSERT expects {} expressions, got {}",
                                    table_meta.columns.len(),
                                    row.len()
                                ),
                            ));
                        }
                        _ => {}
                    }
                    let mut full = vec![CellInput::Default; table_meta.columns.len()];
                    match &column_map {
                        Some(cols) => {
                            for (idx, src) in row.iter().enumerate() {
                                let value = evaluate_insert_source(src, &params)?;
                                full[cols[idx]] = value;
                            }
                        }
                        None => {
                            for (idx, src) in row.iter().enumerate() {
                                full[idx] = evaluate_insert_source(src, &params)?;
                            }
                        }
                    }
                    realized.push(full);
                }
                let mut db = self.db.write();
                let inserted = db
                    .insert_full_rows(schema_name, &table.name, realized)
                    .map_err(map_db_err)?;
                drop(db);
                let tag = format!("INSERT 0 {}", inserted);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some(tag),
                    None,
                ))
            }
            Plan::Update {
                table,
                sets,
                filter,
            } => {
                let assignments = sets
                    .iter()
                    .map(|set| match set {
                        UpdateSet::ByIndex(idx, expr) => Ok((*idx, expr.clone())),
                        UpdateSet::ByName(name, _) => {
                            Err(fe(format!("unbound assignment target: {name}")))
                        }
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let count = db
                    .update_rows(
                        schema_name,
                        &table.name,
                        &assignments,
                        filter.as_ref(),
                        &params,
                    )
                    .map_err(map_db_err)?;
                drop(db);
                let tag = format!("UPDATE {}", count);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some(tag),
                    None,
                ))
            }
            Plan::Delete { table, filter } => {
                let mut db = self.db.write();
                let schema_name = table.schema.as_deref().unwrap_or("public");
                let count = db
                    .delete_rows(schema_name, &table.name, filter.as_ref(), &params)
                    .map_err(map_db_err)?;
                drop(db);
                let tag = format!("DELETE {}", count);
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some(tag),
                    None,
                ))
            }
            Plan::ShowVariable { name, schema } => {
                let value = match lookup_show_value(name) {
                    Some(v) => v,
                    None => return Err(fe_code("0A000", format!("SHOW {} not supported", name))),
                };
                let rows = vec![vec![Expr::Literal(Value::Text(value))]];
                let exec = ValuesExec::new(schema.clone(), rows)?;
                Ok((Box::new(exec), Some("SHOW".into()), Some(1)))
            }
            Plan::SetVariable { name, .. } => {
                if name != "client_min_messages" {
                    return Err(fe_code("0A000", format!("SET {} not supported", name)));
                }
                Ok((
                    Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                    Some("SET".into()),
                    None,
                ))
            }
            Plan::UnboundSeqScan { .. } => Err(fe("unbound plan; call binder first")),
        }
    }

    fn describe_plan(&self, plan: &Plan) -> PgWireResult<Vec<FieldInfo>> {
        let db = self.db.read();
        let bound = bind(&db, plan.clone())?;
        Ok(plan_fields(&bound))
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

#[async_trait::async_trait]
impl NoopStartupHandler for Mockgres {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
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
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match Planner::plan_sql(query) {
            Ok(lp0) => {
                // bind (names -> positions) using catalog
                let db_read = self.db.read();
                let bound = bind(&db_read, lp0)?;
                drop(db_read);

                let params = Arc::new(Vec::new());
                let (exec, tag, row_count) = self.build_executor(&bound, params)?;
                let (fields, rows) = to_pgwire_stream(exec, FieldFormat::Text)?;
                let mut qr = QueryResponse::new(fields, rows);
                if let Some(t) = tag {
                    // explicit tag from executor (e.g., insert)
                    qr.set_command_tag(&t);
                } else if let Some(n) = row_count {
                    // dynamic select row count
                    qr.set_command_tag(&format!("SELECT {}", n));
                } else {
                    qr.set_command_tag(Self::command_tag(&bound));
                }
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
        let db = self.db.read();
        let bound = bind(&db, target.statement.clone())?;
        drop(db);
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
        _client: &mut C,
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

        let db = self.db.read();
        let bound = bind(&db, portal.statement.statement.clone())?;
        drop(db);

        let params = build_params_for_portal(&bound, portal)?;
        let (exec, tag, row_count) = self.build_executor(&bound, params.clone())?;
        let (fields, rows) = to_pgwire_stream(exec, fmt)?;
        let mut qr = QueryResponse::new(fields, rows);
        if let Some(t) = tag {
            qr.set_command_tag(&t);
        } else if let Some(n) = row_count {
            qr.set_command_tag(&format!("SELECT {}", n));
        } else {
            qr.set_command_tag(Self::command_tag(&bound));
        }
        Ok(Response::Query(qr))
    }
}

fn build_params_for_portal(
    plan: &Plan,
    portal: &pgwire::api::portal::Portal<Plan>,
) -> PgWireResult<Arc<Vec<Value>>> {
    let mut hints = HashMap::new();
    collect_param_hints_from_plan(plan, &mut hints);

    let mut values = Vec::with_capacity(portal.parameters.len());
    for (idx, raw) in portal.parameters.iter().enumerate() {
        let fmt = portal.parameter_format.format_for(idx);
        let ty_from_plan = hints.get(&idx).cloned();
        let ty_from_stmt = portal
            .statement
            .parameter_types
            .get(idx)
            .and_then(map_pg_type_to_datatype);
        let ty = ty_from_plan.or(ty_from_stmt);
        let val = decode_param_value(raw.as_ref().map(|b| b.as_ref()), fmt, ty)?;
        values.push(val);
    }
    Ok(Arc::new(values))
}

fn plan_parameter_types(plan: &Plan) -> Vec<Type> {
    let mut indexes = BTreeSet::new();
    collect_param_indexes(plan, &mut indexes);
    if indexes.is_empty() {
        return vec![];
    }
    let mut hints = HashMap::new();
    collect_param_hints_from_plan(plan, &mut hints);
    indexes
        .into_iter()
        .map(|idx| {
            hints
                .get(&idx)
                .map(|dt| map_datatype_to_pg_type(dt))
                .unwrap_or(Type::UNKNOWN)
        })
        .collect()
}

fn collect_param_hints_from_plan(plan: &Plan, out: &mut HashMap<usize, DataType>) {
    match plan {
        Plan::Filter { input, expr, .. } => {
            collect_param_hints_from_plan(input, out);
            collect_param_hints_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::Limit { input, .. } => {
            collect_param_hints_from_plan(input, out)
        }
        Plan::Projection { input, exprs, .. } => {
            collect_param_hints_from_plan(input, out);
            for (expr, _) in exprs {
                collect_param_hints_from_scalar(expr, out);
            }
        }
        Plan::Update { sets, filter, .. } => {
            collect_param_hints_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
        }
        Plan::Delete { filter, .. } => {
            if let Some(expr) = filter {
                collect_param_hints_from_bool(expr, out);
            }
        }
        Plan::InsertValues { rows, .. } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_hints_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::SeqScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::Values { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. } => {}
    }
}

fn collect_param_hints_from_bool(expr: &BoolExpr, out: &mut HashMap<usize, DataType>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_param_hints_from_scalar(lhs, out);
            collect_param_hints_from_scalar(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_param_hints_from_bool(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_param_hints_from_bool(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_param_hints_from_scalar(expr, out),
    }
}

fn collect_param_hints_from_scalar(expr: &ScalarExpr, out: &mut HashMap<usize, DataType>) {
    match expr {
        ScalarExpr::Param { idx, ty } => {
            if let Some(dt) = ty {
                out.entry(*idx).or_insert(dt.clone());
            }
        }
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_param_hints_from_scalar(left, out);
            collect_param_hints_from_scalar(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_param_hints_from_scalar(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_param_hints_from_scalar(arg, out);
            }
        }
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(..) | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_hints_from_update_sets(sets: &[UpdateSet], out: &mut HashMap<usize, DataType>) {
    for set in sets {
        match set {
            UpdateSet::ByIndex(_, expr) | UpdateSet::ByName(_, expr) => {
                collect_param_hints_from_scalar(expr, out);
            }
        }
    }
}

fn collect_param_indexes(plan: &Plan, out: &mut BTreeSet<usize>) {
    match plan {
        Plan::Filter { input, expr, .. } => {
            collect_param_indexes(input, out);
            collect_param_indexes_from_bool(expr, out);
        }
        Plan::Order { input, .. } | Plan::Limit { input, .. } => collect_param_indexes(input, out),
        Plan::Projection { input, exprs, .. } => {
            collect_param_indexes(input, out);
            for (expr, _) in exprs {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
        Plan::Update { sets, filter, .. } => {
            collect_param_indexes_from_update_sets(sets, out);
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
        }
        Plan::Delete { filter, .. } => {
            if let Some(expr) = filter {
                collect_param_indexes_from_bool(expr, out);
            }
        }
        Plan::InsertValues { rows, .. } => {
            for row in rows {
                for src in row {
                    if let InsertSource::Expr(expr) = src {
                        collect_param_indexes_from_scalar(expr, out);
                    }
                }
            }
        }
        Plan::SeqScan { .. }
        | Plan::UnboundSeqScan { .. }
        | Plan::Values { .. }
        | Plan::CreateTable { .. }
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableDropColumn { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::ShowVariable { .. }
        | Plan::SetVariable { .. } => {}
    }
}

fn collect_param_indexes_from_bool(expr: &BoolExpr, out: &mut BTreeSet<usize>) {
    match expr {
        BoolExpr::Literal(_) => {}
        BoolExpr::Comparison { lhs, rhs, .. } => {
            collect_param_indexes_from_scalar(lhs, out);
            collect_param_indexes_from_scalar(rhs, out);
        }
        BoolExpr::And(parts) | BoolExpr::Or(parts) => {
            for p in parts {
                collect_param_indexes_from_bool(p, out);
            }
        }
        BoolExpr::Not(inner) => collect_param_indexes_from_bool(inner, out),
        BoolExpr::IsNull { expr, .. } => collect_param_indexes_from_scalar(expr, out),
    }
}

fn collect_param_indexes_from_scalar(expr: &ScalarExpr, out: &mut BTreeSet<usize>) {
    match expr {
        ScalarExpr::Param { idx, .. } => {
            out.insert(*idx);
        }
        ScalarExpr::BinaryOp { left, right, .. } => {
            collect_param_indexes_from_scalar(left, out);
            collect_param_indexes_from_scalar(right, out);
        }
        ScalarExpr::UnaryOp { expr, .. } => collect_param_indexes_from_scalar(expr, out),
        ScalarExpr::Func { args, .. } => {
            for arg in args {
                collect_param_indexes_from_scalar(arg, out);
            }
        }
        ScalarExpr::Column(..) | ScalarExpr::ColumnIdx(..) | ScalarExpr::Literal(_) => {}
    }
}

fn collect_param_indexes_from_update_sets(sets: &[UpdateSet], out: &mut BTreeSet<usize>) {
    for set in sets {
        match set {
            UpdateSet::ByIndex(_, expr) | UpdateSet::ByName(_, expr) => {
                collect_param_indexes_from_scalar(expr, out);
            }
        }
    }
}

fn map_pg_type_to_datatype(t: &Type) -> Option<DataType> {
    match *t {
        Type::INT4 => Some(DataType::Int4),
        Type::INT8 => Some(DataType::Int8),
        Type::FLOAT8 => Some(DataType::Float8),
        Type::TEXT | Type::VARCHAR => Some(DataType::Text),
        Type::BOOL => Some(DataType::Bool),
        Type::DATE => Some(DataType::Date),
        Type::TIMESTAMP => Some(DataType::Timestamp),
        Type::BYTEA => Some(DataType::Bytea),
        _ => None,
    }
}

fn map_datatype_to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int4 => Type::INT4,
        DataType::Int8 => Type::INT8,
        DataType::Float8 => Type::FLOAT8,
        DataType::Text => Type::TEXT,
        DataType::Bool => Type::BOOL,
        DataType::Date => Type::DATE,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::Bytea => Type::BYTEA,
    }
}

fn lookup_show_value(name: &str) -> Option<String> {
    match name {
        "server_version" => Some("15.0".to_string()),
        "search_path" => Some("public".to_string()),
        _ => None,
    }
}

fn map_db_err(err: anyhow::Error) -> PgWireError {
    if let Some(sql) = err.downcast_ref::<SqlError>() {
        fe_code(sql.code, sql.message.clone())
    } else {
        fe(err.to_string())
    }
}

fn evaluate_insert_source(src: &InsertSource, params: &[Value]) -> PgWireResult<CellInput> {
    match src {
        InsertSource::Default => Ok(CellInput::Default),
        InsertSource::Expr(expr) => {
            let value = eval_scalar_expr(&[], expr, params)?;
            Ok(CellInput::Value(value))
        }
    }
}

fn plan_fields(plan: &Plan) -> Vec<FieldInfo> {
    plan.schema()
        .fields
        .iter()
        .map(|f| {
            FieldInfo::new(
                f.name.clone(),
                None,
                None,
                f.data_type.to_pg(),
                FieldFormat::Text,
            )
        })
        .collect()
}

fn decode_param_value(
    raw: Option<&[u8]>,
    fmt: FieldFormat,
    ty: Option<DataType>,
) -> PgWireResult<Value> {
    if raw.is_none() {
        return Ok(Value::Null);
    }
    let bytes = raw.unwrap();
    let ty = ty.unwrap_or(DataType::Text);
    match fmt {
        FieldFormat::Text => parse_text_value(bytes, &ty),
        FieldFormat::Binary => parse_binary_value(bytes, &ty),
    }
}

fn parse_text_value(bytes: &[u8], ty: &DataType) -> PgWireResult<Value> {
    let s = std::str::from_utf8(bytes).map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
    match ty {
        DataType::Int4 => {
            let v: i32 = s.parse().map_err(|e| fe(format!("bad int4 param: {e}")))?;
            Ok(Value::Int64(v as i64))
        }
        DataType::Int8 => {
            let v: i64 = s.parse().map_err(|e| fe(format!("bad int8 param: {e}")))?;
            Ok(Value::Int64(v))
        }
        DataType::Float8 => {
            let v: f64 = s
                .parse()
                .map_err(|e| fe(format!("bad float8 param: {e}")))?;
            Ok(Value::from_f64(v))
        }
        DataType::Text => Ok(Value::Text(s.to_string())),
        DataType::Bool => {
            let lowered = s.to_ascii_lowercase();
            match lowered.as_str() {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                other => Err(fe(format!("bad bool param: {other}"))),
            }
        }
        DataType::Date => {
            let days = parse_date_str(s).map_err(|e| fe(e))?;
            Ok(Value::Date(days))
        }
        DataType::Timestamp => {
            let micros = parse_timestamp_str(s).map_err(|e| fe(e))?;
            Ok(Value::TimestampMicros(micros))
        }
        DataType::Bytea => {
            let bytes = parse_bytea_text(s).map_err(|e| fe(e))?;
            Ok(Value::Bytes(bytes))
        }
    }
}

fn parse_binary_value(bytes: &[u8], ty: &DataType) -> PgWireResult<Value> {
    match ty {
        DataType::Int4 => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| fe("binary int4 must be 4 bytes"))?;
            Ok(Value::Int64(i32::from_be_bytes(arr) as i64))
        }
        DataType::Int8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary int8 must be 8 bytes"))?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        DataType::Float8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| fe("binary float8 must be 8 bytes"))?;
            Ok(Value::Float64Bits(u64::from_be_bytes(arr)))
        }
        DataType::Bool => {
            if bytes.len() != 1 {
                return Err(fe("binary bool must be 1 byte"));
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        DataType::Text => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| fe(format!("invalid utf8 parameter: {e}")))?;
            Ok(Value::Text(s.to_string()))
        }
        DataType::Bytea => Ok(Value::Bytes(bytes.to_vec())),
        DataType::Date | DataType::Timestamp => {
            Err(fe("binary parameters for date/timestamp not supported"))
        }
    }
}

/// Pgwire adapter: parse -> our `Plan`
pub mod pgwire_parser {
    use async_trait::async_trait;
    use pgwire::api::{ClientInfo, Type};
    use pgwire::error::PgWireResult;

    use crate::engine::Plan;
    use crate::parser::Planner;

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
