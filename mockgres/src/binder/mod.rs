use std::collections::{HashMap, HashSet};

use crate::catalog::{SchemaId, TableMeta};
use crate::db::Db;
use crate::engine::{
    AggCall, AggFunc, ColumnSpec, DataType, DbDdlKind, Expr, Field, FieldOrigin, InsertSource,
    LockSpec, ObjName, OnConflictAction, Plan, ScalarExpr, Schema, Selection, SortKey, SqlError,
    UpdateSet, WindowSpec, fe, fe_code,
};
use crate::session::Session;
use anyhow::Error;
use pgwire::error::{PgWireError, PgWireResult};

mod expr;
mod query;
mod returning;
mod time;
mod write;

use self::expr::{
    bind_bool_expr, bind_bool_expr_allow_excluded, bind_scalar_expr,
    bind_scalar_expr_allow_excluded, scalar_expr_type,
};
use self::query::bind_with_search_path;
use self::returning::bind_returning_clause;
pub(super) use self::time::{BindTimeContext, bind_time_scalar_func};

#[derive(Clone)]
struct CteBinding {
    schema: Schema,
}

type CteScope = HashMap<String, CteBinding>;

fn bind_window_spec(
    spec: &WindowSpec,
    schema: &Schema,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
) -> PgWireResult<WindowSpec> {
    let mut partition_by = Vec::with_capacity(spec.partition_by.len());
    for expr in &spec.partition_by {
        partition_by.push(bind_scalar_expr(
            expr,
            schema,
            None,
            db,
            search_path,
            current_database,
            time_ctx,
        )?);
    }
    let mut order_by = Vec::with_capacity(spec.order_by.len());
    for key in &spec.order_by {
        match key {
            SortKey::ByName {
                col,
                asc,
                nulls_first,
            } => {
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f.name == *col)
                    .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                order_by.push(SortKey::ByIndex {
                    idx,
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
            }
            SortKey::ByIndex {
                idx,
                asc,
                nulls_first,
            } => order_by.push(SortKey::ByIndex {
                idx: *idx,
                asc: *asc,
                nulls_first: *nulls_first,
            }),
            SortKey::Expr {
                expr,
                asc,
                nulls_first,
            } => {
                let bound = bind_scalar_expr(
                    expr,
                    schema,
                    None,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?;
                order_by.push(SortKey::Expr {
                    expr: bound,
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
            }
        }
    }
    Ok(WindowSpec {
        partition_by,
        order_by,
    })
}

pub fn bind(db: &Db, session: &Session, p: Plan) -> PgWireResult<Plan> {
    let search_path = session.search_path();
    let current_database = session.database_name();
    let time_ctx =
        BindTimeContext::new(session.statement_time_micros(), session.txn_start_micros());
    let cte_scope = CteScope::new();
    match p {
        Plan::DeclareCursor { name, query } => Ok(Plan::DeclareCursor {
            name,
            query: Box::new(bind_with_search_path(
                db,
                &search_path,
                current_database.as_deref(),
                time_ctx,
                &cte_scope,
                *query,
            )?),
        }),
        Plan::FetchCursor { name } => session
            .cursor(&name)
            .ok_or_else(|| fe_code("34000", format!("cursor \"{name}\" does not exist"))),
        other => bind_with_search_path(
            db,
            &search_path,
            current_database.as_deref(),
            time_ctx,
            &cte_scope,
            other,
        ),
    }
}

fn resolve_table_meta<'a>(
    db: &'a Db,
    search_path: &[SchemaId],
    table: &ObjName,
) -> anyhow::Result<&'a crate::catalog::TableMeta> {
    if let Some(schema) = &table.schema {
        db.resolve_table(schema.as_str(), &table.name)
    } else {
        db.resolve_table_in_search_path(search_path, &table.name)
    }
}

#[derive(Clone)]
struct MergedColumn {
    spec: ColumnSpec,
    conflicting_default: bool,
    is_local: bool,
}

fn bind_inherited_columns(
    db: &Db,
    search_path: &[SchemaId],
    local_columns: Vec<ColumnSpec>,
    parents: Vec<ObjName>,
) -> PgWireResult<(Vec<ColumnSpec>, Vec<ObjName>)> {
    let mut merged: Vec<MergedColumn> = Vec::new();
    let mut resolved_parents = Vec::with_capacity(parents.len());
    let mut parent_ids = HashSet::with_capacity(parents.len());

    for mut parent in parents {
        let parent_meta = resolve_table_meta(db, search_path, &parent).map_err(map_catalog_err)?;
        if !parent_ids.insert(parent_meta.id) {
            return Err(fe_code(
                "42710",
                format!(
                    "relation \"{}\" would be inherited from more than once",
                    parent_meta.name
                ),
            ));
        }
        if parent.schema.is_none() {
            parent.schema = Some(parent_meta.schema.clone());
        }
        resolved_parents.push(parent);

        for column in &parent_meta.columns {
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| existing.spec.0 == column.name)
            {
                if existing.spec.1 != column.data_type {
                    return Err(fe_code(
                        "42804",
                        format!("inherited column \"{}\" has a type conflict", column.name),
                    ));
                }
                existing.spec.2 &= column.nullable;
                match (&existing.spec.3, &column.default) {
                    (Some(left), Some(right)) if left != right => {
                        existing.conflicting_default = true;
                    }
                    (None, Some(default)) => existing.spec.3 = Some(default.clone()),
                    _ => {}
                }
            } else {
                merged.push(MergedColumn {
                    spec: (
                        column.name.clone(),
                        column.data_type.clone(),
                        column.nullable,
                        column.default.clone(),
                        None,
                    ),
                    conflicting_default: false,
                    is_local: false,
                });
            }
        }
    }

    for local in local_columns {
        let (name, data_type, nullable, default, identity) = local;
        if let Some(existing) = merged.iter_mut().find(|existing| existing.spec.0 == name) {
            if existing.is_local {
                return Err(fe_code(
                    "42701",
                    format!("column \"{name}\" specified more than once"),
                ));
            }
            if existing.spec.1 != data_type {
                return Err(fe_code(
                    "42804",
                    format!("column \"{name}\" has a type conflict"),
                ));
            }
            existing.spec.2 &= nullable;
            if default.is_some() {
                existing.spec.3 = default;
                existing.conflicting_default = false;
            } else if existing.conflicting_default {
                return Err(fe_code(
                    "42611",
                    format!("column \"{name}\" inherits conflicting default values"),
                ));
            }
            if identity.is_some() {
                existing.spec.2 = false;
                existing.spec.4 = identity;
            }
            existing.is_local = true;
        } else {
            merged.push(MergedColumn {
                spec: (name, data_type, nullable, default, identity),
                conflicting_default: false,
                is_local: true,
            });
        }
    }

    if let Some(conflict) = merged.iter().find(|column| column.conflicting_default) {
        return Err(fe_code(
            "42611",
            format!(
                "column \"{}\" inherits conflicting default values",
                conflict.spec.0
            ),
        ));
    }

    Ok((
        merged.into_iter().map(|column| column.spec).collect(),
        resolved_parents,
    ))
}

pub(super) fn current_schema_name(db: &Db, search_path: &[SchemaId]) -> String {
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(schema_id) {
            return entry.name.as_str().to_string();
        }
    }
    "public".to_string()
}

pub(super) fn schema_names_for_path(db: &Db, search_path: &[SchemaId]) -> Vec<String> {
    let mut names = Vec::new();
    for schema_id in search_path {
        if let Some(entry) = db.catalog.schemas.get(schema_id) {
            names.push(entry.name.as_str().to_string());
        }
    }
    if names.is_empty() {
        names.push("public".to_string());
    }
    names
}

fn map_catalog_err(err: Error) -> PgWireError {
    if let Some(sql) = err.downcast_ref::<SqlError>() {
        fe_code(sql.code, sql.message.clone())
    } else {
        fe(err.to_string())
    }
}

fn should_defer_cte_binding(
    err: &PgWireError,
    unresolved_names: &HashSet<String>,
    cte_name: &str,
) -> bool {
    let msg = err.to_string();
    if !msg.contains("42P01") && !msg.contains("no such table") {
        return false;
    }
    unresolved_names
        .iter()
        .filter(|name| name.as_str() != cte_name)
        .any(|name| cte_name_appears_in_relation_error(&msg, name))
        || cte_name_appears_in_relation_error(&msg, cte_name)
}

fn cte_name_appears_in_relation_error(msg: &str, cte_name: &str) -> bool {
    msg.contains(&format!("no such table {cte_name}"))
        || msg.contains(&format!("no such table public.{cte_name}"))
        || msg.contains(&format!(".{cte_name}"))
}

#[allow(clippy::too_many_arguments)]
fn bind_update_sets(
    sets: Vec<UpdateSet>,
    schema: &Schema,
    tm: &TableMeta,
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    allow_excluded: bool,
) -> PgWireResult<Vec<UpdateSet>> {
    let mut bound_sets = Vec::with_capacity(sets.len());
    for set in sets {
        match set {
            UpdateSet::ByIndex(idx, expr) => {
                let hint = schema.field(idx).data_type.clone();
                let bound_expr = if allow_excluded {
                    bind_scalar_expr_allow_excluded(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                } else {
                    bind_scalar_expr(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                };
                bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
            }
            UpdateSet::ByName(name, expr) => {
                let idx = tm
                    .columns
                    .iter()
                    .position(|c| c.name == name)
                    .ok_or_else(|| fe_code("42703", format!("unknown column in UPDATE: {name}")))?;
                let hint = schema.field(idx).data_type.clone();
                let bound_expr = if allow_excluded {
                    bind_scalar_expr_allow_excluded(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                } else {
                    bind_scalar_expr(
                        &expr,
                        schema,
                        Some(&hint),
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                    )?
                };
                bound_sets.push(UpdateSet::ByIndex(idx, bound_expr));
            }
        }
    }
    if bound_sets.is_empty() {
        return Err(fe("UPDATE requires SET clauses"));
    }
    Ok(bound_sets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::engine::DataType;
    use crate::session::Session;
    use crate::sql::Planner;

    fn make_session(db: &Db) -> Session {
        let session = Session::new(7);
        let public_id = db.catalog.schema_id("public").expect("public schema");
        session.set_search_path(vec![public_id]);
        session
    }

    fn contains_cte_scan(plan: &Plan, name: &str) -> bool {
        match plan {
            Plan::CteScan {
                name: scan_name, ..
            } => scan_name == name,
            Plan::With { ctes, body } => {
                ctes.iter().any(|cte| contains_cte_scan(&cte.plan, name))
                    || contains_cte_scan(body, name)
            }
            Plan::Projection { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Order { input, .. }
            | Plan::Limit { input, .. }
            | Plan::CountRows { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Alias { input, .. }
            | Plan::Aggregate { input, .. } => contains_cte_scan(input, name),
            Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right, .. } => {
                contains_cte_scan(left, name) || contains_cte_scan(right, name)
            }
            Plan::Update { from, .. } => from
                .as_ref()
                .is_some_and(|from_plan| contains_cte_scan(from_plan, name)),
            Plan::InsertSelect { select, .. } => contains_cte_scan(select, name),
            _ => false,
        }
    }

    fn contains_seq_scan(plan: &Plan, table_name: &str) -> bool {
        match plan {
            Plan::SeqScan { table, .. } => table.name == table_name,
            Plan::With { ctes, body } => {
                ctes.iter()
                    .any(|cte| contains_seq_scan(&cte.plan, table_name))
                    || contains_seq_scan(body, table_name)
            }
            Plan::Projection { input, .. }
            | Plan::Filter { input, .. }
            | Plan::Order { input, .. }
            | Plan::Limit { input, .. }
            | Plan::CountRows { input, .. }
            | Plan::LockRows { input, .. }
            | Plan::Alias { input, .. }
            | Plan::Aggregate { input, .. } => contains_seq_scan(input, table_name),
            Plan::Join { left, right, .. } | Plan::UnboundJoin { left, right, .. } => {
                contains_seq_scan(left, table_name) || contains_seq_scan(right, table_name)
            }
            Plan::Update { from, .. } => from
                .as_ref()
                .is_some_and(|from_plan| contains_seq_scan(from_plan, table_name)),
            Plan::InsertSelect { select, .. } => contains_seq_scan(select, table_name),
            _ => false,
        }
    }

    #[test]
    fn later_ctes_can_reference_earlier_ctes() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql(
            "with first as (select 1 as id), second as (select id from first) select id from second",
        )
        .expect("plan");
        let bound = bind(&db, &session, plan).expect("bind");
        assert!(contains_cte_scan(&bound, "first"));
        assert!(contains_cte_scan(&bound, "second"));
    }

    #[test]
    fn earlier_ctes_can_reference_later_ctes() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql(
            "with second as (select id from first), first as (select 1 as id) select id from second",
        )
        .expect("plan");
        let bound = bind(&db, &session, plan).expect("bind");
        assert!(contains_cte_scan(&bound, "first"));
        assert!(contains_cte_scan(&bound, "second"));
    }

    #[test]
    fn cte_scope_is_statement_local() {
        let db = Db::default();
        let session = make_session(&db);
        let with_plan = Planner::plan_sql("with scoped as (select 1 as id) select id from scoped")
            .expect("plan");
        bind(&db, &session, with_plan).expect("bind with cte");

        let plain_plan = Planner::plan_sql("select id from scoped").expect("plan plain");
        let err = bind(&db, &session, plain_plan).expect_err("expected unknown table");
        assert!(
            err.to_string().contains("no such table"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cte_name_shadows_catalog_table_with_same_name() {
        let mut db = Db::default();
        let public_id = db.catalog.schema_id("public").expect("public schema");
        db.create_table(
            "public",
            "dupe",
            vec![("id".to_string(), DataType::Int4, true, None, None)],
            None,
            Vec::new(),
            &[public_id],
        )
        .expect("create table");
        let session = make_session(&db);

        let with_plan =
            Planner::plan_sql("with dupe as (select 1 as id) select id from dupe").expect("plan");
        let bound_with = bind(&db, &session, with_plan).expect("bind");
        assert!(contains_cte_scan(&bound_with, "dupe"));

        let plain_plan = Planner::plan_sql("select id from dupe").expect("plan plain");
        let bound_plain = bind(&db, &session, plain_plan).expect("bind plain");
        assert!(contains_seq_scan(&bound_plain, "dupe"));
    }

    #[test]
    fn cte_column_alias_count_must_match_projection_width() {
        let db = Db::default();
        let session = make_session(&db);
        let plan = Planner::plan_sql("with c(a, b) as (select 1) select a from c").expect("plan");
        let err = bind(&db, &session, plan).expect_err("expected alias mismatch error");
        assert!(
            err.to_string()
                .contains("CTE \"c\" has 1 columns but 2 column aliases were provided"),
            "unexpected error: {err}"
        );
    }
}
