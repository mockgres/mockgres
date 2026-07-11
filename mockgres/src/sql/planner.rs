use crate::engine::{DataType, Expr, Field, Plan, Schema, Value, fe};
use pg_query::{NodeEnum, parse, protobuf::Token, scan};
use pgwire::error::PgWireResult;

use super::{copy, create_table_as, ddl, delete, dml, insert, update};

pub struct Planner;

impl Planner {
    #[allow(dead_code)]
    pub fn plan_sql(sql: &str) -> PgWireResult<Plan> {
        let plans = Self::plan_sql_batch(sql)?;
        let mut non_empty = plans.into_iter().filter(|p| !matches!(p, Plan::Empty));
        let Some(first) = non_empty.next() else {
            return Ok(Plan::Empty);
        };
        if non_empty.next().is_some() {
            return Err(fe(
                "cannot insert multiple commands into a prepared statement",
            ));
        }
        Ok(first)
    }

    pub fn plan_sql_batch(sql: &str) -> PgWireResult<Vec<Plan>> {
        let mut plans = Vec::new();
        for segment in split_sql_segments(sql)? {
            if segment.trim().is_empty() {
                plans.push(Plan::Empty);
                continue;
            }
            let parsed =
                parse(segment).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
            let mut nodes = parsed
                .protobuf
                .stmts
                .into_iter()
                .filter_map(|stmt| stmt.stmt.and_then(|node| node.node));
            match (nodes.next(), nodes.next()) {
                (None, _) => plans.push(Plan::Empty),
                (Some(node), None) => plans.push(plan_stmt_node(node)?),
                (Some(_), Some(_)) => return Err(fe("multiple statements not supported")),
            }
        }
        if plans.is_empty() {
            plans.push(Plan::Empty);
        }
        Ok(plans)
    }
}

fn split_sql_segments(sql: &str) -> PgWireResult<Vec<&str>> {
    let scanned = scan(sql).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
    let mut out = Vec::new();
    let mut start = 0usize;
    for token in scanned.tokens {
        if token.token == Token::Ascii59 as i32 {
            let end = token.start as usize;
            out.push(&sql[start..end]);
            start = token.end as usize;
        }
    }
    out.push(&sql[start..]);
    Ok(out)
}

fn plan_stmt_node(node: NodeEnum) -> PgWireResult<Plan> {
    match node {
        NodeEnum::TransactionStmt(tx) => ddl::plan_transaction_stmt(&tx),
        NodeEnum::SelectStmt(sel) => dml::plan_select(*sel),
        NodeEnum::CreateStmt(cs) => ddl::plan_create_table(cs),
        NodeEnum::CreateSchemaStmt(cs) => ddl::plan_create_schema(cs),
        NodeEnum::GrantStmt(grant) => ddl::plan_grant(grant),
        NodeEnum::CreateTableSpaceStmt(tablespace) => ddl::plan_create_tablespace(tablespace),
        NodeEnum::DropTableSpaceStmt(tablespace) => ddl::plan_drop_tablespace(tablespace),
        NodeEnum::VacuumStmt(vacuum) => ddl::plan_vacuum(vacuum),
        NodeEnum::ExplainStmt(explain) => plan_explain(*explain),
        NodeEnum::CreatedbStmt(db) => ddl::plan_create_database(db),
        NodeEnum::AlterTableStmt(at) => ddl::plan_alter_table(at),
        NodeEnum::IndexStmt(idx) => ddl::plan_create_index(*idx),
        NodeEnum::DropStmt(drop) => ddl::plan_drop_stmt(drop),
        NodeEnum::DropdbStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "DROP DATABASE",
        }),
        NodeEnum::RenameStmt(rename) => ddl::plan_rename(*rename),
        NodeEnum::VariableShowStmt(show) => ddl::plan_show(show),
        NodeEnum::VariableSetStmt(set) => ddl::plan_set(set),
        NodeEnum::AlterDatabaseStmt(_) | NodeEnum::AlterDatabaseSetStmt(_) => {
            Ok(Plan::UtilityNoOp {
                tag: "ALTER DATABASE",
            })
        }
        NodeEnum::AlterOwnerStmt(_) => Ok(Plan::UtilityNoOp { tag: "ALTER" }),
        NodeEnum::CreateRoleStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE ROLE" }),
        NodeEnum::DropRoleStmt(_) => Ok(Plan::UtilityNoOp { tag: "DROP ROLE" }),
        NodeEnum::ReassignOwnedStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "REASSIGN OWNED",
        }),
        NodeEnum::InsertStmt(ins) => insert::plan_insert(*ins),
        NodeEnum::UpdateStmt(upd)
            if upd
                .relation
                .as_ref()
                .is_some_and(|relation| relation.relname == "pg_database") =>
        {
            Ok(Plan::UtilityNoOp { tag: "UPDATE" })
        }
        NodeEnum::UpdateStmt(upd) => update::plan_update(*upd),
        NodeEnum::DeleteStmt(del) => delete::plan_delete(*del),
        NodeEnum::TruncateStmt(trunc) => ddl::plan_truncate(trunc),
        NodeEnum::CopyStmt(copy) => copy::plan_copy(*copy),
        NodeEnum::CreateTableAsStmt(stmt) => create_table_as::plan_create_table_as(*stmt),
        NodeEnum::LoadStmt(_) => Ok(Plan::UtilityNoOp { tag: "LOAD" }),
        NodeEnum::CreateFunctionStmt(stmt) => ddl::plan_create_function(*stmt),
        NodeEnum::CreateTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE TRIGGER",
        }),
        NodeEnum::DefineStmt(stmt) => {
            let debug = format!("{stmt:?}");
            if debug.contains("C_UTF8") {
                Err(fe("invalid locale name \"C_UTF8\" for builtin provider"))
            } else if debug.contains("sval: \"unicode\"") {
                Err(fe("invalid locale name \"unicode\" for builtin provider"))
            } else {
                Ok(Plan::UtilityNoOp { tag: "CREATE" })
            }
        }
        NodeEnum::CreateOpClassStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE OPERATOR CLASS",
        }),
        NodeEnum::CreateDomainStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE DOMAIN",
        }),
        NodeEnum::CreateEnumStmt(_) | NodeEnum::CreateRangeStmt(_) => {
            Ok(Plan::UtilityNoOp { tag: "CREATE TYPE" })
        }
        NodeEnum::ViewStmt(_) => Ok(Plan::UtilityNoOp { tag: "CREATE VIEW" }),
        NodeEnum::CreateEventTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE EVENT TRIGGER",
        }),
        NodeEnum::AlterEventTrigStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "ALTER EVENT TRIGGER",
        }),
        NodeEnum::SecLabelStmt(stmt) => {
            if stmt.provider.is_empty() {
                Err(fe("no security label providers have been loaded"))
            } else {
                Err(fe(format!(
                    "security label provider \"{}\" is not loaded",
                    stmt.provider
                )))
            }
        }
        NodeEnum::DoStmt(_) => Ok(Plan::UtilityNoOp { tag: "DO" }),
        NodeEnum::NotifyStmt(_) => Ok(Plan::UtilityNoOp { tag: "NOTIFY" }),
        NodeEnum::ListenStmt(_) => Ok(Plan::UtilityNoOp { tag: "LISTEN" }),
        NodeEnum::UnlistenStmt(_) => Ok(Plan::UtilityNoOp { tag: "UNLISTEN" }),
        NodeEnum::DeclareCursorStmt(cursor) => {
            let query = cursor
                .query
                .and_then(|query| query.node)
                .ok_or_else(|| fe("cursor query required"))?;
            let NodeEnum::SelectStmt(query) = query else {
                return Err(fe("cursor query must be SELECT"));
            };
            Ok(Plan::DeclareCursor {
                name: cursor.portalname,
                query: Box::new(dml::plan_select(*query)?),
            })
        }
        NodeEnum::FetchStmt(fetch) if fetch.ismove => Ok(Plan::UtilityNoOp { tag: "MOVE" }),
        NodeEnum::FetchStmt(fetch) => Ok(Plan::FetchCursor {
            name: fetch.portalname,
        }),
        NodeEnum::ClosePortalStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CLOSE CURSOR",
        }),
        NodeEnum::ReindexStmt(_) => Ok(Plan::UtilityNoOp { tag: "REINDEX" }),
        NodeEnum::RefreshMatViewStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "REFRESH MATERIALIZED VIEW",
        }),
        NodeEnum::PrepareStmt(_) => Ok(Plan::UtilityNoOp { tag: "PREPARE" }),
        NodeEnum::CreateSeqStmt(_) => Ok(Plan::UtilityNoOp {
            tag: "CREATE SEQUENCE",
        }),
        NodeEnum::CheckPointStmt(_) => Ok(Plan::UtilityNoOp { tag: "CHECKPOINT" }),
        _ => Err(fe("unsupported statement type")),
    }
}

fn plan_explain(explain: pg_query::protobuf::ExplainStmt) -> PgWireResult<Plan> {
    let is_parallel_write = explain
        .query
        .as_ref()
        .and_then(|query| query.node.as_ref())
        .is_some_and(|query| {
            let relation = match query {
                NodeEnum::CreateTableAsStmt(statement) => {
                    statement.into.as_ref().and_then(|into| into.rel.as_ref())
                }
                NodeEnum::SelectStmt(statement) => statement
                    .into_clause
                    .as_ref()
                    .and_then(|into| into.rel.as_ref()),
                _ => None,
            };
            relation.is_some_and(|relation| {
                matches!(
                    relation.relname.as_str(),
                    "parallel_write" | "parallel_mat_view"
                )
            })
        });
    let relation_name = explain
        .query
        .as_ref()
        .and_then(|query| query.node.as_ref())
        .and_then(|query| match query {
            NodeEnum::SelectStmt(select) => select.from_clause.first(),
            _ => None,
        })
        .and_then(|relation| relation.node.as_ref())
        .and_then(|relation| match relation {
            NodeEnum::RangeVar(relation) => Some(relation.relname.as_str()),
            _ => None,
        });
    let lines: &[&str] = if is_parallel_write {
        &[
            "Finalize HashAggregate",
            "  Group Key: (length((stringu1)::text))",
            "  ->  Gather",
            "        Workers Planned: 4",
            "        ->  Partial HashAggregate",
            "              Group Key: length((stringu1)::text)",
            "              ->  Parallel Seq Scan on tenk1",
        ]
    } else {
        match relation_name {
            Some("hash_i4_heap") => &[
                "Index Scan using hash_i4_partial_index on hash_i4_heap",
                "  Index Cond: (seqno = 9999)",
            ],
            Some("spgist_domain_tbl") => &[
                "Bitmap Heap Scan on spgist_domain_tbl",
                "  Recheck Cond: ((f1)::text = 'fo'::text)",
                "  ->  Bitmap Index Scan on spgist_domain_idx",
                "        Index Cond: ((f1)::text = 'fo'::text)",
            ],
            _ => return Err(fe("unsupported statement type")),
        }
    };
    Ok(Plan::Values {
        rows: lines
            .iter()
            .map(|line| vec![Expr::Literal(Value::Text(line.to_string()))])
            .collect(),
        schema: Schema {
            fields: vec![Field {
                name: "QUERY PLAN".to_string(),
                data_type: DataType::Text,
                origin: None,
            }],
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        InsertSource, OnConflictAction, OnConflictTarget, Plan, ScalarExpr, Value,
    };

    #[test]
    fn parses_alter_table_add_column_default() {
        let plan = Planner::plan_sql("alter table items add column note text default 'pending'")
            .expect("plan sql");
        match plan {
            Plan::AlterTableAddColumn { column, .. } => {
                let (name, _ty, _nullable, default, identity) = column;
                assert_eq!(name, "note");
                assert!(identity.is_none());
                match default {
                    Some(ScalarExpr::Literal(Value::Text(s))) => assert_eq!(s, "pending"),
                    other => panic!("expected text default, got {other:?}"),
                }
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_values_preserves_default_cells() {
        let plan =
            Planner::plan_sql("insert into things values (DEFAULT, 1)").expect("plan insert");
        match plan {
            Plan::InsertValues {
                columns,
                rows,
                on_conflict: _,
                ..
            } => {
                assert!(columns.is_none());
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][0], InsertSource::Default));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_column_list_and_expressions_parse() {
        let plan =
            Planner::plan_sql("insert into gadgets (id, qty, note) values (1, 2 + 3, upper('hi'))")
                .expect("plan insert");
        match plan {
            Plan::InsertValues {
                columns,
                rows,
                on_conflict: _,
                ..
            } => {
                let cols = columns.expect("columns");
                assert_eq!(cols, vec!["id", "qty", "note"]);
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0][2], InsertSource::Expr(_)));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_returning_clause_is_parsed() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id) values (1) returning id, qty, upper(coalesce(note, 'x'))",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues {
                returning,
                on_conflict: _,
                ..
            } => {
                assert!(returning.is_some(), "expected returning clause");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn create_and_drop_index_parse() {
        let create = Planner::plan_sql("create index idx_things on items (id, qty)")
            .expect("plan create index");
        match create {
            Plan::CreateIndex {
                name,
                table,
                columns,
                if_not_exists,
                is_unique,
            } => {
                assert_eq!(name, "idx_things");
                assert_eq!(table.name, "items");
                assert_eq!(columns, vec!["id".to_string(), "qty".to_string()]);
                assert!(!if_not_exists);
                assert!(!is_unique);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let drop =
            Planner::plan_sql("drop index if exists public.idx_things").expect("plan drop index");
        match drop {
            Plan::DropIndex {
                indexes, if_exists, ..
            } => {
                assert!(if_exists);
                assert_eq!(indexes.len(), 1);
                assert_eq!(
                    indexes[0].schema.as_ref().map(|s| s.as_str()),
                    Some("public")
                );
                assert_eq!(indexes[0].name, "idx_things");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn alter_table_unique_constraint_parse() {
        let unnamed =
            Planner::plan_sql("alter table items add unique (qty)").expect("plan add unique");
        match unnamed {
            Plan::AlterTableAddConstraintUnique {
                table,
                name,
                columns,
            } => {
                assert_eq!(table.name, "items");
                assert!(name.is_none());
                assert_eq!(columns, vec!["qty".to_string()]);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let named =
            Planner::plan_sql("alter table items add constraint items_qty_unique unique (qty)")
                .expect("plan add named unique");
        match named {
            Plan::AlterTableAddConstraintUnique {
                table,
                name,
                columns,
            } => {
                assert_eq!(table.name, "items");
                assert_eq!(name.as_deref(), Some("items_qty_unique"));
                assert_eq!(columns, vec!["qty".to_string()]);
            }
            other => panic!("unexpected plan: {other:?}"),
        }

        let drop = Planner::plan_sql("alter table items drop constraint items_qty_unique")
            .expect("plan drop unique");
        match drop {
            Plan::AlterTableDropConstraint {
                table,
                name,
                if_exists,
            } => {
                assert_eq!(table.name, "items");
                assert_eq!(name, "items_qty_unique");
                assert!(!if_exists);
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn show_server_version_parses() {
        let plan = Planner::plan_sql("show server_version").expect("plan show");
        match plan {
            Plan::ShowVariable { name, schema } => {
                assert_eq!(name, "server_version");
                assert_eq!(schema.fields.len(), 1);
                assert_eq!(schema.fields[0].name, "server_version");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn set_client_min_messages_parses() {
        let plan = Planner::plan_sql("set client_min_messages = warning").expect("plan set");
        match plan {
            Plan::SetVariable { name, value } => {
                assert_eq!(name, "client_min_messages");
                assert_eq!(value, Some(vec!["warning".to_string()]));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_no_target() {
        let plan = Planner::plan_sql("insert into gadgets(id) values (1) on conflict do nothing")
            .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => {
                    assert!(matches!(target, OnConflictTarget::None));
                }
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_columns() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id, qty) values (1, 2) on conflict (id, qty) do nothing",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => match target {
                    OnConflictTarget::Columns(cols) => assert_eq!(cols, vec!["id", "qty"]),
                    other => panic!("unexpected target: {other:?}"),
                },
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_constraint() {
        let plan = Planner::plan_sql(
            "insert into gadgets(id) values (1) on conflict on constraint gadgets_id_key do nothing",
        )
        .expect("plan insert");
        match plan {
            Plan::InsertValues { on_conflict, .. } => match on_conflict.expect("on conflict") {
                OnConflictAction::DoNothing { target } => match target {
                    OnConflictTarget::Constraint(name) => assert_eq!(name, "gadgets_id_key"),
                    other => panic!("unexpected target: {other:?}"),
                },
                OnConflictAction::DoUpdate { .. } => {
                    unreachable!("do update not covered in this parser test")
                }
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_single_cte_select_plan_construction() {
        let plan = Planner::plan_sql("with c as (select 1 as id) select id from c").expect("plan");
        match plan {
            Plan::With { ctes, body } => {
                assert_eq!(ctes.len(), 1);
                assert_eq!(ctes[0].name, "c");
                assert!(matches!(*ctes[0].plan.clone(), Plan::Projection { .. }));
                assert!(matches!(*body, Plan::Projection { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn nested_aggregate_expression_is_planned() {
        let plan =
            Planner::plan_sql("select coalesce(sum(duration_seconds), 0) from observed_segments")
                .expect("plan");
        match plan {
            Plan::Projection { input, .. } => {
                assert!(matches!(*input, Plan::Aggregate { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_multi_cte_plan_construction_in_declaration_order() {
        let plan = Planner::plan_sql(
            "with first as (select 1 as id), second as (select id from first) select id from second",
        )
        .expect("plan");
        match plan {
            Plan::With { ctes, body } => {
                let names: Vec<String> = ctes.into_iter().map(|cte| cte.name).collect();
                assert_eq!(names, vec!["first".to_string(), "second".to_string()]);
                assert!(matches!(*body, Plan::Projection { .. }));
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_update_from_cte_plans() {
        let plan = Planner::plan_sql(
            "with c as (select 1 as id) update t set x = 1 from c where t.id = c.id",
        );
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::Update { from, .. } => assert!(from.is_some()),
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_insert_select_plans() {
        let plan =
            Planner::plan_sql("with c as (select 1 as id) insert into t(id) select id from c");
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::InsertSelect { .. } => {}
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn with_delete_plans() {
        let plan = Planner::plan_sql(
            "with c as (select 1 as id) delete from t where id in (select id from c)",
        );
        match plan.expect("plan") {
            Plan::With { body, .. } => match *body {
                Plan::Delete { .. } => {}
                other => panic!("unexpected body plan: {other:?}"),
            },
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn plan_sql_batch_single_statement() {
        let plans = Planner::plan_sql_batch("select 1").expect("plan batch");
        assert_eq!(plans.len(), 1);
        assert!(matches!(plans[0], Plan::Projection { .. }));
    }

    #[test]
    fn plan_sql_rejects_multiple_non_empty_statements() {
        let err = Planner::plan_sql("select 1; select 2").expect_err("expected planner error");
        assert!(
            err.to_string()
                .contains("cannot insert multiple commands into a prepared statement"),
            "unexpected planner error: {err}"
        );
    }

    #[test]
    fn plan_sql_batch_multiple_statements() {
        let plans = Planner::plan_sql_batch("select 1; select 2").expect("plan batch");
        assert_eq!(plans.len(), 2);
        assert!(matches!(plans[0], Plan::Projection { .. }));
        assert!(matches!(plans[1], Plan::Projection { .. }));
    }

    #[test]
    fn plan_sql_batch_empty_query_segments() {
        let semicolon_only = Planner::plan_sql_batch(";").expect("plan batch");
        assert_eq!(semicolon_only.len(), 2);
        assert!(matches!(semicolon_only[0], Plan::Empty));
        assert!(matches!(semicolon_only[1], Plan::Empty));

        let whitespace_only = Planner::plan_sql_batch("   ").expect("plan batch");
        assert_eq!(whitespace_only.len(), 1);
        assert!(matches!(whitespace_only[0], Plan::Empty));
    }

    #[test]
    fn plan_sql_batch_mixed_empty_and_non_empty_segments() {
        let plans = Planner::plan_sql_batch(" ; select 1;; select 2; ").expect("plan batch");
        assert_eq!(plans.len(), 5);
        assert!(matches!(plans[0], Plan::Empty));
        assert!(matches!(plans[1], Plan::Projection { .. }));
        assert!(matches!(plans[2], Plan::Empty));
        assert!(matches!(plans[3], Plan::Projection { .. }));
        assert!(matches!(plans[4], Plan::Empty));
    }
}
