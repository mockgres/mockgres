use crate::engine::{Plan, fe};
use pg_query::{NodeEnum, parse};
use pgwire::error::PgWireResult;

use super::{ddl, delete, dml, insert, update};

pub struct Planner;

impl Planner {
    pub fn plan_sql(sql: &str) -> PgWireResult<Plan> {
        let parsed = parse(sql).map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;
        let stmts: Vec<_> = parsed
            .protobuf
            .stmts
            .into_iter()
            .filter(|s| s.stmt.is_some())
            .collect();
        if stmts.len() > 1 {
            return Err(fe("multiple statements not supported"));
        }
        let Some(stmt) = stmts.into_iter().next() else {
            return Ok(Plan::Empty);
        };
        match stmt.stmt.and_then(|n| n.node) {
            Some(NodeEnum::TransactionStmt(tx)) => ddl::plan_transaction_stmt(&tx),
            Some(NodeEnum::SelectStmt(sel)) => dml::plan_select(*sel),
            Some(NodeEnum::CreateStmt(cs)) => ddl::plan_create_table(cs),
            Some(NodeEnum::CreateSchemaStmt(cs)) => ddl::plan_create_schema(cs),
            Some(NodeEnum::CreatedbStmt(db)) => ddl::plan_create_database(db),
            Some(NodeEnum::AlterTableStmt(at)) => ddl::plan_alter_table(at),
            Some(NodeEnum::IndexStmt(idx)) => ddl::plan_create_index(*idx),
            Some(NodeEnum::DropStmt(drop)) => ddl::plan_drop_stmt(drop),
            Some(NodeEnum::DropdbStmt(db)) => ddl::plan_drop_database(db),
            Some(NodeEnum::RenameStmt(rename)) => ddl::plan_rename_schema(*rename),
            Some(NodeEnum::VariableShowStmt(show)) => ddl::plan_show(show),
            Some(NodeEnum::VariableSetStmt(set)) => ddl::plan_set(set),
            Some(NodeEnum::AlterDatabaseStmt(db)) => ddl::plan_alter_database(db),
            Some(NodeEnum::AlterDatabaseSetStmt(db)) => ddl::plan_alter_database_set(db),
            Some(NodeEnum::InsertStmt(ins)) => insert::plan_insert(*ins),
            Some(NodeEnum::UpdateStmt(upd)) => update::plan_update(*upd),
            Some(NodeEnum::DeleteStmt(del)) => delete::plan_delete(*del),
            _ => Err(fe("unsupported statement type")),
        }
    }
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
}
