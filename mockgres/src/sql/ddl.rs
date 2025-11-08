use crate::engine::{DataType, Field, ObjName, Plan, Schema, fe, fe_code};
use pg_query::protobuf::{
    AlterTableStmt, AlterTableType, CreateStmt, DropStmt, IndexStmt, ObjectType, TransactionStmt,
    VariableSetKind, VariableSetStmt, VariableShowStmt,
};
use pgwire::error::PgWireResult;

use super::tokens::{
    parse_column_def, parse_index_columns, parse_obj_name_from_list, parse_set_value,
};

pub(super) fn plan_transaction_stmt(stmt: &TransactionStmt) -> PgWireResult<Plan> {
    if !stmt.options.is_empty() {
        return Err(fe_code("0A000", "transaction options not supported"));
    }
    if stmt.chain {
        return Err(fe_code("0A000", "transaction chain not supported"));
    }
    let kind = pg_query::protobuf::TransactionStmtKind::try_from(stmt.kind)
        .map_err(|_| fe("unknown transaction kind"))?;
    match kind {
        pg_query::protobuf::TransactionStmtKind::TransStmtBegin
        | pg_query::protobuf::TransactionStmtKind::TransStmtStart => Ok(Plan::BeginTransaction),
        pg_query::protobuf::TransactionStmtKind::TransStmtCommit => Ok(Plan::CommitTransaction),
        pg_query::protobuf::TransactionStmtKind::TransStmtRollback => Ok(Plan::RollbackTransaction),
        pg_query::protobuf::TransactionStmtKind::Undefined => {
            Err(fe("transaction kind not specified"))
        }
        pg_query::protobuf::TransactionStmtKind::TransStmtSavepoint
        | pg_query::protobuf::TransactionStmtKind::TransStmtRelease
        | pg_query::protobuf::TransactionStmtKind::TransStmtRollbackTo => {
            Err(fe_code("0A000", "savepoints are not supported"))
        }
        pg_query::protobuf::TransactionStmtKind::TransStmtPrepare
        | pg_query::protobuf::TransactionStmtKind::TransStmtCommitPrepared
        | pg_query::protobuf::TransactionStmtKind::TransStmtRollbackPrepared => {
            Err(fe_code("0A000", "two-phase commit is not supported"))
        }
    }
}

pub(super) fn plan_create_table(stmt: CreateStmt) -> PgWireResult<Plan> {
    let rv = stmt.relation.ok_or_else(|| fe("missing table name"))?;
    let table = ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname)
        },
        name: rv.relname,
    };

    let mut cols = Vec::new();
    let mut pk: Option<Vec<String>> = None;

    for elt in stmt.table_elts {
        match elt.node.unwrap() {
            pg_query::NodeEnum::ColumnDef(cd) => {
                let (cname, dt, nullable, default) = parse_column_def(&cd)?;
                let col_name_clone = cname.clone();
                cols.push((cname, dt, nullable, default));
                if cd.constraints.iter().any(|c| {
                    matches!(
                        c.node.as_ref(),
                        Some(pg_query::NodeEnum::Constraint(cons))
                            if cons.contype == pg_query::protobuf::ConstrType::ConstrPrimary as i32
                    )
                }) {
                    pk = Some(vec![col_name_clone]);
                }
            }
            pg_query::NodeEnum::Constraint(cons) => {
                if cons.contype == pg_query::protobuf::ConstrType::ConstrPrimary as i32 {
                    let mut names = Vec::new();
                    for n in cons.keys {
                        let pg_query::NodeEnum::String(s) = n.node.unwrap() else {
                            continue;
                        };
                        names.push(s.sval);
                    }
                    pk = Some(names);
                }
            }
            _ => {}
        }
    }

    Ok(Plan::CreateTable { table, cols, pk })
}

pub(super) fn plan_alter_table(stmt: AlterTableStmt) -> PgWireResult<Plan> {
    let rv = stmt.relation.ok_or_else(|| fe("missing table name"))?;
    let table = ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname)
        },
        name: rv.relname,
    };
    if stmt.cmds.len() != 1 {
        return Err(fe("one ALTER TABLE command at a time"));
    }
    let cmd_node = stmt.cmds.into_iter().next().unwrap();
    let cmd = cmd_node.node.ok_or_else(|| fe("bad ALTER TABLE command"))?;
    let pg_query::NodeEnum::AlterTableCmd(cmd) = cmd else {
        return Err(fe("bad ALTER TABLE command"));
    };
    match AlterTableType::try_from(cmd.subtype).map_err(|_| fe("bad ALTER TABLE type"))? {
        AlterTableType::AtAddColumn => {
            let col_node = cmd
                .def
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("ADD COLUMN requires column definition"))?;
            let pg_query::NodeEnum::ColumnDef(cd) = col_node else {
                return Err(fe("ADD COLUMN expects column definition"));
            };
            let column = parse_column_def(cd)?;
            Ok(Plan::AlterTableAddColumn {
                table,
                column,
                if_not_exists: cmd.missing_ok,
            })
        }
        AlterTableType::AtDropColumn => {
            if cmd.name.is_empty() {
                return Err(fe("DROP COLUMN requires name"));
            }
            Ok(Plan::AlterTableDropColumn {
                table,
                column: cmd.name,
                if_exists: cmd.missing_ok,
            })
        }
        _ => Err(fe("unsupported ALTER TABLE command")),
    }
}

pub(super) fn plan_create_index(idx: IndexStmt) -> PgWireResult<Plan> {
    let table_rv = idx.relation.ok_or_else(|| fe("missing index table"))?;
    let table = ObjName {
        schema: if table_rv.schemaname.is_empty() {
            None
        } else {
            Some(table_rv.schemaname)
        },
        name: table_rv.relname,
    };
    if idx.idxname.is_empty() {
        return Err(fe("index name required"));
    }
    let columns = parse_index_columns(&idx.index_params)?;
    Ok(Plan::CreateIndex {
        name: idx.idxname,
        table,
        columns,
        if_not_exists: idx.if_not_exists,
    })
}

pub(super) fn plan_drop_stmt(drop: DropStmt) -> PgWireResult<Plan> {
    let remove_type = ObjectType::try_from(drop.remove_type).map_err(|_| fe("bad drop type"))?;
    if remove_type != ObjectType::ObjectIndex {
        return Err(fe("only DROP INDEX supported here"));
    }
    if drop.objects.is_empty() {
        return Err(fe("DROP INDEX requires names"));
    }
    let mut names = Vec::with_capacity(drop.objects.len());
    for obj in drop.objects {
        let node = obj.node.ok_or_else(|| fe("bad DROP INDEX name"))?;
        names.push(parse_obj_name_from_list(&node)?);
    }
    Ok(Plan::DropIndex {
        indexes: names,
        if_exists: drop.missing_ok,
    })
}

pub(super) fn plan_show(show: VariableShowStmt) -> PgWireResult<Plan> {
    let schema = Schema {
        fields: vec![Field {
            name: show.name.clone(),
            data_type: DataType::Text,
        }],
    };
    Ok(Plan::ShowVariable {
        name: show.name.to_ascii_lowercase(),
        schema,
    })
}

pub(super) fn plan_set(set: VariableSetStmt) -> PgWireResult<Plan> {
    let name_lower = set.name.to_ascii_lowercase();
    if name_lower != "client_min_messages" {
        return Err(fe_code("0A000", format!("SET {} not supported", set.name)));
    }
    let kind = VariableSetKind::try_from(set.kind).map_err(|_| fe("bad SET kind"))?;
    let value = match kind {
        VariableSetKind::VarSetValue | VariableSetKind::VarSetCurrent => {
            Some(parse_set_value(&set.args)?)
        }
        VariableSetKind::VarSetDefault
        | VariableSetKind::VarReset
        | VariableSetKind::VarResetAll => None,
        VariableSetKind::VarSetMulti => {
            return Err(fe("SET MULTI not supported"));
        }
        VariableSetKind::Undefined => return Err(fe("bad SET kind")),
    };
    Ok(Plan::SetVariable {
        name: name_lower,
        value,
    })
}
