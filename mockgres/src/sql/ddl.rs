use crate::engine::{DataType, Field, ForeignKeySpec, ObjName, Plan, Schema, fe, fe_code};
use pg_query::protobuf::{
    AlterTableStmt, AlterTableType, Constraint, CreateStmt, DropStmt, IndexStmt, ObjectType,
    RangeVar, TransactionStmt, VariableSetKind, VariableSetStmt, VariableShowStmt,
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
    let mut foreign_keys = Vec::new();

    for elt in stmt.table_elts {
        match elt.node.unwrap() {
            pg_query::NodeEnum::ColumnDef(cd) => {
                let (cname, dt, nullable, default) = parse_column_def(&cd)?;
                let column_fks = collect_column_foreign_keys(&cd, &cname)?;
                foreign_keys.extend(column_fks);
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
                } else if cons.contype == pg_query::protobuf::ConstrType::ConstrForeign as i32 {
                    if let Some(fk) = parse_foreign_key_constraint(&cons, None)? {
                        foreign_keys.push(fk);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Plan::CreateTable {
        table,
        cols,
        pk,
        foreign_keys,
    })
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
    if drop.objects.is_empty() {
        return Err(fe("DROP requires at least one name"));
    }
    let mut names = Vec::with_capacity(drop.objects.len());
    for obj in drop.objects {
        let node = obj.node.ok_or_else(|| fe("bad DROP name"))?;
        names.push(parse_obj_name_from_list(&node)?);
    }
    match remove_type {
        ObjectType::ObjectIndex => Ok(Plan::DropIndex {
            indexes: names,
            if_exists: drop.missing_ok,
        }),
        ObjectType::ObjectTable => Ok(Plan::DropTable {
            tables: names,
            if_exists: drop.missing_ok,
        }),
        _ => Err(fe("only DROP INDEX or DROP TABLE supported")),
    }
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

fn collect_column_foreign_keys(
    cd: &pg_query::protobuf::ColumnDef,
    column_name: &str,
) -> PgWireResult<Vec<ForeignKeySpec>> {
    let mut out = Vec::new();
    for cons in &cd.constraints {
        let Some(pg_query::NodeEnum::Constraint(c)) = cons.node.as_ref() else {
            continue;
        };
        if let Some(fk) = parse_foreign_key_constraint(c, Some(column_name))? {
            out.push(fk);
        }
    }
    Ok(out)
}

fn parse_foreign_key_constraint(
    cons: &Constraint,
    default_column: Option<&str>,
) -> PgWireResult<Option<ForeignKeySpec>> {
    if cons.contype != pg_query::protobuf::ConstrType::ConstrForeign as i32 {
        return Ok(None);
    }
    let columns = if !cons.fk_attrs.is_empty() {
        parse_identifier_list(&cons.fk_attrs)?
    } else if let Some(col) = default_column {
        vec![col.to_string()]
    } else {
        return Err(fe("FOREIGN KEY requires column list"));
    };
    let pktable = cons
        .pktable
        .as_ref()
        .ok_or_else(|| fe("FOREIGN KEY requires referenced table"))?;
    let referenced_table = range_var_to_obj_name(pktable);
    let referenced_columns = if cons.pk_attrs.is_empty() {
        None
    } else {
        Some(parse_identifier_list(&cons.pk_attrs)?)
    };
    Ok(Some(ForeignKeySpec {
        columns,
        referenced_table,
        referenced_columns,
    }))
}

fn parse_identifier_list(nodes: &[pg_query::Node]) -> PgWireResult<Vec<String>> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let Some(n) = node.node.as_ref() else {
            return Err(fe("bad identifier"));
        };
        if let pg_query::NodeEnum::String(s) = n {
            out.push(s.sval.clone());
        } else {
            return Err(fe("identifier must be string"));
        }
    }
    Ok(out)
}

fn range_var_to_obj_name(rv: &RangeVar) -> ObjName {
    ObjName {
        schema: if rv.schemaname.is_empty() {
            None
        } else {
            Some(rv.schemaname.clone())
        },
        name: rv.relname.clone(),
    }
}
