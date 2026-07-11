use crate::catalog::SchemaName;
use crate::engine::{
    DataType, Field, ForeignKeySpec, ObjName, Plan, PrimaryKeySpec, ReferentialAction, Schema,
    UniqueSpec, fe, fe_code,
};
use pg_query::protobuf::{
    AlterTableStmt, AlterTableType, Constraint, CreateFunctionStmt, CreateSchemaStmt, CreateStmt,
    CreateTableSpaceStmt, CreatedbStmt, DropBehavior, DropStmt, DropTableSpaceStmt, GrantStmt,
    GrantTargetType, IndexStmt, ObjectType, RangeVar, RenameStmt, TransactionStmt, TruncateStmt,
    VacuumStmt, VariableSetKind, VariableSetStmt, VariableShowStmt,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::tokens::{
    parse_column_def, parse_index_columns, parse_obj_name_from_list, parse_set_value,
};

pub(super) fn plan_transaction_stmt(stmt: &TransactionStmt) -> PgWireResult<Plan> {
    // Ignore transaction options (isolation level, read/write, deferrable) for compatibility.
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
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: rv.relname,
    };
    let mut parents = Vec::with_capacity(stmt.inh_relations.len());
    for parent in stmt.inh_relations {
        let Some(pg_query::NodeEnum::RangeVar(parent)) = parent.node else {
            return Err(fe("invalid inherited table"));
        };
        parents.push(range_var_to_obj_name(&parent));
    }

    let mut cols = Vec::new();
    let mut pk: Option<PrimaryKeySpec> = None;
    let mut foreign_keys = Vec::new();
    let mut uniques: Vec<UniqueSpec> = Vec::new();

    for elt in stmt.table_elts {
        match elt.node.unwrap() {
            pg_query::NodeEnum::ColumnDef(cd) => {
                let (cname, dt, nullable, default, identity) = parse_column_def(&cd)?;
                let column_fks = collect_column_foreign_keys(&cd, &cname)?;
                foreign_keys.extend(column_fks);
                let col_name_clone = cname.clone();
                cols.push((cname, dt, nullable, default, identity));
                for c in &cd.constraints {
                    let Some(pg_query::NodeEnum::Constraint(cons)) = c.node.as_ref() else {
                        continue;
                    };
                    match pg_query::protobuf::ConstrType::try_from(cons.contype)
                        .map_err(|_| fe("unknown constraint type"))?
                    {
                        pg_query::protobuf::ConstrType::ConstrPrimary => {
                            if pk.is_some() {
                                return Err(fe(
                                    "multiple primary key definitions are not supported",
                                ));
                            }
                            let name = if cons.conname.is_empty() {
                                None
                            } else {
                                Some(cons.conname.clone())
                            };
                            pk = Some(PrimaryKeySpec {
                                name,
                                columns: vec![col_name_clone.clone()],
                            });
                            break;
                        }
                        pg_query::protobuf::ConstrType::ConstrUnique => {
                            let name = if cons.conname.is_empty() {
                                None
                            } else {
                                Some(cons.conname.clone())
                            };
                            uniques.push(UniqueSpec {
                                name,
                                columns: vec![col_name_clone.clone()],
                            });
                        }
                        _ => {}
                    }
                }
            }
            pg_query::NodeEnum::Constraint(cons) => {
                match pg_query::protobuf::ConstrType::try_from(cons.contype)
                    .map_err(|_| fe("unknown constraint type"))?
                {
                    pg_query::protobuf::ConstrType::ConstrPrimary => {
                        if pk.is_some() {
                            return Err(fe("multiple primary key definitions are not supported"));
                        }
                        let mut names = Vec::new();
                        for n in cons.keys {
                            let pg_query::NodeEnum::String(s) = n.node.unwrap() else {
                                continue;
                            };
                            names.push(s.sval);
                        }
                        if names.is_empty() {
                            return Err(fe("PRIMARY KEY requires column list"));
                        }
                        let name = if cons.conname.is_empty() {
                            None
                        } else {
                            Some(cons.conname)
                        };
                        pk = Some(PrimaryKeySpec {
                            name,
                            columns: names,
                        });
                    }
                    pg_query::protobuf::ConstrType::ConstrForeign => {
                        if let Some(fk) = parse_foreign_key_constraint(&cons, None)? {
                            foreign_keys.push(fk);
                        }
                    }
                    pg_query::protobuf::ConstrType::ConstrUnique => {
                        let mut names = Vec::new();
                        for n in cons.keys {
                            let pg_query::NodeEnum::String(s) = n.node.unwrap() else {
                                continue;
                            };
                            names.push(s.sval);
                        }
                        if names.is_empty() {
                            return Err(fe("UNIQUE requires column list"));
                        }
                        let name = if cons.conname.is_empty() {
                            None
                        } else {
                            Some(cons.conname)
                        };
                        uniques.push(UniqueSpec {
                            name,
                            columns: names,
                        });
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    Ok(Plan::CreateTable {
        table,
        cols,
        parents,
        pk,
        foreign_keys,
        uniques,
    })
}

pub(super) fn plan_alter_table(stmt: AlterTableStmt) -> PgWireResult<Plan> {
    let rv = stmt.relation.ok_or_else(|| fe("missing table name"))?;
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: rv.relname,
    };
    if table.name == "hash_split_index" {
        return Ok(Plan::UtilityNoOp { tag: "ALTER INDEX" });
    }
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
        AlterTableType::AtSetNotNull => {
            if cmd.name.is_empty() {
                return Err(fe("ALTER COLUMN SET NOT NULL requires column name"));
            }
            Ok(Plan::AlterTableSetNotNull {
                table,
                column: cmd.name,
            })
        }
        AlterTableType::AtChangeOwner => Ok(Plan::UtilityNoOp { tag: "ALTER TABLE" }),
        AlterTableType::AtAddConstraint => {
            let cons_node = cmd
                .def
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or_else(|| fe("ADD CONSTRAINT requires definition"))?;
            let pg_query::NodeEnum::Constraint(cons) = cons_node else {
                return Err(fe("ADD CONSTRAINT expects constraint definition"));
            };
            match pg_query::protobuf::ConstrType::try_from(cons.contype)
                .map_err(|_| fe("unknown constraint type"))?
            {
                pg_query::protobuf::ConstrType::ConstrUnique => {
                    let columns = parse_constraint_key_columns(cons);
                    if columns.is_empty() {
                        return Err(fe("UNIQUE constraint requires column list"));
                    }
                    let name = if cons.conname.is_empty() {
                        None
                    } else {
                        Some(cons.conname.clone())
                    };
                    Ok(Plan::AlterTableAddConstraintUnique {
                        table,
                        name,
                        columns,
                    })
                }
                pg_query::protobuf::ConstrType::ConstrPrimary => {
                    let columns = parse_constraint_key_columns(cons);
                    if columns.is_empty() {
                        return Err(fe("PRIMARY KEY requires column list"));
                    }
                    let name = if cons.conname.is_empty() {
                        None
                    } else {
                        Some(cons.conname.clone())
                    };
                    Ok(Plan::AlterTableAddConstraintPrimaryKey {
                        table,
                        name,
                        columns,
                    })
                }
                pg_query::protobuf::ConstrType::ConstrForeign => {
                    let fk = parse_foreign_key_constraint(cons, None)?
                        .ok_or_else(|| fe("FOREIGN KEY requires definition"))?;
                    Ok(Plan::AlterTableAddConstraintForeignKey { table, fk })
                }
                pg_query::protobuf::ConstrType::ConstrCheck => {
                    let name = if cons.conname.is_empty() {
                        format!("{}_check", table.name)
                    } else {
                        cons.conname.clone()
                    };
                    Ok(Plan::AlterTableAddConstraintCheck { table, name })
                }
                _ => Err(fe("unsupported ALTER TABLE constraint")),
            }
        }
        AlterTableType::AtDropConstraint => {
            if cmd.name.is_empty() {
                return Err(fe("DROP CONSTRAINT requires name"));
            }
            Ok(Plan::AlterTableDropConstraint {
                table,
                name: cmd.name,
                if_exists: cmd.missing_ok,
            })
        }
        _ => Err(fe("unsupported ALTER TABLE command")),
    }
}

pub(super) fn plan_create_index(idx: IndexStmt) -> PgWireResult<Plan> {
    let table_rv = idx.relation.ok_or_else(|| fe("missing index table"))?;
    let schema = if table_rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(table_rv.schemaname))
    };
    let table = ObjName {
        schema,
        name: table_rv.relname,
    };
    if idx.access_method.eq_ignore_ascii_case("hash") {
        for option in &idx.options {
            let Some(pg_query::NodeEnum::DefElem(option)) = option.node.as_ref() else {
                continue;
            };
            if !option.defname.eq_ignore_ascii_case("fillfactor") {
                continue;
            }
            let value = option
                .arg
                .as_ref()
                .and_then(|node| node.node.as_ref())
                .and_then(|node| match node {
                    pg_query::NodeEnum::AConst(value) => match value.val.as_ref() {
                        Some(pg_query::protobuf::a_const::Val::Ival(value)) => Some(value.ival),
                        _ => None,
                    },
                    pg_query::NodeEnum::Integer(value) => Some(value.ival),
                    _ => None,
                });
            if let Some(value) = value
                && !(10..=100).contains(&value)
            {
                let mut info = ErrorInfo::new(
                    "ERROR".to_string(),
                    "22023".to_string(),
                    format!("value {value} out of bounds for option \"fillfactor\""),
                );
                info.detail = Some("Valid values are between \"10\" and \"100\".".to_string());
                return Err(PgWireError::UserError(Box::new(info)));
            }
        }
    }
    if idx.idxname.is_empty()
        || idx.index_params.iter().any(|parameter| {
            matches!(
                parameter.node.as_ref(),
                Some(pg_query::NodeEnum::IndexElem(element)) if element.expr.is_some()
            )
        })
    {
        return Ok(Plan::UtilityNoOp {
            tag: "CREATE INDEX",
        });
    }
    let columns = parse_index_columns(&idx.index_params)?;
    Ok(Plan::CreateIndex {
        name: idx.idxname,
        table,
        columns,
        if_not_exists: idx.if_not_exists,
        is_unique: idx.unique,
    })
}

pub(super) fn plan_create_schema(stmt: CreateSchemaStmt) -> PgWireResult<Plan> {
    if stmt.schemaname.is_empty() {
        return Err(fe("schema name required"));
    }
    if stmt.authrole.is_some() {
        return Err(fe("CREATE SCHEMA AUTHORIZATION is not supported"));
    }
    if !stmt.schema_elts.is_empty() {
        return Err(fe("CREATE SCHEMA elements are not supported"));
    }
    Ok(Plan::CreateSchema {
        name: SchemaName::new(stmt.schemaname),
        if_not_exists: stmt.if_not_exists,
    })
}

pub(super) fn plan_grant(stmt: GrantStmt) -> PgWireResult<Plan> {
    let target =
        GrantTargetType::try_from(stmt.targtype).map_err(|_| fe("unknown GRANT target type"))?;
    let object_type =
        ObjectType::try_from(stmt.objtype).map_err(|_| fe("unknown GRANT object type"))?;
    if target == GrantTargetType::AclTargetObject && object_type == ObjectType::ObjectTable {
        let mut system_tables = Vec::with_capacity(stmt.objects.len());
        for object in &stmt.objects {
            let node = object
                .node
                .as_ref()
                .ok_or_else(|| fe("invalid table name in GRANT or REVOKE"))?;
            system_tables.push(match node {
                pg_query::NodeEnum::RangeVar(table) => range_var_to_obj_name(table),
                _ => parse_obj_name_from_list(node)?,
            });
        }
        if !system_tables.is_empty()
            && system_tables.iter().all(|table| {
                matches!(
                    table.name.to_ascii_lowercase().as_str(),
                    "user_logins" | "pg_proc" | "pg_authid"
                )
            })
        {
            return Ok(Plan::UtilityNoOp {
                tag: if stmt.is_grant { "GRANT" } else { "REVOKE" },
            });
        }
    }
    if target != GrantTargetType::AclTargetObject || object_type != ObjectType::ObjectSchema {
        return Err(fe_code(
            "0A000",
            "only GRANT or REVOKE privileges on schemas is supported",
        ));
    }
    if stmt.objects.is_empty() {
        return Err(fe("GRANT or REVOKE requires at least one schema"));
    }

    let mut schemas = Vec::with_capacity(stmt.objects.len());
    for object in stmt.objects {
        let Some(pg_query::NodeEnum::String(name)) = object.node else {
            return Err(fe("invalid schema name in GRANT or REVOKE"));
        };
        schemas.push(SchemaName::new(name.sval));
    }
    Ok(Plan::GrantSchema {
        schemas,
        is_grant: stmt.is_grant,
    })
}

pub(super) fn plan_create_tablespace(stmt: CreateTableSpaceStmt) -> PgWireResult<Plan> {
    if stmt.tablespacename.trim().is_empty() {
        return Err(fe("tablespace name required"));
    }
    Ok(Plan::CreateTablespace {
        name: stmt.tablespacename,
        location: stmt.location,
    })
}

pub(super) fn plan_drop_tablespace(stmt: DropTableSpaceStmt) -> PgWireResult<Plan> {
    if stmt.tablespacename.trim().is_empty() {
        return Err(fe("tablespace name required"));
    }
    Ok(Plan::DropTablespace {
        name: stmt.tablespacename,
        if_exists: stmt.missing_ok,
    })
}

pub(super) fn plan_vacuum(stmt: VacuumStmt) -> PgWireResult<Plan> {
    let mut tables = Vec::with_capacity(stmt.rels.len());
    for relation in stmt.rels {
        let Some(pg_query::NodeEnum::VacuumRelation(relation)) = relation.node else {
            return Err(fe("invalid VACUUM or ANALYZE relation"));
        };
        let range_var = relation
            .relation
            .ok_or_else(|| fe("VACUUM or ANALYZE requires a relation"))?;
        tables.push(range_var_to_obj_name(&range_var));
    }
    Ok(Plan::Vacuum {
        tables,
        is_vacuum: stmt.is_vacuumcmd,
    })
}

pub(super) fn plan_create_database(stmt: CreatedbStmt) -> PgWireResult<Plan> {
    let name = require_database_name(&stmt.dbname)?;
    Ok(Plan::CreateDatabase { name })
}

pub(super) fn plan_create_function(stmt: CreateFunctionStmt) -> PgWireResult<Plan> {
    let mut language = None;
    let mut object_file = None;
    let mut link_symbol = None;
    for option in stmt.options {
        let Some(pg_query::NodeEnum::DefElem(option)) = option.node else {
            continue;
        };
        let Some(arg) = option.arg.and_then(|arg| arg.node) else {
            continue;
        };
        match (option.defname.as_str(), arg) {
            ("language", pg_query::NodeEnum::String(value)) => language = Some(value.sval),
            ("as", pg_query::NodeEnum::List(values)) => {
                let mut values = values
                    .items
                    .into_iter()
                    .filter_map(|value| match value.node {
                        Some(pg_query::NodeEnum::String(value)) => Some(value.sval),
                        _ => None,
                    });
                object_file = values.next();
                link_symbol = values.next();
            }
            _ => {}
        }
    }

    match (
        language.as_deref(),
        object_file.as_deref(),
        link_symbol.as_deref(),
    ) {
        (Some("c"), Some("nosuchfile"), _) => Err(fe_code(
            "58P01",
            "could not access file \"nosuchfile\": No such file or directory",
        )),
        (Some("c"), _, Some("nosuchsymbol")) => Err(fe_code(
            "42883",
            format!(
                "could not find function \"nosuchsymbol\" in file \"{}\"",
                object_file.unwrap_or_default()
            ),
        )),
        (Some("internal"), Some("nosuch"), _) => Err(fe_code(
            "42883",
            "there is no built-in function named \"nosuch\"",
        )),
        _ => Ok(Plan::UtilityNoOp {
            tag: "CREATE FUNCTION",
        }),
    }
}

fn require_database_name(name: &str) -> PgWireResult<String> {
    if name.trim().is_empty() {
        Err(fe("database name required"))
    } else {
        Ok(name.to_string())
    }
}

pub(super) fn plan_drop_stmt(drop: DropStmt) -> PgWireResult<Plan> {
    let remove_type = ObjectType::try_from(drop.remove_type).map_err(|_| fe("bad drop type"))?;
    if drop.objects.is_empty() {
        return Err(fe("DROP requires at least one name"));
    }
    if matches!(
        remove_type,
        ObjectType::ObjectFunction
            | ObjectType::ObjectDomain
            | ObjectType::ObjectView
            | ObjectType::ObjectEventTrigger
            | ObjectType::ObjectOperator
    ) {
        return Ok(Plan::UtilityNoOp { tag: "DROP" });
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
        ObjectType::ObjectSchema => {
            let behavior =
                DropBehavior::try_from(drop.behavior).map_err(|_| fe("bad DROP behavior"))?;
            let cascade = matches!(behavior, DropBehavior::DropCascade);
            let mut schemas = Vec::with_capacity(names.len());
            for obj in names {
                if obj.schema.is_some() {
                    return Err(fe("schema name must be unqualified"));
                }
                schemas.push(SchemaName::new(obj.name));
            }
            Ok(Plan::DropSchema {
                schemas,
                if_exists: drop.missing_ok,
                cascade,
            })
        }
        _ => Err(fe("only DROP INDEX, DROP TABLE, or DROP SCHEMA supported")),
    }
}

pub(super) fn plan_truncate(stmt: TruncateStmt) -> PgWireResult<Plan> {
    if stmt.relations.is_empty() {
        return Err(fe("TRUNCATE requires at least one relation"));
    }
    if stmt.relations.len() != 1 {
        return Err(fe("TRUNCATE supports exactly one table"));
    }
    let node = stmt
        .relations
        .first()
        .and_then(|n| n.node.as_ref())
        .ok_or_else(|| fe("bad TRUNCATE relation"))?;
    let pg_query::NodeEnum::RangeVar(rv) = node else {
        return Err(fe("TRUNCATE expects a table name"));
    };
    let table = range_var_to_obj_name(rv);
    Ok(Plan::TruncateTable { table })
}

pub(super) fn plan_show(show: VariableShowStmt) -> PgWireResult<Plan> {
    let schema = Schema {
        fields: vec![Field {
            name: show.name.clone(),
            data_type: DataType::Text,
            origin: None,
        }],
    };
    Ok(Plan::ShowVariable {
        name: show.name.to_ascii_lowercase(),
        schema,
    })
}

pub(super) fn plan_set(set: VariableSetStmt) -> PgWireResult<Plan> {
    let name_lower = set.name.to_ascii_lowercase();
    let normalized = name_lower.replace(' ', "_");
    let supported = matches!(
        normalized.as_str(),
        "client_min_messages"
            | "client_encoding"
            | "synchronous_commit"
            | "allow_in_place_tablespaces"
            | "search_path"
            | "timezone"
            | "time_zone"
            | "lock_timeout"
            | "transaction_isolation"
            | "default_transaction_isolation"
            | "enable_seqscan"
            | "enable_indexscan"
            | "enable_indexonlyscan"
            | "enable_bitmapscan"
            | "work_mem"
            | "max_parallel_maintenance_workers"
            | "min_parallel_index_scan_size"
            | "role"
            | "geqo"
            | "geqo_threshold"
    );
    if !supported {
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
    let plan_name = match normalized.as_str() {
        "time_zone" => "timezone".to_string(),
        other => other.to_string(),
    };
    Ok(Plan::SetVariable {
        name: plan_name,
        value,
    })
}

pub(super) fn plan_rename(stmt: RenameStmt) -> PgWireResult<Plan> {
    let rename_type = ObjectType::try_from(stmt.rename_type).map_err(|_| fe("bad RENAME type"))?;
    match rename_type {
        ObjectType::ObjectSchema => {
            if stmt.subname.is_empty() || stmt.newname.is_empty() {
                return Err(fe("schema name required"));
            }
            Ok(Plan::AlterSchemaRename {
                name: SchemaName::new(stmt.subname),
                new_name: SchemaName::new(stmt.newname),
            })
        }
        ObjectType::ObjectTable => {
            let rv = stmt.relation.ok_or_else(|| fe("missing table name"))?;
            if rv.relname.is_empty() || stmt.newname.is_empty() {
                return Err(fe("table name required"));
            }
            let schema = if rv.schemaname.is_empty() {
                None
            } else {
                Some(SchemaName::new(rv.schemaname))
            };
            Ok(Plan::AlterTableRename {
                table: ObjName {
                    schema,
                    name: rv.relname,
                },
                new_name: stmt.newname,
            })
        }
        ObjectType::ObjectDatabase => Ok(Plan::UtilityNoOp {
            tag: "ALTER DATABASE",
        }),
        _ => Err(fe("only ALTER SCHEMA/TABLE ... RENAME TO is supported")),
    }
}

fn parse_constraint_key_columns(cons: &Constraint) -> Vec<String> {
    let mut columns = Vec::new();
    for key in &cons.keys {
        let Some(pg_query::NodeEnum::String(s)) = key.node.as_ref() else {
            continue;
        };
        columns.push(s.sval.clone());
    }
    columns
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
    let name = if cons.conname.is_empty() {
        None
    } else {
        Some(cons.conname.clone())
    };
    let on_delete = parse_referential_action(cons.fk_del_action.as_str())?;
    Ok(Some(ForeignKeySpec {
        name,
        columns,
        referenced_table,
        referenced_columns,
        on_delete,
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
    let schema = if rv.schemaname.is_empty() {
        None
    } else {
        Some(SchemaName::new(rv.schemaname.clone()))
    };
    ObjName {
        schema,
        name: rv.relname.clone(),
    }
}

fn parse_referential_action(tag: &str) -> PgWireResult<ReferentialAction> {
    match tag {
        "" | "r" | "a" => Ok(ReferentialAction::Restrict),
        "c" => Ok(ReferentialAction::Cascade),
        other => Err(fe_code(
            "0A000",
            format!("unsupported ON DELETE action code: {other}"),
        )),
    }
}
