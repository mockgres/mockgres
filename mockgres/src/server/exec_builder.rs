use std::sync::Arc;

use pgwire::error::PgWireResult;

use crate::db::Db;
use crate::engine::{
    ExecNode, Expr, FilterExec, LimitExec, OrderExec, Plan, ProjectExec, ScalarExpr, Schema,
    SeqScanExec, UpdateSet, Value, ValuesExec, fe, fe_code,
};

use super::errors::map_db_err;
use super::insert::evaluate_insert_source;

pub fn command_tag(plan: &Plan) -> &'static str {
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
        Plan::DropTable { .. } => "DROP TABLE",
        Plan::ShowVariable { .. } => "SHOW",
        Plan::SetVariable { .. } => "SET",
        Plan::InsertValues { .. } => "INSERT 0",
        Plan::Update { .. } => "UPDATE 0",
        Plan::Delete { .. } => "DELETE 0",
        Plan::UnboundSeqScan { .. } => "SELECT 0",
        Plan::BeginTransaction => "BEGIN",
        Plan::CommitTransaction => "COMMIT",
        Plan::RollbackTransaction => "ROLLBACK",
    }
}

pub fn build_executor(
    db: &Arc<parking_lot::RwLock<Db>>,
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
            let (child, _tag, cnt) = build_executor(db, input, params.clone())?;
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
            let db_read = db.read();
            let schema_name = table.schema.as_deref().unwrap_or("public");
            let _tm = db_read
                .resolve_table(schema_name, &table.name)
                .map_err(map_db_err)?;
            let positions: Vec<usize> = cols.iter().map(|(i, _)| *i).collect();
            let (rows, _) = if positions.is_empty() && schema.fields.is_empty() {
                (vec![], vec![])
            } else {
                db_read
                    .scan_bound_positions(schema_name, &table.name, &positions)
                    .map_err(map_db_err)?
            };
            drop(db_read);
            let cnt = rows.len();
            Ok((
                Box::new(SeqScanExec::new(schema.clone(), rows)),
                None,
                Some(cnt),
            ))
        }

        Plan::Filter {
            input,
            expr,
            project_prefix_len,
        } => {
            let (child, _tag, _cnt) = build_executor(db, input, params.clone())?;
            let child_schema = child.schema().clone();
            let mut node: Box<dyn ExecNode> = Box::new(FilterExec::new(
                child_schema.clone(),
                child,
                expr.clone(),
                params.clone(),
            ));

            if let Some(n) = *project_prefix_len {
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
            let (child, _tag, cnt) = build_executor(db, input, params.clone())?;
            let schema = child.schema().clone();
            let exec = Box::new(OrderExec::new(schema, child, keys.clone(), params.clone())?);
            Ok((exec, None, cnt))
        }

        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let limit_val = *limit;
            let offset_val = *offset;
            let (child, _tag, cnt) = build_executor(db, input, params.clone())?;
            let remaining_after_offset = cnt.map(|c| c.saturating_sub(offset_val));
            let out_cnt = match (remaining_after_offset, limit_val) {
                (Some(c), Some(lim)) => Some(c.min(lim)),
                (Some(c), None) => Some(c),
                _ => None,
            };
            let schema = child.schema().clone();
            Ok((
                Box::new(LimitExec::new(schema, child, limit_val, offset_val)),
                None,
                out_cnt,
            ))
        }

        Plan::CreateTable { table, cols, pk } => {
            let mut db = db.write();
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
            let mut db = db.write();
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
            let mut db = db.write();
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
            let mut db = db.write();
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
            let mut db = db.write();
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
        Plan::DropTable { tables, if_exists } => {
            let mut db = db.write();
            for table in tables {
                let schema_name = table.schema.as_deref().unwrap_or("public");
                db.drop_table(schema_name, &table.name, *if_exists)
                    .map_err(map_db_err)?;
            }
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("DROP TABLE".into()),
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
                let db = db.read();
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
                let mut full = vec![crate::db::CellInput::Default; table_meta.columns.len()];
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
            let mut db = db.write();
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
            let mut db = db.write();
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
            let mut db = db.write();
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
            let value = match super::mapping::lookup_show_value(name) {
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
        Plan::BeginTransaction => {
            let mut db = db.write();
            db.begin_transaction().map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("BEGIN".into()),
                None,
            ))
        }
        Plan::CommitTransaction => {
            let mut db = db.write();
            db.commit_transaction().map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("COMMIT".into()),
                None,
            ))
        }
        Plan::RollbackTransaction => {
            let mut db = db.write();
            db.rollback_transaction().map_err(map_db_err)?;
            drop(db);
            Ok((
                Box::new(ValuesExec::new(Schema { fields: vec![] }, vec![])?),
                Some("ROLLBACK".into()),
                None,
            ))
        }
        Plan::UnboundSeqScan { .. } => Err(fe("unbound plan; call binder first")),
    }
}
