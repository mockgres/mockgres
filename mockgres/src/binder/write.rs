use super::*;

pub(super) fn bind_write_plan(
    db: &Db,
    search_path: &[SchemaId],
    current_database: Option<&str>,
    time_ctx: BindTimeContext,
    cte_scope: &CteScope,
    p: Plan,
) -> PgWireResult<Plan> {
    match p {
        Plan::CallBuiltin { name, args, schema } => Ok(Plan::CallBuiltin { name, args, schema }),
        Plan::InsertValues {
            mut table,
            columns,
            rows,
            override_system_value,
            on_conflict,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: None,
            });
            let table_schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let bound_on_conflict = match on_conflict {
                Some(OnConflictAction::DoUpdate {
                    target,
                    sets,
                    where_clause,
                }) => {
                    let bound_sets = bind_update_sets(
                        sets,
                        &table_schema,
                        tm,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        true,
                    )?;
                    let bound_where = where_clause
                        .map(|c| {
                            bind_bool_expr_allow_excluded(
                                &c,
                                &table_schema,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                                cte_scope,
                            )
                        })
                        .transpose()?;
                    Some(OnConflictAction::DoUpdate {
                        target,
                        sets: bound_sets,
                        where_clause: bound_where,
                    })
                }
                Some(OnConflictAction::DoNothing { target }) => {
                    Some(OnConflictAction::DoNothing { target })
                }
                None => None,
            };
            let column_positions = if let Some(cols) = &columns {
                let mut seen = HashSet::new();
                let mut positions = Vec::with_capacity(cols.len());
                for col in cols {
                    let idx = tm
                        .columns
                        .iter()
                        .position(|c| c.name == *col)
                        .ok_or_else(|| fe_code("42703", format!("unknown column: {col}")))?;
                    if !seen.insert(idx) {
                        return Err(fe_code("42701", format!("column {col} specified twice")));
                    }
                    positions.push(idx);
                }
                positions
            } else {
                (0..table_schema.len()).collect()
            };
            let expected_len = column_positions.len();
            let mut bound_rows = Vec::with_capacity(rows.len());
            for row in rows {
                if row.len() != expected_len && (columns.is_some() || row.len() > expected_len) {
                    let msg = if columns.is_some() {
                        format!(
                            "INSERT has {} target columns but {} expressions",
                            expected_len,
                            row.len()
                        )
                    } else {
                        format!(
                            "INSERT expects {} expressions, got {}",
                            expected_len,
                            row.len()
                        )
                    };
                    return Err(fe_code("21P01", msg));
                }
                let mut bound_row = Vec::with_capacity(row.len());
                for (expr_idx, src) in row.into_iter().enumerate() {
                    let target_idx = column_positions[expr_idx];
                    match src {
                        InsertSource::Default => bound_row.push(InsertSource::Default),
                        InsertSource::Expr(expr) => {
                            let field = table_schema.field(target_idx);
                            let hint = match field.data_type {
                                DataType::Int2
                                | DataType::Int4
                                | DataType::Int8
                                | DataType::Float8
                                | DataType::Varchar(_)
                                | DataType::PgChar
                                | DataType::Name
                                | DataType::BpChar(_)
                                | DataType::Point
                                | DataType::Lseg
                                | DataType::Line
                                | DataType::Circle
                                | DataType::Box
                                | DataType::Tid
                                | DataType::Path
                                | DataType::Bool
                                | DataType::Date
                                | DataType::Timestamp
                                | DataType::Timestamptz
                                | DataType::Bytea => Some(&field.data_type),
                                _ => None,
                            };
                            let bound = bind_scalar_expr(
                                &expr,
                                &table_schema,
                                hint,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                            )?;
                            bound_row.push(InsertSource::Expr(bound));
                        }
                    }
                }
                bound_rows.push(bound_row);
            }
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &table_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::InsertValues {
                table,
                columns,
                rows: bound_rows,
                override_system_value,
                on_conflict: bound_on_conflict,
                returning,
                returning_schema,
            })
        }
        Plan::InsertSelect {
            mut table,
            columns,
            select,
            override_system_value,
            on_conflict,
            mut returning,
            returning_schema: _,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            let schema_origin = Some(FieldOrigin {
                schema: Some(tm.schema.as_str().to_string()),
                table: Some(tm.name.clone()),
                alias: None,
            });
            let table_schema = Schema {
                fields: tm
                    .columns
                    .iter()
                    .map(|c| Field {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        origin: schema_origin.clone(),
                    })
                    .collect(),
            };
            let bound_on_conflict = match on_conflict {
                Some(OnConflictAction::DoUpdate {
                    target,
                    sets,
                    where_clause,
                }) => {
                    let bound_sets = bind_update_sets(
                        sets,
                        &table_schema,
                        tm,
                        db,
                        search_path,
                        current_database,
                        time_ctx,
                        true,
                    )?;
                    let bound_where = where_clause
                        .map(|c| {
                            bind_bool_expr_allow_excluded(
                                &c,
                                &table_schema,
                                db,
                                search_path,
                                current_database,
                                time_ctx,
                                cte_scope,
                            )
                        })
                        .transpose()?;
                    Some(OnConflictAction::DoUpdate {
                        target,
                        sets: bound_sets,
                        where_clause: bound_where,
                    })
                }
                Some(OnConflictAction::DoNothing { target }) => {
                    Some(OnConflictAction::DoNothing { target })
                }
                None => None,
            };
            let bound_select = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *select,
            )?;
            let expected_len = columns.as_ref().map_or(table_schema.len(), Vec::len);
            let actual_len = bound_select.schema().len();
            if actual_len != expected_len {
                let msg = if columns.is_some() {
                    format!(
                        "INSERT has {} target columns but {} expressions",
                        expected_len, actual_len
                    )
                } else {
                    format!(
                        "INSERT expects {} expressions, got {}",
                        expected_len, actual_len
                    )
                };
                return Err(fe_code("21P01", msg));
            }
            let returning_schema = match returning.as_mut() {
                Some(clause) => Some(bind_returning_clause(
                    clause,
                    &table_schema,
                    db,
                    search_path,
                    current_database,
                    time_ctx,
                )?),
                None => None,
            };
            Ok(Plan::InsertSelect {
                table,
                columns,
                select: Box::new(bound_select),
                override_system_value,
                on_conflict: bound_on_conflict,
                returning,
                returning_schema,
            })
        }
        Plan::CreateTable {
            table,
            cols,
            parents,
            pk,
            foreign_keys,
            uniques,
        } => {
            let (cols, parents) = bind_inherited_columns(db, search_path, cols, parents)?;
            Ok(Plan::CreateTable {
                table,
                cols,
                parents,
                pk,
                foreign_keys,
                uniques,
            })
        }
        Plan::CreateTableAs {
            table,
            column_names,
            query,
            with_data,
            if_not_exists,
        } => {
            let query = bind_with_search_path(
                db,
                search_path,
                current_database,
                time_ctx,
                cte_scope,
                *query,
            )?;
            if column_names.len() > query.schema().fields.len() {
                return Err(fe_code("42601", "too many column names were specified"));
            }
            let mut seen = HashSet::with_capacity(query.schema().fields.len());
            for (index, field) in query.schema().fields.iter().enumerate() {
                let name = column_names.get(index).unwrap_or(&field.name);
                if !seen.insert(name) {
                    return Err(fe_code(
                        "42701",
                        format!("column \"{name}\" specified more than once"),
                    ));
                }
            }
            Ok(Plan::CreateTableAs {
                table,
                column_names,
                query: Box::new(query),
                with_data,
                if_not_exists,
            })
        }
        Plan::CopyFrom {
            mut table,
            columns,
            filename,
            encoding,
        } => {
            let tm = resolve_table_meta(db, search_path, &table).map_err(map_catalog_err)?;
            if table.schema.is_none() {
                table.schema = Some(tm.schema.clone());
            }
            if let Some(columns) = &columns {
                let mut seen = HashSet::with_capacity(columns.len());
                for column in columns {
                    if !tm.columns.iter().any(|candidate| candidate.name == *column) {
                        return Err(fe_code("42703", format!("unknown column: {column}")));
                    }
                    if !seen.insert(column) {
                        return Err(fe_code("42701", format!("column {column} specified twice")));
                    }
                }
            }
            Ok(Plan::CopyFrom {
                table,
                columns,
                filename,
                encoding,
            })
        }
        Plan::CreateDatabase { .. } => Ok(p),
        Plan::DropDatabase { name } => Ok(Plan::UnsupportedDbDDL {
            kind: DbDdlKind::Drop,
            name,
        }),
        Plan::AlterDatabase { name } => Ok(Plan::UnsupportedDbDDL {
            kind: DbDdlKind::Alter,
            name,
        }),
        Plan::UnsupportedDbDDL { .. } => Ok(p),

        other => Ok(other),
    }
}
